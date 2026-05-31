#!/usr/bin/env python3
"""
Execution Reports Aggregator v9 — True Streaming Edition.

Reads orders:exec stream chunk-by-chunk, writes each chunk to disk
immediately, and keeps only lightweight per-group profit accumulators
in memory.  Peak RSS ≈ O(chunk_size + num_groups) instead of O(total_records).

Usage:
    python3 aggregate_exec.py --start "2025-10-25T00:00:00Z" \
                               --end "2025-10-26T00:00:00Z" \
                               --out reports/exec_2025-10-25.parquet
"""

import csv
import json
import math
import os
import argparse
import resource
import time
from collections import defaultdict
from datetime import datetime

import redis

from core.redis_client import get_redis, wait_for_redis
from core.redis_keys import RedisStreams as RS, RedisKeyPrefixes as RK

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
EXEC_STREAM = os.getenv("EXEC_STREAM", RS.ORDERS_EXEC)
SNAP_PREFIX = os.getenv("SNAP_PREFIX", RK.SIGNAL_SNAP)

# Chunk sizes ─ tune for throughput vs memory
_REDIS_CHUNK = int(os.getenv("EXEC_AGG_REDIS_CHUNK", "5000"))
_SNAP_BATCH = int(os.getenv("EXEC_AGG_SNAP_BATCH", "1000"))

# Hard RSS safety limit (MiB)
_MAX_RSS_MB = int(os.getenv("AGGREGATOR_MAX_RSS_MB", "2048"))

# Numeric columns that need type coercion on export
_NUMERIC_COLS = frozenset(
    ["price", "exec_price", "profit", "volume", "lot", "sl", "tp"]
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _rss_mb() -> float:
    """Current peak RSS in MiB."""
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024


def _check_rss(label: str = "") -> None:
    rss = _rss_mb()
    if rss > _MAX_RSS_MB:
        raise MemoryError(
            f"RSS {rss:.0f} MB exceeds limit {_MAX_RSS_MB} MB at [{label}]"
        )


def to_ms(s: str) -> str:
    """Convert ISO or epoch-ms string to Redis stream ID."""
    if s.isdigit():
        return f"{int(s)}-0"
    dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
    return f"{int(dt.timestamp() * 1000)}-0"


def _advance_cursor(last_mid) -> str:
    """Return stream ID immediately after *last_mid*."""
    if isinstance(last_mid, bytes):
        last_mid = last_mid.decode()
    ms, seq = last_mid.split("-")
    return f"{ms}-{int(seq) + 1}"


# ---------------------------------------------------------------------------
# Snapshot loader (batched MGET)
# ---------------------------------------------------------------------------
def _fetch_snapshots(r: redis.Redis, sids: set) -> dict:
    """Batch-fetch signal snapshots. Returns {sid: {note, side, entry, atr}}."""
    out: dict = {}
    if not sids:
        return out

    sids_list = list(sids)
    for i in range(0, len(sids_list), _SNAP_BATCH):
        batch = sids_list[i : i + _SNAP_BATCH]
        keys = [SNAP_PREFIX + sid for sid in batch]
        try:
            vals = r.mget(keys)
            for sid, val in zip(batch, vals):
                if not val:
                    continue
                try:
                    if isinstance(val, bytes):
                        val = val.decode()
                    j = json.loads(val)
                    out[sid] = {
                        "note": j.get("note", ""),
                        "side": j.get("side", ""),
                        "entry": float(j.get("price") or 0),
                        "atr": j.get("risk", {}).get("atr", 0.0),
                    }
                except Exception:
                    pass
        except Exception as e:
            print(f"⚠️  Snapshot batch error: {e}")

    return out


# ---------------------------------------------------------------------------
# Row parser
# ---------------------------------------------------------------------------
def _parse_row(fields: dict, snapshots: dict) -> dict:
    """Parse raw stream entry → flat dict, attach snapshot in place."""
    row = fields  # reuse dict — no copy

    j_str = row.pop("json", None)
    if j_str and isinstance(j_str, str) and j_str.startswith("{"):
        try:
            row.update(json.loads(j_str))
        except json.JSONDecodeError:
            pass

    sid = str(row.get("sid", ""))
    if sid and sid in snapshots:
        row.update(snapshots[sid])

    return row


# ---------------------------------------------------------------------------
# Online per-group accumulator (Welford + min/max + profit list for median)
# ---------------------------------------------------------------------------
class StreamingStats:
    """
    Memory: O(total_profit_values) — only float profits kept per group,
    NOT full record dicts.  For 100K trades ≈ 0.8 MB vs 100+ MB raw.
    """

    __slots__ = ("_g", "total_n", "total_profit", "total_wins")

    def __init__(self):
        self._g: dict[str, dict] = defaultdict(
            lambda: {
                "n": 0,
                "mean": 0.0,
                "M2": 0.0,
                "total": 0.0,
                "wins": 0,
                "pmax": float("-inf"),
                "pmin": float("inf"),
                "profits": [],
            }
        )
        self.total_n = 0
        self.total_profit = 0.0
        self.total_wins = 0

    def update(self, group: str, profit: float) -> None:
        self.total_n += 1
        self.total_profit += profit
        if profit > 0:
            self.total_wins += 1

        g = self._g[group]
        g["n"] += 1
        delta = profit - g["mean"]
        g["mean"] += delta / g["n"]
        g["M2"] += delta * (profit - g["mean"])
        g["total"] += profit
        if profit > 0:
            g["wins"] += 1
        if profit > g["pmax"]:
            g["pmax"] = profit
        if profit < g["pmin"]:
            g["pmin"] = profit
        g["profits"].append(profit)

    # ---------- reporting ------------------------------------------------
    def print_summary(self) -> None:
        if self.total_n == 0:
            print("⚠️  No 'profit' data found")
            print("   Make sure MT5 executor sends profit in /orders/confirm payloads")
            return

        print()
        print("=" * 80)
        print("📈 Performance Summary by GROUP")
        print("=" * 80)

        header = (
            f"{'group':<30} {'trades':>6} {'avg':>10} {'median':>10} "
            f"{'total':>12} {'WR':>7} {'max':>10} {'min':>10} "
            f"{'std':>10} {'sharpe':>8}"
        )
        print(header)
        print("-" * len(header))

        sorted_groups = sorted(self._g.items(), key=lambda x: -x[1]["total"])
        for key, g in sorted_groups:
            n = g["n"]
            std = math.sqrt(g["M2"] / (n - 1)) if n > 1 else 0.0
            ps = sorted(g["profits"])
            mid = len(ps) // 2
            median = (
                ps[mid]
                if n % 2 == 1
                else (ps[mid - 1] + ps[mid]) / 2
                if n > 1
                else (ps[0] if n == 1 else 0.0)
            )
            wr = g["wins"] / n if n else 0
            pmax = g["pmax"] if g["pmax"] != float("-inf") else 0
            pmin = g["pmin"] if g["pmin"] != float("inf") else 0
            sharpe = g["mean"] / (std + 1e-9)
            print(
                f"{key:<30} {n:>6} {g['mean']:>10.2f} {median:>10.2f} "
                f"{g['total']:>12.2f} {wr:>6.1%} {pmax:>10.2f} {pmin:>10.2f} "
                f"{std:>10.2f} {sharpe:>8.3f}"
            )

        print()
        print("=" * 80)
        print("🎯 OVERALL SUMMARY")
        print("=" * 80)
        wr_all = self.total_wins / self.total_n if self.total_n else 0
        print(f"Total trades:    {self.total_n}")
        print(f"Total profit:    ${self.total_profit:.2f}")
        print(f"Average profit:  ${self.total_profit / self.total_n:.2f}")
        print(f"Win rate:        {wr_all:.1%}")
        print(f"Peak RSS:        {_rss_mb():.0f} MB")
        print()


# ---------------------------------------------------------------------------
# Core streaming pipeline
# ---------------------------------------------------------------------------
def stream_and_write(
    r: redis.Redis, start_id: str, end_id: str, out_path: str
) -> None:
    """
    True streaming: chunk → parse → write-to-disk → discard.

    1. Read _REDIS_CHUNK entries from orders:exec
    2. Batch-fetch snapshots for SIDs in this chunk
    3. Parse / flatten / enrich each row
    4. Update lightweight StreamingStats accumulator (profits only)
    5. Append rows to CSV on disk
    6. Free chunk memory
    7. At the end: convert CSV → Parquet if requested (chunked read)
    """
    stats = StreamingStats()
    last = start_id
    total = 0

    # Always stream to CSV first; convert to parquet at end if needed
    want_parquet = out_path.endswith(".parquet")
    csv_path = out_path if not want_parquet else out_path.replace(".parquet", ".tmp.csv")

    csv_fh = None
    csv_writer = None
    fieldnames = None

    print(
        f"📊 Streaming {EXEC_STREAM}: {start_id} → {end_id} "
        f"(chunk={_REDIS_CHUNK})"
    )

    try:
        while True:
            # ── 1. Read chunk from Redis ─────────────────────────────
            try:
                chunk = r.xrange(
                    EXEC_STREAM, min=last, max=end_id, count=_REDIS_CHUNK
                )
            except redis.exceptions.BusyLoadingError:
                print("⚠️  Redis loading, retry in 10s...")
                time.sleep(10)
                continue
            except redis.exceptions.ConnectionError as e:
                print(f"❌ Redis connection error: {e}")
                raise

            if not chunk:
                break

            # ── 2. Collect raw rows + SIDs for this chunk ────────────
            raw_rows = []
            chunk_sids: set[str] = set()
            for mid, fields in chunk:
                d = dict(fields)
                if isinstance(mid, bytes):
                    mid = mid.decode()
                d["stream_id"] = mid
                raw_rows.append(d)
                sid = d.get("sid")
                if sid:
                    chunk_sids.add(str(sid) if isinstance(sid, bytes) else sid)

            # ── 3. Fetch snapshots for this chunk only ───────────────
            snapshots = _fetch_snapshots(r, chunk_sids)

            # ── 4. Parse + flatten rows ──────────────────────────────
            rows = []
            for d in raw_rows:
                row = _parse_row(d, snapshots)

                # Coerce numeric in-place (cheap)
                for col in _NUMERIC_COLS:
                    v = row.get(col)
                    if v is not None:
                        try:
                            row[col] = float(v)
                        except (ValueError, TypeError):
                            pass

                rows.append(row)

            total += len(rows)

            # ── 5. Update online stats (profits only) ────────────────
            for row in rows:
                profit = row.get("profit")
                if profit is not None and not (
                    isinstance(profit, float) and math.isnan(profit)
                ):
                    try:
                        p = float(profit)
                        group = str(
                            row.get("note", row.get("status", "unknown"))
                        )
                        stats.update(group, p)
                    except (ValueError, TypeError):
                        pass

            # ── 6. Append to CSV on disk ─────────────────────────────
            if csv_writer is None:
                # Discover fieldnames from the first chunk
                fieldnames = sorted(
                    {k for row in rows for k in row.keys()}
                )
                csv_fh = open(csv_path, "w", newline="", encoding="utf-8")
                csv_writer = csv.DictWriter(
                    csv_fh, fieldnames=fieldnames, extrasaction="ignore"
                )
                csv_writer.writeheader()

            csv_writer.writerows(rows)
            csv_fh.flush()

            # ── 7. Discard chunk memory ──────────────────────────────
            del raw_rows, chunk_sids, snapshots, rows

            # Advance cursor
            last = _advance_cursor(chunk[-1][0])

            if total % 20_000 == 0:
                print(f"  ... {total:,} records streamed (RSS {_rss_mb():.0f} MB)")

            _check_rss("streaming_loop")

    finally:
        if csv_fh:
            csv_fh.close()

    print(f"✅ Streamed {total:,} records → {csv_path}")

    # ── Convert CSV → Parquet (chunked, memory-safe) ─────────────────
    if want_parquet and total > 0:
        _csv_to_parquet_chunked(csv_path, out_path)

    # ── Print summary from online accumulators ───────────────────────
    stats.print_summary()


# ---------------------------------------------------------------------------
# CSV → Parquet conversion (chunked via pyarrow row-group writer)
# ---------------------------------------------------------------------------
def _csv_to_parquet_chunked(csv_path: str, parquet_path: str) -> None:
    """
    Convert CSV to Parquet using PyArrow's ParquetWriter for chunked writes.
    Each CSV chunk becomes a separate row group — constant memory.
    """
    print("📦 Converting CSV → Parquet (chunked)...")
    try:
        import pandas as pd
        import pyarrow as pa
        import pyarrow.parquet as pq

        reader = pd.read_csv(csv_path, chunksize=50_000, low_memory=False)
        pw = None
        rows_converted = 0

        for df_chunk in reader:
            # Coerce numeric columns for proper Parquet types
            for col in _NUMERIC_COLS:
                if col in df_chunk.columns:
                    df_chunk[col] = pd.to_numeric(df_chunk[col], errors="coerce")

            table = pa.Table.from_pandas(df_chunk, preserve_index=False)

            if pw is None:
                pw = pq.ParquetWriter(parquet_path, table.schema)

            pw.write_table(table)
            rows_converted += len(df_chunk)
            del df_chunk, table

        if pw:
            pw.close()

        os.remove(csv_path)
        print(f"✅ Parquet written: {parquet_path} ({rows_converted:,} rows)")

    except ImportError:
        print("⚠️  pyarrow not available, keeping CSV output")
    except Exception as e:
        print(f"⚠️  Parquet conversion failed: {e}")
        print(f"    CSV retained at: {csv_path}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    ap = argparse.ArgumentParser(
        description="Aggregate execution reports with PnL analysis (streaming v9)"
    )
    ap.add_argument("--start", required=True, help="Start time (ISO or epoch-ms)")
    ap.add_argument("--end", required=True, help="End time (ISO or epoch-ms)")
    ap.add_argument("--out", required=True, help="Output file (*.csv or *.parquet)")
    args = ap.parse_args()

    print("=" * 80)
    print("📊 XAUUSD Execution Reports Aggregator v9 (true streaming)")
    print("=" * 80)
    print()

    # ── Connect to Redis ─────────────────────────────────────────────
    redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
    print("🔌 Connecting to Redis...")
    try:
        r = get_redis(retry_attempts=20, retry_delay=2)
        print("⏳ Waiting for Redis to be ready...")
        if not wait_for_redis(r, max_retries=30, delay=10.0):
            raise RuntimeError("Redis is not ready after waiting")
        print(f"✅ Redis connection established: {redis_url.split('@')[-1]}")
    except redis.exceptions.BusyLoadingError:
        print("⚠️ Redis still loading, extended wait...")
        r = get_redis(retry_attempts=30, retry_delay=3)
        if not wait_for_redis(r, max_retries=30, delay=10.0):
            raise RuntimeError("Redis not ready after extended wait")
        print("✅ Redis connected after extended wait")
    except Exception as e:
        print(f"❌ Failed to connect to Redis: {e}")
        raise

    # ── Parse time range ─────────────────────────────────────────────
    start_id = to_ms(args.start)
    end_id = to_ms(args.end).replace("-0", "-999999")

    # ── Stream-process and write ─────────────────────────────────────
    stream_and_write(r, start_id, end_id, args.out)


if __name__ == "__main__":
    main()
