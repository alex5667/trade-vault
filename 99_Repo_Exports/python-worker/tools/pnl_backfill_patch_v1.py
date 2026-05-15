#!/usr/bin/env python3
"""
pnl_backfill_patch_v1.py  — PNL double-add backfill (bug 2026-05-14)

Scans `trades:closed` stream, identifies INITIAL_SL trades where
|pnl_gross| > 1.5 × (lot × |entry_price − sl|), and writes corrected
pnl_gross / pnl_net values to the `trades:pnl:corrections` Redis Hash.

The hash is keyed by order_id. `periodic_reporter.py` loads this overlay
at report time and applies it before accumulating metrics — so the PRIMARY
report numbers (not just the "🔧 Corrected view") reflect honest values.

Original stream entries are NEVER modified (audit preservation).

Usage (inside python-worker container):
    python -m tools.pnl_backfill_patch_v1               # dry-run (default)
    python -m tools.pnl_backfill_patch_v1 --apply        # write corrections
    python -m tools.pnl_backfill_patch_v1 --apply --hours 72   # last 72 h

    python -m tools.pnl_backfill_patch_v1 --report       # show correction summary

ENV:
    REDIS_URL          — worker-1 URL (default: redis://redis-worker-1:6379/0)
    PNL_CORR_KEY       — override correction hash key (default: trades:pnl:corrections)
    PNL_OVERSHOOT_RATIO — trigger ratio |pnl_gross| / theoretical_loss (default 1.5)
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from typing import Any

# Allow running from outside docker: cd python-worker && python -m tools.pnl_backfill_patch_v1
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import redis as redis_lib
from core.redis_keys import RedisStreams as RS

_CORRECTIONS_KEY = os.getenv("PNL_CORR_KEY", "trades:pnl:corrections")
_OVERSHOOT_RATIO = float(os.getenv("PNL_OVERSHOOT_RATIO", "1.5"))
_MIN_THEORETICAL_LOSS = 1.0  # ignore dust trades (< $1 risk)
_CORRECTION_REASON = "double_add_bug_2026-05-14"

STREAM_BATCH = 2000
MAX_ENTRIES = 500_000


def _sf(v: Any, d: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return d


def _get_redis(url: str) -> redis_lib.Redis:
    return redis_lib.from_url(
        url,
        decode_responses=True,
        socket_timeout=10,
        socket_connect_timeout=5,
    )


def _hydrate_from_order_hash(r: redis_lib.Redis, fields: dict[str, str]) -> dict[str, str]:
    """If compact stream (missing pnl_gross/lot/entry_price/sl), fetch from order:{id} hash."""
    oid = fields.get("order_id") or ""
    if not oid:
        return fields
    missing = not fields.get("pnl_gross") or not fields.get("lot") or not fields.get("entry_price")
    if not missing:
        return fields
    try:
        h: dict[str, str] = r.hgetall(f"order:{oid}") or {}  # type: ignore[assignment]
        if h:
            merged: dict[str, str] = {**h, **fields}  # stream fields take precedence
            return merged
    except Exception:
        pass
    return fields


def _compute_correction(fields: dict[str, str]) -> dict[str, Any] | None:
    """
    Returns correction dict if the entry needs patching, else None.
    """
    close_reason = (fields.get("close_reason") or fields.get("close_reason_raw") or "").upper().strip()
    if "INITIAL_SL" not in close_reason and close_reason != "SL":
        return None

    lot = _sf(fields.get("lot"))
    entry_price = _sf(fields.get("entry_price") or fields.get("avg_entry_price"))
    sl = _sf(fields.get("sl") or fields.get("sl_price") or fields.get("stop_loss"))
    pnl_gross = _sf(fields.get("pnl_gross"))
    pnl_net = _sf(fields.get("pnl_net") or fields.get("pnl"))
    fees = _sf(fields.get("fees") or fields.get("fees_usd"))

    if lot <= 0 or entry_price <= 0 or sl <= 0:
        return None

    theoretical_loss = lot * abs(entry_price - sl)
    if theoretical_loss < _MIN_THEORETICAL_LOSS:
        return None

    if abs(pnl_gross) <= _OVERSHOOT_RATIO * theoretical_loss:
        return None  # not overshooting

    corrected_pnl_gross = -theoretical_loss
    # Use stored fees; if fees=0, estimate from pnl_net-pnl_gross difference
    if fees <= 0:
        fees = abs(pnl_gross - pnl_net) if abs(pnl_gross - pnl_net) < theoretical_loss else 0.0
    corrected_pnl_net = corrected_pnl_gross - fees

    return {
        "pnl_gross": corrected_pnl_gross,
        "pnl_net": corrected_pnl_net,
        "original_pnl_gross": pnl_gross,
        "original_pnl_net": pnl_net,
        "theoretical_loss": theoretical_loss,
        "lot": lot,
        "entry_price": entry_price,
        "sl": sl,
        "fees": fees,
        "correction_reason": _CORRECTION_REASON,
        "correction_ts": int(time.time() * 1000),
        "order_id": fields.get("order_id") or "",
        "symbol": fields.get("symbol") or "",
        "stream_key": RS.TRADES_CLOSED,
    }


def scan_stream(
    r: redis_lib.Redis,
    *,
    since_ms: int = 0,
    dry_run: bool = True,
    verbose: bool = False,
) -> tuple[int, int, int]:
    """
    Scan trades:closed and write corrections.

    Returns: (scanned, corrected, skipped_no_fields)
    """
    scanned = 0
    corrected = 0
    skipped = 0
    last_id = "+"
    min_id = f"{since_ms}-0" if since_ms > 0 else "-"

    print(f"{'[DRY RUN] ' if dry_run else ''}Scanning {RS.TRADES_CLOSED} "
          f"{'from ' + str(since_ms) + ' ms' if since_ms else '(all history)'}...")

    pipe_batch: list[tuple[str, dict[str, Any]]] = []

    while scanned < MAX_ENTRIES:
        batch: list = r.xrevrange(RS.TRADES_CLOSED, max=last_id, min=min_id, count=STREAM_BATCH)  # type: ignore[assignment]
        if not batch:
            break

        for msg_id, fields in batch:
            if msg_id == last_id:
                continue
            last_id = msg_id
            scanned += 1

            # hydrate if compact
            fields = _hydrate_from_order_hash(r, fields)

            oid = fields.get("order_id") or ""
            if not oid:
                skipped += 1
                continue

            corr = _compute_correction(fields)
            if corr is None:
                continue

            corrected += 1
            if verbose:
                print(
                    f"  [{msg_id}] {corr['symbol']} order={oid} "
                    f"pnl_gross {corr['original_pnl_gross']:+.2f} → {corr['pnl_gross']:+.2f} "
                    f"(theory={corr['theoretical_loss']:.2f})"
                )

            if not dry_run:
                pipe_batch.append((oid, corr))

        # flush write batch
        if not dry_run and pipe_batch:
            pipe = r.pipeline(transaction=False)
            for oid, corr in pipe_batch:
                pipe.hset(_CORRECTIONS_KEY, oid, json.dumps(corr))
            pipe.execute()
            pipe_batch.clear()

        # stop if we've gone past the time window
        if since_ms > 0 and batch:
            last_entry_id = batch[-1][0]
            last_ts = int(str(last_entry_id).split("-")[0])
            if last_ts < since_ms:
                break

    # flush remaining
    if not dry_run and pipe_batch:
        pipe = r.pipeline(transaction=False)
        for oid, corr in pipe_batch:
            pipe.hset(_CORRECTIONS_KEY, oid, json.dumps(corr))
        pipe.execute()

    return scanned, corrected, skipped


def report_corrections(r: redis_lib.Redis) -> None:
    """Show summary of stored corrections."""
    all_corr: dict[str, str] = r.hgetall(_CORRECTIONS_KEY) or {}  # type: ignore[assignment]
    if not all_corr:
        print("No corrections stored in", _CORRECTIONS_KEY)
        return

    total_pnl_gross_raw = 0.0
    total_pnl_gross_corr = 0.0
    total_pnl_net_raw = 0.0
    total_pnl_net_corr = 0.0
    by_symbol: dict[str, int] = {}

    for oid, v in all_corr.items():
        try:
            c = json.loads(v)
        except Exception:
            continue
        total_pnl_gross_raw += c.get("original_pnl_gross", 0.0)
        total_pnl_gross_corr += c.get("pnl_gross", 0.0)
        total_pnl_net_raw += c.get("original_pnl_net", 0.0)
        total_pnl_net_corr += c.get("pnl_net", 0.0)
        sym = c.get("symbol", "?")
        by_symbol[sym] = by_symbol.get(sym, 0) + 1

    print(f"\n=== trades:pnl:corrections  ({len(all_corr)} entries) ===")
    print(f"  P/L gross  RAW:  {total_pnl_gross_raw:+.2f}$   CORRECTED: {total_pnl_gross_corr:+.2f}$  "
          f"(Δ {total_pnl_gross_corr - total_pnl_gross_raw:+.2f}$)")
    print(f"  P/L net    RAW:  {total_pnl_net_raw:+.2f}$   CORRECTED: {total_pnl_net_corr:+.2f}$  "
          f"(Δ {total_pnl_net_corr - total_pnl_net_raw:+.2f}$)")
    print(f"  By symbol: {dict(sorted(by_symbol.items(), key=lambda x: -x[1]))}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="PNL double-add backfill patch (bug 2026-05-14)")
    parser.add_argument("--apply", action="store_true", help="Write corrections to Redis (default: dry-run)")
    parser.add_argument("--report", action="store_true", help="Show stored corrections summary")
    parser.add_argument("--hours", type=float, default=0, help="Scan only last N hours (default: all)")
    parser.add_argument("--verbose", "-v", action="store_true", help="Print each corrected trade")
    parser.add_argument(
        "--redis-url",
        default=os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0"),
        help="Redis URL (default: REDIS_URL env or redis://redis-worker-1:6379/0)",
    )
    args = parser.parse_args(argv)

    try:
        r = _get_redis(args.redis_url)
        r.ping()
        print(f"Connected: {args.redis_url}")
    except Exception as e:
        print(f"ERROR: cannot connect to Redis: {e}", file=sys.stderr)
        return 1

    if args.report:
        report_corrections(r)
        return 0

    since_ms = int((time.time() - args.hours * 3600) * 1000) if args.hours > 0 else 0
    dry_run = not args.apply

    scanned, corrected, skipped = scan_stream(
        r, since_ms=since_ms, dry_run=dry_run, verbose=args.verbose
    )

    action = "would patch" if dry_run else "patched"
    print(
        f"\nDone. Scanned={scanned}  {action}={corrected}  skipped_no_id={skipped}"
    )
    if dry_run and corrected > 0:
        print(f"Run with --apply to write corrections to '{_CORRECTIONS_KEY}'")

    if args.apply:
        report_corrections(r)

    return 0


if __name__ == "__main__":
    sys.exit(main())
