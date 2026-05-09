from __future__ import annotations

#!/usr/bin/env python3
"""dq_threshold_eval_harness_p112.py

Goal
  Compute empirical distributions for strict-DQ runtime metrics from replay inputs.

Why
  dq_gate_v1 defaults (SAFE/STRICT) are intentionally conservative.
  This harness provides a data-driven way to pick initial thresholds:
    - tick_gap_p95_ms
    - tick_missing_seq_ema
    - book_missing_seq_ema

Input
  Either:
    A) archive directory from replay_inputs_archiver (ml_replay_inputs_v1)
    B) an exported payload NDJSON slice (each line is a payload dict)

Output
  JSON summary (stdout and/or --out-json) and optional Markdown report (--out-md).

Notes
  - All time bucketing is in UTC.
  - This tool does NOT modify any runtime config; it only reports.

Usage (archive)
  python3 orderflow_services/dq_threshold_eval_harness_p112.py \
    --archive-dir /var/lib/trade/archives/ml_replay_inputs_v1 \
    --start-ts-ms 1700000000000 --end-ts-ms 1700086400000 \
    --out-json /tmp/dq_eval.json --out-md /tmp/dq_eval.md

Usage (ndjson)
  python3 orderflow_services/dq_threshold_eval_harness_p112.py \
    --inputs /tmp/inputs.ndjson --out-json /tmp/dq_eval.json,
""",
import argparse
import gzip
import io
import json
import math
import os
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any


def _utc_hour(ts_ms: int) -> int:
    dt = datetime.fromtimestamp(ts_ms / 1000.0, tz=UTC)
    return int(dt.hour)


def _open_text(path: str) -> io.TextIOBase:
    if path.endswith(".gz"):
        return gzip.open(path, "rt", encoding="utf-8")
    return open(path, encoding="utf-8")


def _as_payload(obj: dict[str, Any]) -> dict[str, Any]:
    """Normalize record to a payload dict.

    Some archives store {payload: <dict>} or {payload: "{...}"}.
    Exported NDJSON slices typically store the payload dict directly.
    """,
    v = obj.get("payload")
    if isinstance(v, dict):
        return v
    if isinstance(v, str) and v.lstrip().startswith("{"):
        try:
            d = json.loads(v)
            return d if isinstance(d, dict) else obj
        except Exception:
            return obj
    return obj


def _pick_ts_ms(p: dict[str, Any]) -> int:
    for k in ("ts_ms", "timestamp_ms", "t_ms"):
        v = p.get(k)
        if isinstance(v, (int, float)) and v > 0:
            return int(v)
    # last fallback: sometimes close blob has close_ts_ms
    close = p.get("close")
    if isinstance(close, dict):
        v = close.get("close_ts_ms")
        if isinstance(v, (int, float)) and v > 0:
            return int(v)
    return 0


def _sym(p: dict[str, Any]) -> str:
    return (p.get("symbol") or "").upper().strip()


def _indicators(p: dict[str, Any]) -> dict[str, Any]:
    ind = p.get("indicators") or {}
    if isinstance(ind, str) and ind.strip().startswith("{"):
        try:
            d = json.loads(ind)
            return d if isinstance(d, dict) else {}
        except Exception:
            return {}
    return ind if isinstance(ind, dict) else {}


@dataclass
class HistCfg:
    min_v: float
    max_v: float
    step: float


class Hist:
    """Fixed-bin histogram with basic quantiles.

    Deterministic, small-memory, and robust to large inputs.
    """,
    def __init__(self, cfg: HistCfg):
        self.cfg = cfg
        n_bins = int(math.ceil((cfg.max_v - cfg.min_v) / cfg.step))
        self.bins: list[int] = [0] * max(1, n_bins)
        self.n: int = 0
        self.sum: float = 0.0
        self.min_seen: float | None = None
        self.max_seen: float | None = None

    def add(self, x: float) -> None:
        if not math.isfinite(x):
            return
        self.n += 1
        self.sum += float(x)
        self.min_seen = x if self.min_seen is None else min(self.min_seen, x)
        self.max_seen = x if self.max_seen is None else max(self.max_seen, x)
        # clamp into bins
        xx = min(max(x, self.cfg.min_v), self.cfg.max_v - 1e-12)
        idx = int((xx - self.cfg.min_v) / self.cfg.step)
        idx = max(0, min(idx, len(self.bins) - 1))
        self.bins[idx] += 1

    def mean(self) -> float:
        return (self.sum / self.n) if self.n else 0.0

    def q(self, q: float) -> float:
        if self.n <= 0:
            return 0.0
        q = max(0.0, min(1.0, float(q)))
        target = q * (self.n - 1)
        cum = 0
        for i, c in enumerate(self.bins):
            if c <= 0:
                continue
            prev = cum
            cum += c
            if target < cum:
                # return bin midpoint; deterministic and sufficient for thresholding
                return self.cfg.min_v + (i + 0.5) * self.cfg.step
        return float(self.cfg.max_v)

    def as_dict(self) -> dict[str, Any]:
        return {
            "n": int(self.n),
            "min": float(self.min_seen) if self.min_seen is not None else None,
            "max": float(self.max_seen) if self.max_seen is not None else None,
            "mean": float(self.mean()),
            "p50": float(self.q(0.50)),
            "p90": float(self.q(0.90)),
            "p95": float(self.q(0.95)),
            "p99": float(self.q(0.99)),
        }


@dataclass
class SymAgg:
    gap: Hist
    tick_seq: Hist
    book_seq: Hist
    n_rows: int = 0
    n_with_gap_samples: int = 0


def _iter_payloads_from_ndjson(path: str, max_records: int = 0) -> Iterator[dict[str, Any]]:
    n = 0
    with _open_text(path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except Exception:
                continue
            if not isinstance(obj, dict):
                continue
            yield _as_payload(obj)
            n += 1
            if max_records and n >= max_records:
                break


def _iter_payloads_from_archive(
    archive_dir: str,
    start_ts_ms: int,
    end_ts_ms: int,
    max_records: int = 0,
) -> Iterator[dict[str, Any]]:
    try:
        from ml_analysis.tools.replay_inputs_reader_v1 import ReplayInputsReader  # type: ignore
    except Exception as e:
        raise SystemExit(f"ReplayInputsReader import failed: {e}")

    reader = ReplayInputsReader(archive_dir=archive_dir)
    n = 0
    for rec in reader.read_records(start_ts_ms=int(start_ts_ms), end_ts_ms=int(end_ts_ms)):
        if not isinstance(rec, dict):
            continue
        yield _as_payload(rec)
        n += 1
        if max_records and n >= max_records:
            break


def _suggest_thresholds_from_p99(p99: float, default_soft: float, default_hard: float, default_extreme: float) -> dict[str, Any]:
    """Heuristic suggestions based on p99.

    Rationale
      p99 approximates the tail where risk becomes non-negligible.
      We keep defaults as a floor to avoid overfitting small samples.
    """,
    soft = max(default_soft, p99)
    hard = max(default_hard, p99 * 1.2)
    extreme = max(default_extreme, p99 * 1.5)
    return {
        "soft": float(round(soft)),
        "hard": float(round(hard)),
        "extreme": float(round(extreme)),
    }


def _write_text(path: str, content: str) -> None:
    os.makedirs(os.path.dirname(os.path.abspath(path)) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write(content)


def _render_md(report: dict[str, Any]) -> str:
    lines: list[str] = []
    lines.append("# DQ threshold eval (p112)")
    lines.append("")
    params = report.get("params", {})
    lines.append("## Params")
    lines.append("```json")
    lines.append(json.dumps(params, ensure_ascii=False, indent=2))
    lines.append("```")
    lines.append("")

    lines.append("## Global")
    lines.append("```json")
    lines.append(json.dumps(report.get("global", {}), ensure_ascii=False, indent=2))
    lines.append("```")
    lines.append("")

    lines.append("## Suggested overrides (heuristic)")
    lines.append("```json")
    lines.append(json.dumps(report.get("suggested", {}), ensure_ascii=False, indent=2))
    lines.append("```")
    lines.append("")

    by_sym = report.get("by_symbol", {})
    if isinstance(by_sym, dict) and by_sym:
        lines.append("## Per-symbol summary")
        lines.append("(p50/p90/p95/p99; n = records with the metric present)")
        lines.append("")
        lines.append("| symbol | gap_p99_ms | tick_seq_p99 | book_seq_p99 | n |")
        lines.append("|---|---:|---:|---:|---:|")
        for sym in sorted(by_sym.keys()):
            s = by_sym[sym]
            gap_p99 = (((s.get("gap") or {}).get("p99")) or 0)
            tick_p99 = (((s.get("tick_missing_seq_ema") or {}).get("p99")) or 0)
            book_p99 = (((s.get("book_missing_seq_ema") or {}).get("p99")) or 0)
            n = int((s.get("gap") or {}).get("n") or 0)
            lines.append(f"| {sym} | {gap_p99:.0f} | {tick_p99:.3f} | {book_p99:.3f} | {n} |")
        lines.append("")

    by_hour = report.get("by_hour_utc", {})
    if isinstance(by_hour, dict) and by_hour:
        lines.append("## Hourly (UTC) summary")
        lines.append("| hour | gap_p99_ms | tick_seq_p99 | book_seq_p99 | n |")
        lines.append("|---:|---:|---:|---:|---:|")
        for h in range(24):
            hh = str(h)
            s = by_hour.get(hh) or {}
            gap_p99 = (((s.get("gap") or {}).get("p99")) or 0)
            tick_p99 = (((s.get("tick_missing_seq_ema") or {}).get("p99")) or 0)
            book_p99 = (((s.get("book_missing_seq_ema") or {}).get("p99")) or 0)
            n = int((s.get("gap") or {}).get("n") or 0)
            lines.append(f"| {h:02d} | {gap_p99:.0f} | {tick_p99:.3f} | {book_p99:.3f} | {n} |")
        lines.append("")

    return "\n".join(lines) + "\n"


def main() -> None:
    ap = argparse.ArgumentParser(description="DQ distributions & threshold calibration harness (p112)")

    src = ap.add_mutually_exclusive_group(required=True)
    src.add_argument("--inputs", default="", help="payload NDJSON slice (optionally .gz)")
    src.add_argument("--archive-dir", default="", help="archive dir from replay_inputs_archiver")

    ap.add_argument("--start-ts-ms", type=int, default=0, help="archive start ts (ms, inclusive)")
    ap.add_argument("--end-ts-ms", type=int, default=0, help="archive end ts (ms, exclusive)")

    ap.add_argument("--symbol", default="", help="optional symbol filter (e.g. BTCUSDT)")
    ap.add_argument("--max-records", type=int, default=0, help="optional cap (0=unlimited)")

    ap.add_argument("--min-gap-samples", type=int, default=50, help="min tick_gap_n to treat tick_gap_p95_ms as valid")

    ap.add_argument("--gap-max-ms", type=float, default=20000.0)
    ap.add_argument("--gap-step-ms", type=float, default=250.0)
    ap.add_argument("--ema-step", type=float, default=0.01)

    ap.add_argument("--out-json", default="", help="optional output json file")
    ap.add_argument("--out-md", default="", help="optional output markdown report")

    args = ap.parse_args()

    sym_filter = str(args.symbol or "").upper().strip()
    min_gap_samples = int(args.min_gap_samples)

    gap_cfg = HistCfg(min_v=0.0, max_v=float(args.gap_max_ms), step=float(args.gap_step_ms))
    ema_cfg = HistCfg(min_v=0.0, max_v=1.0, step=float(args.ema_step))

    def _new_agg() -> SymAgg:
        return SymAgg(gap=Hist(gap_cfg), tick_seq=Hist(ema_cfg), book_seq=Hist(ema_cfg))

    by_symbol: dict[str, SymAgg] = {}
    by_hour: dict[int, SymAgg] = {h: _new_agg() for h in range(24)}
    global_agg = _new_agg()

    if args.inputs:
        it = _iter_payloads_from_ndjson(str(args.inputs), max_records=int(args.max_records))
    else:
        if not args.start_ts_ms or not args.end_ts_ms:
            raise SystemExit("--start-ts-ms and --end-ts-ms are required with --archive-dir")
        it = _iter_payloads_from_archive(
            archive_dir=str(args.archive_dir),
            start_ts_ms=int(args.start_ts_ms),
            end_ts_ms=int(args.end_ts_ms),
            max_records=int(args.max_records),
        )

    n_scanned = 0
    for p in it:
        if not isinstance(p, dict):
            continue
        n_scanned += 1
        sym = _sym(p)
        if not sym:
            continue
        if sym_filter and sym != sym_filter:
            continue
        ts_ms = _pick_ts_ms(p)
        if ts_ms <= 0:
            continue
        h = _utc_hour(ts_ms)
        ind = _indicators(p)

        gap_p95 = ind.get("tick_gap_p95_ms")
        gap_n = ind.get("tick_gap_n")
        tick_seq = ind.get("tick_missing_seq_ema")
        book_seq = ind.get("book_missing_seq_ema")

        agg = by_symbol.get(sym)
        if agg is None:
            agg = _new_agg()
            by_symbol[sym] = agg
        agg.n_rows += 1
        by_hour[h].n_rows += 1
        global_agg.n_rows += 1

        # tick_gap_p95_ms is meaningful only when the estimator has enough samples.
        if isinstance(gap_p95, (int, float)) and isinstance(gap_n, (int, float)):
            if int(gap_n) >= min_gap_samples:
                agg.gap.add(float(gap_p95))
                by_hour[h].gap.add(float(gap_p95))
                global_agg.gap.add(float(gap_p95))
                agg.n_with_gap_samples += 1
                by_hour[h].n_with_gap_samples += 1
                global_agg.n_with_gap_samples += 1

        if isinstance(tick_seq, (int, float)):
            agg.tick_seq.add(float(tick_seq))
            by_hour[h].tick_seq.add(float(tick_seq))
            global_agg.tick_seq.add(float(tick_seq))

        if isinstance(book_seq, (int, float)):
            agg.book_seq.add(float(book_seq))
            by_hour[h].book_seq.add(float(book_seq))
            global_agg.book_seq.add(float(book_seq))

    # suggestions (heuristic)
    # SAFE defaults from dq_gate_v1 (floors)
    SAFE_GAP = (5000.0, 8000.0, 12000.0)
    STRICT_GAP = (3000.0, 4500.0, 9000.0)
    SAFE_TICK_SEQ = (0.125, 0.25)
    STRICT_TICK_SEQ = (0.05, 0.15)
    SAFE_BOOK_SEQ = (0.125, 0.25)
    STRICT_BOOK_SEQ = (0.03, 0.10)

    g_gap_p99 = float(global_agg.gap.q(0.99))
    g_tick_p99 = float(global_agg.tick_seq.q(0.99))
    g_book_p99 = float(global_agg.book_seq.q(0.99))

    suggested = {
        "safe": {
            "DQ_TICK_GAP_P95_MS_SOFT": _suggest_thresholds_from_p99(g_gap_p99, *SAFE_GAP)["soft"],
            "DQ_TICK_GAP_P95_MS_HARD": _suggest_thresholds_from_p99(g_gap_p99, *SAFE_GAP)["hard"],
            "DQ_TICK_GAP_P95_MS_EXTREME": _suggest_thresholds_from_p99(g_gap_p99, *SAFE_GAP)["extreme"],
            "DQ_TICK_MISSING_SEQ_EMA_SOFT": float(max(SAFE_TICK_SEQ[0], g_tick_p99)),
            "DQ_TICK_MISSING_SEQ_EMA_HARD": float(max(SAFE_TICK_SEQ[1], g_tick_p99 * 1.2)),
            "DQ_BOOK_MISSING_SEQ_EMA_SOFT": float(max(SAFE_BOOK_SEQ[0], g_book_p99)),
            "DQ_BOOK_MISSING_SEQ_EMA_HARD": float(max(SAFE_BOOK_SEQ[1], g_book_p99 * 1.2)),
        },
        "strict": {
            "DQ_TICK_GAP_P95_MS_SOFT": _suggest_thresholds_from_p99(g_gap_p99, *STRICT_GAP)["soft"],
            "DQ_TICK_GAP_P95_MS_HARD": _suggest_thresholds_from_p99(g_gap_p99, *STRICT_GAP)["hard"],
            "DQ_TICK_GAP_P95_MS_EXTREME": _suggest_thresholds_from_p99(g_gap_p99, *STRICT_GAP)["extreme"],
            "DQ_TICK_MISSING_SEQ_EMA_SOFT": float(max(STRICT_TICK_SEQ[0], g_tick_p99)),
            "DQ_TICK_MISSING_SEQ_EMA_HARD": float(max(STRICT_TICK_SEQ[1], g_tick_p99 * 1.2)),
            "DQ_BOOK_MISSING_SEQ_EMA_SOFT": float(max(STRICT_BOOK_SEQ[0], g_book_p99)),
            "DQ_BOOK_MISSING_SEQ_EMA_HARD": float(max(STRICT_BOOK_SEQ[1], g_book_p99 * 1.2)),
        }
    }

    report: dict[str, Any] = {
        "params": {
            "inputs": str(args.inputs or ""),
            "archive_dir": str(args.archive_dir or ""),
            "start_ts_ms": int(args.start_ts_ms or 0),
            "end_ts_ms": int(args.end_ts_ms or 0),
            "symbol": sym_filter,
            "max_records": int(args.max_records or 0),
            "min_gap_samples": int(min_gap_samples),
            "gap_hist": {"max_ms": float(gap_cfg.max_v), "step_ms": float(gap_cfg.step)},
            "ema_hist": {"step": float(ema_cfg.step)},
        },
        "scanned": int(n_scanned),
        "global": {
            "gap": global_agg.gap.as_dict(),
            "tick_missing_seq_ema": global_agg.tick_seq.as_dict(),
            "book_missing_seq_ema": global_agg.book_seq.as_dict(),
            "n_rows": int(global_agg.n_rows),
            "n_with_gap_samples": int(global_agg.n_with_gap_samples),
        },
        "by_symbol": {},
        "by_hour_utc": {},
        "suggested": suggested,
    }

    for sym, agg in sorted(by_symbol.items()):
        report["by_symbol"][sym] = {
            "gap": agg.gap.as_dict(),
            "tick_missing_seq_ema": agg.tick_seq.as_dict(),
            "book_missing_seq_ema": agg.book_seq.as_dict(),
            "n_rows": int(agg.n_rows),
            "n_with_gap_samples": int(agg.n_with_gap_samples),
        }

    for h in range(24):
        agg = by_hour[h]
        report["by_hour_utc"][str(h)] = {
            "gap": agg.gap.as_dict(),
            "tick_missing_seq_ema": agg.tick_seq.as_dict(),
            "book_missing_seq_ema": agg.book_seq.as_dict(),
            "n_rows": int(agg.n_rows),
            "n_with_gap_samples": int(agg.n_with_gap_samples),
        }

    out = json.dumps(report, ensure_ascii=False, indent=2)
    print(out)

    if args.out_json:
        _write_text(str(args.out_json), out + "\n")

    if args.out_md:
        _write_text(str(args.out_md), _render_md(report))


if __name__ == "__main__":
    main()
