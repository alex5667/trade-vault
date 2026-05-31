#!/usr/bin/env python3
from __future__ import annotations

"""Soak helper: summarize dq_level==2 share and key DQ inputs per symbol.

Intended usage:
  - Run capture (B6/B7) for 24h, producing NDJSON (one JSON per line).
  - Feed the NDJSON into this script to get per-symbol health stats.

Design goals:
  - deterministic parsing (no external deps)
  - tolerant to several record layouts (top-level / nested / decision_-prefixed)
  - avoids high memory usage via bounded per-symbol sampling (true ring buffer)
"""


import argparse
import json
import math
import sys
from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


def _f(x: Any, d: float = 0.0) -> float:
    try:
        v = float(x)
        return v if math.isfinite(v) else d
    except Exception:
        return d


def _i(x: Any, d: int = 0) -> int:
    try:
        return int(x)
    except Exception:
        return d


def _get_nested(d: dict[str, Any], keys: Iterable[str]) -> Any:
    cur: Any = d
    for k in keys:
        if not isinstance(cur, dict):
            return None
        if k not in cur:
            return None
        cur = cur.get(k)
    return cur


def _extract_symbol(rec: dict[str, Any]) -> str:
    for path in (
        ("symbol",),
        ("meta", "symbol"),
        ("decision", "symbol"),
        ("dr", "symbol"),
    ):
        v = _get_nested(rec, path)
        if isinstance(v, str) and v:
            return v
    return "UNKNOWN"


def _extract_indicators(rec: dict[str, Any]) -> dict[str, Any]:
    v = rec.get("indicators")
    if isinstance(v, dict):
        return v
    v = _get_nested(rec, ("decision", "indicators"))
    if isinstance(v, dict):
        return v
    v = _get_nested(rec, ("dr", "indicators"))
    if isinstance(v, dict):
        return v
    v = rec.get("features")
    if isinstance(v, dict):
        return v
    v = _get_nested(rec, ("dr", "features"))
    if isinstance(v, dict):
        return v
    return {}


def _extract_dq_level(rec: dict[str, Any], ind: dict[str, Any]) -> int:
    # Top-level / nested decision-record locations (existing layouts)
    for path in (
        ("dq_level",),
        ("decision_dq_level",),
        ("dq", "dq_level"),
        ("dq_state", "dq_level"),
        ("decision", "dq_level"),
        ("decision", "dq", "dq_level"),
        ("dr", "dq_level"),
    ):
        v = _get_nested(rec, path)
        if v is not None:
            return _i(v, 0)
    if "dq_level" in ind:
        return _i(ind.get("dq_level"), 0)
    return 0


def _extract_dq_value(rec: dict[str, Any], ind: dict[str, Any], canonical: str) -> float:
    """Pull a DQ EMA/gap value from indicators, top-level decision_*-prefixed,
    or `dq_state` dict. Returns NaN when unavailable."""
    v = ind.get(canonical)
    if v is not None:
        return _f(v, float("nan"))
    v = rec.get(f"decision_{canonical}")
    if v is not None:
        return _f(v, float("nan"))
    v = rec.get(canonical)
    if v is not None:
        return _f(v, float("nan"))
    v = _get_nested(rec, ("dq_state", canonical))
    if v is not None:
        return _f(v, float("nan"))
    return float("nan")


@dataclass
class SymStats:
    n: int = 0
    dq2: int = 0
    n_book: int = 0  # rows with finite book_ema sample
    n_tick: int = 0
    n_gap: int = 0
    # bounded samples for quantiles (true ring buffer with per-buffer index)
    book_ema: list[float] = field(default_factory=list)
    tick_ema: list[float] = field(default_factory=list)
    gap_p95: list[float] = field(default_factory=list)
    _book_idx: int = 0
    _tick_idx: int = 0
    _gap_idx: int = 0


def _ring_append(arr: list[float], idx_attr: str, stats: SymStats, value: float, max_points: int) -> None:
    """Append into a bounded ring buffer that overwrites all slots evenly."""
    if max_points <= 0:
        return
    if len(arr) < max_points:
        arr.append(float(value))
        return
    idx = int(getattr(stats, idx_attr, 0)) % max_points
    arr[idx] = float(value)
    setattr(stats, idx_attr, (idx + 1) % max_points)


def _q(arr: list[float], q: float) -> float:
    if not arr:
        return float("nan")
    a = sorted(arr)
    i = int(round((len(a) - 1) * q))
    i = max(0, min(len(a) - 1, i))
    return float(a[i])


def _fmt_q(arr: list[float], q: float) -> str:
    """Format a quantile: '-' when no samples, else 6-significant-digit float."""
    if not arr:
        return "-"
    return f"{_q(arr, q):.6g}"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("ndjson", type=str, help="Path to capture NDJSON file")
    ap.add_argument("--max-points", type=int, default=20000, help="Max samples per symbol for quantiles")
    args = ap.parse_args()

    path = Path(args.ndjson)
    if not path.exists():
        print(f"Not found: {path}", file=sys.stderr)
        return 2

    stats: dict[str, SymStats] = defaultdict(SymStats)

    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except Exception:
                continue
            if not isinstance(rec, dict):
                continue

            sym = _extract_symbol(rec)
            ind = _extract_indicators(rec)
            dq_level = _extract_dq_level(rec, ind)

            s = stats[sym]
            s.n += 1
            if dq_level == 2:
                s.dq2 += 1

            # DQ inputs (multiple fallback locations)
            b = _extract_dq_value(rec, ind, "book_missing_seq_ema")
            t = _extract_dq_value(rec, ind, "tick_missing_seq_ema")
            g = _extract_dq_value(rec, ind, "tick_gap_p95_ms")
            if math.isfinite(b):
                s.n_book += 1
                _ring_append(s.book_ema, "_book_idx", s, b, args.max_points)
            if math.isfinite(t):
                s.n_tick += 1
                _ring_append(s.tick_ema, "_tick_idx", s, t, args.max_points)
            if math.isfinite(g):
                s.n_gap += 1
                _ring_append(s.gap_p95, "_gap_idx", s, g, args.max_points)

    # Output
    rows: list[tuple[str, SymStats]] = sorted(stats.items(), key=lambda kv: (-kv[1].dq2 / max(1, kv[1].n), kv[0]))

    print(
        "symbol\tn\tdq2\tshare\t"
        "book_n\tbook_ema_p50\tbook_ema_p90\tbook_ema_p99\t"
        "tick_n\ttick_ema_p90\t"
        "gap_n\tgap_p95_p90"
    )
    for sym, s in rows:
        share = s.dq2 / max(1, s.n)
        print(
            f"{sym}\t{s.n}\t{s.dq2}\t{share:.4f}\t"
            f"{s.n_book}\t{_fmt_q(s.book_ema, 0.50)}\t{_fmt_q(s.book_ema, 0.90)}\t{_fmt_q(s.book_ema, 0.99)}\t"
            f"{s.n_tick}\t{_fmt_q(s.tick_ema, 0.90)}\t"
            f"{s.n_gap}\t{_fmt_q(s.gap_p95, 0.90)}"
        )

    # Diagnostic: warn when DQ inputs are uniformly absent — most likely the
    # capture pipeline isn't emitting `book_missing_seq_ema` / `tick_*` keys.
    total_n = sum(s.n for s in stats.values())
    total_book = sum(s.n_book for s in stats.values())
    total_tick = sum(s.n_tick for s in stats.values())
    total_gap = sum(s.n_gap for s in stats.values())
    if total_n > 0 and (total_book + total_tick + total_gap) == 0:
        print(
            "\nWARNING: 0/{n} rows had book_missing_seq_ema / tick_missing_seq_ema / "
            "tick_gap_p95_ms — the capture source likely strips nested `indicators` "
            "and the decision-record enrichment isn't writing `decision_*` DQ fields. "
            "See services/orderflow/decision_ctx_fields.ensure_decision_ctx_fields.".format(n=total_n)
        )

    print("\nNotes:")
    print("  - share is dq_level==2 share over all captured rows.")
    print("  - book_n / tick_n / gap_n: rows where the field was present and finite.")
    print("  - Use this to calibrate SAFE/STRICT thresholds and EMA alpha.")
    print("  - For book EMA alpha guidance, pick alpha according to stream frequency (Hz):")
    print("      10Hz -> 0.05..0.10, 4Hz -> 0.20, 2Hz -> 0.30, 1Hz -> 0.30..0.50")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
