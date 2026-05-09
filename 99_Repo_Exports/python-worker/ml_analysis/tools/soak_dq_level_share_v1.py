#!/usr/bin/env python3
from __future__ import annotations

"""Soak helper: summarize dq_level==2 share and key DQ inputs per symbol.

Intended usage:
  - Run capture (B6/B7) for 24h, producing NDJSON (one JSON per line).
  - Feed the NDJSON into this script to get per-symbol health stats.

Design goals:
  - deterministic parsing (no external deps)
  - tolerant to several record layouts (top-level or nested)
  - avoids high memory usage via bounded per-symbol sampling
"""


import argparse
import json
import math
import sys
from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass
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
    return {}


def _extract_dq_level(rec: dict[str, Any], ind: dict[str, Any]) -> int:
    for path in (
        ("dq_level",),
        ("dq", "dq_level"),
        ("decision", "dq_level"),
        ("decision", "dq", "dq_level"),
    ):
        v = _get_nested(rec, path)
        if v is not None:
            return _i(v, 0)
    if "dq_level" in ind:
        return _i(ind.get("dq_level"), 0)
    return 0


@dataclass
class SymStats:
    n: int = 0
    dq2: int = 0
    # bounded samples for quantiles
    book_ema: list[float] = None  # type: ignore
    tick_ema: list[float] = None  # type: ignore
    gap_p95: list[float] = None  # type: ignore

    def __post_init__(self) -> None:
        self.book_ema = []
        self.tick_ema = []
        self.gap_p95 = []


def _maybe_sample(arr: list[float], x: float, max_points: int) -> None:
    if max_points <= 0:
        return
    if len(arr) < max_points:
        arr.append(float(x))
        return
    # Deterministic downsample: keep every k-th point by overwriting in a ring.
    idx = len(arr) % max_points
    arr[idx] = float(x)


def _q(arr: list[float], q: float) -> float:
    if not arr:
        return float("nan")
    a = sorted(arr)
    i = int(round((len(a) - 1) * q))
    i = max(0, min(len(a) - 1, i))
    return float(a[i])


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

            # DQ inputs (pull from indicators when available)
            b = _f(ind.get("book_missing_seq_ema"), float("nan"))
            t = _f(ind.get("tick_missing_seq_ema"), float("nan"))
            g = _f(ind.get("tick_gap_p95_ms"), float("nan"))
            if math.isfinite(b):
                _maybe_sample(s.book_ema, b, args.max_points)
            if math.isfinite(t):
                _maybe_sample(s.tick_ema, t, args.max_points)
            if math.isfinite(g):
                _maybe_sample(s.gap_p95, g, args.max_points)

    # Output
    rows: list[tuple[str, SymStats]] = sorted(stats.items(), key=lambda kv: (-kv[1].dq2 / max(1, kv[1].n), kv[0]))

    print("symbol\tn\tdq2\tshare\tbook_ema_p50\tbook_ema_p90\tbook_ema_p99\ttick_ema_p90\tgap_p95_p90")
    for sym, s in rows:
        share = s.dq2 / max(1, s.n)
        print(
            f"{sym}\t{s.n}\t{s.dq2}\t{share:.4f}\t"
            f"{_q(s.book_ema, 0.50):.6g}\t{_q(s.book_ema, 0.90):.6g}\t{_q(s.book_ema, 0.99):.6g}\t"
            f"{_q(s.tick_ema, 0.90):.6g}\t{_q(s.gap_p95, 0.90):.6g}"
        )

    print("\nNotes:")
    print("  - share is dq_level==2 share over all captured rows.")
    print("  - Use this to calibrate SAFE/STRICT thresholds and EMA alpha.")
    print("  - For book EMA alpha guidance, pick alpha according to stream frequency (Hz):")
    print("      10Hz -> 0.05..0.10, 4Hz -> 0.20, 2Hz -> 0.30, 1Hz -> 0.30..0.50")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
