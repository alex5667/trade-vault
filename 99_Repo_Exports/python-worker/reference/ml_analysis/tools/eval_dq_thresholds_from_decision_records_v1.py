#!/usr/bin/env python3
from __future__ import annotations
"""eval_dq_thresholds_from_decision_records_v1.py

Estimate reasonable SAFE/STRICT thresholds for DQ indicators from offline archives.

Primary target metrics (as emitted by TickProcessor into indicators):
  - tick_gap_p95_ms
  - tick_missing_seq_ema
  - book_missing_seq_ema

The script is intentionally *best-effort* and can work with multiple NDJSON layouts:
  - replay inputs archives (signals:of:inputs) where the record contains `indicators`.
  - decision records (decisions:final) if the numeric DQ fields are present.
  - stream export variants that wrap the JSON under a `payload` key (dict or JSON string).

Output:
  - JSON or YAML (picked by --out extension).

Suggested robust rule (defaults):
  SAFE:
    soft = max(p99,  median + 6*MAD)
    hard = max(p999, median + 10*MAD)
    extreme (for tick_gap_p95_ms) = max(p9999, median + 14*MAD)

  STRICT (more sensitive):
    soft = max(p95,  median + 4*MAD)
    hard = max(p99,  median + 7*MAD)
    extreme = max(p999, median + 10*MAD)

For EMA metrics (0..1), outputs are capped to [0,1].

Usage:
  python3 -m ml_analysis.tools.eval_dq_thresholds_from_decision_records_v1 \
    --in /var/lib/trade/archives/ml_replay_inputs_v1/ \
    --out /tmp/dq_thresholds.yml \
    --by-hour
"""

from utils.time_utils import get_ny_time_millis

import argparse
import gzip
import json
import math
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Optional, Tuple


def _now_ms() -> int:
    return get_ny_time_millis()


def _iter_lines(path: Path) -> Iterator[str]:
    p = str(path)
    if p.endswith(".gz"):
        with gzip.open(p, "rt", encoding="utf-8", errors="ignore") as f:
            for line in f:
                line = line.strip()
                if line:
                    yield line
    else:
        with open(p, "r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                line = line.strip()
                if line:
                    yield line


def _expand_paths(p: str) -> List[Path]:
    path = Path(p)
    if path.is_dir():
        out: List[Path] = []
        for ext in ("*.ndjson", "*.ndjson.gz", "*.jsonl", "*.jsonl.gz", "*.json", "*.json.gz"):
            out.extend(sorted(path.glob(ext)))
        return out
    return [path]


def _iter_ndjson(paths: List[Path]) -> Iterator[Dict[str, Any]]:
    for p in paths:
        for line in _iter_lines(p):
            try:
                obj = json.loads(line)
                if isinstance(obj, dict):
                    yield obj
            except Exception:
                continue


def _as_payload(obj: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize common export variants that wrap the record under `payload`."""
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


def _coerce_ts_ms(v: Any) -> Optional[int]:
    try:
        x = int(float(v))
    except Exception:
        return None
    if x <= 0:
        return None
    # seconds → ms
    if x < 10_000_000_000:  # < year ~2286 in seconds
        return x * 1000
    return x


def _get_ts_ms(rec: Dict[str, Any]) -> Optional[int]:
    # common root keys
    for k in ("ts_ms", "decision_ts_ms", "generated_at", "tick_ts_ms", "tick_ts"):
        if k in rec:
            ts = _coerce_ts_ms(rec.get(k))
            if ts is not None:
                return ts
    # nested
    inputs = rec.get("inputs")
    if isinstance(inputs, dict):
        for k in ("tick_ts_ms", "ts_ms", "tick_ts"):
            if k in inputs:
                ts = _coerce_ts_ms(inputs.get(k))
                if ts is not None:
                    return ts
    ind = rec.get("indicators")
    if isinstance(ind, dict):
        for k in ("tick_ts", "tick_ts_ms", "ts_ms"):
            if k in ind:
                ts = _coerce_ts_ms(ind.get(k))
                if ts is not None:
                    return ts
    return None


def _get_symbol(rec: Dict[str, Any]) -> str:
    for k in ("symbol",):
        v = rec.get(k)
        if v:
            return str(v).upper().strip()
    ctx = rec.get("ctx")
    if isinstance(ctx, dict) and ctx.get("symbol"):
        return str(ctx.get("symbol")).upper().strip()
    return "UNKNOWN"


def _to_float(v: Any) -> Optional[float]:
    try:
        x = float(v)
    except Exception:
        return None
    if not math.isfinite(x):
        return None
    return x


def _get_metric(rec: Dict[str, Any], key: str) -> Optional[float]:
    # direct
    if key in rec:
        return _to_float(rec.get(key))

    # common nests
    for parent_k in ("indicators", "inputs", "dq", "data_quality"):
        parent = rec.get(parent_k)
        if isinstance(parent, dict) and key in parent:
            return _to_float(parent.get(key))

    # one more level: indicators.dq.*
    ind = rec.get("indicators")
    if isinstance(ind, dict):
        dq = ind.get("dq")
        if isinstance(dq, dict) and key in dq:
            return _to_float(dq.get(key))

    return None


def _quantile_sorted(xs: List[float], q: float) -> float:
    """Linear-interpolated quantile; xs must be sorted."""
    n = len(xs)
    if n == 0:
        return float("nan")
    if n == 1:
        return float(xs[0])
    q = max(0.0, min(1.0, float(q)))
    pos = q * (n - 1)
    lo = int(math.floor(pos))
    hi = int(math.ceil(pos))
    if lo == hi:
        return float(xs[lo])
    w = pos - lo
    return float(xs[lo] * (1.0 - w) + xs[hi] * w)


def _median_sorted(xs: List[float]) -> float:
    return _quantile_sorted(xs, 0.5)


def _mad(xs_sorted: List[float], median: float) -> float:
    dev = [abs(x - median) for x in xs_sorted]
    dev.sort()
    return _median_sorted(dev)


@dataclass(frozen=True)
class Preset:
    name: str
    q_soft: float
    q_hard: float
    q_ext: float
    k_soft: float
    k_hard: float
    k_ext: float


SAFE = Preset(name="safe", q_soft=0.99, q_hard=0.999, q_ext=0.9999, k_soft=6.0, k_hard=10.0, k_ext=14.0)
STRICT = Preset(name="strict", q_soft=0.95, q_hard=0.99, q_ext=0.999, k_soft=4.0, k_hard=7.0, k_ext=10.0)


def _cap01(x: float) -> float:
    if not math.isfinite(x):
        return float("nan")
    return max(0.0, min(1.0, float(x)))


def _compute_thresholds_for_metric(values: List[float], *, preset: Preset, kind: str) -> Dict[str, Any]:
    xs = [float(x) for x in values if math.isfinite(float(x))]
    xs.sort()
    n = len(xs)
    if n == 0:
        return {"n": 0}

    med = _median_sorted(xs)
    mad = _mad(xs, med)
    mad = float(mad)

    # robust backstop: avoid zero-MAD producing identical thresholds
    if mad <= 0.0:
        mad = 0.0

    p_soft = _quantile_sorted(xs, preset.q_soft)
    p_hard = _quantile_sorted(xs, preset.q_hard)
    p_ext = _quantile_sorted(xs, preset.q_ext)

    soft = max(p_soft, med + preset.k_soft * mad)
    hard = max(p_hard, med + preset.k_hard * mad, soft)

    out: Dict[str, Any] = {
        "n": n,
        "min": float(xs[0]),
        "max": float(xs[-1]),
        "median": float(med),
        "mad": float(mad),
        "p95": float(_quantile_sorted(xs, 0.95)),
        "p99": float(_quantile_sorted(xs, 0.99)),
        "p999": float(_quantile_sorted(xs, 0.999)),
        "p9999": float(_quantile_sorted(xs, 0.9999)),
    }

    if kind == "ema":
        soft = _cap01(soft)
        hard = _cap01(hard)
        out.update({
            "soft": float(soft),
            "hard": float(hard),
            "share_above_soft": float(sum(1 for x in xs if x > soft) / n),
            "share_above_hard": float(sum(1 for x in xs if x > hard) / n),
        })
        return out

    # ms-like
    ext = max(p_ext, med + preset.k_ext * mad, hard)
    out.update({
        "soft": float(soft),
        "hard": float(hard),
        "extreme": float(ext),
        "share_above_soft": float(sum(1 for x in xs if x > soft) / n),
        "share_above_hard": float(sum(1 for x in xs if x > hard) / n),
        "share_above_extreme": float(sum(1 for x in xs if x > ext) / n),
    })
    return out


def _utc_hour_bucket(ts_ms: int) -> int:
    # Always UTC to keep deterministic across deployments.
    return int((int(ts_ms) // 1000) // 3600) % 24


def _write_out(path: str, obj: Dict[str, Any]) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    if path.endswith(".yml") or path.endswith(".yaml"):
        import yaml

        with open(path, "w", encoding="utf-8") as f:
            yaml.safe_dump(obj, f, sort_keys=False, allow_unicode=True)
        return
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2, sort_keys=False)


def main(argv: Optional[Iterable[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="Estimate DQ thresholds from NDJSON archives")
    ap.add_argument(
        "--in",
        dest="inp",
        required=True,
        nargs="+",
        help="NDJSON file(s) or directory(ies). Supports .gz.",
    )
    ap.add_argument("--out", required=True, help="Output path (.json/.yml)")
    ap.add_argument(
        "--by-hour",
        action="store_true",
        help="Also compute per UTC hour-of-day buckets (0..23).",
    )
    ap.add_argument(
        "--min-n",
        type=int,
        default=200,
        help="Minimum sample size per group to emit recommendations.",
    )
    ap.add_argument(
        "--max-records",
        type=int,
        default=0,
        help="Optional cap (0=unlimited).",
    )
    ap.add_argument(
        "--metrics",
        default="tick_gap_p95_ms,tick_missing_seq_ema,book_missing_seq_ema",
        help="Comma-separated metric keys to extract.",
    )
    args = ap.parse_args(list(argv) if argv is not None else None)

    metrics = [m.strip() for m in str(args.metrics or "").split(",") if m.strip()]
    if not metrics:
        raise SystemExit("--metrics is empty")

    metric_kind: Dict[str, str] = {}
    for m in metrics:
        metric_kind[m] = "ema" if m.endswith("_ema") else "ms"

    paths: List[Path] = []
    for p in args.inp:
        paths.extend(_expand_paths(p))
    if not paths:
        raise SystemExit("No input files found")

    # Aggregation buffers
    by_symbol: Dict[str, Dict[str, List[float]]] = {}
    by_symbol_hour: Dict[Tuple[str, int], Dict[str, List[float]]] = {}

    scanned = 0
    used = 0
    missing_all = 0

    for obj in _iter_ndjson(paths):
        scanned += 1
        if args.max_records and scanned > int(args.max_records):
            break
        rec = _as_payload(obj)

        sym = _get_symbol(rec)
        ts_ms = _get_ts_ms(rec)

        any_found = False
        vals: Dict[str, float] = {}
        for m in metrics:
            v = _get_metric(rec, m)
            if v is None:
                continue
            any_found = True
            vals[m] = float(v)
        if not any_found:
            missing_all += 1
            continue
        used += 1

        buf = by_symbol.setdefault(sym, {k: [] for k in metrics})
        for m, v in vals.items():
            buf[m].append(float(v))

        if args.by_hour and ts_ms is not None:
            hb = _utc_hour_bucket(int(ts_ms))
            hbuf = by_symbol_hour.setdefault((sym, hb), {k: [] for k in metrics})
            for m, v in vals.items():
                hbuf[m].append(float(v)),

    def _emit_group(buf: Dict[str, List[float]]) -> Dict[str, Any]:
        out: Dict[str, Any] = {},
        for m in metrics:
            vs = buf.get(m, []),
            if len(vs) < int(args.min_n):
                out[m] = {"n": len(vs), "skipped": "min_n"},
                continue
            out[m] = {
                "safe": _compute_thresholds_for_metric(vs, preset=SAFE, kind=metric_kind[m]),
                "strict": _compute_thresholds_for_metric(vs, preset=STRICT, kind=metric_kind[m]),
            },
        return out,

    out: Dict[str, Any] = {
        "version": "eval_dq_thresholds_from_decision_records_v1",
        "generated_at_ms": _now_ms(),
        "inputs": {
            "paths": [str(p) for p in paths],
            "scanned": scanned,
            "used": used,
            "missing_all_metrics": missing_all,
            "metrics": metrics,
            "min_n": int(args.min_n),
            "by_hour": bool(args.by_hour),
        },
        "by_symbol": {},
    }

    # Per symbol
    for sym, buf in sorted(by_symbol.items()):
        out["by_symbol"][sym] = _emit_group(buf)

    # Per symbol + hour
    if args.by_hour:
        by_hour_out: Dict[str, Any] = {}
        for (sym, hb), buf in sorted(by_symbol_hour.items(), key=lambda x: (x[0][0], x[0][1])):
            by_hour_out.setdefault(sym, {})[str(hb)] = _emit_group(buf)
        out["by_symbol_hour_utc"] = by_hour_out

    _write_out(str(args.out), out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
