from __future__ import annotations

"""Confidence parts drift report (world practice).

Reads JSONL dataset rows (typically produced by build_edge_stack_dataset_from_redis) where:
  row['indicators']['confidence_parts'] is a dict[str, float]
  row may include row['symbol'] and row['indicators']['regime_class']/['market_mode']

Outputs a compact JSON report with robust drift Z (median/MAD) for each part key.
"""


import argparse
import json
import math
import os
from collections import defaultdict
from collections.abc import Iterable
from datetime import UTC, datetime
from typing import Any


def _as_int(x: Any, default: int = 0) -> int:
    try:
        if x is None:
            return default
        if isinstance(x, bool):
            return default
        if isinstance(x, (int, float)):
            return int(x)
        return int(float(str(x).strip()))
    except Exception:
        return default


def _as_str(x: Any) -> str:
    if x is None:
        return ""
    if isinstance(x, bytes):
        try:
            return x.decode("utf-8", "ignore")
        except Exception:
            return ""
    return str(x)


def _utc_day_from_ts_ms(ts_ms: int) -> str:
    if ts_ms <= 0:
        return ""
    dt = datetime.fromtimestamp(ts_ms / 1000.0, tz=UTC)
    return dt.date().isoformat()


def _median(xs: list[float]) -> float:
    if not xs:
        return float("nan")
    xs = sorted(xs)
    n = len(xs)
    mid = n // 2
    if n % 2 == 1:
        return float(xs[mid])
    return 0.5 * (float(xs[mid - 1]) + float(xs[mid]))


def _mad(xs: list[float], med: float) -> float:
    if not xs:
        return float("nan")
    # Handle NaN in median if it happened
    if not math.isfinite(med):
         return float("nan")

    dev = [abs(float(x) - med) for x in xs]
    return _median(dev)


def robust_median_mad(xs: list[float]) -> tuple[float, float]:
    """Returns (median, MAD)."""
    m = _median(xs)
    d = _mad(xs, m)
    return m, d


def drift_z(base: list[float], target: list[float], eps: float = 1e-9) -> float:
    """Robust drift Z using baseline median and MAD (scaled to sigma)."""
    if not base or not target:
        return float("nan")
    base_med, base_mad = robust_median_mad(base)
    tgt_med = _median(target)

    if not (math.isfinite(base_med) and math.isfinite(base_mad) and math.isfinite(tgt_med)):
        return float("nan")

    sigma = 1.4826 * float(base_mad)
    denom = sigma if sigma > eps else eps
    return (float(tgt_med) - float(base_med)) / denom


def _read_jsonl(path: str) -> Iterable[dict[str, Any]]:
    with open(path, encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if not s:
                continue
            try:
                obj = json.loads(s)
                if isinstance(obj, dict):
                    yield obj
            except Exception:
                continue


def _get_group_key(row: dict[str, Any], group_by: str) -> tuple[str, ...]:
    symbol = _as_str(row.get("symbol")).upper()
    ind = row.get("indicators") if isinstance(row.get("indicators"), dict) else {}
    regime = _as_str(ind.get("regime_class") or ind.get("market_mode") or "").lower()
    if group_by == "global":
        return ("GLOBAL",)
    if group_by == "symbol":
        return (symbol or "UNKNOWN",)
    if group_by == "symbol_regime":
        return (symbol or "UNKNOWN", regime or "unknown")
    return ("GLOBAL",)


def build_report(
    rows: Iterable[dict[str, Any]],
    *,
    group_by: str = "symbol_regime",
    baseline_days: int = 7,
    target_day: str | None = None,
    top_n: int = 50,
) -> dict[str, Any]:
    # Collect part values by (group, day, key)
    data: defaultdict[tuple[str, ...], defaultdict[str, defaultdict[str, list[float]]]] = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))

    max_day = ""
    for row in rows:
        ts_ms = _as_int(row.get("ts_ms") or row.get("close_ts_ms") or 0, 0)
        day = _utc_day_from_ts_ms(ts_ms)
        if not day:
            continue
        if day > max_day:
            max_day = day

        ind = row.get("indicators") if isinstance(row.get("indicators"), dict) else {}
        parts = ind.get("confidence_parts")
        if not isinstance(parts, dict) or not parts:
            parts = ind.get("confidence_breakdown")
        if not isinstance(parts, dict) or not parts:
            continue

        gk = _get_group_key(row, group_by)
        for k, v in parts.items():
            if not isinstance(k, str):
                continue
            if isinstance(v, bool):
                continue
            if isinstance(v, (int, float)) and not isinstance(v, bool) and math.isfinite(float(v)):
                data[gk][day][k].append(float(v))

    if target_day is None:
        target_day = max_day

    # Determine baseline range
    # We keep it simple: baseline is the previous N distinct days < target_day.
    all_days = sorted({d for g in data.values() for d in g.keys()})
    base_days = [d for d in all_days if d < target_day][-max(0, int(baseline_days)):]
    base_set = set(base_days)

    out_groups: list[dict[str, Any]] = []
    # Sort groups for deterministic output
    for gk, by_day in sorted(data.items(), key=lambda kv: kv[0]):
        tgt = by_day.get(target_day, {})
        if not tgt:
            continue

        # Build baseline pool per key
        baseline_pool: defaultdict[str, list[float]] = defaultdict(list)
        for d, per_key in by_day.items():
            if d in base_set:
                for k, vs in per_key.items():
                    baseline_pool[k].extend(vs)

        parts_rep = []
        for k, tgt_vs in tgt.items():
            base_vs = baseline_pool.get(k, [])
            # Simple threshold for robust stats: need enough samples
            if len(base_vs) < 50 or len(tgt_vs) < 20:
                # Not enough data for robust drift; still report counts
                z = float("nan")
                base_med, base_mad = robust_median_mad(base_vs) if base_vs else (float("nan"), float("nan"))
                tgt_med = _median(tgt_vs) if tgt_vs else float("nan")
            else:
                z = drift_z(base_vs, tgt_vs)
                base_med, base_mad = robust_median_mad(base_vs)
                tgt_med = _median(tgt_vs)

            parts_rep.append({
                "key": k,
                "n_base": int(len(base_vs)),
                "n_target": int(len(tgt_vs)),
                "baseline_median": base_med,
                "baseline_mad": base_mad,
                "target_median": tgt_med,
                "drift_z": z,
            })

        # order by abs drift_z, then by n_target
        parts_rep.sort(key=lambda r: (-(abs(r["drift_z"]) if math.isfinite(r["drift_z"]) else -1.0), -r["n_target"]))
        if top_n > 0:
            parts_rep = parts_rep[: int(top_n)]

        out_groups.append({
            "group": list(gk),
            "target_day": target_day,
            "baseline_days": list(base_days),
            "parts": parts_rep,
        })

    return {
        "target_day": target_day,
        "baseline_days": list(base_days),
        "group_by": group_by,
        "groups": out_groups,
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--in_jsonl", required=True, help="input JSONL dataset (rows with indicators.confidence_parts)")
    ap.add_argument("--out_json", required=True, help="output JSON report path")
    ap.add_argument("--group_by", default=os.getenv("CONF_PARTS_DRIFT_GROUP_BY", "symbol_regime"), choices=["global", "symbol", "symbol_regime"])
    ap.add_argument("--baseline_days", type=int, default=int(os.getenv("CONF_PARTS_DRIFT_BASELINE_DAYS", "7") or 7))
    ap.add_argument("--target_day", default=os.getenv("CONF_PARTS_DRIFT_TARGET_DAY", "") or None, help="YYYY-MM-DD; default = last day in data")
    ap.add_argument("--top_n", type=int, default=int(os.getenv("CONF_PARTS_DRIFT_TOP_N", "50") or 50))
    args = ap.parse_args()

    # Streaming read since datasets can be large
    try:
        rep = build_report(
            _read_jsonl(args.in_jsonl),
            group_by=args.group_by,
            baseline_days=args.baseline_days,
            target_day=args.target_day,
            top_n=args.top_n,
        )
        with open(args.out_json, "w", encoding="utf-8") as f:
            json.dump(rep, f, ensure_ascii=False, indent=2, sort_keys=True)
    except Exception as e:
        print(f"Error: {e}")
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
