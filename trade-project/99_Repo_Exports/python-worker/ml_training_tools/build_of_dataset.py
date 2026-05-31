from __future__ import annotations

from domain.evidence_keys import MetaKeys

"""Dataset join: OF replay rows ↔ POSITION_CLOSED by sid (fallback — approximate key) and labels.

Why:
  Outcome-labeling loop requires joining engine replay outputs with trade outcomes (r_mult)
  to build training dataset for calibration and ML meta-labeling.

Usage:
  python -m tools.build_of_dataset --replay /tmp/replay.ndjson --trades /tmp/trades.ndjson --out /tmp/dataset.ndjson --pos-th 0.5 --neg-th -0.5
"""


import argparse
import json
from typing import Any


def iter_ndjson(path: str):
    with open(path, encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if not s:
                continue
            yield json.loads(s)


def _f(x: Any, d: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return d


def _i(x: Any, d: int = 0) -> int:
    try:
        return int(x)
    except Exception:
        return d


def build_trade_index(trades_path: str) -> dict[str, dict[str, Any]]:
    """
    Index trades by sid.
    export_trade_closed_ndjson.py in your repo already flattens meta fields; sid must exist for join.
    """
    idx: dict[str, dict[str, Any]] = {}
    for r in iter_ndjson(trades_path):
        sid = (r.get("sid", "") or "")
        if sid:
            idx[sid] = r
    return idx


def extract_features(replay_row: dict[str, Any]) -> dict[str, Any]:
    ev = replay_row.get("evidence") or {}
    legs = ev.get("legs") or {}
    if not isinstance(legs, dict):
        legs = {}

    # Some fields may be nested inside evidence.score_breakdown
    sb = ev.get("score_breakdown") or {}
    if not isinstance(sb, dict):
        sb = {}

    def leg(name: str) -> int:
        v = legs.get(name, 0)
        try:
            return int(v)
        except Exception:
            return 0

    out = {
        "sid": (replay_row.get("sid", "") or ""),
        "symbol": (replay_row.get("symbol", "") or ""),
        "ts_ms": _i(replay_row.get("ts_ms", 0)),
        "direction": (replay_row.get("direction", "") or ""),
        "scenario": (replay_row.get("scenario", "") or ""),
        "ok": _i(replay_row.get("ok", 0)),
        "have": _i(replay_row.get("have", 0)),
        "need": _i(replay_row.get("need", 0)),
        # Prefer base_score if available, else score (may already include exec penalty)
        "score": _f(replay_row.get("score", 0.0)),
        "base_score": _f(sb.get("base_score", replay_row.get("score", 0.0))),
        "scenario_v4": (ev.get("scenario_v4", "") or ""),
        "need_reason": (ev.get("need_reason", "") or ""),
        "ok_soft": _i(ev.get("ok_soft", 0)),
        # execution-risk
        "exec_risk_bps": _f(ev.get("exec_risk_bps", 0.0)),
        "exec_risk_norm": _f(ev.get("exec_risk_norm", 0.0)),
        # legs (binary)
        "leg_ofi_leg": leg("ofi_leg"),
        "leg_fp_edge_absorb": leg("fp_edge_absorb"),
        "leg_obi_stable": leg("obi_stable"),
        "leg_iceberg_strict": leg("iceberg_strict"),
        "leg_abs_lvl_ok": leg("abs_lvl_ok"),
        "leg_reclaim_recent": leg("reclaim_recent"),
        "leg_weak_progress": leg("weak_progress"),
        "leg_sweep_recent": leg("sweep_recent"),
        # meta-model telemetry (if enabled in engine)
        "meta_p": _f(ev.get(MetaKeys.P, -1.0)),
        "meta_veto": _i(ev.get(MetaKeys.VETO, 0)),
    }
    return out


def extract_trade_labels(tr: dict[str, Any]) -> dict[str, Any]:
    meta = tr.get("meta") if isinstance(tr.get("meta"), dict) else {}
    return {
        "r_mult": _f(tr.get("r_mult", 0.0)),
        "pnl": _f(tr.get("pnl", 0.0)),
        "risk_usd": _f(tr.get("risk_usd", 0.0)),
        "close_reason": str(meta.get("close_reason", "") or tr.get("close_reason", "") or ""),
        "is_virtual": _i(tr.get("is_virtual", 0)),
    }


def make_label_binary(r_mult: float, *, pos_th: float, neg_th: float) -> int | None:
    """
    returns 1 for good, 0 for bad, None for ignore zone.
    """
    if r_mult >= pos_th:
        return 1
    if r_mult <= neg_th:
        return 0
    return None


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--replay", required=True, help="NDJSON from of_engine_replay_from_inputs.py (must include sid)")
    ap.add_argument("--trades", required=True, help="NDJSON from export_trade_closed_ndjson.py")
    ap.add_argument("--out", required=True, help="dataset NDJSON output")
    ap.add_argument("--pos-th", type=float, default=0.5, help="good label if r_mult >= pos_th")
    ap.add_argument("--neg-th", type=float, default=-0.5, help="bad label if r_mult <= neg_th")
    ap.add_argument("--min-n", type=int, default=200, help="fail if dataset smaller than this (quality gate)")
    ap.add_argument("--include-virtual", action="store_true", default=False,
                    help="include virtual/paper trades in dataset (default: excluded)")
    args = ap.parse_args()

    trade_idx = build_trade_index(args.trades)
    written = 0
    skipped_virtual = 0

    with open(args.out, "w", encoding="utf-8") as f:
        for rr in iter_ndjson(args.replay):
            sid = (rr.get("sid", "") or "")
            if not sid:
                continue
            tr = trade_idx.get(sid)
            if not tr:
                continue

            if not args.include_virtual and _i(tr.get("is_virtual", 0)) == 1:
                skipped_virtual += 1
                continue

            feat = extract_features(rr)
            lab = extract_trade_labels(tr)
            y = make_label_binary(lab["r_mult"], pos_th=args.pos_th, neg_th=args.neg_th)
            if y is None:
                continue

            row = {**feat, **lab, "y": int(y)}
            f.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")
            written += 1

    if skipped_virtual:
        print(f"skipped_virtual={skipped_virtual}")
    if written < args.min_n:
        raise SystemExit(f"dataset_too_small written={written} < min_n={args.min_n}")

    print(f"written={written}")


if __name__ == "__main__":
    main()

