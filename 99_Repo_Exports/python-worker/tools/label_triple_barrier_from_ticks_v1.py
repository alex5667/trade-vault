from __future__ import annotations

import argparse
import json
import os
from collections.abc import Iterable
from typing import Any

from core.triple_barrier import BarrierSpec, label_path, pick_entry_price


def _read_ndjson(path: str) -> Iterable[dict[str, Any]]:
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            yield json.loads(line)


def _safe_json(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"))


def _i(x: Any, d: int = 0) -> int:
    try:
        return int(float(x))
    except Exception:
        return d


def _f(x: Any, d: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return d


def group_ticks(ticks: Iterable[dict[str, Any]]) -> dict[str, list[tuple[int, float]]]:
    out: dict[str, list[tuple[int, float]]] = {}
    for t in ticks:
        sym = (t.get("symbol", "") or "").upper()
        ts = _i(t.get("ts_ms", 0), 0)
        px = _f(t.get("price", 0.0), 0.0)
        if not sym or ts <= 0 or px <= 0:
            continue
        out.setdefault(sym, []).append((ts, px))
    for sym in list(out.keys()):
        out[sym].sort(key=lambda x: x[0])
    return out


def slice_path(series: list[tuple[int, float]], ts0: int, ts1: int) -> list[tuple[int, float]]:
    return [(ts, px) for ts, px in series if ts0 <= ts <= ts1]


def infer_tp_sl_bps(indicators: dict[str, Any], *, tp_k_atr: float, sl_k_atr: float, fallback_tp_bps: float, fallback_sl_bps: float) -> tuple[float, float, float]:
    stop_bps = _f(indicators.get("stop_bps", 0.0), 0.0)
    atr_bps = _f(indicators.get("atr_bps", 0.0), 0.0)
    if stop_bps > 1e-6:
        return tp_k_atr * stop_bps, sl_k_atr * stop_bps, stop_bps
    if atr_bps > 1e-6:
        return tp_k_atr * atr_bps, sl_k_atr * atr_bps, atr_bps
    return fallback_tp_bps, fallback_sl_bps, 0.0


def infer_cost_bps(indicators: dict[str, Any], *, fees_bps_one_side: float, fallback_cost_bps: float) -> float:
    """Round-trip execution cost in bps: spread + 2·fees + slippage_estimate.

    All components sourced from `indicators` (per-signal snapshot in signals:of:inputs).
    Fail-open: if both spread_bps and slippage are missing, returns fallback_cost_bps
    (default 0.0 → preserves backward-compat labels when no cost data is present).
    """
    spread_bps = _f(indicators.get("spread_bps", 0.0), 0.0)
    # Slippage key name varies across producers — accept several common aliases.
    slip = _f(
        indicators.get("expected_slippage_bps",
                       indicators.get("max_expected_slippage_bps_eff",
                                      indicators.get("slippage_bps_est", 0.0))),
        0.0,
    )
    if spread_bps <= 0.0 and slip <= 0.0:
        return fallback_cost_bps
    return spread_bps + 2.0 * fees_bps_one_side + slip


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--inputs", required=True)
    ap.add_argument("--ticks", required=True)
    ap.add_argument("--out", required=True)

    ap.add_argument("--h-ms", type=int, default=int(os.getenv("TB_H_MS", "180000") or 180000))
    ap.add_argument("--tp-k-atr", type=float, default=float(os.getenv("TB_TP_K_ATR", "1.0") or 1.0))
    ap.add_argument("--sl-k-atr", type=float, default=float(os.getenv("TB_SL_K_ATR", "1.0") or 1.0))
    ap.add_argument("--fallback-tp-bps", type=float, default=float(os.getenv("TB_FALLBACK_TP_BPS", "30") or 30))
    ap.add_argument("--fallback-sl-bps", type=float, default=float(os.getenv("TB_FALLBACK_SL_BPS", "30") or 30))

    # v14_of cost-aware labels — backward-compat default 0.0 (cost_bps=0 ⇒ identical to legacy behavior).
    # Set TB_FEES_BPS_ONE_SIDE > 0 to enable round-trip cost accounting.
    ap.add_argument("--fees-bps-one-side", type=float,
                    default=float(os.getenv("TB_FEES_BPS_ONE_SIDE", "0") or 0))
    ap.add_argument("--fallback-cost-bps", type=float,
                    default=float(os.getenv("TB_FALLBACK_COST_BPS", "0") or 0))

    ap.add_argument("--adv-max", type=float, default=float(os.getenv("ML_LABEL_ADV_MAX", "1.2") or 1.2))
    args = ap.parse_args()

    tick_map = group_ticks(_read_ndjson(args.ticks))
    out_rows: list[dict[str, Any]] = []

    for inp in _read_ndjson(args.inputs):
        sid = (inp.get("sid", "") or "")
        sym = (inp.get("symbol", "") or "").upper()
        ts0 = _i(inp.get("ts_ms", inp.get("ts", 0)), 0)
        direction = (inp.get("direction", "") or "").upper()
        indicators = inp.get("indicators") if isinstance(inp.get("indicators"), dict) else {}

        if not sid or not sym or ts0 <= 0 or direction not in ("LONG", "SHORT"):
            continue

        series = tick_map.get(sym, [])
        ts1 = ts0 + int(args.h_ms)
        path = slice_path(series, ts0, ts1)
        if not path:
            # DROP samples with no ticks to prevent data leakage (avoiding y_edge=0 for NO_TICKS)
            continue

        entry_px = _f(inp.get("entry_px", 0.0), 0.0)
        if entry_px <= 0.0:
            entry_px = pick_entry_price(path)

        tp_bps, sl_bps, scale_bps = infer_tp_sl_bps(
            indicators,
            tp_k_atr=float(args.tp_k_atr),
            sl_k_atr=float(args.sl_k_atr),
            fallback_tp_bps=float(args.fallback_tp_bps),
            fallback_sl_bps=float(args.fallback_sl_bps),
        )

        cost_bps = infer_cost_bps(
            indicators,
            fees_bps_one_side=float(args.fees_bps_one_side),
            fallback_cost_bps=float(args.fallback_cost_bps),
        )

        res = label_path(
            ts0_ms=ts0,
            direction=direction,
            entry_px=float(entry_px),
            path=path,
            spec=BarrierSpec(
                h_ms=int(args.h_ms),
                tp_bps=float(tp_bps),
                sl_bps=float(sl_bps),
                cost_bps=float(cost_bps),
            ),
        )

        mae_r = (res.mae_bps / scale_bps) if scale_bps > 1e-9 else 0.0
        mfe_r = (res.mfe_bps / scale_bps) if scale_bps > 1e-9 else 0.0

        # Legacy gross label (TP-hit with bounded adverse drift)
        y_edge = 1 if (res.outcome == "TP_HIT" and res.adverse_proxy <= float(args.adv_max)) else 0

        out_rows.append({
            "sid": sid, "symbol": sym, "ts_ms": ts0, "direction": direction,
            "entry_px": float(entry_px),
            "h_ms": int(args.h_ms),
            "tp_bps": float(tp_bps), "sl_bps": float(sl_bps),
            "tb_outcome": res.outcome, "tb_hit_ms": int(res.hit_ms),
            "mae_bps": float(res.mae_bps), "mfe_bps": float(res.mfe_bps),
            "mae_r": float(mae_r), "mfe_r": float(mfe_r),
            "adverse_proxy": float(res.adverse_proxy),
            "y_edge": int(y_edge),
            # v14_of cost-aware label fields (cost_bps==0 ⇒ y_edge_cost_aware==y_edge for TP_HIT)
            "cost_bps": float(res.cost_bps),
            "realized_close_bps": float(res.realized_close_bps),
            "edge_after_cost_bps": float(res.edge_after_cost_bps),
            "y_edge_cost_aware": int(res.y_edge_cost_aware),
        })

    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as f:
        for r0 in out_rows:
            f.write(_safe_json(r0) + "\n")
    print(_safe_json({"written": len(out_rows), "out": args.out}))


if __name__ == "__main__":
    main()
