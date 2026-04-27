from __future__ import annotations

import argparse
import json
import os
from typing import Any, Dict, Iterable, List, Tuple

from core.triple_barrier import BarrierSpec, label_path, pick_entry_price


def _read_ndjson(path: str) -> Iterable[Dict[str, Any]]:
    with open(path, "r", encoding="utf-8") as f:
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


def group_ticks(ticks: Iterable[Dict[str, Any]]) -> Dict[str, List[Tuple[int, float]]]:
    out: Dict[str, List[Tuple[int, float]]] = {}
    for t in ticks:
        sym = str(t.get("symbol", "") or "").upper()
        ts = _i(t.get("ts_ms", 0), 0)
        px = _f(t.get("price", 0.0), 0.0)
        if not sym or ts <= 0 or px <= 0:
            continue
        out.setdefault(sym, []).append((ts, px))
    for sym in list(out.keys()):
        out[sym].sort(key=lambda x: x[0])
    return out


def slice_path(series: List[Tuple[int, float]], ts0: int, ts1: int) -> List[Tuple[int, float]]:
    return [(ts, px) for ts, px in series if ts0 <= ts <= ts1]


def infer_tp_sl_bps(indicators: Dict[str, Any], *, tp_k_atr: float, sl_k_atr: float, fallback_tp_bps: float, fallback_sl_bps: float) -> Tuple[float, float, float]:
    stop_bps = _f(indicators.get("stop_bps", 0.0), 0.0)
    atr_bps = _f(indicators.get("atr_bps", 0.0), 0.0)
    if stop_bps > 1e-6:
        return tp_k_atr * stop_bps, sl_k_atr * stop_bps, stop_bps
    if atr_bps > 1e-6:
        return tp_k_atr * atr_bps, sl_k_atr * atr_bps, atr_bps
    return fallback_tp_bps, fallback_sl_bps, 0.0


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

    ap.add_argument("--adv-max", type=float, default=float(os.getenv("ML_LABEL_ADV_MAX", "1.2") or 1.2))
    args = ap.parse_args()

    tick_map = group_ticks(_read_ndjson(args.ticks))
    out_rows: List[Dict[str, Any]] = []

    for inp in _read_ndjson(args.inputs):
        sid = str(inp.get("sid", "") or "")
        sym = str(inp.get("symbol", "") or "").upper()
        ts0 = _i(inp.get("ts_ms", inp.get("ts", 0)), 0)
        direction = str(inp.get("direction", "") or "").upper()
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

        res = label_path(
            ts0_ms=ts0,
            direction=direction,
            entry_px=float(entry_px),
            path=path,
            spec=BarrierSpec(h_ms=int(args.h_ms), tp_bps=float(tp_bps), sl_bps=float(sl_bps)),
        )

        mae_r = (res.mae_bps / scale_bps) if scale_bps > 1e-9 else 0.0
        mfe_r = (res.mfe_bps / scale_bps) if scale_bps > 1e-9 else 0.0

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
        })

    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as f:
        for r0 in out_rows:
            f.write(_safe_json(r0) + "\n")
    print(_safe_json({"written": len(out_rows), "out": args.out}))


if __name__ == "__main__":
    main()
