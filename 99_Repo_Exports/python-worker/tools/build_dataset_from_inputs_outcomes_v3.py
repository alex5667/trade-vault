from __future__ import annotations

import argparse
import json
import os
from collections.abc import Iterable
from typing import Any

import pandas as pd


def _read_ndjson(path: str) -> Iterable[dict[str, Any]]:
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            yield json.loads(line)


def _get_payload(obj: dict[str, Any]) -> dict[str, Any]:
    if "payload" in obj and isinstance(obj["payload"], str) and obj["payload"].strip().startswith("{"):
        try:
            return json.loads(obj["payload"])
        except Exception:
            return obj
    return obj


def _f(x: Any, d: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return d


def _pick(obj: dict[str, Any], keys: list[str], default=None):
    for k in keys:
        if k in obj and obj[k] is not None:
            return obj[k]
    return default


def compute_adverse_proxy(close: dict[str, Any]) -> float:
    mae_r = _pick(close, ["mae_r", "MAE_R"], None)
    mfe_r = _pick(close, ["mfe_r", "MFE_R"], None)
    if mae_r is not None:
        mae_r = _f(mae_r, 0.0)
        mfe_rv = _f(mfe_r, 0.0) if mfe_r is not None else 0.0
        if mfe_rv > 1e-9:
            return float(mae_r / mfe_rv)
        return float(mae_r)

    mae_bps = _f(_pick(close, ["mae_bps", "MAE_BPS"], 0.0), 0.0)
    mfe_bps = _f(_pick(close, ["mfe_bps", "MFE_BPS"], 0.0), 0.0)
    if mfe_bps > 1e-6:
        return float(mae_bps / mfe_bps)
    return float(mae_bps)


def compute_r_net(close: dict[str, Any]) -> float:
    r_mult = _pick(close, ["r_mult", "realized_r", "realized_R", "R"], 0.0)
    r = _f(r_mult, 0.0)

    fees_bps = _f(_pick(close, ["fees_bps", "fee_bps"], 0.0), 0.0)
    slip_bps = _f(_pick(close, ["slippage_bps_real", "slippage_bps", "slip_bps"], 0.0), 0.0)
    risk_bps = _f(_pick(close, ["risk_bps", "stop_bps"], 0.0), 0.0)
    if risk_bps > 1e-6:
        r = r - (fees_bps + slip_bps) / risk_bps
    return float(r)


def compute_y_edge(close: dict[str, Any], *, r_min: float, adv_max: float) -> int:
    r_net = compute_r_net(close)
    adv = compute_adverse_proxy(close)
    return 1 if (r_net >= r_min and adv <= adv_max) else 0


def compute_y_util(close: dict[str, Any], *, cost_bps_per_r: float, risk_penalty: float) -> float:
    pnl = _f(_pick(close, ["pnl_net", "pnl", "pnl_usd"], 0.0), 0.0)
    risk_usd = _f(_pick(close, ["risk_usd", "risk", "risk_amount"], 0.0), 0.0)
    r_mult = _f(_pick(close, ["r_mult", "realized_r", "realized_R"], 0.0), 0.0)

    exec_risk_bps = _f(_pick(close, ["exec_risk_bps_real", "exec_risk_bps"], 0.0), 0.0)
    if exec_risk_bps <= 0.0:
        spread = _f(_pick(close, ["spread_bps_at_entry", "spread_bps"], 0.0), 0.0)
        slip = _f(_pick(close, ["slippage_bps_real", "slippage_bps"], 0.0), 0.0)
        exec_risk_bps = spread + slip

    cost_r = exec_risk_bps / max(cost_bps_per_r, 1e-6)
    if risk_usd > 0.0:
        return float(pnl - (cost_r * risk_usd) - (risk_penalty * risk_usd))
    return float(r_mult - cost_r - risk_penalty)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--inputs", required=True)
    ap.add_argument("--closed", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--r-min", type=float, default=float(os.getenv("ML_LABEL_R_MIN", "0.5") or 0.5))
    ap.add_argument("--adv-max", type=float, default=float(os.getenv("ML_LABEL_ADV_MAX", "1.2") or 1.2))
    ap.add_argument("--cost-bps-per-r", type=float, default=float(os.getenv("ML_LABEL_COST_BPS_PER_R", "10.0") or 10.0))
    ap.add_argument("--risk-penalty", type=float, default=float(os.getenv("ML_LABEL_RISK_PENALTY", "0.0") or 0.0))
    args = ap.parse_args()

    closed: dict[str, dict[str, Any]] = {}
    for obj in _read_ndjson(args.closed):
        o = _get_payload(obj)
        sid = (o.get("sid", "") or "")
        if sid:
            closed[sid] = o

    rows: list[dict[str, Any]] = []
    miss = 0
    for obj in _read_ndjson(args.inputs):
        o = _get_payload(obj)
        sid = (o.get("sid", "") or "")
        if not sid:
            continue
        c = closed.get(sid)
        if not c:
            miss += 1
            continue

        y_edge = compute_y_edge(c, r_min=float(args.r_min), adv_max=float(args.adv_max))
        y_util = compute_y_util(c, cost_bps_per_r=float(args.cost_bps_per_r), risk_penalty=float(args.risk_penalty))
        adv = compute_adverse_proxy(c)
        r_net = compute_r_net(c)

        indicators = o.get("indicators") if isinstance(o.get("indicators"), dict) else {}

        row = {
            "sid": sid,
            "ts_ms": int(o.get("ts_ms", o.get("ts", 0)) or 0),
            "symbol": (o.get("symbol", "") or ""),
            "direction": (o.get("direction", "") or ""),
            "scenario_v4": (o.get("scenario_v4", o.get("scenario", "")) or ""),
            "indicators": indicators,
            "r_mult": float(_f(_pick(c, ["r_mult", "realized_r", "realized_R"], 0.0), 0.0)),
            "r_net": float(r_net),
            "y_edge": int(y_edge),
            "y_util": float(y_util),
            "adverse_proxy": float(adv),
            "mae_r": float(_f(_pick(c, ["mae_r", "MAE_R"], 0.0), 0.0)),
            "mfe_r": float(_f(_pick(c, ["mfe_r", "MFE_R"], 0.0), 0.0)),
            "mae_bps": float(_f(_pick(c, ["mae_bps", "MAE_BPS"], 0.0), 0.0)),
            "mfe_bps": float(_f(_pick(c, ["mfe_bps", "MFE_BPS"], 0.0), 0.0)),
            "close_reason": str(_pick(c, ["close_reason", "reason"], "") or ""),
        }
        for k in ("rule_score", "rule_have", "rule_need", "cancel_spike_veto"):
            if k in o:
                row[k] = o.get(k)
        rows.append(row)

    df = pd.DataFrame(rows)
    df.to_parquet(args.out, index=False)

    summary = {
        "inputs_rows": len(rows) + miss,
        "joined_rows": len(rows),
        "missing_closed": miss,
        "label_r_min": float(args.r_min),
        "label_adv_max": float(args.adv_max),
        "pos_rate": float(df["y_edge"].mean()) if len(df) else 0.0,
        "util_mean": float(df["y_util"].mean()) if len(df) else 0.0,
    }
    with open(args.out + ".json", "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)


if __name__ == "__main__":
    main()

