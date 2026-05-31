"""Fit ConfidenceScorer confirmation bonus weights from closed trades.

Goal:
- Learn per-confirmation weights and a few interaction terms using recent closed trades.
- Output a JSON config that can be pasted into your symbol/source config or pushed to Redis.

Data requirements:
- trades_closed(_p0) rows with config_json containing signal_payload.of.evidence/confirmations.

Usage (example):
  python -m ml_analysis.tools.fit_confidence_bonus_weights_v1 \
    --dsn "$ANALYTICS_DB_DSN" --table trades_closed_p0 \
    --symbol BTCUSDT --source binance_futures --limit 5000 \
    --out /var/lib/trade/of_reports/out/confidence/bonus_weights_BTCUSDT.json

Notes:
- This is Phase-2 calibration for the heuristic scorer (bonuses/synergies).
- It does not train the ML model; Phase-3 ML uses ml_p_cal fusion in the scorer.
"""

from __future__ import annotations

import argparse
import json
import math
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Tuple

import numpy as np


@dataclass
class Row:
    y: int
    feats: Dict[str, float]


FEATURES: Tuple[str, ...] = (
    # base confirmations
    "reclaim",
    "obi_stable",
    "iceberg_strict",
    "fp_edge_absorb",
    "rsi_agree",
    "div_match",
    # sweep types
    "sweep_eqh",
    "sweep_eql",
    "sweep_eq",
    "sweep",
    # interactions
    "syn_sweep_reclaim",
    "syn_sweepeq_reclaim",
    "syn_sweep_fp",
    "syn_div_sweep",
    "syn_rsi_obi",
    "syn_ice_fp",
    # regimes (optional)
    "regime_trend",
    "regime_range",
    "regime_mixed",
)


def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        if x is None:
            return default
        return float(x)
    except Exception:
        return default


def _sigmoid(z: float) -> float:
    if z >= 0:
        ez = math.exp(-z)
        return 1.0 / (1.0 + ez)
    ez = math.exp(z)
    return ez / (1.0 + ez)


def _logit(p: float, eps: float = 1e-6) -> float:
    p = min(max(p, eps), 1.0 - eps)
    return math.log(p / (1.0 - p))


def _auc_roc(y: np.ndarray, p: np.ndarray) -> float:
    # Simple AUC via rank statistic (Mann–Whitney U)
    y = y.astype(int)
    pos = p[y == 1]
    neg = p[y == 0]
    if len(pos) == 0 or len(neg) == 0:
        return float("nan")
    # ranks of all
    order = np.argsort(p)
    ranks = np.empty_like(order, dtype=float)
    ranks[order] = np.arange(len(p)) + 1
    sum_ranks_pos = float(ranks[y == 1].sum())
    n_pos = float(len(pos))
    n_neg = float(len(neg))
    u = sum_ranks_pos - n_pos * (n_pos + 1.0) / 2.0
    return u / (n_pos * n_neg)


def _brier(y: np.ndarray, p: np.ndarray) -> float:
    return float(np.mean((p - y) ** 2))


def _extract_conf_keys(evidence: Dict[str, Any] | None, confirmations: List[Any] | None) -> set[str]:
    keys: set[str] = set()
    if isinstance(evidence, dict):
        for k in evidence.keys():
            keys.add(str(k))
    if confirmations:
        for c in confirmations:
            s = str(c)
            if "=" in s:
                keys.add(s.split("=", 1)[0])
            else:
                keys.add(s)
    return keys


def _extract_row(config_json: Dict[str, Any], y: int) -> Row | None:
    sp = config_json.get("signal_payload") or config_json.get("signal") or {}
    of = sp.get("of") or {}

    evidence = of.get("evidence") if isinstance(of, dict) else {}
    confirmations = of.get("confirmations") if isinstance(of, dict) else []

    keys = _extract_conf_keys(evidence if isinstance(evidence, dict) else None, confirmations if isinstance(confirmations, list) else None)

    feats: Dict[str, float] = {k: 0.0 for k in FEATURES}

    def has(k: str) -> bool:
        return k in keys

    # base
    feats["reclaim"] = 1.0 if has("reclaim") else 0.0
    feats["obi_stable"] = 1.0 if has("obi_stable") else 0.0
    feats["iceberg_strict"] = 1.0 if (has("iceberg_strict") or has("ice_strict")) else 0.0
    feats["fp_edge_absorb"] = 1.0 if has("fp_edge_absorb") else 0.0
    feats["rsi_agree"] = 1.0 if has("rsi_agree") else 0.0
    feats["div_match"] = 1.0 if has("div_match") else 0.0

    # sweep kind
    sweep_kind: str | None = None
    if has("sweep_eqh"):
        sweep_kind = "sweep_eqh"
    elif has("sweep_eql"):
        sweep_kind = "sweep_eql"
    elif has("sweep_eq"):
        sweep_kind = "sweep_eq"
    elif has("sweep"):
        sweep_kind = "sweep"

    if sweep_kind:
        feats[sweep_kind] = 1.0

    # synergies
    feats["syn_sweep_reclaim"] = 1.0 if (sweep_kind is not None and feats["reclaim"] > 0.0) else 0.0
    feats["syn_sweepeq_reclaim"] = 1.0 if (sweep_kind in ("sweep_eqh", "sweep_eql", "sweep_eq") and feats["reclaim"] > 0.0) else 0.0
    feats["syn_sweep_fp"] = 1.0 if (sweep_kind is not None and feats["fp_edge_absorb"] > 0.0) else 0.0
    feats["syn_div_sweep"] = 1.0 if (sweep_kind is not None and feats["div_match"] > 0.0) else 0.0
    feats["syn_rsi_obi"] = 1.0 if (feats["rsi_agree"] > 0.0 and feats["obi_stable"] > 0.0) else 0.0
    feats["syn_ice_fp"] = 1.0 if (feats["iceberg_strict"] > 0.0 and feats["fp_edge_absorb"] > 0.0) else 0.0

    # regime (best effort)
    ind = of.get("indicators") if isinstance(of, dict) else None
    regime_s = ""
    if isinstance(ind, dict):
        regime_s = str(ind.get("market_regime") or ind.get("regime") or "").lower()
    if not regime_s:
        regime_s = str(config_json.get("regime") or "").lower()

    feats["regime_trend"] = 1.0 if "trend" in regime_s else 0.0
    feats["regime_range"] = 1.0 if "range" in regime_s else 0.0
    feats["regime_mixed"] = 1.0 if "mixed" in regime_s else 0.0

    return Row(y=y, feats=feats)


def _fetch_rows(dsn: str, table: str, symbol: str, source: str, limit: int) -> List[Tuple[float, float, Dict[str, Any]]]:
    import psycopg2

    q = f"""
        SELECT
            COALESCE(r_multiple, 0) AS r_multiple,
            COALESCE(pnl_net, 0) AS pnl_net,
            COALESCE(config_json, '{{}}'::jsonb) AS config_json
        FROM {table}
        WHERE symbol = %s AND source = %s
        ORDER BY exit_ts DESC
        LIMIT %s
    """

    with psycopg2.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(q, (symbol, source, limit))
            rows = cur.fetchall()

    out: List[Tuple[float, float, Dict[str, Any]]] = []
    for r_mul, pnl_net, cfg in rows:
        if isinstance(cfg, str):
            try:
                cfg = json.loads(cfg)
            except Exception:
                cfg = {}
        out.append((_safe_float(r_mul), _safe_float(pnl_net), cfg if isinstance(cfg, dict) else {}))
    return out


def _fit_logreg(X: np.ndarray, y: np.ndarray, l2: float = 1.0, steps: int = 2000, lr: float = 0.05) -> Tuple[np.ndarray, float]:
    # Simple logistic regression with L2, gradient descent.
    n, d = X.shape
    w = np.zeros(d, dtype=float)
    b = 0.0

    for _ in range(steps):
        z = X @ w + b
        p = 1.0 / (1.0 + np.exp(-np.clip(z, -35, 35)))
        # gradients
        g = (p - y)
        dw = (X.T @ g) / n + l2 * w
        db = float(np.mean(g))
        w -= lr * dw
        b -= lr * db
    return w, b


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dsn", default=os.getenv("ANALYTICS_DB_DSN") or (os.getenv("ANALYTICS_DB_DSN") or os.getenv("PG_DSN")) or "")
    ap.add_argument("--table", default=os.getenv("TRADES_CLOSED_TABLE", "trades_closed_p0"))
    ap.add_argument("--symbol", required=True)
    ap.add_argument("--source", required=True)
    ap.add_argument("--limit", type=int, default=int(os.getenv("LIMIT", "5000")))
    ap.add_argument("--label", choices=["rpos", "pnlpos"], default=os.getenv("LABEL", "rpos"))
    ap.add_argument("--min_rows", type=int, default=int(os.getenv("MIN_ROWS", "300")))
    ap.add_argument("--out", default=os.getenv("OUT", ""))
    args = ap.parse_args()

    if not args.dsn:
        raise SystemExit("DSN not provided. Set --dsn or ANALYTICS_DB_DSN/PG_DSN")

    raw = _fetch_rows(args.dsn, args.table, args.symbol, args.source, args.limit)

    rows: List[Row] = []
    for r_mul, pnl_net, cfg in raw:
        if args.label == "pnlpos":
            y = 1 if pnl_net > 0 else 0
        else:
            y = 1 if r_mul > 0 else 0
        rr = _extract_row(cfg, y)
        if rr is not None:
            rows.append(rr)

    if len(rows) < args.min_rows:
        raise SystemExit(f"Not enough rows: {len(rows)} < {args.min_rows} (symbol={args.symbol}, source={args.source})")

    feat_names = list(FEATURES)
    X = np.asarray([[r.feats[n] for n in feat_names] for r in rows], dtype=float)
    y = np.asarray([r.y for r in rows], dtype=float)

    # Try sklearn if available; fallback to local GD.
    coef: np.ndarray
    intercept: float
    used = "gd"
    try:
        from sklearn.linear_model import LogisticRegression  # type: ignore

        model = LogisticRegression(penalty="l2", C=1.0, solver="lbfgs", max_iter=400)
        model.fit(X, y)
        coef = model.coef_[0].astype(float)
        intercept = float(model.intercept_[0])
        used = "sklearn"
    except Exception:
        coef, intercept = _fit_logreg(X, y, l2=0.5, steps=2500, lr=0.08)

    p = 1.0 / (1.0 + np.exp(-np.clip(X @ coef + intercept, -35, 35)))

    metrics = {
        "rows": int(len(rows)),
        "pos_rate": float(np.mean(y)),
        "auc": _auc_roc(y.astype(int), p),
        "brier": _brier(y, p),
        "fit": used,
    }

    # Map coefficients to scorer config keys (roughly in bonus-space).
    # We keep only positive coefficients for bonuses; interactions are separate keys.
    cfg_out: Dict[str, Any] = {
        "symbol": args.symbol,
        "source": args.source,
        "metrics": metrics,
        "logit_intercept": float(intercept),
        "weights": {},
    }

    def put(k: str, v: float) -> None:
        cfg_out["weights"][k] = float(v)

    coef_map = dict(zip(feat_names, coef.tolist()))

    # bonuses
    put("conf_bonus_reclaim", max(0.0, coef_map["reclaim"]))
    put("conf_bonus_obi_stable", max(0.0, coef_map["obi_stable"]))
    put("conf_bonus_iceberg_strict", max(0.0, coef_map["iceberg_strict"]))
    put("conf_bonus_fp_edge_absorb", max(0.0, coef_map["fp_edge_absorb"]))
    put("conf_bonus_rsi_agree", max(0.0, coef_map["rsi_agree"]))
    put("conf_bonus_div_match", max(0.0, coef_map["div_match"]))

    put("conf_bonus_sweep_eqh", max(0.0, coef_map["sweep_eqh"]))
    put("conf_bonus_sweep_eql", max(0.0, coef_map["sweep_eql"]))
    put("conf_bonus_sweep_eq", max(0.0, coef_map["sweep_eq"]))
    put("conf_bonus_sweep", max(0.0, coef_map["sweep"]))

    # synergies
    put("conf_syn_sweep_reclaim", max(0.0, coef_map["syn_sweep_reclaim"]))
    put("conf_syn_sweepeq_reclaim", max(0.0, coef_map["syn_sweepeq_reclaim"]))
    put("conf_syn_sweep_fp", max(0.0, coef_map["syn_sweep_fp"]))
    put("conf_syn_div_sweep", max(0.0, coef_map["syn_div_sweep"]))
    put("conf_syn_rsi_obi", max(0.0, coef_map["syn_rsi_obi"]))
    put("conf_syn_ice_fp", max(0.0, coef_map["syn_ice_fp"]))

    # optional: cap + oscillator multipliers are left to manual tuning.

    out_path = args.out
    if not out_path:
        out_path = f"confidence_bonus_weights_{args.source}_{args.symbol}.json"

    outp = Path(out_path)
    outp.parent.mkdir(parents=True, exist_ok=True)
    outp.write_text(json.dumps(cfg_out, ensure_ascii=False, indent=2), encoding="utf-8")

    print(json.dumps({"out": str(outp), "metrics": metrics}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
