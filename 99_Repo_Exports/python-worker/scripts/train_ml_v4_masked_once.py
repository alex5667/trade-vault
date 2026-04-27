#!/usr/bin/env python3
"""
Единоразовый скрипт: Обогащенная ретроспективная фильтрация + переобучение Scorer V4.

Добавляет 7 "золотых" признаков (риск исполнения, дельта, OFI и др.)
и применяет маскировку по liq_book_stale_ms.
"""
from __future__ import annotations
import argparse, hashlib, json, logging, math, os, shutil, time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
import numpy as np

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("train_v4_enriched")

NOTIFY_STREAM = os.getenv("NOTIFY_STREAM", "notify:telegram")
MASK_BOOK_STALE_MS_MAX = int(os.getenv("MASK_BOOK_STALE_MS_MAX", "1000"))

def _get_dsn() -> str:
    for v in ("ANALYTICS_DB_DSN", "TRADES_DB_DSN", "PG_DSN", "DATABASE_URL"):
        x = os.getenv(v)
        if x: return x
    return f"postgresql://trading:{os.getenv('TRADING_PASSWORD','trading_password')}@postgres:5432/scanner_analytics"

def _redis():
    try:
        import redis as _r
        r = _r.from_url(os.getenv("REDIS_URL","redis://redis-worker-1:6379/0"), decode_responses=True)
        r.ping(); return r
    except Exception: return None

def _notify(r, text: str):
    if r is None: return
    try:
        r.xadd(NOTIFY_STREAM, {"type":"report","text":text,"parse_mode":"HTML","source":"train_v4_enriched"}, maxlen=50_000)
    except Exception: pass

# ---------------------------------------------------------------------------
# Features (Обогащенный набор V4)
# ---------------------------------------------------------------------------
NUMERIC_FEATURES = [
    "atr_14","obi_avg_20","weak_progress_ratio",
    "l3_spread_bps","l3_microprice_shift_bps_20","l3_microprice_velocity_bps",
    "l3_obi_5","l3_obi_20","l3_obi_50","l3_obi_persistence_score",
    "l3_cancel_to_trade_bid_5s","l3_cancel_to_trade_ask_5s",
    "l3_cancel_to_trade_bid_20s","l3_cancel_to_trade_ask_20s",
    "l3_queue_pressure_bid","l3_queue_pressure_ask","l3_market_depth_imbalance",
    # Golden Features (Execution & Microstructure)
    "exec_risk_bps", "fill_prob_proxy", "delta_z", "ofi_z", "spread_bps", "burst_z", "data_health"
]
DERIVED = ["direction_long","cancel_to_trade_max","obi_spread","queue_imbalance","outlier_count","is_extreme_outlier"]

FETCH_SQL = """
SELECT
    s.ts,
    EXTRACT(EPOCH FROM s.ts)::BIGINT * 1000 AS ts_ms,
    s.signal_id,
    s.symbol,
    s.direction,
    s.signal_family,
    s.conf_score,
    s.atr_14,
    s.delta_spike_z,
    s.obi_avg_20,
    s.weak_progress_ratio,
    s.l3_spread_bps,
    s.l3_microprice_shift_bps_20,
    s.l3_microprice_velocity_bps,
    s.l3_obi_5,
    s.l3_obi_20,
    s.l3_obi_50,
    s.l3_obi_persistence_score,
    s.l3_cancel_to_trade_bid_5s,
    s.l3_cancel_to_trade_ask_5s,
    s.l3_cancel_to_trade_bid_20s,
    s.l3_cancel_to_trade_ask_20s,
    s.l3_queue_pressure_bid,
    s.l3_queue_pressure_ask,
    s.l3_market_depth_imbalance,
    t.r          AS pnl_r,
    t.hit        AS is_win,
    t.slippage_bps,
    t.adverse_bps,
    t.holding_ms,
    t.close_reason_bucket,
    -- book_stale
    COALESCE(
        (tc.config_json->'indicators'->>'liq_book_stale_ms')::BIGINT,
        (tc.config_json->'indicators'->>'book_ts_gap_ms')::BIGINT,
        0
    ) AS ind_book_stale_ms,
    -- Golden Features
    COALESCE((tc.config_json->'indicators'->>'exec_risk_bps')::FLOAT, 0.0) AS exec_risk_bps,
    COALESCE((tc.config_json->'indicators'->>'fill_prob_proxy')::FLOAT, 0.0) AS fill_prob_proxy,
    COALESCE((tc.config_json->'indicators'->>'delta_z')::FLOAT, 0.0) AS delta_z,
    COALESCE((tc.config_json->'indicators'->>'ofi_z')::FLOAT, 0.0) AS ofi_z,
    COALESCE((tc.config_json->'indicators'->>'spread_bps')::FLOAT, 0.0) AS spread_bps,
    COALESCE((tc.config_json->'indicators'->>'burst_z')::FLOAT, 0.0) AS burst_z,
    COALESCE((tc.config_json->'indicators'->>'data_health')::FLOAT, 1.0) AS data_health
FROM signal_facts s
JOIN trade_performance t ON s.signal_id = t.signal_id
LEFT JOIN trades_closed tc ON tc.sid = s.signal_id
WHERE s.ts > NOW() - INTERVAL '{lookback} days'
  AND t.r IS NOT NULL
  AND ABS(t.r) < 20.0
  AND s.symbol NOT IN ('XAUUSDT','XAUUSD','GOLD','XAGUSD','XAGUSDT')
ORDER BY s.ts ASC
"""

def fetch_data(lookback: int) -> Optional[Tuple]:
    try:
        import psycopg2
    except ImportError:
        log.error("psycopg2 not installed"); return None

    dsn = _get_dsn()
    log.info("Connecting to DB...")
    sql = FETCH_SQL.format(lookback=lookback)
    try:
        conn = psycopg2.connect(dsn, connect_timeout=15, options="-c statement_timeout=120000")
        cur = conn.cursor()
        cur.execute(sql)
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
        cur.close(); conn.close()
    except Exception as e:
        log.error("DB fetch failed: %s", e); return None

    if not rows:
        log.warning("No rows returned"); return None

    log.info("Fetched %d rows before masking", len(rows))
    ci = {c: i for i, c in enumerate(cols)}
    bsm_i = ci.get("ind_book_stale_ms")
    kept, drop_book = [], 0
    for row in rows:
        bsm = float(row[bsm_i] or 0) if bsm_i is not None else 0.0
        if bsm > 0 and bsm > MASK_BOOK_STALE_MS_MAX:
            drop_book += 1; continue
        kept.append(row)

    total = len(rows)
    log.info("After masking: kept=%d dropped=%d (%.1f%%)", len(kept), drop_book, 100*drop_book/max(1,total))
    
    keep_cols = [c for c in cols if c != "ind_book_stale_ms"]
    keep_idx  = [ci[c] for c in keep_cols]
    rows_out  = [tuple(row[i] for i in keep_idx) for row in kept]
    return keep_cols, rows_out, total, drop_book, {"book_stale": drop_book}

def _f(x, d=0.0):
    try:
        v = float(x) if x is not None else float(d)
        return v if math.isfinite(v) else float(d)
    except Exception: return float(d)

def _feat(rd: Dict[str, Any]) -> List[float]:
    out = [_f(rd.get(c)) for c in NUMERIC_FEATURES]
    out.append(1.0 if str(rd.get("direction")).upper() == "LONG" else 0.0)
    c2t = [_f(rd.get(k)) for k in ("l3_cancel_to_trade_bid_5s","l3_cancel_to_trade_ask_5s","l3_cancel_to_trade_bid_20s","l3_cancel_to_trade_ask_20s")]
    out.append(max(c2t) if c2t else 0.0)
    out.append(_f(rd.get("l3_obi_5")) - _f(rd.get("l3_obi_50")))
    out.append(_f(rd.get("l3_queue_pressure_bid")) - _f(rd.get("l3_queue_pressure_ask")))
    oc = sum(1.0 for v in out if abs(v) > 10.0)
    out += [oc, 1.0 if oc > 0 else 0.0]
    return out

def _feat_names(): return [f"f_{c}" for c in NUMERIC_FEATURES] + DERIVED
def _target(rd): return 1.0 if _f(rd.get("pnl_r")) >= 0.3 else 0.0

def _fit_scaler(X, names):
    p = {}
    for i, n in enumerate(names):
        col = X[:, i].astype(np.float64)
        c = float(np.median(col))
        s = float(np.median(np.abs(col - c)))
        if not math.isfinite(s) or s <= 1e-12: s = float(np.std(col)) or 1.0
        p[n] = {"center": c, "scale": s}
    return p

def _apply_scaler(X, names, p):
    out = X.copy()
    for i, n in enumerate(names):
        if n in p: out[:, i] = (X[:, i] - p[n]["center"]) / max(p[n]["scale"], 1e-12)
    return out

def train_model(X, y, ts_ms):
    try: import lightgbm as lgb
    except ImportError: raise SystemExit("pip install lightgbm")
    
    params = {"objective":"binary","metric":"auc","verbose":-1,"learning_rate":0.05,
              "num_leaves":31,"min_data_in_leaf":50,"max_depth":5,"feature_fraction":0.8,
              "bagging_fraction":0.8,"bagging_freq":5,"reg_lambda":5.0,"seed":42,"n_jobs":2}

    ts_arr = np.asarray(ts_ms, dtype=np.int64)
    order  = np.argsort(ts_arr, kind="mergesort")
    n = len(order)
    n_splits = 5
    min_tr = 2000
    sz = [n // n_splits] * n_splits
    for i in range(n % n_splits): sz[i] += 1

    oof = np.full(n, np.nan)
    top5s, ymeans = [], []
    fold_n, start = 0, 0

    for fs in sz:
        end = start + fs
        va_idx = order[start:end]
        if len(va_idx) == 0: start = end; continue
        cut = int(np.min(ts_arr[va_idx])) - 300_000
        tr_idx = np.where(ts_arr < cut)[0]
        if len(tr_idx) < min_tr: start = end; continue
        fold_n += 1
        Xtr, ytr = X[tr_idx], y[tr_idx]
        Xva, yva = X[va_idx], y[va_idx]
        m = lgb.train(params, lgb.Dataset(Xtr, ytr), num_boost_round=100)
        preds = m.predict(Xva)
        oof[va_idx] = preds
        k = max(1, int(0.05 * len(va_idx)))
        top_i = np.argsort(preds)[::-1][:k]
        top5s.append(float(np.mean(yva[top_i] > 0)))
        ymeans.append(float(np.mean(yva > 0)))
        log.info("Fold %d: tr=%d va=%d pos=%.1f%% top5=%.1f%%", fold_n, len(ytr), len(yva), ymeans[-1]*100, top5s[-1]*100)
        start = end

    if fold_n == 0: raise SystemExit("No valid folds")

    final = lgb.train(params, lgb.Dataset(X, y), num_boost_round=100)
    from sklearn.metrics import roc_auc_score
    mask = np.isfinite(oof)
    roc = float(roc_auc_score(y[mask], oof[mask]))
    metrics = {"roc_auc_oof":roc, "top5_hit_rate":float(np.mean(top5s)), "folds":fold_n, "y_mean":float(np.mean(y))}
    log.info("OOF ROC=%.4f Top5=%.2f%%", roc, metrics["top5_hit_rate"]*100)
    return final, oof, metrics

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--lookback", type=int, default=90)
    ap.add_argument("--output", type=str, default="/var/lib/trade/ml_models/scorer_v4/scorer_v4.joblib")
    args = ap.parse_args()
    
    try: import joblib
    except ImportError: raise SystemExit("pip install joblib")

    r = _redis()
    result = fetch_data(args.lookback)
    if result is None:
        _notify(r, "❌ <b>V4 Enriched</b> — нет данных"); return 1

    cols, rows, total, dropped, drop_stats = result
    names = _feat_names()
    rds   = [dict(zip(cols, row)) for row in rows]
    X_raw = np.array([_feat(rd) for rd in rds], dtype=np.float64)
    y_raw = np.array([_target(rd) for rd in rds], dtype=np.float64)
    ts_ms = [int(_f(rd.get("ts_ms"))) for rd in rds]

    valid = np.isfinite(y_raw) & np.all(np.isfinite(X_raw), axis=1)
    X, y  = X_raw[valid], y_raw[valid]
    ts_ms = [ts_ms[i] for i in range(len(ts_ms)) if valid[i]]
    log.info("Final dataset: %d samples, pos_rate=%.1f%%", len(X), float(np.mean(y>0))*100)

    scaler = _fit_scaler(X, names)
    Xs     = _apply_scaler(X, names, scaler)
    model, oof, metrics = train_model(Xs, y, ts_ms)

    # Save
    pack = {
        "schema_version": 3, "kind": "ml_scorer_v4_enriched",
        "model": model, "feature_cols": names,
        "robust_scaler": scaler, "metrics": metrics,
        "trained_at_ms": int(time.time()*1000), "n_samples": len(X)
    }
    out = Path(args.output)
    out.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump(pack, str(out))
    log.info("✅ Saved: %s", out)

    _notify(r, f"✅ <b>V4 Enriched</b> ROC=<code>{metrics['roc_auc_oof']:.4f}</code> Top5=<code>{metrics['top5_hit_rate']:.2%}</code>")
    return 0

if __name__ == "__main__":
    main()
