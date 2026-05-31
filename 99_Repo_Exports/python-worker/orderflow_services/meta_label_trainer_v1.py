"""
meta_label_trainer_v1.py — Phase 2.1: Meta-labeling model trainer (timer service).

Reads resolved signal_outcome records (label IS NOT NULL), trains LightGBM
meta-labeling model with purged walk-forward CV, publishes state to Redis.

Runs on a periodic interval (META_LABEL_TRAIN_INTERVAL_SEC, default 86400 = 1 day).
Model is consumed by services/meta_labeling_gate.MetaLabelGate.

SHADOW by default: META_LABEL_GATE_ENABLED=0 means gate uses model for scoring
but never vetoes signals. Enable gate with META_LABEL_GATE_ENABLED=1.

Prerequisites:
  - signal_outcome table populated (Phase 0: SIGNAL_OUTCOME_ENABLED=1)
  - >= CALIBRATION_MIN_SAMPLES resolved records in window

ENV:
  META_LABEL_TRAINER_DB_DSN       = (SO_RESOLVER_DB_DSN or TRADES_DB_DSN)
  META_LABEL_TRAINER_REDIS_URL    = redis://redis-worker-1:6379/0
  META_LABEL_TRAINER_PORT         = 9913
  META_LABEL_TRAIN_INTERVAL_SEC   = 86400         Training interval (1 day default)
  META_LABEL_WINDOW_DAYS          = 30            Lookback window for training data
  META_LABEL_ROW_LIMIT            = 50000         Max rows to load
  CALIBRATION_MIN_SAMPLES         = 200           Min resolved records to train
  CALIBRATION_N_BLOCKS            = 8
  CALIBRATION_EMBARGO_MS          = 600000
  META_LABEL_THR_DEFAULT          = 0.45          Default P(TP) gate threshold

Prometheus metrics (port META_LABEL_TRAINER_PORT):
  meta_label_train_roc_auc        Gauge    OOS ROC-AUC from last training run
  meta_label_train_n_samples      Gauge    Number of records used
  meta_label_train_dsr            Gauge    DSR from last run
  meta_label_train_total          Counter  Training runs attempted
  meta_label_train_success_total  Counter  Successful training runs
  meta_label_train_error_total    Counter  Training errors
"""
from __future__ import annotations

import json
import logging
import os
import time
from typing import Any

log = logging.getLogger("meta_label_trainer")


def _env(k: str, d: str = "") -> str:
    return os.environ.get(k, d)

def _env_int(k: str, d: int) -> int:
    try:
        return int(_env(k, str(d)))
    except Exception:
        return d

def _env_float(k: str, d: float) -> float:
    try:
        return float(_env(k, str(d)))
    except Exception:
        return d

def _env_bool(k: str, d: bool) -> bool:
    raw = _env(k, "")
    if not raw:
        return d
    return raw.strip().lower() in ("1", "true", "yes", "on")


_FETCH_SQL = """
    SELECT symbol, source, decision_time_ms, resolved_time_ms,
           label, COALESCE(realized_r, 0.0) AS realized_r,
           features
    FROM signal_outcome
    WHERE label IS NOT NULL
      AND resolved_time_ms IS NOT NULL
      AND decision_time_ms > %s
    ORDER BY decision_time_ms ASC
    LIMIT %s
"""


def _fetch_rows(conn: Any, window_days: float, row_limit: int) -> list[dict]:
    cutoff_ms = int((time.time() - window_days * 86_400) * 1000)
    with conn.cursor() as cur:
        cur.execute(_FETCH_SQL, (cutoff_ms, row_limit))
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]


def read_ensemble_weights(rc: Any, symbol: str) -> dict[str, float]:
    """Read per-source ensemble weights for a symbol from Redis.

    Returns dict {source: weight} normalised to sum=1, or {} on miss/error.
    Consumers use this as sample_weight during training so that historically
    better-performing sources receive proportionally more influence.

    Key: ensemble:weights:{symbol}  (HASH, set by ensemble_weights_publisher_v1)
    ENV: ENSEMBLE_WEIGHTS_READ_ENABLED=1 to enable (default 0 → returns {})
    """
    if not _env_bool("ENSEMBLE_WEIGHTS_READ_ENABLED", False):
        return {}
    try:
        from core.redis_keys import RedisKeyPrefixes as RK
        key = RK.ENSEMBLE_WEIGHTS_TPL.format(symbol=symbol)
        raw: dict = rc.hgetall(key) or {}
        if not raw:
            return {}
        weights: dict[str, float] = {}
        total = 0.0
        for src, val in raw.items():
            try:
                w = float(val)
                if w > 0:
                    weights[src] = w
                    total += w
            except (TypeError, ValueError):
                pass
        if not weights or total <= 0:
            return {}
        return {src: w / total for src, w in weights.items()}
    except Exception as e:
        log.debug("read_ensemble_weights error: %s", e)
        return {}


def _apply_ensemble_weights(rows: list[dict], weights_by_symbol: dict[str, dict[str, float]]) -> list[float]:
    """Return per-row sample weights from ensemble weights.

    Rows without a matching (symbol, source) weight get weight=1.0.
    Used as sample_weight in LightGBM training.
    """
    sample_weights: list[float] = []
    for row in rows:
        sym = str(row.get("symbol") or "")
        src = str(row.get("source") or "")
        w = weights_by_symbol.get(sym, {}).get(src, 1.0)
        # Clamp to avoid extreme weights distorting training
        sample_weights.append(max(0.1, min(10.0, w)))
    return sample_weights


def main() -> None:
    from prometheus_client import Counter, Gauge, start_http_server

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )

    db_dsn       = _env("META_LABEL_TRAINER_DB_DSN",
                        _env("SO_RESOLVER_DB_DSN", _env("TRADES_DB_DSN", "")))
    redis_url    = _env("META_LABEL_TRAINER_REDIS_URL",
                        _env("REDIS_URL", "redis://redis-worker-1:6379/0"))
    port         = _env_int("META_LABEL_TRAINER_PORT", 9913)
    interval_sec = _env_int("META_LABEL_TRAIN_INTERVAL_SEC", 86_400)
    window_days  = _env_float("META_LABEL_WINDOW_DAYS", 30.0)
    row_limit    = _env_int("META_LABEL_ROW_LIMIT", 50_000)
    min_samples  = _env_int("CALIBRATION_MIN_SAMPLES", 200)
    n_blocks     = _env_int("CALIBRATION_N_BLOCKS", 8)
    embargo_ms   = _env_int("CALIBRATION_EMBARGO_MS", 600_000)
    default_thr  = _env_float("META_LABEL_THR_DEFAULT", 0.45)

    log.info(
        "meta_label_trainer starting | port=%d interval=%ds window=%.0fd",
        port, interval_sec, window_days,
    )

    import redis  # type: ignore
    rc = redis.from_url(redis_url, decode_responses=True)

    start_http_server(port)
    g_auc    = Gauge("meta_label_train_roc_auc",    "OOS ROC-AUC from last training", [])
    g_n      = Gauge("meta_label_train_n_samples",  "Records used in last training",  [])
    g_dsr    = Gauge("meta_label_train_dsr",         "DSR from last training run",     [])
    c_total  = Counter("meta_label_train_total",         "Training runs attempted",  [])
    c_ok     = Counter("meta_label_train_success_total", "Successful training runs", [])
    c_err    = Counter("meta_label_train_error_total",   "Training errors",          [])

    from core.redis_keys import RedisKeyPrefixes as RK

    conn = None
    last_train_ms: int = 0  # track last successful training time

    def _get_conn():
        nonlocal conn
        if conn is None or conn.closed:
            import psycopg2  # type: ignore
            conn = psycopg2.connect(db_dsn)
        return conn

    def _autopilot_training_enabled() -> bool:
        """Check autopilot flag as fallback when normal interval hasn't elapsed."""
        try:
            from orderflow_services.calibration_autopilot_v1 import read_autopilot_flag
            return read_autopilot_flag(rc, "meta_label_training_enabled")
        except Exception:
            return False

    while True:
        time.sleep(interval_sec)

        if not db_dsn:
            log.debug("META_LABEL_TRAINER_DB_DSN not set — skipping")
            continue

        # Autopilot can trigger initial training before first interval elapses.
        # After first successful run, respect normal interval.
        if last_train_ms > 0:
            elapsed_h = (time.time() * 1000 - last_train_ms) / 3_600_000
            if elapsed_h < interval_sec / 3600:
                if not _autopilot_training_enabled():
                    log.debug("meta_label_trainer: interval not elapsed and autopilot not active")
                    continue

        c_total.inc()

        try:
            rows = _fetch_rows(_get_conn(), window_days, row_limit)
        except Exception as e:
            c_err.inc()
            log.warning("meta_label_trainer: fetch error: %s", e)
            conn = None
            continue

        if len(rows) < min_samples:
            log.info(
                "meta_label_trainer: insufficient rows (%d < %d) — skipping",
                len(rows), min_samples,
            )
            continue

        try:
            from calibration.meta_labeling_model import train_meta_labeling_model

            # Build per-row sample weights from ensemble weights (if enabled)
            symbols = list({str(r.get("symbol") or "") for r in rows})
            weights_by_symbol = {
                sym: read_ensemble_weights(rc, sym)
                for sym in symbols
            }
            sample_weights = _apply_ensemble_weights(rows, weights_by_symbol) if any(weights_by_symbol.values()) else None

            state = train_meta_labeling_model(
                rows,
                n_blocks=n_blocks,
                embargo_ms=embargo_ms,
                min_samples=min_samples,
                default_threshold=default_thr,
                sample_weights=sample_weights,
            )
        except Exception as e:
            c_err.inc()
            log.warning("meta_label_trainer: training error: %s", e)
            continue

        if state is None:
            c_err.inc()
            log.warning("meta_label_trainer: training returned None (insufficient positive/negative samples)")
            continue

        try:
            rc.set(RK.META_LABEL_MODEL_STATE, json.dumps(state))
        except Exception as e:
            c_err.inc()
            log.warning("meta_label_trainer: Redis SET error: %s", e)
            continue

        c_ok.inc()
        last_train_ms = int(time.time() * 1000)
        g_auc.set(state.get("roc_auc_oos", 0.0))
        g_n.set(state.get("n_samples", 0))
        g_dsr.set(state.get("dsr", 0.0))

        log.info(
            "meta_label_trainer: trained | n=%d folds=%d auc_oos=%.3f dsr=%.3f",
            state["n_samples"],
            state["n_folds"],
            state["roc_auc_oos"],
            state["dsr"],
        )


if __name__ == "__main__":
    main()
