"""
calibration_autopilot_v1.py — Calibration pipeline auto-switcher.

Polls resolved label count (DB) and model quality (Redis) hourly,
activating calibration stages automatically as data accumulates.

Activation sequence (sticky HSETNX — never deactivated):
  1. resolved labels >= META_LABEL_MIN_SAMPLES (200)
       → flag `meta_label_training_enabled=1`
       → meta_label_trainer_v1 triggers training on next cycle

  2. resolved labels >= PURGED_CV_MIN_SAMPLES (500)
       → flag `purged_cv_enabled=1`
       → purged_cv_validator_v1 activates purged walk-forward guard

  3. model trained (META_LABEL_MODEL_STATE exists) AND
     roc_auc_oos >= AUTOPILOT_MIN_AUC (0.55) AND dsr >= 0
       → flag `meta_label_gate_enabled=1`
       → meta_labeling_gate switches from SHADOW_VETO → VETO

  4. gate active >= AUTOPILOT_KELLY_GATE_MIN_HOURS (48h) AND
     roc_auc_oos >= AUTOPILOT_KELLY_MIN_AUC (0.58)
       → flag `kelly_sizing_enabled=1`
       → kelly_sizer_v2 switches from shadow → enforce

  5. resolved labels >= AUTOPILOT_ADAPTIVE_TTL_MIN_SAMPLES (300)
       → flag `adaptive_ttl_enabled=1`
       → adaptive_ttl_publisher_v1 publishes barrier recs to Redis

  6. resolved labels >= AUTOPILOT_ENSEMBLE_MIN_SAMPLES (500)
       AND at least 2 distinct sources observed
       → flag `ensemble_weights_enabled=1`
       → ensemble_weights_publisher_v1 publishes per-symbol HSET

Sticky: HSETNX (set-if-not-exists). No flag is ever cleared automatically.
Manual ENV overrides always take priority over autopilot flags (consumers
check ENV first, then autopilot state as fallback).

ENV:
  CALIBRATION_AUTOPILOT_ENABLED      = 0     master switch (0=shadow: count+check, no writes)
  CALIBRATION_AUTOPILOT_DB_DSN       = (TRADES_DB_DSN)
  CALIBRATION_AUTOPILOT_REDIS_URL    = redis://redis-worker-1:6379/0
  CALIBRATION_AUTOPILOT_PORT         = 9917
  CALIBRATION_AUTOPILOT_INTERVAL_SEC = 3600
  META_LABEL_MIN_SAMPLES             = 200
  PURGED_CV_MIN_SAMPLES              = 500
  AUTOPILOT_MIN_AUC                  = 0.55   min OOS ROC-AUC to activate gate
  AUTOPILOT_KELLY_MIN_AUC            = 0.58   min OOS ROC-AUC to activate Kelly
  AUTOPILOT_KELLY_GATE_MIN_HOURS     = 48     gate must be active for this long before Kelly
  AUTOPILOT_ADAPTIVE_TTL_MIN_SAMPLES = 300    resolved labels before adaptive TTL publish
  AUTOPILOT_ENSEMBLE_MIN_SAMPLES     = 500    resolved labels before ensemble publish
  AUTOPILOT_ENSEMBLE_MIN_SOURCES     = 2      distinct sources required

Prometheus (port CALIBRATION_AUTOPILOT_PORT):
  calibration_autopilot_resolved_labels_total   Gauge
  calibration_autopilot_flag_active             Gauge  {flag}
  calibration_autopilot_flag_activated_total    Counter {flag}
  calibration_autopilot_model_auc               Gauge
  calibration_autopilot_check_total             Counter {result}
"""
from __future__ import annotations

import json
import logging
import os
import time
from typing import Any

log = logging.getLogger("calibration_autopilot")

# ─── Redis key ────────────────────────────────────────────────────────────────
_AUTOPILOT_KEY = "calibration:autopilot:state"
_MODEL_KEY     = "meta_label_model:state"

# ─── Flag names ──────────────────────────────────────────────────────────────
FLAG_META_TRAIN   = "meta_label_training_enabled"   # phase 1: trigger training
FLAG_PURGED_CV    = "purged_cv_enabled"             # phase 2: purged CV guard
FLAG_META_GATE    = "meta_label_gate_enabled"       # phase 3: gate enforcement
FLAG_KELLY        = "kelly_sizing_enabled"          # phase 4: Kelly sizing
FLAG_ADAPTIVE_TTL = "adaptive_ttl_enabled"          # phase 5: barrier publisher
FLAG_ENSEMBLE     = "ensemble_weights_enabled"      # phase 6: ensemble publisher

ALL_FLAGS = (
    FLAG_META_TRAIN, FLAG_PURGED_CV, FLAG_META_GATE,
    FLAG_KELLY, FLAG_ADAPTIVE_TTL, FLAG_ENSEMBLE,
)


# ─── ENV helpers ─────────────────────────────────────────────────────────────

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


# ─── SQL ─────────────────────────────────────────────────────────────────────

_COUNT_SQL = "SELECT COUNT(*) FROM signal_outcome WHERE label IS NOT NULL"
_DISTINCT_SOURCES_SQL = (
    "SELECT COUNT(DISTINCT source) FROM signal_outcome WHERE label IS NOT NULL"
)


def _count_resolved(conn: Any) -> int:
    with conn.cursor() as cur:
        cur.execute(_COUNT_SQL)
        row = cur.fetchone()
        return int(row[0]) if row else 0


def _count_distinct_sources(conn: Any) -> int:
    with conn.cursor() as cur:
        cur.execute(_DISTINCT_SOURCES_SQL)
        row = cur.fetchone()
        return int(row[0]) if row else 0


# ─── Flag reader (imported by purged_cv_validator_v1 and meta_label_trainer_v1) ──

def read_autopilot_flag(rc: Any, flag: str) -> bool:
    """Return True if the given autopilot flag is active in Redis.

    Fail-open for training flags (False = not ready yet → don't train prematurely).
    Fail-open for gate/kelly flags as well (False = shadow mode, safe default).
    """
    try:
        val = rc.hget(_AUTOPILOT_KEY, flag)
        return str(val).strip() == "1"
    except Exception:
        return False


def read_autopilot_state(rc: Any) -> dict:
    """Return the full autopilot HASH as a dict. Empty dict on error."""
    try:
        return rc.hgetall(_AUTOPILOT_KEY) or {}
    except Exception:
        return {}


# ─── Model quality reader ────────────────────────────────────────────────────

def _read_model_quality(rc: Any) -> dict | None:
    """Read META_LABEL_MODEL_STATE from Redis. Returns None if absent/invalid."""
    try:
        raw = rc.get(_MODEL_KEY)
        if not raw:
            return None
        state = json.loads(str(raw))
        auc = float(state.get("roc_auc_oos", 0.0) or 0.0)
        dsr = float(state.get("dsr", -1.0) or -1.0)
        ts  = int(state.get("ts_ms", 0) or 0)
        n   = int(state.get("n_samples", 0) or 0)
        return {"auc": auc, "dsr": dsr, "ts_ms": ts, "n_samples": n}
    except Exception as e:
        log.debug("calibration_autopilot: model read error: %s", e)
        return None


# ─── Flag writer ─────────────────────────────────────────────────────────────

def _activate_flag(
    rc: Any,
    flag: str,
    now_ms: int,
    c_activated: Any,
    g_active: Any,
) -> bool:
    """HSETNX the flag. Returns True if newly activated (0→1)."""
    try:
        newly_set = rc.hsetnx(_AUTOPILOT_KEY, flag, "1")
        if newly_set:
            rc.hset(_AUTOPILOT_KEY, f"activated_at_{flag}_ms", str(now_ms))
            log.info(
                "calibration_autopilot: FLAG ACTIVATED flag=%s at_ms=%d", flag, now_ms
            )
            c_activated.labels(flag=flag).inc()
            g_active.labels(flag=flag).set(1)
            return True
        return False
    except Exception as e:
        log.warning("calibration_autopilot: HSETNX error flag=%s err=%s", flag, e)
        return False


def _sync_gauge(rc: Any, flag: str, g_active: Any) -> None:
    """Sync Prometheus gauge from Redis (idempotent on already-active flags)."""
    if read_autopilot_flag(rc, flag):
        g_active.labels(flag=flag).set(1)


# ─── Main loop ────────────────────────────────────────────────────────────────

def main() -> None:
    from prometheus_client import Counter, Gauge, start_http_server

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )

    enabled           = _env_bool("CALIBRATION_AUTOPILOT_ENABLED", False)
    db_dsn            = _env("CALIBRATION_AUTOPILOT_DB_DSN", _env("TRADES_DB_DSN", ""))
    redis_url         = _env("CALIBRATION_AUTOPILOT_REDIS_URL",
                             _env("REDIS_URL", "redis://redis-worker-1:6379/0"))
    port              = _env_int("CALIBRATION_AUTOPILOT_PORT", 9917)
    interval_sec      = _env_int("CALIBRATION_AUTOPILOT_INTERVAL_SEC", 3600)
    thr_meta_train    = _env_int("META_LABEL_MIN_SAMPLES", 200)
    thr_purged_cv     = _env_int("PURGED_CV_MIN_SAMPLES", 500)
    min_auc_gate      = _env_float("AUTOPILOT_MIN_AUC", 0.55)
    min_auc_kelly     = _env_float("AUTOPILOT_KELLY_MIN_AUC", 0.58)
    kelly_gate_min_h  = _env_float("AUTOPILOT_KELLY_GATE_MIN_HOURS", 48.0)
    thr_adaptive_ttl  = _env_int("AUTOPILOT_ADAPTIVE_TTL_MIN_SAMPLES", 300)
    thr_ensemble      = _env_int("AUTOPILOT_ENSEMBLE_MIN_SAMPLES", 500)
    thr_ens_sources   = _env_int("AUTOPILOT_ENSEMBLE_MIN_SOURCES", 2)

    # Manual ENV overrides — autopilot won't fight them
    manual_cv          = _env("CALIBRATION_VALIDATION", "").strip().lower()
    manual_gate        = _env_bool("META_LABEL_GATE_ENABLED", False)
    manual_kelly       = _env_bool("KELLY_SIZING_ENABLED", False)
    manual_adaptive    = _env_bool("ADAPTIVE_TTL_ENABLED", False)
    manual_ensemble    = _env_bool("ENSEMBLE_WEIGHTS_ENABLED", False)

    log.info(
        "calibration_autopilot starting | enabled=%s port=%d interval=%ds "
        "thr_train=%d thr_cv=%d min_auc_gate=%.2f min_auc_kelly=%.2f kelly_gate_min_h=%.0f",
        enabled, port, interval_sec,
        thr_meta_train, thr_purged_cv, min_auc_gate, min_auc_kelly, kelly_gate_min_h,
    )
    if manual_cv == "purged_walkforward":
        log.info("calibration_autopilot: CALIBRATION_VALIDATION manually set — "
                 "purged_cv flag tracking only")
    if manual_gate:
        log.info("calibration_autopilot: META_LABEL_GATE_ENABLED manually set — "
                 "meta_gate flag tracking only")
    if manual_kelly:
        log.info("calibration_autopilot: KELLY_SIZING_ENABLED manually set — "
                 "kelly flag tracking only")

    import redis as redis_lib  # type: ignore
    rc = redis_lib.from_url(redis_url, decode_responses=True)

    start_http_server(port)

    g_resolved  = Gauge("calibration_autopilot_resolved_labels_total",
                        "Current resolved signal_outcome label count")
    g_active    = Gauge("calibration_autopilot_flag_active",
                        "Autopilot flag active (1=active)", ["flag"])
    g_model_auc = Gauge("calibration_autopilot_model_auc",
                        "OOS ROC-AUC from latest meta-label model")
    c_activated = Counter("calibration_autopilot_flag_activated_total",
                          "New flag activations", ["flag"])
    c_check     = Counter("calibration_autopilot_check_total",
                          "Check iterations", ["result"])

    for flag in ALL_FLAGS:
        g_active.labels(flag=flag).set(0)

    conn = None

    def _get_conn():
        nonlocal conn
        if conn is None or conn.closed:
            import psycopg2  # type: ignore
            conn = psycopg2.connect(db_dsn)
            conn.autocommit = True
        return conn

    while True:
        time.sleep(interval_sec)

        if not db_dsn:
            log.debug("CALIBRATION_AUTOPILOT_DB_DSN not set — skipping")
            c_check.labels(result="skip_no_dsn").inc()
            continue

        # ── 1. Count resolved labels + distinct sources ───────────────────
        try:
            n_resolved      = _count_resolved(_get_conn())
            n_sources_dist  = _count_distinct_sources(_get_conn())
        except Exception as e:
            log.warning("calibration_autopilot: DB error: %s", e)
            conn = None
            c_check.labels(result="error").inc()
            continue

        g_resolved.set(n_resolved)
        now_ms = int(time.time() * 1000)

        # ── 2. Read model quality from Redis ─────────────────────────────
        model = _read_model_quality(rc)
        if model:
            g_model_auc.set(model["auc"])

        # ── 3. Common: update count/timestamp in Redis hash ───────────────
        try:
            rc.hset(_AUTOPILOT_KEY, "resolved_labels_total", str(n_resolved))
            rc.hset(_AUTOPILOT_KEY, "last_check_ms", str(now_ms))
        except Exception as e:
            log.warning("calibration_autopilot: Redis HSET error: %s", e)

        if not enabled:
            # Shadow: sync gauges from Redis, log projection
            for flag in ALL_FLAGS:
                _sync_gauge(rc, flag, g_active)
            log.info(
                "calibration_autopilot SHADOW | resolved=%d model_auc=%s "
                "thr_train=%d thr_cv=%d",
                n_resolved,
                f"{model['auc']:.3f}" if model else "none",
                thr_meta_train, thr_purged_cv,
            )
            c_check.labels(result="shadow").inc()
            continue

        # ── 4. Enforce: activate flags in sequence ────────────────────────

        # Flag 1: meta_label_training_enabled
        if n_resolved >= thr_meta_train:
            if not _activate_flag(rc, FLAG_META_TRAIN, now_ms, c_activated, g_active):
                _sync_gauge(rc, FLAG_META_TRAIN, g_active)

        # Flag 2: purged_cv_enabled
        if n_resolved >= thr_purged_cv:
            if manual_cv == "purged_walkforward":
                _sync_gauge(rc, FLAG_PURGED_CV, g_active)
            elif not _activate_flag(rc, FLAG_PURGED_CV, now_ms, c_activated, g_active):
                _sync_gauge(rc, FLAG_PURGED_CV, g_active)

        # Flag 3: meta_label_gate_enabled (requires trained model + AUC quality)
        if model and model["auc"] >= min_auc_gate and model["dsr"] >= 0:
            if manual_gate:
                _sync_gauge(rc, FLAG_META_GATE, g_active)
            elif not _activate_flag(rc, FLAG_META_GATE, now_ms, c_activated, g_active):
                _sync_gauge(rc, FLAG_META_GATE, g_active)

        # Flag 4: kelly_sizing_enabled
        # Requires: gate active for >= kelly_gate_min_h AND AUC above Kelly threshold
        gate_activated_ms_str = None
        try:
            gate_activated_ms_str = rc.hget(
                _AUTOPILOT_KEY, f"activated_at_{FLAG_META_GATE}_ms"
            )
        except Exception:
            pass

        gate_active_h = 0.0
        if gate_activated_ms_str:
            try:
                gate_active_h = (now_ms - int(str(gate_activated_ms_str))) / 3_600_000
            except Exception:
                pass

        kelly_quality_ok = (
            model is not None
            and model["auc"] >= min_auc_kelly
            and model["dsr"] >= 0
        )
        gate_mature = (
            gate_active_h >= kelly_gate_min_h
            or read_autopilot_flag(rc, FLAG_META_GATE)  # already active from prior run
        ) and gate_active_h >= kelly_gate_min_h

        if kelly_quality_ok and gate_mature:
            if manual_kelly:
                _sync_gauge(rc, FLAG_KELLY, g_active)
            elif not _activate_flag(rc, FLAG_KELLY, now_ms, c_activated, g_active):
                _sync_gauge(rc, FLAG_KELLY, g_active)

        # Flag 5: adaptive_ttl_enabled — pure data threshold
        if n_resolved >= thr_adaptive_ttl:
            if manual_adaptive:
                _sync_gauge(rc, FLAG_ADAPTIVE_TTL, g_active)
            elif not _activate_flag(rc, FLAG_ADAPTIVE_TTL, now_ms, c_activated, g_active):
                _sync_gauge(rc, FLAG_ADAPTIVE_TTL, g_active)

        # Flag 6: ensemble_weights_enabled — data + source-diversity threshold
        if n_resolved >= thr_ensemble and n_sources_dist >= thr_ens_sources:
            if manual_ensemble:
                _sync_gauge(rc, FLAG_ENSEMBLE, g_active)
            elif not _activate_flag(rc, FLAG_ENSEMBLE, now_ms, c_activated, g_active):
                _sync_gauge(rc, FLAG_ENSEMBLE, g_active)

        log.info(
            "calibration_autopilot: check done | resolved=%d sources=%d model_auc=%s "
            "gate_active_h=%.1f flags: train=%s cv=%s gate=%s kelly=%s "
            "adaptive_ttl=%s ensemble=%s",
            n_resolved, n_sources_dist,
            f"{model['auc']:.3f}" if model else "none",
            gate_active_h,
            read_autopilot_flag(rc, FLAG_META_TRAIN),
            read_autopilot_flag(rc, FLAG_PURGED_CV),
            read_autopilot_flag(rc, FLAG_META_GATE),
            read_autopilot_flag(rc, FLAG_KELLY),
            read_autopilot_flag(rc, FLAG_ADAPTIVE_TTL),
            read_autopilot_flag(rc, FLAG_ENSEMBLE),
        )
        c_check.labels(result="ok").inc()


if __name__ == "__main__":
    main()
