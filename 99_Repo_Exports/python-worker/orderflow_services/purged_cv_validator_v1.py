"""
purged_cv_validator_v1.py — Phase 1: Purged walk-forward CV guard service.

Reads resolved records from signal_outcome (label IS NOT NULL) and periodically
validates calibration quality via purged walk-forward CV + DSR + PBO guards.
Publishes guard state to Redis key `calibration:purged_cv:state`.

Consumers (pre_publish_gate_calibrator, tb_cost_bps_calibrator, p_edge_calibrator)
optionally check this guard before promoting thresholds when
CALIBRATION_VALIDATION=purged_walkforward is set.

Guard is fail-open: if signal_outcome has < CALIBRATION_MIN_SAMPLES resolved records,
the guard passes unconditionally (insufficient_data reason).

Master switch: CALIBRATION_VALIDATION=purged_walkforward (empty = disabled → always pass).

ENV:
  CALIBRATION_VALIDATION            = ""              set to "purged_walkforward" to enable
  PURGED_CV_VALIDATOR_DB_DSN        = (SO_RESOLVER_DB_DSN or TRADES_DB_DSN)
  PURGED_CV_VALIDATOR_REDIS_URL     = redis://redis-worker-1:6379/0
  PURGED_CV_VALIDATOR_PORT          = 9912
  PURGED_CV_VALIDATOR_INTERVAL_SEC  = 3600
  PURGED_CV_VALIDATOR_WINDOW_DAYS   = 30
  PURGED_CV_VALIDATOR_ROW_LIMIT     = 50000
  CALIBRATION_MIN_SAMPLES           = 500
  CALIBRATION_N_BLOCKS              = 8
  CALIBRATION_EMBARGO_MS            = 600000
  CALIBRATION_MIN_DSR               = 0.0
  CALIBRATION_MAX_PBO               = 0.5

Prometheus metrics (port PURGED_CV_VALIDATOR_PORT):
  calibration_pbo_last{symbol, source}          Gauge
  calibration_dsr_last{symbol, source}          Gauge
  calibration_guard_pass{symbol, source}        Gauge  (1=pass, 0=fail)
  calibration_n_resolved{symbol, source}        Gauge
  calibration_guard_run_total                   Counter
  calibration_guard_error_total                 Counter
"""
from __future__ import annotations

import json
import logging
import math
import os
import time
from collections import defaultdict
from typing import Any

import numpy as np

log = logging.getLogger("purged_cv_validator")


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

def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        f = float(v)
        return f if math.isfinite(f) else default
    except (TypeError, ValueError):
        return default


# ─── DB query ────────────────────────────────────────────────────────────────

_FETCH_RESOLVED_SQL = """
    SELECT symbol, source, decision_time_ms, resolved_time_ms,
           COALESCE(realized_r, 0.0) AS realized_r
    FROM signal_outcome
    WHERE label IS NOT NULL
      AND resolved_time_ms IS NOT NULL
      AND decision_time_ms > %s
    ORDER BY decision_time_ms ASC
    LIMIT %s
"""


def fetch_resolved(conn: Any, window_days: float, row_limit: int) -> list[dict]:
    """Load resolved signal_outcome records within the validation window."""
    cutoff_ms = int((time.time() - window_days * 86_400) * 1000)
    with conn.cursor() as cur:
        cur.execute(_FETCH_RESOLVED_SQL, (cutoff_ms, row_limit))
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]


# ─── Validation logic ─────────────────────────────────────────────────────────

def _validate_group(
    rows: list[dict],
    n_blocks: int,
    embargo_ms: int,
    min_samples: int,
    min_dsr: float,
    max_pbo: float,
    symbol: str,
    source: str,
) -> dict:
    """
    Run purged walk-forward CV on a (symbol, source) group.

    Returns a guard dict with keys:
      passed, reason, n, dsr, pbo, dsr_ok, pbo_ok, n_folds
    """
    from calibration.purged_cv import purged_walkforward, check_calibration_guards

    n = len(rows)
    if n < min_samples:
        return {
            "passed": True,
            "reason": "insufficient_samples",
            "n": n, "dsr": 0.0, "pbo": 0.0, "dsr_ok": True, "pbo_ok": True, "n_folds": 0,
        }

    d_ms = np.array([r["decision_time_ms"] for r in rows], dtype=float)
    r_ms = np.array([r["resolved_time_ms"] for r in rows], dtype=float)
    realized_r = np.array([_safe_float(r.get("realized_r"), 0.0) for r in rows], dtype=float)

    folds = list(purged_walkforward(d_ms, r_ms, n_blocks=n_blocks, embargo_ms=embargo_ms))
    if len(folds) < 2:
        return {
            "passed": True,
            "reason": "too_few_folds",
            "n": n, "dsr": 0.0, "pbo": 0.0, "dsr_ok": True, "pbo_ok": True, "n_folds": len(folds),
        }

    # OOS fold returns = mean realized_r on each test fold
    fold_rets = [float(np.mean(realized_r[test_idx])) if len(test_idx) > 0 else 0.0
                 for _, test_idx in folds]

    ret_std = float(np.std(fold_rets)) if len(fold_rets) > 1 else 1e-9
    sr = float(np.mean(fold_rets)) / (ret_std + 1e-9) if ret_std > 0 else 0.0

    passed, details = check_calibration_guards(
        sr=sr,
        n_trials=len(folds),
        skew=0.0,
        kurt=0.0,
        n_obs=n,
        fold_returns=None,   # PBO across sources computed by caller
        min_dsr=min_dsr,
        max_pbo=max_pbo,
    )

    return {
        "passed": passed,
        "reason": "guard_evaluated",
        "n": n,
        "dsr": details["dsr"],
        "pbo": 0.0,   # source-level PBO not applicable (single source)
        "dsr_ok": details["dsr_ok"],
        "pbo_ok": True,
        "n_folds": len(folds),
        "sr": round(sr, 4),
    }


def _compute_cross_source_pbo(
    rows_by_source: dict[str, list[dict]],
    n_blocks: int,
    embargo_ms: int,
    min_samples: int,
    max_pbo: float,
) -> tuple[float, bool]:
    """
    Compute PBO across all sources for a symbol using a shared fold grid.
    Returns (pbo, pbo_ok).
    """
    from calibration.purged_cv import purged_walkforward, pbo_estimate

    sources = sorted(s for s, rows in rows_by_source.items() if len(rows) >= min_samples)
    if len(sources) < 2:
        return 0.0, True  # no cross-source comparison possible

    # Combined fold grid from all sources together
    all_rows = [r for src in sources for r in rows_by_source[src]]
    all_rows.sort(key=lambda r: r["decision_time_ms"])

    d_ms = np.array([r["decision_time_ms"] for r in all_rows], dtype=float)
    r_ms = np.array([r["resolved_time_ms"] for r in all_rows], dtype=float)

    folds = list(purged_walkforward(d_ms, r_ms, n_blocks=n_blocks, embargo_ms=embargo_ms))
    if len(folds) < 2:
        return 0.0, True

    # Build source index lookup
    source_of = [r["source"] for r in all_rows]
    r_arr = np.array([_safe_float(r.get("realized_r"), 0.0) for r in all_rows], dtype=float)

    # fold_returns[fold_idx] = list of per-source avg realized_r
    fold_returns: list[list[float]] = []
    for _, test_idx in folds:
        if len(test_idx) == 0:
            continue
        source_rets = []
        for src in sources:
            mask = [i for i in test_idx if source_of[i] == src]
            source_rets.append(float(np.mean(r_arr[mask])) if mask else 0.0)
        fold_returns.append(source_rets)

    pbo = pbo_estimate(fold_returns)
    return pbo, pbo <= max_pbo


def run_validation(
    rows: list[dict],
    n_blocks: int,
    embargo_ms: int,
    min_samples: int,
    min_dsr: float,
    max_pbo: float,
) -> dict:
    """
    Run full purged CV validation across all (symbol, source) groups.
    Returns guard state dict suitable for publishing to Redis.
    """
    # Group by (symbol, source)
    by_symbol_source: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for r in rows:
        sym = r.get("symbol") or "*"
        src = r.get("source") or "*"
        by_symbol_source[(sym, src)].append(r)

    # Per-symbol: group by source for cross-source PBO
    by_symbol: dict[str, dict[str, list[dict]]] = defaultdict(lambda: defaultdict(list))
    for (sym, src), group_rows in by_symbol_source.items():
        by_symbol[sym][src] = group_rows

    guards: dict[str, dict] = {}
    pbo_by_symbol: dict[str, tuple[float, bool]] = {}

    # Per-symbol cross-source PBO
    for sym, src_map in by_symbol.items():
        pbo, pbo_ok = _compute_cross_source_pbo(
            src_map, n_blocks, embargo_ms, min_samples, max_pbo,
        )
        pbo_by_symbol[sym] = (pbo, pbo_ok)

    # Per-(symbol, source) DSR
    for (sym, src), group_rows in by_symbol_source.items():
        guard = _validate_group(
            group_rows, n_blocks, embargo_ms, min_samples, min_dsr, max_pbo, sym, src,
        )
        # Overlay cross-source PBO
        pbo, pbo_ok = pbo_by_symbol.get(sym, (0.0, True))
        guard["pbo"] = round(pbo, 4)
        guard["pbo_ok"] = pbo_ok
        guard["passed"] = guard["dsr_ok"] and pbo_ok
        guards[f"{sym}:{src}"] = guard

    overall_passed = all(g.get("passed", True) for g in guards.values()) if guards else True
    n_total = sum(g.get("n", 0) for g in guards.values())

    return {
        "schema_version": 1,
        "ts_ms": int(time.time() * 1000),
        "n_blocks": n_blocks,
        "embargo_ms": embargo_ms,
        "min_dsr": min_dsr,
        "max_pbo": max_pbo,
        "min_samples": min_samples,
        "n_total": n_total,
        "n_groups": len(guards),
        "groups": guards,
        "overall_passed": overall_passed,
    }


# ─── Guard reader (used by other calibrators) ─────────────────────────────────

def read_guard(rc: Any, symbol: str, source: str = "*") -> bool:
    """
    Read purged CV guard for a (symbol, source) pair.
    Returns True (pass) unless CALIBRATION_VALIDATION=purged_walkforward AND guard explicitly fails.
    Fail-open: returns True on any error or missing data.
    """
    manual_mode = _env("CALIBRATION_VALIDATION", "").strip().lower()
    if manual_mode != "purged_walkforward":
        # Check autopilot flag as fallback when ENV is not set manually
        try:
            from orderflow_services.calibration_autopilot_v1 import read_autopilot_flag
            if not read_autopilot_flag(rc, "purged_cv_enabled"):
                return True  # autopilot not yet activated → fail-open
        except Exception:
            return True  # import error → fail-open
    try:
        from core.redis_keys import RedisKeyPrefixes as RK
        raw = rc.get(RK.CALIBRATION_PURGED_CV_STATE)
        if not raw:
            return True  # no data yet → fail-open
        state = json.loads(str(raw))
        key = f"{symbol}:{source}"
        guard = state.get("groups", {}).get(key)
        if guard is None:
            # Try wildcard source
            guard = state.get("groups", {}).get(f"{symbol}:*")
        if guard is None:
            return True  # unknown group → fail-open
        return bool(guard.get("passed", True))
    except Exception:
        return True  # fail-open on error


# ─── Main service ─────────────────────────────────────────────────────────────

def main() -> None:
    from prometheus_client import Counter, Gauge, start_http_server

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )

    validation_mode = _env("CALIBRATION_VALIDATION", "").strip().lower()
    db_dsn          = _env("PURGED_CV_VALIDATOR_DB_DSN",
                           _env("SO_RESOLVER_DB_DSN", _env("TRADES_DB_DSN", "")))
    redis_url       = _env("PURGED_CV_VALIDATOR_REDIS_URL",
                           _env("REDIS_URL", "redis://redis-worker-1:6379/0"))
    port            = _env_int("PURGED_CV_VALIDATOR_PORT", 9912)
    interval_sec    = _env_int("PURGED_CV_VALIDATOR_INTERVAL_SEC", 3600)
    window_days     = _env_float("PURGED_CV_VALIDATOR_WINDOW_DAYS", 30.0)
    row_limit       = _env_int("PURGED_CV_VALIDATOR_ROW_LIMIT", 50_000)
    min_samples     = _env_int("CALIBRATION_MIN_SAMPLES", 500)
    n_blocks        = _env_int("CALIBRATION_N_BLOCKS", 8)
    embargo_ms      = _env_int("CALIBRATION_EMBARGO_MS", 600_000)
    min_dsr         = _env_float("CALIBRATION_MIN_DSR", 0.0)
    max_pbo         = _env_float("CALIBRATION_MAX_PBO", 0.5)

    log.info(
        "purged_cv_validator starting | mode=%s port=%d interval=%ds n_blocks=%d embargo_ms=%d",
        validation_mode or "disabled", port, interval_sec, n_blocks, embargo_ms,
    )

    import redis  # type: ignore
    rc = redis.from_url(redis_url, decode_responses=True)

    start_http_server(port)
    g_pbo   = Gauge("calibration_pbo_last",      "Last PBO from purged CV",     ["symbol", "source"])
    g_dsr   = Gauge("calibration_dsr_last",      "Last DSR from purged CV",     ["symbol", "source"])
    g_pass  = Gauge("calibration_guard_pass",    "Guard pass (1=pass,0=fail)",  ["symbol", "source"])
    g_n     = Gauge("calibration_n_resolved",    "Resolved records in group",   ["symbol", "source"])
    c_run   = Counter("calibration_guard_run_total",   "Validation runs",  [])
    c_err   = Counter("calibration_guard_error_total", "Validation errors", [])

    from core.redis_keys import RedisKeyPrefixes as RK

    conn = None

    def _get_conn():
        nonlocal conn
        if conn is None or conn.closed:
            import psycopg2  # type: ignore
            conn = psycopg2.connect(db_dsn)
        return conn

    while True:
        time.sleep(interval_sec)

        if not db_dsn:
            log.debug("PURGED_CV_VALIDATOR_DB_DSN not set — skipping")
            continue

        try:
            db_conn = _get_conn()
            rows = fetch_resolved(db_conn, window_days, row_limit)
        except Exception as e:
            c_err.inc()
            log.warning("purged_cv_validator fetch error: %s", e)
            conn = None
            continue

        if not rows:
            log.debug("No resolved signal_outcome records found — guard state unchanged")
            continue

        try:
            state = run_validation(
                rows,
                n_blocks=n_blocks,
                embargo_ms=embargo_ms,
                min_samples=min_samples,
                min_dsr=min_dsr,
                max_pbo=max_pbo,
            )
        except Exception as e:
            c_err.inc()
            log.warning("purged_cv_validator run_validation error: %s", e)
            continue

        c_run.inc()

        # Publish to Redis
        try:
            rc.set(RK.CALIBRATION_PURGED_CV_STATE, json.dumps(state))
        except Exception as e:
            log.warning("purged_cv_validator Redis SET error: %s", e)

        # Emit Prometheus metrics per group
        for group_key, guard in state.get("groups", {}).items():
            parts = group_key.split(":", 1)
            sym = parts[0] if parts else "*"
            src = parts[1] if len(parts) > 1 else "*"
            g_pbo.labels(symbol=sym, source=src).set(guard.get("pbo", 0.0))
            g_dsr.labels(symbol=sym, source=src).set(guard.get("dsr", 0.0))
            g_pass.labels(symbol=sym, source=src).set(1.0 if guard.get("passed", True) else 0.0)
            g_n.labels(symbol=sym, source=src).set(guard.get("n", 0))

        n_fail = sum(1 for g in state.get("groups", {}).values() if not g.get("passed", True))
        log.info(
            "purged_cv_validator: %d groups | %d fail | overall_passed=%s | n_records=%d",
            state["n_groups"], n_fail, state["overall_passed"], state["n_total"],
        )


if __name__ == "__main__":
    main()
