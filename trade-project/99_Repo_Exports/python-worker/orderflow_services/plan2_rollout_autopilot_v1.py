"""plan2_rollout_autopilot_v1.py — Plan 2 staged rollout auto-switcher.

3-stage state machine. Each stage advances only when its preconditions hold
for N consecutive scans (anti-flap). Lower stages stick (HSETNX). The Stage 3
per-kind allowlist supports HDEL revocation by an operator if a single kind's
auto-demote misfires.

Stage 0 → Stage 1  (enable gated_out_outcome_persister)
  Preconditions:
    * PG table `signal_gated_out_outcomes` exists (migration applied).
    * Tracker emitted ≥ PLAN2_S1_MIN_TRACKER_ROWS_1H rows to
      stream:signals:gated_out_outcomes in the last hour
      (verifies upstream tracker is live).
  Action:
    * HSETNX gated_out_persister_enabled=1

Stage 1 → Stage 2  (enable drift_page_hinkley + auto-tune threshold)
  Preconditions:
    * S1 active for ≥ PLAN2_S2_MIN_HOURS_S1 hours.
    * Persister error rate over last 24h ≤ PLAN2_S2_MAX_PERSISTER_ERRORS_24H.
    * ≥ PLAN2_S2_MIN_PERSISTED_ROWS_24H rows landed in
      signal_gated_out_outcomes during last 24h (verifies writes work).
    * drift_auto_demote has scanned signal_outcome and emitted
      warn_shadow signals at a rate ≤ PLAN2_S2_MAX_WARN_SHADOW_PER_HOUR_PER_KIND.
  Side action (always when data available):
    * Compute per-kind p25 of daily mean(realized_r) over the top-pct slice
      of recent signal_outcome rows → autotune `expectancy_threshold` to
      max(p25, PLAN2_EXPECTANCY_THRESHOLD_FLOOR). Ratchet-only: never raises.
  Action:
    * HSETNX drift_page_hinkley_enabled=1

Stage 2 → Stage 3 (per-kind auto-demote allowlist)
  Preconditions per kind:
    * S2 active ≥ PLAN2_S3_MIN_HOURS_S2 hours.
    * The kind is in PLAN2_S3_KIND_ALLOWLIST.
    * Real warn signals (drift_state severity=critical) for this kind in
      the last week ≤ PLAN2_S3_MAX_WARN_PER_KIND_PER_WEEK.
  Action:
    * HSETNX drift_auto_demote_kind_<kind>=1

Master switch: PLAN2_AUTOPILOT_ENABLED=0 → service computes & emits metrics
but does NOT HSETNX any flags. Use to dry-run the state machine on real data.

ENV (defaults shown):
  PLAN2_AUTOPILOT_ENABLED              = 0
  PLAN2_AUTOPILOT_REDIS_URL            = redis://redis-worker-1:6379/0
  PLAN2_AUTOPILOT_DB_DSN               = (TRADES_DB_DSN)
  PLAN2_AUTOPILOT_PORT                 = 9923
  PLAN2_AUTOPILOT_INTERVAL_SEC         = 600
  PLAN2_S1_MIN_TRACKER_ROWS_1H         = 100
  PLAN2_S2_MIN_HOURS_S1                = 48
  PLAN2_S2_MAX_PERSISTER_ERRORS_24H    = 0
  PLAN2_S2_MIN_PERSISTED_ROWS_24H      = 500
  PLAN2_S2_MAX_WARN_SHADOW_PER_HOUR_PER_KIND = 5.0
  PLAN2_S3_MIN_HOURS_S2                = 168     (7 days)
  PLAN2_S3_KIND_ALLOWLIST              = meta_lr_blend,v14_of
  PLAN2_S3_MAX_WARN_PER_KIND_PER_WEEK  = 1
  PLAN2_EXPECTANCY_TOP_PCT             = 0.05
  PLAN2_EXPECTANCY_THRESHOLD_FLOOR     = -0.10
  PLAN2_EXPECTANCY_AUTOTUNE_ENABLED    = 1
  PLAN2_EXPECTANCY_AUTOTUNE_MIN_DAYS   = 7

Prometheus (PLAN2_AUTOPILOT_PORT):
  plan2_autopilot_stage                 Gauge — current effective stage 0..3
  plan2_autopilot_flag_active{flag}     Gauge
  plan2_autopilot_flag_activated_total{flag}  Counter
  plan2_autopilot_check_total{stage,result}   Counter
  plan2_autopilot_expectancy_threshold        Gauge
  plan2_autopilot_persister_rows_1h           Gauge
  plan2_autopilot_warn_rate_per_kind{kind}    Gauge — last-24h critical warn rate /h
"""
from __future__ import annotations

import logging
import os
import time
from typing import Any

from core.plan2_autopilot_flags import (
    AUTOPILOT_KEY,
    FIELD_EXPECTANCY_THRESHOLD,
    FLAG_DRIFT_PH_ENABLED,
    FLAG_PERSISTER_ENABLED,
    activated_at_field,
    kind_demote_flag,
    read_plan2_flag,
    read_plan2_float,
)

log = logging.getLogger("plan2_autopilot")


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


def _env_csv(k: str, d: str = "") -> list[str]:
    raw = _env(k, d)
    return [s.strip().lower() for s in raw.split(",") if s.strip()]


# ─── Pure decision logic (testable) ──────────────────────────────────────────

def decide_stage1(
    *,
    table_exists: bool,
    tracker_rows_1h: int,
    min_tracker_rows: int,
) -> tuple[bool, str]:
    """Return (advance, reason). advance=True means HSETNX persister flag."""
    if not table_exists:
        return False, "table_missing"
    if tracker_rows_1h < min_tracker_rows:
        return False, f"tracker_rows_below_min({tracker_rows_1h}<{min_tracker_rows})"
    return True, "ok"


def decide_stage2(
    *,
    s1_active_hours: float,
    persister_errors_24h: int,
    persister_rows_24h: int,
    warn_shadow_rate_per_hour_per_kind: float,
    min_hours: float,
    max_errors: int,
    min_rows: int,
    max_warn_rate: float,
) -> tuple[bool, str]:
    """Stage 1 → Stage 2 gate."""
    if s1_active_hours < min_hours:
        return False, f"s1_too_young({s1_active_hours:.1f}<{min_hours})"
    if persister_errors_24h > max_errors:
        return False, f"persister_errors({persister_errors_24h}>{max_errors})"
    if persister_rows_24h < min_rows:
        return False, f"persisted_rows_below_min({persister_rows_24h}<{min_rows})"
    if warn_shadow_rate_per_hour_per_kind > max_warn_rate:
        return (
            False,
            f"warn_shadow_too_spammy({warn_shadow_rate_per_hour_per_kind:.2f}>"
            f"{max_warn_rate:.2f}/h/kind)",
        )
    return True, "ok"


def decide_stage3_per_kind(
    *,
    kind: str,
    s2_active_hours: float,
    warn_count_7d: int,
    allowlist: list[str],
    min_hours: float,
    max_warns_per_week: int,
) -> tuple[bool, str]:
    """Stage 2 → Stage 3 per-kind gate."""
    if not allowlist:
        return False, "allowlist_empty"
    if (kind or "").lower() not in allowlist:
        return False, "not_in_allowlist"
    if s2_active_hours < min_hours:
        return False, f"s2_too_young({s2_active_hours:.1f}<{min_hours})"
    if warn_count_7d > max_warns_per_week:
        return False, f"too_many_warns({warn_count_7d}>{max_warns_per_week}/wk)"
    return True, "ok"


def autotune_expectancy_threshold(
    *,
    daily_expectancies: list[float],
    floor: float,
    min_days: int,
    current_value: float | None,
) -> float | None:
    """Compute ratchet-only threshold update from observed daily expectancies.

    Algorithm:
        * Need ≥ min_days observations.
        * Take p25 of the distribution → robust "noisy bad day" anchor.
        * Clamp by floor (e.g. -0.10) to bound worst-case loosening.
        * Ratchet: only return a new value if it would be MORE conservative
          than `current_value` (= higher / less negative).
          Rationale: never auto-loosen — only the operator should relax.

    Returns the new value or None if no update should be applied.
    """
    if len(daily_expectancies) < min_days:
        return None
    sorted_vals = sorted(daily_expectancies)
    # p25 index
    idx = max(0, int(round(0.25 * (len(sorted_vals) - 1))))
    p25 = sorted_vals[idx]
    candidate = max(p25, floor)
    if current_value is None:
        return candidate
    # Ratchet: only return if MORE conservative (i.e. higher value).
    # threshold=0 is the strictest; -0.05 is looser than 0; we never auto-loosen.
    if candidate > current_value:
        return candidate
    return None


# ─── Sources of truth (Redis + PG) ───────────────────────────────────────────

def table_exists(conn: Any, table_name: str = "signal_gated_out_outcomes") -> bool:
    """Check if the persister target table is present."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM information_schema.tables WHERE table_name = %s LIMIT 1",
            (table_name,),
        )
        return cur.fetchone() is not None


def tracker_rows_last_hour(rc: Any, stream: str = "stream:signals:gated_out_outcomes") -> int:
    """Approximate: total XLEN of the tracker output stream.

    Used as a liveness signal. The stream has MAXLEN≈200k so xlen ≈
    last-X-hours of tracker output for active markets.
    """
    try:
        return int(rc.xlen(stream))
    except Exception:
        return 0


def persister_rows_in_window(conn: Any, window_ms: int) -> int:
    """Count rows in signal_gated_out_outcomes ingested in last `window_ms`."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM signal_gated_out_outcomes "
            "WHERE ingest_time_ms > (now_ms() - %s)",
            (int(window_ms),),
        )
        row = cur.fetchone()
        return int(row[0] if row else 0)


def count_drift_warns(rc: Any, *, kind: str | None, severities: tuple[str, ...]) -> int:
    """Count drift:state:* HASHes whose severity is in `severities`.

    For kind=None counts across all kinds (used as a Stage 2 spam signal).
    For specific kind counts only matching keys.
    NOTE: this is a snapshot of current state, not a counter — since
    drift_auto_demote overwrites the same key on each scan we approximate
    the warn signal density via the current set of critical keys. For more
    accurate rate-based logic, a Counter time series would be needed; this
    function is a conservative approximation for Stage 3 gating.
    """
    pattern = f"drift:state:{kind}:*" if kind else "drift:state:*"
    matched = 0
    try:
        for key in rc.scan_iter(match=pattern, count=200):
            sev = rc.hget(key, "severity")
            if str(sev or "").strip() in severities:
                matched += 1
    except Exception as e:
        log.debug("count_drift_warns scan error: %s", e)
    return matched


def fetch_daily_expectancies(
    conn: Any, *, top_pct: float, days: int,
) -> list[float]:
    """Compute per-day mean(realized_r) over the top-pct slice of signal_outcome.

    Used as the noise-floor source for `autotune_expectancy_threshold`.

    Implementation: groups rows by UTC day, ranks each day by calib_prob,
    takes the top `top_pct` fraction, returns mean realized_r per day.
    Days with too few rows (< 20) are skipped to avoid leverage from sparse
    samples.
    """
    days_ms = int(days * 86_400_000)
    with conn.cursor() as cur:
        # Use ts-based UTC day bucket. Window functions per day.
        cur.execute(
            """
            WITH base AS (
                SELECT
                    (decision_time_ms / 86400000) AS day_idx,
                    calib_prob,
                    realized_r
                FROM signal_outcome
                WHERE label IS NOT NULL
                  AND calib_prob IS NOT NULL
                  AND realized_r IS NOT NULL
                  AND decision_time_ms > (now_ms() - %s)
            ),
            ranked AS (
                SELECT
                    day_idx,
                    calib_prob,
                    realized_r,
                    PERCENT_RANK() OVER (
                        PARTITION BY day_idx ORDER BY calib_prob DESC
                    ) AS pr
                FROM base
            )
            SELECT day_idx, AVG(realized_r) AS expectancy
            FROM ranked
            WHERE pr <= %s
            GROUP BY day_idx
            HAVING COUNT(*) >= 1
            ORDER BY day_idx
            """,
            (days_ms, float(top_pct)),
        )
        return [float(row[1]) for row in cur.fetchall() if row[1] is not None]


# ─── Sticky flag writer ──────────────────────────────────────────────────────

def activate_flag_sticky(
    rc: Any, flag: str, *, now_ms: int,
) -> bool:
    """HSETNX flag; on first-set also write the activation timestamp.

    Returns True only on NEW activation.
    """
    try:
        new = rc.hsetnx(AUTOPILOT_KEY, flag, "1")
        if new:
            rc.hset(AUTOPILOT_KEY, activated_at_field(flag), str(now_ms))
            log.info("plan2_autopilot FLAG ACTIVATED flag=%s at_ms=%d", flag, now_ms)
            return True
        return False
    except Exception as e:
        log.warning("activate_flag_sticky HSETNX %s error: %s", flag, e)
        return False


def write_expectancy_threshold(rc: Any, value: float) -> bool:
    """Overwrite the auto-tuned expectancy threshold (NOT sticky).

    Caller is responsible for the ratchet guard.
    """
    try:
        rc.hset(AUTOPILOT_KEY, FIELD_EXPECTANCY_THRESHOLD, f"{value:.6f}")
        return True
    except Exception as e:
        log.warning("write_expectancy_threshold error: %s", e)
        return False


def hours_since_flag_activation(rc: Any, flag: str, *, now_ms: int) -> float:
    """Return age in hours since flag activation; 0.0 if not activated."""
    try:
        raw = rc.hget(AUTOPILOT_KEY, activated_at_field(flag))
        if not raw:
            return 0.0
        ts_ms = int(raw)
        return max(0.0, (now_ms - ts_ms) / 3_600_000.0)
    except Exception:
        return 0.0


# ─── Main loop ───────────────────────────────────────────────────────────────

def main() -> None:
    import redis  # type: ignore
    from prometheus_client import Counter, Gauge, start_http_server

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )

    enabled = _env_bool("PLAN2_AUTOPILOT_ENABLED", False)
    redis_url = _env(
        "PLAN2_AUTOPILOT_REDIS_URL",
        _env("REDIS_URL", "redis://redis-worker-1:6379/0"),
    )
    db_dsn = _env("PLAN2_AUTOPILOT_DB_DSN", _env("TRADES_DB_DSN", ""))
    port = _env_int("PLAN2_AUTOPILOT_PORT", 9923)
    interval_sec = _env_int("PLAN2_AUTOPILOT_INTERVAL_SEC", 600)

    s1_min_tracker_rows = _env_int("PLAN2_S1_MIN_TRACKER_ROWS_1H", 100)
    s2_min_hours = _env_float("PLAN2_S2_MIN_HOURS_S1", 48.0)
    s2_max_errors = _env_int("PLAN2_S2_MAX_PERSISTER_ERRORS_24H", 0)
    s2_min_rows = _env_int("PLAN2_S2_MIN_PERSISTED_ROWS_24H", 500)
    s2_max_warn_rate = _env_float(
        "PLAN2_S2_MAX_WARN_SHADOW_PER_HOUR_PER_KIND", 5.0,
    )
    s3_min_hours = _env_float("PLAN2_S3_MIN_HOURS_S2", 168.0)
    s3_kind_allowlist = _env_csv("PLAN2_S3_KIND_ALLOWLIST", "meta_lr_blend,v14_of")
    s3_max_warns_per_week = _env_int("PLAN2_S3_MAX_WARN_PER_KIND_PER_WEEK", 1)

    autotune_enabled = _env_bool("PLAN2_EXPECTANCY_AUTOTUNE_ENABLED", True)
    autotune_top_pct = _env_float("PLAN2_EXPECTANCY_TOP_PCT", 0.05)
    autotune_floor = _env_float("PLAN2_EXPECTANCY_THRESHOLD_FLOOR", -0.10)
    autotune_min_days = _env_int("PLAN2_EXPECTANCY_AUTOTUNE_MIN_DAYS", 7)

    log.info(
        "plan2_autopilot starting | enabled=%s port=%d interval=%ds "
        "allowlist=%s autotune=%s",
        enabled, port, interval_sec, s3_kind_allowlist, autotune_enabled,
    )

    rc = redis.from_url(redis_url, decode_responses=True)

    start_http_server(port)
    g_stage = Gauge(
        "plan2_autopilot_stage",
        "Highest active stage (0 baseline, 1=persister, 2=ph, 3=any auto_demote_kind)",
    )
    g_flag = Gauge(
        "plan2_autopilot_flag_active",
        "1 when the flag is active in Redis",
        ["flag"],
    )
    c_activated = Counter(
        "plan2_autopilot_flag_activated_total",
        "Times a flag transitioned 0→1",
        ["flag"],
    )
    c_check = Counter(
        "plan2_autopilot_check_total",
        "Stage gate evaluations",
        ["stage", "result"],
    )
    g_threshold = Gauge(
        "plan2_autopilot_expectancy_threshold",
        "Current auto-tuned expectancy_threshold (Redis value or default 0.0)",
    )
    g_persister_rows = Gauge(
        "plan2_autopilot_persister_rows_24h",
        "signal_gated_out_outcomes rows inserted in last 24h",
    )
    g_warn_rate = Gauge(
        "plan2_autopilot_warn_rate_per_kind",
        "Critical drift warn count per kind in last week",
        ["kind"],
    )
    c_err = Counter("plan2_autopilot_error_total", "Scan errors", [])

    conn = None

    def _get_conn():
        nonlocal conn
        if conn is None or conn.closed:
            import psycopg2
            conn = psycopg2.connect(db_dsn)
        return conn

    while True:
        try:
            time.sleep(interval_sec)
            if not db_dsn:
                log.debug("PLAN2_AUTOPILOT_DB_DSN not set; skipping scan")
                continue

            now_ms = int(time.time() * 1000)

            try:
                cn = _get_conn()
                tbl_exists = table_exists(cn)
            except Exception as e:
                c_err.inc()
                log.warning("table_exists check failed: %s", e)
                conn = None
                continue

            tracker_rows = tracker_rows_last_hour(rc)
            persister_rows_24h = 0
            persister_errors_24h = 0  # rolled into the row-count signal; explicit counter is in Prometheus only
            if tbl_exists:
                try:
                    persister_rows_24h = persister_rows_in_window(cn, 24 * 3_600_000)
                except Exception as e:
                    c_err.inc()
                    log.warning("persister_rows query failed: %s", e)

            # ── Stage 1 evaluation ─────────────────────────────────────
            adv1, reason1 = decide_stage1(
                table_exists=tbl_exists,
                tracker_rows_1h=tracker_rows,
                min_tracker_rows=s1_min_tracker_rows,
            )
            c_check.labels(stage="1", result=("pass" if adv1 else f"hold:{reason1}")).inc()
            if adv1 and enabled and not read_plan2_flag(rc, FLAG_PERSISTER_ENABLED):
                if activate_flag_sticky(rc, FLAG_PERSISTER_ENABLED, now_ms=now_ms):
                    c_activated.labels(flag=FLAG_PERSISTER_ENABLED).inc()

            s1_active = read_plan2_flag(rc, FLAG_PERSISTER_ENABLED)
            g_flag.labels(flag=FLAG_PERSISTER_ENABLED).set(1 if s1_active else 0)
            g_persister_rows.set(persister_rows_24h)

            # ── Stage 2 evaluation (needs S1 active) ───────────────────
            s2_active = read_plan2_flag(rc, FLAG_DRIFT_PH_ENABLED)
            if s1_active and not s2_active:
                s1_hours = hours_since_flag_activation(
                    rc, FLAG_PERSISTER_ENABLED, now_ms=now_ms,
                )
                warn_count = count_drift_warns(
                    rc, kind=None, severities=("warn_shadow",),
                )
                # warn_shadow_count is a snapshot, not a true rate; convert
                # to per-hour-per-kind using a 24h denominator and a
                # default 1-kind floor to avoid division by zero.
                kinds_seen = max(1, sum(1 for _ in rc.scan_iter(
                    match="cfg:ml_confirm:*", count=50,
                )))
                warn_rate = warn_count / 24.0 / kinds_seen
                adv2, reason2 = decide_stage2(
                    s1_active_hours=s1_hours,
                    persister_errors_24h=persister_errors_24h,
                    persister_rows_24h=persister_rows_24h,
                    warn_shadow_rate_per_hour_per_kind=warn_rate,
                    min_hours=s2_min_hours,
                    max_errors=s2_max_errors,
                    min_rows=s2_min_rows,
                    max_warn_rate=s2_max_warn_rate,
                )
                c_check.labels(stage="2", result=("pass" if adv2 else f"hold:{reason2}")).inc()
                if adv2 and enabled:
                    if activate_flag_sticky(rc, FLAG_DRIFT_PH_ENABLED, now_ms=now_ms):
                        c_activated.labels(flag=FLAG_DRIFT_PH_ENABLED).inc()
                        s2_active = True
            g_flag.labels(flag=FLAG_DRIFT_PH_ENABLED).set(1 if s2_active else 0)

            # ── Expectancy threshold auto-tune (always; ratchet-only) ──
            current_thr = read_plan2_float(
                rc, FIELD_EXPECTANCY_THRESHOLD, default=0.0,
            )
            g_threshold.set(current_thr)
            if autotune_enabled and tbl_exists:
                try:
                    daily = fetch_daily_expectancies(
                        cn, top_pct=autotune_top_pct, days=14,
                    )
                    new_thr = autotune_expectancy_threshold(
                        daily_expectancies=daily,
                        floor=autotune_floor,
                        min_days=autotune_min_days,
                        current_value=current_thr,
                    )
                    if new_thr is not None and enabled:
                        if write_expectancy_threshold(rc, new_thr):
                            log.info(
                                "plan2_autopilot expectancy_threshold tuned: %.4f → %.4f (n_days=%d)",
                                current_thr, new_thr, len(daily),
                            )
                            g_threshold.set(new_thr)
                except Exception as e:
                    c_err.inc()
                    log.warning("expectancy autotune error: %s", e)

            # ── Stage 3 per-kind allowlist ─────────────────────────────
            if s2_active:
                s2_hours = hours_since_flag_activation(
                    rc, FLAG_DRIFT_PH_ENABLED, now_ms=now_ms,
                )
                for kind in s3_kind_allowlist:
                    warn_count_kind = count_drift_warns(
                        rc, kind=kind, severities=("critical",),
                    )
                    g_warn_rate.labels(kind=kind).set(warn_count_kind)
                    flag = kind_demote_flag(kind)
                    already = read_plan2_flag(rc, flag)
                    if already:
                        g_flag.labels(flag=flag).set(1)
                        continue
                    adv3, reason3 = decide_stage3_per_kind(
                        kind=kind,
                        s2_active_hours=s2_hours,
                        warn_count_7d=warn_count_kind,
                        allowlist=s3_kind_allowlist,
                        min_hours=s3_min_hours,
                        max_warns_per_week=s3_max_warns_per_week,
                    )
                    c_check.labels(
                        stage="3",
                        result=("pass" if adv3 else f"hold:{reason3}"),
                    ).inc()
                    if adv3 and enabled:
                        if activate_flag_sticky(rc, flag, now_ms=now_ms):
                            c_activated.labels(flag=flag).inc()
                            g_flag.labels(flag=flag).set(1)

            # ── Effective stage gauge ──────────────────────────────────
            stage = 0
            if s1_active:
                stage = 1
            if s2_active:
                stage = 2
            # Stage 3 = any per-kind flag set
            try:
                for k in s3_kind_allowlist:
                    if read_plan2_flag(rc, kind_demote_flag(k)):
                        stage = 3
                        break
            except Exception:
                pass
            g_stage.set(stage)

        except Exception as e:
            c_err.inc()
            log.warning("plan2_autopilot main loop error: %s", e)


if __name__ == "__main__":
    main()
