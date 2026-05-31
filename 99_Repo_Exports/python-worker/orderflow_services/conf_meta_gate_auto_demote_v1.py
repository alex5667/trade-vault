"""orderflow_services/conf_meta_gate_auto_demote_v1.py — Plan 1 Phase 7.

Auto-demotion watcher for the confidence meta-gate. Mirrors
`drift_auto_demote_v1` but watches meta-gate-specific signals:

  * fallback rate over the scan window
  * legacy/meta disagreement rate
  * top-pct expectancy for the canary cohort (re-uses ExpectancyMonitor)

When any criterion fires the sustained-negative streak the watcher writes
`cfg:conf_meta_gate` (HSET mode=SHADOW + reason + ts). The runtime reader
in `services.confidence_meta_gate.runtime` consults this override before
each decision and forces mode=SHADOW when present.

SHADOW by default (`CONF_META_GATE_AUTO_DEMOTE_ENABLED=0` → score/log only,
no mutation). ENV always wins; the watcher only ever flips OFF→ON, never
re-enables a manually shadowed gate.

ENV:
  CONF_META_GATE_AUTO_DEMOTE_ENABLED       = 0
  CONF_META_GATE_AUTO_DEMOTE_REDIS_URL     = redis://redis-worker-1:6379/0
  CONF_META_GATE_AUTO_DEMOTE_DB_DSN        = (from TRADES_DB_DSN)
  CONF_META_GATE_AUTO_DEMOTE_INTERVAL_SEC  = 600
  CONF_META_GATE_AUTO_DEMOTE_WINDOW_MIN    = 240
  CONF_META_GATE_AUTO_DEMOTE_MIN_N         = 200
  CONF_META_GATE_AUTO_DEMOTE_FALLBACK_MAX  = 0.05
  CONF_META_GATE_AUTO_DEMOTE_DISAGREE_MAX  = 0.40
  CONF_META_GATE_AUTO_DEMOTE_EXP_TOP_PCT   = 0.05
  CONF_META_GATE_AUTO_DEMOTE_EXP_MIN_N     = 50
  CONF_META_GATE_AUTO_DEMOTE_EXP_THRESHOLD = 0.0
  CONF_META_GATE_AUTO_DEMOTE_EXP_SUSTAIN   = 3
  CONF_META_GATE_AUTO_DEMOTE_PORT          = 9928

Prometheus:
  conf_meta_gate_auto_demote_fallback_rate       Gauge
  conf_meta_gate_auto_demote_disagreement_rate   Gauge
  conf_meta_gate_auto_demote_window_count        Gauge
  conf_meta_gate_auto_demote_expectancy_r        Gauge
  conf_meta_gate_auto_demote_sustained_neg_scans Gauge
  conf_meta_gate_auto_demote_action_total{reason,action}
  conf_meta_gate_auto_demote_scan_error_total
"""
from __future__ import annotations

import logging
import os
import time
from typing import Any

log = logging.getLogger("conf_meta_gate_auto_demote")


def _env(k: str, d: str = "") -> str:
    return os.environ.get(k, d)


def _env_int(k: str, d: int) -> int:
    try:
        return int(_env(k, str(d)))
    except (TypeError, ValueError):
        return d


def _env_float(k: str, d: float) -> float:
    try:
        return float(_env(k, str(d)))
    except (TypeError, ValueError):
        return d


def _env_bool(k: str, d: bool) -> bool:
    raw = _env(k, "")
    if not raw:
        return d
    return raw.strip().lower() in ("1", "true", "yes", "on")


_FETCH_SQL = """
    SELECT
        sid,
        ts,
        model_ver,
        mode,
        legacy_decision,
        meta_decision,
        active_decision,
        canary_selected,
        p_win_calibrated
    FROM confidence_meta_gate_decisions
    WHERE ts >= to_timestamp(%s / 1000.0)
    ORDER BY ts ASC
    LIMIT %s
"""

_FETCH_OUTCOMES_SQL = """
    SELECT
        d.sid,
        d.p_win_calibrated,
        d.canary_selected,
        d.meta_decision,
        o.realized_r
    FROM confidence_meta_gate_decisions d
    JOIN signal_outcome o ON o.sid = d.sid
    WHERE d.ts >= to_timestamp(%s / 1000.0)
      AND o.realized_r IS NOT NULL
      AND d.canary_selected IS TRUE
    LIMIT %s
"""


def fetch_decisions(conn: Any, since_ms: int, max_rows: int = 50_000) -> list[dict]:
    with conn.cursor() as cur:
        cur.execute(_FETCH_SQL, (since_ms, max_rows))
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]


def fetch_canary_outcomes(conn: Any, since_ms: int, max_rows: int = 50_000) -> list[dict]:
    with conn.cursor() as cur:
        cur.execute(_FETCH_OUTCOMES_SQL, (since_ms, max_rows))
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]


def compute_fallback_rate(rows: list[dict]) -> tuple[float, int]:
    """Share of rows that fell back to legacy (meta_decision == FALLBACK_LEGACY)."""
    if not rows:
        return 0.0, 0
    n = len(rows)
    fb = sum(1 for r in rows if str(r.get("meta_decision") or "") == "FALLBACK_LEGACY")
    return fb / n, n


def compute_disagreement_rate(rows: list[dict]) -> tuple[float, int]:
    """Share where legacy_decision != meta_decision (excluding fallbacks).

    Fallback rows are excluded — they reflect lifecycle issues, not gate
    disagreement, so they would distort the disagreement signal.
    """
    eligible = [
        r for r in rows
        if str(r.get("meta_decision") or "") not in ("", "FALLBACK_LEGACY")
    ]
    n = len(eligible)
    if n == 0:
        return 0.0, 0
    legacy_kinds = []
    meta_kinds = []
    for r in eligible:
        legacy_kinds.append(_collapse(r.get("legacy_decision"), legacy=True))
        meta_kinds.append(_collapse(r.get("meta_decision"), legacy=False))
    diff = sum(1 for a, b in zip(legacy_kinds, meta_kinds) if a != b)
    return diff / n, n


def _collapse(decision: Any, *, legacy: bool) -> str:
    s = str(decision or "").upper()
    if legacy:
        return "ALLOW" if s == "ALLOW" else "DENY"
    if s in ("ALLOW", "ALLOW_TIGHTENED", "SHADOW_ALLOW"):
        return "ALLOW"
    if s in ("DENY_SOFT", "SHADOW_DENY"):
        return "DENY"
    return "FALLBACK"


def compute_canary_top_pct_expectancy(
    rows: list[dict], *, top_pct: float, min_n: int,
) -> tuple[float | None, int]:
    """Average realized_r over the top calib_prob slice of canary-selected rows.

    Mirrors `drift_auto_demote.compute_expectancy_top_pct` semantics so the
    promotion-blocking criterion matches the ML drift watcher.
    """
    if top_pct <= 0.0 or top_pct > 1.0:
        return None, 0
    eligible = [
        (float(r["p_win_calibrated"]), float(r["realized_r"]))
        for r in rows
        if r.get("p_win_calibrated") is not None
        and r.get("realized_r") is not None
    ]
    n = len(eligible)
    if n < min_n:
        return None, n
    eligible.sort(key=lambda pair: pair[0], reverse=True)
    take_n = max(1, round(n * top_pct))
    top = eligible[:take_n]
    return sum(r for _, r in top) / len(top), n


class _SustainedNegativeMonitor:
    """Generic sustained-negative streak counter for a single scalar series.

    Used for fallback/disagreement criteria. When the metric exceeds
    `threshold` for `sustain_scans` consecutive scans, the monitor fires.
    """

    def __init__(self, *, threshold: float, sustain_scans: int) -> None:
        self.threshold = threshold
        self.sustain_scans = max(1, sustain_scans)
        self._streak = 0

    def evaluate(self, value: float | None) -> tuple[int, bool]:
        if value is None:
            self._streak = 0
            return 0, False
        if value > self.threshold:
            self._streak += 1
        else:
            self._streak = 0
        return self._streak, self._streak >= self.sustain_scans


def force_shadow(rc: Any, *, reason: str) -> bool:
    """HSET cfg:conf_meta_gate mode=SHADOW + audit fields.

    Returns True on success. Caller is responsible for the master switch.
    The runtime reader picks this up and overrides the configured mode.
    """
    key = "cfg:conf_meta_gate"
    try:
        rc.hset(key, mapping={
            "mode": "SHADOW",
            "auto_demoted": "1",
            "auto_demote_reason": reason,
            "auto_demote_ts_ms": str(int(time.time() * 1000)),
        })
        # No expire — the override persists until ops clears it.
        return True
    except Exception as e:
        log.warning("force_shadow HSET failed: %s", e)
        return False


def main() -> None:
    import redis  # type: ignore
    from prometheus_client import Counter, Gauge, start_http_server

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )

    enabled = _env_bool("CONF_META_GATE_AUTO_DEMOTE_ENABLED", False)
    redis_url = _env(
        "CONF_META_GATE_AUTO_DEMOTE_REDIS_URL",
        _env("REDIS_URL", "redis://redis-worker-1:6379/0"),
    )
    db_dsn = _env(
        "CONF_META_GATE_AUTO_DEMOTE_DB_DSN", _env("TRADES_DB_DSN", ""),
    )
    interval_sec = _env_int("CONF_META_GATE_AUTO_DEMOTE_INTERVAL_SEC", 600)
    window_min = _env_int("CONF_META_GATE_AUTO_DEMOTE_WINDOW_MIN", 240)
    min_n = _env_int("CONF_META_GATE_AUTO_DEMOTE_MIN_N", 200)
    fallback_max = _env_float("CONF_META_GATE_AUTO_DEMOTE_FALLBACK_MAX", 0.05)
    disagree_max = _env_float("CONF_META_GATE_AUTO_DEMOTE_DISAGREE_MAX", 0.40)
    exp_top_pct = _env_float("CONF_META_GATE_AUTO_DEMOTE_EXP_TOP_PCT", 0.05)
    exp_min_n = _env_int("CONF_META_GATE_AUTO_DEMOTE_EXP_MIN_N", 50)
    exp_threshold = _env_float("CONF_META_GATE_AUTO_DEMOTE_EXP_THRESHOLD", 0.0)
    exp_sustain = _env_int("CONF_META_GATE_AUTO_DEMOTE_EXP_SUSTAIN", 3)
    port = _env_int("CONF_META_GATE_AUTO_DEMOTE_PORT", 9928)

    log.info(
        "conf_meta_gate_auto_demote starting | enabled=%s port=%d interval=%ds window=%dmin",
        enabled, port, interval_sec, window_min,
    )

    rc = redis.from_url(redis_url, decode_responses=True)

    start_http_server(port)
    g_fb = Gauge(
        "conf_meta_gate_auto_demote_fallback_rate",
        "Share of decisions that fell back to legacy",
    )
    g_diff = Gauge(
        "conf_meta_gate_auto_demote_disagreement_rate",
        "Share where legacy and meta decisions differ (fallbacks excluded)",
    )
    g_n = Gauge(
        "conf_meta_gate_auto_demote_window_count",
        "Decisions seen in the current scan window",
    )
    g_exp = Gauge(
        "conf_meta_gate_auto_demote_expectancy_r",
        "Average realized_r for the top-p_cal canary slice",
    )
    g_exp_streak = Gauge(
        "conf_meta_gate_auto_demote_sustained_neg_scans",
        "Consecutive scans where the canary expectancy is below threshold",
    )
    c_act = Counter(
        "conf_meta_gate_auto_demote_action_total",
        "Auto-demote actions taken (or skipped in SHADOW)",
        ["reason", "action"],
    )
    c_err = Counter(
        "conf_meta_gate_auto_demote_scan_error_total",
        "Errors during scan", [],
    )

    fallback_mon = _SustainedNegativeMonitor(
        threshold=fallback_max, sustain_scans=2,
    )
    disagree_mon = _SustainedNegativeMonitor(
        threshold=disagree_max, sustain_scans=2,
    )
    # Reuse the same sustained-negative pattern but inverted: expectancy
    # below threshold is "bad", so we feed (threshold - expectancy) and
    # check positivity.
    exp_streak = 0

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
                log.debug("CONF_META_GATE_AUTO_DEMOTE_DB_DSN not set; skipping scan")
                continue

            now_ms = int(time.time() * 1000)
            since_ms = now_ms - window_min * 60_000

            try:
                rows = fetch_decisions(_get_conn(), since_ms)
                outcomes = fetch_canary_outcomes(_get_conn(), since_ms)
            except Exception as e:
                c_err.inc()
                log.warning("fetch error: %s", e)
                conn = None
                continue

            g_n.set(len(rows))
            if len(rows) < min_n:
                log.debug("window %d rows < min_n %d; skipping", len(rows), min_n)
                continue

            fb_rate, _ = compute_fallback_rate(rows)
            diff_rate, _ = compute_disagreement_rate(rows)
            g_fb.set(fb_rate)
            g_diff.set(diff_rate)

            fb_streak, fb_fired = fallback_mon.evaluate(fb_rate)
            diff_streak, diff_fired = disagree_mon.evaluate(diff_rate)

            exp_val, n_exp = compute_canary_top_pct_expectancy(
                outcomes, top_pct=exp_top_pct, min_n=exp_min_n,
            )
            if exp_val is not None:
                g_exp.set(exp_val)
                if exp_val < exp_threshold:
                    exp_streak += 1
                else:
                    exp_streak = 0
            else:
                exp_streak = 0
            g_exp_streak.set(exp_streak)
            exp_fired = exp_streak >= max(1, exp_sustain)

            for reason, fired in (
                ("fallback_rate_high", fb_fired),
                ("disagreement_rate_high", diff_fired),
                ("canary_expectancy_negative", exp_fired),
            ):
                if not fired:
                    continue
                if not enabled:
                    c_act.labels(reason=reason, action="warn_shadow").inc()
                    log.info(
                        "[SHADOW] would force conf_meta_gate=SHADOW reason=%s "
                        "fb=%.3f diff=%.3f exp=%s exp_streak=%d",
                        reason, fb_rate, diff_rate,
                        f"{exp_val:.4f}" if exp_val is not None else "n/a",
                        exp_streak,
                    )
                    continue
                if force_shadow(rc, reason=f"auto:{reason}"):
                    c_act.labels(reason=reason, action="demote").inc()
                    log.warning(
                        "forced conf_meta_gate=SHADOW reason=%s "
                        "fb=%.3f diff=%.3f exp=%s",
                        reason, fb_rate, diff_rate,
                        f"{exp_val:.4f}" if exp_val is not None else "n/a",
                    )
                else:
                    c_act.labels(reason=reason, action="demote_failed").inc()

        except Exception as e:
            c_err.inc()
            log.warning("auto_demote loop error: %s", e)


if __name__ == "__main__":
    main()
