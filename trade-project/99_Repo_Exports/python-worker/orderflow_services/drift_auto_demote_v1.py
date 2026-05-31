"""
orderflow_services/drift_auto_demote_v1.py — Plan 3 / Step 3 drift-monitor service.

Periodically scans resolved signal_outcome rows per (kind, symbol) bucket,
feeds rolling util_r and (when calib_prob present) Brier-score residuals
into Page-Hinkley detectors, and on a positive signal writes one of:

  * DRIFT_WARN    — Redis HSET drift:state:{kind}:{symbol} severity=warn
  * DRIFT_DEMOTE  — additionally HSET cfg:ml_confirm:{kind} mode=SHADOW
                    (only when DRIFT_AUTO_DEMOTE_ENABLED=1; otherwise warn-only)
  * DRIFT_FREEZE  — symbol-bucket trade halt (when DRIFT_AUTO_FREEZE_ENABLED=1)

Design constraints:
  * Read-only of DB except for Redis HSET state — never UPDATE signal_outcome.
  * SHADOW by default: master switch off → publishes drift score gauges only,
    does NOT mutate model mode or freeze flags. Useful to tune thresholds.
  * Per-bucket Page-Hinkley state lives in process memory (reset on restart).
    Trade-off: short downtime erases warm-up; acceptable because thresholds
    have built-in min_n warm-up.

ENV:
  DRIFT_PAGE_HINKLEY_ENABLED  = 0      master switch (0 = scoring only, no mode flips)
  DRIFT_AUTO_DEMOTE_ENABLED   = 0      also writes mode=SHADOW to cfg:ml_confirm:{kind}
  DRIFT_AUTO_FREEZE_ENABLED   = 0      also writes drift:freeze:{kind}:{symbol}=1
  DRIFT_REDIS_URL             = redis://redis-worker-1:6379/0
  DRIFT_DB_DSN                = (from TRADES_DB_DSN)
  DRIFT_INTERVAL_SEC          = 600
  DRIFT_WINDOW_MIN            = 240    rolling window (minutes) of resolved labels
  DRIFT_MIN_N                 = 100    Page-Hinkley warm-up samples
  DRIFT_EDGE_DELTA            = 0.02
  DRIFT_EDGE_THRESHOLD        = 2.5
  DRIFT_BRIER_DELTA           = 0.005
  DRIFT_BRIER_THRESHOLD       = 2.5
  DRIFT_PORT                  = 9926

  # Plan 2 Gap 6 — top-5% expectancy auto-demote trigger
  DRIFT_EXPECTANCY_ENABLED    = 1      compute top-pct expectancy per scan
  DRIFT_EXPECTANCY_TOP_PCT    = 0.05   top-percentile by calib_prob
  DRIFT_EXPECTANCY_MIN_N      = 50     min rows in bucket before evaluating
  DRIFT_EXPECTANCY_THRESHOLD  = 0.0    expectancy < this is "negative"
  DRIFT_EXPECTANCY_SUSTAIN    = 3      consecutive negative scans before action
                                       (3 × DRIFT_INTERVAL_SEC = sustained window)

Prometheus:
  drift_page_hinkley_score{kind,symbol,metric}    Gauge — current PH score
  drift_page_hinkley_signals_total{kind,symbol,metric,action}
  drift_model_auto_demotions_total{kind,symbol,reason}
  drift_resolved_window_count{kind,symbol}        Gauge — labels in last window
  drift_expectancy_r_top_pct{kind,symbol}         Gauge — current top-pct expectancy
  drift_expectancy_sustained_negative_scans{kind,symbol}  Gauge — consecutive negative
  drift_scan_error_total
"""
from __future__ import annotations

import logging
import os
import time
from collections import defaultdict
from typing import Any

log = logging.getLogger("drift_auto_demote")


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
    SELECT kind, symbol, realized_r, calib_prob, label
    FROM signal_outcome
    WHERE resolved_time_ms IS NOT NULL
      AND label IS NOT NULL
      AND decision_time_ms >= %s
    ORDER BY decision_time_ms ASC
    LIMIT %s
"""


def fetch_recent_resolved(conn: Any, since_ms: int, max_rows: int = 50_000) -> list[dict]:
    with conn.cursor() as cur:
        cur.execute(_FETCH_SQL, (since_ms, max_rows))
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]


def compute_brier(calib_prob: float | None, label: int | None) -> float | None:
    """Brier score for a single sample.

    y ∈ {0,1}: 1 for TP, 0 for SL. Timeout (label=0) → 0 (treated as loss).
    Returns None if calib_prob missing.
    """
    if calib_prob is None or label is None:
        return None
    y = 1 if label == 1 else 0
    return (float(calib_prob) - y) ** 2


def compute_expectancy_top_pct(
    rows: list[dict],
    *,
    top_pct: float = 0.05,
    min_n: int = 50,
) -> tuple[float | None, int]:
    """Average realized_r over the top-percentile of rows ranked by calib_prob.

    Mirrors `ml_outcome_expectancy_r_top5pct` semantics: when the model's
    most confident predictions stop printing positive EV, the model has lost
    its discrimination edge — primary auto-demote signal.

    Args:
        rows: bucket rows containing calib_prob and realized_r.
        top_pct: fraction (0,1] selecting the highest-calib_prob slice.
        min_n: minimum eligible rows (with both fields) to compute. Below
               this, returns (None, n_eligible) — caller skips.

    Returns:
        (expectancy_r_top_pct, n_eligible). expectancy is None when min_n unmet.
    """
    if top_pct <= 0.0 or top_pct > 1.0:
        return None, 0
    eligible = [
        (float(r["calib_prob"]), float(r["realized_r"]))
        for r in rows
        if r.get("calib_prob") is not None and r.get("realized_r") is not None
    ]
    n_eligible = len(eligible)
    if n_eligible < min_n:
        return None, n_eligible
    eligible.sort(key=lambda pair: pair[0], reverse=True)
    take_n = max(1, round(n_eligible * top_pct))
    top_slice = eligible[:take_n]
    avg_r = sum(r for _, r in top_slice) / len(top_slice)
    return avg_r, n_eligible


class ExpectancyMonitor:
    """Sustained-negative top-pct expectancy tracker per (kind, symbol).

    Plan 2 Gap 6: closes the loop on `MLOutcomeExpectancyNegative` — until
    now the alert was page-only with no programmatic response.

    The trigger fires only when expectancy stays below `threshold` for
    `sustain_scans` consecutive scans. With DRIFT_INTERVAL_SEC=600 and
    sustain=3 this is 30 minutes of sustained loss in the top slice —
    long enough to filter intra-window noise, short enough to react
    before further bleed.
    """

    def __init__(
        self,
        *,
        top_pct: float,
        min_n: int,
        threshold: float,
        sustain_scans: int,
    ) -> None:
        self.top_pct = top_pct
        self.min_n = min_n
        self.threshold = threshold
        self.sustain_scans = max(1, sustain_scans)
        # key = (kind, symbol) ; value = consecutive-negative scan count
        self._negative_streak: dict[tuple[str, str], int] = {}

    def evaluate(
        self, kind: str, symbol: str, rows: list[dict],
    ) -> dict[str, Any]:
        """Compute expectancy for the bucket and update sustained-negative state.

        Returns:
            {
              "expectancy": float | None,
              "n_eligible": int,
              "streak": int,        # consecutive negative scans (post-update)
              "fired": bool,        # streak >= sustain_scans
            }
        """
        expectancy, n_eligible = compute_expectancy_top_pct(
            rows, top_pct=self.top_pct, min_n=self.min_n,
        )
        key = (kind, symbol)
        if expectancy is None:
            # Not enough data — reset streak so noise doesn't persist on cold buckets.
            self._negative_streak[key] = 0
            return {
                "expectancy": None,
                "n_eligible": n_eligible,
                "streak": 0,
                "fired": False,
            }

        if expectancy < self.threshold:
            self._negative_streak[key] = self._negative_streak.get(key, 0) + 1
        else:
            self._negative_streak[key] = 0

        streak = self._negative_streak[key]
        return {
            "expectancy": expectancy,
            "n_eligible": n_eligible,
            "streak": streak,
            "fired": streak >= self.sustain_scans,
        }


def split_by_bucket(rows: list[dict]) -> dict[tuple[str, str], list[dict]]:
    """Group rows by (kind, symbol)."""
    out: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for r in rows:
        kind = str(r.get("kind") or "_default")
        symbol = str(r.get("symbol") or "_unknown")
        out[(kind, symbol)].append(r)
    return out


class DriftMonitor:
    """In-memory Page-Hinkley state per (kind, symbol, metric) bucket."""

    def __init__(
        self,
        *,
        min_n: int,
        edge_delta: float,
        edge_threshold: float,
        brier_delta: float,
        brier_threshold: float,
    ) -> None:
        self.min_n = min_n
        self.edge_delta = edge_delta
        self.edge_threshold = edge_threshold
        self.brier_delta = brier_delta
        self.brier_threshold = brier_threshold
        # key = (kind, symbol, metric); value = PageHinkley
        self._detectors: dict[tuple[str, str, str], Any] = {}
        # Track which row IDs we've fed so periodic re-scan doesn't double-count.
        # Use cursor = max decision_time_ms seen per bucket.
        self._last_seen_ms: dict[tuple[str, str], int] = defaultdict(int)

    def _get(self, kind: str, symbol: str, metric: str):
        from core.page_hinkley import (
            detector_for_brier_increase,
            detector_for_edge_drop,
        )
        key = (kind, symbol, metric)
        if key not in self._detectors:
            if metric == "edge":
                self._detectors[key] = detector_for_edge_drop(min_n=self.min_n)
            elif metric == "brier":
                self._detectors[key] = detector_for_brier_increase(min_n=self.min_n)
            else:
                raise ValueError(f"unknown metric: {metric}")
        return self._detectors[key]

    def process_bucket(
        self,
        kind: str,
        symbol: str,
        rows: list[dict],
    ) -> dict[str, dict]:
        """Feed bucket rows into detectors; return per-metric outcome dict.

        Result shape:
            {
              "edge":  {"signal": bool, "score": float, "n": int},
              "brier": {...} | None,
            }
        Only feeds rows newer than self._last_seen_ms[bucket].
        """
        last = self._last_seen_ms[(kind, symbol)]
        result: dict[str, dict] = {}

        edge_det = self._get(kind, symbol, "edge")
        brier_det = self._get(kind, symbol, "brier")

        edge_signal = False
        brier_signal = False
        brier_seen = 0

        max_seen = last
        for r in rows:
            ts = int(r.get("decision_time_ms") or 0)
            if ts <= last:
                continue
            if ts > max_seen:
                max_seen = ts

            realized_r = r.get("realized_r")
            if realized_r is not None:
                # Higher = worse: negate util_r so positive means a loss
                if edge_det.update(-float(realized_r)):
                    edge_signal = True

            b = compute_brier(r.get("calib_prob"), r.get("label"))
            if b is not None:
                brier_seen += 1
                if brier_det.update(b):
                    brier_signal = True

        if max_seen > last:
            self._last_seen_ms[(kind, symbol)] = max_seen

        result["edge"] = {
            "signal": edge_signal,
            "score": edge_det.score(),
            "n": edge_det.n(),
        }
        if brier_seen > 0:
            result["brier"] = {
                "signal": brier_signal,
                "score": brier_det.score(),
                "n": brier_det.n(),
            }
        return result


def write_drift_state(
    rc: Any,
    *,
    kind: str,
    symbol: str,
    metric: str,
    severity: str,
    score: float,
    n: int,
    action: str,
    now_ms: int,
) -> None:
    """HSET drift:state:{kind}:{symbol}:{metric} with timestamped severity."""
    key = f"drift:state:{kind}:{symbol}:{metric}"
    try:
        rc.hset(key, mapping={
            "severity": severity,
            "score": f"{score:.4f}",
            "n": str(n),
            "action": action,
            "ts_ms": str(now_ms),
        })
        rc.expire(key, 7 * 24 * 3600)
    except Exception as e:
        log.debug("drift state HSET error %s: %s", key, e)


def demote_model_mode(rc: Any, *, kind: str, reason: str) -> bool:
    """HSET cfg:ml_confirm:{kind} mode=SHADOW.

    Returns True on success. Caller is responsible for the master switch.
    """
    key = f"cfg:ml_confirm:{kind}"
    try:
        rc.hset(key, mapping={
            "mode": "SHADOW",
            "auto_demoted": "1",
            "auto_demote_reason": reason,
            "auto_demote_ts_ms": str(int(time.time() * 1000)),
        })
        return True
    except Exception as e:
        log.warning("demote_model_mode HSET %s failed: %s", key, e)
        return False


def freeze_bucket(rc: Any, *, kind: str, symbol: str, reason: str) -> bool:
    """SET drift:freeze:{kind}:{symbol}=1 with reason."""
    key = f"drift:freeze:{kind}:{symbol}"
    try:
        rc.hset(key, mapping={
            "frozen": "1",
            "reason": reason,
            "ts_ms": str(int(time.time() * 1000)),
        })
        rc.expire(key, 24 * 3600)
        return True
    except Exception as e:
        log.warning("freeze_bucket HSET %s failed: %s", key, e)
        return False


def main() -> None:
    import redis  # type: ignore
    from prometheus_client import Counter, Gauge, start_http_server

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )

    env_ph_enabled = _env_bool("DRIFT_PAGE_HINKLEY_ENABLED", False)
    env_demote_enabled = _env_bool("DRIFT_AUTO_DEMOTE_ENABLED", False)
    freeze_enabled = _env_bool("DRIFT_AUTO_FREEZE_ENABLED", False)
    redis_url = _env("DRIFT_REDIS_URL", _env("REDIS_URL", "redis://redis-worker-1:6379/0"))
    db_dsn = _env("DRIFT_DB_DSN", _env("TRADES_DB_DSN", ""))
    interval_sec = _env_int("DRIFT_INTERVAL_SEC", 600)
    window_min = _env_int("DRIFT_WINDOW_MIN", 240)
    min_n = _env_int("DRIFT_MIN_N", 100)
    edge_delta = _env_float("DRIFT_EDGE_DELTA", 0.02)
    edge_threshold = _env_float("DRIFT_EDGE_THRESHOLD", 2.5)
    brier_delta = _env_float("DRIFT_BRIER_DELTA", 0.005)
    brier_threshold = _env_float("DRIFT_BRIER_THRESHOLD", 2.5)
    expectancy_enabled = _env_bool("DRIFT_EXPECTANCY_ENABLED", True)
    expectancy_top_pct = _env_float("DRIFT_EXPECTANCY_TOP_PCT", 0.05)
    expectancy_min_n = _env_int("DRIFT_EXPECTANCY_MIN_N", 50)
    expectancy_threshold = _env_float("DRIFT_EXPECTANCY_THRESHOLD", 0.0)
    expectancy_sustain = _env_int("DRIFT_EXPECTANCY_SUSTAIN", 3)
    port = _env_int("DRIFT_PORT", 9926)

    log.info(
        "drift_auto_demote starting | env_ph=%s env_demote=%s freeze=%s port=%d interval=%ds window=%dmin",
        env_ph_enabled, env_demote_enabled, freeze_enabled, port, interval_sec, window_min,
    )

    rc = redis.from_url(redis_url, decode_responses=True)

    # Plan 2 autopilot wiring. Effective values resolved per scan iteration:
    #   ph_enabled         = ENV  OR autopilot S2 flag
    #   demote_enabled[k]  = ENV  OR (autopilot S2 + autopilot per-kind S3 flag)
    #   expectancy threshold ratcheted-only from Redis (autopilot autotune)
    # ENV always wins as override; autopilot only flips OFF→ON.
    from core.plan2_autopilot_flags import (
        FIELD_EXPECTANCY_THRESHOLD,
        FLAG_DRIFT_PH_ENABLED,
        is_kind_auto_demote_enabled,
        read_plan2_flag,
        read_plan2_float,
    )

    def _effective_ph() -> bool:
        return env_ph_enabled or read_plan2_flag(rc, FLAG_DRIFT_PH_ENABLED)

    def _effective_demote_for_kind(kind: str) -> bool:
        if env_demote_enabled:
            return True
        # Per-kind allowlist requires both S2 (PH enabled by autopilot) and
        # explicit per-kind S3 flag — never demote without the upstream gate.
        return read_plan2_flag(rc, FLAG_DRIFT_PH_ENABLED) and \
               is_kind_auto_demote_enabled(rc, kind)

    def _refresh_expectancy_threshold() -> None:
        tuned = read_plan2_float(
            rc, FIELD_EXPECTANCY_THRESHOLD, default=expectancy_threshold,
        )
        # Ratchet enforcement also here in case autopilot's value somehow regresses:
        # only adopt if MORE conservative than current monitor threshold.
        if tuned > expectancy_monitor.threshold:
            expectancy_monitor.threshold = tuned

    start_http_server(port)
    g_score = Gauge(
        "drift_page_hinkley_score",
        "Page-Hinkley cumulative-min score per bucket+metric",
        ["kind", "symbol", "metric"],
    )
    c_signal = Counter(
        "drift_page_hinkley_signals_total",
        "Page-Hinkley drift signals raised",
        ["kind", "symbol", "metric", "action"],
    )
    c_demote = Counter(
        "drift_model_auto_demotions_total",
        "Auto-demote actions taken (or skipped due to master switch)",
        ["kind", "symbol", "reason"],
    )
    g_window = Gauge(
        "drift_resolved_window_count",
        "Resolved labels in last window per bucket",
        ["kind", "symbol"],
    )
    c_err = Counter("drift_scan_error_total", "Errors during scan", [])
    g_expectancy = Gauge(
        "drift_expectancy_r_top_pct",
        "Top-percentile (by calib_prob) expectancy_r per bucket",
        ["kind", "symbol"],
    )
    g_exp_streak = Gauge(
        "drift_expectancy_sustained_negative_scans",
        "Consecutive scans where top-pct expectancy < threshold",
        ["kind", "symbol"],
    )

    monitor = DriftMonitor(
        min_n=min_n,
        edge_delta=edge_delta,
        edge_threshold=edge_threshold,
        brier_delta=brier_delta,
        brier_threshold=brier_threshold,
    )
    expectancy_monitor = ExpectancyMonitor(
        top_pct=expectancy_top_pct,
        min_n=expectancy_min_n,
        threshold=expectancy_threshold,
        sustain_scans=expectancy_sustain,
    )

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
                log.debug("DRIFT_DB_DSN not set; skipping scan")
                continue

            now_ms = int(time.time() * 1000)
            since_ms = now_ms - window_min * 60_000

            try:
                rows = fetch_recent_resolved(_get_conn(), since_ms)
            except Exception as e:
                c_err.inc()
                log.warning("drift fetch error: %s", e)
                conn = None
                continue

            buckets = split_by_bucket(rows)

            # Resolve Plan 2 autopilot effective flags once per scan. ph_enabled
            # is bucket-independent; demote_enabled is per-kind so it's evaluated
            # inside the loop below.
            ph_enabled = _effective_ph()
            _refresh_expectancy_threshold()

            for (kind, symbol), brows in buckets.items():
                g_window.labels(kind=kind, symbol=symbol).set(len(brows))
                results = monitor.process_bucket(kind, symbol, brows)
                # Per-kind allowlist: governs whether a fired drift signal mutates
                # cfg:ml_confirm:{kind}.mode=SHADOW. Snapshot once per bucket
                # (used by both the expectancy block and the PH metrics block).
                demote_enabled = _effective_demote_for_kind(kind)

                # Plan 2 Gap 6: top-pct expectancy trigger (sustained-negative).
                # Runs alongside Page-Hinkley edge/brier — independent signal.
                if expectancy_enabled:
                    exp = expectancy_monitor.evaluate(kind, symbol, brows)
                    if exp["expectancy"] is not None:
                        g_expectancy.labels(kind=kind, symbol=symbol).set(exp["expectancy"])
                    g_exp_streak.labels(kind=kind, symbol=symbol).set(exp["streak"])
                    if exp["fired"]:
                        # SHADOW master switch off → record signal, no mutation.
                        if not ph_enabled:
                            c_signal.labels(
                                kind=kind, symbol=symbol,
                                metric="expectancy_top_pct", action="warn_shadow",
                            ).inc()
                            write_drift_state(
                                rc, kind=kind, symbol=symbol, metric="expectancy_top_pct",
                                severity="warn_shadow",
                                score=float(exp["expectancy"] or 0.0),
                                n=exp["n_eligible"],
                                action="warn_shadow", now_ms=now_ms,
                            )
                        else:
                            action = "warn"
                            if demote_enabled:
                                if demote_model_mode(rc, kind=kind, reason="drift:expectancy_top_pct"):
                                    action = "demote"
                                    c_demote.labels(
                                        kind=kind, symbol=symbol,
                                        reason="drift_expectancy_top_pct",
                                    ).inc()
                            if freeze_enabled:
                                if freeze_bucket(rc, kind=kind, symbol=symbol, reason="drift:expectancy_top_pct"):
                                    action = "freeze" if action == "warn" else action + "+freeze"
                            c_signal.labels(
                                kind=kind, symbol=symbol,
                                metric="expectancy_top_pct", action=action,
                            ).inc()
                            write_drift_state(
                                rc, kind=kind, symbol=symbol, metric="expectancy_top_pct",
                                severity="critical",
                                score=float(exp["expectancy"] or 0.0),
                                n=exp["n_eligible"],
                                action=action, now_ms=now_ms,
                            )

                for metric, info in results.items():
                    g_score.labels(kind=kind, symbol=symbol, metric=metric).set(info["score"])
                    if not info["signal"]:
                        continue

                    # SHADOW master switch off → warn only, no mutation
                    if not ph_enabled:
                        c_signal.labels(kind=kind, symbol=symbol, metric=metric, action="warn_shadow").inc()
                        write_drift_state(
                            rc, kind=kind, symbol=symbol, metric=metric,
                            severity="warn_shadow",
                            score=info["score"], n=info["n"],
                            action="warn_shadow", now_ms=now_ms,
                        )
                        continue

                    # ph_enabled: at minimum warn, then demote/freeze per flags
                    action = "warn"
                    if demote_enabled:
                        if demote_model_mode(rc, kind=kind, reason=f"drift:{metric}"):
                            action = "demote"
                            c_demote.labels(kind=kind, symbol=symbol, reason=f"drift_{metric}").inc()
                    if freeze_enabled:
                        if freeze_bucket(rc, kind=kind, symbol=symbol, reason=f"drift:{metric}"):
                            action = "freeze" if action == "warn" else action + "+freeze"

                    c_signal.labels(kind=kind, symbol=symbol, metric=metric, action=action).inc()
                    write_drift_state(
                        rc, kind=kind, symbol=symbol, metric=metric,
                        severity="critical",
                        score=info["score"], n=info["n"],
                        action=action, now_ms=now_ms,
                    )

        except Exception as e:
            c_err.inc()
            log.warning("drift main loop error: %s", e)


if __name__ == "__main__":
    main()
