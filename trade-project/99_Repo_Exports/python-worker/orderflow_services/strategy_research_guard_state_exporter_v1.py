from __future__ import annotations

from utils.time_utils import get_ny_time_millis

"""Prometheus exporter for strategy research guard state (P5.1).

Reads two compact Redis hashes produced by the nightly research job / blocker path:
  - STRATEGY_RESEARCH_GUARD_SUMMARY_KEY (default: metrics:strategy_research_guard:last)
  - STRATEGY_RESEARCH_GUARD_BLOCKER_KEY (default: cfg:research_guard:blocker:v1)

Design goals:
  - low-cardinality metrics only
  - fail-open on missing Redis / missing hashes
  - deterministic classification for blocker reason and notifier state

The exporter intentionally does not emit free-form string labels from Redis to avoid
accidental cardinality explosions in Prometheus.
""",
import logging
import os
import time
from collections.abc import Mapping
from dataclasses import dataclass

from prometheus_client import Gauge, start_http_server

try:
    import redis  # type: ignore
except Exception:  # pragma: no cover
    redis = None


logger = logging.getLogger(__name__)


def _env(name: str, default: str = "") -> str:
    return (os.getenv(name) or default).strip()


def _to_float(value: object, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _to_int(value: object, default: int = 0) -> int:
    try:
        return int(float(value))
    except Exception:
        return default


def _redis_client():
    if redis is None:
        return None
    try:
        return redis.Redis.from_url(
            _env("REDIS_URL", "redis://redis-worker-1:6379/0"),
            decode_responses=True,
        )
    except Exception:
        return None


def _read_hash(client, key: str) -> dict[str, str]:
    if client is None or not key:
        return {}
    try:
        data = client.hgetall(key)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


KNOWN_REASON_KINDS = (
    "none",
    "stale",
    "pbo_high",
    "psr_low",
    "dsr_low",
    "metric_low",
    "manual",
    "unknown",
)


def classify_reason_kind(reason: str) -> str:
    s = (reason or "").strip().lower()
    if not s:
        return "none"
    if "pbo" in s:
        return "pbo_high"
    if "psr" in s:
        return "psr_low"
    if "dsr" in s:
        return "dsr_low"
    if "stale" in s or "fresh" in s:
        return "stale"
    if "expectancy" in s or "precision" in s or "mean_r" in s or "downside" in s or "hit_rate" in s:
        return "metric_low"
    if "manual" in s or "operator" in s:
        return "manual"
    return "unknown"


@dataclass(frozen=True)
class ExportState:
    summary_present: float = 0.0
    blocker_present: float = 0.0
    last_success: float = 0.0
    report_only: float = 1.0
    blocker_active: float = 0.0
    blocker_reason_kind: str = "none"
    updated_ts_ms: float = 0.0
    report_age_seconds: float = 0.0
    primary_metric_value: float = 0.0
    net_expectancy: float = 0.0
    precision_at_top_x: float = 0.0
    mean_r: float = 0.0
    downside_adjusted_return: float = 0.0
    hit_rate_conditioned_on_cost: float = 0.0
    psr: float = 0.0
    dsr: float = 0.0
    pbo: float = 0.0
    cscv_splits: float = 0.0
    chosen_variant_unique: float = 0.0


def _compute_state(summary: Mapping[str, str], blocker: Mapping[str, str], now_ms: int | None = None) -> ExportState:
    now_ms = int(now_ms if now_ms is not None else get_ny_time_millis())
    summary_present = 1.0 if summary else 0.0
    blocker_present = 1.0 if blocker else 0.0

    updated_ts_ms = _to_float(summary.get("updated_ts_ms") or summary.get("ts_ms") or 0)
    if updated_ts_ms <= 0:
        updated_ts_ms = _to_float(blocker.get("updated_ts_ms") or 0)

    report_age_seconds = 0.0
    if updated_ts_ms > 0:
        report_age_seconds = max(0.0, (now_ms - updated_ts_ms) / 1000.0)

    reason = str(blocker.get("reason") or summary.get("blocker_reason") or "").strip()
    reason_kind = classify_reason_kind(reason)

    # Report-only defaults to enabled until the user explicitly flips the blocker mode.
    report_only_raw = blocker.get("report_only", summary.get("report_only", "1"))
    report_only = 1.0 if _to_int(report_only_raw, 1) > 0 else 0.0
    blocker_active_raw = blocker.get("blocked", blocker.get("active", summary.get("blocker_active", "0")))
    blocker_active = 1.0 if _to_int(blocker_active_raw, 0) > 0 else 0.0

    last_success = 1.0 if _to_int(summary.get("success", summary.get("last_success", "0")), 0) > 0 else 0.0
    chosen_variant_unique = 1.0 if _to_int(summary.get("chosen_variant_unique", "0"), 0) > 0 else 0.0

    return ExportState(
        summary_present=summary_present,
        blocker_present=blocker_present,
        last_success=last_success,
        report_only=report_only,
        blocker_active=blocker_active,
        blocker_reason_kind=reason_kind,
        updated_ts_ms=updated_ts_ms,
        report_age_seconds=report_age_seconds,
        primary_metric_value=_to_float(summary.get("primary_metric_value", 0.0)),
        net_expectancy=_to_float(summary.get("net_expectancy", 0.0)),
        precision_at_top_x=_to_float(summary.get("precision_at_top_x", 0.0)),
        mean_r=_to_float(summary.get("mean_r", 0.0)),
        downside_adjusted_return=_to_float(summary.get("downside_adjusted_return", 0.0)),
        hit_rate_conditioned_on_cost=_to_float(summary.get("hit_rate_conditioned_on_cost", 0.0)),
        psr=_to_float(summary.get("psr", 0.0)),
        dsr=_to_float(summary.get("dsr", 0.0)),
        pbo=_to_float(summary.get("pbo", 0.0)),
        cscv_splits=_to_float(summary.get("cscv_splits", 0.0)),
        chosen_variant_unique=chosen_variant_unique,
    )


UP = Gauge("strategy_research_guard_exporter_up", "1 if exporter loop is alive")
REDIS_READ_OK = Gauge("strategy_research_guard_exporter_redis_read_ok", "1 if exporter read Redis successfully")
SUMMARY_PRESENT = Gauge("strategy_research_guard_summary_present", "1 if summary hash exists")
BLOCKER_PRESENT = Gauge("strategy_research_guard_blocker_present", "1 if blocker hash exists")
LAST_SUCCESS = Gauge("strategy_research_guard_last_success", "1 if latest research guard job succeeded")
REPORT_ONLY = Gauge("strategy_research_guard_report_only", "1 if blocker is in report-only mode")
BLOCKER_ACTIVE = Gauge("strategy_research_guard_blocker_active", "1 if promotion/apply blocker is active")
BLOCKER_REASON = Gauge("strategy_research_guard_blocker_reason", "One-hot blocker reason kind", ["kind"])
UPDATED_TS_MS = Gauge("strategy_research_guard_last_updated_ts_ms", "updated_ts_ms from latest research report")
AGE_SECONDS = Gauge("strategy_research_guard_report_age_seconds", "Age of latest research report in seconds")
PRIMARY_METRIC = Gauge("strategy_research_guard_primary_metric_value", "Primary evaluator metric value")
NET_EXPECTANCY = Gauge("strategy_research_guard_net_expectancy", "Research net expectancy")
PRECISION_TOPX = Gauge("strategy_research_guard_precision_at_top_x", "Precision at selected top-X bucket")
MEAN_R = Gauge("strategy_research_guard_mean_r", "Mean R of research sample")
DOWNSIDE_RETURN = Gauge("strategy_research_guard_downside_adjusted_return", "Downside-adjusted return")
HITRATE_COST = Gauge("strategy_research_guard_hit_rate_conditioned_on_cost", "Hit-rate conditioned on cost")
PSR = Gauge("strategy_research_guard_psr", "Probabilistic Sharpe Ratio or equivalent normalized score")
DSR = Gauge("strategy_research_guard_dsr", "Deflated Sharpe Ratio or conservative proxy")
PBO = Gauge("strategy_research_guard_pbo", "Probability of Backtest Overfitting")
CSCV_SPLITS = Gauge("strategy_research_guard_cscv_splits", "CSCV split count used in latest report")
CHOSEN_VARIANT_UNIQUE = Gauge("strategy_research_guard_chosen_variant_unique", "1 if latest best variant is uniquely identified")


def _export_state(state: ExportState) -> None:
    SUMMARY_PRESENT.set(state.summary_present)
    BLOCKER_PRESENT.set(state.blocker_present)
    LAST_SUCCESS.set(state.last_success)
    REPORT_ONLY.set(state.report_only)
    BLOCKER_ACTIVE.set(state.blocker_active)
    UPDATED_TS_MS.set(state.updated_ts_ms)
    AGE_SECONDS.set(state.report_age_seconds)
    PRIMARY_METRIC.set(state.primary_metric_value)
    NET_EXPECTANCY.set(state.net_expectancy)
    PRECISION_TOPX.set(state.precision_at_top_x)
    MEAN_R.set(state.mean_r)
    DOWNSIDE_RETURN.set(state.downside_adjusted_return)
    HITRATE_COST.set(state.hit_rate_conditioned_on_cost)
    PSR.set(state.psr)
    DSR.set(state.dsr)
    PBO.set(state.pbo)
    CSCV_SPLITS.set(state.cscv_splits)
    CHOSEN_VARIANT_UNIQUE.set(state.chosen_variant_unique)
    for kind in KNOWN_REASON_KINDS:
        BLOCKER_REASON.labels(kind=kind).set(1.0 if kind == state.blocker_reason_kind else 0.0)


def main() -> None:
    port = int(_env("STRATEGY_RESEARCH_GUARD_EXPORTER_PORT", "9836") or 9836)
    interval_s = float(_env("STRATEGY_RESEARCH_GUARD_EXPORTER_INTERVAL_S", "15") or 15)
    summary_key = _env("STRATEGY_RESEARCH_GUARD_SUMMARY_KEY", "metrics:strategy_research_guard:last")
    blocker_key = _env("STRATEGY_RESEARCH_GUARD_BLOCKER_KEY", "cfg:research_guard:blocker:v1")

    start_http_server(port)
    logger.info("strategy research guard exporter listening on %s", port)

    while True:
        UP.set(1.0)
        client = _redis_client()
        if client is None:
            REDIS_READ_OK.set(0.0)
            time.sleep(interval_s)
            continue

        try:
            summary = _read_hash(client, summary_key)
            blocker = _read_hash(client, blocker_key)
            REDIS_READ_OK.set(1.0)
            _export_state(_compute_state(summary, blocker))
        except Exception:
            logger.exception("strategy research guard exporter iteration failed")
            REDIS_READ_OK.set(0.0)
        time.sleep(interval_s)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
