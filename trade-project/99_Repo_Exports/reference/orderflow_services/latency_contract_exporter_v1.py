from __future__ import annotations

"""Prometheus exporter for unified latency contract Redis state hashes.

Scrapes all metrics:latency_contract:last:* keys and exposes:
  latency_contract_exporter_stage_latest_ms
  latency_contract_exporter_stage_age_ms
  latency_contract_exporter_stage_budget_ratio

Key schema: {prefix}:{service}:{stage}:{symbol}
Hash fields: last_duration_ms, last_ts_ms, ts_event_ms, ts_redis_read_ms, ts_feature_ms, ts_emit_ms

ENV vars:
  LATENCY_CONTRACT_KEY_PREFIX              (default: metrics:latency_contract:last)
  LATENCY_CONTRACT_EXPORTER_PORT           (default: 9830)
  LATENCY_CONTRACT_EXPORTER_INTERVAL_S     (default: 5)
  LATENCY_CONTRACT_EXPORTER_STALE_S        (default: 60)
  LATENCY_BUDGET_REDIS_TO_FEATURE_MS       (default: 50)
  LATENCY_BUDGET_FEATURE_TO_EMIT_MS        (default: 100)
  LATENCY_BUDGET_END_TO_END_EVENT_MS       (default: 200)
"""

import asyncio
import logging
import os
import time
from typing import Any, Dict, Optional, Tuple

from prometheus_client import Gauge, Counter, Histogram, start_http_server

from services.observability.latency_semconv import required_stage_owners, default_symbol_allowlist

logger = logging.getLogger("latency_contract_exporter")

# ------------------------------------------------------------------
# Metrics
# ------------------------------------------------------------------
latency_contract_exporter_stage_latest_ms = Gauge(
    "latency_contract_exporter_stage_latest_ms",
    "Latest stage duration read from Redis state hash (ms)",
    ["service", "stage", "symbol"],
)
latency_contract_exporter_stage_age_ms = Gauge(
    "latency_contract_exporter_stage_age_ms",
    "Age of the last Redis state hash entry (ms since last_ts_ms)",
    ["service", "stage", "symbol"],
)
latency_contract_exporter_stage_budget_ratio = Gauge(
    "latency_contract_exporter_stage_budget_ratio",
    "Ratio of last_duration_ms to configured budget (>1 means budget breach)",
    ["service", "stage", "symbol"],
)
latency_contract_exporter_stale_total = Counter(
    "latency_contract_exporter_stale_total",
    "Number of state hash entries considered stale (age > LATENCY_CONTRACT_EXPORTER_STALE_S)",
    ["service", "stage"],
)
latency_contract_exporter_scrape_errors_total = Counter(
    "latency_contract_exporter_scrape_errors_total",
    "Errors during exporter scrape loop",
    [],
)
latency_contract_exporter_last_scrape_ts = Gauge(
    "latency_contract_exporter_last_scrape_ts",
    "Unix timestamp of the last successful scrape",
    [],
)


_LATENCY_STAGE_BUCKETS = (1, 2, 5, 10, 20, 30, 50, 75, 100, 150, 200, 300, 500, 750, 1000, 1500, 2000, 5000)
_LATENCY_EVENT_LAG_BUCKETS = (1, 2, 5, 10, 20, 30, 50, 75, 100, 150, 200, 300, 500, 750, 1000, 1500, 2000, 5000, 10000)

latency_contract_stage_ms = Histogram(
    "latency_contract_stage_ms",
    "Observed latency-contract stage durations (ms), de-duplicated by last_ts_ms per key",
    ["service", "stage", "symbol"],
    buckets=_LATENCY_STAGE_BUCKETS,
)
latency_contract_event_lag_ms = Histogram(
    "latency_contract_event_lag_ms",
    "Observed event-time lag (last_ts_ms - ts_event_ms) in ms, de-duplicated by last_ts_ms per key",
    ["service", "stage", "symbol"],
    buckets=_LATENCY_EVENT_LAG_BUCKETS,
)
latency_contract_event_lag_latest_ms = Gauge(
    "latency_contract_event_lag_latest_ms",
    "Latest event-time lag (last_ts_ms - ts_event_ms) in ms",
    ["service", "stage", "symbol"],
)

# Dedupe repeated scrapes of the same Redis hash row.
# Keyed by the exact Redis key so one unchanged row contributes to histograms only once.
_HISTOGRAM_OBSERVED_TOKENS: Dict[str, Tuple[int, int, int]] = {}


def _extract_event_lag_ms(row: Dict[str, Any]) -> int:
    ts_event_ms = _safe_int(row.get("ts_event_ms"), 0)
    last_ts_ms = _safe_int(row.get("last_ts_ms"), 0)
    if ts_event_ms <= 0 or last_ts_ms <= 0 or last_ts_ms < ts_event_ms:
        return 0
    return max(0, int(last_ts_ms - ts_event_ms))


def _observe_histograms_if_fresh(obs_key: str, service: str, stage: str, symbol: str, row: Dict[str, Any]) -> None:
    last_ts_ms = _safe_int(row.get("last_ts_ms"), 0)
    dur_ms = max(0, _safe_int(row.get("last_duration_ms"), 0))
    event_lag_ms = max(0, _extract_event_lag_ms(row))
    token = (int(last_ts_ms), int(dur_ms), int(event_lag_ms))
    if last_ts_ms <= 0:
        return
    if _HISTOGRAM_OBSERVED_TOKENS.get(obs_key) == token:
        return
    _HISTOGRAM_OBSERVED_TOKENS[obs_key] = token
    latency_contract_stage_ms.labels(service=service, stage=stage, symbol=symbol).observe(float(dur_ms))
    if event_lag_ms > 0:
        latency_contract_event_lag_ms.labels(service=service, stage=stage, symbol=symbol).observe(float(event_lag_ms))


# ------------------------------------------------------------------
# P4.1 — required stage coverage and SLO gate
# ------------------------------------------------------------------
latency_contract_required_stage_present = Gauge(
    "latency_contract_required_stage_present",
    "Required service-stage-symbol contract presence (1=hash exists, 0=missing)",
    ["service", "stage", "symbol"],
)
latency_contract_required_stage_age_seconds = Gauge(
    "latency_contract_required_stage_age_seconds",
    "Age in seconds since last_ts_ms was written for a required stage",
    ["service", "stage", "symbol"],
)
latency_contract_slo_gate_ok = Gauge(
    "latency_contract_slo_gate_ok",
    "1 when all required stages are present and fresh, 0 otherwise",
    [],
)
latency_contract_slo_missing_total = Gauge(
    "latency_contract_slo_missing_total",
    "Count of required stages with no Redis hash (per SLO gate cycle)",
    [],
)
latency_contract_slo_stale_total = Gauge(
    "latency_contract_slo_stale_total",
    "Count of required stages whose hash is stale (per SLO gate cycle)",
    [],
)
latency_contract_slo_budget_breach_total = Gauge(
    "latency_contract_slo_budget_breach_total",
    "Count of required stages exceeding latency budget (per SLO gate cycle)",
    [],
)

# ------------------------------------------------------------------
# P4.2 — rollout gate gauges
# ------------------------------------------------------------------
latency_contract_rollout_gate_active = Gauge(
    "latency_contract_rollout_gate_active",
    "Latency contract rollout gate active (1=block, 0=allow)",
    [],
)
latency_contract_rollout_gate_external_missing_total = Gauge(
    "latency_contract_rollout_gate_external_missing_total",
    "Latency contract rollout gate external missing total",
    [],
)
latency_contract_rollout_gate_budget_breach_total = Gauge(
    "latency_contract_rollout_gate_budget_breach_total",
    "Latency contract rollout gate current budget breach total",
    [],
)
latency_contract_rollout_gate_budget_hold_seconds = Gauge(
    "latency_contract_rollout_gate_budget_hold_seconds",
    "Latency contract rollout gate budget hold seconds",
    [],
)
latency_contract_rollout_gate_budget_hold_reached = Gauge(
    "latency_contract_rollout_gate_budget_hold_reached",
    "Latency contract rollout gate hold threshold reached",
    [],
)

# ------------------------------------------------------------------
# Budget defaults
# ------------------------------------------------------------------
_DEFAULT_BUDGETS: Dict[str, int] = {
    "ingest_to_redis": 30,
    "redis_to_feature": 50,
    "feature_to_emit": 100,
    "emit_to_ws": 100,
    "end_to_end_event": 200,
}


def _budgets_from_env() -> Dict[str, int]:
    out = dict(_DEFAULT_BUDGETS)
    for stage, envvar in [
        ("ingest_to_redis", "LATENCY_BUDGET_INGEST_TO_REDIS_MS"),
        ("redis_to_feature", "LATENCY_BUDGET_REDIS_TO_FEATURE_MS"),
        ("feature_to_emit", "LATENCY_BUDGET_FEATURE_TO_EMIT_MS"),
        ("emit_to_ws", "LATENCY_BUDGET_EMIT_TO_WS_MS"),
        ("end_to_end_event", "LATENCY_BUDGET_END_TO_END_EVENT_MS"),
    ]:
        raw = os.getenv(envvar, "")
        if raw.strip():
            try:
                out[stage] = int(float(raw))
            except Exception:
                pass
    return out


def _parse_key(key: str, prefix: str) -> Optional[Tuple[str, str, str]]:
    """Extract (service, stage, symbol) from a redis key.

    Key format: {prefix}:{service}:{stage}:{symbol}
    """
    try:
        suffix = key[len(prefix) + 1:]  # strip prefix:
        parts = suffix.split(":", 2)
        if len(parts) != 3:
            return None
        service, stage, symbol = parts
        if not service or not stage or not symbol:
            return None
        return service, stage, symbol
    except Exception:
        return None


def _safe_int(x: Any, default: int = 0) -> int:
    try:
        if x is None:
            return default
        return int(float(str(x).strip()))
    except Exception:
        return default


async def _scrape_once(redis_client: Any, prefix: str, stale_s: int, budgets: Dict[str, int]) -> None:
    now_ms = int(time.time() * 1000)
    pattern = f"{prefix}:*"
    try:
        keys = await redis_client.keys(pattern)
    except Exception as exc:
        logger.warning("latency_contract_exporter: keys scan failed: %s", exc)
        latency_contract_exporter_scrape_errors_total.inc()
        return

    for key in keys:
        parsed = _parse_key(str(key), prefix)
        if not parsed:
            continue
        service, stage, symbol = parsed
        try:
            row = await redis_client.hgetall(key)
        except Exception:
            latency_contract_exporter_scrape_errors_total.inc()
            continue

        dur_ms = _safe_int(row.get("last_duration_ms"), 0)
        last_ts_ms = _safe_int(row.get("last_ts_ms"), 0)
        age_ms = int(now_ms - last_ts_ms) if last_ts_ms > 0 else 0
        event_lag_ms = _extract_event_lag_ms(row)

        # Update gauges
        latency_contract_exporter_stage_latest_ms.labels(service=service, stage=stage, symbol=symbol).set(float(max(0, dur_ms)))
        latency_contract_exporter_stage_age_ms.labels(service=service, stage=stage, symbol=symbol).set(float(max(0, age_ms)))
        latency_contract_event_lag_latest_ms.labels(service=service, stage=stage, symbol=symbol).set(float(max(0, event_lag_ms)))
        _observe_histograms_if_fresh(str(key), service, stage, symbol, row)

        # Budget ratio
        budget = budgets.get(stage, 0)
        if budget > 0 and dur_ms > 0:
            ratio = float(dur_ms) / float(budget)
            latency_contract_exporter_stage_budget_ratio.labels(service=service, stage=stage, symbol=symbol).set(ratio)

        # Staleness counter
        if stale_s > 0 and age_ms > stale_s * 1000:
            latency_contract_exporter_stale_total.labels(service=service, stage=stage).inc()

    latency_contract_exporter_last_scrape_ts.set(float(time.time()))


async def _scrape_required_coverage(
    redis_client: Any,
    prefix: str,
    stale_s: int,
    slo_summary_key: str,
    rollout_gate_state_key: str = 'metrics:latency_contract:rollout_gate:last',
) -> None:
    """P4.1/P4.2: read required stage hashes, SLO gate summary, and rollout gate state."""
    now = time.time()
    symbols = tuple(sorted(default_symbol_allowlist()))
    for service, stage in required_stage_owners():
        for symbol in symbols:
            key = f"{prefix}:{service}:{stage}:{symbol}"
            try:
                row = await redis_client.hgetall(key)
            except Exception:
                row = {}
            present = 1.0 if row else 0.0
            last_ts_ms_raw = row.get('last_ts_ms', '') if row else ''
            last_ts_ms = int(float(last_ts_ms_raw)) if last_ts_ms_raw else 0
            age_s = max(0.0, now - (last_ts_ms / 1000.0)) if last_ts_ms > 0 else float(stale_s)
            latency_contract_required_stage_present.labels(
                service=service, stage=stage, symbol=symbol
            ).set(present)
            latency_contract_required_stage_age_seconds.labels(
                service=service, stage=stage, symbol=symbol
            ).set(age_s)

    # Read SLO gate summary (written by latency_contract_slo_gate_v1.py).
    try:
        summary = await redis_client.hgetall(slo_summary_key) or {}
    except Exception:
        summary = {}

    def _sf(val: Any, default: float = 0.0) -> float:
        try:
            return float(val)
        except Exception:
            return default

    latency_contract_slo_gate_ok.set(_sf(summary.get('gate_ok'), 0.0))
    latency_contract_slo_missing_total.set(_sf(summary.get('missing_total'), 0.0))
    latency_contract_slo_stale_total.set(_sf(summary.get('stale_total'), 0.0))
    latency_contract_slo_budget_breach_total.set(_sf(summary.get('budget_breach_total'), 0.0))

    # P4.2: read rollout gate state hash (written by latency_contract_rollout_gate_v1.py).
    try:
        gate = await redis_client.hgetall(rollout_gate_state_key) or {}
    except Exception:
        gate = {}

    latency_contract_rollout_gate_active.set(_sf(gate.get('gate_active'), 0.0))
    latency_contract_rollout_gate_external_missing_total.set(_sf(gate.get('external_missing_total'), 0.0))
    latency_contract_rollout_gate_budget_breach_total.set(_sf(gate.get('budget_breach_total'), 0.0))
    latency_contract_rollout_gate_budget_hold_seconds.set(_sf(gate.get('budget_breach_hold_s'), 0.0))
    latency_contract_rollout_gate_budget_hold_reached.set(_sf(gate.get('budget_hold_reached'), 0.0))


async def run_exporter_loop(
    redis_client: Any,
    prefix: str,
    interval_s: float,
    stale_s: int,
    budgets: Dict[str, int],
    slo_summary_key: str = 'metrics:latency_contract:slo:last',
    rollout_gate_state_key: str = 'metrics:latency_contract:rollout_gate:last',
) -> None:
    while True:
        try:
            await _scrape_once(redis_client, prefix, stale_s, budgets)
            await _scrape_required_coverage(
                redis_client, prefix, stale_s, slo_summary_key, rollout_gate_state_key
            )
        except Exception as exc:
            logger.error("latency_contract_exporter: scrape loop error: %s", exc)
            latency_contract_exporter_scrape_errors_total.inc()
        await asyncio.sleep(interval_s)


async def main() -> None:
    import redis.asyncio as aioredis  # type: ignore[import]

    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    port = int(os.getenv("LATENCY_CONTRACT_EXPORTER_PORT", "9830"))
    interval_s = float(os.getenv("LATENCY_CONTRACT_EXPORTER_INTERVAL_S", "5"))
    stale_s = int(os.getenv("LATENCY_CONTRACT_EXPORTER_STALE_S", "60"))
    prefix = os.getenv("LATENCY_CONTRACT_KEY_PREFIX", "metrics:latency_contract:last")
    slo_summary_key = os.getenv("LATENCY_CONTRACT_SLO_SUMMARY_KEY", "metrics:latency_contract:slo:last")
    rollout_gate_state_key = os.getenv("LATENCY_CONTRACT_ROLLOUT_GATE_STATE_KEY", "metrics:latency_contract:rollout_gate:last")

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    logger.info("Starting latency_contract_exporter on :%d (interval=%.1fs)", port, interval_s)

    start_http_server(port)

    budgets = _budgets_from_env()
    redis_client = aioredis.from_url(redis_url, decode_responses=True)
    try:
        await run_exporter_loop(redis_client, prefix, interval_s, stale_s, budgets, slo_summary_key, rollout_gate_state_key)
    finally:
        await redis_client.aclose()


if __name__ == "__main__":
    asyncio.run(main())
