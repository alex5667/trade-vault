from __future__ import annotations

"""Prometheus exporter for nightly TCA report summary (P6 gap-closure).

Reads the Redis hash written by tca_nightly_report_v1.py and exposes low-cardinality
Prometheus gauges for alerting and dashboards.

ENV vars:
  REDIS_URL                          (default: redis://redis-worker-1:6379/0)
  TCA_NIGHTLY_REPORT_STATE_KEY       (default: state:tca_nightly_report:last)
  TCA_NIGHTLY_REPORT_EXPORTER_PORT   (default: 9866)
  TCA_NIGHTLY_REPORT_EXPORTER_INTERVAL_S (default: 30)
"""

import os
import time
from typing import Any, Dict

try:
    import redis  # type: ignore
except Exception:  # pragma: no cover
    redis = None
from prometheus_client import Gauge, start_http_server

UP = Gauge('tca_nightly_report_exporter_up', '1 if exporter can read TCA nightly report state')
LAST_UPDATED_TS_MS = Gauge('tca_nightly_report_last_updated_ts_ms', 'Last successful TCA nightly report timestamp in epoch ms')
LAST_AGE_SECONDS = Gauge('tca_nightly_report_last_age_seconds', 'Age of last TCA nightly report in seconds')
DURATION_MS = Gauge('tca_nightly_report_last_duration_ms', 'Duration of last TCA nightly report run in ms')
ROWS_TOTAL = Gauge('tca_nightly_report_rows_total', 'Rows covered by nightly TCA report', ['window'])
GROUPS_TOTAL = Gauge('tca_nightly_report_groups_total', 'Distinct group count in nightly TCA report', ['window'])
BREACH_GROUPS = Gauge('tca_nightly_report_breach_groups', 'Number of 24h TCA groups breaching thresholds', ['metric'])
WORST_VALUE_BPS = Gauge('tca_nightly_report_worst_value_bps', 'Worst 24h summary value by metric (bps)', ['metric'])


def _i(x: Any, d: int = 0) -> int:
    try:
        return int(float(x))
    except Exception:
        return int(d)


def _f(x: Any, d: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return float(d)


def publish_from_mapping(m: Dict[str, Any]) -> None:
    updated_ms = _i(m.get('updated_ts_ms'), 0)
    now_s = time.time()
    LAST_UPDATED_TS_MS.set(float(updated_ms))
    LAST_AGE_SECONDS.set((now_s - (updated_ms / 1000.0)) if updated_ms > 0 else float('nan'))
    DURATION_MS.set(float(_i(m.get('dur_ms'), 0)))
    ROWS_TOTAL.labels(window='24h').set(float(_i(m.get('rows_24h_total'), 0)))
    ROWS_TOTAL.labels(window='7d').set(float(_i(m.get('rows_7d_total'), 0)))
    GROUPS_TOTAL.labels(window='24h').set(float(_i(m.get('groups_24h'), 0)))
    GROUPS_TOTAL.labels(window='7d').set(float(_i(m.get('groups_7d'), 0)))
    BREACH_GROUPS.labels(metric='is_p95').set(float(_i(m.get('breach_is_p95_24h'), 0)))
    BREACH_GROUPS.labels(metric='perm_impact_p95').set(float(_i(m.get('breach_perm_impact_p95_24h'), 0)))
    BREACH_GROUPS.labels(metric='realized_spread_p50').set(float(_i(m.get('breach_realized_spread_p50_24h'), 0)))
    BREACH_GROUPS.labels(metric='eff_spread_p95').set(float(_i(m.get('breach_eff_spread_p95_24h'), 0)))
    WORST_VALUE_BPS.labels(metric='is_p95').set(float(_f(m.get('worst_is_p95_bps_24h'), 0.0)))
    WORST_VALUE_BPS.labels(metric='perm_impact_p95').set(float(_f(m.get('worst_perm_impact_p95_bps_24h'), 0.0)))
    WORST_VALUE_BPS.labels(metric='realized_spread_p50').set(float(_f(m.get('worst_realized_spread_p50_bps_24h'), 0.0)))
    WORST_VALUE_BPS.labels(metric='eff_spread_p95').set(float(_f(m.get('worst_eff_spread_p95_bps_24h'), 0.0)))
    UP.set(1.0 if _i(m.get('ok'), 0) == 1 else 0.0)


def main() -> None:
    if redis is None:
        raise RuntimeError('redis dependency missing')
    redis_url = os.getenv('REDIS_URL', 'redis://redis-worker-1:6379/0')
    key = os.getenv('TCA_NIGHTLY_REPORT_STATE_KEY', 'state:tca_nightly_report:last')
    port = int(os.getenv('TCA_NIGHTLY_REPORT_EXPORTER_PORT', '9866'))
    interval_s = float(os.getenv('TCA_NIGHTLY_REPORT_EXPORTER_INTERVAL_S', '30') or 30)
    r = redis.Redis.from_url(redis_url, decode_responses=True)
    start_http_server(port)
    while True:
        try:
            publish_from_mapping(r.hgetall(key) or {})
        except Exception:
            UP.set(0.0)
        time.sleep(interval_s)


if __name__ == '__main__':
    main()
