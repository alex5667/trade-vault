from __future__ import annotations

from orderflow_services.tca_nightly_report_exporter_v1 import (
    BREACH_GROUPS,
    GROUPS_TOTAL,
    LAST_UPDATED_TS_MS,
    ROWS_TOTAL,
    UP,
    WORST_VALUE_BPS,
    publish_from_mapping,
)


def test_publish_from_mapping_sets_gauges():
    publish_from_mapping({
        'updated_ts_ms': '1700000000000',
        'dur_ms': '912',
        'ok': '1',
        'rows_24h_total': '120',
        'rows_7d_total': '700',
        'groups_24h': '9',
        'groups_7d': '21',
        'breach_is_p95_24h': '2',
        'breach_perm_impact_p95_24h': '1',
        'breach_realized_spread_p50_24h': '3',
        'breach_eff_spread_p95_24h': '4',
        'worst_is_p95_bps_24h': '12.5',
        'worst_perm_impact_p95_bps_24h': '7.2',
        'worst_realized_spread_p50_bps_24h': '-2.3',
        'worst_eff_spread_p95_bps_24h': '6.8',
    })
    assert UP._value.get() == 1.0
    assert LAST_UPDATED_TS_MS._value.get() == 1700000000000.0
    assert ROWS_TOTAL.labels(window='24h')._value.get() == 120.0
    assert GROUPS_TOTAL.labels(window='7d')._value.get() == 21.0
    assert BREACH_GROUPS.labels(metric='realized_spread_p50')._value.get() == 3.0
    assert WORST_VALUE_BPS.labels(metric='realized_spread_p50')._value.get() == -2.3
