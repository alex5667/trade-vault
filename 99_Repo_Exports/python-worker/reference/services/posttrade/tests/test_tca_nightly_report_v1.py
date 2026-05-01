from __future__ import annotations

from services.posttrade.tca_nightly_report_v1 import build_report, build_summary_state


def _row(sym: str, is_p95: float, imp_p95: float, rs_p50: float, eff_p95: float, n: int = 20):
    return {
        'sym': sym,
        'venue': 'binance',
        'session': 'eu',
        'tf': '1m',
        'kind': 'breakout',
        'side': 'LONG',
        'n': n,
        'is_p50_bps': is_p95 / 2.0,
        'is_p95_bps': is_p95,
        'is_p99_bps': is_p95 * 1.1,
        'eff_spread_p95_bps': eff_p95,
        'realized_spread_1s_p50_bps': rs_p50,
        'perm_impact_1s_p95_bps': imp_p95,
        'realized_spread_1s_neg_share': 0.5 if rs_p50 < 0 else 0.1,
    }


def test_build_report_counts_breaches_and_ranks_offenders():
    thr = {
        'max_is_p95_bps': 10.0,
        'max_perm_impact_p95_bps': 7.0,
        'min_realized_spread_p50_bps': -1.0,
        'max_eff_spread_p95_bps': 5.0,
    }
    rows_24h = [
        _row('BTCUSDT', 12.0, 8.0, -2.0, 6.0),
        _row('ETHUSDT', 9.0, 6.0, 0.5, 3.0),
    ]
    rows_7d = [
        _row('BTCUSDT', 11.0, 8.5, -1.5, 5.5, n=100),
        _row('ETHUSDT', 8.0, 5.0, 0.2, 2.5, n=80),
    ]

    report = build_report(rows_24h=rows_24h, rows_7d=rows_7d, thresholds=thr, top_n=5)
    s = report['summary']
    assert s['rows_24h_total'] == 40
    assert s['groups_24h'] == 2
    assert s['breach_groups_24h']['is_p95'] == 1
    assert s['breach_groups_24h']['perm_impact_p95'] == 1
    assert s['breach_groups_24h']['realized_spread_p50'] == 1
    assert s['breach_groups_24h']['eff_spread_p95'] == 1
    assert report['top_offenders_24h']['is_p95_bps'][0]['sym'] == 'BTCUSDT'
    assert report['top_offenders_24h']['adverse_realized_spread_1s_p50_bps'][0]['value'] == -2.0


def test_build_summary_state_extracts_top_values():
    thr = {
        'max_is_p95_bps': 10.0,
        'max_perm_impact_p95_bps': 7.0,
        'min_realized_spread_p50_bps': -1.0,
        'max_eff_spread_p95_bps': 5.0,
    }
    report = build_report(
        rows_24h=[_row('BTCUSDT', 12.5, 8.25, -2.25, 6.5)],
        rows_7d=[],
        thresholds=thr,
        top_n=3,
    )
    state = build_summary_state(report=report, now_ms=123456, dur_ms=789, ok=True)
    assert state['ok'] == '1'
    assert state['rows_24h_total'] == '20'
    assert state['breach_is_p95_24h'] == '1'
    assert state['worst_is_p95_bps_24h'] == '12.500000'
    assert state['worst_perm_impact_p95_bps_24h'] == '8.250000'
    assert state['worst_realized_spread_p50_bps_24h'] == '-2.250000'
