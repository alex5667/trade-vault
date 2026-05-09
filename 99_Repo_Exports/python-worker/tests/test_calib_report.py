from __future__ import annotations

from tools.calib_report import _bars_to_ready, _stability_stats


def test_stability_stats_basic():
    rows = [{"eff_quote_th": 0.01}, {"eff_quote_th": 0.02}, {"eff_quote_th": 0.015}]
    st = _stability_stats(rows, last_n=3)
    assert st["n"] == 3
    assert st["median"] > 0


def test_bars_to_ready_switch():
    rows = [
        {"ts_ms": 1000, "src": "static"},
        {"ts_ms": 6000, "src": "calib_p20"},
    ]
    assert _bars_to_ready(rows, tf_ms=1000) == 5
