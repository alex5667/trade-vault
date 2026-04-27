from __future__ import annotations

from services.stats_aggregator import _parse_json_dict_strfloat


def test_parse_json_dict_strfloat_keeps_negative_values():
    # MAE PnL can be negative; we keep raw PnL and later convert to bps via abs().
    d = _parse_json_dict_strfloat('{"60000": -12.3, "120000": 4.0}')
    assert d[60000] == -12.3
    assert d[120000] == 4.0
