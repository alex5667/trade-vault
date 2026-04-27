from __future__ import annotations

from datetime import datetime, timezone

from core.calendar_flags import (
    calendar_flags_utc,
    is_end_of_month_utc,
    is_end_of_quarter_utc,
    day_of_month_utc,
    day_of_quarter_utc,
)
from core.ml_feature_schema_v7_of import MLFeatureSchemaV7OF


def _ts_ms(y: int, m: int, d: int, hh: int = 0, mm: int = 0, ss: int = 0) -> int:
    return int(datetime(y, m, d, hh, mm, ss, tzinfo=timezone.utc).timestamp() * 1000)


def test_calendar_flags_utc_boundary_eom_non_leap() -> None:
    ts = _ts_ms(2026, 2, 28, 12, 0, 0)
    f = calendar_flags_utc(ts)
    assert f["cal_eom_utc"] == 1
    assert f["cal_eoq_utc"] == 0
    assert f["cal_dom_utc"] == 28
    assert f["cal_doq_utc"] == 59  # Jan(31) + Feb(28)


def test_calendar_flags_utc_boundary_leap_day() -> None:
    ts = _ts_ms(2024, 2, 29, 23, 59, 59)
    f = calendar_flags_utc(ts)
    assert f["cal_eom_utc"] == 1
    assert f["cal_eoq_utc"] == 0
    assert f["cal_dom_utc"] == 29
    assert f["cal_doq_utc"] == 60  # Jan(31) + Feb(29)


def test_calendar_flags_utc_eoq_mar31() -> None:
    ts = _ts_ms(2026, 3, 31, 0, 0, 0)
    f = calendar_flags_utc(ts)
    assert f["cal_eom_utc"] == 1
    assert f["cal_eoq_utc"] == 1
    assert f["cal_dom_utc"] == 31
    assert f["cal_doq_utc"] == 90  # Q1: 31 + 28 + 31


def test_calendar_flags_utc_q2_start() -> None:
    ts = _ts_ms(2026, 4, 1, 0, 0, 0)
    f = calendar_flags_utc(ts)
    assert f["cal_eom_utc"] == 0
    assert f["cal_eoq_utc"] == 0
    assert f["cal_dom_utc"] == 1
    assert f["cal_doq_utc"] == 1


def test_helpers_match_calendar_flags() -> None:
    ts = _ts_ms(2026, 6, 30, 18, 30, 0)
    assert is_end_of_month_utc(ts) == 1
    assert is_end_of_quarter_utc(ts) == 1
    assert day_of_month_utc(ts) == 30
    assert day_of_quarter_utc(ts) >= 1


def test_schema_v7_includes_calendar_keys() -> None:
    s = MLFeatureSchemaV7OF()
    for k in ("cal_eom_utc", "cal_eoq_utc"):
        assert k in s.bool_keys
    for k in ("cal_dom_utc", "cal_doq_utc"):
        assert k in s.num_keys
