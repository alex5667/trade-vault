import time
from dataclasses import dataclass

from common.metrics2 import extract_ts_ms, is_stale


@dataclass
class SnapA:
    ts: int  # может быть ms или sec


@dataclass
class SnapB:
    ts_ms: int


@dataclass
class SnapC:
    ts_utc: float  # sec


def test_extract_ts_ms_from_ts_seconds():
    s = SnapA(ts=1_700_000_000)  # sec
    ms = extract_ts_ms(s)
    assert ms == 1_700_000_000 * 1000


def test_extract_ts_ms_from_ts_ms():
    s = SnapA(ts=1_700_000_000_123)  # ms
    ms = extract_ts_ms(s)
    assert ms == 1_700_000_000_123


def test_extract_ts_ms_from_ts_ms_field():
    s = SnapB(ts_ms=1_700_000_000_555)
    ms = extract_ts_ms(s)
    assert ms == 1_700_000_000_555


def test_extract_ts_ms_from_ts_utc():
    s = SnapC(ts_utc=1_700_000_000.25)
    ms = extract_ts_ms(s)
    assert ms == int(1_700_000_000.25 * 1000)


def test_is_stale_true_when_too_old():
    now_ms = 2_000_000
    s = {"ts_ms": 1_000_000}
    assert is_stale(obj=s, now_ms=now_ms, max_age_ms=500_000) is True


def test_is_stale_false_when_fresh():
    now_ms = 2_000_000
    s = {"ts_ms": 1_900_000}
    assert is_stale(obj=s, now_ms=now_ms, max_age_ms=200_000) is False


def test_is_stale_false_for_future_snapshot():
    # future snapshot не считаем stale (это отдельная защита на ingress)
    now_ms = 2_000_000
    s = {"ts_ms": 2_100_000}
    assert is_stale(obj=s, now_ms=now_ms, max_age_ms=200_000) is False
