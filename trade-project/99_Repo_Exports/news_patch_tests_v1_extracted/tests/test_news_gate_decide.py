import json

import pytest


class FakeRedis:
    def __init__(self):
        self._kv = {}
        self._hash = {}

    def get(self, key):
        return self._kv.get(key)

    def set(self, key, value):
        self._kv[key] = value

    def hset(self, key, mapping):
        self._hash.setdefault(key, {}).update(mapping)

    def hgetall(self, key):
        return dict(self._hash.get(key, {}))


def test_news_gate_manual_hard_block():
    from news_gate import NewsGate

    r = FakeRedis()
    r.set(
        "news:hi:active",
        json.dumps({"active": 1, "until_ts_ms": 2_000_000, "reason": "NFP"}),
    )

    gate = NewsGate(redis_client=r, asset_class="crypto", window_sec=300, grade_min=4)
    dec = gate.decide(now_ts_ms=1_500_000, symbols=("BTCUSDT",))

    assert dec.hard_block is True
    assert dec.risk_factor_bps == 0
    assert dec.until_ts_ms == 2_000_000


def test_news_gate_calendar_hard_block_uses_event_ts_ms():
    from news_gate import NewsGate

    r = FakeRedis()
    # asset_class forex should normalize to fx
    r.hset(
        "calendar:agg:fx",
        {
            "event_grade_id": 4,
            "event_ts_ms": 1_000_000,
            "title": "CPI",
        },
    )

    gate = NewsGate(redis_client=r, asset_class="forex", window_sec=300, grade_min=4)

    # 100 sec before event => within window => hard block
    dec = gate.decide(now_ts_ms=900_000)
    assert dec.hard_block is True
    assert dec.hard_reason == "calendar_hi_impact"
    assert dec.risk_factor_bps == 0


@pytest.mark.parametrize(
    "grade,expected_max",
    [
        (2, 5000),
        (3, 3500),
        (4, 2500),
    ],
)
def test_news_gate_calendar_soft_factor_by_grade(grade, expected_max):
    from news_gate import NewsGate

    r = FakeRedis()
    r.hset(
        "calendar:agg:crypto",
        {
            "event_grade_id": grade,
            "event_ts_ms": 1_000_000,
        },
    )

    gate = NewsGate(
        redis_client=r,
        asset_class="crypto",
        window_sec=300,
        grade_min=4,  # hard only for grade 4
        soft_enabled=True,
        soft_window_sec=300,
    )

    # within soft window
    dec = gate.decide(now_ts_ms=900_000)

    if grade >= 4:
        # grade 4 is also within hard-block window? only if grade_min<=4; grade_min=4 so yes
        assert dec.hard_block is True
    else:
        assert dec.hard_block is False
        assert 0 <= dec.risk_factor_bps <= 10000
        assert dec.risk_factor_bps <= expected_max
