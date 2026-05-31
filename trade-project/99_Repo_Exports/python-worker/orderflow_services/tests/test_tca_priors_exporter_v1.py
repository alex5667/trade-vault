from __future__ import annotations

import pytest

from orderflow_services.tca_priors_exporter_v1 import TCAEMAState, _extract_tca_update


class _StubRedis:
    def __init__(self) -> None:
        self.hashes: dict[str, dict[str, str]] = {}
        self.ttl: dict[str, int] = {}

    def hset(self, key: str, mapping: dict[str, str]) -> None:
        self.hashes[key] = dict(mapping)

    def expire(self, key: str, ttl_sec: int) -> None:
        self.ttl[key] = int(ttl_sec)


def test_extract_tca_update_reads_all_tca_features_from_fill_message() -> None:
    parsed = _extract_tca_update(
        {
            "symbol": "BTCUSDT",
            "kind": "breakout",
            "side": "BUY",
            "ts_ms": "1710000000000",
            "price": "50010",
            "arrival_mid": "50000",
            "eff_spread_bps": "4.0",
            "realized_spread_1s_bps": "-1.5",
            "realized_spread_5s_bps": "-2.5",
            "perm_impact_1s_bps": "1.25",
            "perm_impact_5s_bps": "2.75",
            "is_bps": "0.0",
        }
    )

    assert parsed is not None
    symbol, kind, session, ts_ms, metrics = parsed
    assert symbol == "BTCUSDT"
    assert kind == "breakout"
    assert session == "us"
    assert ts_ms == 1710000000000
    assert metrics == {
        "eff_spread_bps": 4.0,
        "realized_1s_bps": -1.5,
        "realized_5s_bps": -2.5,
        "perm_1s_bps": 1.25,
        "perm_5s_bps": 2.75,
        "is_bps": 0.0,
    }


def test_extract_tca_update_recovers_missing_realized_and_perm_from_mid_after() -> None:
    parsed = _extract_tca_update(
        {
            "symbol": "BTCUSDT",
            "kind": "breakout",
            "side": "BUY",
            "ts_ms": "1710000000000",
            "price": "50010",
            "arrival_mid": "50000",
            "eff_spread_bps": "4.0",
            "mid_after_1s_bps": "1.0",
            "mid_after_5s_bps": "2.0",
        }
    )

    assert parsed is not None
    *_rest, metrics = parsed
    assert metrics == {
        "eff_spread_bps": 4.0,
        "realized_1s_bps": 2.0,
        "realized_5s_bps": 0.0,
        "perm_1s_bps": 1.0,
        "perm_5s_bps": 2.0,
        "is_bps": 4.0,
    }


def test_extract_tca_update_recovers_missing_realized_and_perm_from_mid_after_sell_side() -> None:
    parsed = _extract_tca_update(
        {
            "symbol": "ETHUSDT",
            "kind": "continuation",
            "side": "SELL",
            "ts_ms": "1710000001000",
            "price": "2990",
            "arrival_mid": "3000",
            "eff_spread_bps": "66.666667",
            "mid_after_1s_bps": "-5.0",
        }
    )

    assert parsed is not None
    *_rest, metrics = parsed
    assert metrics["perm_1s_bps"] == 5.0
    assert metrics["realized_1s_bps"] == pytest.approx(56.666667)


def test_tca_state_persists_seven_feature_contract_to_redis_hash() -> None:
    parsed = _extract_tca_update(
        {
            "symbol": "ETHUSDT",
            "kind": "continuation",
            "side": "SELL",
            "ts_ms": "1710000001000",
            "price": "2990",
            "arrival_mid": "3000",
            "eff_spread_bps": "66.666667",
            "realized_spread_1s_bps": "-3.0",
            "realized_spread_5s_bps": "-1.0",
            "perm_impact_1s_bps": "5.0",
            "perm_impact_5s_bps": "7.5",
            "is_bps": "8.0",
        }
    )
    assert parsed is not None
    symbol, kind, session, ts_ms, metrics = parsed

    redis_client = _StubRedis()
    state = TCAEMAState(ema_half_life=10.0, ttl_sec=123)
    new_state = state.update(
        redis_client,
        symbol,
        kind,
        session,
        eff_spread_bps=metrics["eff_spread_bps"],
        realized_1s_bps=metrics["realized_1s_bps"],
        realized_5s_bps=metrics["realized_5s_bps"],
        perm_1s_bps=metrics["perm_1s_bps"],
        perm_5s_bps=metrics["perm_5s_bps"],
        is_bps=metrics["is_bps"],
        ts_ms=ts_ms,
    )

    assert new_state["samples"] == 1.0
    redis_key = f"tca:ema:{symbol}:{kind}:{session}"
    assert redis_key in redis_client.hashes
    assert redis_client.ttl[redis_key] == 123

    payload = redis_client.hashes[redis_key]
    for field in (
        "eff_spread",
        "realized_1s",
        "realized_5s",
        "perm_1s",
        "perm_5s",
        "is_bps",
        "samples",
    ):
        assert field in payload

    assert payload["eff_spread"] == "66.666667"
    assert payload["realized_1s"] == "-3.000000"
    assert payload["realized_5s"] == "-1.000000"
    assert payload["perm_1s"] == "5.000000"
    assert payload["perm_5s"] == "7.500000"
    assert payload["is_bps"] == "8.000000"
    assert payload["samples"] == "1.000000"
