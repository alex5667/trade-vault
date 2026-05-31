from __future__ import annotations

from orderflow_services.cross_context_aggregator_v1 import LiqImbalanceTracker
from orderflow_services.pit_priors_rolling_v1 import seed_rolling_placeholders
from orderflow_services.tca_priors_exporter_v1 import seed_tca_placeholders


class _FakeRedis:
    def __init__(self) -> None:
        self.hashes: dict[str, dict[str, str]] = {}
        self.ttl: dict[str, int] = {}

    def exists(self, key: str) -> int:
        return 1 if key in self.hashes else 0

    def hset(self, key: str, mapping: dict[str, str]) -> None:
        self.hashes[key] = dict(mapping)

    def expire(self, key: str, ttl_sec: int) -> None:
        self.ttl[key] = ttl_sec


def test_liq_snapshot_returns_zero_state_without_events():
    tracker = LiqImbalanceTracker(track_symbols={"BTCUSDT"})
    state = tracker.snapshot("BTCUSDT", 1_700_000_000_000)
    assert state is not None
    assert state["long_n_1m"] == 0.0
    assert state["short_n_1m"] == 0.0
    assert state["imb_1m"] == 0.0
    assert state["imb_5m"] == 0.0


def test_seed_rolling_placeholders_creates_default_hashes():
    r = _FakeRedis()
    written = seed_rolling_placeholders(r, ["BTCUSDT"], ttl_sec=123, now_ms=1_700_000_000_000)
    assert written == 4
    assert "pit_priors:rolling:7d:BTCUSDT:default:asia" in r.hashes
    assert "pit_priors:rolling:7d:BTCUSDT:default:europe" in r.hashes
    assert "pit_priors:rolling:7d:BTCUSDT:default:us" in r.hashes
    assert "pit_priors:rolling:30d:BTCUSDT:default:all" in r.hashes
    assert r.hashes["pit_priors:rolling:30d:BTCUSDT:default:all"]["sample_count"] == "0.000000"
    assert r.ttl["pit_priors:rolling:30d:BTCUSDT:default:all"] == 123


def test_seed_tca_placeholders_creates_default_session_hashes():
    r = _FakeRedis()
    written = seed_tca_placeholders(r, ttl_sec=321, symbols=["ETHUSDT"])
    assert written == 3
    assert "tca:ema:ETHUSDT:default:asia" in r.hashes
    assert "tca:ema:ETHUSDT:default:europe" in r.hashes
    assert "tca:ema:ETHUSDT:default:us" in r.hashes
    assert r.hashes["tca:ema:ETHUSDT:default:us"]["samples"] == "0.000000"
    assert r.ttl["tca:ema:ETHUSDT:default:us"] == 321
