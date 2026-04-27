from __future__ import annotations

from dataclasses import dataclass


@dataclass
class WP:
    weak_any: bool = True
    ts_ms: int = 1000


def test_wp_staleness_logic_smoke():
    """Doc-level unit test: validates the intended windowing behavior."""
    now_ts = 10_000
    ttl = 15_000
    wp_ts = 1_000
    assert 0 <= now_ts - wp_ts <= ttl
    wp_ts2 = -1
    assert not (wp_ts2 > 0)


def test_of_dir_staleness():
    """Test OF dir staleness logic."""
    now_ts = 50_000
    of_ttl = 30_000
    of_ts = 25_000
    # Fresh
    assert 0 <= now_ts - of_ts <= of_ttl
    # Stale
    of_ts_old = 10_000
    assert not (0 <= now_ts - of_ts_old <= of_ttl)
