from unittest.mock import MagicMock

from services.ev_tp1_stats import EvTp1StatsConfig, RedisEvTp1StatsProvider


def test_ev_tp1_stats_provider_get_p():
    # Arrange
    redis = MagicMock()
    # Mock hgetall return
    # total=100, tp1_hits=60, ema_tp1=0.55
    redis.hgetall.return_value = {
        "total_trades": "100",
        "tp1_hits": "60",
        "ema_tp1": "0.55"
    }

    cfg = EvTp1StatsConfig(
        enabled=True,
        use_regime_dim=True,
        min_n=50,
        cache_ms=0, # disable cache for simplicity
        prefer_ema=True
    )
    prov = RedisEvTp1StatsProvider(redis, cfg)

    # Act
    p = prov.get_p_hit_tp1(kind="k", symbol="s", tf="t", regime="r")

    # Assert
    assert p == 0.55 # prefers EMA

def test_ev_tp1_stats_provider_not_enough_samples():
    redis = MagicMock()
    redis.hgetall.return_value = {
        "total_trades": "10", # < min_n=50
        "tp1_hits": "5",
        "ema_tp1": "0.5"
    }
    cfg = EvTp1StatsConfig(
        enabled=True,
        use_regime_dim=True,
        min_n=50,
        cache_ms=0,
        prefer_ema=True
    )
    prov = RedisEvTp1StatsProvider(redis, cfg)

    p = prov.get_p_hit_tp1(kind="k", symbol="s", tf="t", regime="r")
    assert p is None
