from services.atr_policy_resolver import ATRPolicyResolver


def test_resolver_miss_returns_canary_modes():
    r = ATRPolicyResolver(redis_url="redis://localhost:6379/0")
    r.enable = False
    out = r.resolve(
        source="CryptoOrderFlow"
        symbol="BTCUSDT"
        scenario="breakout"
        regime="trend_up"
        risk_horizon_bucket="short"
    )
    assert out["hit"] is False
    assert out["stop_ttl_mode"] == "canary"
    assert out["trailing_mode"] == "canary"
