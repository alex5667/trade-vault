from types import SimpleNamespace

import pytest

from handlers.crypto_orderflow.scoring.confidence_scorer import _crypto_conf_factor


def test_crypto_conf_factor_returns_factor():
    """Test that _crypto_conf_factor returns factor value (0-1)"""

    ctx = SimpleNamespace()
    ctx.symbol = "BTCUSDT"
    # Add required attributes for _crypto_conf_factor
    ctx.atr_q_main = 0.5
    ctx.market_mode = "mixed"
    ctx.obi_sustained = True
    ctx.obi_avg = 0.5
    ctx.z_delta = 1.0
    ctx.spread_bps = 5.0
    ctx.l2_is_stale = False

    # The real _crypto_conf_factor method returns factor (0-1)
    conf_factor, parts = _crypto_conf_factor(ctx, "breakout")

    # Should return a number between 0 and 1
    assert isinstance(conf_factor, (int, float))
    assert 0.0 <= conf_factor <= 1.0
    assert isinstance(parts, dict)


def test_crypto_conf_factor_handles_different_signal_types():
    """Test that _crypto_conf_factor works with different signal types"""

    ctx = SimpleNamespace()
    ctx.symbol = "BTCUSDT"
    # Add required attributes
    ctx.atr_q_main = 0.5
    ctx.market_mode = "mixed"
    ctx.obi_sustained = True
    ctx.obi_avg = 0.5
    ctx.z_delta = 1.0
    ctx.spread_bps = 5.0
    ctx.l2_is_stale = False

    signal_types = ["breakout", "absorption", "sweep", "reclaim", "extreme"]

    for signal_type in signal_types:
        conf_factor, parts = _crypto_conf_factor(ctx, signal_type)

        # Should return valid factor for each signal type
        assert isinstance(conf_factor, (int, float))
        assert 0.0 <= conf_factor <= 1.0
        assert isinstance(parts, dict)


