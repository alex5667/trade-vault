import pytest
from types import SimpleNamespace

from services.time_be_exit_policy import TimeBeExitConfig, should_time_be_exit

@pytest.fixture
def base_cfg():
    return TimeBeExitConfig(
        enabled=True,
        mode="ENFORCE",
        after_ms=900000,
        min_pnl_net_bps=1.5,
        max_loss_net_bps=-2.0,
        require_no_tp1=True,
        disable_when_trailing=True,
        max_price_age_ms=5000,
    )

@pytest.fixture
def mock_pos():
    return SimpleNamespace(
        entry_ts_ms=1000000,
        tp1_hit=False,
        trailing_active=False,
    )

def test_no_close_before_min_hold(base_cfg, mock_pos):
    # Age is 500,000 ms, which is less than 900,000 ms
    now_ms = 1500000
    should_close, reason, mode = should_time_be_exit(
        mock_pos, now_ms, pnl_net_bps=2.0, last_price_ts_ms=now_ms, cfg=base_cfg
    )
    assert not should_close
    assert reason == "TIME_BE_EXIT_TOO_YOUNG"

def test_close_after_max_hold_profit_flat(base_cfg, mock_pos):
    # Age is 1,000,000 ms, which is greater than 900,000 ms
    now_ms = 2000000
    should_close, reason, mode = should_time_be_exit(
        mock_pos, now_ms, pnl_net_bps=1.6, last_price_ts_ms=now_ms, cfg=base_cfg
    )
    assert should_close
    assert reason == "TIME_BE_EXIT_PROFIT_FLAT"

def test_close_after_max_hold_near_flat(base_cfg, mock_pos):
    # Age is 1,000,000 ms
    now_ms = 2000000
    should_close, reason, mode = should_time_be_exit(
        mock_pos, now_ms, pnl_net_bps=-1.0, last_price_ts_ms=now_ms, cfg=base_cfg
    )
    assert should_close
    assert reason == "TIME_BE_EXIT_NEAR_FLAT"

def test_no_close_if_loss_too_big(base_cfg, mock_pos):
    # Age is 1,000,000 ms, but loss is -5 bps, which is worse than max_loss_net_bps (-2.0)
    now_ms = 2000000
    should_close, reason, mode = should_time_be_exit(
        mock_pos, now_ms, pnl_net_bps=-5.0, last_price_ts_ms=now_ms, cfg=base_cfg
    )
    assert not should_close
    assert reason == "NOT_BREAKEVEN"

def test_skip_when_trailing_active(base_cfg, mock_pos):
    now_ms = 2000000
    mock_pos.trailing_active = True
    should_close, reason, mode = should_time_be_exit(
        mock_pos, now_ms, pnl_net_bps=2.0, last_price_ts_ms=now_ms, cfg=base_cfg
    )
    assert not should_close
    assert reason == "TIME_BE_EXIT_TRAILING_ACTIVE_SKIP"

def test_deny_when_price_stale(base_cfg, mock_pos):
    now_ms = 2000000
    # Price is 10,000 ms old, threshold is 5,000 ms
    last_price_ts_ms = 1990000
    should_close, reason, mode = should_time_be_exit(
        mock_pos, now_ms, pnl_net_bps=2.0, last_price_ts_ms=last_price_ts_ms, cfg=base_cfg
    )
    assert not should_close
    assert reason == "TIME_BE_EXIT_PRICE_STALE"

def test_skip_when_tp1_hit(base_cfg, mock_pos):
    now_ms = 2000000
    mock_pos.tp1_hit = True
    should_close, reason, mode = should_time_be_exit(
        mock_pos, now_ms, pnl_net_bps=2.0, last_price_ts_ms=now_ms, cfg=base_cfg
    )
    assert not should_close
    assert reason == "TIME_BE_EXIT_TP1_ALREADY_HIT_SKIP"

def test_shadow_mode_does_not_close_but_returns_would_close(mock_pos):
    cfg = TimeBeExitConfig(
        enabled=True,
        mode="SHADOW",
        after_ms=900000,
        min_pnl_net_bps=1.5,
        max_loss_net_bps=-2.0,
        require_no_tp1=True,
        disable_when_trailing=True,
        max_price_age_ms=5000,
    )
    now_ms = 2000000
    should_close, reason, mode = should_time_be_exit(
        mock_pos, now_ms, pnl_net_bps=2.0, last_price_ts_ms=now_ms, cfg=cfg
    )
    assert not should_close
    assert reason == "TIME_BE_EXIT_PROFIT_FLAT_SHADOW"
    assert mode == "SHADOW"
