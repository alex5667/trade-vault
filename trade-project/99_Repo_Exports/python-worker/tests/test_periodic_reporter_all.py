from unittest.mock import MagicMock, patch

import fakeredis
import pytest

from services.periodic_reporter import PeriodicReporter
from utils.time_utils import get_ny_time_millis

# Mock external imports that might require environment
with patch.dict("sys.modules", {
    "services.edge_gate_reporter": MagicMock(),
    "analyze_trailing_vs_baseline_postgres": MagicMock(),
    "services.trailing_size_recommender": MagicMock(),
}):
    pass

@pytest.fixture
def reporter(monkeypatch):
    # Use fakeredis for the reporter
    fake_r = fakeredis.FakeRedis(decode_responses=True)
    # Add ping method if missing
    if not hasattr(fake_r, "ping"):
        fake_r.ping = MagicMock(return_value=True)

    # Patch get_redis and from_url
    monkeypatch.setattr("services.periodic_reporter.get_redis", lambda: fake_r)
    monkeypatch.setattr("redis.from_url", lambda url, **kwargs: fake_r)

    # Also patch ReportingService inner redis creation if it doesn't use the patched from_url (it probably does)
    # But ReportingService might import redis separately.
    # Let's patch 'services.reporting_service.redis.from_url' just in case
    # or better, patch ReportingService init to not explode.

    # Using patch.dict sys.modules in outer scope handles some, but let's be safe.

    # Set env to force use of stream or at least not fail
    monkeypatch.setenv("PERIODIC_REPORT_USE_ZSET", "0")

    rep = PeriodicReporter()
    rep.redis = fake_r  # Ensure it uses our fake
    rep.reporting = MagicMock() # Mock the actual sending
    return rep, fake_r

def test_iter_recent_trades_window_all_symbol_aggregated(reporter):
    rep, r = reporter

    now_ms = get_ny_time_millis()

    # Needs to match canon_source("CryptoOrderFlow") -> "CryptoOrderFlow"
    # Needs to match filter in _iter_recent_trades_window

    # 1. BTC Trade (Target Source)
    btc_trade = {
        "id": "ord1",
        "order_id": "ord1",
        "symbol": "BTCUSDT",
        "source": "CryptoOrderFlow",
        "strategy": "cryptoorderflow",
        "status": "closed",
        "closed_time": str(now_ms - 1000),
        "exit_ts_ms": str(now_ms - 1000),
        "pnl_net": "10.0",
        "pnl": "10.0",
        "fees": "-1.0",
        "close_reason": "TP"
    }

    # 2. ETH Trade (Target Source)
    eth_trade = {
        "id": "ord2",
        "order_id": "ord2",
        "symbol": "ETHUSDT",
        "source": "CryptoOrderFlow",
        "strategy": "cryptoorderflow",
        "status": "closed",
        "closed_time": str(now_ms - 2000),
        "exit_ts_ms": str(now_ms - 2000),
        "pnl_net": "5.0",
        "pnl": "5.0",
        "fees": "-0.5",
        "close_reason": "TP"
    }

    # 3. XAU Trade (Excluded)
    xau_trade = {
        "id": "ord3",
        "order_id": "ord3",
        "symbol": "",
        "source": "CryptoOrderFlow",
        "strategy": "cryptoorderflow",
        "status": "closed",
        "closed_time": str(now_ms - 3000),
        "exit_ts_ms": str(now_ms - 3000),
        "pnl_net": "100.0",
        "pnl": "100.0",
        "fees": "-5.0",
        "close_reason": "TP"
    }

    # 4. Other Source Trade (Excluded)
    other_trade = {
        "id": "ord4",
        "order_id": "ord4",
        "symbol": "BTCUSDT",
        "source": "TechnicalAnalysis",
        "strategy": "ta",
        "status": "closed",
        "closed_time": str(now_ms - 4000),
        "exit_ts_ms": str(now_ms - 4000),
        "pnl_net": "20.0",
        "pnl": "20.0",
        "fees": "-2.0",
        "close_reason": "TP"
    }

    # Add to stream - MOCK xrevrange instead of xadd
    # xrevrange returns list of (id, fields)

    # helper to format for xrevrange
    stream_data = []
    for i, t in enumerate([btc_trade, eth_trade, xau_trade, other_trade]):
       # Redis stream responses are often byte keys/values if not decode_responses,
       # but we use decode_responses=True in reporter.
       # xrevrange format: [(msg_id, {field: val}), ...]
       msg_id = f"{now_ms - (i+1)*1000}-0"
       stream_data.append((msg_id, t))

    # Mock xrevrange on the fake redis
    # Note: we need to ensure the reporter.redis refers to the object we can patch
    # The fixture uses fake_r, we can patch its xrevrange method
    r.xrevrange = MagicMock(return_value=stream_data)
    r.xadd = MagicMock() # prevent error if called anywhere

    # Act: fetch recent trades for "ALL"
    trades = rep._iter_recent_trades_window(
        strategy="cryptoorderflow", # derived from source usually
        symbol="ALL",
        tf="tick", # irrelevant for ALL stream scan
        source="CryptoOrderFlow",
        window_seconds=3600
    )

    # Assert
    # Should get BTC and ETH.
    # Should NOT get XAU.
    # Should NOT get Other Source.

    ids = [t["order_id"] for t in trades]
    assert "ord1" in ids, "BTC trade should be included"
    assert "ord2" in ids, "ETH trade should be included"
    assert "ord3" not in ids, "XAU trade should be EXCLUDED"
    assert "ord4" not in ids, "Other source trade should be EXCLUDED"

    print("Trades found:", ids)

def test_send_report_for_pair_aggregated(reporter):
    rep, r = reporter

    # Mock _iter_recent_trades_window to return simple list
    rep._iter_recent_trades_window = MagicMock(return_value=[
        {"order_id": "1", "pnl": "10", "pnl_net": "10", "fees": "-1", "close_reason": "TP", "source": "CryptoOrderFlow", "symbol": "BTCUSDT"},
        {"order_id": "2", "pnl": "-5", "pnl_net": "-5", "fees": "-1", "close_reason": "SL", "source": "CryptoOrderFlow", "symbol": "ETHUSDT"},
    ])

    # Act
    rep.send_report_for_pair("CryptoOrderFlow", "ALL", window_seconds=3600)

    # Verify mapping sends aggregated stats
    assert rep.reporting.send_telegram_message.call_count >= 1

    # Check all message parts
    all_msgs = [call[0][0] for call in rep.reporting.send_telegram_message.call_args_list]
    combined_msg = "".join(all_msgs)

    print("Report Messages combined:\n", combined_msg)

    assert "Отчет: CryptoOrderFlow / ALL" in combined_msg
    assert "Сделок: <b>2</b>" in combined_msg
    assert "P/L net: <b>+5.00</b>" in combined_msg # 10 - 5 = 5

def test_check_and_trigger_report_triggers_all(reporter, monkeypatch):
    rep, r = reporter
    monkeypatch.setattr("services.periodic_reporter.get_reporter_instance", lambda: rep)

    rep.send_report_for_pair = MagicMock()

    # Mock time
    import datetime
    now = datetime.datetime(2026, 1, 15, 12, 0, 0, tzinfo=datetime.UTC)

    with patch("services.periodic_reporter.datetime") as mock_dt:
        mock_dt.fromtimestamp.return_value = now
        mock_dt.now.return_value = now

        # Act: trigger for a single symbol
        # Use low-level call to bypass global instance if needed, but we patched it.
        # Note checking 'services.periodic_reporter.check_and_trigger_report' calls get_reporter_instance

        # We call the method on instance directly for ease
        rep._check_and_trigger_report("CryptoOrderFlow", "BTCUSDT", "trades", "ord1")

        # Assert
        # Should call send_report_for_pair for BTCUSDT (hourly)
        # AND for ALL (hourly)

        calls = rep.send_report_for_pair.call_args_list
        # Expecting calls: (CryptoOrderFlow, BTCUSDT), (CryptoOrderFlow, ALL)

        symbols_called = [c[0][1] for c in calls]
        assert "BTCUSDT" in symbols_called
        assert "ALL" in symbols_called

        print("Called symbols:", symbols_called)
