from __future__ import annotations


def _mk_notifier():
    # Реальный import-path в вашем репо:
    # telegram-worker/ -> python package telegram_worker
    from telegram_worker.improved_notifier import ImprovedTelegramNotifier
    return ImprovedTelegramNotifier()


def test_format_includes_config_params_from_parsed(monkeypatch):
    monkeypatch.setenv("TG_SHOW_CONFIG_PARAMS", "1")
    monkeypatch.setenv("TG_CONFIG_PARAMS_MODE", "kv")
    monkeypatch.setenv("TG_CONFIG_PARAMS_MAX_LINES", "10")

    n = _mk_notifier()
    parsed = {
        "symbol": "ICPUSDT",
        "direction": "LONG",
        "entry": 4.7,
        "stop": 5.046,
        "tp": [4.627, 4.55, 4.506],
        "leverage": "19",
        "profitPct": 78.0,
        "exchange": "BYBIT",
        "orderType": "Market",
        "source": "Trading Signals",
        "timestamp": 1735227045000,
        "signal_settings": {"breakoutZThreshold": 2.0},
        "config_params": {"delta_window_ticks": 200, "tp_rr": 1.8},
    }
    raw = {"chat_title": "Trading Signals"}

    msg = n.format_signal_message(parsed, raw)
    assert "🧩 **Config Params:**" in msg
    assert "delta_window_ticks" in msg
    assert "tp_rr" in msg


def test_format_can_take_config_params_from_signal_settings(monkeypatch):
    monkeypatch.setenv("TG_SHOW_CONFIG_PARAMS", "1")
    monkeypatch.setenv("TG_CONFIG_PARAMS_MODE", "kv")

    n = _mk_notifier()
    parsed = {
        "symbol": "BTCUSDT",
        "direction": "SHORT",
        "entry": 42000,
        "stop": 43000,
        "tp": [41000],
        "leverage": "5",
        "profitPct": 2.0,
        "exchange": "BYBIT",
        "orderType": "Market",
        "source": "OrderFlow",
        "signal_settings": {"config_params": {"obi_threshold": 0.65}},
    }
    raw = {}

    msg = n.format_signal_message(parsed, raw)
    assert "obi_threshold" in msg


def test_config_params_rendered_from_signal_settings():
    n = _mk_notifier()
    parsed = {
        "symbol": "BTCUSDT",
        "direction": "SHORT",
        "entry": 42000,
        "stop": 43000,
        "tp": [41000, 40500],
        "leverage": "10",
        "exchange": "BYBIT",
        "orderType": "Market",
        "signal_settings": {
            "breakoutZThreshold": 2.2,
            "absorptionZThreshold": 2.6,
            "config_params": {
                "delta_z_threshold": 3.2,
                "min_signal_interval_sec": 60,
            },
        },
        # raw_text может быть, но НЕ должен bypass'ить форматирование (см. отдельный тест)
        "raw_text": "some original text",
    }
    raw = {"chat_title": "Outbox", "username": "scanner", "timestamp": 1735227045000}

    msg = n.format_signal_message(parsed, raw)
    assert "Signal Settings" in msg
    assert "breakoutZThreshold" not in msg  # мы печатаем human-friendly строки, не обязаны печатать ключ
    assert "Breakout Z" in msg
    assert "Config Params" in msg
    assert "delta_z_threshold" in msg
    assert "min_signal_interval_sec" in msg


def test_format_respects_max_keys(monkeypatch):
    monkeypatch.setenv("TG_SHOW_CONFIG_PARAMS", "1")
    monkeypatch.setenv("TG_CONFIG_PARAMS_MAX_KEYS", "2")
    monkeypatch.setenv("TG_CONFIG_PARAMS_MODE", "kv")

    n = _mk_notifier()
    parsed = {
        "symbol": "ETHUSDT",
        "direction": "LONG",
        "entry": 2200,
        "stop": 2100,
        "tp": [2300],
        "leverage": "3",
        "profitPct": 1.0,
        "exchange": "BYBIT",
        "orderType": "Market",
        "source": "OrderFlow",
        "config_params": {"c": 3, "b": 2, "a": 1},
    }
    raw = {}

    msg = n.format_signal_message(parsed, raw)
    # max_keys=2 => должны остаться только 2 ключа (a,b) из-за sort+slice
    assert "• a:" in msg
    assert "• b:" in msg
    assert "• c:" not in msg


def test_format_can_disable_config_params(monkeypatch):
    monkeypatch.setenv("TG_SHOW_CONFIG_PARAMS", "0")
    n = _mk_notifier()
    parsed = {
        "symbol": "ICPUSDT",
        "direction": "LONG",
        "entry": 4.7,
        "stop": 5.046,
        "tp": [4.627],
        "leverage": "19",
        "profitPct": 78.0,
        "exchange": "BYBIT",
        "orderType": "Market",
        "source": "Trading Signals",
        "config_params": {"delta_window_ticks": 200},
    }
    raw = {}
    msg = n.format_signal_message(parsed, raw)
    assert "Config Params" not in msg
