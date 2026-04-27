from __future__ import annotations


def test_preformatted_xauusd_uses_raw_text():
    from telegram_worker.improved_notifier import ImprovedTelegramNotifier

    n = ImprovedTelegramNotifier()
    parsed = {"raw_text": "READY TEXT", "symbol": "XAUUSD", "is_xauusd": True}
    raw = {"is_xauusd": True, "text": "RAW FALLBACK"}

    msg = n.format_signal_message(parsed, raw)
    assert msg == "READY TEXT"


def test_raw_text_does_not_bypass_formatting_for_normal_signals():
    """
    CRITICAL regression test:
      раньше format_signal_message() возвращал parsed["raw_text"] для любых сигналов,
      из-за чего "красивый формат" и блок settings/meta никогда не показывались.
    """
    from telegram_worker.improved_notifier import ImprovedTelegramNotifier

    n = ImprovedTelegramNotifier()
    parsed = {
        "symbol": "ICPUSDT",
        "direction": "LONG",
        "entry": 4.7,
        "stop": 5.046,
        "tp": [4.627, 4.55, 4.506],
        "leverage": "19",
        "exchange": "BYBIT",
        "orderType": "Market",
        "raw_text": "ORIGINAL CHANNEL TEXT",
    }
    raw = {"chat_title": "Trading Signals", "username": "x", "text": "ORIGINAL CHANNEL TEXT"}

    msg = n.format_signal_message(parsed, raw)
    assert "ТОРГОВЫЙ СИГНАЛ" in msg
    assert msg != "ORIGINAL CHANNEL TEXT"
