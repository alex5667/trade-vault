from __future__ import annotations


def test_cooldown_kind_is_case_insensitive_and_consistent():
    from handlers.cooldown_service import CooldownService

    cd = CooldownService("BTCUSDT", redis_client=None)
    # BREAKOUT has 30s default; we test that "BREAKOUT" and "breakout" share the same key
    cd.mark(kind="BREAKOUT", level_key="p:100", ts_ms=1000)

    # Within 30s -> should be blocked regardless of case
    assert cd.is_allowed(kind="breakout", level_key="p:100", ts_ms=10_000) is False
    assert cd.is_allowed(kind="BREAKOUT", level_key="p:100", ts_ms=10_000) is False

    # After 30s -> allowed
    assert cd.is_allowed(kind="breakout", level_key="p:100", ts_ms=40_001) is True
