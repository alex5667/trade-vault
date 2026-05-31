"""Regression guard: LuaScriptManager MUST expose sync variants alongside the
async ones.

Discovered 2026-05-19: the legacy `services/signal_dispatcher.py` holds a
synchronous `redis.Redis` client and calls `LuaScriptManager.preload_all()` and
`.execute(...)` without `await`. Those are `async def` methods, so the calls
returned coroutine objects that were never awaited, crashing the consumer loop
on every iteration with `TypeError: 'coroutine' object is not iterable`.
Container restart_count climbed to 1715 before this was caught.

Fix: `preload_all_sync()` / `execute_sync()` / `get_sha_sync()` were added to
LuaScriptManager. These tests pin the sync surface so a future refactor can't
silently delete them.
"""
from __future__ import annotations

import inspect
from unittest.mock import MagicMock

from services.dispatcher.lua_scripts import LuaScriptManager


def test_sync_methods_exist_on_manager():
    """Three sync entrypoints must be present and NOT coroutine functions."""
    for name in ("preload_all_sync", "execute_sync", "get_sha_sync"):
        method = getattr(LuaScriptManager, name, None)
        assert method is not None, (
            f"LuaScriptManager.{name} missing — sync dispatcher will crash with "
            "`TypeError: 'coroutine' object is not iterable`. See "
            "services/signal_dispatcher.py for caller."
        )
        assert not inspect.iscoroutinefunction(method), (
            f"LuaScriptManager.{name} is async — sync callers cannot await it."
        )


def test_async_methods_still_exist():
    """Async surface must coexist for async callers (e.g. signal_outbox_dispatcher)."""
    for name in ("preload_all", "execute", "get_sha"):
        method = getattr(LuaScriptManager, name, None)
        assert method is not None, f"async {name} disappeared"
        assert inspect.iscoroutinefunction(method), (
            f"{name} was demoted to sync — async callers expect a coroutine."
        )


def test_get_sha_sync_caches_and_returns_str():
    """SHA must be cached and returned as str (not bytes), matching async behavior."""
    fake_redis = MagicMock()
    fake_redis.script_load.return_value = b"deadbeef00112233445566778899aabbccddeeff"
    mgr = LuaScriptManager(fake_redis)

    sha1 = mgr.get_sha_sync("zpop_due")
    assert isinstance(sha1, str), "SHA must be str (not bytes) for stable label/log behavior"

    # Second call must hit cache — no extra script_load.
    sha2 = mgr.get_sha_sync("zpop_due")
    assert sha2 == sha1
    assert fake_redis.script_load.call_count == 1


def test_execute_sync_uses_evalsha_with_noscript_fallback():
    """Hot path: evalsha first, eval on NOSCRIPT — same contract as async execute()."""
    fake_redis = MagicMock()
    fake_redis.script_load.return_value = "sha-cached"
    fake_redis.evalsha.return_value = ["ok", "result"]
    mgr = LuaScriptManager(fake_redis)

    result = mgr.execute_sync("zpop_due", keys=["k"], args=["1", "10"])
    assert result == ["ok", "result"]
    fake_redis.evalsha.assert_called_once()

    # NOSCRIPT fallback path.
    fake_redis.evalsha.reset_mock()
    fake_redis.evalsha.side_effect = [Exception("NOSCRIPT no script"), ["ok", "retry"]]
    mgr._sha_cache.clear()
    fake_redis.script_load.return_value = "sha-reload"
    result = mgr.execute_sync("zpop_due", keys=["k"], args=["1", "10"])
    assert result == ["ok", "retry"]
    assert fake_redis.evalsha.call_count == 2  # initial fail + reload retry


def test_signal_dispatcher_uses_sync_variants_only():
    """services/signal_dispatcher.py is a SYNC dispatcher. It must never call
    the async LuaScriptManager methods directly (those would return coroutines).
    """
    src_path = "services/signal_dispatcher.py"
    with open(src_path, encoding="utf-8") as fh:
        src = fh.read()

    # No bare async calls
    forbidden = (
        "self.lua_scripts.preload_all()",
        "self.lua_scripts.execute(",
        "self.lua_scripts.get_sha(",
    )
    for f in forbidden:
        assert f not in src, (
            f"{src_path} contains bare async call `{f}` — must use the *_sync "
            "variant. The async return is a coroutine that crashes the dispatcher "
            "loop with `TypeError: 'coroutine' object is not iterable`."
        )

    # At least one sync call must exist (sanity)
    assert "self.lua_scripts.preload_all_sync()" in src
    assert "self.lua_scripts.execute_sync(" in src
