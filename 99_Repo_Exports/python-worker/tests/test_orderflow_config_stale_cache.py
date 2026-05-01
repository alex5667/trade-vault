from __future__ import annotations
"""Regression tests for OrderFlowConfigLoader.build_symbol_config fallback paths.

Primary fix: when the pipeline preload has been failing (`_last_preload_ts`
is stale) but a cached override exists within the stale-tolerance window,
use the cached value instead of firing a per-symbol hgetall — per-symbol
hgetalls saturate the hot-path Redis pool and trigger the cascading
"Таймаут загрузки config:orderflow:*" storm we observed.
"""


import asyncio
import time
from typing import Any, Dict
from unittest.mock import AsyncMock, MagicMock

import pytest
from redis.exceptions import RedisError

from services.orderflow import configuration as cfg_mod
from services.orderflow.configuration import OrderFlowConfigLoader


@pytest.fixture
def fake_redis():
    r = MagicMock()
    r.hgetall = AsyncMock(return_value={})
    return r


def _config_orderflow_calls(fake_redis) -> list:
    """Return only hgetall calls targeting config:orderflow:*.

    build_symbol_config also touches other keys (e.g. symbol:trailing_cfg:*),
    which are unrelated to the preload-vs-individual fallback we're testing.
    """
    return [
        c for c in fake_redis.hgetall.call_args_list
        if c.args and isinstance(c.args[0], str) and c.args[0].startswith("config:orderflow:")
    ]


def _assert_no_config_orderflow_hgetall(fake_redis) -> None:
    calls = _config_orderflow_calls(fake_redis)
    assert calls == [], (
        f"Expected no config:orderflow:* hgetall calls, got: {calls}"
    )


def _assert_config_orderflow_hgetall_called(fake_redis) -> None:
    calls = _config_orderflow_calls(fake_redis)
    assert len(calls) == 1, (
        f"Expected exactly one config:orderflow:* hgetall, got {len(calls)}: {calls}"
    )


@pytest.fixture
def loader(fake_redis, monkeypatch):
    # Tight TTL so we can age the cache by seconds, not minutes.
    monkeypatch.setenv("ORDERFLOW_CONFIG_CACHE_TTL_S", "10.0")
    loader = OrderFlowConfigLoader(fake_redis)
    loader._cache_ttl_sec = 10.0
    return loader


class TestStaleCacheFallback:
    @pytest.mark.asyncio
    async def test_fresh_cache_no_redis_call(self, loader, fake_redis):
        now = time.time()
        loader._cache["BTCUSDT"] = ({"delta_z_threshold": "2.5"}, now)
        await loader.build_symbol_config("BTCUSDT")
        _assert_no_config_orderflow_hgetall(fake_redis)

    @pytest.mark.asyncio
    async def test_preload_recent_uses_cache_no_individual(self, loader, fake_redis):
        # Cache is stale-but-within-TTL'd of preload; build_symbol_config
        # should reuse cached overrides and skip hgetall.
        now = time.time()
        loader._cache["BTCUSDT"] = ({"delta_z_threshold": "2.5"}, now - 20)
        loader._last_preload_ts = now - 5  # < TTL (10s)
        await loader.build_symbol_config("BTCUSDT")
        _assert_no_config_orderflow_hgetall(fake_redis)

    @pytest.mark.asyncio
    async def test_stale_preload_stale_cache_uses_stale_not_hgetall(
        self, loader, fake_redis, monkeypatch
    ):
        """Regression: preload_ts expired, cache is older than TTL but younger
        than stale_limit → use stale cache, DO NOT fire hgetall."""
        monkeypatch.setattr(cfg_mod, "_CONFIG_STALE_MULT", 4.0)
        loader._cache_ttl_sec = 10.0
        now = time.time()
        # Cache is 30s old → past TTL=10s but within stale_limit=40s.
        loader._cache["BTCUSDT"] = ({"delta_z_threshold": "2.5"}, now - 30)
        loader._last_preload_ts = now - 60  # >> TTL; preload is stale.
        await loader.build_symbol_config("BTCUSDT")
        _assert_no_config_orderflow_hgetall(fake_redis)

    @pytest.mark.asyncio
    async def test_stale_preload_no_cache_falls_through_to_hgetall(
        self, loader, fake_redis
    ):
        """When there is no cache at all, we must still try hgetall (cold start)."""
        loader._last_preload_ts = 0.0
        fake_redis.hgetall.return_value = {"delta_z_threshold": "3.0"}
        await loader.build_symbol_config("NEWUSDT")
        _assert_config_orderflow_hgetall_called(fake_redis)

    @pytest.mark.asyncio
    async def test_cache_beyond_stale_limit_still_tries_hgetall(
        self, loader, fake_redis, monkeypatch
    ):
        """Cache beyond stale_limit × TTL → reach for hgetall as last resort."""
        monkeypatch.setattr(cfg_mod, "_CONFIG_STALE_MULT", 2.0)
        loader._cache_ttl_sec = 10.0
        now = time.time()
        # Cache is 30s old, stale_limit is 20s → beyond tolerance.
        loader._cache["OLDUSDT"] = ({"delta_z_threshold": "1.0"}, now - 30)
        loader._last_preload_ts = now - 120
        fake_redis.hgetall.return_value = {"delta_z_threshold": "1.5"}
        await loader.build_symbol_config("OLDUSDT")
        _assert_config_orderflow_hgetall_called(fake_redis)


class TestPreloadFailureEscalation:
    @pytest.mark.asyncio
    async def test_all_chunks_timeout_logs_error(self, loader, fake_redis, caplog):
        """When every chunk fails, preload must log at ERROR so the silent-fail
        mode is visible without having to grep WARNING."""
        pipe = MagicMock()
        pipe.hgetall = MagicMock()
        pipe.execute = AsyncMock(side_effect=asyncio.TimeoutError())
        fake_redis.pipeline = MagicMock(return_value=pipe)

        with caplog.at_level("ERROR", logger="crypto_orderflow.config"):
            await loader.preload_configs(["BTCUSDT", "ETHUSDT"])

        error_records = [r for r in caplog.records if r.levelname == "ERROR"]
        assert any("ALL" in r.message and "chunks failed" in r.message for r in error_records), \
            f"Expected ERROR log about ALL chunks failing, got: {[r.message for r in error_records]}"
        # preload_ts must stay at 0 on full failure
        assert loader._last_preload_ts == 0.0

    @pytest.mark.asyncio
    async def test_one_chunk_ok_no_error_log(self, loader, fake_redis, monkeypatch, caplog):
        """If any chunk succeeds, we must NOT raise the ERROR-level alarm."""
        monkeypatch.setattr(cfg_mod, "_CONFIG_PIPE_CHUNK", 1)

        pipe = MagicMock()
        pipe.hgetall = MagicMock()
        pipe.execute = AsyncMock(side_effect=[[{}], asyncio.TimeoutError()])
        fake_redis.pipeline = MagicMock(return_value=pipe)

        with caplog.at_level("ERROR", logger="crypto_orderflow.config"):
            await loader.preload_configs(["BTCUSDT", "ETHUSDT"])

        error_records = [r for r in caplog.records if r.levelname == "ERROR"]
        assert not any("ALL" in r.message for r in error_records)
        # preload_ts SHOULD be updated because at least one chunk succeeded
        assert loader._last_preload_ts > 0
