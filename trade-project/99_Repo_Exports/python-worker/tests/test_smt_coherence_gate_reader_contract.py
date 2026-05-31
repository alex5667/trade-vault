"""
Contract regression for SMT bundle state reader used by orderflow strategy.

The producer (services.smt_bundle_aggregator.SmtBundleAggregator) writes state via
SET(JSON) at key `smt:bundle:v1:{bundle_id}` on redis-worker-1. The consumer
`handlers.crypto_orderflow.utils.smt_coherence_gate.SmtLeaderCoherenceGate` (wired
into orderflow strategy as `_smt_leader_gate`) is what populates `ctx.smt_*` audit
fields that strategy.py copies into `indicators` and that flow into
`order:{id}.signal_payload.indicators` — the source for periodic-report SMT VETO sim
and reliability calibrators.

Two failure modes pinned here:
  1. Dual writer formats — aggregator currently uses SET(JSON); legacy paths use HSET.
     The reader must handle both.
  2. Async event-loop fail-open — `_sync_get` cannot await coroutines from inside a
     running asyncio loop and returns None, leaving every signal without smt_*.
     `_redis_read_bundle_state` must fall back to a sync Redis client in that case.
"""

import json
from typing import Any

from handlers.crypto_orderflow.utils.smt_coherence_gate import (
    SmtLeaderCoherenceGate,
    _redis_read_bundle_state,
)


class _StubRedis:
    """Minimal sync Redis with GET/SET/HSET/HGETALL (no external deps)."""

    def __init__(self) -> None:
        self._strings: dict[str, str] = {}
        self._hashes: dict[str, dict[str, str]] = {}

    def set(self, key: str, value: str) -> bool:
        self._strings[key] = value
        return True

    def get(self, key: str) -> Any:
        return self._strings.get(key)

    def hset(self, key: str, *, mapping: dict[str, Any]) -> int:
        h = self._hashes.setdefault(key, {})
        for k, v in mapping.items():
            h[k] = str(v)
        return len(mapping)

    def hgetall(self, key: str) -> dict[str, str]:
        return dict(self._hashes.get(key, {}))


def _make_gate(bundle_id: str = "btc_eth_sol", mode: str = "observe") -> SmtLeaderCoherenceGate:
    return SmtLeaderCoherenceGate(
        redis_client=None,  # overwritten per test
        bundle_id=bundle_id,
        mode=mode,
        coh_hi_thr=0.65,
        veto_kinds=None,
        diag_stream="",
        diag_sample=1,
    )


# ── _redis_read_bundle_state: dual-format support ─────────────────────────────


def test_read_state_supports_set_json_writer_path() -> None:
    """Aggregator writes SET(JSON) — reader must parse it (not silently miss)."""
    r = _StubRedis()
    r.set(
        "smt:bundle:v1:btc_eth_sol",
        json.dumps({
            "leader": "BTCUSDT",
            "leader_dir": "UP",
            "leader_confirm": 1,
            "coh": 0.72,
            "ts_ms": 1_700_000_000_000,
        }),
    )

    st = _redis_read_bundle_state(r, "smt:bundle:v1:btc_eth_sol")
    assert st is not None
    assert st["leader_dir"] == "UP"
    assert st["leader_confirm"] == 1
    assert float(st["coh"]) == 0.72


def test_read_state_supports_legacy_hgetall_writer_path() -> None:
    """Legacy HSET writers stay supported (fallback after GET miss)."""
    r = _StubRedis()
    r.hset(
        "smt:bundle:v1:btc_eth_sol",
        mapping={
            "leader": "ETHUSDT",
            "leader_dir": "DOWN",
            "leader_confirm": "1",
            "coh": "0.81",
            "ts_ms": "1700000000000",
        },
    )

    st = _redis_read_bundle_state(r, "smt:bundle:v1:btc_eth_sol")
    assert st is not None
    assert st["leader_dir"] == "DOWN"
    assert st["coh"] == "0.81"


def test_read_state_missing_key_returns_none() -> None:
    r = _StubRedis()
    assert _redis_read_bundle_state(r, "smt:bundle:v1:nope") is None


def test_read_state_async_running_loop_falls_back_to_sync_client(monkeypatch) -> None:
    """
    The real failure mode in prod: strategy.py owns an async aioredis client.
    `_sync_get` inside a running loop closes the coroutine and returns None, so the
    original `redis_client.get/hgetall` path yields nothing. The fallback hatch is
    `_resolve_sync_redis()` (handler_config._get_sync_redis), which returns a sync
    client wired to the same Redis instance. Without that fallback every signal
    publishes without smt_*.
    """

    class _AsyncShim:
        """Mimics aioredis: every call returns a coroutine."""

        def __init__(self, backing: _StubRedis) -> None:
            self._b = backing

        async def get(self, key: str) -> Any:
            return self._b.get(key)

        async def hgetall(self, key: str) -> Any:
            return self._b.hgetall(key)

    backing = _StubRedis()
    backing.set(
        "smt:bundle:v1:btc_eth_sol",
        json.dumps({"leader_dir": "UP", "leader_confirm": 1, "coh": 0.7, "ts_ms": 1_700_000_000_000}),
    )

    monkeypatch.setattr(
        "handlers.crypto_orderflow.utils.smt_coherence_gate._resolve_sync_redis",
        lambda: backing,
    )

    import asyncio

    async def _runner() -> dict[str, Any] | None:
        # Loop is now running — async client coroutines cannot be awaited via _sync_get.
        return _redis_read_bundle_state(_AsyncShim(backing), "smt:bundle:v1:btc_eth_sol")

    st = asyncio.run(_runner())
    assert st is not None, "sync fallback must hydrate state when async loop is running"
    assert st["leader_dir"] == "UP"
    assert st["leader_confirm"] == 1


# ── Gate.evaluate: full audit surface for reporter sim + calibrators ──────────


def test_evaluate_sets_full_audit_surface_for_reporter() -> None:
    """
    Reporter SMT VETO sim + reliability calibrators key off
    smt_leader_dir, smt_leader_confirm, smt_coh, smt_align. Without those the sim
    renders "N/A (поля отсутствуют)". Pin them.
    """
    r = _StubRedis()
    r.set(
        "smt:bundle:v1:btc_eth_sol",
        json.dumps({
            "leader": "BTCUSDT",
            "leader_dir": "UP",
            "leader_confirm": 1,
            "coh": 0.70,
            "ts_ms": 1_700_000_000_000,
        }),
    )

    class Ctx:
        symbol = "1000PEPEUSDT"

    ctx = Ctx()
    gate = _make_gate(mode="observe")
    gate.redis = r  # type: ignore[attr-defined]
    gate.evaluate(ctx=ctx, symbol="1000PEPEUSDT", kind="orderflow_strategy", direction="LONG")

    assert getattr(ctx, "smt_leader_dir", None) == "UP"
    assert getattr(ctx, "smt_leader_confirm", None) == 1
    assert getattr(ctx, "smt_coh", None) == 0.70
    # LONG vs UP leader → aligned
    assert getattr(ctx, "smt_align", None) == 1
    assert getattr(ctx, "smt_bundle_id", None) == "btc_eth_sol"


def test_evaluate_countertrend_marks_align_zero() -> None:
    r = _StubRedis()
    r.set(
        "smt:bundle:v1:btc_eth_sol",
        json.dumps({
            "leader_dir": "UP",
            "leader_confirm": 1,
            "coh": 0.70,
            "ts_ms": 1_700_000_000_000,
        }),
    )

    class Ctx:
        symbol = "1000PEPEUSDT"

    ctx = Ctx()
    gate = _make_gate(mode="observe")
    gate.redis = r  # type: ignore[attr-defined]
    gate.evaluate(ctx=ctx, symbol="1000PEPEUSDT", kind="orderflow_strategy", direction="SHORT")

    assert getattr(ctx, "smt_leader_confirm", None) == 1
    assert getattr(ctx, "smt_align", None) == 0
