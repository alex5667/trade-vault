"""Unit tests for gated_out_outcome_tracker.

Covers:
  Test 1 — v2 ML metadata propagated to outcome payload.
  Test 2 — cost-aware label (y_edge_cost_aware) with full cost model:
    TP+15 bps, fees=10 → positive (15-10=+5 > 0)
    TP+8 bps,  fees=10 → negative (8-10=-2 < 0)
    TIMEOUT+20 bps    → negative by policy (path uncertainty)
    TP with spread → positive only when net > 0 after fees+spread/2+slippage
  Test 3 — _fetch_ticks pagination (single XRANGE 10k cap doesn't truncate path).
  Test 4 — _emit_outcome atomic Lua dispatch: written/duplicate/error/missing_sid.
  Test 5 — NO_TICKS synthetic outcome payload shape.
"""
from __future__ import annotations

import json
from unittest.mock import AsyncMock

import pytest

from services.gated_out_outcome_tracker import tracker as tr
from services.gated_out_outcome_tracker.tracker import (
    COST_FEES_BPS_RT,
    PendingSignal,
    _build_no_ticks_payload,
    _emit_outcome,
    _evaluate_path,
    _fetch_ticks,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_pending(
    entry: float = 60_000.0,
    tp_bps: float = 20.0,
    sl_bps: float = 10.0,
    direction: str = "LONG",
    spread_bps: float = 0.0,
    expected_slippage_bps: float = 0.0,
    sample_policy: str = "confidence_gated_out",
    selection_policy_version: str = "v1",
    selection_prob: float = 0.90,
    selection_weight: float = 0.90,
    virtual_min_conf: float = 0.35,
    meets_virtual_threshold: int = 1,
) -> PendingSignal:
    ts = 1_716_000_000_000
    return PendingSignal(
        msg_id="msg1", sid="sid1", symbol="BTCUSDT", direction=direction,
        entry=entry, sl=entry * 0.99, tp_bps=tp_bps, sl_bps=sl_bps,
        ts_ms=ts, confidence=0.45, min_conf=0.35, expire_ms=ts + 1_800_000,
        spread_bps=spread_bps,
        expected_slippage_bps=expected_slippage_bps,
        sample_policy=sample_policy,
        selection_policy_version=selection_policy_version,
        selection_prob=selection_prob,
        selection_weight=selection_weight,
        virtual_min_conf=virtual_min_conf,
        meets_virtual_threshold=meets_virtual_threshold,
    )


def _tp_path(p: PendingSignal) -> list[tuple[int, float]]:
    """Path that hits TP for both LONG and SHORT."""
    ts = p.ts_ms
    sign = 1.0 if p.direction == "LONG" else -1.0
    tp_px = p.entry * (1 + sign * p.tp_bps / 1e4)
    return [(ts, p.entry), (ts + 500, tp_px + sign * 1.0)]


def _timeout_path(p: PendingSignal, ret_bps: float = 20.0) -> list[tuple[int, float]]:
    """Path that neither hits TP nor SL — ends with a positive return."""
    ts = p.ts_ms
    sign = 1.0 if p.direction == "LONG" else -1.0
    close_px = p.entry * (1 + sign * ret_bps / 1e4)
    # keep price between SL and TP
    return [(ts, p.entry), (ts + 1_800_000, close_px)]


# ---------------------------------------------------------------------------
# Test 1: v2 ML metadata propagated to outcome payload
# ---------------------------------------------------------------------------

class TestOutcomeV2Metadata:
    def test_sample_policy_in_outcome(self) -> None:
        p = _make_pending(sample_policy="confidence_gated_out")
        result = _evaluate_path(p, _tp_path(p))
        assert result is not None
        assert result["sample_policy"] == "confidence_gated_out"

    def test_selection_fields_in_outcome(self) -> None:
        p = _make_pending(selection_prob=0.88, selection_weight=0.88,
                          selection_policy_version="v2")
        result = _evaluate_path(p, _tp_path(p))
        assert result is not None
        assert result["selection_weight"] == pytest.approx(0.88)
        assert result["selection_prob"] == pytest.approx(0.88)
        assert result["selection_policy_version"] == "v2"

    def test_virtual_min_conf_and_threshold_flag_in_outcome(self) -> None:
        p = _make_pending(virtual_min_conf=0.35, meets_virtual_threshold=1)
        result = _evaluate_path(p, _tp_path(p))
        assert result is not None
        assert result["virtual_min_conf"] == pytest.approx(0.35)
        assert result["meets_virtual_threshold"] == 1

    def test_schema_version_is_2(self) -> None:
        p = _make_pending()
        result = _evaluate_path(p, _tp_path(p))
        assert result is not None
        assert result["v"] == 2


# ---------------------------------------------------------------------------
# Test 2: cost-aware label
# ---------------------------------------------------------------------------

class TestCostAwareLabel:
    """y_edge_cost_aware=1 only when TP_HIT and net edge after all costs > 0."""

    def test_tp_above_fees_is_positive(self) -> None:
        # tp_bps=15, fees=10 (default), spread=0, slip=0 → 15-10=+5 → 1
        p = _make_pending(tp_bps=15.0)
        result = _evaluate_path(p, _tp_path(p))
        assert result is not None
        assert result["outcome"] == "TP_HIT"
        assert result["y_edge_cost_aware"] == 1
        assert result["edge_after_cost_bps"] == pytest.approx(15.0 - COST_FEES_BPS_RT)

    def test_tp_below_fees_is_negative(self) -> None:
        # tp_bps=8, fees=10 → 8-10=-2 → 0
        p = _make_pending(tp_bps=8.0)
        result = _evaluate_path(p, _tp_path(p))
        assert result is not None
        assert result["outcome"] == "TP_HIT"
        assert result["y_edge_cost_aware"] == 0
        assert result["edge_after_cost_bps"] == pytest.approx(8.0 - COST_FEES_BPS_RT)

    def test_timeout_always_zero_regardless_of_return(self) -> None:
        # TIMEOUT with +20 bps → still 0 by policy
        p = _make_pending(tp_bps=50.0)  # high TP so path doesn't touch it
        result = _evaluate_path(p, _timeout_path(p, ret_bps=20.0))
        assert result is not None
        assert result["outcome"] == "TIMEOUT"
        assert result["y_edge_cost_aware"] == 0

    def test_sl_hit_always_zero(self) -> None:
        p = _make_pending()
        sl_px = p.entry * (1 - p.sl_bps / 1e4) - 1.0  # below SL
        path = [(p.ts_ms, p.entry), (p.ts_ms + 500, sl_px)]
        result = _evaluate_path(p, path)
        assert result is not None
        assert result["outcome"] == "SL_HIT"
        assert result["y_edge_cost_aware"] == 0

    def test_spread_reduces_net_edge(self) -> None:
        # tp_bps=15, fees=10, spread=12 → cost = 10 + 12/2 = 16 → 15-16=-1 → 0
        p = _make_pending(tp_bps=15.0, spread_bps=12.0)
        result = _evaluate_path(p, _tp_path(p))
        assert result is not None
        assert result["outcome"] == "TP_HIT"
        assert result["y_edge_cost_aware"] == 0
        assert result["cost_bps"] == pytest.approx(COST_FEES_BPS_RT + 6.0)

    def test_slippage_reduces_net_edge(self) -> None:
        # tp_bps=18, fees=10, slippage=5 → cost=15 → 18-15=+3 → 1
        p = _make_pending(tp_bps=18.0, expected_slippage_bps=5.0)
        result = _evaluate_path(p, _tp_path(p))
        assert result is not None
        assert result["outcome"] == "TP_HIT"
        assert result["y_edge_cost_aware"] == 1
        assert result["cost_bps"] == pytest.approx(COST_FEES_BPS_RT + 5.0)

    def test_cost_breakdown_fields_present(self) -> None:
        p = _make_pending(tp_bps=20.0, spread_bps=4.0, expected_slippage_bps=3.0)
        result = _evaluate_path(p, _tp_path(p))
        assert result is not None
        assert "cost_fees_bps" in result
        assert "cost_spread_bps" in result
        assert "cost_slippage_bps" in result
        assert result["cost_fees_bps"] == pytest.approx(COST_FEES_BPS_RT)
        assert result["cost_spread_bps"] == pytest.approx(2.0)   # spread/2
        assert result["cost_slippage_bps"] == pytest.approx(3.0)

    def test_combined_spread_slippage_negative(self) -> None:
        """tp=15, fees=10, spread=4, slippage=5 → cost=17, edge=-2 → y_edge_cost_aware=0."""
        p = _make_pending(tp_bps=15.0, spread_bps=4.0, expected_slippage_bps=5.0)
        result = _evaluate_path(p, _tp_path(p))
        assert result is not None
        assert result["outcome"] == "TP_HIT"
        # cost = 10 + 4/2 + 5 = 17
        assert result["cost_bps"] == pytest.approx(17.0)
        assert result["edge_after_cost_bps"] == pytest.approx(15.0 - 17.0)
        assert result["y_edge_cost_aware"] == 0

    def test_combined_spread_slippage_positive(self) -> None:
        """tp=25, fees=10, spread=4, slippage=5 → cost=17, edge=+8 → y_edge_cost_aware=1."""
        p = _make_pending(tp_bps=25.0, spread_bps=4.0, expected_slippage_bps=5.0)
        result = _evaluate_path(p, _tp_path(p))
        assert result is not None
        assert result["outcome"] == "TP_HIT"
        assert result["cost_bps"] == pytest.approx(17.0)
        assert result["edge_after_cost_bps"] == pytest.approx(25.0 - 17.0)
        assert result["y_edge_cost_aware"] == 1


# ---------------------------------------------------------------------------
# Test 3: _fetch_ticks pagination
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
class TestFetchTicksPagination:
    """XRANGE chunk cap is 10000 by default → must paginate when >chunk ticks exist.

    Verifies:
      a) all ticks past the first chunk are fetched
      b) duplicate-cursor regression (same ms, multiple seqs) doesn't hang
      c) MAX_CHUNKS guard increments truncated counter and exits cleanly
    """

    async def _make_redis(self):
        try:
            import fakeredis.aioredis
        except ImportError:
            pytest.skip("fakeredis not installed")
        return fakeredis.aioredis.FakeRedis(decode_responses=True)

    async def _seed_ticks(self, r, symbol: str, ticks: list[tuple[int, float]]) -> None:
        stream = tr.TICK_STREAM_TPL.format(symbol=symbol)
        for ts, px in ticks:
            await r.xadd(stream, {"price": str(px)}, id=f"{ts}-*")

    async def test_paginates_past_chunk_cap(self, monkeypatch):
        monkeypatch.setattr(tr, "TICK_FETCH_CHUNK", 10)
        monkeypatch.setattr(tr, "TICK_FETCH_MAX_CHUNKS", 10)
        r = await self._make_redis()
        # 35 ticks at distinct ts in [1000, 1340], price ramps up.
        symbol = "BTCUSDT"
        ticks = [(1000 + i * 10, 100.0 + i * 0.1) for i in range(35)]
        await self._seed_ticks(r, symbol, ticks)
        out = await _fetch_ticks(r, symbol, 1000, 1500)
        assert len(out) == 35
        assert out[0][1] == pytest.approx(100.0)
        assert out[-1][1] == pytest.approx(100.0 + 34 * 0.1)

    async def test_advances_cursor_past_duplicate_ms(self, monkeypatch):
        """Несколько entry с одинаковым ms (`1000-0`, `1000-1`, ...) — cursor
        должен прыгнуть на `1000-2`, иначе зацикливание."""
        monkeypatch.setattr(tr, "TICK_FETCH_CHUNK", 2)
        monkeypatch.setattr(tr, "TICK_FETCH_MAX_CHUNKS", 10)
        r = await self._make_redis()
        symbol = "BTCUSDT"
        stream = tr.TICK_STREAM_TPL.format(symbol=symbol)
        # Все 4 entry на ts=1000 с разным seq.
        for i in range(4):
            await r.xadd(stream, {"price": f"{100.0 + i}"}, id=f"1000-{i}")
        await r.xadd(stream, {"price": "200.0"}, id="2000-0")
        out = await _fetch_ticks(r, symbol, 1000, 3000)
        assert len(out) == 5
        prices = [px for _, px in out]
        assert 200.0 in prices

    async def test_truncation_metric_when_max_chunks_hit(self, monkeypatch):
        """Если поток длиннее chunk×max_chunks — взводим truncated counter."""
        monkeypatch.setattr(tr, "TICK_FETCH_CHUNK", 5)
        monkeypatch.setattr(tr, "TICK_FETCH_MAX_CHUNKS", 2)  # cap=10 ticks
        r = await self._make_redis()
        symbol = "BTCUSDT"
        # 25 ticks → должен срезаться на 10.
        ticks = [(1000 + i * 10, 100.0 + i) for i in range(25)]
        await self._seed_ticks(r, symbol, ticks)
        before = tr.g_tick_fetch_truncated._value.get()
        out = await _fetch_ticks(r, symbol, 1000, 9000)
        after = tr.g_tick_fetch_truncated._value.get()
        # Усечено, но без crash.
        assert len(out) == 10
        assert after - before == 1


# ---------------------------------------------------------------------------
# Test 4: _emit_outcome — atomic Lua dispatch
# ---------------------------------------------------------------------------

def _make_mock_redis(lua_response: list) -> AsyncMock:
    """Stub Redis object whose execute_command returns a fixed Lua-style response."""
    r = AsyncMock()
    r.execute_command = AsyncMock(return_value=lua_response)
    return r


@pytest.mark.asyncio
class TestEmitOutcomeAtomicLua:
    """_emit_outcome dispatches to Lua EVAL and maps result codes correctly.

    Unit tests mock execute_command (r.eval fallback) because lupa is not
    installed in this environment. Integration test with real Redis is in
    tests/integration/test_gated_out_atomic_emit.py.
    """

    async def test_written_returns_true_and_bumps_metric(self):
        r = _make_mock_redis([1, "written", "1716000000000-0"])
        before = tr.g_outcome_emit_total.labels(result="written")._value.get()
        ok = await _emit_outcome(r, {"v": 2, "sid": "sid_w", "symbol": "BTCUSDT"})
        assert ok is True
        assert tr.g_outcome_emit_total.labels(result="written")._value.get() - before == 1

    async def test_duplicate_returns_false_and_bumps_dedup_metric(self):
        r = _make_mock_redis([0, "duplicate", "1716000000000-0"])
        before_dup = tr.g_outcome_dedup_skipped._value.get()
        before_lbl = tr.g_outcome_emit_total.labels(result="duplicate")._value.get()
        ok = await _emit_outcome(r, {"v": 2, "sid": "sid_d", "symbol": "BTCUSDT"})
        assert ok is False
        assert tr.g_outcome_dedup_skipped._value.get() - before_dup == 1
        assert tr.g_outcome_emit_total.labels(result="duplicate")._value.get() - before_lbl == 1

    async def test_unexpected_lua_result_returns_false_and_bumps_error(self):
        r = _make_mock_redis([0, "unknown_status", ""])
        before = tr.g_outcome_emit_total.labels(result="error")._value.get()
        ok = await _emit_outcome(r, {"v": 2, "sid": "sid_e", "symbol": "BTCUSDT"})
        assert ok is False
        assert tr.g_outcome_emit_total.labels(result="error")._value.get() - before == 1

    async def test_execute_command_exception_returns_false_and_bumps_error(self):
        r = AsyncMock()
        r.execute_command = AsyncMock(side_effect=ConnectionError("redis down"))
        before = tr.g_outcome_emit_total.labels(result="error")._value.get()
        ok = await _emit_outcome(r, {"v": 2, "sid": "sid_ex", "symbol": "BTCUSDT"})
        assert ok is False
        assert tr.g_outcome_emit_total.labels(result="error")._value.get() - before == 1

    async def test_missing_sid_returns_false_and_bumps_missing_sid_metric(self):
        r = _make_mock_redis([1, "written", "123-0"])
        before = tr.g_outcome_emit_total.labels(result="missing_sid")._value.get()
        ok = await _emit_outcome(r, {"v": 2, "sid": "", "symbol": "BTCUSDT"})
        assert ok is False
        # execute_command must NOT have been called — Lua script never runs for missing sid.
        r.execute_command.assert_not_called()
        assert tr.g_outcome_emit_total.labels(result="missing_sid")._value.get() - before == 1

    async def test_distinct_sids_each_call_lua(self):
        r = _make_mock_redis([1, "written", "123-0"])
        ok1 = await _emit_outcome(r, {"v": 2, "sid": "sid_a", "symbol": "BTCUSDT"})
        ok2 = await _emit_outcome(r, {"v": 2, "sid": "sid_b", "symbol": "ETHUSDT"})
        assert ok1 is True
        assert ok2 is True
        assert r.execute_command.call_count == 2

    async def test_lua_called_with_correct_keys(self):
        """execute_command must pass KEYS[1]=dedup_key, KEYS[2]=OUTPUT_STREAM."""
        r = _make_mock_redis([1, "written", "123-0"])
        sid = "sid_keys_check"
        await _emit_outcome(r, {"v": 2, "sid": sid, "symbol": "BTCUSDT"})
        call_args = r.execute_command.call_args
        # positional: ("EVAL", script, numkeys, KEYS[1], KEYS[2], ARGV[1], ARGV[2], ARGV[3])
        args = call_args.args
        assert args[0] == "EVAL"
        assert args[2] == 2                                           # numkeys
        assert args[3] == tr.OUTCOME_DEDUP_KEY_TPL.format(sid=sid)   # KEYS[1]
        assert args[4] == tr.OUTPUT_STREAM                            # KEYS[2]


# ---------------------------------------------------------------------------
# Test 5: NO_TICKS outcome payload
# ---------------------------------------------------------------------------

class TestNoTicksOutcome:
    def test_no_ticks_payload_shape(self) -> None:
        p = _make_pending()
        out = _build_no_ticks_payload(p)
        assert out["outcome"] == "NO_TICKS"
        assert out["valid"] == 0
        assert out["skip_reason"] == "no_ticks"
        assert out["sid"] == p.sid
        assert out["symbol"] == p.symbol
        assert out["v"] == 2
        assert out["sample_policy"] == p.sample_policy
        assert out["gated_out"] == 1
        # gross/edge labels должны отсутствовать — это не оценка, а пропуск.
        assert "y" not in out
        assert "y_edge_cost_aware" not in out

    def test_no_ticks_serializable_to_json(self) -> None:
        p = _make_pending()
        out = _build_no_ticks_payload(p)
        json.dumps(out)  # не должен бросать
