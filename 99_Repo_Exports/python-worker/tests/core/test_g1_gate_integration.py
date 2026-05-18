"""
Integration tests for G1 · Delta Trigger gate inside TickDecisionEngine.process_tick.

Scope:
  A. Fail-closed: no delta event → returns None, trigger_delta NOT incremented
  B. USD veto: delta_usd < min_usd → returns None + of_g1_veto_min_usd_total incremented
  C. USD veto boundary: delta_usd == min_usd → passes (strict <)
  D. USD disabled (min_usd=0): passes G1 → trigger_delta incremented
  E. Price-zero: early return before USD veto, veto counter unchanged
"""
from __future__ import annotations

import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from services.orderflow.tick_decision_engine import TickDecisionEngine
from services.orderflow.metrics import of_g1_veto_min_usd_total, of_session_outcome_total


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _counter(counter, symbol: str, session: str | None = None, outcome: str | None = None) -> float:
    for m in counter.collect():
        for s in m.samples:
            if not s.name.endswith("_total"):
                continue
            lbl = s.labels
            if lbl.get("symbol") != symbol:
                continue
            if outcome and lbl.get("outcome") != outcome:
                continue
            if session and lbl.get("session") != session:
                continue
            return s.value
    return 0.0


def _veto_count(symbol: str) -> float:
    return _counter(of_g1_veto_min_usd_total, symbol)


def _trigger_delta_count(symbol: str) -> float:
    total = 0.0
    for m in of_session_outcome_total.collect():
        for s in m.samples:
            if not s.name.endswith("_total"):
                continue
            lbl = s.labels
            if lbl.get("symbol") == symbol and lbl.get("outcome") == "trigger_delta":
                total += s.value
    return total


TS = 1_700_000_000_000  # fixed valid epoch_ms


def _make_tick(price: float = 60_000.0, side: str = "BUY", qty: float = 1.0) -> dict:
    return {
        "symbol": "BTCUSDT",
        "price": str(price),
        "qty": str(qty),
        "side": side,
        "ts": str(TS),
    }


def _make_facade() -> MagicMock:
    facade = MagicMock()
    # _env: only hard requirements (rest auto-mock)
    facade._env.maybe_refresh.return_value = None
    facade._env.last_px_ttl_sec = 300
    facade._env.time_max_back_ms = 5_000
    facade._env.time_warn_back_ms = 1_000
    facade.logger = MagicMock()
    facade.redis = AsyncMock()
    facade._log_metrics.return_value = None
    return facade


def _make_runtime(symbol: str, min_usd: float = 0.0, delta_event: dict | None = None) -> MagicMock:
    runtime = MagicMock()
    runtime.symbol = symbol
    runtime.tick_count = 0          # incremented to 1 → burst block (% 200) skipped
    runtime.heartbeat_counter = 0
    runtime.delta_triggers = 0
    runtime.last_ts_ms = 0          # no prev ts → time-order check skipped
    runtime.last_tick_seen_ts = 0   # line 331: compared numerically outside try/except
    runtime.source_inconsistent_until_ms = 0
    runtime.last_book_ts_ms = 0
    runtime.last_spread_bps = 0.0
    runtime.book_rate_ema = 0.0
    runtime.cvd_quarantine_active = 0
    runtime.delta_fallback_mode = "cvd"
    runtime.pressure_sps = 0.0
    runtime.pressure_hi = 0
    runtime.last_regime = "na"
    runtime.config = {
        "delta_abs_min_usd": min_usd,
        "burst_enable": "0",
        "pressure_ema_alpha": 0.20,
        "pressure_hi_sps": 0.12,
    }
    runtime.dynamic_cfg = {}
    runtime.last_regime = "na"

    # G1 key mock
    runtime.delta_detector.push.return_value = delta_event
    runtime.delta_detector.z_threshold = 2.0

    # DN-GATE tiers (needed when G1 passes)
    tiers = MagicMock()
    tiers.tier0_usd = 10_000.0
    tiers.tier1_usd = 50_000.0
    tiers.tier2_usd = 100_000.0
    tiers.src = "test"
    tiers.scale = 1.0
    runtime.tick_dn_calib.tiers.return_value = tiers

    # Fail-open blocks — MagicMock is fine for try/except wrapped code
    p_snap = MagicMock()
    p_snap.per_min_ema = 1.0
    p_snap.cd_rate_ema = 0.1
    runtime.pressure.snapshot.return_value = p_snap
    runtime.pressure.on_raw_trigger.return_value = None

    runtime.signal_attempt_ts_ms = []
    runtime.delta_log_sampler.should_log.return_value = False
    runtime.burst.st.active = False

    return runtime


# ---------------------------------------------------------------------------
# A. Fail-closed: no delta event
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_no_delta_event_returns_none():
    sym = "BTCUSDT_G1_A1"
    facade = _make_facade()
    engine = TickDecisionEngine(facade)
    runtime = _make_runtime(sym, min_usd=0.0, delta_event=None)

    before_trigger = _trigger_delta_count(sym)

    with patch("services.orderflow.tick_decision_engine.refresh_disabled_state",
               new=AsyncMock(return_value=(False, 0, ""))):
        result = await engine.process_tick(runtime, _make_tick())

    assert result is None, "No delta event → must return None (fail-closed)"
    assert _trigger_delta_count(sym) == before_trigger, "trigger_delta must NOT increment"


@pytest.mark.asyncio
async def test_no_delta_event_does_not_increment_veto_counter():
    sym = "BTCUSDT_G1_A2"
    facade = _make_facade()
    engine = TickDecisionEngine(facade)
    runtime = _make_runtime(sym, min_usd=50_000.0, delta_event=None)

    before_veto = _veto_count(sym)

    with patch("services.orderflow.tick_decision_engine.refresh_disabled_state",
               new=AsyncMock(return_value=(False, 0, ""))):
        await engine.process_tick(runtime, _make_tick())

    assert _veto_count(sym) == before_veto, "No event → USD veto counter must not increment"


# ---------------------------------------------------------------------------
# B. USD veto: delta_usd < min_usd
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_usd_veto_returns_none_and_increments_counter():
    """delta_usd = 1.5 * 60000 = $90k < min_usd=$200k → veto."""
    sym = "BTCUSDT_G1_B1"
    facade = _make_facade()
    engine = TickDecisionEngine(facade)
    delta_event = {"type": "delta_spike", "delta": 1.5, "z": 3.0, "ts_ms": TS}
    runtime = _make_runtime(sym, min_usd=200_000.0, delta_event=delta_event)

    before_veto = _veto_count(sym)
    before_trigger = _trigger_delta_count(sym)

    with patch("services.orderflow.tick_decision_engine.refresh_disabled_state",
               new=AsyncMock(return_value=(False, 0, ""))):
        result = await engine.process_tick(runtime, _make_tick(price=60_000.0, qty=1.5))

    assert result is None, "USD veto must return None"
    assert _veto_count(sym) == before_veto + 1.0, "Veto counter must increment by 1"
    assert _trigger_delta_count(sym) == before_trigger, "trigger_delta must NOT increment on veto"


@pytest.mark.asyncio
async def test_usd_veto_sell_spike_uses_abs_delta():
    """SELL spike: delta=-2.0 → abs = 2.0 * 60000 = $120k < $200k → veto."""
    sym = "BTCUSDT_G1_B2"
    facade = _make_facade()
    engine = TickDecisionEngine(facade)
    delta_event = {"type": "delta_spike", "delta": -2.0, "z": -3.0, "ts_ms": TS}
    runtime = _make_runtime(sym, min_usd=200_000.0, delta_event=delta_event)

    before_veto = _veto_count(sym)

    with patch("services.orderflow.tick_decision_engine.refresh_disabled_state",
               new=AsyncMock(return_value=(False, 0, ""))):
        result = await engine.process_tick(runtime, _make_tick(price=60_000.0, side="SELL", qty=2.0))

    assert result is None
    assert _veto_count(sym) == before_veto + 1.0, "SELL spike also vetoed by abs(delta)*price"


@pytest.mark.asyncio
async def test_usd_veto_disabled_when_min_usd_zero():
    """min_usd=0 → veto disabled → G1 passes → trigger_delta incremented."""
    sym = "BTCUSDT_G1_B3"
    facade = _make_facade()
    engine = TickDecisionEngine(facade)
    delta_event = {"type": "delta_spike", "delta": 0.001, "z": 3.0, "ts_ms": TS}
    runtime = _make_runtime(sym, min_usd=0.0, delta_event=delta_event)

    before_veto = _veto_count(sym)
    before_trigger = _trigger_delta_count(sym)

    with patch("services.orderflow.tick_decision_engine.refresh_disabled_state",
               new=AsyncMock(return_value=(False, 0, ""))):
        await engine.process_tick(runtime, _make_tick())

    assert _veto_count(sym) == before_veto, "No veto when min_usd=0"
    assert _trigger_delta_count(sym) == before_trigger + 1.0, "trigger_delta must increment"


@pytest.mark.asyncio
async def test_usd_veto_disabled_when_min_usd_le_one():
    """min_usd=1.0 → condition is `> 1.0` → veto disabled."""
    sym = "BTCUSDT_G1_B4"
    facade = _make_facade()
    engine = TickDecisionEngine(facade)
    delta_event = {"type": "delta_spike", "delta": 0.001, "z": 3.0, "ts_ms": TS}
    runtime = _make_runtime(sym, min_usd=1.0, delta_event=delta_event)

    before_veto = _veto_count(sym)
    before_trigger = _trigger_delta_count(sym)

    with patch("services.orderflow.tick_decision_engine.refresh_disabled_state",
               new=AsyncMock(return_value=(False, 0, ""))):
        await engine.process_tick(runtime, _make_tick())

    assert _veto_count(sym) == before_veto, "min_usd=1.0 is not >1 → veto disabled"
    assert _trigger_delta_count(sym) == before_trigger + 1.0


# ---------------------------------------------------------------------------
# C. USD veto boundary: delta_usd == min_usd → must PASS (strict <)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_usd_veto_boundary_exactly_equal_passes():
    """delta=1.5 * price=10000 = $15000 == min_usd=$15000 → condition is <, must PASS."""
    sym = "BTCUSDT_G1_C1"
    facade = _make_facade()
    engine = TickDecisionEngine(facade)
    delta_event = {"type": "delta_spike", "delta": 1.5, "z": 3.0, "ts_ms": TS}
    runtime = _make_runtime(sym, min_usd=15_000.0, delta_event=delta_event)

    before_veto = _veto_count(sym)
    before_trigger = _trigger_delta_count(sym)

    with patch("services.orderflow.tick_decision_engine.refresh_disabled_state",
               new=AsyncMock(return_value=(False, 0, ""))):
        await engine.process_tick(runtime, _make_tick(price=10_000.0, qty=1.5))

    assert _veto_count(sym) == before_veto, "Exactly equal → NOT vetoed (strict <)"
    assert _trigger_delta_count(sym) == before_trigger + 1.0, "Boundary equal → trigger_delta incremented"


@pytest.mark.asyncio
async def test_usd_veto_one_cent_below_threshold():
    """delta_usd is just below threshold → must be vetoed."""
    sym = "BTCUSDT_G1_C2"
    facade = _make_facade()
    engine = TickDecisionEngine(facade)
    # delta=1.49999, price=10000 → delta_usd=$14999.9 < $15000 → veto
    delta_event = {"type": "delta_spike", "delta": 1.49999, "z": 3.0, "ts_ms": TS}
    runtime = _make_runtime(sym, min_usd=15_000.0, delta_event=delta_event)

    before_veto = _veto_count(sym)

    with patch("services.orderflow.tick_decision_engine.refresh_disabled_state",
               new=AsyncMock(return_value=(False, 0, ""))):
        result = await engine.process_tick(runtime, _make_tick(price=10_000.0))

    assert result is None
    assert _veto_count(sym) == before_veto + 1.0


# ---------------------------------------------------------------------------
# D. Price-zero early exit (before USD veto)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_price_zero_returns_none_before_usd_veto():
    """price=0 → early return at line 632 before USD veto is evaluated."""
    sym = "BTCUSDT_G1_D1"
    facade = _make_facade()
    engine = TickDecisionEngine(facade)
    delta_event = {"type": "delta_spike", "delta": 100.0, "z": 5.0, "ts_ms": TS}
    runtime = _make_runtime(sym, min_usd=1.0, delta_event=delta_event)

    before_veto = _veto_count(sym)
    before_trigger = _trigger_delta_count(sym)

    tick_no_price = {"symbol": sym, "qty": "1.0", "side": "BUY", "ts": str(TS)}

    with patch("services.orderflow.tick_decision_engine.refresh_disabled_state",
               new=AsyncMock(return_value=(False, 0, ""))):
        result = await engine.process_tick(runtime, tick_no_price)

    assert result is None, "No price → early return"
    assert _veto_count(sym) == before_veto, "Veto counter must not change (never reached)"
    assert _trigger_delta_count(sym) == before_trigger, "trigger_delta must not change"


# ---------------------------------------------------------------------------
# E. Per-symbol isolation of counters
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_veto_counters_are_per_symbol():
    """Veto on SYM_A must not affect SYM_B counter."""
    sym_a = "ETHUSDT_G1_E1"
    sym_b = "SOLUSDT_G1_E1"

    before_b = _veto_count(sym_b)

    facade = _make_facade()
    engine = TickDecisionEngine(facade)
    delta_event = {"type": "delta_spike", "delta": 0.5, "z": 3.0, "ts_ms": TS}
    runtime_a = _make_runtime(sym_a, min_usd=500_000.0, delta_event=delta_event)

    with patch("services.orderflow.tick_decision_engine.refresh_disabled_state",
               new=AsyncMock(return_value=(False, 0, ""))):
        await engine.process_tick(runtime_a, _make_tick(price=3_000.0))

    assert _veto_count(sym_a) >= 1.0, "SYM_A veto counter must have incremented"
    assert _veto_count(sym_b) == before_b, "SYM_B counter must be unaffected"
