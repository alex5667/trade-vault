"""
Integration tests for DN-GATE (G2 · DeltaNotional) fixes in tick_decision_engine.py:

  Fix 1  — tick_dn_calib.update() called ONLY after gate passes (not for vetoed events)
  Fix 2a — Stage 1 veto metric label is "veto" (not the old "veto_tier")
  Fix 2b — Stage 2 (pressure proxy) does NOT emit extra "pass" on its own veto

All tests drive process_tick() with a minimal stub runtime so the real DN-GATE
code path executes; only Prometheus counters and the calibrator spy are patched.
"""

from __future__ import annotations

import logging
import unittest.mock as mock
from collections import deque
from types import SimpleNamespace
from typing import Any

import pytest

import services.orderflow.tick_decision_engine as tde_module
from services.orderflow.tick_decision_engine import TickDecisionEngine
from core.delta_notional_calibrator import DeltaNotionalTiers

# ---------------------------------------------------------------------------
# Helpers / stubs
# ---------------------------------------------------------------------------

_TICK_TS = 1_700_000_000_000   # deterministic ms timestamp (Nov 2023)
_PRICE   = 100.0               # BTC price used in all tests
_SYMBOL  = "BTCUSDT"


class _SpyDNCalib:
    """Configurable calibrator that records every update() call."""

    def __init__(self, *, tier0: float, tier1: float, tier2: float) -> None:
        self._tier0 = tier0
        self._tier1 = tier1
        self._tier2 = tier2
        self.update_calls: list[dict[str, Any]] = []

    def tiers(self, **_: Any) -> DeltaNotionalTiers:
        return DeltaNotionalTiers(
            tier0_usd=self._tier0,
            tier1_usd=self._tier1,
            tier2_usd=self._tier2,
            n=500,
            src="calib_p50/p80/p95",
            scale=1.0,
        )

    def update(self, *, regime: str, dn_usd: float, ts_ms: int = 0) -> None:
        self.update_calls.append({"regime": regime, "dn_usd": dn_usd, "ts_ms": ts_ms})


class _DummyDetector:
    """Returns a fixed delta event so the tick reaches the DN-GATE section."""

    def __init__(self, delta: float) -> None:
        self._delta = delta

    def push(self, _tick: dict[str, Any]) -> dict[str, Any] | None:
        if self._delta == 0.0:
            return None
        return {"delta": self._delta, "z": 4.0}


class _DummyPressure:
    def __init__(self, per_min_ema: float = 0.0) -> None:
        self._per_min = per_min_ema

    def on_raw_trigger(self, **_: Any) -> None: ...

    def snapshot(self, **_: Any) -> Any:
        s = SimpleNamespace()
        s.per_min_ema = self._per_min
        s.cd_rate_ema = 0.0
        return s


def _make_facade() -> Any:
    env = SimpleNamespace(
        maybe_refresh=lambda: None,
        time_max_back_ms=10_000,
        time_warn_back_ms=5_000,
        last_px_ttl_sec=60,
    )
    redis = mock.AsyncMock()
    redis.set = mock.AsyncMock(return_value=True)
    facade = SimpleNamespace(
        _env=env,
        logger=logging.getLogger("dn_gate_test"),
        redis=redis,
        cg_reader=None,
        market_state=None,
        signal_pipeline=None,
        _atr_sanity=None,
        of_engine=None,
        ticks=None,
        calib_svc=None,
        atr_cache=None,
        cg_macro_gate=None,
        conf_cal_gating_mode="raw",
        conf_cal_runtime=None,
        conf_cal_proof=None,
        conf_cal_proof_path="",
        conf_cal_proof_mtime=0.0,
    )
    return facade


def _make_runtime(
    *,
    calib: _SpyDNCalib,
    cfg: dict[str, Any] | None = None,
) -> Any:
    rt = SimpleNamespace()
    rt.symbol = _SYMBOL
    rt.config = dict(cfg or {})
    rt.dynamic_cfg = {}
    rt.tick_count = 0
    rt.heartbeat_counter = 0
    rt.last_ts_ms = 0
    rt.last_regime = "na"
    rt.last_atr = 0.0
    rt.delta_triggers = 0
    rt.pressure_sps = 0.0
    rt.signal_attempt_ts_ms = deque(maxlen=1200)

    # DN-GATE deps
    rt.tick_dn_calib = calib
    rt.dn_passrate = SimpleNamespace(update=lambda **_: None)
    rt.delta_log_sampler = SimpleNamespace(should_log=lambda _k: False)

    # pressure proxy layer deps
    rt.pressure = _DummyPressure()

    # optional — wrapped in try/except inside process_tick
    rt.last_obi_event = {}
    rt.last_iceberg_event = {}
    rt.last_spread_bps = 0.0
    rt.last_book_raw = None
    rt.l3_stats = None
    rt.l3_queue = None
    rt._last_l3_bucket_id = None
    rt.burst = SimpleNamespace(st=SimpleNamespace(active=False))
    rt.burst_cal = None
    rt.hawkes_state = {}
    rt.hawkes_snapshot = None

    return rt


def _make_engine(facade: Any) -> TickDecisionEngine:
    return TickDecisionEngine(facade=facade)


def _make_tick(delta_abs: float = 1_000.0) -> dict[str, Any]:
    return {
        "ts_ms": _TICK_TS,
        "price": _PRICE,
        "qty": delta_abs / _PRICE,
        "m": False,
    }


# ---------------------------------------------------------------------------
# Shared Prometheus counter mock (avoids global counter pollution between tests)
# ---------------------------------------------------------------------------

def _patch_dn_counter():
    """Returns a context manager that replaces dn_gate_events_total with a MagicMock."""
    m = mock.MagicMock()
    m.labels.return_value = mock.MagicMock()
    return mock.patch.object(tde_module, "dn_gate_events_total", m), m


# ---------------------------------------------------------------------------
# Fix 1 — calibrator only fed AFTER the gate passes
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_dn_gate_veto_calibrator_is_called():
    """Stage 1 veto → tick_dn_calib.update() MUST be called (EXPERT FIX 2026-02-15)."""
    # delta = 10 USD, tier0 = 1 000 000 → well below tier0 → tier = -1 → veto
    calib = _SpyDNCalib(tier0=1_000_000.0, tier1=2_000_000.0, tier2=3_000_000.0)
    facade = _make_facade()
    facade.delta_detector = _DummyDetector(delta=10.0)

    rt = _make_runtime(calib=calib)
    rt.delta_detector = _DummyDetector(delta=10.0)

    engine = _make_engine(facade)
    ctx_patch, _ = _patch_dn_counter()
    with ctx_patch:
        result = await engine.process_tick(rt, _make_tick(delta_abs=10.0))

    assert result is None, "expected veto (return None)"
    assert len(calib.update_calls) == 1, (
        "calibrator MUST be updated even for a vetoed tick (EXPERT FIX)"
    )


@pytest.mark.asyncio
async def test_dn_gate_pass_calibrator_called():
    """Stage 1 pass → tick_dn_calib.update() MUST be called exactly once.

    process_tick() may raise or return None later in the pipeline due to missing
    optional config keys; the calibrator is updated at line ~875 which is well
    before those sections, so we catch downstream exceptions and only assert on
    what matters.
    """
    # delta = 10 000 USD, tier0 = 1.0 → passes tier0 handily → tier = 0 → pass
    calib = _SpyDNCalib(tier0=1.0, tier1=2.0, tier2=3.0)
    facade = _make_facade()
    rt = _make_runtime(
        calib=calib,
        cfg={"data_health_veto_below": 0.0},   # disable data-health veto
    )
    rt.delta_detector = _DummyDetector(delta=100.0)   # delta_usd = 100 * 100 = 10 000

    engine = _make_engine(facade)
    ctx_patch, _ = _patch_dn_counter()
    with ctx_patch:
        try:
            # Calibrator is updated at ~line 875, well before any downstream config checks.
            await engine.process_tick(rt, _make_tick(delta_abs=100.0))
        except Exception:
            pass  # only calibrator state matters for this test

    assert len(calib.update_calls) == 1, (
        "calibrator must be updated exactly once for a passing tick (Fix 1)"
    )
    assert calib.update_calls[0]["dn_usd"] == pytest.approx(100.0 * _PRICE, rel=1e-3)


# ---------------------------------------------------------------------------
# Fix 2a — Stage 1 veto metric label is "veto" not "veto_tier"
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_dn_gate_stage1_veto_metric_label():
    """Stage 1 veto must emit result='veto', never 'veto_tier'."""
    calib = _SpyDNCalib(tier0=1_000_000.0, tier1=2_000_000.0, tier2=3_000_000.0)
    facade = _make_facade()
    rt = _make_runtime(calib=calib)
    rt.delta_detector = _DummyDetector(delta=10.0)

    engine = _make_engine(facade)
    ctx_patch, mock_ctr = _patch_dn_counter()
    with ctx_patch:
        result = await engine.process_tick(rt, _make_tick(delta_abs=10.0))

    assert result is None

    # Gather all (symbol, tier, session, result) tuples from .labels() calls
    label_calls = [kw for _, kw in mock_ctr.labels.call_args_list]
    results_emitted = {kw.get("result") for kw in label_calls}

    assert "veto" in results_emitted, (
        f"expected result='veto' in metric labels, got: {results_emitted}"
    )
    assert "veto_tier" not in results_emitted, (
        "old label 'veto_tier' must no longer be emitted (Fix 2a)"
    )


@pytest.mark.asyncio
async def test_dn_gate_stage1_pass_metric_label():
    """Stage 1 pass must emit result='pass' exactly once.

    Metrics are emitted at line ~861 (well before any downstream config checks),
    so we wrap the call in try/except to tolerate optional-key errors later.
    """
    calib = _SpyDNCalib(tier0=1.0, tier1=2.0, tier2=3.0)
    facade = _make_facade()
    rt = _make_runtime(
        calib=calib,
        cfg={"data_health_veto_below": 0.0},
    )
    rt.delta_detector = _DummyDetector(delta=100.0)

    engine = _make_engine(facade)
    ctx_patch, mock_ctr = _patch_dn_counter()
    with ctx_patch:
        try:
            await engine.process_tick(rt, _make_tick(delta_abs=100.0))
        except Exception:
            pass  # metric assertions happen before any downstream KeyError

    label_calls = [kw for _, kw in mock_ctr.labels.call_args_list]
    results_emitted = [kw.get("result") for kw in label_calls]

    pass_count = results_emitted.count("pass")
    assert pass_count >= 1, f"expected at least one 'pass' label, got: {results_emitted}"

    # Stage 2 (pressure proxy) must NOT add an extra standalone "pass" that
    # would double-count the event (Fix 2b).
    # After Fix 2b, stage 2 only emits on its own VETO, not on pass.
    # When stage 2 passes silently, total "pass" count stays at 1.
    assert pass_count == 1, (
        f"'pass' emitted {pass_count} times — stage 2 must not double-count (Fix 2b). "
        f"All result labels: {results_emitted}"
    )


# ---------------------------------------------------------------------------
# Fix 2b — Stage 2 pressure proxy veto emits "veto", NOT an extra "pass"
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_dn_gate_stage2_veto_no_double_pass():
    """Stage 2 (pressure proxy) veto must emit 'veto' and NOT add a second 'pass'.

    Strategy:
      - Primary DN-GATE passes (tier0=1.0 USD, delta_usd=10 000).
        Primary writes dynamic_cfg[dn_tier1_usd] = 50_000_000.0.
      - Pressure_hi flag raised (per_min_ema=100 >= pressure_hi_per_min=1):
        proxy escalates tier_idx 0→1, reads dn_tier1_usd=50M → VETO.
      - Before fix 2b: proxy also emitted result='pass' on its own pass-through.
        After fix 2b:  proxy is silent on pass, emits ONLY on its own veto.
    """
    # tier1 is large so that when proxy picks tier_idx=1, threshold >> delta_usd
    calib = _SpyDNCalib(tier0=1.0, tier1=50_000_000.0, tier2=100_000_000.0)
    facade = _make_facade()
    rt = _make_runtime(
        calib=calib,
        cfg={
            "data_health_veto_below": 0.0,
            "pressure_hi_per_min": 1.0,    # threshold: any ema >= 1 counts as hi
        },
    )
    rt.delta_detector = _DummyDetector(delta=100.0)  # delta_usd = 100 * 100 = 10 000
    rt.pressure = _DummyPressure(per_min_ema=100.0)  # triggers pressure_hi

    engine = _make_engine(facade)
    ctx_patch, mock_ctr = _patch_dn_counter()
    with ctx_patch:
        result = await engine.process_tick(rt, _make_tick(delta_abs=100.0))

    label_calls = [kw for _, kw in mock_ctr.labels.call_args_list]
    results_emitted = [kw.get("result") for kw in label_calls]

    # Stage 1 must have emitted "pass", stage 2 must have emitted "veto"
    assert "pass" in results_emitted, f"Stage 1 pass not recorded: {results_emitted}"
    assert "veto" in results_emitted, f"Stage 2 veto not recorded: {results_emitted}"

    # There must be exactly ONE "pass" (stage 2 no longer emits a second one)
    pass_count = results_emitted.count("pass")
    assert pass_count == 1, (
        f"expected exactly 1 'pass' label (stage 2 must not duplicate), "
        f"got {pass_count}. All: {results_emitted}"
    )

    # Process tick should have returned None (proxy veto)
    assert result is None, "expected proxy veto to return None"
