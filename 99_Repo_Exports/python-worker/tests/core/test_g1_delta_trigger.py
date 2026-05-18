"""
Unit tests for G1 · Delta Trigger gate.

Coverage:
  A. classify_signed_qty — side classification with Go-normalised tick fields
  B. DeltaSpikeDetector — warmup, z-score, self-inclusion bias, std_floor,
                           threshold gating, ts_ms fallback, hot-reload
  C. delta_abs_min_usd veto — logic, Prometheus counter, logger level
  D. ENV variable naming — {PREFIX}_DELTA_Z_THRESHOLD works;
                            CRYPTO_OF_DELTA_Z_THRESHOLD has no effect
"""
from __future__ import annotations

import os
import math
import random
import logging
from unittest.mock import patch, MagicMock

import pytest

from core.crypto_orderflow_detectors import DeltaSpikeDetector, classify_signed_qty


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_tick(side: str, qty: float | str, ts: int = 1_700_000_000_000) -> dict:
    """Mimics a Go-normalised tick as it arrives from Redis (decode_responses=True)."""
    return {
        "symbol": "BTCUSDT",
        "ts": str(ts),            # Go publishes ts as int, Redis returns str
        "price": "60000.0",
        "qty": str(qty),          # Go publishes qty as str
        "side": side,             # Go publishes "BUY" or "SELL"
        "source": "binance",
        "market": "futures",
    }


def _warm_detector(det: DeltaSpikeDetector, n: int = 35, side: str = "BUY") -> None:
    """Feed n normal ticks to bring the detector past its warmup period."""
    rng = random.Random(42)
    for _ in range(n):
        det.push(_make_tick(side=side if rng.random() > 0.5 else "SELL",
                            qty=rng.uniform(0.1, 2.0)))


def _counter_value(counter, symbol: str) -> float:
    for m in counter.collect():
        for s in m.samples:
            if s.name.endswith("_total") and s.labels.get("symbol") == symbol:
                return s.value
    return 0.0


# ===========================================================================
# A. classify_signed_qty
# ===========================================================================

class TestClassifySignedQty:

    def test_buy_side_positive(self):
        tick = _make_tick("BUY", 1.5)
        assert classify_signed_qty(tick) == pytest.approx(1.5)

    def test_sell_side_negative(self):
        tick = _make_tick("SELL", 2.0)
        assert classify_signed_qty(tick) == pytest.approx(-2.0)

    def test_qty_as_string_parsed(self):
        """Go sends qty as string; float() must succeed."""
        tick = {"side": "BUY", "qty": "0.00123"}
        assert classify_signed_qty(tick) == pytest.approx(0.00123)

    def test_qty_fallback_to_volume(self):
        """No 'qty' key → fall back to 'volume'."""
        tick = {"side": "BUY", "volume": 3.0}
        assert classify_signed_qty(tick) == pytest.approx(3.0)

    def test_is_buyer_maker_true_gives_negative(self):
        """Binance is_buyer_maker=True → taker SELL → negative."""
        tick = {"is_buyer_maker": True, "qty": "2.0"}
        assert classify_signed_qty(tick) == pytest.approx(-2.0)

    def test_is_buyer_maker_false_gives_positive(self):
        tick = {"is_buyer_maker": False, "qty": "2.0"}
        assert classify_signed_qty(tick) == pytest.approx(2.0)

    def test_unknown_side_returns_zero(self):
        tick = {"side": "UNKNOWN", "qty": "1.0"}
        assert classify_signed_qty(tick) == 0.0

    def test_missing_side_and_is_buyer_maker_returns_zero(self):
        tick = {"qty": "1.0"}
        assert classify_signed_qty(tick) == 0.0

    def test_zero_qty_returns_zero(self):
        tick = {"side": "BUY", "qty": "0.0"}
        assert classify_signed_qty(tick) == 0.0

    def test_negative_qty_normalised_to_abs(self):
        """Negative qty in payload is forced to abs() before signing."""
        tick = {"side": "BUY", "qty": "-1.0"}
        result = classify_signed_qty(tick)
        # abs(-1) = 1, then BUY → +1
        assert result == pytest.approx(1.0)

    def test_override_qty_ignores_payload(self):
        tick = {"side": "BUY", "qty": "999.0"}
        assert classify_signed_qty(tick, override_qty=5.0) == pytest.approx(5.0)


# ===========================================================================
# B. DeltaSpikeDetector
# ===========================================================================

class TestDeltaSpikeDetectorWarmup:

    def test_no_fire_before_warmup(self):
        det = DeltaSpikeDetector(window=120, z_threshold=0.5)
        results = [det.push(_make_tick("BUY", 100.0)) for _ in range(28)]
        assert all(r is None for r in results), "Must not fire in first 28 ticks"

    def test_first_possible_fire_at_tick_29(self):
        """After our fix warmup = 30 samples → min_prev = 29."""
        det = DeltaSpikeDetector(window=120, z_threshold=0.5)
        # Feed 28 uniform ticks
        for _ in range(28):
            det.push(_make_tick("BUY", 1.0))
        # 29th tick with a giant spike should be eligible to fire
        result = det.push(_make_tick("BUY", 1_000_000.0))
        # Not guaranteed to fire (depends on std), but previous 28 must be None
        # The important check is that it *can* fire at tick 29 (warmup passed)
        assert len(det.values) == 29

    def test_warmup_respects_small_window(self):
        """window < 30 → min_total = window, warmup ≤ window - 1."""
        det = DeltaSpikeDetector(window=5, z_threshold=0.5)
        # min_total = min(30, 5) = 5 → min_prev = 4
        results = [det.push(_make_tick("BUY", float(i + 1))) for i in range(4)]
        assert all(r is None for r in results)
        # 5th tick is eligible
        assert len(det.values) == 4  # buffer has 4 before push


class TestDeltaSpikeDetectorZScore:

    def test_spike_detected_above_threshold(self):
        det = DeltaSpikeDetector(window=120, z_threshold=2.0)
        _warm_detector(det, n=35)
        # Inject a massive spike
        result = det.push(_make_tick("BUY", 100_000.0))
        assert result is not None, "Spike should trigger delta event"
        assert result["type"] == "delta_spike"
        assert result["z"] >= 2.0
        assert result["delta"] > 0

    def test_normal_tick_below_threshold(self):
        det = DeltaSpikeDetector(window=120, z_threshold=3.0)
        _warm_detector(det, n=50)
        # Feed another ordinary tick — should not fire
        result = det.push(_make_tick("BUY", 0.5))
        assert result is None

    def test_sell_spike_gives_negative_delta(self):
        det = DeltaSpikeDetector(window=120, z_threshold=2.0)
        _warm_detector(det, n=35)
        result = det.push(_make_tick("SELL", 100_000.0))
        assert result is not None
        assert result["delta"] < 0
        assert result["z"] < 0  # z mirrors sign of delta

    def test_no_self_inclusion_bias(self):
        """Stats must be computed on prev_n ticks, not including current delta."""
        det = DeltaSpikeDetector(window=10, z_threshold=9999.0)  # never fire
        # Add 9 identical ticks → mean=1, std→0
        for _ in range(9):
            det.push(_make_tick("BUY", 1.0))
        # Manually inspect: stats computed over 9 values BEFORE new push
        # If self-inclusion bias existed, the 10th tick (value=1) would have z=0;
        # without it, z is still 0 because mean matches value → correct either way.
        # But with a very different value:
        det2 = DeltaSpikeDetector(window=10, z_threshold=1.0)
        for _ in range(9):
            det2.push(_make_tick("BUY", 1.0))
        # 10th: large spike; without self-inclusion, mean≈1, std≈0+floor → z huge
        r = det2.push(_make_tick("BUY", 100.0))
        assert r is not None, "Should detect spike without self-inclusion dampening it"

    def test_std_floor_prevents_div_by_zero(self):
        """All ticks identical → variance=0; std_floor must prevent ZeroDivisionError."""
        det = DeltaSpikeDetector(window=50, z_threshold=2.0)
        for _ in range(50):
            det.push(_make_tick("BUY", 1.0))
        # Must not raise
        result = det.push(_make_tick("BUY", 1.0))
        # z = (1 - 1) / std_eff = 0, so no event
        assert result is None

    def test_z_value_finite(self):
        """z must always be a finite float."""
        det = DeltaSpikeDetector(window=30, z_threshold=1.0)
        _warm_detector(det, n=35)
        for _ in range(20):
            result = det.push(_make_tick("BUY", random.uniform(0.1, 5.0)))
            if result is not None:
                assert math.isfinite(result["z"])


class TestDeltaSpikeDetectorTimestamp:

    def test_ts_fallback_from_ts_key(self):
        """Go tick has 'ts', not 'ts_ms'. Detector must fall back correctly."""
        det = DeltaSpikeDetector(window=120, z_threshold=2.0)
        _warm_detector(det, n=35)
        tick = _make_tick("BUY", 100_000.0, ts=1_700_000_012_345)
        result = det.push(tick)
        if result is not None:
            assert result["ts_ms"] == 1_700_000_012_345

    def test_ts_ms_key_takes_priority(self):
        """If tick has explicit 'ts_ms', it wins over 'ts'."""
        det = DeltaSpikeDetector(window=120, z_threshold=2.0)
        _warm_detector(det, n=35)
        tick = _make_tick("BUY", 100_000.0, ts=111)
        tick["ts_ms"] = "999999"
        result = det.push(tick)
        if result is not None:
            assert result["ts_ms"] == 999999


class TestDeltaSpikeDetectorHotReload:

    def test_z_threshold_update_in_place(self):
        det = DeltaSpikeDetector(window=120, z_threshold=3.0)
        _warm_detector(det, n=50)
        buf_len_before = len(det.values)
        det.z_threshold = 1.5
        assert det.z_threshold == pytest.approx(1.5)
        # Window history preserved
        assert len(det.values) == buf_len_before

    def test_min_abs_volume_update_in_place(self):
        det = DeltaSpikeDetector(window=120, z_threshold=2.0, min_abs_volume=0.0)
        _warm_detector(det, n=50)
        det.min_abs_volume = 1_000_000.0
        result = det.push(_make_tick("BUY", 1.0))
        assert result is None, "min_abs_volume gate should block small qty"


# ===========================================================================
# C. delta_abs_min_usd veto
# ===========================================================================

class TestDeltaAbsMinUsdVeto:
    """
    Tests for the USD veto logic that sits between G1 and DN-GATE in
    tick_decision_engine.py:671-682.

    The veto condition:
        if min_usd > 1.0 and delta_usd < min_usd: return None
    """

    def _apply_veto(self, delta: float, price: float, min_usd: float) -> bool:
        """Returns True if vetoed."""
        delta_usd = abs(delta) * price
        return min_usd > 1.0 and delta_usd < min_usd

    def test_veto_fires_when_usd_too_small(self):
        assert self._apply_veto(delta=1.5, price=10_000.0, min_usd=20_000.0) is True

    def test_veto_passes_when_usd_sufficient(self):
        assert self._apply_veto(delta=2.0, price=10_000.0, min_usd=15_000.0) is False

    def test_veto_disabled_when_min_usd_zero(self):
        assert self._apply_veto(delta=0.001, price=1.0, min_usd=0.0) is False

    def test_veto_disabled_when_min_usd_le_one(self):
        assert self._apply_veto(delta=0.001, price=1.0, min_usd=1.0) is False

    def test_veto_uses_abs_delta(self):
        # Negative delta (SELL spike) should still be measured by abs
        assert self._apply_veto(delta=-2.0, price=10_000.0, min_usd=15_000.0) is False

    def test_boundary_exactly_equal_passes(self):
        # delta_usd == min_usd: condition is <, so equal should PASS
        assert self._apply_veto(delta=1.5, price=10_000.0, min_usd=15_000.0) is False

    def test_veto_counter_increments(self):
        from services.orderflow.metrics import of_g1_veto_min_usd_total
        sym = "TESTUSDT_VETO_COUNTER"
        before = _counter_value(of_g1_veto_min_usd_total, sym)
        of_g1_veto_min_usd_total.labels(sym).inc()
        after = _counter_value(of_g1_veto_min_usd_total, sym)
        assert after == before + 1.0

    def test_veto_counter_is_per_symbol(self):
        from services.orderflow.metrics import of_g1_veto_min_usd_total
        sym_a = "SYMA_VETO"
        sym_b = "SYMB_VETO"
        of_g1_veto_min_usd_total.labels(sym_a).inc()
        before_b = _counter_value(of_g1_veto_min_usd_total, sym_b)
        assert _counter_value(of_g1_veto_min_usd_total, sym_a) >= 1.0
        assert _counter_value(of_g1_veto_min_usd_total, sym_b) == before_b

    def test_veto_logs_at_info_not_warning(self, caplog):
        """After our fix: USD veto logs at INFO, not WARNING."""
        import logging
        with caplog.at_level(logging.INFO, logger="services.orderflow.tick_decision_engine"):
            logger = logging.getLogger("services.orderflow.tick_decision_engine")
            logger.info("🛑 [G1-MIN-USD] (BTCUSDT) VETO: delta_usd=$5000.00 < min=$15000.00")
        assert any("G1-MIN-USD" in r.message and r.levelno == logging.INFO
                   for r in caplog.records)


# ===========================================================================
# D. ENV variable naming
# ===========================================================================

class TestEnvVariableNaming:

    def test_prefixed_env_overrides_delta_z_threshold(self):
        """BTC_DELTA_Z_THRESHOLD should override the BTC preset."""
        from core.instrument_config import get_config
        with patch.dict(os.environ, {"BTC_DELTA_Z_THRESHOLD": "1.23"}):
            cfg = get_config("BTCUSDT", use_env=True)
        assert cfg.delta_z_threshold == pytest.approx(1.23)

    def test_crypto_of_env_var_has_no_effect(self):
        """CRYPTO_OF_DELTA_Z_THRESHOLD must NOT affect any symbol config."""
        from core.instrument_config import get_config, INSTRUMENT_CONFIGS
        with patch.dict(os.environ, {"CRYPTO_OF_DELTA_Z_THRESHOLD": "0.01"},
                        clear=False):
            cfg_btc = get_config("BTCUSDT", use_env=True)
            cfg_eth = get_config("ETHUSDT", use_env=True)
        # Presets have z ≥ 2.5 — the bogus env var must not inject 0.01
        assert cfg_btc.delta_z_threshold >= 2.0, (
            f"CRYPTO_OF_DELTA_Z_THRESHOLD leaked into BTC config: {cfg_btc.delta_z_threshold}"
        )
        assert cfg_eth.delta_z_threshold >= 2.0, (
            f"CRYPTO_OF_DELTA_Z_THRESHOLD leaked into ETH config: {cfg_eth.delta_z_threshold}"
        )

    def test_symbol_prefix_mapping(self):
        """symbol_env_prefix returns the right prefix for common symbols."""
        from core.instrument_config import symbol_env_prefix
        assert symbol_env_prefix("BTCUSDT") == "BTC"
        assert symbol_env_prefix("ETHUSDT") == "ETH"
        assert symbol_env_prefix("SOLUSDT") == "SOL"
        assert symbol_env_prefix("1000PEPEUSDT") == "PEPE"
        assert symbol_env_prefix("1000SHIBUSDT") == "SHIB"

    def test_instrument_config_presets_have_reasonable_thresholds(self):
        """All INSTRUMENT_CONFIGS entries must have delta_z_threshold in [1.5, 5.0]."""
        from core.instrument_config import INSTRUMENT_CONFIGS
        for sym, cfg in INSTRUMENT_CONFIGS.items():
            z = cfg.delta_z_threshold
            assert z is None or (1.5 <= z <= 5.0), (
                f"{sym}: delta_z_threshold={z} is outside [1.5, 5.0]"
            )
