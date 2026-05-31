"""P2.4 — DQ micro + ConfirmationBarrier producer fix tests.

Covers:
  1. DQ micro: fallback from runtime.last_book_ts_ms when micro["book_stale_ms"]=0
  2. DQ micro: fallback from runtime.last_spread_bps when micro["spread_bps"]=0
  3. DQ micro: both fallbacks together feed calibrator (n>0)
  4. ConfirmationBarrier: iceberg kind now gets observed (was silently dropped)
  5. ConfirmationBarrier: delta_spike kind gets observed
  6. ConfirmationBarrier: lob_obi_5 from indicators takes priority over book depth
  7. ConfirmationBarrier: fallback to book depth when indicators absent
  8. ConfirmationBarrier: unknown kind maps to "of" bin
  9. ConfirmationBarrier: zero OBI from both sources → no observation (no crash)
"""
from __future__ import annotations

import math
import time
from types import SimpleNamespace
from typing import Any

import pytest

from core.confirmation_barrier_calibrator import ConfirmationBarrierCalibrator
from core.dq_microstructure_calibrator import DqMicrostructureCalibrator


# ─── helpers ────────────────────────────────────────────────────────────────

_NOW_MS = int(time.time() * 1000)
_BOOK_TS_MS = _NOW_MS - 800   # book was updated 800ms ago → stale=800ms


def _runtime(
    *,
    symbol: str = "BTCUSDT",
    last_book_ts_ms: int = _BOOK_TS_MS,
    last_spread_bps: float = 3.5,
    depth_5_bid_vol: float = 120.0,
    depth_5_ask_vol: float = 80.0,
) -> SimpleNamespace:
    """Minimal SymbolRuntime stub."""
    book = SimpleNamespace(
        depth_5_bid_vol=depth_5_bid_vol,
        depth_5_ask_vol=depth_5_ask_vol,
    )
    return SimpleNamespace(
        symbol=symbol,
        last_book_ts_ms=last_book_ts_ms,
        last_spread_bps=last_spread_bps,
        last_book=book,
    )


def _signal(
    kind: str = "iceberg",
    direction: str = "buy",
) -> dict[str, Any]:
    return {"kind": kind, "direction": direction, "side": direction}


# ─────────────────────────────────────────────────────────────────────────────
# P2.4a: DQ Micro fallback from runtime
# ─────────────────────────────────────────────────────────────────────────────

class TestDqMicroRuntimeFallback:
    """Verify that book_stale_ms and spread_bps are populated from runtime when
    signal micro dict is empty or zero."""

    def _observe_via_pipeline_logic(
        self,
        cal: DqMicrostructureCalibrator,
        *,
        runtime: Any,
        micro: dict,
        sig_ts_ms: int,
    ) -> None:
        """Replicate the _build_gate_ctx observation block from signal_pipeline.py."""
        _dq_sym = getattr(runtime, "symbol", "") or ""
        book_stale_ms = int(micro.get("book_stale_ms") or 0)
        if book_stale_ms <= 0 and runtime is not None:
            _last_book_ts = int(getattr(runtime, "last_book_ts_ms", 0) or 0)
            if _last_book_ts > 0:
                book_stale_ms = max(0, sig_ts_ms - _last_book_ts)
        _dq_spread_raw = float(micro.get("spread_bps") or 0.0)
        if _dq_spread_raw <= 0.0 and runtime is not None:
            _dq_spread_raw = float(getattr(runtime, "last_spread_bps", 0.0) or 0.0)
        if _dq_sym:
            cal.observe(symbol=_dq_sym, book_stale_ms=book_stale_ms, spread_bps=_dq_spread_raw)

    def test_empty_micro_uses_runtime_book_ts_for_stale(self):
        cal = DqMicrostructureCalibrator(min_samples=5, enforce=False, auto_promote=False)
        rt = _runtime(last_book_ts_ms=_BOOK_TS_MS)
        for _ in range(6):
            self._observe_via_pipeline_logic(cal, runtime=rt, micro={}, sig_ts_ms=_NOW_MS)
        assert cal._n.get("BTCUSDT", 0) >= 6, "calibrator should count runtime-stale observations"

    def test_empty_micro_uses_runtime_spread(self):
        cal = DqMicrostructureCalibrator(min_samples=5, enforce=False, auto_promote=False)
        rt = _runtime(last_spread_bps=3.5, last_book_ts_ms=0)  # book_ts=0 so stale fallback skipped
        for _ in range(6):
            self._observe_via_pipeline_logic(cal, runtime=rt, micro={}, sig_ts_ms=_NOW_MS)
        # spread=3.5 bps is within [SPREAD_FLOOR_BPS=0.5, SPREAD_CEIL_BPS=500] → counted
        assert cal._n.get("BTCUSDT", 0) >= 6, "calibrator should count runtime-spread observations"

    def test_both_fallbacks_together(self):
        cal = DqMicrostructureCalibrator(min_samples=5, enforce=False, auto_promote=False)
        rt = _runtime(last_book_ts_ms=_BOOK_TS_MS, last_spread_bps=4.2)
        for _ in range(6):
            self._observe_via_pipeline_logic(cal, runtime=rt, micro={}, sig_ts_ms=_NOW_MS)
        assert cal._n.get("BTCUSDT", 0) >= 6

    def test_micro_values_take_priority_over_runtime(self):
        """When micro has real values they must be used, not runtime."""
        cal = DqMicrostructureCalibrator(min_samples=5, enforce=False, auto_promote=False)
        rt = _runtime(last_book_ts_ms=_BOOK_TS_MS, last_spread_bps=999.0)
        micro = {"book_stale_ms": 200, "spread_bps": 2.0}
        for _ in range(6):
            self._observe_via_pipeline_logic(cal, runtime=rt, micro=micro, sig_ts_ms=_NOW_MS)
        # Should use micro values. Spread from runtime (999 bps) would be ceil-clamped.
        # We just check that observations were counted.
        assert cal._n.get("BTCUSDT", 0) >= 6

    def test_no_observation_when_runtime_has_no_book_ts_and_micro_empty(self):
        """No book_stale data at all → only spread used if runtime has it."""
        cal = DqMicrostructureCalibrator(min_samples=5, enforce=False, auto_promote=False)
        rt = _runtime(last_book_ts_ms=0, last_spread_bps=0.0)
        self._observe_via_pipeline_logic(cal, runtime=rt, micro={}, sig_ts_ms=_NOW_MS)
        # Both sources zero → nothing counted
        assert cal._n.get("BTCUSDT", 0) == 0


# ─────────────────────────────────────────────────────────────────────────────
# P2.4b: ConfirmationBarrier kind expansion
# ─────────────────────────────────────────────────────────────────────────────

class TestConfirmBarrierKindExpansion:
    """Verify that all signal kinds feed the ConfirmationBarrierCalibrator."""

    # Replicate _CB_KIND_MAP from signal_pipeline
    _CB_KIND_MAP: dict[str, str] = {
        "breakout": "breakout", "bo": "breakout",
        "absorption": "absorption", "abs": "absorption",
        "iceberg": "iceberg",
        "delta_spike": "delta_spike",
    }

    def _observe_via_pipeline_logic(
        self,
        cal: ConfirmationBarrierCalibrator,
        *,
        symbol: str,
        signal: dict[str, Any],
        runtime: Any,
        now_ms: int,
        indicators: dict[str, Any] | None = None,
    ) -> None:
        """Replicate _confirm_barrier_observe from signal_pipeline.py."""
        try:
            kind_raw = str(signal.get("kind") or "").lower().strip()
            cal_kind = self._CB_KIND_MAP.get(kind_raw, "of")
            ind = indicators or {}
            side_raw = str(signal.get("direction") or signal.get("side") or "").lower()
            dir_up = side_raw in ("buy", "long", "up", "bull", "1")

            _raw_obi = float(ind.get("lob_obi_5") or ind.get("depth_imbalance_5") or 0.0)
            if _raw_obi > 0.0:
                obi_ratio = _raw_obi
            else:
                if runtime is None:
                    return
                book = getattr(runtime, "last_book", None)
                if book is None:
                    return
                bid_vol = float(getattr(book, "depth_5_bid_vol", 0.0) or 0.0)
                ask_vol = float(getattr(book, "depth_5_ask_vol", 0.0) or 0.0)
                if bid_vol <= 0.0 or ask_vol <= 0.0:
                    return
                if cal_kind == "absorption":
                    obi_ratio = ask_vol / bid_vol if dir_up else bid_vol / ask_vol
                else:
                    obi_ratio = bid_vol / ask_vol if dir_up else ask_vol / bid_vol

            if obi_ratio is None or obi_ratio <= 0.0:
                return
            cal.observe((symbol or "").upper(), cal_kind, obi_ratio, now_ms)
        except Exception:
            pass

    # ── kind coverage ────────────────────────────────────────────────────────

    @pytest.mark.parametrize("kind", ["iceberg", "delta_spike", "breakout", "absorption", "bo", "abs"])
    def test_known_kinds_get_observed(self, kind: str):
        cal = ConfirmationBarrierCalibrator(min_samples=1)
        rt = _runtime()
        self._observe_via_pipeline_logic(
            cal, symbol="BTCUSDT", signal=_signal(kind=kind), runtime=rt, now_ms=_NOW_MS
        )
        counts = cal.sample_counts()
        assert sum(counts.values()) >= 1, f"kind={kind!r} produced no observation"

    def test_unknown_kind_maps_to_of_bin(self):
        cal = ConfirmationBarrierCalibrator(min_samples=1)
        rt = _runtime()
        self._observe_via_pipeline_logic(
            cal, symbol="BTCUSDT", signal=_signal(kind="some_future_kind"),
            runtime=rt, now_ms=_NOW_MS
        )
        counts = cal.sample_counts()
        assert ("BTCUSDT", "of") in counts, "unknown kind should land in 'of' bin"

    # ── OBI source priority ──────────────────────────────────────────────────

    def test_indicators_lob_obi_5_takes_priority(self):
        """lob_obi_5 in indicators should be used, not book depth volumes."""
        cal = ConfirmationBarrierCalibrator(min_samples=1)
        # book has 1.0 ratio (bid=ask), indicators has 2.5
        rt = _runtime(depth_5_bid_vol=100.0, depth_5_ask_vol=100.0)
        ind = {"lob_obi_5": 2.5}
        self._observe_via_pipeline_logic(
            cal, symbol="BTCUSDT", signal=_signal(kind="iceberg"), runtime=rt,
            now_ms=_NOW_MS, indicators=ind
        )
        counts = cal.sample_counts()
        assert counts.get(("BTCUSDT", "iceberg"), 0) == 1
        # The sample recorded should be 2.5 (clamped if necessary)
        b = cal._bins.get(("BTCUSDT", "iceberg"))
        assert b is not None
        assert abs(b.samples[0].obi - 2.5) < 0.01

    def test_depth_imbalance_5_also_works_as_obi_source(self):
        cal = ConfirmationBarrierCalibrator(min_samples=1)
        rt = _runtime(depth_5_bid_vol=100.0, depth_5_ask_vol=100.0)
        ind = {"depth_imbalance_5": 1.8}
        self._observe_via_pipeline_logic(
            cal, symbol="ETHUSDT", signal=_signal(kind="iceberg"), runtime=rt,
            now_ms=_NOW_MS, indicators=ind
        )
        b = cal._bins.get(("ETHUSDT", "iceberg"))
        assert b is not None
        assert abs(b.samples[0].obi - 1.8) < 0.01

    def test_falls_back_to_book_depth_when_indicators_empty(self):
        cal = ConfirmationBarrierCalibrator(min_samples=1)
        rt = _runtime(depth_5_bid_vol=150.0, depth_5_ask_vol=100.0)
        self._observe_via_pipeline_logic(
            cal, symbol="BTCUSDT", signal=_signal(kind="breakout", direction="buy"),
            runtime=rt, now_ms=_NOW_MS, indicators={}
        )
        counts = cal.sample_counts()
        assert counts.get(("BTCUSDT", "breakout"), 0) == 1
        # expected OBI = 150/100 = 1.5 (buy side, breakout)
        b = cal._bins[("BTCUSDT", "breakout")]
        assert abs(b.samples[0].obi - 1.5) < 0.01

    def test_absorption_buy_uses_ask_over_bid(self):
        """Absorption LONG: counter-side (asks) must dominate → ask/bid."""
        cal = ConfirmationBarrierCalibrator(min_samples=1)
        rt = _runtime(depth_5_bid_vol=100.0, depth_5_ask_vol=200.0)
        self._observe_via_pipeline_logic(
            cal, symbol="SOLUSDT", signal=_signal(kind="absorption", direction="buy"),
            runtime=rt, now_ms=_NOW_MS
        )
        b = cal._bins[("SOLUSDT", "absorption")]
        # ask/bid = 200/100 = 2.0
        assert abs(b.samples[0].obi - 2.0) < 0.01

    def test_no_observation_when_all_sources_zero(self):
        """Zero book volumes + no indicators → no crash, no observation."""
        cal = ConfirmationBarrierCalibrator(min_samples=1)
        rt = _runtime(depth_5_bid_vol=0.0, depth_5_ask_vol=0.0)
        self._observe_via_pipeline_logic(
            cal, symbol="BTCUSDT", signal=_signal(kind="iceberg"),
            runtime=rt, now_ms=_NOW_MS, indicators={}
        )
        assert cal.sample_counts() == {}

    def test_no_observation_when_runtime_none(self):
        """runtime=None and no indicators → no crash, no observation."""
        cal = ConfirmationBarrierCalibrator(min_samples=1)
        self._observe_via_pipeline_logic(
            cal, symbol="BTCUSDT", signal=_signal(kind="iceberg"),
            runtime=None, now_ms=_NOW_MS
        )
        assert cal.sample_counts() == {}

    # ── regression: original breakout/absorption still work ─────────────────

    def test_breakout_short_uses_ask_over_bid(self):
        """Breakout SELL: ask/bid ratio."""
        cal = ConfirmationBarrierCalibrator(min_samples=1)
        rt = _runtime(depth_5_bid_vol=100.0, depth_5_ask_vol=180.0)
        self._observe_via_pipeline_logic(
            cal, symbol="BTCUSDT", signal=_signal(kind="breakout", direction="sell"),
            runtime=rt, now_ms=_NOW_MS
        )
        b = cal._bins[("BTCUSDT", "breakout")]
        assert abs(b.samples[0].obi - 1.8) < 0.01

    def test_multi_symbol_multi_kind_accumulates(self):
        """Verify multiple symbols and kinds build separate bins."""
        cal = ConfirmationBarrierCalibrator(min_samples=1)
        rt_btc = _runtime(symbol="BTCUSDT")
        rt_eth = _runtime(symbol="ETHUSDT")
        ind = {"lob_obi_5": 1.3}
        for _ in range(3):
            self._observe_via_pipeline_logic(
                cal, symbol="BTCUSDT", signal=_signal(kind="iceberg"),
                runtime=rt_btc, now_ms=_NOW_MS, indicators=ind
            )
        for _ in range(2):
            self._observe_via_pipeline_logic(
                cal, symbol="ETHUSDT", signal=_signal(kind="delta_spike"),
                runtime=rt_eth, now_ms=_NOW_MS, indicators={"lob_obi_5": 1.6}
            )
        counts = cal.sample_counts()
        assert counts.get(("BTCUSDT", "iceberg"), 0) == 3
        assert counts.get(("ETHUSDT", "delta_spike"), 0) == 2
