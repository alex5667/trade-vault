"""Tests for CtxGateTightenCalibrator auto-promote logic."""
from __future__ import annotations

import math
import time

import pytest

from core.sentiment_defillama_ctx_calibrator import (
    CtxGateTightenCalibrator,
    SentimentDefiLlamaCtxCalibrator,
)

MS = 1_000
HOUR_MS = 3_600_000
DAY_MS = 24 * HOUR_MS


def _cal(*, min_tightened: int = 50, min_hours: float = 6.0, enforce: bool = False) -> CtxGateTightenCalibrator:
    c = CtxGateTightenCalibrator(gate="test", default_cap_bps=2.0)
    c.min_tightened = min_tightened
    c.auto_promote_min_hours = min_hours
    c.enforce = enforce
    return c


def _feed(cal: CtxGateTightenCalibrator, n: int, *, r: float, tighten_bps: float, base_ms: int) -> None:
    for i in range(n):
        cal.observe(r=r, tighten_bps=tighten_bps, w=1.0, ts_ms=base_ms + i * MS)


# ── basic auto-promote ────────────────────────────────────────────────────────

class TestAutoPromoteBasic:
    def test_not_promoted_below_min_tightened(self):
        c = _cal(min_tightened=50, min_hours=0.0)
        t0 = 1_700_000_000_000
        _feed(c, 49, r=0.1, tighten_bps=1.0, base_ms=t0)
        c.recompute(t0 + 49 * MS)
        assert not c.auto_promoted
        assert not c.enforce

    def test_not_promoted_before_min_hours(self):
        c = _cal(min_tightened=10, min_hours=6.0)
        t0 = 1_700_000_000_000
        _feed(c, 20, r=0.1, tighten_bps=1.0, base_ms=t0)
        # only 1 ms elapsed since first sample
        c.recompute(t0 + 20 * MS)
        assert not c.auto_promoted
        assert not c.enforce

    def test_promotes_when_criteria_met(self):
        c = _cal(min_tightened=10, min_hours=0.0)
        t0 = 1_700_000_000_000
        _feed(c, 15, r=0.1, tighten_bps=1.0, base_ms=t0)
        c.recompute(t0 + DAY_MS)
        assert c.auto_promoted
        assert c.enforce
        assert c._auto_promoted_ms > 0

    def test_promotes_after_min_hours_elapsed(self):
        c = _cal(min_tightened=10, min_hours=6.0)
        t0 = 1_700_000_000_000
        _feed(c, 15, r=0.1, tighten_bps=1.0, base_ms=t0)
        # 5.9 hours — not yet
        c.recompute(t0 + int(5.9 * HOUR_MS))
        assert not c.auto_promoted
        # 6.1 hours — should promote
        c.recompute(t0 + int(6.1 * HOUR_MS))
        assert c.auto_promoted
        assert c.enforce

    def test_promote_is_sticky(self):
        c = _cal(min_tightened=5, min_hours=0.0)
        t0 = 1_700_000_000_000
        _feed(c, 10, r=0.1, tighten_bps=1.0, base_ms=t0)
        c.recompute(t0 + DAY_MS)
        assert c.auto_promoted
        # Window expires — buffer empties — but still promoted
        c.recompute(t0 + 30 * DAY_MS)
        assert c.auto_promoted
        assert c.enforce

    def test_auto_promote_disabled(self):
        c = _cal(min_tightened=5, min_hours=0.0)
        c.auto_promote = False
        t0 = 1_700_000_000_000
        _feed(c, 10, r=0.1, tighten_bps=1.0, base_ms=t0)
        c.recompute(t0 + DAY_MS)
        assert not c.auto_promoted
        assert not c.enforce


# ── snapshot round-trip ───────────────────────────────────────────────────────

class TestSnapshotRoundTrip:
    def test_snapshot_includes_auto_promoted(self):
        c = _cal(min_tightened=5, min_hours=0.0)
        t0 = 1_700_000_000_000
        _feed(c, 10, r=0.1, tighten_bps=1.0, base_ms=t0)
        c.recompute(t0 + DAY_MS)
        snap = c.snapshot()
        assert snap["auto_promoted"] is True
        assert snap["auto_promoted_ms"] > 0
        assert snap["first_sample_ms"] == t0

    def test_snapshot_not_promoted_before_criteria(self):
        c = _cal(min_tightened=100, min_hours=0.0)
        t0 = 1_700_000_000_000
        _feed(c, 5, r=0.1, tighten_bps=1.0, base_ms=t0)
        c.recompute(t0 + DAY_MS)
        snap = c.snapshot()
        assert snap["auto_promoted"] is False
        assert snap["auto_promoted_ms"] == 0

    def test_loads_gate_state_restores_promoted(self):
        c = _cal(min_tightened=5, min_hours=0.0)
        t0 = 1_700_000_000_000
        _feed(c, 10, r=0.15, tighten_bps=1.0, base_ms=t0)
        c.recompute(t0 + DAY_MS)
        snap = c.snapshot()

        c2 = _cal()
        c2.loads_gate_state(snap)
        assert c2.auto_promoted
        assert c2.enforce  # sticky restore
        assert c2._auto_promoted_ms == snap["auto_promoted_ms"]
        assert c2._first_sample_ms == snap["first_sample_ms"]

    def test_loads_gate_state_not_promoted(self):
        c = _cal(min_tightened=100)
        t0 = 1_700_000_000_000
        _feed(c, 5, r=0.1, tighten_bps=1.0, base_ms=t0)
        c.recompute(t0 + DAY_MS)
        snap = c.snapshot()

        c2 = _cal()
        c2.loads_gate_state(snap)
        assert not c2.auto_promoted
        assert not c2.enforce

    def test_loads_gate_state_enforce_not_overridden_if_not_promoted(self):
        # enforce=True via ENV should not be cleared by a not-promoted snapshot
        c = _cal(min_tightened=100, enforce=True)
        t0 = 1_700_000_000_000
        _feed(c, 5, r=0.1, tighten_bps=1.0, base_ms=t0)
        c.recompute(t0 + DAY_MS)
        snap = c.snapshot()

        c2 = CtxGateTightenCalibrator(gate="test", default_cap_bps=2.0, enforce=True)
        c2.loads_gate_state(snap)
        # enforce was True from ENV, snap says not promoted — enforce stays True
        assert c2.enforce


# ── umbrella calibrator ───────────────────────────────────────────────────────

class TestSentimentDefiLlamaAutoPromote:
    def test_umbrella_snapshot_includes_both_gates(self):
        cal = SentimentDefiLlamaCtxCalibrator(enforce=False)
        cal.sentiment.min_tightened = 5
        cal.sentiment.auto_promote_min_hours = 0.0
        cal.defillama.min_tightened = 5
        cal.defillama.auto_promote_min_hours = 0.0
        t0 = 1_700_000_000_000

        for i in range(10):
            cal.observe(
                r=0.12, sentiment_tighten_bps=1.0, defillama_tighten_bps=2.0,
                w=1.0, ts_ms=t0 + i * MS,
            )
        cal.recompute(t0 + DAY_MS)
        snap = cal.snapshot()

        assert snap["sentiment"]["auto_promoted"] is True
        assert snap["defillama"]["auto_promoted"] is True

    def test_umbrella_loads_restores_promote_state(self):
        cal = SentimentDefiLlamaCtxCalibrator(enforce=False)
        cal.sentiment.min_tightened = 5
        cal.sentiment.auto_promote_min_hours = 0.0
        cal.defillama.min_tightened = 5
        cal.defillama.auto_promote_min_hours = 0.0
        t0 = 1_700_000_000_000

        for i in range(10):
            cal.observe(
                r=0.12, sentiment_tighten_bps=1.0, defillama_tighten_bps=2.0,
                w=1.0, ts_ms=t0 + i * MS,
            )
        cal.recompute(t0 + DAY_MS)
        snap = cal.snapshot()

        cal2 = SentimentDefiLlamaCtxCalibrator.loads(snap, enforce=False)
        assert cal2.sentiment.auto_promoted
        assert cal2.sentiment.enforce
        assert cal2.defillama.auto_promoted
        assert cal2.defillama.enforce

    def test_independent_gate_promotion(self):
        """Sentiment reaches threshold, defillama does not."""
        cal = SentimentDefiLlamaCtxCalibrator(enforce=False)
        cal.sentiment.min_tightened = 5
        cal.sentiment.auto_promote_min_hours = 0.0
        cal.defillama.min_tightened = 100
        cal.defillama.auto_promote_min_hours = 0.0
        t0 = 1_700_000_000_000

        for i in range(10):
            cal.observe(
                r=0.1, sentiment_tighten_bps=1.0, defillama_tighten_bps=0.0,
                w=1.0, ts_ms=t0 + i * MS,
            )
        cal.recompute(t0 + DAY_MS)

        assert cal.sentiment.auto_promoted
        assert cal.sentiment.enforce
        assert not cal.defillama.auto_promoted
        assert not cal.defillama.enforce


# ── cap adaptation after promote ─────────────────────────────────────────────

class TestCapAdaptationAfterPromote:
    def test_cap_moves_up_when_ev_low(self):
        c = _cal(min_tightened=10, min_hours=0.0)
        c.hold_ms = 0  # no hold for test speed
        t0 = 1_700_000_000_000
        # EV = -0.05 (well below target 0.08)
        _feed(c, 20, r=-0.05, tighten_bps=1.0, base_ms=t0)
        c.recompute(t0 + DAY_MS)
        assert c.auto_promoted  # promoted
        assert c.cap_bps > 2.0  # cap moved up

    def test_cap_moves_down_when_ev_high(self):
        c = _cal(min_tightened=10, min_hours=0.0)
        c.hold_ms = 0
        t0 = 1_700_000_000_000
        # EV = 0.5 (well above target+margin)
        _feed(c, 20, r=0.5, tighten_bps=1.0, base_ms=t0)
        c.recompute(t0 + DAY_MS)
        assert c.auto_promoted
        assert c.cap_bps < 2.0  # cap moved down

    def test_no_cap_change_in_shadow_before_promote(self):
        c = _cal(min_tightened=100, min_hours=0.0)  # never promotes
        c.hold_ms = 0
        t0 = 1_700_000_000_000
        _feed(c, 5, r=-0.5, tighten_bps=1.0, base_ms=t0)
        c.recompute(t0 + DAY_MS)
        # cap unchanged — enforce=False, not promoted
        assert c.cap_bps == 2.0
