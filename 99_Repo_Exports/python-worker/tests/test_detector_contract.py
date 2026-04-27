from __future__ import annotations

from dataclasses import dataclass

from handlers.detector.detector import Detector


@dataclass
class Ctx:
    price: float
    z_delta: float = 0.0
    weak_progress: float = 0.0
    obi: float = 0.0
    obi_sustained: bool = False
    wall_here: bool = False
    refill: bool = False
    mp_contra: bool = False
    micro_proxy: bool = False


def test_detector_emits_breakout_and_extreme_when_z_high():
    d = Detector()
    ctx = Ctx(price=100.0, z_delta=4.0)
    cands = d.detect(ctx)
    kinds = {c.kind for c in cands}
    assert "breakout" in kinds
    assert "extreme" in kinds
    assert all(c.raw_score > 0 for c in cands)


def test_detector_absorption_event_only_no_veto_logic():
    d = Detector()
    ctx = Ctx(price=100.0, weak_progress=0.20, wall_here=True, mp_contra=False, micro_proxy=False)
    cands = d.detect(ctx)
    assert any(c.kind == "absorption" for c in cands)


def test_detector_obi_spike_when_sustained():
    d = Detector()
    ctx = Ctx(price=100.0, obi=1.6, obi_sustained=True)
    cands = d.detect(ctx)
    assert any(c.kind == "obi_spike" for c in cands)
