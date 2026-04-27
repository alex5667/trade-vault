from __future__ import annotations

import pytest
from core.of_confirm_contract import OFConfirmV3, pack_bits, BIT_A, BIT_B, BIT_C
from core.strong_of_gate import eval_reversal, eval_continuation


def test_pack_bits():
    assert pack_bits(True, False, False) == BIT_A
    assert pack_bits(False, True, False) == BIT_B
    assert pack_bits(False, False, True) == BIT_C
    assert pack_bits(True, True, True) == BIT_A | BIT_B | BIT_C


def test_reversal_gate_bits():
    cfg = {"strong_z_min": 2.0}
    # Scenario: A and B are True, C is False
    dec = eval_reversal(
        direction="LONG",
        delta_z=3.0,
        weak_progress=True,     # -> A
        sweep_recent=True,      # -> B
        reclaim_recent=True,    # -> B
        obi_stable=False,       # -> C
        iceberg_strict=False,   # -> C
        abs_lvl_ok=False,
        cfg=cfg
    )
    assert dec.ok is True
    assert dec.gate_bits == BIT_A | BIT_B
    assert dec.a == 1
    assert dec.b == 1
    assert dec.c == 0


def test_continuation_gate_bits():
    cfg = {}
    # Scenario: A and C are True, B is False
    # A: hidden_ctx_recent and direction==trend_dir
    # B: iceberg_strict or obi_stable
    # C: cont_ctx_recent
    dec = eval_continuation(
        direction="LONG",
        trend_dir="LONG",
        hidden_ctx_recent=True, # -> A
        iceberg_strict=False,   # -> B
        obi_stable=False,       # -> B
        cont_ctx_recent=True,    # -> C
        abs_lvl_ok=False,
        cfg=cfg
    )
    assert dec.ok is True
    assert dec.gate_bits == BIT_A | BIT_C
    assert dec.a == 1
    assert dec.b == 0
    assert dec.c == 1


def test_of_confirm_v3_to_dict():
    ofc = OFConfirmV3(
        v=3,
        symbol="BTCUSDT",
        ts_ms=123456789,
        direction="LONG",
        scenario="reversal",
        ok=1,
        score=0.75,
        have=2,
        need=2,
        gate_bits=BIT_A | BIT_B,
        reason="reversal_gate",
        evidence={"foo": "bar"},
        contrib={}
    )
    d = ofc.to_dict()
    assert d["v"] == 3
    assert d["gate_bits"] == BIT_A | BIT_B
    assert d["reason"] == "reversal_gate"
