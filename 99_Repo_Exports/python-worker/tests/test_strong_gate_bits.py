from __future__ import annotations

from core.strong_of_gate import eval_continuation
from core.of_confirm_contract import BIT_A, BIT_B, BIT_C, BIT_D


def test_gate_bits_continuation_sets_bits():
    cfg = {"strong_need_continuation": 2, "strong_use_iceberg": True, "abs_lvl_enable": 1, "abs_lvl_counts_as": "A"}
    dec = eval_continuation(
        direction="LONG",
        trend_dir="LONG",
        hidden_ctx_recent=True,   # A=1
        iceberg_strict=False,
        obi_stable=True,          # B=1
        cont_ctx_recent=False,    # C=0
        abs_lvl_ok=False,
        cfg=cfg,
    )
    assert (dec.gate_bits & BIT_A) != 0
    assert (dec.gate_bits & BIT_B) != 0
    assert (dec.gate_bits & BIT_C) == 0
    assert (dec.gate_bits & BIT_D) == 0


def test_gate_bits_sets_abs_lvl_d_bit():
    cfg = {"strong_need_continuation": 2, "abs_lvl_enable": 1, "abs_lvl_counts_as": "A"}
    dec = eval_continuation(
        direction="LONG",
        trend_dir="LONG",
        hidden_ctx_recent=False,
        iceberg_strict=False,
        obi_stable=False,
        cont_ctx_recent=True,
        abs_lvl_ok=True,
        cfg=cfg,
    )
    assert (dec.gate_bits & BIT_D) != 0
