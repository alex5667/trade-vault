from core.strong_of_gate import eval_continuation, eval_reversal

# --- REVERSAL TESTS ---

def test_reversal_A_and_B():
    """Reversal: Needs 2. Provides A and B."""
    cfg = {"strong_need_reversal": 2, "strong_z_min": 2.0}
    dec = eval_reversal(
        direction="LONG",
        delta_z=2.5, weak_progress=True,  # A = True
        sweep_recent=True, reclaim_recent=True,  # B = True
        obi_stable=False, iceberg_strict=False, abs_lvl_ok=False, fp_edge_absorb=False, ofi_leg=False, # C = False
        cfg=cfg
    )
    assert dec.ok is True
    assert dec.have == 2
    assert dec.a == 1 and dec.b == 1 and dec.c == 0

def test_reversal_A_and_C_obi():
    """Reversal: Needs 2. Provides A and C (via obi_stable)."""
    cfg = {"strong_need_reversal": 2, "strong_z_min": 2.0}
    dec = eval_reversal(
        direction="LONG",
        delta_z=-2.5, weak_progress=True,  # A = True (abs(delta_z))
        sweep_recent=False, reclaim_recent=False,  # B = False
        obi_stable=True, iceberg_strict=False, abs_lvl_ok=False, fp_edge_absorb=False, ofi_leg=False, # C = True
        cfg=cfg
    )
    assert dec.ok is True
    assert dec.have == 2
    assert dec.a == 1 and dec.b == 0 and dec.c == 1

def test_reversal_B_and_C_ofi():
    """Reversal: Needs 2. Provides B and C (via ofi_leg)."""
    cfg = {"strong_need_reversal": 2, "strong_z_min": 2.0}
    dec = eval_reversal(
        direction="LONG",
        delta_z=1.0, weak_progress=False,  # A = False
        sweep_recent=True, reclaim_recent=True,  # B = True
        obi_stable=False, iceberg_strict=False, abs_lvl_ok=False, fp_edge_absorb=False, ofi_leg=True, # C = True
        cfg=cfg
    )
    assert dec.ok is True
    assert dec.have == 2
    assert dec.a == 0 and dec.b == 1 and dec.c == 1
    # Check gate_bits bit 2 for C
    assert (dec.gate_bits & (1 << 2)) != 0

def test_reversal_B_and_C_fp_edge():
    """Reversal: Needs 2. Provides B and C (via fp_edge_absorb)."""
    cfg = {"strong_need_reversal": 2, "strong_z_min": 2.0}
    dec = eval_reversal(
        direction="LONG",
        delta_z=1.0, weak_progress=False,  # A = False
        sweep_recent=True, reclaim_recent=True,  # B = True
        obi_stable=False, iceberg_strict=False, abs_lvl_ok=False, fp_edge_absorb=True, ofi_leg=False, # C = True
        cfg=cfg
    )
    assert dec.ok is True
    assert dec.have == 2
    assert dec.a == 0 and dec.b == 1 and dec.c == 1

def test_reversal_A_only_fails():
    """Reversal: Needs 2. Provides only A -> Fail."""
    cfg = {"strong_need_reversal": 2, "strong_z_min": 2.0}
    dec = eval_reversal(
        direction="LONG",
        delta_z=2.5, weak_progress=True,  # A = True
        sweep_recent=False, reclaim_recent=True,  # B = False (needs both)
        obi_stable=False, iceberg_strict=False, abs_lvl_ok=False, fp_edge_absorb=False, ofi_leg=False, # C = False
        cfg=cfg
    )
    assert dec.ok is False
    assert dec.have == 1

def test_reversal_abs_lvl_counts_as_A():
    """Reversal: abs_lvl_ok configures as A."""
    cfg = {"strong_need_reversal": 2, "strong_z_min": 2.0, "abs_lvl_enable": 1, "abs_lvl_counts_as": "A"}
    dec = eval_reversal(
        direction="LONG",
        delta_z=1.0, weak_progress=False,  # Normally A = False
        sweep_recent=True, reclaim_recent=True,  # B = True
        obi_stable=False, iceberg_strict=False, abs_lvl_ok=True, fp_edge_absorb=False, ofi_leg=False, # C = False, abs_lvl_ok=True -> A=True
        cfg=cfg
    )
    assert dec.ok is True
    assert dec.have == 2
    assert dec.a == 1 and dec.b == 1 and dec.c == 0

def test_reversal_abs_lvl_counts_as_C():
    """Reversal: abs_lvl_ok configures as C."""
    cfg = {"strong_need_reversal": 2, "strong_z_min": 2.0, "abs_lvl_enable": 1, "abs_lvl_counts_as": "C"}
    dec = eval_reversal(
        direction="LONG",
        delta_z=2.5, weak_progress=True,  # A = True
        sweep_recent=False, reclaim_recent=False,  # B = False
        obi_stable=False, iceberg_strict=False, abs_lvl_ok=True, fp_edge_absorb=False, ofi_leg=False, # normally C = False, but abs_lvl_ok=True -> C=True
        cfg=cfg
    )
    assert dec.ok is True
    assert dec.have == 2
    assert dec.a == 1 and dec.b == 0 and dec.c == 1

def test_reversal_abs_lvl_disabled():
    """Reversal: abs_lvl_ok is true, but disabled in config."""
    cfg = {"strong_need_reversal": 2, "strong_z_min": 2.0, "abs_lvl_enable": 0, "abs_lvl_counts_as": "C"}
    dec = eval_reversal(
        direction="LONG",
        delta_z=2.5, weak_progress=True,  # A = True
        sweep_recent=False, reclaim_recent=False,  # B = False
        obi_stable=False, iceberg_strict=False, abs_lvl_ok=True, fp_edge_absorb=False, ofi_leg=False, # C = False, abs_lvl_enable=0 ignores abs_lvl
        cfg=cfg
    )
    assert dec.ok is False
    assert dec.have == 1
    assert dec.a == 1 and dec.b == 0 and dec.c == 0

# --- CONTINUATION TESTS ---

def test_continuation_no_trend_dir():
    """Continuation: Fails immediately if trend_dir is None."""
    cfg = {"strong_need_continuation": 2}
    dec = eval_continuation(
        direction="LONG", trend_dir=None,
        hidden_ctx_recent=True, iceberg_strict=True, obi_stable=True, cont_ctx_recent=True,
        abs_lvl_ok=False, ofi_leg=False, fp_edge_absorb=False,
        cfg=cfg
    )
    assert dec.ok is False
    assert dec.reason == "no_trend_dir"

def test_continuation_A_and_B():
    """Continuation: Needs 2. Provides A and B (via obi_stable)."""
    cfg = {"strong_need_continuation": 2}
    dec = eval_continuation(
        direction="LONG", trend_dir="LONG",
        hidden_ctx_recent=True,  # A = True
        iceberg_strict=False, obi_stable=True, cont_ctx_recent=False, # B = True, C = False
        abs_lvl_ok=False, ofi_leg=False, fp_edge_absorb=False,
        cfg=cfg
    )
    assert dec.ok is True
    assert dec.have == 2
    assert dec.a == 1 and dec.b == 1 and dec.c == 0

def test_continuation_A_and_B_fp_edge():
    """Continuation: Needs 2. Provides A and B (via fp_edge_absorb)."""
    cfg = {"strong_need_continuation": 2}
    dec = eval_continuation(
        direction="LONG", trend_dir="LONG",
        hidden_ctx_recent=True,  # A = True
        iceberg_strict=False, obi_stable=False, cont_ctx_recent=False, # C = False
        abs_lvl_ok=False, ofi_leg=False, fp_edge_absorb=True, # B = True
        cfg=cfg
    )
    assert dec.ok is True
    assert dec.have == 2
    assert dec.a == 1 and dec.b == 1 and dec.c == 0

def test_continuation_wrong_direction_A():
    """Continuation: A is false if direction != trend_dir."""
    cfg = {"strong_need_continuation": 2}
    dec = eval_continuation(
        direction="LONG", trend_dir="SHORT",
        hidden_ctx_recent=True,  # Context is recent but direction mismatch -> A = False
        iceberg_strict=False, obi_stable=True, cont_ctx_recent=False, # B = True, C = False
        abs_lvl_ok=False, ofi_leg=False, fp_edge_absorb=False,
        cfg=cfg
    )
    assert dec.ok is False
    assert dec.have == 1
    assert dec.a == 0 and dec.b == 1

def test_continuation_B_and_C():
    """Continuation: Needs 2. Provides B and C."""
    cfg = {"strong_need_continuation": 2}
    dec = eval_continuation(
        direction="LONG", trend_dir="LONG",
        hidden_ctx_recent=False,  # A = False
        iceberg_strict=False, obi_stable=False, cont_ctx_recent=True, # C = True
        abs_lvl_ok=False, ofi_leg=True, fp_edge_absorb=False, # B = True
        cfg=cfg
    )
    assert dec.ok is True
    assert dec.have == 2
    assert dec.a == 0 and dec.b == 1 and dec.c == 1

def test_continuation_abs_lvl_counts_as_B():
    """Continuation: abs_lvl_ok configures as B."""
    cfg = {"strong_need_continuation": 2, "abs_lvl_enable": 1, "abs_lvl_counts_as": "B"}
    dec = eval_continuation(
        direction="LONG", trend_dir="LONG",
        hidden_ctx_recent=True,  # A = True
        iceberg_strict=False, obi_stable=False, cont_ctx_recent=False, # normally B = False, C=False
        abs_lvl_ok=True, ofi_leg=False, fp_edge_absorb=False, # abs_lvl_ok -> B=True
        cfg=cfg
    )
    assert dec.ok is True
    assert dec.have == 2
    assert dec.a == 1 and dec.b == 1 and dec.c == 0

def test_continuation_abs_lvl_counts_as_A():
    """Continuation: abs_lvl_ok configures as A."""
    cfg = {"strong_need_continuation": 2, "abs_lvl_enable": 1, "abs_lvl_counts_as": "A"}
    dec = eval_continuation(
        direction="LONG", trend_dir="LONG",
        hidden_ctx_recent=False,  # normally A = False
        iceberg_strict=False, obi_stable=True, cont_ctx_recent=False, # B = True, C = False
        abs_lvl_ok=True, ofi_leg=False, fp_edge_absorb=False, # abs_lvl_ok -> A=True
        cfg=cfg
    )
    assert dec.ok is True
    assert dec.have == 2
    assert dec.a == 1 and dec.b == 1 and dec.c == 0
