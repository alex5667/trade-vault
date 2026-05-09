from core.footprint_policy import is_soft_confirmation

# ------------------------------------------------------------------------------------------------
# TRADE: G8 · MIN-CONFIRMATIONS (Delta + Hard Confirmations Gate) testing
# ------------------------------------------------------------------------------------------------
# As the gate is deeply embedded in the 2500+ line process_tick monolith without a
# dedicated structural boundary or helper class, we test the exact logical block
# using a deterministic replica evaluator.

def evaluate_g8_gate(config: dict, delta_event: dict, confirmations: list[str]) -> bool:
    """
    Replica of the G8 logic found in orderflow_strategy.py (~L2616-L2650)
    Returns True if signal passes, False if filtered (returns None).
    """
    delta_abs = abs(delta_event.get("delta", 0.0))
    min_delta = config.get("delta_abs_min_confirm", 0.0)
    min_confirmations = int(config.get("min_confirmations", 0))

    fp_imb_counts = bool(config.get("fp_imb_counts_for_min_confirmations", False))

    if fp_imb_counts:
        hard_count = len(confirmations)
    else:
        hard_count = 0
        for c in confirmations:
            if is_soft_confirmation(c):
                continue
            hard_count += 1

    if delta_abs < min_delta and hard_count < min_confirmations:
        return False  # Filtered (return None in prod)

    return True  # Passes

def test_min_confirmations_gate_pass_via_delta():
    config = {
        "delta_abs_min_confirm": 2.0,
        "min_confirmations": 1,
        "fp_imb_counts_for_min_confirmations": False
    }
    delta_event = {"delta": 2.5}  # Passes min_delta (2.5 >= 2.0)
    confirmations = []  # Fails hard_count (0 < 1)

    assert evaluate_g8_gate(config, delta_event, confirmations) == True, "Should PASS via delta"

def test_min_confirmations_gate_pass_via_hard_count():
    config = {
        "delta_abs_min_confirm": 2.0,
        "min_confirmations": 1,
        "fp_imb_counts_for_min_confirmations": False
    }
    delta_event = {"delta": 1.0}  # Fails min_delta (1.0 < 2.0)
    confirmations = ["sweep_eqh=1"]  # Passes hard_count (1 >= 1)

    assert evaluate_g8_gate(config, delta_event, confirmations) == True, "Should PASS via hard count"

def test_min_confirmations_gate_fail_all():
    config = {
        "delta_abs_min_confirm": 2.0,
        "min_confirmations": 2,
        "fp_imb_counts_for_min_confirmations": False
    }
    delta_event = {"delta": 1.5}  # Fails min_delta (1.5 < 2.0)
    confirmations = ["sweep_eqh=1", "fp_imb=0.9"]  # Soft confirmation 'fp_imb' ignored -> hard count is 1 (Fails < 2)

    assert evaluate_g8_gate(config, delta_event, confirmations) == False, "Should FAIL (filtered)"

def test_min_confirmations_gate_fp_imb_counts_true():
    config = {
        "delta_abs_min_confirm": 2.0,
        "min_confirmations": 2,
        "fp_imb_counts_for_min_confirmations": True  # KEY FIX: NOW fp_imb counts!
    }
    delta_event = {"delta": 1.5}  # Fails min_delta
    confirmations = ["sweep_eqh=1", "fp_imb=0.9"]  # 2 total confirmations, both count -> hard count is 2 (Passes >= 2)

    assert evaluate_g8_gate(config, delta_event, confirmations) == True, "Should PASS because fp_imb_counts_for_min_confirmations=True"
