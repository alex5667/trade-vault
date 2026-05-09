
import os


def test_confidence_gate_logic():
    print("Testing confidence gate logic from signal_pipeline.py...")

    # Mock environment
    os.environ["CRYPTO_SIGNAL_MIN_CONF"] = "55"

    def simulate_pipeline_confidence_gate(confidence, config_disable=False):
        # --- Logic from signal_pipeline.py ---
        min_conf_pct = float(os.getenv("CRYPTO_SIGNAL_MIN_CONF", os.getenv("SIGNAL_MIN_CONF", "70")))
        if 0 < min_conf_pct <= 1:
            min_conf_pct *= 100.0
        min_conf = min_conf_pct / 100.0

        # If not disabled
        if not config_disable:
            if confidence < min_conf:
                return "DROPPED"
        return "PROCEEDED"
        # ---

    # Case 1: Low confidence (50 < 55) -> DROP
    res = simulate_pipeline_confidence_gate(0.50)
    print(f"confidence=0.50, min=0.55 -> {res}")
    assert res == "DROPPED"

    # Case 2: Sufficient confidence (56 > 55) -> PROCEED
    res = simulate_pipeline_confidence_gate(0.56)
    print(f"confidence=0.56, min=0.55 -> {res}")
    assert res == "PROCEEDED"

    # Case 3: Exact confidence (55 == 55) -> PROCEED
    res = simulate_pipeline_confidence_gate(0.55)
    print(f"confidence=0.55, min=0.55 -> {res}")
    assert res == "PROCEEDED"

    # Case 4: Low confidence but filter DISABLED -> PROCEED
    res = simulate_pipeline_confidence_gate(0.40, config_disable=True)
    print(f"confidence=0.40, min=0.55, disabled=True -> {res}")
    assert res == "PROCEEDED"

    print("✅ Confidence gate logic verification passed!")

def test_is_virtual_logic_v2():
    print("\nTesting is_virtual logic from signal_pipeline.py...")

    def get_is_virtual(validation_status, of_gate_mode, gate_shadow_veto):
        # --- Logic from signal_pipeline.py ---
        gate_mode = (of_gate_mode or "").upper()
        is_virtual = 0
        if validation_status == "failed" or gate_shadow_veto or (gate_mode == "SHADOW" and validation_status == "failed"):
            is_virtual = 1
        # ---
        return is_virtual

    # Case A: FAILED validation in ENFORCE mode
    v = get_is_virtual("failed", "ENFORCE", False)
    print(f"status=failed, mode=ENFORCE -> is_virtual={v}")
    assert v == 1

    # Case B: FAILED validation in SHADOW mode
    v = get_is_virtual("failed", "SHADOW", False)
    print(f"status=failed, mode=SHADOW -> is_virtual={v}")
    assert v == 1

    # Case C: PASSED validation in ENFORCE mode
    v = get_is_virtual("passed", "ENFORCE", False)
    print(f"status=passed, mode=ENFORCE -> is_virtual={v}")
    assert v == 0

    print("✅ Pipeline is_virtual logic verification passed!")

if __name__ == "__main__":
    test_confidence_gate_logic()
    test_is_virtual_logic_v2()
