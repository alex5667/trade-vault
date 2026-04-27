from signal_scoring.reason_registry import iter_known_reason_codes, reason_code_to_u16, u16_to_reason_code


def test_reason_registry_is_complete_and_bijective() -> None:
    seen = set()
    for code in iter_known_reason_codes():
        u = int(reason_code_to_u16(code))
        assert u > 0, f"reason_code is not mapped to u16: {code}"
        assert u not in seen, f"duplicate u16 mapping: {u} for {code}"
        seen.add(u)
        back = u16_to_reason_code(u)
        assert back == code, f"reverse mapping mismatch: {code} -> {u} -> {back}"
