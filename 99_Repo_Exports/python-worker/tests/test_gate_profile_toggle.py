from domain.gate_profile import strict_enabled


def test_gate_profile_priority(monkeypatch):
    # GATE_PROFILE has priority over GATES_STRICT
    monkeypatch.setenv("GATES_STRICT", "1")
    monkeypatch.setenv("GATE_PROFILE", "default")
    assert strict_enabled() is False

    monkeypatch.setenv("GATE_PROFILE", "strict")
    assert strict_enabled() is True


def test_gates_strict_fallback(monkeypatch):
    monkeypatch.delenv("GATE_PROFILE", raising=False)
    monkeypatch.setenv("GATES_STRICT", "1")
    assert strict_enabled() is True

    monkeypatch.setenv("GATES_STRICT", "0")
    assert strict_enabled() is False
