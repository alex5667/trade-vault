import importlib
import os
import pytest


def test_strict_reason_codes_raises_on_unknown(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("STRICT_REASON_CODES", "1")

    # Re-import module to re-evaluate STRICT flag
    import handlers.confirmations.engine as engine_mod
    importlib.reload(engine_mod)

    Validation = engine_mod.Validation

    # Unknown reason_code + unknown legacy reason => should raise
    with pytest.raises(ValueError):
        Validation(True, 0.0, [], "some_legacy_reason", {}, reason_code="NOT_A_CODE")


def test_strict_reason_codes_allows_mapped_legacy(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("STRICT_REASON_CODES", "1")

    import handlers.confirmations.engine as engine_mod
    importlib.reload(engine_mod)

    Validation = engine_mod.Validation

    # If legacy_reason_to_code knows this string, it should not raise.
    v = Validation(True, 0.0, [], "bo_l2_missing", {}, reason_code="NOT_A_CODE")
    assert v.veto is True
    assert v.reason_u16 != 0
