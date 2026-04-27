import pytest


def test_finalize_reason_sets_structured_and_u16():
    # тестируем "ультра-слой": engine умеет получить reason_u16 по reason_code/legacy
    from handlers.confirmations.engine import _finalize_reason
    code, u16 = _finalize_reason("near_big_wall", "")
    assert code == "VETO_WALL_NEAR"
    assert isinstance(u16, int)
    assert 0 < u16 <= 65535


def test_finalize_reason_strict_unknown_raises(monkeypatch):
    monkeypatch.setenv("STRICT_REASON_CODES", "1")
    from handlers.confirmations.engine import _finalize_reason
    # reason_code задан явно и неизвестен -> должен упасть (fail-closed)
    with pytest.raises(ValueError):
        _finalize_reason("", "SOME_UNKNOWN_REASON_CODE")
