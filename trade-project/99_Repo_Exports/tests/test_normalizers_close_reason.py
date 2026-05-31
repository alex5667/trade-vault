# tests/test_normalizers_close_reason.py
import pytest

# Импорты из проекта
try:
    from domain.normalizers import norm_close_reason, bucket_close_reason
except ImportError:
    # Fallback для случая, когда тесты запускаются из другой директории
    import sys
    import os
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'python-worker'))
    from domain.normalizers import norm_close_reason, bucket_close_reason


def test_norm_close_reason_orphan_timeout():
    """Тест: ORPHAN_TIMEOUT нормализуется корректно."""
    assert norm_close_reason("ORPHAN_TIMEOUT") == "ORPHAN_TIMEOUT"
    assert norm_close_reason("orphan timeout") == "ORPHAN_TIMEOUT"
    assert norm_close_reason("ORPHAN_TIMEOUT_NO_PRICE") == "ORPHAN_TIMEOUT"


def test_bucket_close_reason_orphan_timeout():
    """Тест: ORPHAN_TIMEOUT попадает в bucket EXPIRED."""
    assert bucket_close_reason("ORPHAN_TIMEOUT") == "EXPIRED"
    assert bucket_close_reason("ORPHAN_TIMEOUT_NO_PRICE") == "EXPIRED"
    assert bucket_close_reason("ORPHAN_TIMEOUT_STALE_PRICE") == "EXPIRED"


def test_bucket_close_reason_expired_no_target_passthrough():
    """Тест: EXPIRED_NO_TARGET/EXPIRED_NO_ENTRY сохраняются как есть."""
    # если такой raw реально приходит — bucket оставляем читаемым
    assert bucket_close_reason("EXPIRED_NO_TARGET") == "EXPIRED_NO_TARGET"
    assert bucket_close_reason("expired no entry") == "EXPIRED_NO_ENTRY"


def test_bucket_close_reason_tp_levels_are_preserved():
    """Тест: уровни TP сохраняются."""
    assert bucket_close_reason("TP1") == "TP1"
    assert bucket_close_reason("tp2") == "TP2"
    assert bucket_close_reason("TP3") == "TP3"


def test_bucket_close_reason_sl_variants():
    """Тест: различные варианты SL нормализуются в SL."""
    assert bucket_close_reason("SL") == "SL"
    assert bucket_close_reason("STOP_LOSS") == "SL"
    assert bucket_close_reason("SL_AFTER_TP1") == "SL"
    assert bucket_close_reason("SL_AFTER_TP2") == "SL"


def test_bucket_close_reason_trailing_stop():
    """Тест: TRAILING_STOP нормализуется корректно."""
    assert bucket_close_reason("TRAILING_STOP") == "TRAILING_STOP"
    assert bucket_close_reason("TRAILING") == "TRAILING_STOP"
    assert bucket_close_reason("TRAILING_PROFIT") == "TRAILING_STOP"


def test_norm_close_reason_preserves_tp_levels():
    """Тест: norm_close_reason сохраняет уровни TP."""
    assert norm_close_reason("TP1") == "TP1"
    assert norm_close_reason("TP2") == "TP2"
    assert norm_close_reason("TP3") == "TP3"


def test_norm_close_reason_general_tp():
    """Тест: общий TP нормализуется в TP."""
    assert norm_close_reason("TAKE_PROFIT") == "TP"
    assert norm_close_reason("MANUAL_TP") == "TP"


def test_norm_close_reason_expired_variants():
    """Тест: EXPIRED_NO_ENTRY и EXPIRED_NO_TARGET сохраняются."""
    assert norm_close_reason("EXPIRED_NO_ENTRY") == "EXPIRED_NO_ENTRY"
    assert norm_close_reason("EXPIRED_NO_TARGET") == "EXPIRED_NO_TARGET"


def test_bucket_close_reason_empty_input():
    """Тест: пустой input возвращает пустую строку."""
    assert bucket_close_reason("") == ""
    assert bucket_close_reason(None) == ""


def test_norm_close_reason_empty_input():
    """Тест: пустой input возвращает пустую строку."""
    assert norm_close_reason("") == ""
    assert norm_close_reason(None) == ""


def test_bucket_close_reason_case_insensitive():
    """Тест: функция case-insensitive."""
    assert bucket_close_reason("orphan_timeout") == "EXPIRED"
    assert bucket_close_reason("Orphan_Timeout") == "EXPIRED"
    assert bucket_close_reason("ORPHAN_TIMEOUT") == "EXPIRED"


def test_norm_close_reason_orphan_with_spaces():
    """Тест: пробелы в названии конвертируются в подчеркивания."""
    assert norm_close_reason("orphan timeout") == "ORPHAN_TIMEOUT"
    assert norm_close_reason("ORPHAN TIMEOUT NO PRICE") == "ORPHAN_TIMEOUT"


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])

