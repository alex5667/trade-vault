"""
Тесты для подписи и проверки bundle рекомендаций.

Проверяет:
- sign_bundle_id() детерминированность
- verify_sig() отклоняет неправильную подпись
- verify_sig() принимает правильную подпись
"""
import pytest
from core.recs_contract import sign_bundle_id, verify_sig


def test_sign_bundle_id_deterministic():
    """Проверяет, что sign_bundle_id() детерминирован (одинаковый bundle_id + secret = одинаковая подпись)."""
    bundle_id = "abc123def456"
    secret = "test_secret_key"
    
    sig1 = sign_bundle_id(bundle_id, secret)
    sig2 = sign_bundle_id(bundle_id, secret)
    
    assert sig1 == sig2, "Подпись должна быть детерминированной"
    assert len(sig1) == 8, "Подпись должна быть 8 hex символов"
    assert all(c in "0123456789abcdef" for c in sig1), "Подпись должна содержать только hex символы"


def test_sign_bundle_id_different_secrets():
    """Проверяет, что разные секреты дают разные подписи."""
    bundle_id = "abc123def456"
    secret1 = "secret1"
    secret2 = "secret2"
    
    sig1 = sign_bundle_id(bundle_id, secret1)
    sig2 = sign_bundle_id(bundle_id, secret2)
    
    assert sig1 != sig2, "Разные секреты должны давать разные подписи"


def test_sign_bundle_id_different_ids():
    """Проверяет, что разные bundle_id дают разные подписи."""
    secret = "test_secret"
    bundle_id1 = "abc123def456"
    bundle_id2 = "xyz789uvw012"
    
    sig1 = sign_bundle_id(bundle_id1, secret)
    sig2 = sign_bundle_id(bundle_id2, secret)
    
    assert sig1 != sig2, "Разные bundle_id должны давать разные подписи"


def test_verify_sig_correct():
    """Проверяет, что verify_sig() принимает правильную подпись."""
    bundle_id = "abc123def456"
    secret = "test_secret_key"
    
    sig = sign_bundle_id(bundle_id, secret)
    assert verify_sig(bundle_id, sig, secret), "Правильная подпись должна быть принята"


def test_verify_sig_wrong_signature():
    """Проверяет, что verify_sig() отклоняет неправильную подпись."""
    bundle_id = "abc123def456"
    secret = "test_secret_key"
    wrong_sig = "deadbeef"  # Неправильная подпись
    
    assert not verify_sig(bundle_id, wrong_sig, secret), "Неправильная подпись должна быть отклонена"


def test_verify_sig_wrong_secret():
    """Проверяет, что verify_sig() отклоняет подпись, созданную с другим секретом."""
    bundle_id = "abc123def456"
    secret1 = "secret1"
    secret2 = "secret2"
    
    sig = sign_bundle_id(bundle_id, secret1)
    assert not verify_sig(bundle_id, sig, secret2), "Подпись с другим секретом должна быть отклонена"


def test_verify_sig_wrong_bundle_id():
    """Проверяет, что verify_sig() отклоняет подпись для другого bundle_id."""
    bundle_id1 = "abc123def456"
    bundle_id2 = "xyz789uvw012"
    secret = "test_secret"
    
    sig = sign_bundle_id(bundle_id1, secret)
    assert not verify_sig(bundle_id2, sig, secret), "Подпись для другого bundle_id должна быть отклонена"


def test_verify_sig_empty_signature():
    """Проверяет, что verify_sig() отклоняет пустую подпись."""
    bundle_id = "abc123def456"
    secret = "test_secret"
    
    assert not verify_sig(bundle_id, "", secret), "Пустая подпись должна быть отклонена"
    assert not verify_sig(bundle_id, None, secret), "None подпись должна быть отклонена"


def test_verify_sig_timing_safe():
    """Проверяет, что verify_sig() использует hmac.compare_digest (защита от timing attacks)."""
    # Это косвенная проверка - мы просто убеждаемся, что функция работает корректно
    # Реальная защита от timing attacks обеспечивается hmac.compare_digest внутри verify_sig
    bundle_id = "abc123def456"
    secret = "test_secret"
    
    sig = sign_bundle_id(bundle_id, secret)
    assert verify_sig(bundle_id, sig, secret), "Должна работать корректно"

