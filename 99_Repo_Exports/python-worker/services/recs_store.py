from __future__ import annotations

import json

from core.recs_contract import RecBundle

# Redis ключи для хранения bundle, статуса и аудита
BUNDLE_KEY = "recs:bundle:"
STATUS_KEY = "recs:status:"
AUDIT_KEY = "recs:audit:"   # JSON list of {key,field,old,new}


def store_bundle(r, bundle: RecBundle) -> None:
    """
    Сохраняет bundle в Redis с TTL.
    
    Сохраняет:
    - recs:bundle:<id> = JSON bundle
    - recs:status:<id> = "PENDING"
    
    Args:
        r: Redis клиент (redis.Redis)
        bundle: RecBundle для сохранения
    """
    k = BUNDLE_KEY + bundle.id
    r.set(k, bundle.to_json(), ex=bundle.ttl_sec)
    r.set(STATUS_KEY + bundle.id, "PENDING", ex=bundle.ttl_sec)


def get_bundle(r, bundle_id: str) -> RecBundle | None:
    """
    Читает bundle из Redis.
    
    Args:
        r: Redis клиент
        bundle_id: Идентификатор bundle
        
    Returns:
        RecBundle если найден, None иначе
    """
    raw = r.get(BUNDLE_KEY + bundle_id)
    if not raw:
        return None
    if isinstance(raw, (bytes, bytearray)):
        raw = raw.decode("utf-8", errors="replace")
    d = json.loads(raw)
    return RecBundle.from_dict(d)


def get_status(r, bundle_id: str) -> str:
    """
    Читает статус bundle из Redis.
    
    Возможные статусы:
    - PENDING: Ожидает одобрения (создано cron)
    - PREVIEWED: Показан diff (после preview)
    - APPLIED: Применен
    - REJECTED: Отклонен
    - ROLLED_BACK: Откачен
    - MISSING: Bundle не найден
    
    Args:
        r: Redis клиент
        bundle_id: Идентификатор bundle
        
    Returns:
        Строка статуса
    """
    raw = r.get(STATUS_KEY + bundle_id)
    if not raw:
        return "MISSING"
    if isinstance(raw, (bytes, bytearray)):
        raw = raw.decode("utf-8", errors="replace")
    return str(raw)


def set_status(r, bundle_id: str, status: str, ttl_sec: int) -> None:
    """
    Устанавливает статус bundle в Redis.
    
    Args:
        r: Redis клиент
        bundle_id: Идентификатор bundle
        status: Новый статус (APPLIED, REJECTED, ROLLED_BACK)
        ttl_sec: TTL в секундах (обычно берется из bundle.ttl_sec)
    """
    r.set(STATUS_KEY + bundle_id, status, ex=ttl_sec)


def append_audit(r, bundle_id: str, entry: dict, ttl_sec: int) -> None:
    """
    Добавляет запись в audit log для bundle.
    
    Audit log хранится как Redis List (append-only) с JSON строками.
    Каждая запись содержит: {ts_ms, key, field, old, new}
    
    Используется для rollback: читаем все записи и восстанавливаем старые значения.
    
    Args:
        r: Redis клиент
        bundle_id: Идентификатор bundle
        entry: Словарь с полями {ts_ms, key, field, old, new}
        ttl_sec: TTL для audit ключа (обычно = bundle.ttl_sec)
    """
    # append-only list stored as JSON lines in a redis list
    r.rpush(AUDIT_KEY + bundle_id, json.dumps(entry, ensure_ascii=False, separators=(",", ":")))
    r.expire(AUDIT_KEY + bundle_id, ttl_sec)

