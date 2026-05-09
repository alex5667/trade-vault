from __future__ import annotations

import hashlib
import hmac
import json
from dataclasses import dataclass
from typing import Any

from utils.time_utils import get_ny_time_millis


def _now_ms() -> int:
    """Возвращает текущее время в миллисекундах (epoch)."""
    return get_ny_time_millis()


def sign_bundle_id(bundle_id: str, secret: str) -> str:
    """
    Генерирует короткую HMAC подпись для bundle_id (8 hex символов).
    
    Это необходимо, чтобы уложиться в ограничение Telegram callback_data (64 байта).
    
    Args:
        bundle_id: Идентификатор bundle (12 hex символов)
        secret: Секретный ключ для подписи (из env RECS_HMAC_SECRET)
        
    Returns:
        8 hex символов подписи
    """
    d = hmac.new(secret.encode("utf-8"), bundle_id.encode("utf-8"), hashlib.sha256).hexdigest()
    return d[:8]


def verify_sig(bundle_id: str, sig: str, secret: str) -> bool:
    """
    Проверяет подпись bundle_id.
    
    Использует hmac.compare_digest для защиты от timing attacks.
    
    Args:
        bundle_id: Идентификатор bundle
        sig: Подпись для проверки (8 hex символов)
        secret: Секретный ключ
        
    Returns:
        True если подпись валидна, False иначе
    """
    return hmac.compare_digest(sign_bundle_id(bundle_id, secret), (sig or ""))


@dataclass
class RecOp:
    """
    Операция для применения рекомендации.
    
    Сейчас поддерживается только HSET для Redis Hash (config:orderflow:<SYMBOL>).
    В будущем можно расширить для других операций (SET, DEL, и т.д.).
    """
    op: str                 # "HSET"
    key: str                # e.g. "config:orderflow:BTCUSDT"
    field: str              # e.g. "w_exec_risk"
    value: str              # "0.200"


@dataclass
class RecBundle:
    """
    Bundle (набор операций) для применения рекомендаций.
    
    Bundle создается cron_of_reports.py при генерации рекомендаций,
    сохраняется в Redis с TTL, и может быть одобрен/отклонен через Telegram.
    """
    id: str                 # 12 hex символов (secrets.token_hex(6))
    created_ms: int         # Timestamp создания (epoch ms)
    ttl_sec: int            # TTL в секундах (обычно 86400 = 24 часа)
    who: str                # Источник создания (e.g. "cron_of_reports")
    ops: list[RecOp]        # Список операций для применения
    meta: dict[str, Any]    # Метаданные (mode, ts, и т.д.)

    def to_json(self) -> str:
        """
        Сериализует bundle в JSON строку для хранения в Redis.
        
        Использует compact формат (без пробелов) для экономии памяти.
        """
        return json.dumps({
            "id": self.id,
            "created_ms": self.created_ms,
            "ttl_sec": self.ttl_sec,
            "who": self.who,
            "ops": [op.__dict__ for op in self.ops],
            "meta": self.meta,
        }, ensure_ascii=False, separators=(",", ":"))

    @staticmethod
    def from_dict(d: dict[str, Any]) -> RecBundle:
        """
        Десериализует bundle из словаря (после чтения из Redis).
        
        Args:
            d: Словарь с полями bundle
            
        Returns:
            RecBundle объект
        """
        ops = [RecOp(**x) for x in (d.get("ops") or [])]
        return RecBundle(
            id=(d.get("id", "")),
            created_ms=int(d.get("created_ms", 0) or 0),
            ttl_sec=int(d.get("ttl_sec", 0) or 0),
            who=(d.get("who", "")),
            ops=ops,
            meta=dict(d.get("meta") or {}),
        )

