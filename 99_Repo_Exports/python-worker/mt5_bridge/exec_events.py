from __future__ import annotations
"""
Execution Events - MT5 сделки в Redis Streams

Классы для представления событий исполнения сделок и публикации их в Redis streams.
Обеспечивает связь между реальными сделками MT5 и остальной системой scanner_infra.
"""


from dataclasses import dataclass, asdict
from datetime import datetime
from typing import Optional, Dict, Any

import json
import redis


@dataclass
class ExecutionEvent:
    """
    Унифицированное событие о фактическом исполнении сделки на MT5.

    Виды событий (kind):
      - "fill"    — фактическая сделка (open/close/partial и т.п.)
      - "command" — команда (например, CLOSE_REQUEST) — для этого моста это опционально.

    Типы событий (event_type):
      - "OPEN", "CLOSE", "PARTIAL_CLOSE", "SL", "TP", "DEAL", "CLOSE_REQUEST", ...
    """

    signal_id: str
    symbol: str
    side: str                 # "long" / "short" / "unknown"
    venue: str                # "mt5"
    kind: str                 # "fill" | "command"
    event_type: str           # тип события (OPEN, CLOSE, DEAL и т.д.)

    ts_event: datetime
    price: float
    qty_lots: float           # объём в лотах

    pnl_ccy: float = 0.0      # реализованный PnL по сделке в валюте счёта
    account_ccy: str = "USD"  # валюта счёта MT5

    mt5_deal: Optional[int] = None
    mt5_order: Optional[int] = None
    mt5_position_id: Optional[int] = None

    comment: Optional[str] = None
    meta: Optional[Dict[str, Any]] = None

    def _to_payload(self) -> Dict[str, Any]:
        """
        Payload, который пойдёт внутри JSON.
        Конвертируем datetime в ISO строку.
        """
        d = asdict(self)
        d["ts_event"] = self.ts_event.isoformat()
        # meta может быть None - оставляем как есть
        return d

    def to_redis_fields(self) -> Dict[str, str]:
        """
        Структура записи в stream:signals:exec_events.
        Делаем плоские поля + payload JSON (как для plans).
        """
        payload = self._to_payload()
        return {
            "signal_id": self.signal_id,
            "venue": self.venue,
            "kind": self.kind,
            "event_type": self.event_type,
            "payload": json.dumps(payload, separators=(",", ":")),
        }


class ExecEventsPublisher:
    """
    Простая обёртка для XADD в stream:signals:exec_events.

    Публикует ExecutionEvent в Redis stream для потребления
    SignalPerformanceTracker и другими компонентами системы.
    """

    def __init__(self, redis_dsn: str, stream_key: str = "stream:signals:exec_events"):
        self._r = redis.from_url(redis_dsn, decode_responses=True)
        self.stream_key = stream_key

    def publish(self, event: ExecutionEvent) -> str:
        """
        Публикует событие в Redis stream.

        Args:
            event: ExecutionEvent для публикации

        Returns:
            str: ID сообщения в stream
        """
        fields = event.to_redis_fields()
        msg_id = self._r.xadd(self.stream_key, fields, maxlen=50000)
        return msg_id
