from __future__ import annotations
"""
Signal Publisher

Высокоуровневый интерфейс. Работает через SignalOutboxPublisher.publish()
и возвращает PublishResult (чтобы upstream корректно делал ACK/метрики).
"""

from utils.time_utils import get_ny_time_millis

from typing import Dict, Any, Optional
import time
import logging

from .outbox_utils import PublishResult, ensure_ts_ms, normalize_to_bucket

log = logging.getLogger(__name__)


class SignalPublisher:
    """
    Высокоуровневый интерфейс. Работает через SignalOutboxPublisher.publish()
    и возвращает PublishResult (чтобы upstream корректно делал ACK/метрики).
    """

    def __init__(
        self,
        *,
        outbox: Any,                      # SignalOutboxPublisher
        source: str = "orderflow",
        strategy: str = "orderflow",
        dedup_bucket_ms: int = 60000,     # бакет дедупа (1 мин по умолчанию)
        dedup_ttl_ms: Optional[int] = None,
    ):
        self.outbox = outbox
        self.source = source
        self.strategy = strategy
        self.dedup_bucket_ms = int(dedup_bucket_ms)
        self.dedup_ttl_ms = int(dedup_ttl_ms) if dedup_ttl_ms is not None else None

    def publish(self, signal: Dict[str, Any]) -> PublishResult:
        """Публикует сигнал через outbox с дедупликацией."""
        symbol = str(signal.get("symbol", "unknown"))

        # side: LONG/SHORT в upper case
        side = signal.get("side")
        if not side:
            direction = int(signal.get("direction", 0) or 0)
            side = "LONG" if direction > 0 else "SHORT" if direction < 0 else "UNKNOWN"

        # kind: тип сигнала
        kind = str(signal.get("kind") or signal.get("signal_type") or "ENTRY")

        # level_key: избегаем пустой строки для корректного дедупа
        level_key = str(signal.get("level_key") or signal.get("context", {}).get("level_key") or "")
        if level_key == "":
            # если уровня нет, используем цену как fallback
            price = signal.get("price") or signal.get("context", {}).get("price", 0)
            level_key = f"px:{round(float(price), 2)}"

        # ts_ms: нормализуем к бакету дедупа
        ts_ms = signal.get("ts_ms")
        if ts_ms is None:
            ts_ms = signal.get("ts")
        if ts_ms is None:
            ts_ms = get_ny_time_millis()
        ts_ms = int(ts_ms)
        ts_norm = normalize_to_bucket(ts_ms, self.dedup_bucket_ms)

        # Confidence shouldn't leak to the payload wire format
        envelope = dict(signal)
        envelope.pop("confidence", None)

        try:
            msg_id = self.outbox.publish(
                source=self.source,
                strategy=self.strategy,
                symbol=symbol,
                side=str(side).upper(),
                kind=kind,
                level_key=level_key,
                ts_ms=ts_norm,
                envelope=envelope,
                dedup_ttl_ms=self.dedup_ttl_ms,
            )
            if msg_id is None:
                # Dedup hit: signal was NOT sent to Redis
                return PublishResult(sent=False, dedup=True, msg_id=None)
            return PublishResult(sent=True, dedup=False, msg_id=str(msg_id))
        except Exception as e:
            log.exception("Outbox publish failed: %s", e)
            return PublishResult(sent=False, dedup=False, msg_id=None)

    # backward-compat: старый интерфейс send() можно оставить
    def send(self, signal: Dict[str, Any]) -> None:
        """Устаревший интерфейс для совместимости."""
        _ = self.publish(signal)
