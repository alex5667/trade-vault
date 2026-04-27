# error_handler.py
"""
Функционал обработки ошибок и DLQ, извлеченный из base_orderflow_handler.py
"""

from __future__ import annotations
from utils.time_utils import get_ny_time_millis

from typing import Optional, Dict, Any, Tuple
import time
from common.transient_errors import is_transient_error
import json
import traceback
import os

try:
    import redis
    from redis.exceptions import ConnectionError as RedisConnError, TimeoutError as RedisTimeoutError, ResponseError as RedisResponseError
except Exception:  # pragma: no cover
    redis = None
    RedisConnError = RedisTimeoutError = RedisResponseError = Exception  # type: ignore

from common.transient import is_transient_error

def setup_logger(name):
    import logging
    return logging.getLogger(name)

class ErrorHandler:
    """
    Обрабатывает классификацию ошибок, операции DLQ и логику backoff.
    """

    def __init__(self, symbol: str, max_fail_retries: int = 3):
        self.symbol = symbol
        self.max_fail_retries = int(max_fail_retries)
        self.logger = setup_logger(f"ErrorHandler:{symbol}")

    def is_transient_error(self, e: Exception) -> bool:
        """Публичный метод классификации transient ошибок (один источник истины)."""
        return self._is_transient_error(e)

    def is_transient_error(self, e: Exception) -> bool:
        """Единая логика transient (общий модуль)."""
        return bool(is_transient_error(e))

    def is_transient_error(self, e: Exception) -> bool:
        """
        (E) Единый источник истины transient-классификации.
        Комбинируем:
          - типовые исключения Redis/сети
          - errno-сценарии
          - строковые токены (fallback)
        """
        # 1) типовые исключения
        try:
            import redis
            from redis.exceptions import ConnectionError as RedisConnErr
            from redis.exceptions import TimeoutError as RedisTimeoutErr
            from redis.exceptions import BusyLoadingError as RedisBusyLoadingErr
        except Exception:
            redis = None  # type: ignore
            RedisConnErr = ()  # type: ignore
            RedisTimeoutErr = ()  # type: ignore
            RedisBusyLoadingErr = ()  # type: ignore

        if isinstance(e, (OSError, TimeoutError)):
            return True
        if RedisConnErr and isinstance(e, RedisConnErr):
            return True
        if RedisTimeoutErr and isinstance(e, RedisTimeoutErr):
            return True
        if RedisBusyLoadingErr and isinstance(e, RedisBusyLoadingErr):
            return True

        # 2) errno / socket patterns
        err_no = getattr(e, "errno", None)
        if err_no in (104, 110, 111, 32, 54):  # ECONNRESET, ETIMEDOUT, ECONNREFUSED, EPIPE, ECONNRESET(mac)
            return True

        # 3) строковые токены (последний рубеж)
        tokens = (
            "timeout", "timed out",
            "connection", "connection error", "connection refused",
            "broken pipe", "eof",
            "try again", "temporarily",
            "reset by peer",
            "busy loading", "loading the dataset",
            "read only", "readonly",
            "i/o error",
        )
        msg = (str(e) or "").lower()
        return any(t in msg for t in tokens)

    # backward compat
    def _is_transient_error(self, e: Exception) -> bool:
        return self.is_transient_error(e)

    def _sanitize_dlq_payload(self, payload: Dict[str, Any]) -> Dict[str, str]:
        """Очистка payload для сохранения в DLQ (все значения должны быть строками)."""
        out: Dict[str, str] = {}
        for k, v in payload.items():
            try:
                if isinstance(v, str):
                    out[str(k)] = v
                else:
                    out[str(k)] = json.dumps(v, ensure_ascii=False, default=str)
            except Exception:
                out[str(k)] = str(v)
        return out

    def _dlq_write(self, consumer: object, dlq_stream: str, payload: Dict[str, str]) -> None:
        """Низкоуровневая запись в DLQ с фоллбеками."""
        # 1) Предпочтительный API хелпера
        fn = getattr(consumer, "add_dlq", None)
        if callable(fn):
            fn(dlq_stream, payload)  # type: ignore
            return

        # 2) Fallback: напрямую через Redis клиент
        client = getattr(consumer, "client", None)
        if client is not None and hasattr(client, "xadd"):
            client.xadd(dlq_stream, payload, maxlen=200000)
            return

        raise RuntimeError("DLQ writer unavailable: consumer has no add_dlq() and no client.xadd(, maxlen=200000)")

    def _try_add_dlq_or_backoff(
        self,
        consumer: object,
        dlq_payload: Dict[str, Any],
        *,
        backoff: object,
        where: str,
        stop_event: Any = None,
    ) -> bool:
        """
        Попытка добавить сообщение в DLQ. 
        Различает временные ошибки записи (backoff) и перманентные (stuck prevention).
        """
        dlq_stream = os.getenv("ORDERFLOW_DLQ_STREAM", "stream:dlq:orderflow")
        payload = self._sanitize_dlq_payload(dlq_payload)

        try:
            self._dlq_write(consumer, dlq_stream, payload)
            self.logger.warning("Added poison message to DLQ: %s", dlq_payload.get("msg_id", "unknown"))
            return True

        except Exception as dlq_e:
            # Если ошибка transient (сеть/redis) -> backoff и повторим позже (не выбрасываем сообщение)
            if self._is_transient_error(dlq_e):
                delay = float(getattr(backoff, "next_sleep", getattr(backoff, "get_delay", lambda: 1.0))())
                self.logger.warning(
                    "Transient DLQ write error in %s: %s (backoff=%.2fs)",
                    where, dlq_e, delay
                )
                if stop_event is not None and hasattr(stop_event, "wait"):
                    stop_event.wait(timeout=delay)
                else:
                    time.sleep(delay)
                return False

            # Перманентная проблема (нет метода/битый конфиг) -> логируем payload и разрешаем ACK,
            # иначе воркер застрянет навсегда на этом сообщении.
            self.logger.error(
                "Permanent DLQ write failure in %s: %s. Will ACK poison to avoid stuck. msg_id=%s",
                where, dlq_e, dlq_payload.get("msg_id", "unknown")
            )
            try:
                # Дампим payload в логи в урезанном виде для ручного восстановления
                self.logger.error("DLQ payload preview (trimmed): %s", str(payload)[:3000])
            except Exception:
                pass
            return True  # Считаем "обработано" -> MessageHandler сделает ACK

    def handle_message_error(
        self,
        e: Exception,
        consumer: object,
        backoff: object,
        fail_counts: Dict[Tuple[str, str], int],
        stream: str,
        msg_id: str,
        fields: Optional[Dict[str, Any]] = None,
        where: str = "unknown",
        stop_event: Any = None,
    ) -> bool:
        """
        Обработка ошибки при процессинге сообщения.
        Возвращает True, если нужно повторить сообщение, False, если обработано (ACKed или DLQed).
        """
        # Transient -> backoff и ретрай (не считаем как poison)
        if self._is_transient_error(e):
            delay = float(getattr(backoff, "next_sleep", getattr(backoff, "get_delay", lambda: 1.0))())
            self.logger.warning("Transient error in %s (will retry): %s (backoff=%.2fs)", where, e, delay)
            if stop_event is not None and hasattr(stop_event, "wait"):
                stop_event.wait(timeout=delay)
            else:
                time.sleep(delay)
            return True

        # Poison message
        key = (stream, msg_id)
        fail_counts[key] = fail_counts.get(key, 0) + 1

        if fail_counts[key] < self.max_fail_retries:
            return True

        # DLQ payload
        dlq_payload = {
            "ts": get_ny_time_millis(),
            "symbol": self.symbol,
            "handler": "ErrorHandler",
            "stream": stream,
            "msg_id": msg_id,
            "fields": {k: str(v) for k, v in (fields or {}).items()},
            "error": str(e),
            "error_type": type(e).__name__,
            "trace": traceback.format_exc()[-4000:],  # трим для redis payload
            "fail_count": fail_counts[key],
            "location": where,
        }

        success = self._try_add_dlq_or_backoff(
            consumer, dlq_payload, backoff=backoff, where=where, stop_event=stop_event
        )
        if success:
            fail_counts.pop(key, None)
            return False  # “обработано” (можно ACK в MessageHandler)
        return True  # DLQ transient error -> ретрай позже

    def cleanup_fail_counts(self, fail_counts: Dict[Tuple[str, str], int]) -> None:
        """Очистка словаря fail_counts для предотвращения неограниченного роста."""
        if len(fail_counts) > 20000:
            # Trim oldest 5000
            keys = list(fail_counts.keys())[:5000]
            for k in keys:
                fail_counts.pop(k, None)
            self.logger.warning("Trimmed fail_counts dict (size limit exceeded)")

    def log_processing_stats(
        self,
        processed: int,
        errors: int,
        dlq_count: int,
        duration_ms: float,
    ) -> None:
        """Логирование статистики обработки."""
        if processed > 0 or errors > 0:
            error_rate = (errors / max(processed + errors, 1)) * 100
            self.logger.info(
                "Processing stats: processed=%d, errors=%d, dlq=%d, error_rate=%.1f%%, duration=%.0fms",
                processed, errors, dlq_count, error_rate, duration_ms
            )
