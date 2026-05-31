# main_loop_service.py
from __future__ import annotations

"""
Функционал основного цикла обработки, извлеченный из base_orderflow_handler.py
"""

import logging
import random
import time
from dataclasses import dataclass
from typing import Any

from services.orderflow.exec_health_slo_contract import flush_exec_health_contract_state_sync
from utils.time_utils import get_ny_time_millis

try:
    from health_metrics import HealthMetrics
except Exception:  # pragma: no cover
    HealthMetrics = Any  # type: ignore

def setup_logger(name: str):
    return logging.getLogger(name)

# Backoff может жить в другом модуле в вашем проекте.
# Чтобы этот сервис был самодостаточным, делаем безопасный fallback.
try:  # pragma: no cover
    from common.backoff import Backoff  # type: ignore
except Exception:  # pragma: no cover
    @dataclass
    class Backoff:
        base_delay: float = 0.25
        multiplier: float = 2.0
        max_delay: float = 5.0
        jitter: bool = True
        _cur: float = 0.0

        def reset(self) -> None:
            self._cur = 0.0

        def get_delay(self) -> float:
            if self._cur <= 0.0:
                self._cur = float(self.base_delay)
            else:
                self._cur = min(float(self.max_delay), float(self._cur) * float(self.multiplier))
            if self.jitter:
                # простая эвристика jitter
                return self._cur * (0.5 + random.random())
            return self._cur


class MainLoopService:
    """
    Сервис для управления основным циклом обработки Redis stream.
    """

    def __init__(
        self,
        *args: Any,
        symbol: str | None = None,
        health_metrics: HealthMetrics | None = None,  # type: ignore
        **kwargs: Any,
    ) -> None:
        super().__init__()  # in case of mixins
        self.symbol = symbol or getattr(self, "symbol", None)  # сохраняем обратную совместимость
        self.health_metrics = health_metrics
        self.config = kwargs.get("config", getattr(self, "config", None))
        self.logger = setup_logger(f"MainLoopService:{self.symbol or 'unknown'}")

        # Трекинг статистики (всегда инициализировать)
        self.tick_cnt = 0
        self.book_cnt = 0
        self.total_tick_cnt = 0
        self.last_stat_mono_s = time.monotonic()

        # Таймер семплирования pending
        self._pending_sample_last_ms = 0  # обратная совместимость
        self._pending_sample_last_mono_ms = 0
        self._last_pending_metrics_mono_ms = 0

        # Инициализация атрибутов стрима из kwargs (новый способ)
        self.tick_stream = kwargs.get("tick_stream")
        self.book_stream = kwargs.get("book_stream")
        self.l3_stream = kwargs.get("l3_stream")
        self.message_handler = kwargs.get("message_handler")
        self.error_handler = kwargs.get("error_handler")

        # Обратная совместимость для старой сигнатуры
        if args:
            self._init_old(*args, **kwargs)

    def _init_old(
        self,
        symbol: str,
        tick_stream: str,
        book_stream: str,
        l3_stream: str,
        message_handler: Any,
        error_handler: Any,
        config: Any,
        health_metrics: Any = None,
    ):
        self.symbol = symbol or self.symbol
        self.tick_stream = tick_stream
        self.book_stream = book_stream
        self.l3_stream = l3_stream
        self.message_handler = message_handler
        self.error_handler = error_handler
        self.config = config
        self.health_metrics = health_metrics or self.health_metrics
        self.logger = setup_logger(f"MainLoopService:{self.symbol}")

        # Stats tracking
        self.tick_cnt = 0
        self.book_cnt = 0
        self.total_tick_cnt = 0
        self.last_stat_mono_s = time.monotonic()

        # Pending sampling timer
        self._pending_sample_last_ms = 0  # backward compat
        self._pending_sample_last_mono_ms = 0
        self._last_pending_metrics_mono_ms = 0

    def _xpending_len_safe(self, consumer: object, stream: str) -> int:
        """
        Возврат summary 'pending' из XPENDING для стрима/группы.
        Использует consumer.pending_len() если доступно.
        """
        try:
            fn = getattr(consumer, "pending_len", None)
            if callable(fn):
                return int(fn(stream) or 0)  # type: ignore
        except Exception:
            return 0
        return 0

    def _hm_set_pending_len(self, symbol: str, kind: str, pending: int, ts_ms: int, stream: str | None = None) -> None:
        """
        Унификация разных HealthMetrics API:
          - on_pending_len(symbol, kind, pending)
          - set_pending_len(symbol, kind, pending, ts_ms=...)
          - gauge("pending_len", pending, tags={...})
        """
        hm = self.health_metrics
        if hm is None:
            return
        try:
            if hasattr(hm, "on_pending_len"):
                hm.on_pending_len(symbol, kind, int(pending))
                return
        except Exception:
            pass
        try:
            if hasattr(hm, "set_pending_len"):
                hm.set_pending_len(symbol, kind, int(pending), ts_ms=int(ts_ms))
                return
        except Exception:
            pass
        try:
            if hasattr(hm, "gauge"):
                hm.gauge("pending_len", int(pending), tags={
                    "symbol": symbol,
                    "kind": kind,
                    "stream": stream or str(kind)
                })
                return
        except Exception:
            pass

    def _gauge(self, name: str, value: int, **tags) -> None:
        """Отправка метрики gauge с фоллбеком на debug логирование."""
        # Сначала пробуем health_metrics/metrics
        hm = getattr(self, "health_metrics", None) or getattr(self, "metrics", None)
        if hm is not None:
            if hasattr(hm, "gauge"):
                hm.gauge(name, value, tags=tags)
                return
            if hasattr(hm, "set_gauge"):
                hm.set_gauge(name, value, tags=tags)
                return

        # Фоллбек на debug логирование
        self.logger.debug("%s=%s tags=%s", name, value, tags)

    def run(self, consumer: Any, stop_event: Any) -> None:
        """Публичная точка входа, используется хендлером; гарантирует корректный стоп."""
        try:
            self._run_loop(consumer, stop_event)
        except Exception:
            self.logger.exception("Main loop crashed for %s", self.symbol)
            raise
        finally:
            self.logger.info("Main loop stopped for %s", self.symbol)

    def _emit_pending_metrics(self, consumer: Any, mono_now_ms: int, wall_now_ms: int) -> None:
        """Периодическая отправка метрик длины pending для каждого стрима."""
        interval_ms = int(getattr(self.config, "pending_metrics_interval_ms", 5000)) if self.config else 5000
        last = int(getattr(self, "_last_pending_metrics_mono_ms", 0) or 0)
        if mono_now_ms - last < interval_ms:
            return
        self._last_pending_metrics_mono_ms = mono_now_ms

        if not self.symbol:
            return

        for stream, kind in [
            (self.book_stream, "book"),
            (self.l3_stream, "l3"),
            (self.tick_stream, "ticks"),
        ]:
            if not stream:
                continue
            pending = self._xpending_len_safe(consumer, stream)
            self._hm_set_pending_len(self.symbol, kind, pending, wall_now_ms, stream=stream)

        # P4 SLO contract: periodic flush to prevent staleness alerts when no signals are emitted
        try:
            client = getattr(consumer, "client", None)
            if client:
                for scope in ("edge", "pipeline", "entry_policy"):
                    flush_exec_health_contract_state_sync(redis_client=client, scope=scope)
        except Exception:
            pass

    def _is_transient(self, e: Exception) -> bool:
        """
        Унифицированная проверка на transient error.
        Приоритет: ErrorHandler.is_transient_error() -> MessageHandler._is_transient_error()
        """
        # 1. Пробуем ErrorHandler
        eh = getattr(self, "error_handler", None)
        if eh is not None:
            fn = getattr(eh, "is_transient_error", None) or getattr(eh, "_is_transient_error", None)
            if callable(fn):
                try:
                    return bool(fn(e))
                except Exception:
                    pass

        # 2. Fallback на MessageHandler
        mh = getattr(self, "message_handler", None)
        if mh is not None:
            fn2 = getattr(mh, "is_transient_error", None) or getattr(mh, "_is_transient_error", None)
            if callable(fn2):
                try:
                    return bool(fn2(e))
                except Exception:
                    pass

        return False

    def _read_priority_batch(self, consumer, allow_block: bool = True) -> list:
        read_count = int(getattr(self.config, "read_count", 100)) if self.config else 100
        block_ms = int(getattr(self.config, "read_block_ms", 1000)) if self.config else 1000

        book_count = int(getattr(self.config, "read_count_book", max(10, read_count // 2))) if self.config else max(10, read_count // 2)
        l3_count = int(getattr(self.config, "read_count_l3", max(10, read_count // 4))) if self.config else max(10, read_count // 4)
        tick_count = int(getattr(self.config, "read_count_tick", max(20, read_count))) if self.config else max(20, read_count)

        msgs = []

        # 1) book — главный источник L2 качества: читаем первым и можно block
        # Блокируем, только если разрешено (обычно если в прошлом цикле было пусто)
        if self.book_stream:
            actual_block = min(block_ms, 200) if allow_block else 0
            msgs += consumer.read_new([self.book_stream], count=book_count, block_ms=actual_block) or []

        # 2) l3 — вторым
        if self.l3_stream:
            msgs += consumer.read_new([self.l3_stream], count=l3_count, block_ms=0) or []

        # 3) ticks — последним (обычно самый плотный поток)
        if self.tick_stream:
            msgs += consumer.read_new([self.tick_stream], count=tick_count, block_ms=0) or []

        return msgs

    def _run_loop(self, consumer: Any, stop_event: Any) -> None:
        # Валидация зависимостей
        if not hasattr(self.message_handler, 'process_message_batch'):
            self.logger.error("message_handler missing process_message_batch, stopping")
            return
        if not hasattr(consumer, 'read_new'):
             self.logger.error("consumer missing read_new, stopping")
             return

        backoff_new = Backoff(
            base_delay=float(getattr(self.config, "backoff_base", 0.25)) if self.config else 0.25,
            multiplier=float(getattr(self.config, "backoff_multiplier", 2.0)) if self.config else 2.0,
            max_delay=float(getattr(self.config, "backoff_max", 5.0)) if self.config else 5.0,
            jitter=bool(getattr(self.config, "backoff_jitter", True)) if self.config else True,
        )
        backoff_pending = Backoff(
            base_delay=float(getattr(self.config, "backoff_pending_base", 0.25)) if self.config else 0.25,
            multiplier=float(getattr(self.config, "backoff_pending_multiplier", 2.0)) if self.config else 2.0,
            max_delay=float(getattr(self.config, "backoff_pending_max", 5.0)) if self.config else 5.0,
            jitter=bool(getattr(self.config, "backoff_jitter", True)) if self.config else True,
        )

        idle_sleep = float(getattr(self.config, "idle_sleep_s", 0.05)) if self.config else 0.05
        claim_interval_ms = int(getattr(self.config, "claim_interval_ms", 30000)) if self.config else 30000
        claim_interval_idle_ms = int(getattr(self.config, "claim_interval_idle_ms", 120000)) if self.config else 120000
        claim_interval_drain_ms = int(getattr(self.config, "claim_interval_drain_ms", 1000)) if self.config else 1000
        # опционально: лимит сообщений за итерацию (уменьшает ACK-latency)
        max_msgs_per_loop = int(getattr(self.config, "max_msgs_per_loop", 0) or 0) if self.config else 0
        if max_msgs_per_loop < 0:
            max_msgs_per_loop = 0

        # streams (dedupe)
        streams = list(dict.fromkeys([s for s in (self.tick_stream, self.book_stream, self.l3_stream) if s]))
        consumer.ensure_groups(streams, stop_event=stop_event)
        if stop_event.is_set():
            return

        # Лимит размера fail_counts
        fail_counts: dict[tuple[str, str], int] = {}
        next_claim_at_mono_ms = 0
        claim_start_ids = dict.fromkeys(streams, "0-0")

        self.logger.info("Main loop started for %s", self.symbol)

        # начальное восстановление pending
        _ = self._claim_and_process_pending(consumer, streams, claim_start_ids, fail_counts, backoff_pending, stop_event=stop_event)
        if stop_event.is_set():
            return

        should_block = True

        while not stop_event.is_set():
            mono_now_ms = int(time.monotonic() * 1000)
            wall_now_ms = get_ny_time_millis()

            # Очистка fail_counts если слишком большой (защита от утечки)
            if len(fail_counts) > 20000:
                # подрезаем часть, а не clear(), чтобы не потерять текущий прогресс poison-detection
                keys = list(fail_counts.keys())[:5000]
                for k in keys:
                    fail_counts.pop(k, None)

            if mono_now_ms >= next_claim_at_mono_ms:
                ok = self._claim_and_process_pending(
                    consumer, streams, claim_start_ids, fail_counts, backoff_pending, stop_event=stop_event
                )
                # Вытаскиваем "телеметрию" из MessageHandler (без изменения API)
                mh = self.message_handler
                claimed = int(getattr(mh, "_last_pending_claimed", 0) or 0)
                full_empty = bool(getattr(mh, "_last_pending_full_scan_empty", False))

                if not ok:
                    # transient/ошибка — попробуем снова раньше
                    next_claim_at_mono_ms = mono_now_ms + max(250, min(claim_interval_drain_ms, 2000))
                elif claimed > 0:
                    # есть что разбирать — дожимаем быстро
                    next_claim_at_mono_ms = mono_now_ms + max(200, claim_interval_drain_ms)
                elif full_empty:
                    # полный пустой проход — реже
                    next_claim_at_mono_ms = mono_now_ms + max(claim_interval_ms, claim_interval_idle_ms)
                else:
                    # обычный режим
                    next_claim_at_mono_ms = mono_now_ms + claim_interval_ms

            # Периодическая отправка метрик pending
            self._emit_pending_metrics(consumer, mono_now_ms, wall_now_ms)
            # Useless triple sampling removed

            try:
                msgs = self._read_priority_batch(consumer, allow_block=should_block)
            except Exception as e:
                if self._is_transient(e):
                    delay = backoff_new.get_delay()
                    self.logger.warning("Transient error on read_new: %s (backoff=%.2fs)", e, delay)
                    stop_event.wait(timeout=delay)
                    continue
                raise

            if stop_event.is_set():
                break

            if not msgs:
                should_block = True
                backoff_new.reset()
                if idle_sleep > 0:
                    stop_event.wait(timeout=idle_sleep)
                continue

            # Если сообщения есть, в след. раз не блокируем (читаем быстро остаток)
            should_block = False

            # --- chunking (опционально) ---
            total_tick = 0
            total_book = 0
            all_success = True

            if max_msgs_per_loop > 0 and len(msgs) > max_msgs_per_loop:
                for i in range(0, len(msgs), max_msgs_per_loop):
                    if stop_event.is_set():
                        all_success = False
                        break
                    chunk = msgs[i:i + max_msgs_per_loop]
                    t, b, ok = self.message_handler.process_message_batch(  # type: ignore
                        chunk, backoff_new, fail_counts, consumer, stop_event=stop_event
                    )
                    total_tick += t
                    total_book += b
                    if not ok:
                        all_success = False
                        break
            else:
                total_tick, total_book, all_success = self.message_handler.process_message_batch(  # type: ignore
                    msgs, backoff_new, fail_counts, consumer, stop_event=stop_event
                )

            self.tick_cnt += total_tick
            self.total_tick_cnt += total_tick
            self.book_cnt += total_book
            if all_success:
                backoff_new.reset()
            else:
                # after transient batch message_handler already waited backoff
                should_block = True

            if (time.monotonic() - float(getattr(self, "last_stat_mono_s", time.monotonic()))) >= 60.0:
                self._log_stats()
                self.last_stat_mono_s = time.monotonic()

    def _claim_and_process_pending(self, consumer: Any, streams: list[str],
                                  start_ids: dict[str, str],
                                  fail_counts: dict[tuple[str, str], int],
                                  backoff: Backoff,
                                  stop_event: Any) -> bool:
        """
        Wrapper вокруг message_handler.claim_and_process_pending с защитой от transient error
        и stop_event.
        """
        try:
            return self.message_handler.claim_and_process_pending(  # type: ignore
                consumer, streams, start_ids, fail_counts, backoff, stop_event=stop_event
            )
        except Exception as e:
            if self._is_transient(e):
                delay = backoff.get_delay()
                self.logger.warning("Transient error in claim_pending: %s (backoff=%.2fs)", e, delay)
                if stop_event:
                    stop_event.wait(timeout=delay)
                else:
                    time.sleep(delay)
                return False
            raise

    def _log_stats(self) -> None:
        self.logger.info("Статистика обработки: ticks=%d, books=%d (последние 60с для %s)",
                         self.tick_cnt, self.book_cnt, self.symbol)
        self.tick_cnt = 0
        self.book_cnt = 0
