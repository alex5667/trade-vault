# message_handler.py
from __future__ import annotations

"""
Функционал обработки сообщений, извлеченный из base_orderflow_handler.py
"""

import os
import time
from types import SimpleNamespace
from typing import Any

from common.transient import is_transient_error
from utils.time_utils import get_ny_time_millis

try:
    from health_metrics import HealthMetrics
except Exception:  # pragma: no cover
    HealthMetrics = Any  # type: ignore

# optional: HealthMetrics внедряется через BaseOrderFlowHandler в рантайме
# from common.log import setup_logger
def setup_logger(name):
    import logging
    return logging.getLogger(name)


class MessageHandler:
    """
    Обрабатывает сообщения Redis stream и логику клейма pending сообщений.
    """

    def __init__(
        self,
        *args: Any,
        health_metrics: HealthMetrics | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__()  # in case of mixins
        self.health_metrics = health_metrics

        # Для обратной совместимости, поддержка старой сигнатуры
        if args and not kwargs.get('symbol'):
            # Старая сигнатура: MessageHandler(symbol, tick_stream, book_stream, l3_stream, ...)
            self._init_old(*args, **kwargs)
        else:
            # Новая сигнатура с kwargs
            self._init_new(**kwargs)

    def _init_old(
        self,
        symbol: str,
        tick_stream: str,
        book_stream: str,
        l3_stream: str,
        data_parser: Any,
        data_processor: Any,
        config: Any = None,
        max_fail_retries: int = 3,
        claim_min_idle_ms: int = 60000,
        claim_count: int = 100,
        health_metrics: Any = None,
    ):
        self.symbol = symbol
        self.tick_stream = tick_stream
        self.book_stream = book_stream
        self.l3_stream = l3_stream
        self.data_parser = data_parser
        self.data_processor = data_processor
        self.config = config
        self.max_fail_retries = max_fail_retries
        self.claim_min_idle_ms = claim_min_idle_ms
        self.claim_count = claim_count
        self.health_metrics = health_metrics or self.health_metrics
        self.logger = setup_logger(f"MessageHandler:{symbol}")

    def _init_new(self, **kwargs: Any) -> None:
        """Инициализация с новой сигнатурой на основе kwargs."""
        self.symbol = kwargs.get('symbol', 'unknown')
        self.tick_stream = kwargs.get('tick_stream', '')
        self.book_stream = kwargs.get('book_stream', '')
        self.l3_stream = kwargs.get('l3_stream', '')
        self.data_parser = kwargs.get('data_parser')
        self.data_processor = kwargs.get('data_processor')
        self.config = kwargs.get('config')

        cfg = self.config
        self.max_fail_retries = int(
            kwargs.get("max_fail_retries")
            or getattr(cfg, "max_fail_retries", 3)
            or 3
        )
        self.claim_min_idle_ms = int(
            kwargs.get("claim_min_idle_ms")
            or getattr(cfg, "claim_min_idle_ms", 60000)
            or 60000
        )
        self.claim_count = int(
            kwargs.get("claim_count")
            or getattr(cfg, "claim_count", 100)
            or 100
        )

        self.health_metrics = kwargs.get('health_metrics') or self.health_metrics
        self.on_bar_closed = kwargs.get('on_bar_closed')
        self.on_bucket_closed = kwargs.get('on_bucket_closed')
        self.error_handler = kwargs.get('error_handler')
        self.on_l3_event = kwargs.get('on_l3_event')

        # Кеш для дожатия ACK (только для сообщений, которые ПРИМЕНЕНЫ, но ACK упал)
        self._ack_retry_cache: dict[tuple[str, str], float] = {}
        self._last_ack_retry_cleanup = time.monotonic()
        self._ack_retry_ttl_s = float(getattr(cfg, "ack_retry_ttl_s", 600.0) if cfg else 600.0)
        self._ack_retry_max = int(getattr(cfg, "ack_retry_max", 20000) if cfg else 20000)

        self.logger = setup_logger(f"MessageHandler:{self.symbol}")

        # Telemetry for adaptive claim scheduling
        self._last_pending_claimed = 0
        self._last_pending_any_msgs = False
        self._last_pending_full_scan_empty = False

    def _gauge(self, name: str, value: int, **tags) -> None:
        """Отправка метрики gauge с фоллбеком на debug логирование."""
        hm = self.health_metrics or getattr(self, "metrics", None)
        if hm is not None:
            if hasattr(hm, "gauge"):
                hm.gauge(name, value, tags=tags)
                return
            if hasattr(hm, "set_gauge"):
                hm.set_gauge(name, value, tags=tags)
                return
        self.logger.debug("%s=%s tags=%s", name, value, tags)

    def _is_transient_error(self, e: Exception) -> bool:
        """
        Унифицированная проверка на transient error.
        Приоритет: ErrorHandler.is_transient_error() -> local fallback.
        """
        eh = self.error_handler
        if eh is not None:
            fn = getattr(eh, "is_transient_error", None) or getattr(eh, "_is_transient_error", None)
            if callable(fn):
                try:
                    return bool(fn(e))
                except Exception:
                    pass
        return bool(is_transient_error(e))

    def _handle_error(
        self,
        e: Exception,
        *,
        consumer: object,
        backoff: object,
        fail_counts: dict[tuple[str, str], int],
        stream: str,
        msg_id: str,
        fields: dict[str, Any] | None,
        where: str,
        stop_event: Any = None,
    ) -> tuple[bool, bool]:
        """Returns: (retry, is_transient)"""
        is_transient = self._is_transient_error(e)
        eh = self.error_handler

        if eh is not None and hasattr(eh, "handle_message_error"):
            try:
                retry = bool(
                    eh.handle_message_error(
                        e, consumer, backoff, fail_counts, stream, msg_id, fields=fields, where=where, stop_event=stop_event
                    )
                )
                return retry, is_transient
            except Exception as outer_e:
                self.logger.error("ErrorHandler crash: %s", outer_e)
                return True, is_transient

        if is_transient:
            return True, True

        key = (stream, msg_id)
        fail_counts[key] = fail_counts.get(key, 0) + 1
        if fail_counts[key] >= self.max_fail_retries:
            self.logger.warning("Poison message %s in %s reached max retries.", msg_id, stream)
            if os.getenv("DROP_POISON_ON_MAX_RETRIES", "0") == "1":
                self.logger.error("DROP_POISON_ON_MAX_RETRIES is set, dropping poison %s", msg_id)
                return False, False
            return True, False
        return True, False

    def _priority(self, stream: str) -> int:
        """book=0, l3=1, ticks=2"""
        if stream == self.book_stream:
            return 0
        if stream == self.l3_stream:
            return 1
        if stream == self.tick_stream:
            return 2
        return 9

    def process_message_batch(
        self,
        msgs: list[Any],
        backoff: object,
        fail_counts: dict[tuple[str, str], int],
        consumer: object,
        stop_event: Any = None,
        *,
        from_pending: bool = False,
    ) -> tuple[int, int, bool]:
        """Обработка пакета сообщений из Redis streams."""
        tick_cnt = 0
        book_cnt = 0
        invalid_tick = 0
        invalid_book = 0
        batch_had_transient = False
        acked_in_batch = 0

        strict_parsing = getattr(self, 'config', None) and getattr(self.config, 'parser_strict', False)
        if not strict_parsing:
            strict_parsing = os.getenv("PARSER_STRICT", "false").lower() == "true"

        msgs_sorted = sorted(msgs, key=lambda m: self._priority(getattr(m, "stream", "")))
        hm = self.health_metrics
        now_ms = get_ny_time_millis()

        # PERF: pre-capture normalize_ts to avoid repeated getattr in hot path
        norm_ts = getattr(self.data_processor, "_normalize_ts", None)

        for m in msgs_sorted:
            ok = False
            stream = getattr(m, "stream", "")
            msg_id = getattr(m, "msg_id", "")
            key = (stream, msg_id)

            if stop_event is not None and stop_event.is_set():
                batch_had_transient = True
                break

            # 1) ACK retry check
            if key in self._ack_retry_cache:
                try:
                    consumer.ack(stream, msg_id) # type: ignore
                    self._ack_retry_cache.pop(key, None)
                    fail_counts.pop(key, None)
                    acked_in_batch += 1
                    ok = True
                    continue
                except Exception as ack_e:
                    if self._is_transient_error(ack_e):
                        delay = float(getattr(backoff, "next_sleep", getattr(backoff, "get_delay", lambda: 1.0))())
                        self.logger.warning("Transient ACK retry error (batch): %s", ack_e)
                        if stop_event is not None and hasattr(stop_event, "wait"):
                            stop_event.wait(timeout=delay)
                        else:
                            time.sleep(delay)
                        batch_had_transient = True
                        break
                    self.logger.warning("ACK retry failed (non-transient): %s", ack_e)
                    batch_had_transient = True
                    break

            # 2) Normal processing
            try:
                now_curr = get_ny_time_millis()

                if stream == self.tick_stream:
                    tick = self.data_parser._parse_tick(m.fields)
                    if tick is None:
                        invalid_tick += 1
                        if strict_parsing:
                            raise ValueError(f"tick_parse_failed [from_pending={from_pending}]")
                        ok = True
                    else:
                        raw_ts = getattr(tick, "ts", 0)
                        msg_ts = int(norm_ts(raw_ts) or 0) if callable(norm_ts) else 0
                        if msg_ts > 0 and hm:
                            hm.on_stream_lag(self.symbol, "ticks", max(0, now_curr - msg_ts))

                        finished_bar, closed_bucket_ts_ms = self.data_processor._process_tick(tick)
                        if closed_bucket_ts_ms is not None and hm and hasattr(hm, "on_bucket_event"):
                            try:
                                is_suppressed = bool(finished_bar is not None and self.on_bar_closed is not None)
                                hm.on_bucket_event(self.symbol, processed=False, suppressed=is_suppressed)
                            except Exception:
                                pass

                        if finished_bar is not None and self.on_bar_closed is not None:
                            self.on_bar_closed(finished_bar)
                        elif closed_bucket_ts_ms is not None and self.on_bucket_closed is not None:
                            try:
                                self.on_bucket_closed(int(closed_bucket_ts_ms))
                            except Exception as e:
                                self.logger.warning("on_bucket_closed failed: %s", e)

                        tick_cnt += 1
                        ok = True

                elif stream == self.book_stream:
                    book = self.data_parser._parse_book(m.fields)
                    if book is None:
                        invalid_book += 1
                        if strict_parsing:
                            raise ValueError(f"book_parse_failed [from_pending={from_pending}]")
                        ok = True
                    else:
                        ts_raw = book.get("ts_ms") or 0
                        ts_ms = int(norm_ts(ts_raw) or 0) if callable(norm_ts) else 0
                        if ts_ms > 0 and hm:
                            hm.on_stream_lag(self.symbol, "book", max(0, now_curr - ts_ms))
                        self.data_processor._process_book(book)
                        book_cnt += 1
                        ok = True

                elif stream == self.l3_stream:
                    l3_event = self.data_parser._parse_l3_event(m.fields)
                    if l3_event:
                        ts_raw = getattr(l3_event, "ts_ms", 0) or 0
                        if ts_raw <= 0 and isinstance(l3_event, dict):
                            ts_raw = l3_event.get("ts_ms", 0) or 0

                        ts_ms = int(norm_ts(ts_raw) or 0) if callable(norm_ts) else 0
                        if ts_ms > 0 and hm:
                            hm.on_stream_lag(self.symbol, "l3", max(0, now_curr - ts_ms))

                        if self.on_l3_event is not None:
                            self.on_l3_event(l3_event)
                    ok = True

                else:
                    ok = True

            except Exception as e:
                # MARK: Indentation verify point (12 spaces)
                retry, is_transient = self._handle_error(
                    e,
                    consumer=consumer,
                    backoff=backoff,
                    fail_counts=fail_counts,
                    stream=stream,
                    msg_id=msg_id,
                    fields=m.fields,
                    where="batch",
                    stop_event=stop_event,
                )
                if retry:
                    batch_had_transient = True
                    ok = False
                    if is_transient:
                        break
                    else:
                        continue
                else:
                    ok = True

            # 3) ACK
            if ok:
                fail_counts.pop(key, None)
                if stream and msg_id:
                    try:
                        consumer.ack(stream, msg_id) # type: ignore
                        acked_in_batch += 1
                    except Exception as ack_e:
                        if self._is_transient_error(ack_e):
                            self._ack_retry_cache[key] = time.monotonic()
                            if len(self._ack_retry_cache) > self._ack_retry_max:
                                sorted_keys = sorted(self._ack_retry_cache.keys(), key=lambda k: self._ack_retry_cache[k])
                                for old_k in sorted_keys[:max(1, self._ack_retry_max // 10)]:
                                    self._ack_retry_cache.pop(old_k, None)

                            delay = float(getattr(backoff, "next_sleep", getattr(backoff, "get_delay", lambda: 1.0))())
                            self.logger.warning("Transient ACK error (batch-ack): %s", ack_e)
                            if stop_event is not None and hasattr(stop_event, "wait"):
                                stop_event.wait(timeout=delay)
                            else:
                                time.sleep(delay)
                            batch_had_transient = True
                            break
                        self.logger.warning("Failed to ACK message %s: %s", msg_id, ack_e)
                        batch_had_transient = True
                        break
                else:
                    self.logger.warning("Skip ACK: empty stream/msg_id (stream=%r msg_id=%r)", stream, msg_id)
            else:
                batch_had_transient = True

        # Cleanup
        now_mono = time.monotonic()
        if now_mono - self._last_ack_retry_cleanup > 60:
            self._last_ack_retry_cleanup = now_mono
            ttl = float(self._ack_retry_ttl_s)
            self._ack_retry_cache = {k: v for k, v in self._ack_retry_cache.items() if now_mono - v < ttl}

        if len(fail_counts) > 20000:
            keys = list(fail_counts.keys())[:5000]
            for k in keys:
                fail_counts.pop(k, None)

        if invalid_tick or invalid_book:
            self.logger.info("Parse stats: invalid_tick=%d invalid_book=%d (acked=%d)", invalid_tick, invalid_book, acked_in_batch)

        return tick_cnt, book_cnt, not batch_had_transient

    def claim_and_process_pending(
        self,
        consumer: object,
        streams: list[str],
        start_ids: dict[str, str],
        fail_counts: dict[tuple[str, str], int],
        backoff: object,
        stop_event: Any = None,
    ) -> bool:
        """Клейм и обработка pending сообщений."""
        streams_sorted = sorted(streams, key=self._priority)
        claimed: list[Any] = []
        any_msgs = False

        for stream in streams_sorted:
            if stop_event is not None and stop_event.is_set():
                return False

            start_id = start_ids.get(stream, "0-0")
            try:
                next_id, msgs = consumer.claim_pending(
                    stream,
                    min_idle_ms=self.claim_min_idle_ms,
                    start_id=start_id,
                    count=self.claim_count,
                )
                # Важно: redis-py может вернуть next_id="0-0" когда скан завершён.
                # Если сообщений нет, НЕ сбрасываем start_id на "0-0" — иначе будем крутиться по кругу.
                if (not msgs) and (next_id == "0-0"):
                    start_ids[stream] = start_id
                else:
                    start_ids[stream] = str(next_id)
            except Exception as e:
                if self._is_transient_error(e):
                    delay = float(getattr(backoff, "next_sleep", lambda: 1.0)())
                    self.logger.warning("Transient error on pending claim: %s", e)
                    if stop_event is not None and hasattr(stop_event, "wait"):
                        stop_event.wait(timeout=delay)
                    else:
                        time.sleep(delay)
                    return False
                raise

            if not msgs:
                continue

            any_msgs = True
            for m in msgs:
                s = getattr(m, "stream", None) or stream
                msg_id = getattr(m, "msg_id", None) or getattr(m, "id", None) or ""
                fields = getattr(m, "fields", None) or getattr(m, "data", None) or {}
                claimed.append(SimpleNamespace(stream=s, msg_id=msg_id, fields=fields))

        # Export state for MainLoop adaptive scheduling
        self._last_pending_claimed = int(len(claimed))
        self._last_pending_any_msgs = bool(any_msgs)
        self._last_pending_full_scan_empty = (not any_msgs) and all(
            (start_ids.get(s, "0-0")) == "0-0" for s in streams_sorted
        )

        if not any_msgs:
            try:
                getattr(backoff, "reset", lambda: None)()
            except Exception:
                pass
            return True

        total_tick, total_book, all_ok = self.process_message_batch(
            claimed, backoff, fail_counts, consumer, stop_event=stop_event, from_pending=True
        )
        if all_ok:
            try:
                getattr(backoff, "reset", lambda: None)()
            except Exception:
                pass
        return bool(all_ok)
