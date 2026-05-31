"""
Слушатель событий TP/SL из Redis streams.

Сидит на Redis stream events:trades и обрабатывает:
- TP1_HIT → запуск трейлинга
- TP2_HIT, TP3_HIT → логирование
- SL_HIT → анализ причин
- TRAILING_MOVE → обновление статистики

Интегрировано с scanner_infra:
- Redis streams с consumer groups
- Graceful shutdown
- Health checks
- Prometheus metrics (опционально)
"""

import json
import os
import signal
import sys
import time

# Добавляем путь к python-worker в PYTHONPATH
from pathlib import Path
from typing import Any

import redis
from core.redis_keys import RedisStreams as RS

_worker_path = Path(__file__).parent.parent
if str(_worker_path) not in sys.path:
    sys.path.insert(0, str(_worker_path))

from common.log import setup_logger
from services.tp_hit_trailing_orchestrator import TpHitTrailingOrchestrator, TrailingResult
from services.trailing_profiles import TrailingProfilesRegistry

log = setup_logger("tp_event_listener")

try:
    from prometheus_client import Counter as _Counter, Gauge as _Gauge
    _listener_dlq_total = _Counter(
        "tp_listener_dlq_total",
        "Messages pushed to events:trailing:dlq by listener",
        ["reason"],
    )
    _listener_dlq_write_failed_total = _Counter(
        "tp_listener_dlq_write_failed_total",
        "DLQ write failures — message left in PEL",
    )
    _listener_pel_reclaimed_total = _Counter(
        "tp_listener_pel_reclaimed_total",
        "Messages reclaimed from PEL via XAUTOCLAIM",
    )
    _listener_pel_reclaim_errors_total = _Counter(
        "tp_listener_pel_reclaim_errors_total",
        "PEL XAUTOCLAIM errors",
    )
    _listener_poison_quarantine_total = _Counter(
        "tp_listener_poison_quarantine_total",
        "Messages force-ACKed after exceeding retry cap (poison messages)",
    )
    _listener_pel_pending = _Gauge(
        "tp_listener_pel_pending",
        "Messages currently in PEL (unacknowledged pending list)",
    )
except Exception:
    _listener_dlq_total = None              # type: ignore[assignment]
    _listener_dlq_write_failed_total = None  # type: ignore[assignment]
    _listener_pel_reclaimed_total = None     # type: ignore[assignment]
    _listener_pel_reclaim_errors_total = None  # type: ignore[assignment]
    _listener_poison_quarantine_total = None  # type: ignore[assignment]
    _listener_pel_pending = None             # type: ignore[assignment]


class TPEventListener:
    """
    Слушатель событий TP/SL из Redis streams.
    
    Обрабатывает события торговых событий и запускает трейлинг после TP1.
    """

    def __init__(self):
        # Конфигурация из env
        self.redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
        self.events_stream = os.getenv("TP_EVENTS_STREAM", RS.EVENTS_TRADES)
        self.consumer_group = os.getenv("TP_EVENTS_GROUP", "tp1-trailing-group")
        self.consumer_name = os.getenv("TP_EVENTS_CONSUMER", f"tp1-trailing-{int(time.time())}")

        # Redis connection
        self.r = redis.from_url(self.redis_url, decode_responses=True)
        log.info("✅ Connected to Redis: %s", self.redis_url)

        # Ensure consumer group exists
        self._ensure_group()

        # Инициализация компонентов
        self.profiles = TrailingProfilesRegistry()
        self.orchestrator = TpHitTrailingOrchestrator(
            redis_client=self.r,
            profiles=self.profiles
        )

        # Флаг для graceful shutdown
        self.running = False

        # TrailingStateWorker (Phase B shadow)
        self._tsw: "TrailingStateWorker | None" = None
        self._tsw_stop: list[bool] = [False]
        if os.getenv("TRAILING_STATE_ENABLED", "0") == "1":
            try:
                from services.trailing_state_worker import TrailingStateWorker
                self._tsw = TrailingStateWorker(redis_client=self.r)
                log.info(
                    "✅ TrailingStateWorker initialized (shadow=%s)",
                    os.getenv("TRAILING_STATE_SHADOW", "1"),
                )
            except Exception as exc:
                log.warning("TrailingStateWorker init failed (disabled): %s", exc)

        # Статистика
        self.stats = {
            "messages_read": 0,
            "messages_processed": 0,
            "messages_acked": 0,
            "errors": 0,
            "last_message_ts": 0
        }

        # Настройка обработчиков сигналов для graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

        log.info(
            "✅ TPEventListener initialized | stream=%s group=%s consumer=%s",
            self.events_stream, self.consumer_group, self.consumer_name
        )

    def _ensure_group(self):
        """Создать consumer group если не существует."""
        max_retries = 30  # Retry for up to 30 attempts (about 2.5 minutes)
        retry_count = 0

        while retry_count < max_retries:
            try:
                self.r.xgroup_create(
                    self.events_stream,
                    self.consumer_group,
                    id="0",
                    mkstream=True
                )
                log.info("✅ Consumer group created: %s", self.consumer_group)
                return
            except redis.BusyLoadingError:
                # Redis is loading dataset from disk
                retry_count += 1
                wait_time = min(5 * retry_count, 30)  # Exponential backoff, max 30 seconds
                log.warning("⚠️ Redis загружает данные в память (попытка %d/%d), ждём %d сек...",
                          retry_count, max_retries, wait_time)
                time.sleep(wait_time)
                continue
            except redis.ResponseError as e:
                error_msg = str(e)
                if "BUSYGROUP" in error_msg:
                    log.debug("Consumer group already exists: %s", self.consumer_group)
                    return
                if "Redis is loading the dataset in memory" in error_msg:
                    retry_count += 1
                    wait_time = min(5 * retry_count, 30)  # Exponential backoff, max 30 seconds
                    log.warning("⚠️ Redis загружает данные в память (попытка %d/%d), ждём %d сек...",
                              retry_count, max_retries, wait_time)
                    time.sleep(wait_time)
                    continue
                else:
                    log.error("Failed to create consumer group: %s", e)
                    raise
            except (redis.ConnectionError, redis.TimeoutError) as e:
                # Handle connection issues with retry
                retry_count += 1
                wait_time = min(2 * retry_count, 10)  # Shorter backoff for connection issues
                log.warning("⚠️ Redis connection error (попытка %d/%d), ждём %d сек: %s",
                          retry_count, max_retries, wait_time, str(e))
                time.sleep(wait_time)
                continue

        # If we exhausted all retries
        log.error("❌ Не удалось создать consumer group после %d попыток: Redis недоступен", max_retries)
        raise RuntimeError(f"Failed to create consumer group after {max_retries} attempts")

    def _signal_handler(self, signum, _frame: Any) -> None:
        """Обработчик сигналов для graceful shutdown."""
        log.info("⛔ Received signal %d, shutting down gracefully...", signum)
        self.running = False
        self._tsw_stop[0] = True

    # ── Priority 1: listener-level DLQ ───────────────────────────────────────

    def _push_listener_dlq(self, msg_id: str, fields: dict[str, Any], reason: str) -> bool:
        """Записывает сообщение в events:trailing:dlq.

        Вызывается:
          - при parse_error (event = None из _parse_event)
          - при orchestrator_error (result.error is not None and not skipped)
          - при необработанном исключении во время handle_event

        Возвращает True при успехе. При False вызывающий НЕ делает XACK —
        сообщение остаётся в PEL для reclaimer-а.
        """
        try:
            entry = {
                "msg_id": msg_id,
                "reason": reason[:256],
                "fields_json": json.dumps(fields, ensure_ascii=False, default=str)[:2000],
                "ts_ms": str(int(time.time() * 1000)),
                "source": "tp_event_listener",
                "stream": self.events_stream,
            }
            self.r.xadd(RS.EVENTS_TRAILING_DLQ, entry, maxlen=5000, approximate=True)  # type: ignore[arg-type]
            self.stats.setdefault("dlq_pushed", 0)
            self.stats["dlq_pushed"] += 1
            if _listener_dlq_total is not None:
                _listener_dlq_total.labels(reason=reason.split(":")[0]).inc()
            log.warning("⚠️ DLQ push: msg_id=%s reason=%s", msg_id, reason)
            return True
        except Exception as exc:
            log.warning("⚠️ DLQ push failed — leaving msg_id=%s in PEL: %s", msg_id, exc)
            self.stats.setdefault("dlq_write_failed", 0)
            self.stats["dlq_write_failed"] += 1
            if _listener_dlq_write_failed_total is not None:
                _listener_dlq_write_failed_total.inc()
            return False

    def _xack(self, msg_id: str) -> None:
        """XACK с подавлением ошибок."""
        try:
            self.r.xack(self.events_stream, self.consumer_group, msg_id)
            self.stats["messages_acked"] += 1
        except redis.BusyLoadingError:
            log.warning("⚠️ Redis loading, skipping XACK for %s", msg_id)
        except Exception as ack_err:
            log.warning("⚠️ XACK failed for %s: %s", msg_id, ack_err)

    # ── Poison-message guard ──────────────────────────────────────────────────

    _MAX_RETRIES: int = int(os.getenv("TP_PEL_MAX_RETRIES", "5"))
    _RETRY_TTL_SEC: int = 3600  # retry key expires after 1 hour

    def _check_poison_cap(self, msg_id: str) -> bool:
        """Increment delivery counter; return True if message is a poison pill.

        Increments `tp_listener:retries:{msg_id}` on every call.
        When count > TP_PEL_MAX_RETRIES the caller must force-ACK (even if DLQ fails)
        to break the infinite PEL→reclaim loop.
        """
        key = f"tp_listener:retries:{msg_id}"
        try:
            count = int(self.r.incr(key) or 0)  # type: ignore[arg-type]
            self.r.expire(key, self._RETRY_TTL_SEC)
            if count > self._MAX_RETRIES:
                self.stats.setdefault("pel_poison_acked", 0)
                self.stats["pel_poison_acked"] += 1
                if _listener_poison_quarantine_total is not None:
                    _listener_poison_quarantine_total.inc()
                log.warning(
                    "⚠️ Poison-message cap: msg_id=%s retries=%d > max=%d → force DLQ+ACK",
                    msg_id, count, self._MAX_RETRIES,
                )
                return True
        except Exception as exc:
            log.debug("retry-counter error (ignored): %s", exc)
        return False

    def _process_one_message(self, msg_id: str, fields: dict[str, Any]) -> None:
        """Обрабатывает одно сообщение из stream: parse → handle → DLQ if error → ACK.

        Выделен в отдельный метод для юнит-тестирования без запуска run()-цикла.
        Poison-message guard: если сообщение было доставлено > TP_PEL_MAX_RETRIES раз,
        оно принудительно отправляется в DLQ и ACK-ается (чтобы разорвать петлю PEL).
        """
        # Poison-message guard: force-ACK if delivery count exceeds cap
        if self._check_poison_cap(msg_id):
            self._push_listener_dlq(msg_id, fields, "max_retries_exceeded")
            self._xack(msg_id)  # force ACK even if DLQ write failed — must break the loop
            return

        try:
            self.stats["messages_read"] += 1

            # ── parse ────────────────────────────────────────────────────────
            event = self._parse_event(fields)
            if not event:
                ok = self._push_listener_dlq(msg_id, fields, "parse_error")
                if ok:
                    self._xack(msg_id)
                # if DLQ write failed, leave in PEL for reclaimer
                self.stats["errors"] += 1
                return

            # ── handle ───────────────────────────────────────────────────────
            result: TrailingResult = self.orchestrator.handle_event(event)

            # Hard failure (не skipped, явный error) → DLQ; only ACK if DLQ succeeded
            if not result.success and not result.skipped and result.error:
                ok = self._push_listener_dlq(
                    msg_id, fields,
                    f"orchestrator_error:{result.error}",
                )
                if not ok:
                    return  # leave in PEL

            # Phase B: dispatch to TrailingStateWorker BEFORE XACK so that
            # an exception leaves the message in PEL for reclaimer (audit §4.3).
            # In shadow mode this is fail-open (debug log + continue); in live
            # mode any HWM failure should block ACK so the message can retry.
            _tsw = getattr(self, "_tsw", None)
            if _tsw is not None and event:
                try:
                    _tsw.dispatch_event(event)
                except Exception as _tsw_exc:
                    # Shadow mode: keep prior fail-open behaviour (log only).
                    # Live mode: push to DLQ and don't ACK — let reclaimer retry.
                    if getattr(_tsw, "shadow", True):
                        log.debug("TrailingStateWorker dispatch error (shadow): %s", _tsw_exc)
                    else:
                        log.error("TrailingStateWorker dispatch error (LIVE): %s", _tsw_exc)
                        ok = self._push_listener_dlq(
                            msg_id, fields,
                            f"tsw_dispatch_error:{type(_tsw_exc).__name__}",
                        )
                        if not ok:
                            return  # leave in PEL
                        # DLQ ok → still ACK below so we don't loop

            self._xack(msg_id)
            self.stats["messages_processed"] += 1
            self.stats["last_message_ts"] = int(time.time())

        except Exception as e:
            self.stats["errors"] += 1
            log.error("❌ Error processing message %s: %s", msg_id, str(e), exc_info=True)
            ok = self._push_listener_dlq(msg_id, fields, f"exception:{type(e).__name__}")
            if ok:
                self._xack(msg_id)
            # if DLQ write failed, leave in PEL for reclaimer

    # ── PEL reclaimer ─────────────────────────────────────────────────────────

    def _reclaim_pel(self) -> None:
        """XAUTOCLAIM сообщений зависших в PEL > TP_PEL_STALE_MS мс.

        Вызывается из run() раз в TP_PEL_RECLAIM_INTERVAL_S секунд.
        Reprocess через _process_one_message; при повторном сбое оставляет в PEL.
        Метрики: stats["pel_reclaimed"], stats["pel_reclaim_errors"].
        """
        stale_ms = int(os.getenv("TP_PEL_STALE_MS", "60000"))
        count = int(os.getenv("TP_PEL_RECLAIM_COUNT", "50"))
        try:
            result = self.r.xautoclaim(
                self.events_stream,
                self.consumer_group,
                self.consumer_name,
                stale_ms,
                "0-0",
                count=count,
            )
            # xautoclaim returns (next_id, [(msg_id, fields), ...], [deleted_ids])
            claimed = result[1] if isinstance(result, (list, tuple)) and len(result) > 1 else []
            if claimed:
                log.info("♻️ PEL reclaimer: %d messages claimed", len(claimed))
                if _listener_pel_reclaimed_total is not None:
                    _listener_pel_reclaimed_total.inc(len(claimed))
            for msg_id, fields in claimed:
                self.stats.setdefault("pel_reclaimed", 0)
                self.stats["pel_reclaimed"] += 1
                self._process_one_message(msg_id, fields)
            # Update PEL pending gauge after reclaim
            try:
                pending_info = self.r.xpending(self.events_stream, self.consumer_group)
                if pending_info and isinstance(pending_info, dict):
                    pending_count = int(pending_info.get("pending", 0) or 0)
                    if _listener_pel_pending is not None:
                        _listener_pel_pending.set(pending_count)
            except Exception:
                pass
        except AttributeError:
            pass  # redis-py < 4.3 doesn't support xautoclaim
        except Exception as exc:
            self.stats.setdefault("pel_reclaim_errors", 0)
            self.stats["pel_reclaim_errors"] += 1
            if _listener_pel_reclaim_errors_total is not None:
                _listener_pel_reclaim_errors_total.inc()
            log.debug("PEL reclaim error (ignored): %s", exc)

    def run(self):
        """Основной цикл обработки событий.

        Читает события из Redis stream через consumer group.
        При parse-ошибке или hard-failure оркестратора пишет в events:trailing:dlq,
        затем ACK-ает сообщение (чтобы не застрять в PEL).
        PEL reclaimer подбирает зависшие сообщения каждые TP_PEL_RECLAIM_INTERVAL_S сек.
        """
        log.info("🚀 Starting event listener loop...")
        self.running = True

        # Start TrailingStateWorker tick loop in background thread (fail-open)
        if self._tsw is not None:
            import threading
            _tick_redis_url = os.getenv("REDIS_TICKS_URL", os.getenv("REDIS_URL", "redis://redis:6379/0"))
            try:
                _tick_r = __import__("redis").from_url(_tick_redis_url, decode_responses=True)
            except Exception:
                _tick_r = None
            _tsw_thread = threading.Thread(
                target=self._tsw.run_tick_loop,
                args=(_tick_r,),
                kwargs={"stop_flag": self._tsw_stop},
                daemon=True,
                name="trailing-state-tick-loop",
            )
            _tsw_thread.start()
            log.info("🔄 TrailingStateWorker tick loop thread started")

        batch_size = int(os.getenv("TP_EVENTS_BATCH_SIZE", "50"))
        block_ms = int(os.getenv("TP_EVENTS_BLOCK_MS", "5000"))
        stats_interval = int(os.getenv("STATS_INTERVAL_SEC", "300"))
        reclaim_interval = int(os.getenv("TP_PEL_RECLAIM_INTERVAL_S", "30"))
        last_stats_log = time.time()
        last_reclaim = time.time()

        log.info("📊 Batch size: %d | Block timeout: %dms", batch_size, block_ms)

        while self.running:
            try:
                messages = self._read_messages(batch_size, block_ms)

                if not messages:
                    if time.time() - last_stats_log >= stats_interval:
                        self._log_stats()
                        last_stats_log = time.time()
                    if time.time() - last_reclaim >= reclaim_interval:
                        self._reclaim_pel()
                        last_reclaim = time.time()
                    continue

                for msg_id, fields in messages:
                    self._process_one_message(msg_id, fields)

                if time.time() - last_reclaim >= reclaim_interval:
                    self._reclaim_pel()
                    last_reclaim = time.time()

                if time.time() - last_stats_log >= stats_interval:
                    self._log_stats()
                    last_stats_log = time.time()

                time.sleep(0.1)

            except KeyboardInterrupt:
                log.info("⛔ Keyboard interrupt, shutting down...")
                self.running = False
                break

            except redis.ConnectionError as e:
                self.stats["errors"] += 1
                log.error("❌ Redis connection error: %s", str(e))
                time.sleep(5.0)

            except Exception as e:
                self.stats["errors"] += 1
                log.error("❌ Loop error: %s", str(e), exc_info=True)
                time.sleep(1.0)

        # Final cleanup
        log.info("🛑 Event listener stopped")
        self._log_stats()
        log.info("✅ Shutdown complete")

    def _read_messages(
        self,
        count: int,
        block_ms: int
    ) -> list[tuple[str, dict[str, str]]]:
        """
        Читает сообщения из Redis stream через consumer group.

        Args:
            count: Количество сообщений для чтения
            block_ms: Таймаут блокировки (мс)

        Returns:
            List of (msg_id, fields_dict) tuples
        """
        try:
            resp = self.r.xreadgroup(
                self.consumer_group,
                self.consumer_name,
                streams={self.events_stream: ">"},
                count=count,
                block=block_ms
            )

            if not resp:
                return []

            # resp: [(stream, [(id, {fields})])]
            for stream_key, msgs in resp:
                if stream_key == self.events_stream:
                    return msgs

            return []

        except redis.BusyLoadingError:
            # Redis is still loading, wait a bit
            log.warning("⚠️ Redis всё ещё загружает данные, пропускаем чтение сообщений")
            time.sleep(5)
            return []
        except Exception as e:
            msg = str(e)
            if "NOGROUP" in msg:
                log.error("❌ Error reading from stream (NOGROUP) -> recreating group: %s", msg)
                try:
                    self._ensure_group()
                except Exception as create_err:
                    log.error("❌ Failed to recreate consumer group: %s", create_err)
                return []
            log.error("❌ Error reading from stream: %s", msg)
            return []

    def _parse_event(self, fields: dict[str, str]) -> dict[str, Any] | None:
        """
        Парсит событие из Redis stream fields.
        
        Поддерживает:
        - JSON в поле 'data'
        - Flat key-value поля
        - JSON-строки в значениях
        
        Args:
            fields: Поля сообщения из Redis stream
            
        Returns:
            Словарь события или None
        """
        if not fields:
            return None

        try:
            # Если есть поле 'data' с JSON
            if "data" in fields:
                try:
                    return json.loads(fields["data"])
                except json.JSONDecodeError:
                    log.warning("Failed to parse JSON from 'data' field")
                    return None

            # Иначе возвращаем поля как есть
            event = {}
            for key, value in fields.items():
                # Пытаемся распарсить JSON-значения
                if value and isinstance(value, str):
                    if value.startswith("{") or value.startswith("["):
                        try:
                            event[key] = json.loads(value)
                        except json.JSONDecodeError:
                            event[key] = value
                    else:
                        event[key] = value
                else:
                    event[key] = value

            # Normalize bare TP_HIT → TP{n}_HIT so _parse_tp_level can route it.
            # Executor publishes event_type=TP_HIT with tp_level=<int> field.
            et = event.get("event_type", "")
            if isinstance(et, str) and et.upper() == "TP_HIT":
                try:
                    lvl = int(event.get("tp_level", 0))
                except (TypeError, ValueError):
                    lvl = 0
                if lvl >= 1:
                    event["event_type"] = f"TP{lvl}_HIT"

            return event if event else None

        except Exception as e:
            log.warning("Failed to parse event: %s", str(e))
            return None

    def _log_stats(self):
        """Вывести статистику в лог."""
        log.info(
            "📊 Listener Stats: read=%d processed=%d acked=%d errors=%d last_msg=%ds_ago",
            self.stats["messages_read"],
            self.stats["messages_processed"],
            self.stats["messages_acked"],
            self.stats["errors"],
            int(time.time()) - self.stats["last_message_ts"] if self.stats["last_message_ts"] > 0 else -1
        )

        # Статистика оркестратора
        self.orchestrator.log_stats()

    def health_check(self) -> dict[str, Any]:
        """
        Проверка здоровья сервиса.

        Returns:
            Словарь с информацией о состоянии
        """
        try:
            # Проверка Redis
            self.r.ping()
            redis_ok = True
        except redis.BusyLoadingError:
            # Redis is loading data, consider it unhealthy for now
            redis_ok = False
        except Exception:
            redis_ok = False

        # Проверка активности (последнее сообщение не более 10 минут назад)
        last_msg_age = int(time.time()) - self.stats["last_message_ts"]
        is_active = last_msg_age < 600 if self.stats["last_message_ts"] > 0 else True

        return {
            "status": "healthy" if (redis_ok and self.running) else "unhealthy",
            "running": self.running,
            "redis_connected": redis_ok,
            "active": is_active,
            "stats": self.stats,
            "orchestrator_stats": self.orchestrator.get_stats()
        }


def main():
    """Entry point."""
    log.info("=" * 80)
    log.info("TP Event Listener Service")
    log.info("=" * 80)

    metrics_port = int(os.getenv("METRICS_PORT", "9921"))
    try:
        from prometheus_client import start_http_server
        start_http_server(metrics_port)
        log.info("Prometheus metrics: :%d/metrics", metrics_port)
    except Exception as exc:
        log.warning("Prometheus HTTP server not started: %s", exc)

    listener = TPEventListener()

    log.info("Configuration:")
    log.info("  Redis URL: %s", listener.redis_url)
    log.info("  Events stream: %s", listener.events_stream)
    log.info("  Consumer group: %s", listener.consumer_group)
    log.info("  Consumer name: %s", listener.consumer_name)
    log.info("=" * 80)

    try:
        listener.run()
    except Exception as e:
        log.error("❌ Fatal error: %s", str(e), exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()

