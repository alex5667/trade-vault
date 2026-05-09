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

_worker_path = Path(__file__).parent.parent
if str(_worker_path) not in sys.path:
    sys.path.insert(0, str(_worker_path))

from common.log import setup_logger
from services.tp1_trailing_orchestrator import TP1TrailingOrchestrator
from services.trailing_profiles import TrailingProfilesRegistry

log = setup_logger("tp_event_listener")


class TPEventListener:
    """
    Слушатель событий TP/SL из Redis streams.
    
    Обрабатывает события торговых событий и запускает трейлинг после TP1.
    """

    def __init__(self):
        # Конфигурация из env
        self.redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
        self.events_stream = os.getenv("TP_EVENTS_STREAM", "events:trades")
        self.consumer_group = os.getenv("TP_EVENTS_GROUP", "tp1-trailing-group")
        self.consumer_name = os.getenv("TP_EVENTS_CONSUMER", f"tp1-trailing-{int(time.time())}")

        # Redis connection
        self.r = redis.from_url(self.redis_url, decode_responses=True)
        log.info("✅ Connected to Redis: %s", self.redis_url)

        # Ensure consumer group exists
        self._ensure_group()

        # Инициализация компонентов
        self.profiles = TrailingProfilesRegistry()
        self.orchestrator = TP1TrailingOrchestrator(
            redis_client=self.r,
            profiles=self.profiles
        )

        # Флаг для graceful shutdown
        self.running = False

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

    def _signal_handler(self, signum, frame):
        """Обработчик сигналов для graceful shutdown."""
        log.info("⛔ Received signal %d, shutting down gracefully...", signum)
        self.running = False

    def run(self):
        """
        Основной цикл обработки событий.
        
        Читает события из Redis stream и обрабатывает их.
        Использует XREADGROUP для надёжной обработки с consumer groups.
        """
        log.info("🚀 Starting event listener loop...")
        self.running = True

        # Параметры чтения
        batch_size = int(os.getenv("TP_EVENTS_BATCH_SIZE", "50"))
        block_ms = int(os.getenv("TP_EVENTS_BLOCK_MS", "5000"))

        # Периодическая статистика
        stats_interval = int(os.getenv("STATS_INTERVAL_SEC", "300"))  # 5 минут
        last_stats_log = time.time()

        log.info("📊 Batch size: %d | Block timeout: %dms", batch_size, block_ms)

        while self.running:
            try:
                # Читаем сообщения из stream
                messages = self._read_messages(batch_size, block_ms)

                if not messages:
                    # Нет новых сообщений - проверяем статистику
                    if time.time() - last_stats_log >= stats_interval:
                        self._log_stats()
                        last_stats_log = time.time()
                    continue

                # Обрабатываем сообщения
                for msg_id, fields in messages:
                    try:
                        self.stats["messages_read"] += 1

                        # Парсим событие
                        event = self._parse_event(fields)
                        if not event:
                            # Не удалось распарсить - всё равно подтверждаем
                            self.r.xack(self.events_stream, self.consumer_group, msg_id)
                            self.stats["messages_acked"] += 1
                            continue

                        # Обрабатываем событие
                        self.orchestrator.handle_event(event)

                        # Подтверждаем обработку
                        try:
                            self.r.xack(self.events_stream, self.consumer_group, msg_id)
                        except redis.BusyLoadingError:
                            log.warning("⚠️ Redis загружает данные, пропускаем XACK для %s", msg_id)
                        except Exception as ack_err:
                            log.warning("⚠️ Failed to XACK message %s: %s", msg_id, ack_err)

                        self.stats["messages_processed"] += 1
                        self.stats["messages_acked"] += 1
                        self.stats["last_message_ts"] = int(time.time())

                    except Exception as e:
                        self.stats["errors"] += 1
                        log.error(
                            "❌ Error processing message %s: %s",
                            msg_id, str(e), exc_info=True
                        )
                        # Подтверждаем даже при ошибке, чтобы не застрять
                        try:
                            self.r.xack(self.events_stream, self.consumer_group, msg_id)
                            self.stats["messages_acked"] += 1
                        except redis.BusyLoadingError:
                            log.warning("⚠️ Redis загружает данные, не удалось подтвердить сообщение %s", msg_id)
                        except Exception as ack_err:
                            log.warning("⚠️ Failed to XACK failed message %s: %s", msg_id, ack_err)

                # Небольшая пауза между батчами
                time.sleep(0.1)

                # Периодическая статистика
                if time.time() - last_stats_log >= stats_interval:
                    self._log_stats()
                    last_stats_log = time.time()

            except KeyboardInterrupt:
                log.info("⛔ Keyboard interrupt, shutting down...")
                self.running = False
                break

            except redis.ConnectionError as e:
                self.stats["errors"] += 1
                log.error("❌ Redis connection error: %s", str(e))
                # Exponential backoff or just longer sleep to avoid spam
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

