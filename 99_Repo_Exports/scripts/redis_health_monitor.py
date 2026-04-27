#!/usr/bin/env python3
"""
Мониторинг здоровья Redis и автоматическое исправление проблем.
"""

import redis
import time
import subprocess
import sys
import os
from datetime import datetime

class RedisHealthMonitor:
    def __init__(self, host='localhost', port=6379):
        self.host = host
        self.port = port
        self.redis_client = None
        self.connection_retries = 0
        self.max_retries = 5

    def connect(self):
        """Подключается к Redis с повторными попытками."""
        try:
            self.redis_client = redis.Redis(
                host=self.host,
                port=self.port,
                decode_responses=True,
                socket_connect_timeout=5,
                socket_timeout=5,
                health_check_interval=30
            )
            # Тестируем подключение
            self.redis_client.ping()
            print(f"✅ Redis подключение установлено: {self.host}:{self.port}")
            self.connection_retries = 0
            return True
        except Exception as e:
            self.connection_retries += 1
            print(f"❌ Ошибка подключения к Redis (попытка {self.connection_retries}): {e}")
            return False

    def check_redis_health(self):
        """Проверяет здоровье Redis."""
        if not self.redis_client:
            return False

        try:
            # Проверяем ping
            self.redis_client.ping()

            # Проверяем info
            info = self.redis_client.info()

            # Проверяем память
            used_memory = info.get('used_memory', 0)
            max_memory = info.get('maxmemory', 0)
            memory_usage = (used_memory / max_memory * 100) if max_memory > 0 else 0

            # Проверяем подключения
            connected_clients = info.get('connected_clients', 0)

            print("📊 Redis Health Check:")
            print(f"   - Подключенные клиенты: {connected_clients}")
            print(f"   - Использование памяти: {memory_usage:.1f}%")
            print(f"   - Uptime: {info.get('uptime_in_seconds', 0)} секунд")

            return True

        except Exception as e:
            print(f"❌ Ошибка проверки здоровья Redis: {e}")
            return False

    def check_streams_health(self):
        """Проверяет здоровье Redis Streams."""
        if not self.redis_client:
            return False

        try:
            # Получаем список всех streams
            stream_keys = self.redis_client.keys("stream:*")

            print("📊 Redis Streams Health Check:")
            print(f"   - Найдено streams: {len(stream_keys)}")

            # Проверяем ключевые streams
            critical_streams = [
                'stream:top-gainers',
                'stream:top-losers',
                'stream:volatilityRange',
                'stream:ws-new-pairs'
            ]

            for stream in critical_streams:
                try:
                    info = self.redis_client.xinfo_stream(stream)
                    length = info.get('length', 0)
                    last_id = info.get('last-generated-id', 'N/A')
                    print(f"   - {stream}: {length} сообщений, последний ID: {last_id}")
                except Exception as e:
                    print(f"   - {stream}: ❌ Ошибка - {e}")

            return True

        except Exception as e:
            print(f"❌ Ошибка проверки streams: {e}")
            return False

    def restart_redis_if_needed(self):
        """Перезапускает Redis если необходимо."""
        if self.connection_retries >= self.max_retries:
            print("🔄 Попытка перезапуска Redis...")
            try:
                # Перезапускаем Redis контейнер
                subprocess.run([
                    'docker-compose', 'restart', 'redis'
                ], check=True, cwd='/home/alex/front/trade/scanner_infra')

                print("✅ Redis перезапущен")
                time.sleep(10)  # Ждем запуска

                # Сбрасываем счетчик попыток
                self.connection_retries = 0
                return True

            except Exception as e:
                print(f"❌ Ошибка перезапуска Redis: {e}")
                return False

        return False

    def monitor_continuously(self, interval=30):
        """Непрерывный мониторинг Redis."""
        print(f"🚀 Запуск мониторинга Redis каждые {interval} секунд...")
        print("=" * 60)

        while True:
            try:
                print(f"\n🕐 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - Проверка Redis...")

                # Подключаемся если нужно
                if not self.redis_client:
                    if not self.connect():
                        if self.restart_redis_if_needed():
                            continue
                        else:
                            time.sleep(interval)
                            continue

                # Проверяем здоровье
                if not self.check_redis_health():
                    self.redis_client = None
                    continue

                # Проверяем streams
                if not self.check_streams_health():
                    self.redis_client = None
                    continue

                print("✅ Redis работает нормально")

            except KeyboardInterrupt:
                print("\n🛑 Мониторинг остановлен пользователем")
                break
            except Exception as e:
                print(f"❌ Неожиданная ошибка: {e}")
                self.redis_client = None

            time.sleep(interval)

def main():
    """Основная функция."""
    print("🔧 Redis Health Monitor")
    print("Мониторинг и автоматическое исправление проблем Redis")
    print("=" * 60)

    # Определяем хост Redis
    redis_host = os.getenv('REDIS_HOST', 'localhost')
    redis_port = int(os.getenv('REDIS_PORT', 6379))

    print(f"🎯 Мониторинг Redis: {redis_host}:{redis_port}")

    monitor = RedisHealthMonitor(redis_host, redis_port)

    # Проверяем подключение
    if not monitor.connect():
        print("❌ Не удалось подключиться к Redis")
        sys.exit(1)

    # Запускаем мониторинг
    try:
        monitor.monitor_continuously(interval=30)
    except KeyboardInterrupt:
        print("\n👋 Мониторинг завершен")

if __name__ == "__main__":
    main()
