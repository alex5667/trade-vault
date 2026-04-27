#!/usr/bin/env python3
"""
Comprehensive Health Monitoring для Telegram Worker
Senior Developer approach: Full observability
"""

import docker
import redis
from datetime import datetime

class TelegramWorkerMonitor:
    """Мониторинг здоровья telegram-worker"""

    def __init__(self):
        self.docker_client = docker.from_env()
        self.redis_client = None
        self.container_name = "scanner-telegram-worker"

    def connect_redis(self) -> bool:
        """Подключение к Redis"""
        try:
            self.redis_client = redis.Redis(host='localhost', port=6379, decode_responses=True)
            self.redis_client.ping()
            return True
        except Exception as e:
            print(f"❌ Redis недоступен: {e}")
            return False

    def check_container_status(self) -> tuple[bool, dict]:
        """Проверка статуса контейнера"""
        try:
            container = self.docker_client.containers.get(self.container_name)
            status = {
                'running': container.status == 'running',
                'status': container.status,
                'uptime': None,
                'restarts': container.attrs.get('RestartCount', 0)
            }

            if status['running']:
                started_at = container.attrs['State']['StartedAt']
                # Parse timestamp and calculate uptime
                # Simplified for now
                status['started_at'] = started_at

            return True, status
        except docker.errors.NotFound:
            return False, {'error': 'Container not found'}
        except Exception as e:
            return False, {'error': str(e)}

    def check_channels_health(self) -> dict:
        """Проверка здоровья каналов"""
        if not self.redis_client:
            return {'error': 'Redis not connected'}

        try:
            total_channels = self.redis_client.scard('telegram:channels:usernames')
            failed_entities = self.redis_client.smembers('telegram:failed_entities')

            return {
                'total': total_channels,
                'failed': len(failed_entities),
                'success_rate': ((total_channels - len(failed_entities)) / total_channels * 100) if total_channels > 0 else 0,
                'failed_list': list(failed_entities)[:10]  # First 10
            }
        except Exception as e:
            return {'error': str(e)}

    def check_message_flow(self) -> dict:
        """Проверка потока сообщений"""
        if not self.redis_client:
            return {'error': 'Redis not connected'}

        try:
            raw_count = self.redis_client.xlen('signal:telegram:raw')
            parsed_count = self.redis_client.xlen('signal:telegram:parsed')

            return {
                'raw_messages': raw_count,
                'parsed_messages': parsed_count,
                'processing_rate': (parsed_count / raw_count * 100) if raw_count > 0 else 100
            }
        except Exception as e:
            return {'error': str(e)}

    def check_event_handler(self) -> tuple[bool, str]:
        """Проверка регистрации event handler"""
        try:
            container = self.docker_client.containers.get(self.container_name)
            logs = container.logs(tail=200).decode('utf-8')

            if 'Обработчик зарегистрирован для' in logs:
                # Extract channel count
                for line in logs.split('\n'):
                    if 'Обработчик зарегистрирован для' in line:
                        return True, line.strip()
                return True, "Handler registered"
            else:
                return False, "Handler not found in logs"
        except Exception as e:
            return False, f"Error: {e}"

    def check_event_loop(self) -> tuple[bool, str]:
        """Проверка event loop"""
        try:
            container = self.docker_client.containers.get(self.container_name)
            logs = container.logs(tail=50).decode('utf-8')

            if 'event loop активен' in logs:
                return True, "Event loop active"
            else:
                return False, "Event loop not active"
        except Exception as e:
            return False, f"Error: {e}"

    def get_recent_errors(self) -> list[str]:
        """Получение последних ошибок"""
        try:
            container = self.docker_client.containers.get(self.container_name)
            logs = container.logs(tail=500).decode('utf-8')

            errors = []
            for line in logs.split('\n'):
                if any(keyword in line for keyword in ['ERROR', 'Error', '❌', 'CRITICAL']):
                    errors.append(line.strip())

            return errors[-10:]  # Last 10 errors
        except Exception as e:
            return [f"Error getting logs: {e}"]

    def print_health_report(self):
        """Вывод полного отчета о здоровье"""
        print("=" * 100)
        print(f"{'TELEGRAM WORKER HEALTH REPORT':^100}")
        print(f"{'Generated at: ' + datetime.now().strftime('%Y-%m-%d %H:%M:%S'):^100}")
        print("=" * 100)
        print()

        # Container Status
        print("📦 CONTAINER STATUS")
        print("-" * 100)
        exists, status = self.check_container_status()
        if exists and status.get('running'):
            print("   ✅ Status: RUNNING")
            print(f"   ⏰ Started: {status.get('started_at', 'N/A')}")
            print(f"   🔄 Restarts: {status.get('restarts', 0)}")
        else:
            print(f"   ❌ Status: {status.get('status', 'NOT RUNNING')}")
            if 'error' in status:
                print(f"   Error: {status['error']}")
        print()

        # Redis Connection
        print("💾 REDIS CONNECTION")
        print("-" * 100)
        if self.connect_redis():
            print("   ✅ Connected")
        else:
            print("   ❌ Failed to connect")
            return
        print()

        # Channels Health
        print("📡 CHANNELS HEALTH")
        print("-" * 100)
        channels = self.check_channels_health()
        if 'error' not in channels:
            print(f"   📊 Total channels: {channels['total']}")
            print(f"   ✅ Success rate: {channels['success_rate']:.1f}%")
            print(f"   ❌ Failed entities: {channels['failed']}")
            if channels['failed'] > 0:
                print("   Failed channels:")
                for ch in channels['failed_list']:
                    print(f"      - {ch}")
        else:
            print(f"   ❌ Error: {channels['error']}")
        print()

        # Message Flow
        print("📨 MESSAGE FLOW")
        print("-" * 100)
        flow = self.check_message_flow()
        if 'error' not in flow:
            print(f"   📥 Raw messages: {flow['raw_messages']}")
            print(f"   📤 Parsed messages: {flow['parsed_messages']}")
            print(f"   📊 Processing rate: {flow['processing_rate']:.1f}%")
        else:
            print(f"   ❌ Error: {flow['error']}")
        print()

        # Event Handler
        print("🎯 EVENT HANDLER")
        print("-" * 100)
        handler_ok, handler_msg = self.check_event_handler()
        if handler_ok:
            print(f"   ✅ {handler_msg}")
        else:
            print(f"   ❌ {handler_msg}")
        print()

        # Event Loop
        print("🔄 EVENT LOOP")
        print("-" * 100)
        loop_ok, loop_msg = self.check_event_loop()
        if loop_ok:
            print(f"   ✅ {loop_msg}")
        else:
            print(f"   ❌ {loop_msg}")
        print()

        # Recent Errors
        print("⚠️  RECENT ERRORS")
        print("-" * 100)
        errors = self.get_recent_errors()
        if errors:
            for err in errors:
                print(f"   {err}")
        else:
            print("   ✅ No recent errors")
        print()

        # Overall Status
        print("=" * 100)
        all_good = (
            exists and status.get('running') and
            handler_ok and loop_ok and
            channels.get('success_rate', 0) > 90
        )

        if all_good:
            print(f"{'🎉 SYSTEM HEALTHY':^100}")
        else:
            print(f"{'⚠️  SYSTEM HAS ISSUES':^100}")
        print("=" * 100)

def main():
    monitor = TelegramWorkerMonitor()
    monitor.print_health_report()

if __name__ == "__main__":
    main()

