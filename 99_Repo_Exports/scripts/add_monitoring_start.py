#!/usr/bin/env python3
"""
Добавляем запуск мониторинга каналов в start метод
"""

# Читаем файл
with open('telegram-worker/multithreaded_worker.py', 'r', encoding='utf-8') as f:
    content = f.read()

# Добавляем запуск мониторинга каналов
old_tasks = '''            # НОВЫЕ ЗАДАЧИ ДЛЯ ПРЕДОТВРАЩЕНИЯ "ЗАСЫПАНИЯ"
            asyncio.create_task(self.health_check())
            asyncio.create_task(self.keep_alive())
            asyncio.create_task(self.monitor_connection())

            self.logger.info("✅ Все задачи мониторинга запущены")'''
new_tasks = '''            # НОВЫЕ ЗАДАЧИ ДЛЯ ПРЕДОТВРАЩЕНИЯ "ЗАСЫПАНИЯ"
            asyncio.create_task(self.health_check())
            asyncio.create_task(self.keep_alive())
            asyncio.create_task(self.monitor_connection())

            # Запускаем мониторинг каналов
            asyncio.create_task(self.monitor_channels())

            self.logger.info("✅ Все задачи мониторинга запущены")'''

if old_tasks in content:
    content = content.replace(old_tasks, new_tasks)
    print("✅ Добавлен запуск мониторинга каналов")
else:
    print("❌ Задачи мониторинга не найдены")

# Добавляем метод monitor_channels
old_stop_method = '    def stop(self):'
new_monitor_method = '''    async def monitor_channels(self):
        """Мониторинг активности каналов."""
        while self.running:
            try:
                # Проверяем неактивные каналы
                inactive_channels = self.channel_monitor.get_inactive_channels()

                for channel_name in inactive_channels:
                    channel = self.channel_monitor.channels.get(channel_name)
                    if channel and channel.is_active:
                        # Канал стал неактивным
                        inactive_hours = (time.time() - channel.last_message_time) / 3600
                        self.alert_system.alert_channel_inactive(channel_name, inactive_hours)
                        channel.is_active = False

                # Очищаем старые алерты
                self.alert_system.cleanup_old_alerts(days=7)

                await asyncio.sleep(300)  # Проверяем каждые 5 минут

            except Exception as e:
                self.logger.error(f"❌ Ошибка в мониторинге каналов: {e}")
                await asyncio.sleep(60)

    def stop(self):'''

if old_stop_method in content:
    content = content.replace(old_stop_method, new_monitor_method)
    print("✅ Добавлен метод monitor_channels")
else:
    print("❌ Метод stop не найден")

# Сохраняем файл
with open('telegram-worker/multithreaded_worker.py', 'w', encoding='utf-8') as f:
    f.write(content)

print("✅ Файл multithreaded_worker.py обновлен")
