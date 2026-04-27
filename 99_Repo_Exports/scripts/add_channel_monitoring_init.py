#!/usr/bin/env python3
"""
Добавляем инициализацию каналов в мониторинг
"""

# Читаем файл
with open('telegram-worker/multithreaded_worker.py', 'r', encoding='utf-8') as f:
    content = f.read()

# Добавляем инициализацию каналов в мониторинг после подписки
old_subscription_end = '            self.logger.info(f"✅ Успешно подписано на {len(subscribed_channels)} каналов")'
new_subscription_end = '''            self.logger.info(f"✅ Успешно подписано на {len(subscribed_channels)} каналов")

            # Инициализируем каналы в мониторинге
            for channel in subscribed_channels:
                self.channel_monitor.add_channel(channel)

            # Загружаем статистику каналов из Redis
            self.channel_monitor.load_channel_stats()

            # Отправляем алерт о запуске системы
            self.alert_system.alert_system_startup(len(subscribed_channels))'''

if old_subscription_end in content:
    content = content.replace(old_subscription_end, new_subscription_end)
    print("✅ Добавлена инициализация каналов в мониторинг")
else:
    print("❌ Конец подписки не найден")

# Сохраняем файл
with open('telegram-worker/multithreaded_worker.py', 'w', encoding='utf-8') as f:
    f.write(content)

print("✅ Файл multithreaded_worker.py обновлен")
