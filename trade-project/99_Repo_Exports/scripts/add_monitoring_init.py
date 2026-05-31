#!/usr/bin/env python3
"""
Добавляем инициализацию мониторинга в конструктор TelegramWorker
"""

# Читаем файл
with open('telegram-worker/multithreaded_worker.py', 'r', encoding='utf-8') as f:
    content = f.read()

# Добавляем инициализацию мониторинга после инициализации status_checker
old_init = '        self.status_checker = ChannelStatusChecker(self.redis)'
new_init = '''        self.status_checker = ChannelStatusChecker(self.redis)

        # Инициализация мониторинга каналов
        self.channel_monitor = ChannelMonitor(self.redis, self.logger)

        # Инициализация системы алертов
        bot_token = os.getenv('TELEGRAM_BOT_TOKEN')
        chat_ids = os.getenv('TELEGRAM_NOTIFY_CHAT_IDS', '').split(',')
        chat_ids = [cid.strip() for cid in chat_ids if cid.strip()]

        self.alert_system = AlertSystem(
            redis_client=self.redis,
            logger=self.logger,
            bot_token=bot_token,
            chat_ids=chat_ids
        )'''

if old_init in content:
    content = content.replace(old_init, new_init)
    print("✅ Добавлена инициализация мониторинга")
else:
    print("❌ Инициализация status_checker не найдена")

# Сохраняем файл
with open('telegram-worker/multithreaded_worker.py', 'w', encoding='utf-8') as f:
    f.write(content)

print("✅ Файл multithreaded_worker.py обновлен")
