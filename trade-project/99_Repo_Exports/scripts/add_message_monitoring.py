#!/usr/bin/env python3
"""
Добавляем мониторинг в обработчик сообщений
"""

# Читаем файл
with open('telegram-worker/multithreaded_worker.py', 'r', encoding='utf-8') as f:
    content = f.read()

# Добавляем мониторинг после получения сообщения
old_message_handling = '            text = msg.message or msg.raw_text or ""\n            self.logger.info(f"📨 Получено сообщение от {username or chat_title}")\n            self.logger.info(f"📝 Текст сообщения: {text[:200]}...")\n            self.logger.info(f"🔍 Длина текста: {len(text)} символов")\n            self.logger.info(f"⏰ Время сообщения: {msg.date}")\n            self.logger.info(f"🆔 ID сообщения: {msg.id}")'
new_message_handling = '''            text = msg.message or msg.raw_text or ""
            self.logger.info(f"📨 Получено сообщение от {username or chat_title}")
            self.logger.info(f"📝 Текст сообщения: {text[:200]}...")
            self.logger.info(f"🔍 Длина текста: {len(text)} символов")
            self.logger.info(f"⏰ Время сообщения: {msg.date}")
            self.logger.info(f"🆔 ID сообщения: {msg.id}")

            # Обновляем мониторинг канала
            channel_name = username or chat_title
            if channel_name:
                self.channel_monitor.update_channel_activity(channel_name, msg.date.timestamp())'''

if old_message_handling in content:
    content = content.replace(old_message_handling, new_message_handling)
    print("✅ Добавлен мониторинг в обработчик сообщений")
else:
    print("❌ Обработчик сообщений не найден")

# Сохраняем файл
with open('telegram-worker/multithreaded_worker.py', 'w', encoding='utf-8') as f:
    f.write(content)

print("✅ Файл multithreaded_worker.py обновлен")
