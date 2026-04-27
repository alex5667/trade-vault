#!/usr/bin/env python3
"""
Добавляем мониторинг в процесс парсинга
"""

# Читаем файл
with open('telegram-worker/multithreaded_worker.py', 'r', encoding='utf-8') as f:
    content = f.read()

# Добавляем мониторинг после парсинга
old_parsing = '            # Если есть основные поля, считаем сигнал валидным\n            if has_direction and has_symbol:\n                self.logger.info(f"DEBUG: Сигнал считается валидным, публикуем в Redis.")\n                # Записываем в parsed stream\n                self.redis.xadd(self.settings.parsed_stream, parsed)'
new_parsing = '''            # Если есть основные поля, считаем сигнал валидным
            if has_direction and has_symbol:
                self.logger.info(f"DEBUG: Сигнал считается валидным, публикуем в Redis.")
                # Записываем в parsed stream
                self.redis.xadd(self.settings.parsed_stream, parsed)

                # Отправляем алерт об успешном парсинге
                channel_name = message_data.get('username') or message_data.get('chat_title')
                if channel_name:
                    self.alert_system.send_alert(
                        message=f"Успешно распарсен сигнал {parsed.get('symbol')} {parsed.get('direction')} от {channel_name}",
                        alert_type="success",
                        channel=channel_name,
                        data={"symbol": parsed.get('symbol'), "direction": parsed.get('direction')},
                        send_telegram=False  # Не спамим в Telegram
                    )'''

if old_parsing in content:
    content = content.replace(old_parsing, new_parsing)
    print("✅ Добавлен мониторинг в процесс парсинга")
else:
    print("❌ Процесс парсинга не найден")

# Добавляем мониторинг ошибок парсинга
old_error_handling = '            else:\n                self.logger.debug(f"Сообщение от {message_data[\'username\']} не является валидным сигналом (отсутствуют direction или symbol)")'
new_error_handling = '''            else:
                self.logger.debug(f"Сообщение от {message_data['username']} не является валидным сигналом (отсутствуют direction или symbol)")

                # Отправляем алерт о невалидном сигнале
                channel_name = message_data.get('username') or message_data.get('chat_title')
                if channel_name:
                    self.alert_system.send_alert(
                        message=f"Невалидный сигнал от {channel_name}: отсутствуют direction или symbol",
                        alert_type="warning",
                        channel=channel_name,
                        data={"text": text_to_parse[:200]},
                        send_telegram=False
                    )'''

if old_error_handling in content:
    content = content.replace(old_error_handling, new_error_handling)
    print("✅ Добавлен мониторинг ошибок парсинга")
else:
    print("❌ Обработка ошибок не найдена")

# Сохраняем файл
with open('telegram-worker/multithreaded_worker.py', 'w', encoding='utf-8') as f:
    f.write(content)

print("✅ Файл multithreaded_worker.py обновлен")
