#!/usr/bin/env python3
"""
Добавляем дополнительные отладочные логи в multithreaded_worker.py
"""

# Читаем файл
with open('telegram-worker/multithreaded_worker.py', 'r', encoding='utf-8') as f:
    content = f.read()

# Добавляем логи в _handle_all_messages
old_log = 'self.logger.info(f"DEBUG: В _handle_all_messages получено сообщение от {username or chat_title}: {text[:100]}...")'
new_log = '''self.logger.info(f"📨 Получено сообщение от {username or chat_title}")
            self.logger.info(f"📝 Текст сообщения: {text[:200]}...")
            self.logger.info(f"🔍 Длина текста: {len(text)} символов")
            self.logger.info(f"⏰ Время сообщения: {msg.date}")
            self.logger.info(f"🆔 ID сообщения: {msg.id}")'''

if old_log in content:
    content = content.replace(old_log, new_log)
    print("✅ Обновлены логи в _handle_all_messages")

# Добавляем логи в process_message
old_process_log = 'self.logger.info(f"DEBUG: Попытка поместить сообщение в очередь: {message_data[\'text\'][:100]}...")'
new_process_log = '''self.logger.info(f"📤 Отправляем сообщение в очередь для обработки")
            self.logger.info(f"📊 Данные сообщения: chat_id={message_data['chat_id']}, username={message_data['username']}")
            self.logger.info(f"📝 Текст для обработки: {message_data['text'][:100]}...")'''

if old_process_log in content:
    content = content.replace(old_process_log, new_process_log)
    print("✅ Обновлены логи в process_message")

# Добавляем логи в process_message_queue
process_queue_log = '''self.logger.info(f"🔄 Обрабатываем сообщение из очереди: {message_data['text'][:100]}...")
            self.logger.info(f"📊 Статистика очереди: {self.message_queue.qsize()} сообщений в очереди")
            self.logger.info(f"⏰ Время обработки: {time.strftime('%H:%M:%S')}")'''

# Ищем место для вставки логов в process_message_queue
if 'def process_message_queue(self):' in content:
    # Находим начало функции и добавляем логи
    lines = content.split('\n')
    new_lines = []
    in_process_queue = False

    for i, line in enumerate(lines):  # noqa: B007
        new_lines.append(line)
        if 'def process_message_queue(self):' in line:
            in_process_queue = True
        elif in_process_queue and line.strip() and not line.startswith('        ') and not line.startswith('    '):
            # Конец функции
            in_process_queue = False
        elif in_process_queue and 'message_data = self.message_queue.get_nowait()' in line:
            # Добавляем логи после получения сообщения
            new_lines.append('            ' + process_queue_log)

    content = '\n'.join(new_lines)
    print("✅ Добавлены логи в process_message_queue")

# Сохраняем файл
with open('telegram-worker/multithreaded_worker.py', 'w', encoding='utf-8') as f:
    f.write(content)

print("✅ Файл multithreaded_worker.py обновлен с дополнительными логами")
