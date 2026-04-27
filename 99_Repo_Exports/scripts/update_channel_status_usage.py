#!/usr/bin/env python3
"""
Обновляем использование ChannelStatusChecker в multithreaded_worker.py
"""

# Читаем файл
with open('telegram-worker/multithreaded_worker.py', 'r', encoding='utf-8') as f:
    content = f.read()

# Обновляем создание ChannelStatusChecker
old_usage = '''        self.status_checker = ChannelStatusChecker(self.redis)'''
new_usage = '''        self.status_checker = ChannelStatusChecker(self.redis, self.logger)'''

if old_usage in content:
    content = content.replace(old_usage, new_usage)
    print("✅ Обновлено использование ChannelStatusChecker")
else:
    print("❌ ChannelStatusChecker не найден")

# Сохраняем файл
with open('telegram-worker/multithreaded_worker.py', 'w', encoding='utf-8') as f:
    f.write(content)

print("✅ Файл multithreaded_worker.py обновлен")
