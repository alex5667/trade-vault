#!/usr/bin/env python3
"""
Добавляем импорты для мониторинга в multithreaded_worker.py
"""

# Читаем файл
with open('telegram-worker/multithreaded_worker.py', 'r', encoding='utf-8') as f:
    content = f.read()

# Добавляем импорты после существующих импортов
old_imports = 'from app.config import load_settings\nfrom app.channel_status import ChannelStatusChecker\nfrom app.parse_utils import parse_signal\nimport redis'
new_imports = '''from app.config import load_settings
from app.channel_status import ChannelStatusChecker
from app.parse_utils import parse_signal
from app.channel_monitor import ChannelMonitor
from app.alert_system import AlertSystem
import redis'''

if old_imports in content:
    content = content.replace(old_imports, new_imports)
    print("✅ Добавлены импорты для мониторинга")
else:
    print("❌ Импорты не найдены")

# Сохраняем файл
with open('telegram-worker/multithreaded_worker.py', 'w', encoding='utf-8') as f:
    f.write(content)

print("✅ Файл multithreaded_worker.py обновлен")
