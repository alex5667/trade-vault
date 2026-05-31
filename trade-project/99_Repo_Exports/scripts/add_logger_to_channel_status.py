#!/usr/bin/env python3
"""
Добавляем logger в ChannelStatusChecker
"""

# Читаем файл
with open('telegram-worker/app/channel_status.py', 'r', encoding='utf-8') as f:
    content = f.read()

# Добавляем logger в конструктор
old_constructor = '''    def __init__(self, redis_client: redis.Redis):
        """
        Инициализирует проверщик статусов каналов.

        Аргументы:
            redis_client: Redis клиент для проверки статусов
        """
        self.redis_client = redis_client
        self.status_key_prefix = "telegram:channel:"
        self.status_key_suffix = ":status"'''

new_constructor = '''    def __init__(self, redis_client: redis.Redis, logger=None):
        """
        Инициализирует проверщик статусов каналов.

        Аргументы:
            redis_client: Redis клиент для проверки статусов
            logger: Логгер для вывода сообщений
        """
        self.redis_client = redis_client
        self.logger = logger or logging.getLogger(__name__)
        self.status_key_prefix = "telegram:channel:"
        self.status_key_suffix = ":status"'''

if old_constructor in content:
    content = content.replace(old_constructor, new_constructor)
    print("✅ Добавлен logger в конструктор ChannelStatusChecker")
else:
    print("❌ Конструктор ChannelStatusChecker не найден")

# Добавляем импорт logging
if "import logging" not in content:
    content = "import logging\n" + content
    print("✅ Добавлен импорт logging")

# Сохраняем файл
with open('telegram-worker/app/channel_status.py', 'w', encoding='utf-8') as f:
    f.write(content)

print("✅ Файл channel_status.py исправлен")
