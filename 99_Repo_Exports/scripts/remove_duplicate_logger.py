#!/usr/bin/env python3
"""
Удаляем дублирующий logger
"""

# Читаем файл
with open('telegram-worker/multithreaded_worker.py', 'r', encoding='utf-8') as f:
    content = f.read()

# Удаляем дублирующий logger из setup_logging
old_setup_logging = '''    def setup_logging(self):
        """Настраивает логирование."""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(threadName)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('multithreaded_worker.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)'''
new_setup_logging = '''    def setup_logging(self):
        """Настраивает логирование."""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(threadName)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('multithreaded_worker.log'),
                logging.StreamHandler()
            ]
        )
        # self.logger уже создан в __init__'''

if old_setup_logging in content:
    content = content.replace(old_setup_logging, new_setup_logging)
    print("✅ Удален дублирующий logger из setup_logging")
else:
    print("❌ setup_logging не найден")

# Сохраняем файл
with open('telegram-worker/multithreaded_worker.py', 'w', encoding='utf-8') as f:
    f.write(content)

print("✅ Файл multithreaded_worker.py исправлен")
