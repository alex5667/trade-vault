#!/usr/bin/env python3
"""
Добавляем детальные логи в функцию подписки
"""

# Читаем файл
with open('telegram-worker/multithreaded_worker.py', 'r', encoding='utf-8') as f:
    content = f.read()

# Добавляем логи в функцию subscribe_to_channels
old_code = '''    async def subscribe_to_channels(self) -> List[str]:
        """Подписывается на активные каналы."""
        try:
            self.logger.info("📡 Подписка на активные каналы...")

            # Получаем список активных каналов
            active_channels = self.status_checker.get_active_channels()

            if not active_channels:
                self.logger.warning("⚠️ Нет активных каналов для подписки")
                return []'''

new_code = '''    async def subscribe_to_channels(self) -> List[str]:
        """Подписывается на активные каналы."""
        try:
            self.logger.info("📡 Подписка на активные каналы...")

            # Получаем список активных каналов
            self.logger.info("🔍 Получаем список активных каналов...")
            active_channels = self.status_checker.get_active_channels()
            self.logger.info(f"✅ Получено {len(active_channels)} активных каналов: {active_channels[:5]}...")

            if not active_channels:
                self.logger.warning("⚠️ Нет активных каналов для подписки")
                return []'''

if old_code in content:
    content = content.replace(old_code, new_code)
    print("✅ Добавлены детальные логи в subscribe_to_channels")
else:
    print("❌ subscribe_to_channels не найден")

# Сохраняем файл
with open('telegram-worker/multithreaded_worker.py', 'w', encoding='utf-8') as f:
    f.write(content)

print("✅ Файл multithreaded_worker.py обновлен")
