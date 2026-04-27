#!/usr/bin/env python3
"""
Добавляем логи в функцию start
"""

# Читаем файл
with open('telegram-worker/multithreaded_worker.py', 'r', encoding='utf-8') as f:
    content = f.read()

# Добавляем логи в функцию start
old_code = '''    async def start(self):
        """Запускает worker."""
        try:
            self.logger.info("🚀 Запуск многопоточного telegram-worker")

            # Инициализируем основной клиент
            if not await self.initialize_main_client():
                return False

            # Подписываемся на каналы
            active_channels = await self.subscribe_to_channels()
            if not active_channels:
                self.logger.warning("⚠️ Нет активных каналов для подписки")
                return False'''

new_code = '''    async def start(self):
        """Запускает worker."""
        try:
            self.logger.info("🚀 Запуск многопоточного telegram-worker")

            # Инициализируем основной клиент
            self.logger.info("🔐 Начинаем инициализацию основного клиента...")
            if not await self.initialize_main_client():
                self.logger.error("❌ Ошибка инициализации основного клиента")
                return False
            self.logger.info("✅ Основной клиент инициализирован")

            # Подписываемся на каналы
            self.logger.info("📡 Начинаем подписку на каналы...")
            active_channels = await self.subscribe_to_channels()
            self.logger.info(f"📊 Результат подписки: {len(active_channels) if active_channels else 0} каналов")
            if not active_channels:
                self.logger.warning("⚠️ Нет активных каналов для подписки")
                return False'''

if old_code in content:
    content = content.replace(old_code, new_code)
    print("✅ Добавлены логи в функцию start")
else:
    print("❌ Функция start не найдена")

# Сохраняем файл
with open('telegram-worker/multithreaded_worker.py', 'w', encoding='utf-8') as f:
    f.write(content)

print("✅ Файл multithreaded_worker.py обновлен")
