#!/usr/bin/env python3
"""
Главная точка входа телеграм‑воркера.

Назначение:
- Инициализировать настройки и Telethon‑клиент
- Обеспечить авторизацию (включая 2FA)
- Подписаться на входящие сообщения и публиковать два события:
  1) сырое сообщение в Redis Stream signal:telegram:raw
  2) распарсенное сообщение в Redis Stream signal:telegram:parsed
"""
import asyncio
import sys
import signal
from multithreaded_worker import MultithreadedTelegramWorker

async def main():
    """Главная функция."""
    print("🚀 Запуск многопоточного telegram-worker")
    
    # Создаем worker
    worker = MultithreadedTelegramWorker()
    
    # Настраиваем обработку сигналов
    def signal_handler(signum, frame):
        print(f"🛑 Получен сигнал {signum}, останавливаем worker...")
        worker.stop()
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        # Запускаем worker
        await worker.start()
    except KeyboardInterrupt:
        print("🛑 Получен Ctrl+C, останавливаем worker...")
        worker.stop()
    except Exception as e:
        print(f"❌ Ошибка в main: {e}")
        worker.stop()
        raise

if __name__ == "__main__":
    asyncio.run(main())
