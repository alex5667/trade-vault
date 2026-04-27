"""
Конфигурация telegram-worker.

Назначение:
- Загрузка настроек из переменных окружения
- Конфигурация Redis, Telegram API и других параметров
- Настройки многопоточного режима
"""

import os
from typing import Optional
from dotenv import load_dotenv

# Загружаем переменные окружения из .env файла
load_dotenv()


class Settings:
    """Настройки приложения."""
    
    # Telegram API
    api_id: int = int(os.getenv("TG_API_ID", "0"))
    api_hash: str = os.getenv("TG_API_HASH", "")
    phone: Optional[str] = os.getenv("TG_PHONE")
    code: Optional[str] = os.getenv("TG_CODE")
    password: Optional[str] = os.getenv("TG_PASSWORD")
    
    # Redis
    redis_url: str = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    stream_maxlen: int = int(os.getenv("STREAM_MAXLEN", "1000"))
    
    # Каналы
    channels_redis_key: str = os.getenv("CHANNELS_REDIS_KEY", "telegram:channels:usernames")
    channels_refresh_sec: int = int(os.getenv("CHANNELS_REFRESH_SEC", "300"))  # 5 минут
    
    # Сессии
    sessions_dir: str = os.getenv("SESSIONS_DIR", "./sessions")
    session_name: str = os.getenv("SESSION_NAME", "telegram_worker")
    
    # Потоки
    max_threads: int = int(os.getenv("MAX_THREADS", "4"))
    channels_per_thread: int = int(os.getenv("CHANNELS_PER_THREAD", "10"))
    
    # Потоки Redis
    raw_stream: str = os.getenv("RAW_STREAM", "signal:telegram:raw")
    parsed_stream: str = os.getenv("PARSED_STREAM", "signal:telegram:parsed")


def load_settings() -> Settings:
    """
    Загружает настройки из переменных окружения.
    
    Возвращает:
        Settings: объект с настройками
    """
    return Settings()