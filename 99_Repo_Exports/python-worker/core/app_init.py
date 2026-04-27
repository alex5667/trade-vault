import time
import sys
from datetime import datetime, timezone
from common.log import setup_logger

# Инициализируем логгер для app_init
_logger = setup_logger("app_init")


def print_startup_banner() -> None:
    """Выводит стартовый баннер приложения"""
    _logger.info("=========================================")
    _logger.info("🛑 СТАРТ PYTHON WORKER")
    # ВАЖНО: Используем UTC время для согласованности
    utc_time = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')
    _logger.info(f"⏰ Время запуска: {utc_time}")
    _logger.info("=========================================")


def print_startup_message() -> None:
    """Выводит сообщение о запуске воркера"""
    _logger.info("🚀 Python worker запущен")


def print_shutdown_message() -> None:
    """Выводит сообщение о завершении работы воркера"""
    _logger.info("⛔ Python worker завершается...") 