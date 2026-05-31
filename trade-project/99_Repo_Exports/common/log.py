# common/log.py
"""
Unified logging setup for all scanner_infra modules.
Provides consistent log formatting across the system.

Поддерживает запись в файлы (абсолютно идентично trade_back):
- app.txt - все логи
- log.txt - обычные логи
- error.txt - ошибки
- warn_YYYY-MM-DD_HH-MM-SS.txt - предупреждения с датой и временем
- error_YYYY-MM-DD_HH-MM-SS.txt - ошибки с датой и временем
"""
import logging
import os
import sys

# Импортируем file_logger если доступен
try:
    from common.file_logger import setup_file_logger
    FILE_LOGGING_AVAILABLE = True
except ImportError:
    FILE_LOGGING_AVAILABLE = False


def setup_logger(name: str, use_file_logging: bool = None) -> logging.Logger:
    """
    Setup a logger with consistent formatting.
    
    Args:
        name: Logger name (usually module name)
        use_file_logging: Использовать запись в файлы (по умолчанию из USE_FILE_LOGGING env или True)
    
    Returns:
        Configured logger instance
    
    Environment:
        LOG_LEVEL: Logging level (DEBUG, INFO, WARNING, ERROR). Default: INFO
        USE_FILE_LOGGING: Использовать запись в файлы (true/false). Default: true
        LOG_FILE_SIZE: Размер файла перед ротацией в KB. Default: 18
        LOG_FILES_TOTAL_SIZE_MB: Максимальный размер всех логов в MB. Default: 512
    """
    # Определяем, использовать ли файловое логирование
    if use_file_logging is None:
        use_file_logging = os.getenv("USE_FILE_LOGGING", "true").lower() == "true"
    
    # Если доступно файловое логирование и оно включено, используем его
    if FILE_LOGGING_AVAILABLE and use_file_logging:
        return setup_file_logger(name)
    
    # Иначе используем простое консольное логирование
    level = os.getenv("LOG_LEVEL", "INFO").upper()
    logger = logging.getLogger(name)
    
    # Avoid adding duplicate handlers
    if logger.handlers:
        return logger
    
    logger.setLevel(getattr(logging, level, logging.INFO))
    handler = logging.StreamHandler(sys.stdout)
    fmt = "%(asctime)s | %(levelname)-7s | %(name)s | %(message)s"
    handler.setFormatter(logging.Formatter(fmt))
    logger.addHandler(handler)
    logger.propagate = False
    
    return logger

