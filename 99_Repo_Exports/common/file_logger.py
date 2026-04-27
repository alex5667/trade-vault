# -*- coding: utf-8 -*-
"""
File Logger - Логирование в файлы с ротацией (абсолютно идентично trade_back)

Особенности:
- Запись в папку logs/ в корне проекта
- Ротация по размеру файла
- Ограничение общего размера логов
- Форматирование идентично консольному выводу
- Асинхронная запись (не блокирует выполнение)
- Автоматическое создание директории
- Файлы с датой/временем для warn и error

Файлы (как в trade_back):
- app.txt - все логи (как в консоли)
- log.txt - обычные логи (log, warn, debug, verbose)
- error.txt - только ошибки (error)
- warn_YYYY-MM-DD_HH-MM-SS.txt - предупреждения с датой и временем
- error_YYYY-MM-DD_HH-MM-SS.txt - ошибки с датой и временем
"""

import os
import sys
import logging
import logging.handlers
from pathlib import Path
from datetime import datetime
from typing import Optional
import json

# Попытка импортировать zoneinfo (Python 3.9+) или pytz для работы с временными зонами
try:
    from zoneinfo import ZoneInfo
    HAS_ZONEINFO = True
except ImportError:
    try:
        import pytz
        HAS_PYTZ = True
        HAS_ZONEINFO = False
    except ImportError:
        HAS_PYTZ = False
        HAS_ZONEINFO = False

# Константы (абсолютно идентично trade_back)
BYTES_IN_KILOBYTE = 1024
BYTES_IN_MEGABYTE = BYTES_IN_KILOBYTE * 1024
DEFAULT_LOG_LEVEL = "INFO"
DEFAULT_LOG_FILE_SIZE_KB = 18
DEFAULT_LOG_FILES_TOTAL_SIZE_MB = 512


class FileLoggerHandler(logging.Handler):
    """
    Кастомный handler для записи логов в файлы с ротацией.
    Абсолютно идентично MyLogger из trade_back.
    """
    
    def __init__(
        self,
        logs_dir: str,
        log_file_size_kb: int = DEFAULT_LOG_FILE_SIZE_KB,
        logs_total_size_mb: int = DEFAULT_LOG_FILES_TOTAL_SIZE_MB,
    ):
        super().__init__()
        self.logs_dir = Path(logs_dir)
        self.log_file_size = log_file_size_kb * BYTES_IN_KILOBYTE
        self.logs_total_size_limit = logs_total_size_mb * BYTES_IN_MEGABYTE
        
        # Расширение файлов (как в trade_back)
        self.file_extension = '.txt'
        
        # Создаем директорию для логов
        self.logs_dir.mkdir(parents=True, exist_ok=True)
        
        # Имена файлов (абсолютно идентично trade_back)
        self.app_log_file = self.logs_dir / "app.txt"
        self.log_file = self.logs_dir / "log.txt"
        self.error_file = self.logs_dir / "error.txt"
        
        # Файлы с датой/временем (обновляются при необходимости)
        self.warn_date_file: Optional[Path] = None
        self.error_date_file: Optional[Path] = None
        
        # Инициализируем файлы с датой и временем
        self._initialize_date_time_files()
        
        # Очищаем существующие пустые файлы warn и error
        self._cleanup_existing_empty_files()
        
        # Тестовая запись при старте
        self._test_write()
    
    def _get_kyiv_datetime(self) -> str:
        """
        Получение текущей даты и времени по Киеву в формате YYYY-MM-DD_HH-MM-SS
        Абсолютно идентично trade_back (использует Europe/Kyiv с правильной обработкой DST)
        """
        now = datetime.now()
        
        # Используем zoneinfo (Python 3.9+) или pytz для правильной обработки временной зоны
        if HAS_ZONEINFO:
            kyiv_tz = ZoneInfo('Europe/Kyiv')
            kyiv_time = now.astimezone(kyiv_tz)
        elif HAS_PYTZ:
            kyiv_tz = pytz.timezone('Europe/Kyiv')
            kyiv_time = now.astimezone(kyiv_tz)
        else:
            # Fallback: используем UTC+2 (приблизительно)
            from datetime import timedelta
            kyiv_offset = timedelta(hours=2)
            kyiv_time = now + kyiv_offset
        
        return kyiv_time.strftime("%Y-%m-%d_%H-%M-%S")
    
    def _get_date_str(self) -> str:
        """Получение даты в формате YYYY-MM-DD из киевского времени"""
        kyiv_datetime = self._get_kyiv_datetime()
        return kyiv_datetime.split('_')[0]  # YYYY-MM-DD
    
    def _initialize_date_time_files(self):
        """Инициализация имен файлов для warn и error с датой и временем (как в trade_back)"""
        date_time = self._get_kyiv_datetime()
        self.warn_date_file = self.logs_dir / f"warn_{date_time}.txt"
        self.error_date_file = self.logs_dir / f"error_{date_time}.txt"
    
    def _update_date_time_files(self, level: str):
        """
        Обновление имени файла лога с датой и временем при необходимости
        Создает новый файл каждый день (как в trade_back)
        
        Args:
            level: Уровень логирования ('warn' или 'error')
        """
        current_date_time = self._get_kyiv_datetime()
        current_date = current_date_time.split('_')[0]  # YYYY-MM-DD
        
        if level == 'warn':
            if not self.warn_date_file:
                self.warn_date_file = self.logs_dir / f"warn_{current_date_time}.txt"
                return
            
            # Извлекаем дату из имени файла: warn_YYYY-MM-DD_HH-MM-SS -> YYYY-MM-DD
            warn_file_name = self.warn_date_file.name
            if warn_file_name.startswith('warn_'):
                parts = warn_file_name.replace('.txt', '').split('_')
                if len(parts) >= 2:
                    current_warn_date = parts[1]  # YYYY-MM-DD
                    if current_warn_date != current_date:
                        self.warn_date_file = self.logs_dir / f"warn_{current_date_time}.txt"
                else:
                    self.warn_date_file = self.logs_dir / f"warn_{current_date_time}.txt"
            else:
                self.warn_date_file = self.logs_dir / f"warn_{current_date_time}.txt"
        
        elif level == 'error':
            if not self.error_date_file:
                self.error_date_file = self.logs_dir / f"error_{current_date_time}.txt"
                return
            
            # Извлекаем дату из имени файла: error_YYYY-MM-DD_HH-MM-SS -> YYYY-MM-DD
            error_file_name = self.error_date_file.name
            if error_file_name.startswith('error_'):
                parts = error_file_name.replace('.txt', '').split('_')
                if len(parts) >= 2:
                    current_error_date = parts[1]  # YYYY-MM-DD
                    if current_error_date != current_date:
                        self.error_date_file = self.logs_dir / f"error_{current_date_time}.txt"
                else:
                    self.error_date_file = self.logs_dir / f"error_{current_date_time}.txt"
            else:
                self.error_date_file = self.logs_dir / f"error_{current_date_time}.txt"
            
            # Дополнительная проверка: если имя все еще пустое, устанавливаем его
            if not self.error_date_file:
                self.error_date_file = self.logs_dir / f"error_{current_date_time}.txt"
    
    def _cleanup_existing_empty_files(self):
        """
        Удаляет существующие пустые файлы warn_*.txt и error_*.txt
        Это предотвращает накопление пустых файлов
        """
        try:
            # Ищем все файлы warn_*.txt и error_*.txt в директории логов
            for entry in self.logs_dir.iterdir():
                if not entry.is_file():
                    continue
                
                name = entry.name
                # Проверяем, является ли файл warn или error файлом с датой
                if (name.startswith("warn_") or name.startswith("error_")) and name.endswith(".txt"):
                    # Проверяем размер файла
                    try:
                        if entry.stat().st_size == 0:
                            # Файл пустой, удаляем его
                            entry.unlink()
                    except (OSError, FileNotFoundError):
                        # Игнорируем ошибки при удалении
                        pass
        except Exception:
            # Игнорируем ошибки, чтобы не ломать инициализацию
            pass
    
    def _test_write(self):
        """Тестовая запись при старте для проверки работоспособности (как в trade_back)"""
        try:
            test_msg = f"🚀 Приложение запущено: {datetime.now().isoformat()}"
            self._write_to_file(self.app_log_file, test_msg)
            print(f"✅ Тестовая запись в лог успешна: {self.app_log_file}")
        except Exception as e:
            print(f"❌ Ошибка тестовой записи в лог: {e}")
            print(f"   Путь: {self.app_log_file}")
            print(f"   Директория существует: {self.logs_dir.exists()}")
    
    def _write_to_file(self, file_path: Path, message: str):
        """
        Запись сообщения в файл с проверкой ротации (как в trade_back)
        """
        try:
            # Проверяем, что имя файла не пустое
            if not file_path or not file_path.name or file_path.name.strip() == '':
                print(f"❌ Попытка записи в файл с пустым именем. Сообщение: {message}", file=sys.stderr)
                return
            
            # Убеждаемся, что директория существует
            self.logs_dir.mkdir(parents=True, exist_ok=True)
            
            # Проверяем ротацию файла (не блокируем запись при ошибке)
            try:
                self._rotate_if_needed(file_path)
            except Exception:
                # Игнорируем ошибки ротации для первой записи
                pass
            
            # Записываем в файл
            with open(file_path, "a", encoding="utf-8") as f:
                f.write(message + "\n")
            
            # Очищаем старые логи при необходимости
            try:
                self._trim_logs_directory()
            except Exception:
                # Игнорируем ошибки очистки
                pass
        except Exception as e:
            # Не блокируем выполнение при ошибках записи
            error_msg = str(e)
            print(f"❌ Ошибка записи в файл логов ({file_path.name}): {error_msg}", file=sys.stderr)
            print(f"   Путь: {file_path}", file=sys.stderr)
    
    def _rotate_if_needed(self, file_path: Path):
        """
        Ротация файла при превышении размера (как в trade_back)
        Формат нового имени: fileName-YYYY-MM-DD_HH-MM-SS.txt
        """
        try:
            if not file_path.exists():
                return
            
            size = file_path.stat().st_size
            if size > self.log_file_size:
                # Создаем новое имя с датой/временем по Киеву (как в trade_back)
                date_time = self._get_kyiv_datetime()
                new_name = file_path.stem + f"-{date_time}" + self.file_extension
                new_path = file_path.parent / new_name
                
                # Копируем файл
                import shutil
                shutil.copy2(file_path, new_path)
                
                # Очищаем текущий файл
                file_path.write_text("", encoding="utf-8")
                
                # Запускаем очистку старых логов
                self._trim_logs_directory()
        except FileNotFoundError:
            # Если файла нет (первая запись) — это нормально
            pass
        except Exception as e:
            # Логируем другие ошибки, но не пробрасываем
            print(f"⚠️ Ошибка ротации файла {file_path.name}: {e}", file=sys.stderr)
    
    def _trim_logs_directory(self):
        """
        Ограничивает суммарный размер директории логов, удаляя старые файлы (как в trade_back)
        """
        try:
            total_size = 0
            files_info = []
            
            # Собираем информацию о файлах (только .txt файлы)
            for file_path in self.logs_dir.glob("*.txt"):
                if file_path.is_file():
                    try:
                        size = file_path.stat().st_size
                        mtime = file_path.stat().st_mtime
                        total_size += size
                        files_info.append((file_path, size, mtime))
                    except (OSError, FileNotFoundError):
                        # Игнорируем файлы, которые были удалены между проверками
                        continue
            
            # Если размер в пределах лимита, ничего не делаем
            if total_size <= self.logs_total_size_limit:
                return
            
            # Сортируем по времени модификации (старые первыми)
            files_info.sort(key=lambda x: x[2])
            
            # Удаляем старые файлы до достижения лимита
            for file_path, size, _ in files_info:
                if total_size <= self.logs_total_size_limit:
                    break
                try:
                    file_path.unlink()
                    total_size -= size
                except (OSError, FileNotFoundError):
                    # Игнорируем ошибки удаления
                    pass
        except Exception:
            # Игнорируем все ошибки при очистке
            pass
    
    def _create_log_message(self, record: logging.LogRecord) -> str:
        """
        Создание сообщения для записи в файл (как в trade_back)
        Формат: [Nest] PID  - DATE, TIME    LEVEL [Context] message params
        """
        now = datetime.now()
        pid = os.getpid()
        
        # Форматируем дату и время как в trade_back
        # Формат: MM/DD/YYYY, HH:MM:SS AM/PM
        date_str = now.strftime("%m/%d/%Y")
        time_str = now.strftime("%I:%M:%S %p")
        
        # Получаем уровень логирования
        level_name = record.levelname
        
        # Получаем контекст (имя логгера)
        context = record.name if record.name else ''
        context_str = f"[{context}]" if context else ''
        
        # Получаем сообщение и очищаем его от уже имеющихся дат/времени
        # (на случай, если сообщение уже содержит форматированную дату)
        message = record.getMessage()
        
        # Удаляем паттерны даты/времени из начала сообщения, если они есть
        # Паттерны: YYYY/MM/DD HH:MM:SS или MM/DD/YYYY, HH:MM:SS AM/PM
        import re
        # Удаляем дату/время в формате YYYY/MM/DD HH:MM:SS из начала строки (с любыми пробелами после)
        message = re.sub(r'^\d{4}/\d{2}/\d{2}\s+\d{2}:\d{2}:\d{2}\s+', '', message)
        # Удаляем дату/время в формате MM/DD/YYYY, HH:MM:SS AM/PM из начала строки (с любыми пробелами после)
        message = re.sub(r'^\d{2}/\d{2}/\d{4},\s+\d{2}:\d{2}:\d{2}\s+[AP]M\s+', '', message)
        # Удаляем дату/время в формате YYYY-MM-DD HH:MM:SS из начала строки
        message = re.sub(r'^\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}\s+', '', message)
        # Удаляем дату/время в формате YYYY-MM-DDTHH:MM:SS из начала строки (ISO format)
        message = re.sub(r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}[.\d]*\s+', '', message)
        
        # Формируем строку точно как в trade_back
        # Пример: [Nest] 7  - 11/12/2025, 2:30:21 PM    WARN [SymbolMetaService] ⚠️ Rate limit exceeded
        log_line = f"[Nest] {pid}  - {date_str}, {time_str}    {level_name} {context_str} {message}"
        
        # Добавляем дополнительные параметры (если есть)
        if record.args:
            try:
                args_str = ' '.join(str(arg) for arg in record.args)
                if args_str:
                    log_line += f" {args_str}"
            except Exception:
                pass
        
        return log_line
    
    def emit(self, record: logging.LogRecord):
        """
        Обработка записи лога (абсолютно идентично trade_back)
        """
        try:
            # Создаем сообщение в формате trade_back
            message = self._create_log_message(record)
            
            # Определяем уровень логирования
            level = record.levelno
            
            # Записываем в app.txt (все логи) - как в trade_back
            self._write_to_file(self.app_log_file, message)
            
            # Записываем в специфичные файлы
            if level >= logging.ERROR:
                # Обновляем имя файла error с датой и временем, если нужно
                self._update_date_time_files('error')
                # Гарантируем, что error_date_file установлен
                if not self.error_date_file:
                    date_time = self._get_kyiv_datetime()
                    self.error_date_file = self.logs_dir / f"error_{date_time}.txt"
                
                self._write_to_file(self.error_file, message)
                if self.error_date_file:
                    self._write_to_file(self.error_date_file, message)
            elif level >= logging.WARNING:
                # Обновляем имя файла warn с датой и временем, если нужно
                self._update_date_time_files('warn')
                
                self._write_to_file(self.log_file, message)
                if self.warn_date_file:
                    self._write_to_file(self.warn_date_file, message)
            else:
                # INFO, DEBUG, VERBOSE
                self._write_to_file(self.log_file, message)
        except Exception:
            # Игнорируем ошибки, чтобы не ломать работу приложения
            self.handleError(record)


def setup_file_logger(
    name: str,
    logs_dir: Optional[str] = None,
    log_level: Optional[str] = None,
    log_file_size_kb: Optional[int] = None,
    logs_total_size_mb: Optional[int] = None,
) -> logging.Logger:
    """
    Настройка логгера с записью в файлы (аналогично trade_back).
    
    Args:
        name: Имя логгера (обычно имя модуля)
        logs_dir: Путь к директории логов (по умолчанию logs/ в корне проекта)
        log_level: Уровень логирования (по умолчанию из LOG_LEVEL env)
        log_file_size_kb: Размер файла перед ротацией в KB (по умолчанию из LOG_FILE_SIZE env)
        logs_total_size_mb: Максимальный размер всех логов в MB (по умолчанию из LOG_FILES_TOTAL_SIZE_MB env)
    
    Returns:
        Настроенный logger с записью в файлы и консоль
    """
    # Определяем директорию логов
    if logs_dir is None:
        # Сначала проверяем переменную окружения LOG_DIR
        logs_dir = os.getenv("LOG_DIR")
        
        if logs_dir:
            # Используем путь из переменной окружения
            logs_dir = str(Path(logs_dir))
        else:
            # Проверяем, находимся ли мы в Docker контейнере (путь /app/logs)
            if Path("/app/logs").exists() or os.getenv("DOCKER_CONTAINER") == "true":
                logs_dir = "/app/logs"
            else:
                # Ищем корень проекта (где есть docker-compose.yml или README.md)
                project_root = Path.cwd()
                # Проверяем, есть ли docker-compose.yml или README.md в текущей директории
                if not (project_root / "docker-compose.yml").exists() and not (project_root / "README.md").exists():
                    # Пытаемся найти корень проекта
                    for parent in project_root.parents:
                        if (parent / "docker-compose.yml").exists() or (parent / "README.md").exists():
                            project_root = parent
                            break
                logs_dir = str(project_root / "logs")
    
    # Получаем настройки из env
    log_level = log_level or os.getenv("LOG_LEVEL", DEFAULT_LOG_LEVEL).upper()
    log_file_size_kb = log_file_size_kb or int(os.getenv("LOG_FILE_SIZE", str(DEFAULT_LOG_FILE_SIZE_KB)))
    logs_total_size_mb = logs_total_size_mb or int(os.getenv("LOG_FILES_TOTAL_SIZE_MB", str(DEFAULT_LOG_FILES_TOTAL_SIZE_MB)))
    
    # Создаем логгер
    logger = logging.getLogger(name)
    
    # Избегаем дублирования handlers
    if logger.handlers:
        return logger
    
    # Устанавливаем уровень
    logger.setLevel(getattr(logging, log_level, logging.INFO))
    
    # Handler для консоли (без форматирования, чтобы избежать дублирования)
    # Форматирование происходит в FileLoggerHandler для файлов
    console_handler = logging.StreamHandler(sys.stdout)
    # Используем простой формат для консоли
    console_formatter = logging.Formatter(
        fmt="%(levelname)-7s | %(name)s | %(message)s"
    )
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)
    
    # Handler для файлов (форматирование происходит внутри handler, как в trade_back)
    file_handler = FileLoggerHandler(
        logs_dir=logs_dir,
        log_file_size_kb=log_file_size_kb,
        logs_total_size_mb=logs_total_size_mb,
    )
    # Не устанавливаем formatter для file_handler, так как форматирование происходит в _create_log_message()
    logger.addHandler(file_handler)
    
    logger.propagate = False
    
    # Логируем путь к директории логов
    print(f"📁 Логи будут записываться в: {logs_dir}")
    
    return logger

