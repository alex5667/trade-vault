from utils.time_utils import get_ny_time_millis

"""
Dynamic Symbol Manager - Динамическое управление торговыми символами через Redis

Функционал:
- Чтение списка символов из Redis stream
- Динамическое создание/удаление handlers
- Hot-reload без перезапуска контейнера
- Graceful shutdown для удаленных символов

Redis Stream: config:symbols
Format: {"action": "add|remove|set", "symbols": ["BTCUSD", ...]}
"""

import json
import threading
import time

import redis

from core.symbol_config import SymbolConfig, SymbolConfigFactory
from handlers.base_orderflow_handler import BaseOrderFlowHandler
from handlers.handler_factory import create_handler


class SymbolManager:
    """
    Менеджер для динамического управления символами и их обработчиками.
    
    Читает команды из Redis stream 'config:symbols' и динамически
    создает/удаляет handlers без перезапуска сервиса.
    """

    def __init__(
        self,
        redis_url: str = "redis://localhost:6379/0",
        config_stream: str = "config:symbols",
        initial_symbols: list[str] | None = None
    ):
        """
        Args:
            redis_url: URL для подключения к Redis
            config_stream: Redis stream для конфигурации символов
            initial_symbols: Начальный список символов (опционально)
        """
        self.redis_client = redis.from_url(
            redis_url,
            decode_responses=True,
            socket_keepalive=True,
            health_check_interval=30,
            socket_connect_timeout=5,
            retry_on_timeout=False
        )
        self.config_stream = config_stream

        # Активные handlers и их конфигурации
        self.handlers: dict[str, BaseOrderFlowHandler] = {}
        self.configs: dict[str, SymbolConfig] = {}  # Symbol configurations
        self.active_symbols: set[str] = set()

        # Управление жизненным циклом
        self.is_running = False
        self._watcher_thread: threading.Thread | None = None
        self._lock = threading.Lock()

        # Инициализация с начальными символами
        if initial_symbols:
            for symbol in initial_symbols:
                # Проверяем, есть ли сохраненная конфигурация в Redis
                saved_config = self._load_symbol_config(symbol)
                if saved_config and saved_config.is_custom:
                    # Используем кастомную конфигурацию из Redis
                    print(f"📥 Loading custom config for {symbol} from Redis")
                    self._add_symbol(symbol, saved_config)
                else:
                    # Используем defaults
                    print(f"📦 Using default config for {symbol}")
                    self._add_symbol(symbol)

        # Создаем consumer group для конфигурации
        try:
            self.redis_client.xgroup_create(
                self.config_stream,
                "symbol-manager",
                id='$',
                mkstream=True
            )
            print(f"✅ Consumer group 'symbol-manager' создана для {self.config_stream}")
        except Exception as e:
            if "BUSYGROUP" in str(e):
                print("ℹ️  Consumer group 'symbol-manager' уже существует")
            else:
                print(f"⚠️  Ошибка создания consumer group: {e}")

    def restore_state(self) -> None:
        """
        Восстанавливает состояние из сохраненных конфигураций в Redis.
        Загружает все символы, для которых есть сохраненные конфигурации.
        """
        print("🔄 Восстановление состояния из Redis...")

        try:
            # Use SMEMBERS on the config:symbols:all Set (maintained by _save_symbol_config)
            # instead of KEYS "config:symbol:*" which blocks the entire Redis instance
            # for O(keyspace) time.
            all_symbols = self.redis_client.smembers("config:symbols:all") or set()
            config_keys = [f"config:symbol:{s}" for s in all_symbols]
            restored_symbols = []

            for key in config_keys:
                # Извлекаем символ из ключа config:symbol:SYMBOL
                symbol = key.replace("config:symbol:", "")
                # Ignore history streams (shouldn't be in the Set, but guard anyway)
                if symbol.endswith(":history"):
                    continue

                if symbol:
                    # Загружаем конфигурацию
                    config = self._load_symbol_config(symbol)
                    if config:
                        # Активируем символ
                        self._add_symbol(symbol, config)
                        restored_symbols.append(symbol)
                        print(f"✅ Восстановлен символ: {symbol}")

            if restored_symbols:
                print(f"🎯 Восстановлено символов: {len(restored_symbols)} - {', '.join(sorted(restored_symbols))}")
            else:
                print("ℹ️  Нет сохраненных конфигураций для восстановления")

        except Exception as e:
            print(f"❌ Ошибка при восстановлении состояния: {e}")

    def start(self) -> None:
        """Запускает watcher для мониторинга конфигурации символов"""
        if self.is_running:
            print("⚠️  SymbolManager уже запущен")
            return

        self.is_running = True

        # Всегда пытаемся восстановить состояние из Redis (для дополнительных символов)
        # Это безопасно, так как initial_symbols имеют приоритет
        self.restore_state()

        # Запускаем все активные handlers
        with self._lock:
            for symbol, handler in self.handlers.items():
                if not handler.is_running:
                    handler.start()

        # Запускаем watcher в отдельном потоке
        self._watcher_thread = threading.Thread(
            target=self._watch_config_stream,
            daemon=True,
            name="symbol-config-watcher"
        )
        self._watcher_thread.start()

        print(f"🚀 SymbolManager запущен (active symbols: {sorted(self.active_symbols)})")

    def stop(self) -> None:
        """Останавливает все handlers и watcher"""
        print("⛔ Остановка SymbolManager...")
        self.is_running = False

        # Останавливаем все handlers
        with self._lock:
            for symbol, handler in list(self.handlers.items()):
                self._remove_symbol(symbol)

        print("✅ SymbolManager остановлен")

    def _watch_config_stream(self) -> None:
        """
        Мониторит Redis stream для изменений конфигурации символов.
        
        Формат команды:
        {
            "action": "add" | "remove" | "set",
            "symbols": ["BTCUSD", ...]
        }
        """
        consumer_name = f"manager-{int(time.time())}"

        print(f"🔄 Запуск watcher для {self.config_stream}...")

        while self.is_running:
            try:
                # Читаем из stream
                messages = self.redis_client.xreadgroup(
                    "symbol-manager",
                    consumer_name,
                    {self.config_stream: '>'},
                    count=10,
                    block=1000  # 1 секунда
                )

                if not messages:
                    continue

                for stream, items in messages:
                    for msg_id, fields in items:
                        try:
                            # Парсим команду
                            data = json.loads(fields.get("data", "{}"))
                            action = data.get("action", "set")
                            symbols = data.get("symbols", [])

                            # Выполняем команду
                            self._handle_config_command(action, symbols)

                            # ACK сообщение
                            self.redis_client.xack(stream, "symbol-manager", msg_id)

                        except Exception as e:
                            print(f"❌ Ошибка обработки config message {msg_id}: {e}")

            except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError):
                print("⚠️  Redis connection lost in config watcher. Reconnecting...")
                time.sleep(3)
            except Exception as e:
                if "NOGROUP" in str(e):
                    try:
                        self.redis_client.xgroup_create(
                            self.config_stream,
                            "symbol-manager",
                            id='$',
                            mkstream=True
                        )
                        print(f"✅ Consumer group 'symbol-manager' восстановлена для {self.config_stream}")
                    except Exception as create_err:
                        if "BUSYGROUP" not in str(create_err):
                            print(f"❌ Ошибка восстановления consumer group: {create_err}")
                else:
                    print(f"❌ Ошибка в config watcher: {e}")
                time.sleep(1)

    def _handle_config_command(self, action: str, symbols: list[str] | list[dict]) -> None:
        """
        Обрабатывает команду изменения конфигурации.
        
        Args:
            action: Действие (add, remove, set, update_config)
            symbols: Список символов (str) или список {symbol, config} (dict)
        """
        print(f"📝 Config command: {action} {symbols}")

        if action == "add":
            # Добавить символы (с конфигурацией или без)
            for item in symbols:
                if isinstance(item, dict):
                    # Формат: {"symbol": "BTCUSD", "config": {...}}
                    symbol = item['symbol']
                    config_data = item.get('config')

                    # Сначала проверяем Redis на наличие сохраненной конфигурации
                    saved_config = self._load_symbol_config(symbol)

                    if config_data:
                        # Передана конфигурация → создаем и помечаем как custom
                        config = SymbolConfigFactory.create_from_symbol(
                            symbol,
                            custom_params=config_data
                        )
                        config.is_custom = True  # Помечаем как кастомная
                        config.updated_at = get_ny_time_millis()
                    elif saved_config and saved_config.is_custom:
                        # Есть сохраненная кастомная конфигурация в Redis
                        print(f"📥 Using saved custom config for {symbol}")
                        config = saved_config
                    else:
                        # Используем defaults
                        config = SymbolConfigFactory.create_from_symbol(symbol)
                        config.is_custom = False
                        config.created_at = get_ny_time_millis()

                    self._add_symbol(symbol, config)
                else:
                    # Старый формат: просто строка "BTCUSD"
                    # Проверяем сохраненную конфигурацию
                    saved_config = self._load_symbol_config(item)
                    if saved_config and saved_config.is_custom:
                        print(f"📥 Using saved custom config for {item}")
                        self._add_symbol(item, saved_config)
                    else:
                        config = SymbolConfigFactory.create_from_symbol(item)
                        config.is_custom = False
                        config.created_at = get_ny_time_millis()
                        self._add_symbol(item, config)

        elif action == "remove":
            # Удалить символы
            for item in symbols:
                symbol = item if isinstance(item, str) else item['symbol']
                self._remove_symbol(symbol)

        elif action == "set":
            # Установить список (удалить все кроме указанных, добавить новые)
            # Извлекаем названия символов
            if symbols and isinstance(symbols[0], dict):
                new_symbols = set(item['symbol'] for item in symbols)
            else:
                new_symbols = set(symbols)

            current_symbols = set(self.active_symbols)

            # Удалить те, которых нет в новом списке
            to_remove = current_symbols - new_symbols
            for symbol in to_remove:
                self._remove_symbol(symbol)

            # Добавить новые
            to_add = new_symbols - current_symbols
            for item in symbols:
                if isinstance(item, dict):
                    symbol = item['symbol']
                    if symbol in to_add:
                        config_data = item.get('config')
                        config = SymbolConfigFactory.create_from_symbol(
                            symbol,
                            custom_params=config_data
                        ) if config_data else SymbolConfigFactory.create_from_symbol(symbol)
                        self._add_symbol(symbol, config)
                else:
                    if item in to_add:
                        config = SymbolConfigFactory.create_from_symbol(item)
                        self._add_symbol(item, config)

        elif action == "update_config":
            # Обновить конфигурацию существующего символа (вручную)
            symbol = symbols if isinstance(symbols, str) else symbols.get('symbol')
            config_data = symbols.get('config') if isinstance(symbols, dict) else None

            if symbol in self.active_symbols and config_data:
                # Обновляем конфигурацию
                current_config = self.configs.get(symbol)
                if current_config:
                    updated_config = SymbolConfigFactory.create_from_symbol(
                        symbol,
                        custom_params=config_data
                    )

                    # ВАЖНО: Помечаем как custom (изменена вручную)
                    updated_config.is_custom = True
                    updated_config.updated_at = get_ny_time_millis()

                    self.configs[symbol] = updated_config

                    # Пересоздаем handler с новой конфигурацией
                    self._remove_symbol(symbol)
                    self._add_symbol(symbol, updated_config)

                    print(f"✅ Config updated for {symbol} (marked as CUSTOM)")

        elif action == "reset_config":
            # Сбросить конфигурацию к defaults (удалить кастомную)
            symbol = symbols if isinstance(symbols, str) else symbols.get('symbol')

            if symbol in self.active_symbols:
                # Удаляем кастомную конфигурацию из Redis
                self.redis_client.delete(f"config:symbol:{symbol}")

                # Создаем новую конфигурацию из defaults
                default_config = SymbolConfigFactory.create_from_symbol(symbol)
                default_config.is_custom = False
                default_config.created_at = get_ny_time_millis()

                # Пересоздаем handler
                self._remove_symbol(symbol)
                self._add_symbol(symbol, default_config)

                print(f"✅ Config reset to defaults for {symbol}")

        print(f"✅ Active symbols: {sorted(self.active_symbols)}")

    def _add_symbol(self, symbol: str, config: SymbolConfig | None = None) -> bool:
        """
        Добавляет новый символ и запускает handler с конфигурацией.
        
        Args:
            symbol: Символ для добавления
            config: Конфигурация символа (если None, создается из defaults)
            
        Returns:
            True если успешно добавлен, False иначе
        """
        with self._lock:
            if symbol in self.active_symbols:
                print(f"ℹ️  Symbol {symbol} already active")
                return False

            try:
                print(f"🔄 Adding symbol {symbol}...")

                # Создаем или используем переданную конфигурацию
                if config is None:
                    # Проверяем, есть ли сохраненная конфигурация в Redis
                    saved_config = self._load_symbol_config(symbol)
                    if saved_config and saved_config.is_custom:
                        # Используем сохраненную кастомную конфигурацию
                        config = saved_config
                        print(f"📥 Using saved CUSTOM config for {symbol}")
                    else:
                        # Используем defaults
                        config = SymbolConfigFactory.create_from_symbol(symbol)
                        config.is_custom = False
                        config.created_at = get_ny_time_millis()
                        print(f"📦 Using DEFAULT config for {symbol}")

                # Сохраняем конфигурацию
                self.configs[symbol] = config

                # Сохраняем в Redis для persistence
                self._save_symbol_config(symbol, config)

                # Конвертируем SymbolConfig в OrderFlowConfig из instrument_config.py
                # для использования в handlers
                try:
                    handler_config = config.to_instrument_config()
                except Exception as e:
                    print(f"⚠️  Failed to convert config for {symbol}: {e}, using default config")
                    handler_config = None

                # Создаем handler с конфигурацией
                handler = create_handler(symbol, handler_config)

                # Запускаем если manager запущен
                if self.is_running:
                    handler.start()

                # Сохраняем handler
                self.handlers[symbol] = handler
                self.active_symbols.add(symbol)

                config_type = "CUSTOM" if config.is_custom else "DEFAULT"
                print(f"✅ Symbol {symbol} added and started (config: {config.symbol_type.value}, type: {config_type})")
                return True

            except Exception as e:
                print(f"❌ Failed to add symbol {symbol}: {e}")
                return False

    def _remove_symbol(self, symbol: str) -> bool:
        """
        Удаляет символ и останавливает handler.
        
        Args:
            symbol: Символ для удаления
            
        Returns:
            True если успешно удален, False иначе
        """
        with self._lock:
            if symbol not in self.active_symbols:
                print(f"ℹ️  Symbol {symbol} not active")
                return False

            try:
                print(f"🔄 Removing symbol {symbol}...")

                # Останавливаем handler
                handler = self.handlers.get(symbol)
                if handler:
                    handler.stop()
                    del self.handlers[symbol]

                # Удаляем из активных и конфигурации
                self.active_symbols.remove(symbol)
                if symbol in self.configs:
                    del self.configs[symbol]

                # Удаляем из Redis
                self.redis_client.delete(f"config:symbol:{symbol}")

                print(f"✅ Symbol {symbol} removed and stopped")
                return True

            except Exception as e:
                print(f"❌ Failed to remove symbol {symbol}: {e}")
                return False

    def get_active_symbols(self) -> list[str]:
        """Возвращает список активных символов"""
        with self._lock:
            return sorted(self.active_symbols)

    def get_handler(self, symbol: str) -> BaseOrderFlowHandler | None:
        """
        Возвращает handler для символа.
        
        Args:
            symbol: Символ
            
        Returns:
            Handler или None если не найден
        """
        with self._lock:
            return self.handlers.get(symbol)

    def _load_symbol_config(self, symbol: str) -> SymbolConfig | None:
        """
        Загружает конфигурацию символа из Redis.
        
        Args:
            symbol: Название символа
            
        Returns:
            SymbolConfig если найдена в Redis, None иначе
        """
        try:
            config_json = self.redis_client.get(f"config:symbol:{symbol}")
            if config_json:
                config = SymbolConfig.from_json(config_json)
                return config
            return None
        except Exception as e:
            print(f"⚠️  Failed to load config for {symbol}: {e}")
            return None

    def _save_symbol_config(self, symbol: str, config: SymbolConfig) -> None:
        """
        Сохраняет конфигурацию символа в Redis.
        
        ВАЖНО: Сохраняет флаг is_custom для отслеживания ручных изменений.
        """
        try:
            # Сохраняем текущую конфигурацию
            self.redis_client.set(
                f"config:symbol:{symbol}",
                config.to_json(),
                ex=86400 * 30  # TTL 30 days
            )

            # Добавляем в set активных символов
            self.redis_client.sadd("config:symbols:all", symbol)

            # Сохраняем в history stream
            self.redis_client.xadd(
                f"config:symbol:{symbol}:history",
                {
                    'config': config.to_json(),
                    'is_custom': str(config.is_custom),
                    'ts': str(get_ny_time_millis())
                },
                maxlen=100
            )

            # Логируем тип конфигурации
            config_type = "CUSTOM" if config.is_custom else "DEFAULT"
            print(f"💾 Config saved for {symbol} (type: {config_type})")

        except Exception as e:
            print(f"⚠️  Failed to save config for {symbol}: {e}")

    def get_status(self) -> dict:
        """
        Возвращает статус всех handlers с их конфигурациями.
        
        Returns:
            Словарь {symbol: {is_running, processed_ticks, config, ...}}
        """
        status = {}

        with self._lock:
            for symbol, handler in self.handlers.items():
                config = self.configs.get(symbol)
                status[symbol] = {
                    "is_running": handler.is_running,
                    "processed_ticks": handler.processed_ticks,
                    "signal_count_long": handler.signal_count_long,
                    "signal_count_short": handler.signal_count_short,
                    "tick_stream": handler.tick_stream,
                    "book_stream": handler.book_stream,
                    "config_type": config.symbol_type.value if config else "unknown",
                    "config": config.to_dict() if config else None
                }

        return status


# ═════════════════════════════════════════════════════════════════════
# HELPER FUNCTIONS для публикации команд в Redis
# ═════════════════════════════════════════════════════════════════════

def publish_add_symbols(redis_client: redis.Redis, symbols: list[str], stream: str = "config:symbols") -> None:
    """
    Публикует команду добавления символов.
    
    Args:
        redis_client: Redis client
        symbols: Список символов для добавления
        stream: Redis stream (default: config:symbols)
    
    Examples:
        >>> publish_add_symbols(redis_client, ["BTCUSD", "ETHUSD"])
        # Handler для BTCUSD и ETHUSD будут созданы и запущены
    """
    command = {
        "action": "add",
        "symbols": symbols,
        "ts": get_ny_time_millis()
    }

    redis_client.xadd(stream, {"data": json.dumps(command)}, maxlen=50000, approximate=True)
    print(f"📤 Published: ADD {symbols}")


def publish_remove_symbols(redis_client: redis.Redis, symbols: list[str], stream: str = "config:symbols") -> None:
    """
    Публикует команду удаления символов.
    
    Args:
        redis_client: Redis client
        symbols: Список символов для удаления
        stream: Redis stream
    
    Examples:
        >>> publish_remove_symbols(redis_client, ["BTCUSD"])
        # Handler для BTCUSD будет остановлен и удален
    """
    command = {
        "action": "remove",
        "symbols": symbols,
        "ts": get_ny_time_millis()
    }

    redis_client.xadd(stream, {"data": json.dumps(command)}, maxlen=50000, approximate=True)
    print(f"📤 Published: REMOVE {symbols}")


def publish_set_symbols(redis_client: redis.Redis, symbols: list[str], stream: str = "config:symbols") -> None:
    """
    Публикует команду установки списка символов (заменяет текущий список).
    
    Args:
        redis_client: Redis client
        symbols: Полный список символов
        stream: Redis stream
    
    Examples:
        >>> publish_set_symbols(redis_client, ["BTCUSD", "ETHUSD"])
        # Все handlers кроме  BTCUSD, ETHUSD будут остановлены
        #  BTCUSD, ETHUSD будут запущены (если еще не запущены)
    """
    command = {
        "action": "set",
        "symbols": symbols,
        "ts": get_ny_time_millis()
    }

    redis_client.xadd(stream, {"data": json.dumps(command)}, maxlen=50000, approximate=True)
    print(f"📤 Published: SET {symbols}")


# ═════════════════════════════════════════════════════════════════════
# CLI UTILITY для управления символами
# ═════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    """CLI для управления символами"""
    import os
    import sys

    if len(sys.argv) < 2:
        print("Usage:")
        print("  python symbol_manager.py add BTCUSD ETHUSD")
        print("  python symbol_manager.py remove BTCUSD")
        print("  python symbol_manager.py set  BTCUSD ETHUSD")
        print("  python symbol_manager.py list")
        sys.exit(1)

    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    r = redis.from_url(redis_url, decode_responses=True)

    action = sys.argv[1]
    symbols = sys.argv[2:] if len(sys.argv) > 2 else []

    if action == "add":
        publish_add_symbols(r, symbols)
        print(f"✅ Command sent: ADD {symbols}")

    elif action == "remove":
        publish_remove_symbols(r, symbols)
        print(f"✅ Command sent: REMOVE {symbols}")

    elif action == "set":
        publish_set_symbols(r, symbols)
        print(f"✅ Command sent: SET {symbols}")

    elif action == "list":
        # Получить текущий список из Redis (если хранится)
        try:
            current = r.get("config:symbols:current")
            if current:
                symbols_list = json.loads(current)
                print(f"Current symbols: {symbols_list}")
            else:
                print("No symbols configured (check config:symbols stream)")
        except Exception as e:
            print(f"Error: {e}")

    else:
        print(f"Unknown action: {action}")
        sys.exit(1)

