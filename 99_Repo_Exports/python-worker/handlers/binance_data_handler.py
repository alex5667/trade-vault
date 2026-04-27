"""
BinanceDataHandler — потребляет рыночные данные из Redis Streams.

Назначение:
- Слушает служебные стримы (тикеры 24ч, funding rates, новые пары) из Redis.
- Делегирует обработку отдельным хендлерам.
- Не относится к kline-потоку (его читает KlineDataHandler).
"""

import json
import time
import threading
import sys
from typing import Callable

from core.redis_client import get_redis
from core.redis_keys import RS
from stream_consumer import StreamConsumer
from .ticker_handler import TickerDataHandler
from .funding_handler import FundingDataHandler
from .pairs_handler import PairsDataHandler
from core.config import BINANCE_STREAMS


class BinanceDataHandler:
    """
    Обработчик рыночных данных от Redis Streams (тикеры, funding, новые пары).

    Поток исполнения:
    - start() → _run_handler() в фоне
    - создаётся кастомный StreamConsumer, который вызывает специализированные обработчики
    """
    
    def __init__(self, ws_callback: Callable[[list], None]):
        """
        Args:
            ws_callback: Функция обратного вызова для обновления WebSocket-подписок
        """
        self.redis_client = get_redis()
        self.ws_callback = ws_callback
        self.is_running = False
        self.consumer = None
        self.thread = None
        
        # Список стримов берём из конфига
        self.streams = BINANCE_STREAMS
        
        # Инициализируем обработчики для разных типов данных
        self.ticker_handler = TickerDataHandler(self.redis_client, self.ws_callback)
        self.funding_handler = FundingDataHandler(self.redis_client)
        self.pairs_handler = PairsDataHandler(self.ws_callback)
        
    def start(self) -> None:
        """Запускает обработчик в отдельном потоке."""
        if self.is_running:
            print("⚠️ BinanceDataHandler уже запущен")
            return
            
        self.is_running = True
        self.thread = threading.Thread(target=self._run_handler, daemon=True)
        self.thread.start()
        print("🚀 BinanceDataHandler запущен с Redis Streams")
        sys.stdout.flush()
        
    def stop(self) -> None:
        """Останавливает обработчик и consumer (если существует)."""
        self.is_running = False
        if self.consumer:
            self.consumer.stop()
        print("⛔ BinanceDataHandler остановлен")
        sys.stdout.flush()
    
    def _run_handler(self) -> None:
        """Создаёт кастомный StreamConsumer и запускает потребление стримов."""
        try:
            # Создаем кастомный потребителя стримов
            self.consumer = BinanceStreamConsumer(
                ticker_handler=self.ticker_handler,
                funding_handler=self.funding_handler,
                pairs_handler=self.pairs_handler,
                redis_client=self.redis_client,
                streams=self.streams
            )
            
            print(f"🔄 BinanceDataHandler: Подключение к стримам: {', '.join(self.streams)}")
            sys.stdout.flush()
            
            # Запускаем потребление через кастомный метод start
            self.consumer.start()
            
        except Exception as e:
            print(f"❌ BinanceDataHandler: Ошибка в handler: {e}")
            sys.stdout.flush()


class BinanceStreamConsumer(StreamConsumer):
    """
    Кастомный потребитель стримов для Binance-данных (не kline).

    Переопределяет обработку сообщения, приводя поля в унифицированный вид.
    """
    
    def __init__(self, ticker_handler, funding_handler, pairs_handler, redis_client, streams: list):
        # Инициализируем базовый класс (consumer group зашита в базовом классе)
        super().__init__(consumer_group='binance-handler-group')
        
        self.ticker_handler = ticker_handler
        self.funding_handler = funding_handler
        self.pairs_handler = pairs_handler
        self.binance_redis_client = redis_client
        self.streams_to_consume = streams
        
    def start(self):
        """Запускает потребление стримов с кастомной логикой."""
        if self.running:
            print("⚠️ BinanceStreamConsumer уже запущен")
            return
            
        if not self.connect():
            print("❌ BinanceStreamConsumer: Не удалось подключиться к Redis")
            return
            
        # Создаем consumer groups для всех стримов
        self.utils.create_consumer_groups(
            self.redis_client,
            self.streams_to_consume,
            self.consumer_group,
        )
        
        self.running = True
        print(f"🚀 BinanceStreamConsumer запущен для стримов: {', '.join(self.streams_to_consume)}")
        sys.stdout.flush()
        
        # Запускаем потребление в отдельном потоке
        self.consumer_thread = threading.Thread(target=self._consume_streams_custom, daemon=True)
        self.consumer_thread.start()
        
        # Запускаем статистику в отдельном потоке
        self.stats_thread = threading.Thread(target=self._periodic_stats, daemon=True)
        self.stats_thread.start()
        
    def _consume_streams_custom(self):
        """Кастомное потребление стримов с вызовом process_stream_message."""
        while self.running:
            try:
                # Читаем сообщения из всех стримов
                messages = self.redis_client.xreadgroup(
                    groupname=self.consumer_group,
                    consumername=self.consumer_name,
                    streams={stream: '>' for stream in self.streams_to_consume},
                    count=10,  # Читаем по 10 сообщений за раз
                    block=1000  # Блокируемся на 1 секунду
                )
                
                if messages:
                    print(f"📨 BinanceStreamConsumer: Получено {len(messages)} стримов с сообщениями")
                    sys.stdout.flush()
                    
                    for stream_name, stream_messages in messages:
                        # Закомментировано для уменьшения шума в логах
                        # print(f"📨 BinanceStreamConsumer: Обрабатываем стрим {stream_name} с {len(stream_messages)} сообщениями")
                        # sys.stdout.flush()
                        
                        for message_id, fields in stream_messages:
                            try:
                                # Закомментировано для уменьшения шума в логах
                                # print(f"📨 BinanceStreamConsumer: Обрабатываем сообщение {message_id} из {stream_name}")
                                # sys.stdout.flush()
                                
                                # Обрабатываем сообщение через кастомную логику
                                self.process_stream_message(stream_name, message_id, fields)
                                
                                # Подтверждаем обработку
                                self.redis_client.xack(stream_name, self.consumer_group, message_id)
                                
                                # Обновляем статистику
                                self.stats.update_stats(stream_name, message_id)
                                
                            except Exception as e:
                                print(f"❌ BinanceStreamConsumer: Ошибка обработки сообщения {message_id}: {e}")
                                sys.stdout.flush()
                else:
                    # Нет новых сообщений, продолжаем цикл
                    pass
                                
            except Exception as e:
                print(f"❌ BinanceStreamConsumer: Ошибка чтения стримов: {e}")
                sys.stdout.flush()
                if "NOGROUP" in str(e).upper():
                    print("⚠️ BinanceStreamConsumer: обнаружен NOGROUP, пересоздаём consumer groups...")
                    sys.stdout.flush()
                    self.utils.create_consumer_groups(
                        self.redis_client,
                        self.streams_to_consume,
                        self.consumer_group,
                    )
                time.sleep(1)  # Пауза перед повторной попыткой
                
    def _periodic_stats(self):
        """Периодический вывод статистики."""
        while self.running:
            time.sleep(30)  # Статистика каждые 30 секунд
            if self.running:
                self.stats.print_stats()
                sys.stdout.flush()
        
    def stop(self):
        """Останавливает потребитель."""
        self.running = False
        if hasattr(self, 'redis_client') and self.redis_client:
            self.redis_client.close()
        print("⛔ BinanceStreamConsumer остановлен")
        sys.stdout.flush()
        
    def process_stream_message(self, stream_name: str, message_id: str, fields: dict):
        """
        Переопределенная обработка сообщений из стрима.

        Args:
            stream_name: Имя стрима
            message_id: ID сообщения  
            fields: Поля сообщения (может быть dict или list пар ключ-значение)
        """
        try:
            # Обрабатываем различные форматы данных из Redis streams
            data_field = None
            
            if isinstance(fields, dict):
                data_field = fields.get('data')
            elif isinstance(fields, list) and len(fields) >= 2:
                # Если fields - список, данные могут быть в формате [key, value, key, value, ...]
                try:
                    fields_dict = dict(zip(fields[::2], fields[1::2]))
                    data_field = fields_dict.get('data')
                except Exception:
                    print(f"⚠️ Не удалось преобразовать список в словарь: {fields}")
                    return
            
            if not data_field:
                print(f"⚠️ Сообщение {message_id} не содержит поле 'data'")
                return
            
            # Парсим JSON данные
            message_data = json.loads(data_field)
            
            # Закомментировано для уменьшения шума в логах
            # print(f"📨 BinanceDataHandler: Получено сообщение из {stream_name}")
            # sys.stdout.flush()
            
            # Обработка в зависимости от типа стрима
            if stream_name == 'stream:ticker-24h':
                self.ticker_handler.handle_ticker_stream_data(message_data)
            elif stream_name == 'stream:funding-rates':
                self.funding_handler.handle_funding_stream_data(message_data)
            elif stream_name == RS.WS_NEW_PAIRS_STREAM:
                self.pairs_handler.handle_new_pairs_data(message_data)
            else:
                print(f"⚠️ BinanceDataHandler: Неизвестный стрим: {stream_name}")
                sys.stdout.flush()
                
        except json.JSONDecodeError as e:
            print(f"❌ BinanceDataHandler: Ошибка парсинга JSON в сообщении {message_id}: {e}")
            sys.stdout.flush()
        except Exception as e:
            print(f"❌ BinanceDataHandler: Ошибка обработки сообщения из {stream_name}: {e}")
            sys.stdout.flush() 