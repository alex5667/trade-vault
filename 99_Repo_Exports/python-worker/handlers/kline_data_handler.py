"""
Обработчик сырых данных свечей (klines) через Redis Streams.

Назначение:
- Читает стрим `SUBSCRIBE_STREAM` (по умолчанию `stream:kline-1m`) в consumer group `KLINE_CONSUMER_GROUP`.
- Обрабатывает pending-сообщения при старте, затем читает новые сообщения.
- Для каждой свечи вызывает обработчики сигналов (базовый и по диапазону).
- Ведёт историю диапазонов отдельно по каждому символу.
"""

import json
import time
import threading
import sys
import os
from typing import Dict, List, Any

from core.config import SUBSCRIBE_STREAM, KLINE_CONSUMER_GROUP, KLINE_PENDING_FETCH, KLINE_READ_COUNT, KLINE_READ_BLOCK_MS
from signals.volatility import handle_volatility
from signals.volatility_by_range import handle_volatility_by_range
from core.dq_policy import TickDQPolicy


class KlineDataHandler:
    """
    Высокоуровневый обработчик данных свечей через Redis Streams.

    Поток исполнения:
    - start() → _handle_kline_data() в фоне
    - _handle_kline_data(): создаёт consumer group (если нет), обрабатывает pending, затем читает новые сообщения
    - _process_stream_message() → парсит JSON, извлекает kline
    - _process_kline() → вызывает сигнализаторы и поддерживает историю
    """
    
    def __init__(self):
        """Инициализация клиента Redis и внутренних структур."""
        from core.redis_client import get_redis
        self.redis_client = get_redis()
        self.is_running = False
        self.consumer = None
        # Истории диапазонов по символам: { 'BTCUSDT': [range1, range2, ...], ... }
        self.histories: Dict[str, List[Any]] = {}
        self._stats_thread = None
        self._stats_interval_sec = 60
        self.tick_dq_policy = TickDQPolicy(latency_lenient_mode=True)
        
    def start(self) -> None:
        """Запускает обработчик в отдельном потоке (daemon)."""
        if self.is_running:
            print("⚠️ KlineDataHandler уже запущен")
            return
            
        self.is_running = True
        thread = threading.Thread(target=self._handle_kline_data, daemon=True)
        thread.start()
        print("🚀 KlineDataHandler запущен с Redis Streams")
        sys.stdout.flush()
        # Запускаем периодический вывод статистики размеров историй
        self._stats_thread = threading.Thread(target=self._periodic_history_stats, daemon=True)
        self._stats_thread.start()
        
    def stop(self) -> None:
        """Останавливает обработчик и consumer (если есть)."""
        self.is_running = False
        if self.consumer:
            self.consumer.stop()
        print("⛔ KlineDataHandler остановлен")
        sys.stdout.flush()
    
    def _handle_kline_data(self) -> None:
        """
        Основная функция обработки данных свечей через Redis Streams.
        - Создаёт consumer group (id='$', mkstream=True) — новый поток, только новые сообщения.
        - Обрабатывает pending сообщения.
        - Переходит к основному циклу чтения новых сообщений.
        """
        try:
            print(f"🔄 KlineDataHandler: Подключение к стриму: {SUBSCRIBE_STREAM}")
            sys.stdout.flush()
            
            # Создаем consumer group если не существует
            try:
                self.redis_client.xgroup_create(
                    SUBSCRIBE_STREAM, 
                    KLINE_CONSUMER_GROUP, 
                    id='$',
                    mkstream=True
                )
                print(f"✅ Consumer group {KLINE_CONSUMER_GROUP} создана для {SUBSCRIBE_STREAM}")
            except Exception as e:
                if "BUSYGROUP" in str(e):
                    print(f"ℹ️ Consumer group {KLINE_CONSUMER_GROUP} уже существует для {SUBSCRIBE_STREAM}")
                else:
                    print(f"❌ Ошибка создания consumer group: {e}")
            
            # Устанавливаем уникальное имя потребителя
            consumer_name = f"kline-consumer-{os.getpid()}-{int(time.time())}"
            
            # Сначала обрабатываем pending сообщения
            self._process_pending_kline_messages(consumer_name)
            
            # Затем запускаем основной цикл
            self._consume_kline_loop(consumer_name)
            
        except Exception as e:
            print(f"❌ KlineDataHandler: Ошибка в handler: {e}")
            sys.stdout.flush()
    
    def _process_pending_kline_messages(self, consumer_name: str):
        """Обрабатывает pending сообщения kline (которые уже были доставлены, но не ACK-нуты)."""
        try:
            print("🔄 Проверка pending kline сообщений...")
            
            pending = self.redis_client.xpending_range(
                SUBSCRIBE_STREAM, 
                KLINE_CONSUMER_GROUP, 
                '-', '+', KLINE_PENDING_FETCH
            )
            
            if pending:
                print(f"📦 Найдено {len(pending)} pending kline сообщений")
                
                for pending_info in pending:
                    if isinstance(pending_info, dict):
                        message_id = pending_info['message_id']
                    else:
                        # Если pending_info это список [id, consumer, idle_time, delivery_count]
                        message_id = pending_info[0]
                    
                    # Получаем сообщение и обрабатываем
                    message = self.redis_client.xrange(SUBSCRIBE_STREAM, message_id, message_id, count=1)
                    if message:
                        fields = message[0][1]
                        self._process_stream_message(message_id, fields)
                        self.redis_client.xack(SUBSCRIBE_STREAM, KLINE_CONSUMER_GROUP, message_id)
                        
                print(f"✅ Обработано {len(pending)} pending kline сообщений")
            else:
                print("ℹ️ Pending kline сообщений не найдено")
                
        except Exception as e:
            print(f"❌ Ошибка обработки pending kline сообщений: {e}")
    
    def _consume_kline_loop(self, consumer_name: str):
        """Основной цикл потребления kline сообщений (XREADGROUP блокирующий)."""
        print("🔄 Запуск основного цикла потребления kline...")
        
        while self.is_running:
            try:
                # Читаем новые сообщения
                messages = self.redis_client.xreadgroup(
                    KLINE_CONSUMER_GROUP,
                    consumer_name,
                    {SUBSCRIBE_STREAM: '>'},
                    count=KLINE_READ_COUNT,
                    block=KLINE_READ_BLOCK_MS
                )
                
                if messages:
                    # messages это список в формате [[stream_name, [[message_id, fields], ...]]]
                    for stream_data in messages:
                        stream_name = stream_data[0]
                        stream_messages = stream_data[1]
                        
                        for message_data in stream_messages:
                            message_id = message_data[0]
                            fields = message_data[1]
                            
                            self._process_stream_message(message_id, fields)
                            self.redis_client.xack(stream_name, KLINE_CONSUMER_GROUP, message_id)
                            
            except Exception as e:
                print(f"❌ Ошибка в цикле потребления kline: {e}")
                if "NOGROUP" in str(e).upper():
                    try:
                        self.redis_client.xgroup_create(
                            SUBSCRIBE_STREAM,
                            KLINE_CONSUMER_GROUP,
                            id='$',
                            mkstream=True,
                        )
                        print(f"✅ Consumer group {KLINE_CONSUMER_GROUP} пересоздана для {SUBSCRIBE_STREAM}")
                    except Exception as recreate_err:
                        if "BUSYGROUP" in str(recreate_err):
                            print(f"ℹ️ Consumer group {KLINE_CONSUMER_GROUP} уже существует после проверки")
                        else:
                            print(f"❌ Ошибка пересоздания consumer group: {recreate_err}")
                if self.is_running:
                    time.sleep(1)
    
    def _process_stream_message(self, message_id: str, fields: dict):
        """
        Обработка сообщения из стрима.

        Пытаемся извлечь поле 'data' и распарсить JSON. Далее извлекаем kline из ключа 'k'.
        """
        try:
            # print(f"🔄 KlineDataHandler: Обработка сообщения {message_id}")
            
            if not fields.get('data'):
                print(f"⚠️ Сообщение {message_id} не содержит поле 'data'")
                return
            
            # Парсим JSON данные
            message_data = json.loads(fields['data'])
            # print(f"📊 KlineDataHandler: Получены данные: {str(message_data)[:200]}...")
            
            # Извлекаем данные о свече ('k' - ключ для данных о свече в сообщении от Binance)
            kline = message_data.get('k')
            if not kline:
                print(f"⚠️ KlineDataHandler: Отсутствует ключ 'k' в сообщении: {str(message_data)[:100]}...")
                return
            
            # print(f"🕯️ KlineDataHandler: kline {kline.get('s')}: O={kline.get('o')}, H={kline.get('h')}, L={kline.get('l')}, C={kline.get('c')}")
            
            current_ms = int(time.time() * 1000)
            is_valid, reason = self.tick_dq_policy.validate(kline, current_ms)
            if not is_valid:
                # Log and drop kline (could also send to quarantine stream).
                # We limit verbosity for now but ensure it doesn't process bad data.
                pass
                return

            # Обрабатываем данные свечи
            self._process_kline(kline)
            
            # print(f"✅ KlineDataHandler: Успешно обработано сообщение {message_id}")
                
        except json.JSONDecodeError as e:
            print(f"❌ KlineDataHandler: Ошибка парсинга JSON в сообщении {message_id}: {e}")
            sys.stdout.flush()
        except Exception as e:
            print(f"❌ KlineDataHandler: Ошибка обработки сообщения: {e}")
            sys.stdout.flush()
    
    def _process_kline(self, kline: Dict[str, Any]) -> None:
        """
        Обрабатывает данные одной свечи: вызывает сигнализаторы и ведёт историю.
        """
        # 1. Базовый анализ волатильности (пороговая проверка)
        handle_volatility(kline)
    
        # 2. Анализ волатильности на основе истории диапазонов цен
        symbol = kline['s']
        history = self.histories.setdefault(symbol, [])
        handle_volatility_by_range(kline, history)
    
    def get_histories(self) -> Dict[str, List[Any]]:
        """Возвращает копию словаря историй всех символов."""
        return self.histories.copy()

    def _periodic_history_stats(self) -> None:
        """Периодически выводит количество свечей в истории по каждому символу."""
        while self.is_running:
            try:
                time.sleep(self._stats_interval_sec)
                if not self.is_running:
                    break
                # Формируем краткую сводку: до 20 символов, остальное считаем
                items = list(self.histories.items())
                total_symbols = len(items)
                preview = ", ".join(f"{sym}={len(hist)}" for sym, hist in items[:20])
                if total_symbols > 20:
                    preview += f", ... (+{total_symbols-20} symbols)"
                print(f"📈 History sizes: {preview} | total_symbols={total_symbols}")
                sys.stdout.flush()
            except Exception as e:
                print(f"⚠️ Ошибка вывода статистики историй: {e}")
                sys.stdout.flush() 