"""
Примеры использования Order Flow Worker.

Этот файл демонстрирует различные способы работы с Order Flow метриками:
- Подписка на OF спайки
- Мониторинг всех OF баров
- Анализ CVD трендов
- Работа с ATR кэшем
"""

import redis
import json
import time
from typing import Dict, Any, List
from collections import defaultdict


class OrderFlowMonitor:
    """Монитор Order Flow метрик из Redis Streams."""
    
    def __init__(self, redis_host: str = 'localhost', redis_port: int = 6380):
        """
        Инициализация монитора.
        
        Args:
            redis_host: Хост Redis
            redis_port: Порт Redis (6380 для сигналов)
        """
        self.redis = redis.Redis(
            host=redis_host
            port=redis_port
            decode_responses=True
        )
        self.consumer_group = 'example-of-monitor'
        self.consumer_name = f'monitor-{int(time.time())}'
        
    def subscribe_to_spikes(self, callback: callable) -> None:
        """
        Подписка на OF спайки в реальном времени.
        
        Args:
            callback: Функция обработки спайка (принимает dict)
        """
        try:
            # Создание consumer group
            self.redis.xgroup_create(
                'stream:of-spike'
                self.consumer_group
                id='$'
                mkstream=True
            )
            print(f"✅ Consumer group '{self.consumer_group}' создана")
        except Exception as e:
            if "BUSYGROUP" not in str(e):
                print(f"⚠️ Ошибка создания consumer group: {e}")
        
        print(f"🔄 Подписка на OF спайки...")
        
        while True:
            try:
                messages = self.redis.xreadgroup(
                    self.consumer_group
                    self.consumer_name
                    {'stream:of-spike': '>'}
                    count=10
                    block=1000
                )
                
                if messages:
                    for stream_name, stream_messages in messages:
                        for message_id, fields in stream_messages:
                            data = json.loads(fields['data'])
                            callback(data)
                            
                            # Подтверждаем обработку
                            self.redis.xack('stream:of-spike', self.consumer_group, message_id)
                            
            except KeyboardInterrupt:
                print("\n⛔ Остановка монитора...")
                break
            except Exception as e:
                print(f"❌ Ошибка чтения: {e}")
                time.sleep(1)
    
    def get_recent_bars(self, count: int = 100) -> List[Dict[str, Any]]:
        """
        Получает последние OF бары.
        
        Args:
            count: Количество баров
            
        Returns:
            List[Dict]: Список OF баров
        """
        try:
            messages = self.redis.xrevrange('stream:of-bar', count=count)
            bars = []
            
            for message_id, fields in messages:
                data = json.loads(fields['data'])
                bars.append(data)
            
            return bars
        
        except Exception as e:
            print(f"❌ Ошибка получения баров: {e}")
            return []
    
    def analyze_cvd_by_symbol(self, bars: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
        """
        Анализирует CVD по символам.
        
        Args:
            bars: Список OF баров
            
        Returns:
            Dict: Статистика CVD по символам
        """
        cvd_stats = defaultdict(lambda: {'latest_cvd': 0, 'bars_count': 0, 'total_delta': 0})
        
        for bar in bars:
            symbol = bar['symbol']
            cvd_stats[symbol]['latest_cvd'] = bar['cvd']
            cvd_stats[symbol]['bars_count'] += 1
            cvd_stats[symbol]['total_delta'] += bar['delta']
        
        return dict(cvd_stats)
    
    def get_top_delta_symbols(self, bars: List[Dict[str, Any]], limit: int = 10) -> List[Dict[str, Any]]:
        """
        Находит символы с наибольшим абсолютным delta.
        
        Args:
            bars: Список OF баров
            limit: Количество топ символов
            
        Returns:
            List[Dict]: Топ символов по delta
        """
        symbol_deltas = defaultdict(list)
        
        for bar in bars:
            symbol_deltas[bar['symbol']].append(abs(bar['delta']))
        
        # Вычисляем средний абсолютный delta
        avg_deltas = []
        for symbol, deltas in symbol_deltas.items():
            avg_delta = sum(deltas) / len(deltas)
            avg_deltas.append({
                'symbol': symbol
                'avg_abs_delta': avg_delta
                'bars_count': len(deltas)
            })
        
        # Сортируем по убыванию
        avg_deltas.sort(key=lambda x: x['avg_abs_delta'], reverse=True)
        
        return avg_deltas[:limit]


# ---------------------- Примеры использования ----------------------

def example_1_spike_monitor():
    """Пример 1: Мониторинг OF спайков в реальном времени."""
    print("=== Пример 1: Мониторинг OF спайков ===\n")
    
    monitor = OrderFlowMonitor()
    
    def on_spike(data: Dict[str, Any]):
        """Обработчик спайка."""
        print(f"🎯 Spike: {data['symbol']} {data['timeframe']} {data['direction']}")
        print(f"   z-score: {data['zDelta']:.2f}")
        print(f"   delta ratio: {data['deltaRatio']:.2f}")
        print(f"   CVD: {data['cvd']:.2f}")
        print(f"   volume: {data['volume']:.2f}\n")
    
    monitor.subscribe_to_spikes(on_spike)


def example_2_recent_bars():
    """Пример 2: Анализ последних OF баров."""
    print("=== Пример 2: Анализ последних баров ===\n")
    
    monitor = OrderFlowMonitor()
    
    # Получаем последние 100 баров
    bars = monitor.get_recent_bars(count=100)
    
    print(f"Получено {len(bars)} баров\n")
    
    # Показываем первые 5
    for bar in bars[:5]:
        print(f"{bar['symbol']} {bar['timeframe']}:")
        print(f"  Delta: {bar['delta']:.2f}")
        print(f"  CVD: {bar['cvd']:.2f}")
        print(f"  z-score: {bar['zDelta']:.2f}")
        print(f"  Absorbed: {bar['absorbed']}\n")


def example_3_cvd_analysis():
    """Пример 3: Анализ CVD по символам."""
    print("=== Пример 3: Анализ CVD ===\n")
    
    monitor = OrderFlowMonitor()
    bars = monitor.get_recent_bars(count=1000)
    
    cvd_stats = monitor.analyze_cvd_by_symbol(bars)
    
    # Сортируем по абсолютному CVD
    sorted_symbols = sorted(
        cvd_stats.items()
        key=lambda x: abs(x[1]['latest_cvd'])
        reverse=True
    )
    
    print("Топ-10 символов по абсолютному CVD:\n")
    for symbol, stats in sorted_symbols[:10]:
        print(f"{symbol}:")
        print(f"  CVD: {stats['latest_cvd']:.2f}")
        print(f"  Total Delta: {stats['total_delta']:.2f}")
        print(f"  Bars: {stats['bars_count']}\n")


def example_4_top_delta():
    """Пример 4: Поиск символов с высоким delta."""
    print("=== Пример 4: Топ символов по Delta ===\n")
    
    monitor = OrderFlowMonitor()
    bars = monitor.get_recent_bars(count=500)
    
    top_symbols = monitor.get_top_delta_symbols(bars, limit=10)
    
    print("Символы с наибольшим средним |delta|:\n")
    for i, item in enumerate(top_symbols, 1):
        print(f"{i}. {item['symbol']}")
        print(f"   Avg |Delta|: {item['avg_abs_delta']:.2f}")
        print(f"   Bars: {item['bars_count']}\n")


def example_5_atr_cache():
    """Пример 5: Работа с ATR кэшем."""
    print("=== Пример 5: ATR Cache ===\n")
    
    from utils.atr_cache import get_atr_cache
    
    cache = get_atr_cache()
    
    # Сохранение ATR
    cache.set("BTCUSDT", "1m", 125.5)
    cache.set("ETHUSDT", "1m", 8.3)
    cache.set("BNBUSDT", "5m", 2.1)
    
    print("Сохранено 3 значения ATR\n")
    
    # Получение ATR
    atr_btc = cache.get("BTCUSDT", "1m")
    atr_eth = cache.get("ETHUSDT", "1m")
    atr_missing = cache.get("XRPUSDT", "1m")
    
    print(f"BTCUSDT 1m ATR: {atr_btc}")
    print(f"ETHUSDT 1m ATR: {atr_eth}")
    print(f"XRPUSDT 1m ATR: {atr_missing} (не найдено)\n")
    
    # Очистка всех ATR ключей
    deleted = cache.clear_all()
    print(f"Удалено {deleted} ATR ключей")


if __name__ == '__main__':
    """
    Запуск примеров:
    
    python -m of.example_usage
    
    Раскомментируйте нужный пример ниже.
    """
    
    # example_1_spike_monitor()    # Мониторинг спайков (бесконечный цикл)
    # example_2_recent_bars()       # Последние бары
    # example_3_cvd_analysis()      # Анализ CVD
    # example_4_top_delta()         # Топ по delta
    # example_5_atr_cache()         # ATR кэш
    
    print("Раскомментируйте нужный пример в коде")

