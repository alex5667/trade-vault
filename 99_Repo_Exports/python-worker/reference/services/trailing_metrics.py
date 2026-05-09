"""
Prometheus metrics для TP1 Trailing System.

Собирает метрики:
- Количество TP1/TP2/TP3/SL событий
- Количество запущенных трейлингов
- Количество ошибок
- Latency обработки событий
"""

from prometheus_client import Counter, Gauge, Histogram, Info

# Counters
tp_events_total = Counter(
    'tp_events_total',
    'Total number of TP/SL events processed',
    ['event_type', 'symbol']
)

trailing_started_total = Counter(
    'trailing_started_total',
    'Total number of trailing stops started',
    ['symbol', 'profile']
)

trailing_failed_total = Counter(
    'trailing_failed_total',
    'Total number of failed trailing start attempts',
    ['symbol', 'reason']
)

signals_without_trail_flag = Counter(
    'signals_without_trail_flag',
    'Signals that reached TP1 but had no trail_after_tp1 flag',
    ['symbol']
)

signals_not_found = Counter(
    'signals_not_found',
    'TP1 events for signals not found in Redis',
    ['symbol']
)

# Histograms
event_processing_duration = Histogram(
    'event_processing_duration_seconds',
    'Time spent processing TP/SL events',
    ['event_type']
)

trailing_command_latency = Histogram(
    'trailing_command_latency_seconds',
    'Latency of sending trailing command to gateway',
    buckets=[0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0]
)

# Gauges
active_trailing_positions = Gauge(
    'active_trailing_positions',
    'Number of positions currently in trailing mode',
    ['symbol']
)

pending_events = Gauge(
    'pending_events',
    'Number of pending events in consumer group',
    ['stream', 'group']
)

# Info
trailing_system_info = Info(
    'trailing_system',
    'Information about TP1 Trailing System'
)


class TrailingMetrics:
    """
    Wrapper для удобной работы с метриками.
    """

    @staticmethod
    def record_event(event_type: str, symbol: str):
        """Записать обработанное событие."""
        tp_events_total.labels(event_type=event_type, symbol=symbol).inc()

    @staticmethod
    def record_trailing_started(symbol: str, profile: str):
        """Записать запуск трейлинга."""
        trailing_started_total.labels(symbol=symbol, profile=profile).inc()
        active_trailing_positions.labels(symbol=symbol).inc()

    @staticmethod
    def record_trailing_failed(symbol: str, reason: str):
        """Записать ошибку запуска трейлинга."""
        trailing_failed_total.labels(symbol=symbol, reason=reason).inc()

    @staticmethod
    def record_signal_without_flag(symbol: str):
        """Записать сигнал без флага trail_after_tp1."""
        signals_without_trail_flag.labels(symbol=symbol).inc()

    @staticmethod
    def record_signal_not_found(symbol: str):
        """Записать отсутствие сигнала в Redis."""
        signals_not_found.labels(symbol=symbol).inc()

    @staticmethod
    def record_trailing_stopped(symbol: str):
        """Записать остановку трейлинга."""
        active_trailing_positions.labels(symbol=symbol).dec()

    @staticmethod
    def time_event_processing(event_type: str):
        """Context manager для измерения времени обработки события."""
        return event_processing_duration.labels(event_type=event_type).time()

    @staticmethod
    def time_trailing_command():
        """Context manager для измерения latency команды трейлинга."""
        return trailing_command_latency.time()

    @staticmethod
    def set_pending_events(stream: str, group: str, count: int):
        """Установить количество pending событий."""
        pending_events.labels(stream=stream, group=group).set(count)

    @staticmethod
    def set_system_info(version: str, default_profile: str):
        """Установить информацию о системе."""
        trailing_system_info.info({
            'version': version,
            'default_profile': default_profile
        })


if __name__ == "__main__":
    # Тестирование метрик
    from prometheus_client import REGISTRY, generate_latest

    # Имитация работы
    TrailingMetrics.set_system_info("1.0.0", "rocket_v1")

    TrailingMetrics.record_event("TP1_HIT", "XAUUSD")
    TrailingMetrics.record_trailing_started("XAUUSD", "rocket_v1")

    TrailingMetrics.record_event("TP2_HIT", "XAUUSD")
    TrailingMetrics.record_trailing_stopped("XAUUSD")

    # Генерация метрик
    print("=== Prometheus Metrics ===")
    print(generate_latest(REGISTRY).decode('utf-8'))

