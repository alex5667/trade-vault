"""
Утилита для ограничения частоты повторяющихся лог-сообщений.
Позволяет выводить только каждое N-е повторяющееся сообщение.
"""
import threading
from collections import defaultdict


class LogThrottler:
    """
    Класс для ограничения частоты повторяющихся лог-сообщений.
    
    Использование:
        throttler = LogThrottler()
        if throttler.should_log("expired_ticker", 10000):
            print("Сообщение об устаревшем тикере")
    """

    def __init__(self):
        self._counters: dict[str, int] = defaultdict(int)
        self._lock = threading.Lock()

    def should_log(self, message_key: str, every_n: int = 10000) -> bool:
        """
        Проверяет, нужно ли выводить сообщение.
        
        Args:
            message_key: Уникальный ключ для типа сообщения
            every_n: Выводить каждое N-е сообщение (по умолчанию 10000)
            
        Returns:
            True если сообщение нужно вывести, False если пропустить
        """
        with self._lock:
            self._counters[message_key] += 1
            count = self._counters[message_key]

            # Выводим первое сообщение и каждое N-е
            if count == 1 or count % every_n == 0:
                return True
            return False

    def get_count(self, message_key: str) -> int:
        """Возвращает текущий счетчик для указанного ключа."""
        with self._lock:
            return self._counters[message_key]

    def reset_counter(self, message_key: str):
        """Сбрасывает счетчик для указанного ключа."""
        with self._lock:
            self._counters[message_key] = 0

    def log_with_count(self, message_key: str, message: str, every_n: int = 10000) -> bool:
        """
        Проверяет нужно ли логировать и добавляет информацию о счетчике.
        
        Args:
            message_key: Уникальный ключ для типа сообщения
            message: Сообщение для вывода
            every_n: Выводить каждое N-е сообщение
            
        Returns:
            True если сообщение было выведено, False если пропущено
        """
        if self.should_log(message_key, every_n):
            count = self.get_count(message_key)
            if count == 1:
                print(message)
            else:
                print(f"{message} [показано {count}/{count} раз, далее каждое {every_n}-е]")
            return True
        return False


# Глобальный экземпляр для использования в разных модулях
log_throttler = LogThrottler()
