"""
Обертка над Redis‑клиентом для записи событий в Redis Streams.

Класс RedisClient предоставляет метод xadd с автоматическим:
- приведением None → пустая строка
- ограничением длины стрима (MAXLEN ~)
И утилиту для обрезки по времени (XTRIM MINID) для TTL по временной метке ID.
"""

from typing import Dict, Optional
import redis
import time


class RedisClient:
    """Минимальный клиент Redis для публикации событий в Redis Streams."""

    def __init__(self, url: str, stream_maxlen: int = 1000) -> None:
        """
        Инициализирует соединение с Redis.

        Аргументы:
            url: URL подключения к Redis (например, redis://redis:6379/0)
            stream_maxlen: приблизительное ограничение длины каждого стрима
        """
        self.client = redis.Redis.from_url(url, decode_responses=True)
        self.stream_maxlen = stream_maxlen

    def xadd(self, stream: str, data: Dict[str, str]) -> None:
        """
        Публикует словарь полей в указанный Redis Stream.

        Аргументы:
            stream: имя стрима (например, signal:telegram:parsed)
            data: словарь строковых значений; нестроковые приводятся к строке
        """
        fields = {k: ("" if v is None else str(v)) for k, v in data.items()}
        self.client.xadd(stream, fields, maxlen=self.stream_maxlen, approximate=True)

    def xtrim_by_ttl(self, stream: str, ttl_seconds: int) -> Optional[int]:
        """
        Обрезает стрим по времени жизни записей, используя XTRIM MINID.

        Суть: формируем минимальный ID на основе текущей эпохи (мс) минус ttl.
        Redis хранит ID как <msTime>-<seq>. XTRIM MINID удалит все записи старее указанного ID.

        Возвращает:
            число удалённых записей (если сервер вернул его) или None.
        """
        if ttl_seconds <= 0:
            return None
        # Используем правильное NY время вместо UTC
        current_ny_time_ms = int(time.time() * 1000)
        min_ms = current_ny_time_ms - (ttl_seconds * 1000)
        # MINID обрезает по id, здесь используем только компонент времени
        try:
            return self.client.xtrim(stream, minid=str(min_ms), approximate=True)
        except Exception:
            return None 