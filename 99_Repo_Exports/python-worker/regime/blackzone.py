from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Literal, Optional, List

try:
    import psycopg2
except ImportError:
    psycopg2 = None


Mode = Literal["normal", "blocked", "strict"]


@dataclass
class BlackZone:
    venue: str
    symbol_pattern: str
    family_pattern: str
    timeframe: str
    ts_start: datetime
    ts_end: datetime
    mode: Literal["blocked", "strict"]


class BlackZoneScheduler:
    """
    Оборачиваемся вокруг таблицы signal_news_blackzone.
    Можно кэшировать в памяти и раз в X минут перезагружать.
    """

    def __init__(self, pg_dsn: str):
        if psycopg2 is None:
            raise ImportError("psycopg2 is not available")
        self.pg = psycopg2.connect(pg_dsn)
        self._cache: List[BlackZone] = []
        self._last_reload_at: Optional[datetime] = None

    def reload(self) -> None:
        with self.pg.cursor() as cur:
            cur.execute(
                """
                SELECT venue, symbol_pattern, family_pattern, timeframe,
                       ts_start, ts_end, mode
                FROM signal_news_blackzone
                WHERE ts_end > now() - interval '5 minutes'
                """
            )
            rows = cur.fetchall()

        zones: List[BlackZone] = []
        for row in rows:
            z = BlackZone(
                venue=row[0],
                symbol_pattern=row[1],
                family_pattern=row[2],
                timeframe=row[3],
                ts_start=row[4],
                ts_end=row[5],
                mode=row[6],
            )
            zones.append(z)

        self._cache = zones
        self._last_reload_at = datetime.now(timezone.utc)

    def _match_pattern(self, value: str, pattern: str) -> bool:
        # Для простоты: pattern '%' или точное совпадение.
        # Можно заменить на SQL LIKE или fnmatch.
        if pattern == "%" or pattern == "*":
            return True
        return value == pattern

    def mode_for(
        self,
        *,
        now: datetime,
        venue: str,
        symbol: str,
        family: str,
        timeframe: str,
    ) -> Mode:
        if now.tzinfo is None:
            now = now.replace(tzinfo=timezone.utc)

        # ленивый reload раз в N минут
        if self._last_reload_at is None:
            self.reload()

        current_mode: Mode = "normal"

        for z in self._cache:
            if z.venue != venue:
                continue
            if not self._match_pattern(symbol, z.symbol_pattern):
                continue
            if not self._match_pattern(family, z.family_pattern):
                continue
            if z.timeframe not in ("%", timeframe):
                continue
            if not (z.ts_start <= now <= z.ts_end):
                continue

            # если хоть одна зона 'blocked' — блокируем
            if z.mode == "blocked":
                return "blocked"
            if z.mode == "strict":
                current_mode = "strict"

        return current_mode
