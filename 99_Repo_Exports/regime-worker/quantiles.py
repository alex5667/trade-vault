"""
quantiles.py — Загрузчик квантилей из Postgres с TTL-кэшем и connection pooling.

Квантили используются для адаптивной классификации режима рынка.
"""

from __future__ import annotations

import logging
import os
import time
from typing import Dict, Optional, Tuple

import psycopg2
import psycopg2.pool

logger = logging.getLogger(__name__)

# Дефолты: используются при старте или при недоступности БД
DEFAULTS: Dict[str, float] = {
    "adx_p40": 18.0,
    "adx_p60": 25.0,
    "adx_p75": 32.0,
    "atrp_p25": 0.0008,
    "atrp_p50": 0.0016,
    "atrp_p75": 0.0025,
}

# TTL (сек): как часто разрешаем обновлять кэш из БД
TTL_SEC: int = int(os.getenv("REGIME_QUANTILES_TTL_SEC", "600"))

DATABASE_URL: Optional[str] = os.getenv("DATABASE_URL")

# TTL-кэш: (symbol, tf) -> (timestamp, data)
_cache: Dict[Tuple[str, str], Tuple[float, Dict[str, float]]] = {}

# Debounce для логирования ошибок DB (не спамим логи)
_db_last_error_time: float = 0.0
_DB_ERROR_LOG_INTERVAL_SEC: float = 60.0

# Lazy connection pool (None — нет DB URL или ещё не инициализирован)
_pool: Optional[psycopg2.pool.SimpleConnectionPool] = None


def _get_pool() -> Optional[psycopg2.pool.SimpleConnectionPool]:
    """Инициализирует пул соединений при первом обращении (lazy)."""
    global _pool
    if _pool is not None:
        return _pool
    if not DATABASE_URL:
        return None
    try:
        _pool = psycopg2.pool.SimpleConnectionPool(
            minconn=1,
            maxconn=3,
            dsn=DATABASE_URL,
        )
        logger.info("Quantiles: psycopg2 connection pool created (min=1, max=3)")
    except Exception as exc:
        logger.warning("Quantiles: cannot create connection pool: %s", exc)
        _pool = None
    return _pool


def _fetch_from_db(symbol: str, timeframe: str) -> Optional[Dict[str, float]]:
    """
    Читает квантили для (symbol, timeframe) из таблицы `regime_quantiles`.
    Использует connection pool; при ошибке возвращает None (fallback → DEFAULTS).
    """
    global _db_last_error_time

    pool = _get_pool()
    if pool is None:
        return None

    conn = None
    try:
        conn = pool.getconn()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT adx_p40, adx_p60, adx_p75, atrp_p25, atrp_p50, atrp_p75
                FROM regime_quantiles
                WHERE symbol = %s AND timeframe = %s
                LIMIT 1
                """,
                (symbol, timeframe),
            )
            row = cur.fetchone()
            if not row:
                return None
            adx_p40, adx_p60, adx_p75, atrp_p25, atrp_p50, atrp_p75 = row
            return {
                "adx_p40": float(adx_p40),
                "adx_p60": float(adx_p60),
                "adx_p75": float(adx_p75),
                "atrp_p25": float(atrp_p25),
                "atrp_p50": float(atrp_p50),
                "atrp_p75": float(atrp_p75),
            }
    except Exception as exc:
        now = time.monotonic()
        if now - _db_last_error_time >= _DB_ERROR_LOG_INTERVAL_SEC:
            logger.warning(
                "Quantiles: DB error for %s@%s: %s (будет использован DEFAULTS)",
                symbol,
                timeframe,
                exc,
            )
            _db_last_error_time = now
        return None
    finally:
        if conn is not None:
            try:
                pool.putconn(conn)
            except Exception:
                pass


def load_quantiles(symbol: str, timeframe: str) -> Dict[str, float]:
    """
    Публичная функция для воркера режима:
    - возвращает квантили из TTL-кэша, если не протухли,
    - иначе тянет из БД и обновляет кэш,
    - при отсутствии данных — возвращает DEFAULTS.
    """
    now = time.monotonic()
    key = (symbol, timeframe)

    if key in _cache:
        ts, data = _cache[key]
        if now - ts < TTL_SEC:
            return data

    data = _fetch_from_db(symbol, timeframe) or DEFAULTS
    _cache[key] = (now, data)
    return data


def bust_cache_for(symbol: str, timeframe: str) -> None:
    """Сброс кэша для пары symbol/timeframe (при обновлении квантилей в БД)."""
    _cache.pop((symbol, timeframe), None)
