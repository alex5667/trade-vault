# core/config.py
from __future__ import annotations

import os
import re
from typing import Optional


def _normalize_pattern_key(label: str) -> str:
    """
    breakout_R1  -> BREAKOUT_R1
    fade-PDH     -> FADE_PDH
    fade HTF OB  -> FADE_HTF_OB
    """
    return re.sub(r"[^A-Z0-9]+", "_", label.upper())


# Базовый порог для "golden" (если нет override под конкретный паттерн)
GOLDEN_CONFIDENCE_DEFAULT: int = int(os.getenv("GOLDEN_CONFIDENCE_DEFAULT", "90"))

# Базовый вес паттерна (можешь использовать в скоринге)
GOLDEN_WEIGHT_DEFAULT: float = float(os.getenv("GOLDEN_WEIGHT_DEFAULT", "1.0"))


def get_pattern_conf_threshold(pattern_label: Optional[str]) -> int:
    """
    Порог для golden по паттерну:
      - env: GOLDEN_CONF_<NORMALIZED_LABEL>
      - иначе GOLDEN_CONFIDENCE_DEFAULT
    """
    if not pattern_label:
        return GOLDEN_CONFIDENCE_DEFAULT

    key = _normalize_pattern_key(pattern_label)
    env_name = f"GOLDEN_CONF_{key}"
    raw = os.getenv(env_name)
    if raw is None:
        return GOLDEN_CONFIDENCE_DEFAULT

    try:
        return int(raw)
    except ValueError:
        # Если в ENV мусор — не ломаемся, а используем дефолт
        return GOLDEN_CONFIDENCE_DEFAULT


def get_pattern_weight(pattern_label: Optional[str]) -> float:
    """
    Вес паттерна:
      - env: GOLDEN_WEIGHT_<NORMALIZED_LABEL>
      - иначе GOLDEN_WEIGHT_DEFAULT
    """
    if not pattern_label:
        return GOLDEN_WEIGHT_DEFAULT

    key = _normalize_pattern_key(pattern_label)
    env_name = f"GOLDEN_WEIGHT_{key}"
    raw = os.getenv(env_name)
    if raw is None:
        return GOLDEN_WEIGHT_DEFAULT

    try:
        return float(raw)
    except ValueError:
        return GOLDEN_WEIGHT_DEFAULT


# ===== скоринг по confidence / golden / весу паттерна =====

# масштабирование confidence (0–100 → примерно в 0–1)
CONFIDENCE_SCALE: float = float(os.getenv("CONFIDENCE_SCALE", "0.01"))

# множитель для golden-паттернов
GOLDEN_SCORE_MULTIPLIER: float = float(os.getenv("GOLDEN_SCORE_MULTIPLIER", "1.2"))

# защита от разлёта
FINAL_SCORE_MAX: float = float(os.getenv("FINAL_SCORE_MAX", "5.0"))

# минимальный финальный скор, ниже — сигнал можно дропать
MIN_FINAL_SCORE: float = float(os.getenv("MIN_FINAL_SCORE", "0.5"))


# ===== Stream Consumer Configuration =====

# Consumer group для Redis streams
SCANNER_CONSUMER_GROUP: str = os.getenv("SCANNER_CONSUMER_GROUP", "scanner-consumer-group")

# Список стримов для чтения (можно переопределить через ENV)
SCANNER_STREAMS: list = os.getenv("SCANNER_STREAMS", "stream:tick_XAUUSD,stream:book_XAUUSD").split(",")

# Параметры чтения из Redis streams
SCANNER_READ_COUNT: int = int(os.getenv("SCANNER_READ_COUNT", "10"))
SCANNER_READ_BLOCK_MS: int = int(os.getenv("SCANNER_READ_BLOCK_MS", "5000"))

# Интервал для статистики (секунды)
SCANNER_STATS_INTERVAL_SEC: int = int(os.getenv("SCANNER_STATS_INTERVAL_SEC", "60"))


# ===== Binance Streams Configuration =====

# Список Binance стримов для обработки (тикеры, funding, пары)
BINANCE_STREAMS: list = os.getenv("BINANCE_STREAMS", "stream:binance_tickers,stream:binance_funding,stream:binance_pairs").split(",")


# ===== XAU/MT5 Configuration =====

# XAU tick stream configuration
XAU_TICK_STREAM: str = os.getenv("XAU_TICK_STREAM", "stream:tick_XAUUSD")
XAU_TICK_STREAM_MAXLEN: int = int(os.getenv("XAU_TICK_STREAM_MAXLEN", "10000"))

# XAU book stream configuration
XAU_BOOK_STREAM: str = os.getenv("XAU_BOOK_STREAM", "stream:book_XAUUSD")
XAU_BOOK_STREAM_MAXLEN: int = int(os.getenv("XAU_BOOK_STREAM_MAXLEN", "20000"))

# XAU handler enabled flag
XAU_HANDLER_ENABLED: bool = os.getenv("XAU_HANDLER_ENABLED", "true").lower() == "true"

# Handler configuration
XAU_HANDLER_ENABLED: bool = os.getenv("XAU_HANDLER_ENABLED", "true").lower() == "true"


# ===== Metrics Scheduler Configuration =====

# Интервал для метрик scheduler (секунды)
METRICS_SCHEDULER_INTERVAL_SEC: int = int(os.getenv("METRICS_SCHEDULER_INTERVAL_SEC", "300"))


# ===== Stream Publisher Configuration =====

# Mapping каналов на Redis стримы
STREAM_MAPPING: dict = {
    "signals": "stream:signals",
    "orders": "stream:orders",
    "alerts": "stream:alerts",
    "signal:crypto": "stream:signal_crypto",
    "signal:forex": "stream:signal_forex",
    "trigger:crypto": "stream:trigger_crypto",
    "trigger:forex": "stream:trigger_forex",
    "top:crypto": "stream:top_crypto",
    "top:forex": "stream:top_forex",
}

# Максимальная длина Redis стримов (автоочистка)
STREAM_MAX_LENGTH: int = int(os.getenv("STREAM_MAX_LENGTH", "10000"))

# TTL для дедупликации сигналов (секунды)
SIGNAL_DEDUP_TTL_SEC: int = int(os.getenv("SIGNAL_DEDUP_TTL_SEC", "300"))


# ===== Kline Data Handler Configuration =====

# Subscribe stream для kline данных
SUBSCRIBE_STREAM: str = os.getenv("SUBSCRIBE_STREAM", "stream:subscribe")

# Kline consumer group
KLINE_CONSUMER_GROUP: str = os.getenv("KLINE_CONSUMER_GROUP", "kline-consumer-group")

# Kline pending fetch configuration
KLINE_PENDING_FETCH: int = int(os.getenv("KLINE_PENDING_FETCH", "100"))

# Kline read parameters
KLINE_READ_COUNT: int = int(os.getenv("KLINE_READ_COUNT", "10"))
KLINE_READ_BLOCK_MS: int = int(os.getenv("KLINE_READ_BLOCK_MS", "5000"))


# ===== Database Configuration =====

# PostgreSQL DSN для подключения к базе данных
PG_DSN: str = os.getenv("PG_DSN", "postgresql://user:pass@localhost:5432/trade")


# ===== Volatility Signals Configuration =====

# Минимальный процент волатильности для триггера сигнала
VOLATILITY_SPIKE_MIN_PCT: float = float(os.getenv("VOLATILITY_SPIKE_MIN_PCT", "2.0"))

# Минимальный процент волатильности по диапазону для триггера сигнала
VOLATILITY_RANGE_MIN_PCT: float = float(os.getenv("VOLATILITY_RANGE_MIN_PCT", "1.5"))

# Redis канал для публикации сигналов волатильности
REDIS_CHANNEL_VOLATILITY: str = os.getenv("REDIS_CHANNEL_VOLATILITY", "signals")

# Redis канал для публикации сигналов волатильности по диапазону
REDIS_CHANNEL_VOLATILITY_RANGE: str = os.getenv("REDIS_CHANNEL_VOLATILITY_RANGE", "signals")

# Дефолтный интервал для сигналов (1m, 5m, etc.)
DEFAULT_INTERVAL: str = os.getenv("DEFAULT_INTERVAL", "1m")

# Порог множителя диапазона для волатильности
RANGE_MULTIPLIER_THRESHOLD: float = float(os.getenv("RANGE_MULTIPLIER_THRESHOLD", "1.5"))

# Окно оценки диапазона (количество баров)
RANGE_EVAL_WINDOW: int = int(os.getenv("RANGE_EVAL_WINDOW", "20"))


# ===== Top Signals Configuration =====

# Лимит топ-объемов для сигналов
TOP_VOLUME_LIMIT: int = int(os.getenv("TOP_VOLUME_LIMIT", "10"))

# Лимит топ-гейнеров
TOP_GAINERS_LIMIT: int = int(os.getenv("TOP_GAINERS_LIMIT", "10"))

# Лимит топ-лузеров
TOP_LOSERS_LIMIT: int = int(os.getenv("TOP_LOSERS_LIMIT", "10"))

# Лимит топ-фандинга
TOP_FUNDING_LIMIT: int = int(os.getenv("TOP_FUNDING_LIMIT", "10"))


# ===== Order Flow Configuration =====

# Размер окна в барах для order flow анализа
OF_WINDOW_BARS: int = int(os.getenv("OF_WINDOW_BARS", "100"))

# Z-score порог для order flow сигналов
OF_Z_THRESHOLD: float = float(os.getenv("OF_Z_THRESHOLD", "2.0"))

# Порог соотношения для order flow сигналов
OF_RATIO_THRESHOLD: float = float(os.getenv("OF_RATIO_THRESHOLD", "1.5"))

# Минимальный размер тела свечи в ATR для order flow
OF_MIN_BODY_ATR: float = float(os.getenv("OF_MIN_BODY_ATR", "0.5"))

# Минимальный квантиль объема для order flow
OF_MIN_VOLUME_Q: float = float(os.getenv("OF_MIN_VOLUME_Q", "0.8"))

# Z-score порог для прокси order flow
OF_Z_THRESHOLD_PROXY: float = float(os.getenv("OF_Z_THRESHOLD_PROXY", "1.5"))

# Порог соотношения для прокси order flow
OF_RATIO_THRESHOLD_PROXY: float = float(os.getenv("OF_RATIO_THRESHOLD_PROXY", "1.2"))

# Минимальный квантиль объема для прокси order flow
OF_MIN_VOLUME_Q_PROXY: float = float(os.getenv("OF_MIN_VOLUME_Q_PROXY", "0.6"))

# Stream для баров order flow
OF_STREAM_BAR: str = os.getenv("OF_STREAM_BAR", "stream:of_bar")

# Stream для спайков order flow
OF_STREAM_SPIKE: str = os.getenv("OF_STREAM_SPIKE", "stream:of_spike")

# Consumer group для order flow
OF_CONSUMER_GROUP: str = os.getenv("OF_CONSUMER_GROUP", "of-consumer-group")

# Количество сообщений для чтения order flow
OF_READ_COUNT: int = int(os.getenv("OF_READ_COUNT", "10"))

# Таймаут блокировки для чтения order flow
OF_READ_BLOCK_MS: int = int(os.getenv("OF_READ_BLOCK_MS", "5000"))