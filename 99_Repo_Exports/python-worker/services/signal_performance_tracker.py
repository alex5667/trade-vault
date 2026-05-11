from utils.time_utils import get_ny_time_millis

"""
Signal Performance Tracker - Главный оркестратор системы отслеживания сигналов.

ФУНКЦИОНАЛ:
- Координация Trade Monitor, Stats Aggregator, Reporting Service
- Чтение сигналов из Redis Streams (signals:orderflow: signals:ta:)
- Чтение тиков из stream:tick_
- Обновление позиций в реальном времени
- Периодические задачи (отчеты каждые 3 часа, ежедневные сводки)
- Graceful shutdown
- Multi-threading для сигналов и тиков

ИНТЕГРАЦИЯ:
- TradeMonitor - отслеживание позиций
- StatsAggregator - обновление метрик
- ReportingService - генерация отчетов
- Telegram - уведомления через notify:telegram

ЗАПУСК:
    python -m services.signal_performance_tracker
    
Docker:
    См. docker-compose.yml секцию signal-performance-tracker

Senior Developer + Trading Analyst (40 years exp)
"""

import json
import os
import re
import signal as sig
import sys
import threading
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta
from typing import Any

import redis
from redis.exceptions import RedisError

# Добавляем путь к корню проекта
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from prometheus_client import Counter, Gauge, Histogram, start_http_server

from common.log import setup_logger
from core.redis_keys import RedisStreams as RS
from domain.models import PositionState
from domain.normalizers import canon_source, canon_symbol
from regime.guard import RegimeGuardService
from regime.signal_logger import SignalLogger
from regime.signal_monitor import SignalQualityMonitor
from services.deduper import RedisDeduper, env_int
from services.embedded_periodic_reporter import EmbeddedPeriodicReporter
from services.entry_tag_analytics import analyze_by_entry_tag, load_trades
from services.reporting_service import ReportingService
from services.sharded_serial_executor import ShardedSerialExecutor
from services.stream_worker import StreamWorker, WorkerPolicy
from services.tp_config import parse_tp_ratio as _parse_tp_ratio  # Для обратной совместимости
from services.tp_hit_trailing_orchestrator import TpHitTrailingOrchestrator
from services.trade_monitor import TradeMonitorService
from services.trade_monitor_actor_runtime import TradeMonitorActorRuntime
from utils.atr_cache import get_atr_cache
import contextlib

# ----------------- Prometheus Metrics (Module Level) -----------------
# Define metrics globally to prevent "Duplicated timeseries" if instantiated multiple times.
METRIC_SIGNAL_LATENCY = Histogram(
    "signal_processing_time_ms",
    "End-to-end signal processing time (entry_ts to processing)",
    ["strategy", "symbol"],
    buckets=[100, 500, 1000, 3000, 5000, 10000]
)
METRIC_TICK_LATENCY = Histogram(
    "tracker_tick_processing_time_us",
    "Latency of tick processing in SignalPerformanceTracker",
    ["symbol"],
    buckets=[50, 200, 500, 1000, 5000, 20000]
)

# COUNTERS
METRIC_SIGNAL_LOGGING_FAILED = Counter(
    "signal_logging_failed_total",
    "Total number of signal logging failures to PostgreSQL"
)
METRIC_DEDUP_HITS = Counter(
    "dedup_hits_total",
    "Total number of deduplicator hits",
    ["type"] # signal or event
)

# GAUGES
METRIC_STREAM_LAG = Gauge(
    "redis_stream_lag_ms",
    "Lag between last entry in Redis stream and current processing time",
    ["stream"]
)
# --------------------------------------------------------------------


def _parse_csv_env(name: str, default: list[str] | None = None) -> list[str]:
    value = os.getenv(name)
    if value is None:
        return list(default) if default else []
    items = [item.strip() for item in value.split(",") if item.strip()]
    if not items and default:
        return list(default)
    return items


_SYMBOL_ALIAS_MAP: dict[str, str] = {}

_SAFE_SYMBOLS_CACHE: set[str] = set()
_SAFE_STRATS_CACHE: set[str] = set()

def _sanitize_prom_label(val: Any, cache: set[str], max_len: int, fallback: str, max_cache: int = 500) -> str:
    s = str(val or fallback).strip()[:max_len]
    if s in cache:
        return s
    if len(cache) < max_cache:
        s_clean = re.sub(r'[^a-zA-Z0-9_-]', '', s)
        if not s_clean:
            s_clean = fallback
        cache.add(s_clean)
        return s_clean
    return fallback + "_OOM"

def _safe_symbol(val: Any) -> str:
    return _sanitize_prom_label(val, _SAFE_SYMBOLS_CACHE, 24, "UNKNOWN", 1000).upper()

def _safe_strategy(val: Any) -> str:
    return _sanitize_prom_label(val, _SAFE_STRATS_CACHE, 32, "unknown", 500).lower()


# Recommendation D: Pydantic model for signal validation
@dataclass(slots=True)
class IncomingSignal:
    sid: str
    symbol: str
    direction: str
    strategy: str = "cryptoorderflow"
    source: str = "CryptoOrderFlow"
    tf: str = "tick"
    price: float = 0.0
    ts: int = 0
    payload: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        self.symbol = str(self.symbol).upper() if self.symbol else ""
        v_upper = str(self.direction).upper()
        if v_upper in ("BUY", "LONG"):
            self.direction = "LONG"
        elif v_upper in ("SELL", "SHORT"):
            self.direction = "SHORT"
        else:
            self.direction = v_upper

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "IncomingSignal":
        sid = str(data.get("sid") or data.get("signal_id") or "")
        symbol = (data.get("symbol") or "")
        direction = str(data.get("direction") or data.get("side") or "")
        if not sid or not symbol or not direction:
            raise ValueError(f"Missing required fields: sid='{sid}', symbol='{symbol}', direction='{direction}'")

        try:
            price = float(data.get("price") or 0.0)
        except (ValueError, TypeError):
            price = 0.0

        try:
            ts = int(data.get("ts") or data.get("entry_ts_ms") or 0)
        except (ValueError, TypeError):
            ts = 0

        strategy = (data.get("strategy") or "cryptoorderflow")
        source = (data.get("source") or "CryptoOrderFlow")
        tf = (data.get("tf") or "tick")

        return cls(
            sid=sid,
            symbol=symbol,
            direction=direction,
            strategy=strategy,
            source=source,
            tf=tf,
            price=price,
            ts=ts,
            payload=dict(data)
        )


class SignalPerformanceTracker:
    """
    Главный оркестратор системы отслеживания эффективности сигналов.
    
    Координирует:
    - Trade Monitor (отслеживание позиций)
    - Stats Aggregator (метрики)
    - Reporting Service (отчеты)
    
    Запускает многопоточную обработку:
    - Поток 1: Чтение сигналов
    - Поток 2: Чтение тиков
    - Поток 3: Периодические задачи (отчеты)
    """

    def __init__(self, config: dict | None = None):
        """
        Инициализация Signal Performance Tracker.
        
        Args:
            config: Конфигурация трекера
        """
        self.logger = setup_logger("SignalPerformanceTracker")

        # Конфигурация
        self.config = config or self._load_config_from_env()

        # Redis URL
        self.redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1-worker-1:6379/0")
        self.redis_ticks_url = os.getenv("REDIS_TICKS_URL")

        # Создаем Redis клиент для сигналов/событий
        try:
            self.redis = redis.from_url(self.redis_url, decode_responses=True)
            self.redis.ping()
            self.logger.info(f"✅ Redis подключение установлено: {self.redis_url}")
        except Exception as e:
            self.logger.error(f"❌ Не удалось подключиться к основному Redis: {e}")
            raise

        # Оркестратор трейлинга TP1 (используем общий Redis клиент)
        self.trailing_orchestrator = TpHitTrailingOrchestrator(redis_client=self.redis)

        # ATR Cache for dynamic trailing (Phase 4)
        self.atr_cache = get_atr_cache()

        # Отдельный Redis клиент для тиков (если указан)
        self.redis_ticks = self.redis
        if self.redis_ticks_url:
            try:
                self.redis_ticks = redis.from_url(self.redis_ticks_url, decode_responses=True)
                self.redis_ticks.ping()
                self.logger.info(f"✅ Redis (ticks) подключение установлено: {self.redis_ticks_url}")
            except Exception as e:
                self.logger.error(
                    f"⚠️ Не удалось подключиться к Redis тик-фиду ({self.redis_ticks_url}), "
                    f"используем основной Redis. Ошибка: {e}"
                )
                self.redis_ticks = self.redis

        # Regime Guard Service для контроля качества сигналов (опционально)
        self.regime_guard = None
        try:
            # Используем TRADES_DB_DSN для доступа к scanner_analytics базе данных
            from services.analytics_db import TRADES_DB_DSN
            pg_dsn = TRADES_DB_DSN
            baseline_horizon_days = int(os.getenv("BASELINE_HORIZON_DAYS", "180"))
            self.regime_guard = RegimeGuardService(
                pg_dsn, self.redis_url, baseline_horizon_days=baseline_horizon_days
            )
            self.logger.info("✅ RegimeGuardService initialized")
        except Exception as e:
            self.logger.warning(f"⚠️ RegimeGuardService unavailable (PostgreSQL connection failed): {e}. Continuing without regime control.")

        # Инициализируем компоненты
        self.trade_monitor = TradeMonitorService(
            redis_url=self.redis_url,
            config=self.config,
            regime_guard=self.regime_guard,
            atr_cache=self.atr_cache  # [PHASE 4]
        )

        self.reporting_service = ReportingService(
            redis_url=self.redis_url,
            telegram_config=self.config.get("telegram")
        )
        self.periodic_reporter = EmbeddedPeriodicReporter()
        if self.periodic_reporter.available():
            self.logger.info("🔁 Embedded PeriodicReporter initialized")
        else:
            self.logger.warning("⚠️ Embedded PeriodicReporter unavailable; using fallback summary")

        self.health_key = os.getenv("TRACKER_HEALTH_KEY", "health:signal_performance_tracker")
        self.health_ttl = int(os.getenv("TRACKER_HEALTH_TTL", "300"))

        self.crypto_raw_stream = os.getenv("CRYPTO_RAW_STREAM", RS.CRYPTO_RAW)

        # --- Symbol executor (actor-like serialization) ---
        self._exec: ShardedSerialExecutor | None = None
        self._use_symbol_exec = os.getenv("USE_SYMBOL_EXECUTOR", "1") == "1"
        self._exec_shards = int(os.getenv("SYMBOL_EXECUTOR_SHARDS", "8"))
        self._exec_queue_max = int(os.getenv("SYMBOL_EXECUTOR_QUEUE_MAX", "20000"))
        self._exec_submit_timeout_s = float(os.getenv("SYMBOL_EXECUTOR_SUBMIT_TIMEOUT_S", "2.0"))
        self._exec_task_timeout_s = float(os.getenv("SYMBOL_EXECUTOR_TASK_TIMEOUT_S", "30.0"))

        # --- Actor runtime (core-per-shard, no shared state) ---
        self.tm_runtime: TradeMonitorActorRuntime | None = None
        self._use_actor_runtime = os.getenv("USE_TM_ACTOR_RUNTIME", "1") == "1"


        # Монитор качества сигналов с L3-метриками
        self.quality_monitor = SignalQualityMonitor(max_history_days=30)

        # Logger для сигналов с L3-метриками в TimescaleDB
        self.signal_logger = None
        try:
            pg_dsn = os.getenv("DATABASE_URL")
            self.signal_logger = SignalLogger(pg_dsn)  # type: ignore
            self.logger.info("✅ SignalLogger initialized")  # type: ignore
        except Exception as e:
            self.logger.warning(f"⚠️ SignalLogger unavailable (PostgreSQL connection failed): {e}. Continuing without signal logging.")

        # Streams для прослушивания
        streams_cfg = self.config.get("streams", {})
        symbols_cfg = streams_cfg.get("symbols", ["BTCUSDT", "ETHUSDT"])
        strategies_cfg = streams_cfg.get("strategies", ["orderflow", "ta", "aggregated"])

        raw_symbols = [sym.upper() for sym in symbols_cfg if sym]
        canonical_symbols: list[str] = []
        canonical_set: set[str] = set()
        signal_symbols: list[str] = []
        signal_set: set[str] = set()

        self.symbol_alias_lookup: dict[str, str] = {}
        self.canonical_aliases: dict[str, set[str]] = {}

        for sym in raw_symbols:
            canonical = self._normalize_symbol(sym)
            if canonical and canonical != sym:
                self._register_alias(sym, canonical)

            if sym and sym not in signal_set:
                signal_symbols.append(sym)
                signal_set.add(sym)

            if canonical and canonical not in signal_set:
                signal_symbols.append(canonical)
                signal_set.add(canonical)

            if canonical and canonical not in canonical_set:
                canonical_symbols.append(canonical)
                canonical_set.add(canonical)

        for alias, canonical in _SYMBOL_ALIAS_MAP.items():
            if canonical in canonical_set and alias not in signal_set:
                self._register_alias(alias, canonical)
                signal_symbols.append(alias)
                signal_set.add(alias)

        self.base_symbols = list(canonical_symbols)
        self.symbols = list(self.base_symbols)
        self._symbol_set = set(self.symbols)
        self.signal_symbols = list(signal_symbols)
        self._signal_symbol_set: set[str] = set(self.signal_symbols)
        self._symbol_revision = 0

        if self.symbol_alias_lookup:
            alias_info = ", ".join(
                f"{alias}→{target}" for alias, target in sorted(self.symbol_alias_lookup.items())
            )
            self.logger.info("🔄 Алиасы символов применены: %s", alias_info)

        self.strategies = [strategy.lower() for strategy in strategies_cfg if strategy]
        self.strategies = list(dict.fromkeys(self.strategies))

        self.dynamic_symbols_key = os.getenv("SYMBOLS_REDIS_KEY", "crypto:symbols")
        initial_dynamic_symbols = self._refresh_symbols_from_redis(initial=True)
        if initial_dynamic_symbols:
            self.logger.info(
                "🆕 Добавлены символы из %s: %s",
                self.dynamic_symbols_key,
                ", ".join(sorted(initial_dynamic_symbols)),
            )

        # Consumer группы
        self.signal_group = self.config.get("consumer_group", "signal-tracker-group")
        self.tick_group = f"{self.signal_group}-ticks"
        self.events_group = f"{self.signal_group}-events"
        self.consumer_name = self.config.get("consumer_name", f"tracker-{int(time.time())}")

        # Потоки
        self.threads: list[threading.Thread] = []
        self.running = False

        # Статистика
        self.stats = {
            "signals_processed": 0,
            "ticks_processed": 0,
            "positions_opened": 0,
            "positions_closed": 0,
            "last_signal_time": None,
            "last_tick_time": None,
            "start_time": time.time(),
            "trailing_updates_applied": 0,
            "trailing_updates_missed": 0,
            "external_sl_synced": 0,
            "external_sl_missed": 0,
            "tp1_internal_hits": 0,
            "trailing_internal_started": 0,
            "trailing_internal_failed": 0,
            "dedup_signals_skipped": 0,
            "dedup_events_skipped": 0,
            "dedup_reports_skipped": 0,
        }

        # Recommendation 4: Observability (Prometheus)
        # Reference module-level globals
        self.metric_signal_latency = METRIC_SIGNAL_LATENCY
        self.metric_tick_latency = METRIC_TICK_LATENCY

        # COUNTERS
        self.metric_signal_logging_failed = METRIC_SIGNAL_LOGGING_FAILED
        self.metric_dedup_hits = METRIC_DEDUP_HITS

        # GAUGES
        self.metric_stream_lag = METRIC_STREAM_LAG

        # Start Prometheus exporter
        try:
            metrics_port = int(os.getenv("METRICS_PORT", "9091"))
            start_http_server(metrics_port)
            self.logger.info(f"📊 Prometheus metrics exported on port {metrics_port}")
        except Exception as e:
            self.logger.warning(f"⚠️ Failed to start Prometheus metrics server: {e}")

        # Entry-tag analytics (optional)
        self.entry_tag_analytics_enabled = os.getenv("ENTRY_TAG_ANALYTICS_ENABLED", "false").lower() == "true"
        self.entry_tag_analytics_limit = int(os.getenv("ENTRY_TAG_ANALYTICS_LIMIT", "1000"))
        self.entry_tag_min_trades = int(os.getenv("ENTRY_TAG_MIN_TRADES", "5"))

        # Инициализация deduper
        self.deduper = RedisDeduper(self.redis, prefix=os.getenv("DEDUP_PREFIX", "dedup"))
        self.dedup_signals_ttl = env_int("DEDUP_SIGNALS_TTL_S", 2 * 24 * 3600)  # 2 дня
        self.dedup_events_ttl = env_int("DEDUP_EVENTS_TTL_S", 7 * 24 * 3600)  # 7 дней
        self.dedup_report_ttl = env_int("DEDUP_REPORT_TTL_S", 6 * 3600)  # 6 часов

        self.logger.info("🎯 Signal Performance Tracker инициализирован")
        self.logger.info(f"   Символы: {self.symbols}")
        self.logger.info(f"   Стратегии: {self.strategies}")
        self.logger.info(f"   Dedup TTL: signals={self.dedup_signals_ttl}s, events={self.dedup_events_ttl}s, reports={self.dedup_report_ttl}s")



    def _trade_monitor_core_factory(self, shard_id: int):
        """
        Factory for creating TradeMonitorCore instances.
        Currently returns a TradeMonitorService (which has locks removed conceptually).
        In full implementation, this would create isolated TradeMonitorCore instances.
        """
        # For now, create isolated instances (each with its own state)
        # In production, you'd want TradeMonitorCore without locks
        core = TradeMonitorService(
            redis_url=self.redis_url,
            config=self.config,
            regime_guard=self.regime_guard,
            atr_cache=self.atr_cache  # [PHASE 4]
        )
        # Mark as shard-local (conceptually single-threaded)
        core._shard_id = shard_id  # type: ignore
        return core  # type: ignore

    def _load_config_from_env(self) -> dict:
        """Загрузка конфигурации из переменных окружения."""

        # Загружаем из файла если указан
        config_path = os.getenv("TRACKER_CONFIG_PATH"),
        if config_path and os.path.exists(config_path):  # type: ignore
            try:  # type: ignore
                with open(config_path) as f:  # type: ignore
                    return json.load(f),  # type: ignore
            except Exception as e:  # type: ignore
                self.logger.warning(f"⚠️ Не удалось загрузить конфиг из {config_path}: {e}"),

        # Конфигурация по умолчанию из ENV
        symbols = _parse_csv_env("SYMBOLS", ["BTCUSDT", "ETHUSDT"]),
        strategies = _parse_csv_env("STRATEGIES", ["orderflow", "ta", "aggregated", "cryptoorderflow"]),
        # Preserve order while normalizing casing
        symbols = list(dict.fromkeys(sym.upper() for sym in symbols)),  # type: ignore
        strategies = list(dict.fromkeys(strategy.lower() for strategy in strategies)),  # type: ignore
  # type: ignore
        periodic_hours = int(os.getenv("PERIODIC_REPORT_HOURS", "1")),

        return {
            "streams": {
                "symbols": symbols,
                # ✅ Стратегии нормализованы в lower-case
                "strategies": strategies,
            },
            "consumer_group": os.getenv("CONSUMER_GROUP", "signal-tracker-group"),
            "consumer_name": os.getenv("CONSUMER_NAME", f"tracker-{int(time.time())}"),
            "monitor": {
                "default_lot": float(os.getenv("DEFAULT_LOT", "1.0")),
                "risk_pct": float(os.getenv("RISK_PCT", "1.0")),
                "stop_atr_mult": float(os.getenv("STOP_ATR_MULT", "1.0")),
                "rr_levels": [1.0, 2.0, 3.0],
                "tp_ratio": _parse_tp_ratio(),
                "notify_on_trade_close": os.getenv("NOTIFY_ON_TRADE_CLOSE", "false").lower() == "true"
            },
            "telegram": {
                "bot_token": os.getenv("TELEGRAM_BOT_TOKEN"),
                "chat_id": os.getenv("TELEGRAM_CHAT_ID")
            },
            "reporting": {
                "periodic_interval_hours": periodic_hours,
                "daily_summary_enabled": os.getenv("DAILY_SUMMARY", "true").lower() == "true",
                "daily_summary_hour": int(os.getenv("DAILY_SUMMARY_HOUR", "0"))
            }
        }

    def _normalize_symbol(self, symbol: str | None) -> str | None:
        """Возвращает каноническое имя символа с учетом алиасов."""
        if symbol is None:
            return None
        sym_upper = symbol.strip().upper()
        if not sym_upper:
            return None
        if sym_upper in self.symbol_alias_lookup:
            return self.symbol_alias_lookup[sym_upper]
        canonical = _SYMBOL_ALIAS_MAP.get(sym_upper)
        if canonical:
            return canonical
        return sym_upper

    def _register_alias(self, alias: str, canonical: str) -> None:
        """Регистрирует соответствие алиаса и канонического символа."""
        alias_upper = alias.strip().upper()
        canonical_upper = canonical.strip().upper()
        if not alias_upper or not canonical_upper or alias_upper == canonical_upper:
            return

        existing = self.symbol_alias_lookup.get(alias_upper)
        added = alias_upper not in self.symbol_alias_lookup
        if existing and existing != canonical_upper:
            self.logger.warning(
                "⚠️ Перезапись алиаса символа %s: %s → %s",
                alias_upper,
                existing,
                canonical_upper,
            )

        self.symbol_alias_lookup[alias_upper] = canonical_upper
        if canonical_upper not in self.canonical_aliases:
            self.canonical_aliases[canonical_upper] = set()
        if alias_upper not in self.canonical_aliases[canonical_upper]:
            self.canonical_aliases[canonical_upper].add(alias_upper)
            added = True

        if added and hasattr(self, "_symbol_revision"):
            self._symbol_revision += 1

    def _get_signal_variants(self, symbol: str | None) -> list[str]:
        """Возвращает список вариантов имен для подписки на сигнал (канонический + алиасы)."""
        variants: list[str] = []
        seen: set[str] = set()

        canonical = self._normalize_symbol(symbol)
        if canonical:
            variants.append(canonical)
            seen.add(canonical)

        symbol_upper = symbol.strip().upper() if symbol else ""
        if symbol_upper and symbol_upper not in seen:
            variants.append(symbol_upper)
            seen.add(symbol_upper)

        if canonical:
            for alias in sorted(self.canonical_aliases.get(canonical, set())):
                if alias not in seen:
                    variants.append(alias)
                    seen.add(alias)

        return variants

    def _normalize_signal_payload(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Применяет алиасы к символу внутри payload."""
        canonical = self._normalize_symbol(payload.get("symbol"))
        if canonical:
            payload["symbol"] = canonical
        return payload

    def _extract_json_payload(self, data: dict[str, Any], keys=("payload", "data")) -> dict[str, Any] | None:
        """Извлекает JSON payload из сообщения."""
        raw = None
        for k in keys:
            if data.get(k):
                raw = data.get(k)
                break
        if raw is None:
            return None
        if isinstance(raw, dict):
            return dict(raw)
        if isinstance(raw, str):
            try:
                return json.loads(raw)
            except json.JSONDecodeError:
                return None
        return None

    def _merge_data_field(self, data: dict[str, Any]) -> dict[str, Any]:
        """Мержит поле 'data' (JSON) с основными полями сообщения."""
        out = dict(data)
        if "data" in out and out["data"]:
            if isinstance(out["data"], str):
                try:
                    parsed = json.loads(out["data"])
                    if isinstance(parsed, dict):
                        out.pop("data", None)
                        out = {**parsed, **out}
                except json.JSONDecodeError:
                    pass
            elif isinstance(out["data"], dict):
                parsed = dict(out["data"])
                out.pop("data", None)
                out = {**parsed, **out}
        return out

    def _prepare_signal_message(self, message: dict[str, Any]) -> dict[str, Any]:
        """
        Подготавливает сообщение сигнала к обработке: нормализует символ и сериализует payload.
        """
        prepared = dict(message)

        if "data" in prepared and prepared["data"]:
            raw_payload = prepared["data"]

            if isinstance(raw_payload, str):
                try:
                    payload_dict = json.loads(raw_payload)
                except json.JSONDecodeError as err:
                    self.logger.warning("⚠️ Некорректный JSON в сигнале: %s", err)
                else:
                    payload_dict = self._normalize_signal_payload(payload_dict)
                    prepared["data"] = json.dumps(payload_dict)

            elif isinstance(raw_payload, dict):
                payload_dict = self._normalize_signal_payload(dict(raw_payload))
                prepared["data"] = json.dumps(payload_dict)

        else:
            self._normalize_signal_payload(prepared)

        return prepared

    def _signal_dedup_key(self, stream: str, msg_id: str, payload: dict[str, Any] | None) -> str:
        """
        Формирует ключ дедупликации для сигнала.
        Приоритет: sid > (stream, msg_id)
        """
        sid = None
        if isinstance(payload, dict):
            sid = payload.get("sid") or payload.get("signal_id") or payload.get("id")
        if sid:
            return self.deduper.key("signals", str(sid))
        return self.deduper.key("signals", stream, msg_id)

    def _process_signal_message(self, stream: str, msg_id: str, data: dict[str, Any]) -> bool:
        """
        Processor для сигналов (lossless).
        return True => ACK, False/Exception => retry policy decides
        """
        try:
            payload = None

            if stream == self.crypto_raw_stream:
                payload = self._extract_json_payload(data, keys=("payload", "data"))
                # Unwrap nested payload if present (common in outbox pattern)
                if payload and "payload" in payload:
                    inner = payload["payload"]
                    if isinstance(inner, str):
                        with contextlib.suppress(Exception):
                            inner = json.loads(inner)
                    if isinstance(inner, dict):
                        payload = inner

                if not payload:
                    self.logger.warning("Empty/invalid crypto raw payload: %s", msg_id)
                    return True  # ACK (пустое сообщение не имеет смысла ретраить)

                # Recommendation 4: Track stream lag
                try:
                    ts_part = str(msg_id).split("-")[0]
                    lag = get_ny_time_millis() - int(ts_part)
                    self.metric_stream_lag.labels(stream=stream).set(lag)
                except Exception:
                    pass

                payload = self._normalize_signal_payload(payload)
                payload.setdefault("strategy", "cryptoorderflow")
                payload.setdefault("source", "CryptoOrderFlow")
                payload.setdefault("tf", "tick")

            elif stream.startswith("signals:cryptoorderflow:"):
                payload = self._extract_json_payload(data, keys=("payload", "data"))
                # Unwrap nested payload if present
                if payload and "payload" in payload:
                    inner = payload["payload"]
                    if isinstance(inner, str):
                        with contextlib.suppress(Exception):
                            inner = json.loads(inner)
                    if isinstance(inner, dict):
                        payload = inner

                if not payload:
                    self.logger.warning("Empty/invalid cryptoorderflow payload: %s", msg_id)
                    return True
                payload = self._normalize_signal_payload(payload)
                payload.setdefault("strategy", "cryptoorderflow")
                payload.setdefault("source", "CryptoOrderFlow")
                payload.setdefault("tf", "tick")

            else:
                # обычные сигналы — оставляем как у вас, но для dedup попробуем достать JSON
                payload = self._extract_json_payload(data, keys=("data", "payload"))
                if isinstance(payload, dict):
                    payload = self._normalize_signal_payload(payload)

            # --- DEDUP gate ---
            dkey = self._signal_dedup_key(stream, msg_id, payload)
            first = self.deduper.acquire(dkey, ttl_s=self.dedup_signals_ttl, value=str(int(time.time())))
            if not first:
                self.stats["dedup_signals_skipped"] += 1
                # Recommendation 4: Dedup metric
                self.metric_dedup_hits.labels(type="signal").inc()
                self.logger.debug("DEDUP signal skip: %s %s", stream, msg_id)
                return True  # важно: ACK будет сделан воркером

            # Recommendation D: Use Pydantic for robust parsing
            try:
                # Prepare data for Pydantic (flattened)
                input_data = dict(payload) if payload else {}
                if "data" in data and isinstance(data["data"], dict):
                    input_data.update(data["data"])

                # If sid/signal_id not in payload, look for it in top-level data
                if "sid" not in input_data and "signal_id" not in input_data:
                    for k in ("sid", "signal_id"):
                        if data.get(k):
                            input_data[k] = data[k]

                sig_obj = IncomingSignal.from_dict(input_data)
                symbol = sig_obj.symbol
                sid = sig_obj.sid

                # Update payload for downstream compatibility
                payload = asdict(sig_obj)
            except Exception as e:
                self.logger.warning(f"Invalid signal format: {e}, msg_id={msg_id}")
                return True # ACK invalid signals but log warnings

            # Use actor runtime if available (preferred)
            if self._use_actor_runtime and self.tm_runtime is not None:
                signal_payload = payload
                if not signal_payload and (stream == self.crypto_raw_stream or stream.startswith("signals:cryptoorderflow:")):
                    signal_payload = payload  # already processed
                elif not signal_payload:
                    signal_payload = self._prepare_signal_message(data)

                # Log signal directly before submitting to actor using same logic as standard run
                if self.signal_logger and signal_payload:
                    try:
                        from regime.signal_snapshot import SignalSnapshot
                        snapshot = SignalSnapshot.from_dict(signal_payload)
                        self.signal_logger.log_signal(snapshot)
                    except Exception as log_err:
                        self.metric_signal_logging_failed.inc()
                        self.logger.warning(f"Failed to log signal to PostgreSQL (actor mode): {log_err}")

                fut = self.tm_runtime.submit_signal(symbol=symbol, raw_signal=signal_payload)
                try:
                    pos_id = fut.result(timeout=float(os.getenv("TM_ACTOR_TASK_TIMEOUT_S", "30.0")))
                    if pos_id:
                        self.stats["signals_processed"] += 1
                        self.stats["positions_opened"] += 1
                        self.stats["last_signal_time"] = time.time()
                    return True
                except Exception as e:
                    self.logger.warning("signal handler failed (actor lossless -> retry/DLQ): %s", e)
                    return False

            # Fallback to direct call or executor
            key = self._route_key_for_symbol_or_sid(symbol if symbol else None, str(sid) if sid else None)

            def _run():
                # --- main processing (side effects allowed) ---
                if payload and (stream == self.crypto_raw_stream or stream.startswith("signals:cryptoorderflow:")):
                    pos_id = self.trade_monitor.process_signal(payload)
                    signal_to_log = payload
                else:
                    prepared = self._prepare_signal_message(data)
                    pos_id = self.trade_monitor.process_signal(prepared)
                    signal_to_log = prepared

                if pos_id:
                    self.stats["signals_processed"] += 1
                    self.stats["positions_opened"] += 1
                    self.stats["last_signal_time"] = time.time()

                    # Log signal to PostgreSQL
                    if self.signal_logger and signal_to_log:
                        try:
                            from regime.signal_snapshot import SignalSnapshot
                            # Create snapshot from signal payload
                            snapshot = SignalSnapshot.from_dict(signal_to_log)
                            self.signal_logger.log_signal(snapshot)
                        except Exception as log_err:
                            # Recommendation 3: Log failure metric
                            self.metric_signal_logging_failed.inc()
                            # Fail-open: don't block signal processing if logging fails
                            self.logger.warning(f"Failed to log signal to PostgreSQL: {log_err}")

                # Recommendation 4: End-to-end latency metric
                try:
                    entry_ts = int(payload.get("entry_ts_ms") or payload.get("ts") or 0)
                    if entry_ts > 0:
                        lat_ms = get_ny_time_millis() - entry_ts
                        self.metric_signal_latency.labels(
                            strategy=_safe_strategy(payload.get("strategy")),
                            symbol=_safe_symbol(payload.get("symbol"))
                        ).observe(lat_ms)
                except Exception:
                    pass

            return self._exec_call_lossless(key, _run, name=f"signal:{symbol}:{sid}:{msg_id}")

        except Exception as e:
            # lossless: пусть ретраи решаются политикой воркера
            self.logger.error("Signal processing failed (%s): %s", msg_id, e)
            raise

    def _process_tick_message(self, stream: str, msg_id: str, data: dict[str, Any]) -> bool:
        """
        Processor для тиков (realtime, best-effort, ACK всегда).
        """
        t_start = time.perf_counter()
        # Recommendation 4: Track stream lag
        try:
            ts_part = str(msg_id).split("-")[0]
            lag = get_ny_time_millis() - int(ts_part)
            self.metric_stream_lag.labels(stream=stream).set(lag)
        except Exception:
            pass
        tick_data = self._merge_data_field(data)

        raw_symbol = None
        canonical_symbol = None
        try:
            _, raw_symbol = stream.split("tick_", 1)
            canonical_symbol = self._normalize_symbol(raw_symbol)
        except Exception:
            pass

        # Обработка алиасов символов (как в оригинальном коде)
        if raw_symbol and canonical_symbol and canonical_symbol != raw_symbol:
            mapped = self.symbol_alias_lookup.get(raw_symbol)
            if mapped != canonical_symbol:
                self._register_alias(raw_symbol, canonical_symbol)
        elif raw_symbol and not canonical_symbol:
            canonical_symbol = raw_symbol

        if canonical_symbol:
            tick_data["symbol"] = canonical_symbol
        elif raw_symbol:
            tick_data["symbol"] = raw_symbol
        if raw_symbol:
            tick_data["symbol_alias"] = raw_symbol

        symbol = (canonical_symbol or raw_symbol or "").upper()

        # Use actor runtime if available (preferred)
        if self._use_actor_runtime and self.tm_runtime is not None:
            fut = self.tm_runtime.submit_tick(symbol=symbol, raw_tick=tick_data)
            try:
                fut.result(timeout=float(os.getenv("TM_ACTOR_TASK_TIMEOUT_S", "30.0")))
                self.stats["ticks_processed"] += 1
                self.stats["last_tick_time"] = time.time()

                # Recommendation 4: Prometheus tick latency
                t_dur_us = (time.perf_counter() - t_start) * 1_000_000
                self.metric_tick_latency.labels(symbol=symbol).observe(t_dur_us)
                return True
            except Exception as e:
                self.logger.warning("tick handler failed (actor lossless -> retry/DLQ): %s %s", e, repr(e))
                return False

        # Fallback to direct call or executor
        key = self._route_key_for_symbol_or_sid(symbol if symbol else None, None)
        def _run():
            self.trade_monitor.on_tick(tick_data)
            self.stats["ticks_processed"] += 1
            self.stats["last_tick_time"] = time.time()

            # Recommendation 4: Prometheus tick latency
            t_dur_us = (time.perf_counter() - t_start) * 1_000_000
            self.metric_tick_latency.labels(symbol=symbol).observe(t_dur_us)

        return self._exec_call_lossless(key, _run, name=f"tick:{symbol}:{msg_id}")

    def _event_dedup_key(self, msg_id: str, data: dict[str, Any]) -> str:
        """
        Формирует ключ дедупликации для события.
        Приоритет: (event_type, sid) > (msg_id)
        """
        et = (data.get("event_type") or data.get("event") or "unknown").upper()
        sid = data.get("sid") or data.get("signal_id")
        if sid:
            return self.deduper.key("events", et, str(sid))
        return self.deduper.key("events", "msg", msg_id)

    def _process_event_message(self, stream: str, msg_id: str, data: dict[str, Any]) -> bool:
        """
        Processor для событий (lossless).
        """
        # Recommendation 4: Track stream lag
        try:
            ts_part = str(msg_id).split("-")[0]
            lag = get_ny_time_millis() - int(ts_part)
            self.metric_stream_lag.labels(stream=stream).set(lag)
        except Exception:
            pass

        # --- DEDUP gate ---
        dkey = self._event_dedup_key(msg_id, data)
        first = self.deduper.acquire(dkey, ttl_s=self.dedup_events_ttl, value=str(int(time.time())))
        if not first:
            self.stats["dedup_events_skipped"] += 1
            # Recommendation 4: Dedup hit metric
            self.metric_dedup_hits.labels(type="event").inc()
            self.logger.debug("DEDUP event skip: %s", msg_id)
            return True

        # resolve sid early for routing
        sid = data.get("sid") or data.get("signal_id") or ""
        sym = None
        try:
            # best-effort: sid->symbol from trade_monitor (if open)
            sym = self.trade_monitor.peek_symbol_by_sid(str(sid)) if sid else None
        except Exception:
            sym = None
        key = self._route_key_for_symbol_or_sid(sym, str(sid) if sid else None)

        event_type = (data.get("event_type") or data.get("event") or "").upper()

        if event_type == "TRAILING_STARTED":
            # Recommendation A: Remove early return if source is orchestrator.
            # (dedup already protects against double processing)

            sid = data.get("sid")
            new_sl_raw = data.get("new_sl")
            if not sid or new_sl_raw is None:
                self.stats["trailing_updates_missed"] += 1
                return True  # нет смысла ретраить

            new_sl = float(new_sl_raw)

            clear_tp_levels = False
            clear_flag_raw = data.get("clear_tp_levels")
            if clear_flag_raw is not None:
                if isinstance(clear_flag_raw, str):
                    clear_tp_levels = clear_flag_raw.strip().lower() in {"1", "true", "yes", "on"}
                else:
                    clear_tp_levels = bool(clear_flag_raw)

            # Use actor runtime if available (preferred)
            if self._use_actor_runtime and self.tm_runtime is not None:
                fut = self.tm_runtime.submit_event(
                    symbol=sym,
                    sid=str(sid),
                    fn_name="trailing_started",
                    payload={
                        "new_sl": new_sl,
                        "source": data.get("source"),
                        "profile": data.get("profile"),
                        "clear_tp_levels": clear_tp_levels,
                        "event_id": msg_id,
                    }
                )
                try:
                    ok = fut.result(timeout=float(os.getenv("TM_ACTOR_TASK_TIMEOUT_S", "30.0")))
                    if ok:
                        self.stats["trailing_updates_applied"] += 1
                    else:
                        self.stats["trailing_updates_missed"] += 1
                    return True
                except Exception as e:
                    self.logger.warning("event TRAILING_STARTED failed (actor lossless -> retry/DLQ): %s %s", e, repr(e))
                    return False

            # Fallback to direct call or executor
            def _run():
                ok = self.trade_monitor.update_trailing_sl(
                    signal_id=sid,
                    new_sl=new_sl,
                    source=data.get("source"),
                    profile=data.get("profile"),
                    event_id=msg_id,
                    clear_tp_levels=clear_tp_levels,
                )
                if ok:
                    self.stats["trailing_updates_applied"] += 1
                else:
                    self.stats["trailing_updates_missed"] += 1
            return self._exec_call_lossless(key, _run, name=f"event:TRAILING_STARTED:{sid}")

        if event_type == "SL_HIT":
            sid = data.get("sid") or data.get("signal_id")
            price_raw = data.get("price") or data.get("exit_price") or data.get("sl")
            if not sid or price_raw is None:
                self.stats["external_sl_missed"] += 1
                return True

            price_value = float(price_raw)

            from domain.time_utils import normalize_ts_ms
            ts_raw = data.get("ts") or data.get("timestamp")
            ts_value = normalize_ts_ms(ts_raw) or get_ny_time_millis()

            # Use actor runtime if available (preferred)
            if self._use_actor_runtime and self.tm_runtime is not None:
                fut = self.tm_runtime.submit_event(
                    symbol=sym,
                    sid=str(sid),
                    fn_name="sl_hit",
                    payload={
                        "price": price_value,
                        "ts": ts_value,
                        "source": data.get("source"),
                        "event_id": msg_id,
                    }
                )
                try:
                    applied = fut.result(timeout=float(os.getenv("TM_ACTOR_TASK_TIMEOUT_S", "30.0")))
                    if applied:
                        self.stats["external_sl_synced"] += 1
                    else:
                        self.stats["external_sl_missed"] += 1
                    return True
                except Exception as e:
                    self.logger.warning("event SL_HIT failed (actor lossless -> retry/DLQ): %s %s", e, repr(e))
                    return False

            # Fallback to direct call or executor
            def _run():
                applied = self.trade_monitor.apply_external_sl_hit(
                    signal_id=sid,
                    price=price_value,
                    timestamp=ts_value,
                    source=data.get("source"),
                    event_id=msg_id,
                )
                if applied:
                    self.stats["external_sl_synced"] += 1
                else:
                    self.stats["external_sl_missed"] += 1
            return self._exec_call_lossless(key, _run, name=f"event:SL_HIT:{sid}")

        return True

    def _handle_tp_event(self, position: PositionState, tp_event: dict[str, Any]) -> None:
        """Хендлер локального события TP для запуска трейлинга."""
        try:
            level = tp_event.get("level")
            if level != 1:
                return

            if not position.sid:
                self.logger.debug(
                    "TP1 event for position %s without sid, skip trailing",
                    position.id
                )
                return

            price_raw = tp_event.get("price")
            if price_raw is None:
                self.logger.debug(
                    "TP1 event for %s without price, skip trailing",
                    position.sid
                )
                return

            try:
                price_value = float(price_raw),
            except (TypeError, ValueError):
                self.logger.warning(
                    "⚠️ Invalid TP1 price for %s: %s",
                    position.sid,
                    price_raw
                )
                return

            from domain.time_utils import normalize_ts_ms

            self.stats["tp1_internal_hits"] += 1

            trailing_result = self.trailing_orchestrator.start_trailing(
                sid=position.sid,
                symbol=position.symbol,
                price=price_value,  # type: ignore
                position_id=None,  # type: ignore
                source=position.source,
                event_ts=normalize_ts_ms(tp_event.get("timestamp")) or get_ny_time_millis()
            )

            if trailing_result.skipped:
                self.logger.info(
                    "ℹ️ Trailing skipped for %s (reason=%s)",
                    position.sid,
                    trailing_result.error or "skipped",
                )
                return

            if not trailing_result.success or trailing_result.new_sl is None:
                self.stats["trailing_internal_failed"] += 1
                self.logger.warning(
                    "⚠️ Trailing failed for %s: %s",
                    position.sid,
                    trailing_result.error or "unknown_error"
                )
                return

            metadata = trailing_result.metadata or {}
            clear_tp = bool(metadata.get("tp_levels_cleared"))
            trail_dist = metadata.get("trail_distance_price")
            point_size = metadata.get("point_size")

            applied = self.trade_monitor.apply_trailing_sl_sync(
                sid=position.sid,
                new_sl=trailing_result.new_sl,
                ts_ms=int(tp_event.get("timestamp") or get_ny_time_millis()),
                trailing_distance=trail_dist if trail_dist else 0.0,
                point_size=point_size if point_size else 0.0,
                clear_future_tp_levels=clear_tp,
            )

            if applied:
                self.stats["trailing_internal_started"] += 1
                # Note: PositionState doesn't have signal_payload, trailing is handled by TradeMonitorService
                self.logger.info(
                    "✅ Trailing SL applied for %s new_sl=%.5f",
                    position.sid,
                    trailing_result.new_sl
                )
            else:
                self.stats["trailing_internal_failed"] += 1
                self.logger.warning(
                    "⚠️ Trailing SL update skipped for %s (position already closed)",
                    position.sid
                )

        except Exception as exc:
            self.stats["trailing_internal_failed"] += 1
            self.logger.error(
                "❌ Unexpected error handling TP1 event for %s: %s",
                position.sid or "unknown",
                exc,
            )

    def _create_consumer_groups(self) -> None:
        """Создание consumer groups для всех streams."""

        # Signal streams
        signal_streams = []
        for strategy in self.strategies:
            for symbol in self.signal_symbols:
                stream_name = f"signals:{strategy}:{symbol}"
                signal_streams.append(stream_name)

        for stream in signal_streams:
            max_retries = 10
            retry_count = 0
            while retry_count < max_retries:
                try:
                    self.redis.xgroup_create(stream, self.signal_group, id='0', mkstream=True)
                    self.logger.info(f"✅ Consumer group created for {stream}")
                    break
                except RedisError as e:
                    if "BUSYGROUP" in str(e):
                        self.logger.debug(f"   Group already exists: {stream}")
                        break
                    elif "Redis is loading the dataset in memory" in str(e):
                        retry_count += 1
                        wait_time = min(2 * retry_count, 30)  # Exponential backoff, max 30 seconds
                        self.logger.warning(f"⚠️ Redis is loading data, retrying {stream} group creation (attempt {retry_count}/{max_retries}) in {wait_time}s...")
                        time.sleep(wait_time)
                        continue
                    else:
                        self.logger.error(f"❌ Error creating group for {stream}: {e}")
                        break

        # ✅ notify:telegram - читаем только НОВЫЕ сигналы (с $)
        max_retries = 10
        retry_count = 0
        while retry_count < max_retries:
            try:
                self.redis.xgroup_create(RS.NOTIFY_TELEGRAM, self.signal_group, id='$', mkstream=True)
                self.logger.info(f"✅ Consumer group created for {RS.NOTIFY_TELEGRAM} (NEW messages only)")
                break
            except RedisError as e:
                if "BUSYGROUP" in str(e):
                    self.logger.debug("   Group already exists: notify:telegram")
                    break
                elif "Redis is loading the dataset in memory" in str(e):
                    retry_count += 1
                    wait_time = min(2 * retry_count, 30)  # Exponential backoff, max 30 seconds
                    self.logger.warning(f"⚠️ Redis is loading data, retrying notify:telegram group creation (attempt {retry_count}/{max_retries}) in {wait_time}s...")
                    time.sleep(wait_time)
                    continue
                else:
                    self.logger.error(f"❌ Error creating group for notify:telegram: {e}")
                    break

        # Tick streams - используем '$' для чтения только новых сообщений (не с начала)
        for stream in self._build_tick_streams():
            try:
                # Проверяем, существует ли группа
                try:
                    groups = self.redis_ticks.xinfo_groups(stream)
                    group_exists = any(g.get("name") == self.tick_group for g in groups)
                    if group_exists:
                        self.logger.debug(f"   Group already exists: {stream}")
                        continue
                except Exception:
                    # Stream не существует или нет групп - создаем
                    pass

                # Создаем группу с '$' для чтения только новых сообщений
                self.redis_ticks.xgroup_create(stream, self.tick_group, id='$', mkstream=True)
                self.logger.info(f"✅ Consumer group created for {stream} (NEW messages only)")
            except RedisError as e:
                if "BUSYGROUP" in str(e):
                    self.logger.debug(f"   Group already exists: {stream}")
                elif "Redis is loading the dataset in memory" in str(e):
                    self.logger.warning(f"⚠️ Redis is loading data, skipping group creation for {stream} (will retry later)")
                else:
                    self.logger.error(f"❌ Error creating group for {stream}: {e}")

        # Events stream (trailing updates, etc.)
        max_retries = 10
        retry_count = 0
        while retry_count < max_retries:
            try:
                self.redis.xgroup_create(RS.EVENTS_TRADES, self.events_group, id='$', mkstream=True)
                self.logger.info("✅ Consumer group created for events:trades (NEW messages only)")
                break
            except RedisError as e:
                if "BUSYGROUP" in str(e):
                    self.logger.debug("   Group already exists: events:trades")
                    break
                elif "Redis is loading the dataset in memory" in str(e):
                    retry_count += 1
                    wait_time = min(2 * retry_count, 30)  # Exponential backoff, max 30 seconds
                    self.logger.warning(f"⚠️ Redis is loading data, retrying events:trades group creation (attempt {retry_count}/{max_retries}) in {wait_time}s...")
                    time.sleep(wait_time)
                    continue
                else:
                    self.logger.error(f"❌ Error creating group for events:trades: {e}")
                    break

        # Crypto raw signals stream
        max_retries = 10
        retry_count = 0
        while retry_count < max_retries:
            try:
                self.redis.xgroup_create(self.crypto_raw_stream, self.signal_group, id='0', mkstream=True)
                self.logger.info(f"✅ Consumer group created for {self.crypto_raw_stream}")
                break
            except RedisError as e:
                if "BUSYGROUP" in str(e):
                    self.logger.debug(f"   Group already exists: {self.crypto_raw_stream}")
                    break
                elif "Redis is loading the dataset in memory" in str(e):
                    retry_count += 1
                    wait_time = min(2 * retry_count, 30)  # Exponential backoff, max 30 seconds
                    self.logger.warning(f"⚠️ Redis is loading data, retrying {self.crypto_raw_stream} group creation (attempt {retry_count}/{max_retries}) in {wait_time}s...")
                    time.sleep(wait_time)
                    continue
                else:
                    self.logger.error(f"❌ Error creating group for {self.crypto_raw_stream}: {e}")
                    break

    def _signals_listener_thread(self) -> None:
        """
        Поток прослушивания сигналов из Redis Streams (signals:{strategy}:{symbol}).
        Использует единый StreamWorker для обработки.
        """
        policy = WorkerPolicy(
            ack_mode=os.getenv("SIGNALS_ACK_MODE", "lossless"),
            read_count=int(os.getenv("SIGNALS_READ_COUNT", "20")),
            block_ms=int(os.getenv("SIGNALS_BLOCK_MS", "5000")),
            drain_pending_every_s=int(os.getenv("SIGNALS_DRAIN_PENDING_EVERY_S", "10")),
            claim_orphan_every_s=int(os.getenv("SIGNALS_CLAIM_ORPHAN_EVERY_S", "60")),
            min_idle_ms=int(os.getenv("SIGNALS_MIN_IDLE_MS", "60000")),
            max_attempts=int(os.getenv("SIGNALS_MAX_ATTEMPTS", "5")),
            dlq_stream=os.getenv("SIGNALS_DLQ_STREAM", RS.DLQ_SIGNALS),
        )

        worker = StreamWorker(
            name="signals_listener",
            client=self.redis,
            group=self.signal_group,
            consumer=self.consumer_name,
            build_streams=self._build_signal_streams,
            process=self._process_signal_message,
            policy=policy,
            logger=self.logger,
            health_cb=lambda comp, status, extra: self._update_health_status(comp, status=status, extra=extra),
        )

        worker.run_loop(lambda: self.running)

    def _ticks_listener_thread(self) -> None:
        """
        Поток прослушивания тиков из Redis Streams.
        Читает stream:tick_ stream:tick_BTCUSDT и т.д.
        Использует единый StreamWorker для обработки (realtime режим).
        """
        policy = WorkerPolicy(
            ack_mode=os.getenv("TICKS_ACK_MODE", "realtime"),
            read_count=int(os.getenv("TICKS_READ_COUNT", "200")),
            block_ms=int(os.getenv("TICKS_BLOCK_MS", "1000")),
            dlq_stream=os.getenv("TICKS_DLQ_STREAM", RS.DLQ_TICKS),
        )

        worker = StreamWorker(
            name="ticks_listener",
            client=self.redis_ticks,
            group=self.tick_group,
            consumer=self.consumer_name + "-ticks",
            build_streams=self._build_tick_streams,
            process=self._process_tick_message,
            policy=policy,
            logger=self.logger,
            health_cb=lambda comp, status, extra: self._update_health_status(comp, status=status, extra=extra),
        )

        worker.run_loop(lambda: self.running)

    def _events_listener_thread(self) -> None:
        """
        Поток прослушивания торговых событий (events:trades) для синхронизации с внешними оркестраторами.
        В частности, реагирует на TRAILING_STARTED, чтобы обновить SL в виртуальных позициях.
        Использует единый StreamWorker для обработки (lossless режим).
        """
        policy = WorkerPolicy(
            ack_mode=os.getenv("EVENTS_ACK_MODE", "lossless"),
            read_count=int(os.getenv("EVENTS_READ_COUNT", "50")),
            block_ms=int(os.getenv("EVENTS_BLOCK_MS", "2000")),
            drain_pending_every_s=int(os.getenv("EVENTS_DRAIN_PENDING_EVERY_S", "10")),
            claim_orphan_every_s=int(os.getenv("EVENTS_CLAIM_ORPHAN_EVERY_S", "60")),
            min_idle_ms=int(os.getenv("EVENTS_MIN_IDLE_MS", "60000")),
            max_attempts=int(os.getenv("EVENTS_MAX_ATTEMPTS", "8")),
            dlq_stream=os.getenv("EVENTS_DLQ_STREAM", RS.DLQ_EVENTS),
        )

        def build_events():
            return [RS.EVENTS_TRADES]

        worker = StreamWorker(
            name="events_listener",
            client=self.redis,
            group=self.events_group,
            consumer=self.consumer_name + "-events",
            build_streams=build_events,
            process=self._process_event_message,
            policy=policy,
            logger=self.logger,
            health_cb=lambda comp, status, extra: self._update_health_status(comp, status=status, extra=extra),
        )

        worker.run_loop(lambda: self.running)

    def _periodic_tasks_thread(self) -> None:
        """
        Поток периодических задач.
        
        Задачи:
        - Периодические отчеты (каждые 3 часа)
        - Ежедневные сводки
        - Мониторинг здоровья системы
        """
        self.logger.info("🔄 Periodic tasks thread started")

        reporting_cfg = self.config.get("reporting", {})
        periodic_interval = reporting_cfg.get("periodic_interval_hours", 3)
        daily_enabled = reporting_cfg.get("daily_summary_enabled", True)
        daily_hour = reporting_cfg.get("daily_summary_hour", 17)

        last_periodic_report = 0
        last_daily_report_date = None

        while self.running:
            try:
                current_time = time.time()
                now = datetime.now()

                # Периодический отчет (каждые N часов)
                if current_time - last_periodic_report >= periodic_interval * 3600:
                    self.logger.info("📊 Generating periodic report...")
                    self._send_periodic_report()
                    last_periodic_report = current_time

                # Ежедневная сводка (в заданный час UTC)
                if daily_enabled and now.hour == daily_hour:
                    today = now.date()
                    if last_daily_report_date != today:
                        self.logger.info("📅 Generating daily summary...")
                        self._send_daily_summary()
                        last_daily_report_date = today

                # Логирование статистики каждую минуту
                if int(current_time) % 60 == 0:
                    self._log_stats()

                new_symbols = self._refresh_symbols_from_redis()
                if new_symbols:
                    self.logger.info(
                        "🆕 Выявлены новые символы из %s: %s",
                        self.dynamic_symbols_key,
                        ", ".join(sorted(new_symbols)),
                    )
                    self._ensure_signal_groups_for_symbols(new_symbols)

                self._update_health_status(
                    "periodic_tasks",
                    extra={
                        "last_periodic_report": last_periodic_report,
                        "last_daily_report_date": str(last_daily_report_date),
                    }
                )

                # Спим минуту
                time.sleep(60)

            except Exception as e:
                self.logger.error(f"❌ Error in periodic tasks: {e}")
                self._update_health_status("periodic_tasks", status="error", extra={"reason": str(e)})
                time.sleep(60)

        self.logger.info("🛑 Periodic tasks thread stopped")
        self._update_health_status("periodic_tasks", status="stopped")

    def _send_periodic_report(self) -> None:
        """Отправка периодического отчета в Telegram."""
        try:
            if self.periodic_reporter.available():
                # Calculate window in seconds based on interval from config (default 3 hours)
                # This fixes the issue where reports defaulted to "last 100 trades"
                reporting_cfg = self.config.get("reporting", {})
                periodic_hours = reporting_cfg.get("periodic_interval_hours", 3)
                window_seconds = int(periodic_hours * 3600)

                self.logger.info(f"📊 Generating periodic source report via PeriodicReporter (window={window_seconds}s)...")
                self.periodic_reporter.send_periodic_report(window_seconds=window_seconds)
                return

            self.logger.info("📊 PeriodicReporter недоступен, fallback к daily summary.")
            self.reporting_service.send_daily_summary(include_sources=True)

        except Exception as e:
            self.logger.error(f"❌ Error in periodic report: {e}")

    def _send_daily_summary(self) -> None:
        """Отправка ежедневной сводки в Telegram."""
        try:
            # Prefer PeriodicReporter for daily summary if available
            if self.periodic_reporter.available():
                self.logger.info("📅 Generating daily source report via PeriodicReporter (24h window)...")
                self.periodic_reporter.send_daily_report()
            else:
                self.logger.info("📅 Generating daily summary via ReportingService (fallback)...")
                self.reporting_service.send_daily_summary(include_sources=True)

            # Также отправляем детальный отчет по

            # Также отправляем детальный отчет по
            for symbol in self.symbols:
                for strategy in self.strategies:
                    if strategy != "aggregated":  # aggregated не имеет своей статистики
                        # Опционально — быстрый анализ entry_tag для пары
                        self._maybe_log_entry_tag_stats(strategy, symbol)
                        self.reporting_service.send_strategy_report(
                            strategy=strategy,
                            symbol=symbol,
                            tf="tick"
                        )

            self.logger.info("📅 Daily summary sent successfully via ReportingService")

        except Exception as e:
            self.logger.error(f"❌ Error in daily summary: {e}")

    def _refresh_symbols_from_redis(self, initial: bool = False) -> list[str]:
        """
        Подтягивает символы из Redis множества и добавляет к текущему списку.
        Возвращает список новых символов.
        """
        if not self.dynamic_symbols_key:
            return []

        try:
            redis_symbols = self.redis.smembers(self.dynamic_symbols_key)
        except RedisError as exc:
            self.logger.warning(
                "⚠️ Не удалось загрузить символы из %s: %s",
                self.dynamic_symbols_key,
                exc,
            )
            return []

        new_symbols: list[str] = []
        revision_changed = False

        for sym in redis_symbols:
            sym_upper = sym.strip().upper() if sym else ""
            if not sym_upper:
                continue

            canonical = self._normalize_symbol(sym_upper) or sym_upper

            if canonical != sym_upper:
                if sym_upper not in self.symbol_alias_lookup:
                    self._register_alias(sym_upper, canonical)
                    self.logger.debug("🔁 Добавлен алиас символа из Redis: %s → %s", sym_upper, canonical)
                elif self.symbol_alias_lookup[sym_upper] != canonical:
                    self._register_alias(sym_upper, canonical)

            if sym_upper not in self._signal_symbol_set:
                self.signal_symbols.append(sym_upper)
                self._signal_symbol_set.add(sym_upper)
                revision_changed = True

            if canonical not in self._signal_symbol_set:
                self.signal_symbols.append(canonical)
                self._signal_symbol_set.add(canonical)
                revision_changed = True

            if canonical not in self._symbol_set:
                self.symbols.append(canonical)
                self._symbol_set.add(canonical)
                new_symbols.append(canonical)
                revision_changed = True
                self.canonical_aliases.setdefault(canonical, set())

        if revision_changed:
            self._symbol_revision += 1

        if new_symbols and not initial:
            self.logger.debug(
                "📥 Добавлены символы в отслеживание: %s",
                ", ".join(sorted(new_symbols)),
            )

        return new_symbols

    def _log_stats(self) -> None:
        """Логирование текущей статистики."""
        uptime = time.time() - self.stats["start_time"]
        uptime_str = str(timedelta(seconds=int(uptime)))

        open_positions = self.trade_monitor.get_position_count()

        self.logger.info(
            f"📊 Stats: "
            f"signals={self.stats['signals_processed']}, "
            f"ticks={self.stats['ticks_processed']}, "
            f"opened={self.stats['positions_opened']}, "
            f"closed={self.stats['positions_closed']}, "
            f"open_now={open_positions}, "
            f"trail_synced={self.stats['trailing_updates_applied']}, "
            f"trail_missed={self.stats['trailing_updates_missed']}, "
            f"ext_sl_synced={self.stats['external_sl_synced']}, "
            f"ext_sl_missed={self.stats['external_sl_missed']}, "
            f"tp1_internal={self.stats['tp1_internal_hits']}, "
            f"trail_int_ok={self.stats['trailing_internal_started']}, "
            f"trail_int_fail={self.stats['trailing_internal_failed']}, "
            f"uptime={uptime_str}"
        )
        self._update_health_status(
            component="stats_logger",
            extra={
                "signals_processed": self.stats["signals_processed"],
                "ticks_processed": self.stats["ticks_processed"],
                "positions_open": open_positions,
            }
        )

    def _maybe_log_entry_tag_stats(self, source: str, symbol: str) -> None:
        """Быстрый анализ entry_tag для пары source/symbol (опционально)."""
        if not self.entry_tag_analytics_enabled:
            return
        try:
            src = canon_source(source)
            sym = canon_symbol(symbol)
            trades = list(load_trades(self.redis, src, sym, limit=self.entry_tag_analytics_limit))
            if not trades:
                self.logger.debug("entry_tag analytics: нет сделок для %s/%s", src, sym)
                return
            per_tag = analyze_by_entry_tag(trades, legacy_format=True)
            preview_items = []
            for tag, s in per_tag.items():  # type: ignore
                if tag == "_ALL_":  # type: ignore
                    continue
                n = s.get("n", 0)
                if n < self.entry_tag_min_trades:
                    continue
                wins = s.get("wins", 0)
                wr = (wins / n * 100.0) if n else 0.0
                n_r = s.get("n_r", 0)
                exp_r_net = (s.get("sum_r_net", 0.0) / n_r) if n_r else 0.0
                exp_r_fix = (s.get("sum_r_fixed", 0.0) / n_r) if n_r else 0.0
                preview_items.append((n, tag, wr, exp_r_net, exp_r_fix))
            preview_items.sort(reverse=True)
            top = preview_items[:5]
            if not top:
                self.logger.info("📊 entry_tag stats %s/%s: нет тегов с n>=%s", src, sym, self.entry_tag_min_trades)
                return
            summary = "; ".join(
                f"{tag}:n={n},WR={wr:.1f}%,ExpR={exp_net:+.2f}/fix={exp_fix:+.2f}"
                for n, tag, wr, exp_net, exp_fix in top
            )
            self.logger.info("📊 entry_tag stats %s/%s: %s", src, sym, summary)
        except Exception as e:
            self.logger.debug("entry_tag analytics failed for %s/%s: %s", source, symbol, e)

    def _update_health_status(
        self,
        component: str,
        status: str = "ok",
        extra: dict[str, Any] | None = None,
    ) -> None:
        if not self.health_key:
            return
        payload = {
            "ts": int(time.time()),
            "status": status,
            "component": component,
            "signals_processed": self.stats.get("signals_processed"),
            "ticks_processed": self.stats.get("ticks_processed"),
        }
        if extra:
            payload["extra"] = extra
        try:
            self.redis.hset(self.health_key, component, json.dumps(payload))
            self.redis.expire(self.health_key, self.health_ttl)
        except Exception as exc:
            self.logger.debug("⚠️ Не удалось обновить health статус (%s): %s", component, exc)

    def _ensure_signal_groups_for_symbols(self, symbols: list[str]) -> None:
        """Гарантирует наличие consumer group'ов для новых символов."""
        if not symbols:
            return

        for symbol in symbols:
            variants = self._get_signal_variants(symbol)
            for strategy in self.strategies:
                for variant in variants:
                    stream = f"signals:{strategy}:{variant}"
                    self._ensure_group(self.redis, stream, self.signal_group, start_id='0')

        for symbol in symbols:
            for variant in self._get_signal_variants(symbol):
                stream = f"stream:tick_{variant}"
                # Используем '$' для чтения только новых сообщений
                self._ensure_group(self.redis_ticks, stream, self.tick_group, start_id='$')

    def _ensure_group(self, client: redis.Redis, stream: str, group: str, start_id: str) -> None:
        try:
            client.xgroup_create(stream, group, id=start_id, mkstream=True)
            self.logger.info("✅ Consumer group created for %s", stream)
        except RedisError as e:
            if "BUSYGROUP" in str(e):
                self.logger.debug("   Group already exists: %s", stream)
            else:
                self.logger.error(f"❌ Error creating group for {stream}: {e}")

    def _build_signal_streams(self) -> list[str]:
        streams: list[str] = []
        for strategy in self.strategies:
            for symbol in self.signal_symbols:
                streams.append(f"signals:{strategy}:{symbol}")
        if self.crypto_raw_stream and self.crypto_raw_stream not in streams:
            streams.append(self.crypto_raw_stream)
        return streams

    def _build_tick_streams(self) -> list[str]:
        streams: set[str] = set()
        for canonical in self.symbols:
            streams.add(f"stream:tick_{canonical}")
            for alias in self.canonical_aliases.get(canonical, set()):
                streams.add(f"stream:tick_{alias}")
        return sorted(streams)

    def _handle_nogroup_error(self, err: Exception, context: str) -> None:
        if "NOGROUP" not in str(err).upper():
            return
        self.logger.warning("⚠️ %s: consumer group missing, recreating...", context)
        try:
            self._create_consumer_groups()
        except Exception as recreate_err:
            self.logger.error("❌ Failed to recreate consumer groups after %s: %s", context, recreate_err)

    def start(self) -> None:
        """Запуск всех потоков."""
        if self.running:
            self.logger.warning("⚠️ Tracker already running")
            return

        self.running = True

        # Создаем consumer groups
        self._create_consumer_groups()

        # Запускаем потоки
        self.logger.info("🚀 Starting threads...")

        # --- Symbol executor (actor-like serialization) ---
        # Critical: keep lossless semantics by waiting for task completion before ACK.
        if self._use_symbol_exec and self._exec is None:
            self._exec = ShardedSerialExecutor(
                shards=self._exec_shards,
                queue_max=self._exec_queue_max,
                submit_timeout_s=self._exec_submit_timeout_s,
                name="SymbolExec",
                logger=self.logger,
            )

        # --- Actor runtime (core-per-shard, eliminates global locks) ---
        # Higher level: each shard owns its state, no shared dicts between threads
        if self._use_actor_runtime and self.tm_runtime is None:
            self.tm_runtime = TradeMonitorActorRuntime(
                core_factory=self._trade_monitor_core_factory,
                logger=self.logger,
            )

        # Поток сигналов
        signals_thread = threading.Thread(
            target=self._signals_listener_thread,
            name="SignalsListener",
            daemon=True
        )
        signals_thread.start()
        self.threads.append(signals_thread)

        # Поток тиков
        ticks_thread = threading.Thread(
            target=self._ticks_listener_thread,
            name="TicksListener",
            daemon=True
        )
        ticks_thread.start()
        self.threads.append(ticks_thread)

        # Поток периодических задач
        periodic_thread = threading.Thread(
            target=self._periodic_tasks_thread,
            name="PeriodicTasks",
            daemon=True
        )
        periodic_thread.start()
        self.threads.append(periodic_thread)

        # Поток событий (trailing updates)
        events_thread = threading.Thread(
            target=self._events_listener_thread,
            name="EventsListener",
            daemon=True
        )
        events_thread.start()
        self.threads.append(events_thread)

        self.logger.info(f"✅ All threads started ({len(self.threads)} threads)")

    def stop(self) -> None:
        """Остановка всех потоков (graceful shutdown)."""
        if not self.running:
            return

        self.logger.info("⚠️ Stopping Signal Performance Tracker...")
        self.running = False

        # Shutdown executor
        try:
            if self._exec is not None:
                self._exec.shutdown(join_timeout_s=2.0)
                self._exec = None
        except Exception as e:
            self.logger.warning(f"⚠️ Executor shutdown failed: {e}")

        # Shutdown actor runtime
        try:
            if self.tm_runtime is not None:
                self.tm_runtime.shutdown()
                self.tm_runtime = None
        except Exception as e:
            self.logger.warning(f"⚠️ Actor runtime shutdown failed: {e}")

        # Ждем завершения потоков
        for thread in self.threads:
            if thread.is_alive():
                thread.join(timeout=5)

        self.logger.info("✅ Signal Performance Tracker stopped")

    def _route_key_for_symbol_or_sid(self, symbol: str | None, sid: str | None) -> str:
        """
        Routing key for executor:
          - prefer symbol (best locality + predictable ordering per symbol)
          - fallback to sid (preserve ordering for late/unknown symbol events)
        """
        if symbol:
            return symbol.upper()
        if sid:
            return f"sid:{sid}"
        return "unknown"

    def _exec_call_lossless(self, key: str, fn, *, name: str) -> bool:
        """
        Submit to executor and wait for completion.
        Returns True on success (ACK), False on exception (retry/DLQ by StreamWorker).
        """
        # Prefer actor runtime (higher level: core-per-shard, no shared state)
        if self._use_actor_runtime and self.tm_runtime is not None:
            # For actor runtime, we route via the runtime's submit methods
            # This is handled in the specific _process_*_message methods
            try:
                fn()
                return True
            except Exception as e:
                self.logger.warning("handler failed (actor) %s: %s", name, e)
                return False

        # Fallback to sharded executor (serialization only)
        if not self._use_symbol_exec or self._exec is None:
            try:
                fn()
                return True
            except Exception as e:
                self.logger.warning("handler failed (no-exec) %s: %s", name, e)
                return False
        fut = self._exec.submit(key, fn, name=name)
        try:
            fut.result(timeout=self._exec_task_timeout_s)
            return True
        except Exception as e:
            self.logger.warning("handler failed (exec) %s key=%s: %s", name, key, e)
            return False

    def run_forever(self) -> None:
        """Запуск трекера и ожидание (блокирующий метод)."""
        self.start()

        self.logger.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
        self.logger.info("🚀 Signal Performance Tracker is running")
        self.logger.info("   Press Ctrl+C to stop")
        self.logger.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

        try:
            while self.running:
                time.sleep(1)
        except KeyboardInterrupt:
            self.logger.info("\n⚠️ KeyboardInterrupt received")
            self.stop()


def main():
    """Главная функция запуска."""
    print("\n" + "=" * 70)
    print("🚀 Signal Performance Tracker v2.0")
    print("   Senior Developer + Trading Analyst")
    print("=" * 70 + "\n")

    # Создаем трекер
    try:
        tracker = SignalPerformanceTracker()
    except Exception as e:
        print(f"\n❌ Ошибка инициализации: {e}")
        sys.exit(1)

    # Методы для мониторинга качества сигналов
    def get_quality_report(symbol: str | None = None, family: str | None = None) -> str:
        """Получить отчет о качестве сигналов с L3-метриками."""
        return tracker.quality_monitor.get_quality_report(symbol, family)

    def get_quality_alerts() -> list[str]:
        """Получить алерты о проблемах качества сигналов."""
        return tracker.quality_monitor.get_alerts()

    def log_signal_snapshot(snapshot) -> bool:
        """Логировать snapshot сигнала с L3-метриками."""
        return tracker.signal_logger.log_signal(snapshot)  # type: ignore
  # type: ignore
    def get_recent_signals(symbol: str | None = None, family: str | None = None, limit: int = 100):
        """Получить недавние сигналы для анализа."""
        return tracker.signal_logger.get_recent_signals(symbol, family, limit)  # type: ignore
  # type: ignore
    # Обработка сигналов для graceful shutdown
    def signal_handler(signum, frame):
        print(f"\n⚠️ Получен сигнал {signum}, завершение работы...")
        tracker.stop()
        sys.exit(0)

    sig.signal(sig.SIGINT, signal_handler)
    sig.signal(sig.SIGTERM, signal_handler)

    # Запуск
    try:
        tracker.run_forever()
    except Exception as e:
        print(f"\n❌ Критическая ошибка: {e}")
        tracker.stop()
        sys.exit(1)


if __name__ == "__main__":
    main()

