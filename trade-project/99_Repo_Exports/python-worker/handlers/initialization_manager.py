# initialization_manager.py
from __future__ import annotations

"""
Initialization management functionality extracted from base_orderflow_handler.py
"""

import os
import time
from dataclasses import dataclass
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any
from urllib.parse import urlsplit, urlunsplit

from utils.time_utils import get_ny_time_millis

from .cache_service import CacheService
from .config_manager import ConfigManager
from .error_handler import ErrorHandler


# from common.log import setup_logger
def setup_logger(name):
    import logging
    return logging.getLogger(name)

if TYPE_CHECKING:
    from local_calibration.store import LocalCalibrationStore as LCStoreV2


@dataclass(frozen=True)
class Infra:  # type: ignore
    redis: Any
    redis_ticks: Any
    tick_stream: str
    book_stream: str
    l3_stream: str
    group: str
    consumer_name_prefix: str
    cache_service: CacheService
    config_manager: ConfigManager


class InitializationManager:
    """
    Управляет инициализацией всех подсистем хендлера.
    """

    def __init__(self, handler: Any):
        self.handler = handler
        sym = getattr(handler, "symbol", None) or "UNKNOWN"
        self.logger = setup_logger(f"InitializationManager:{sym}")

    def _ensure_config(self) -> Any:
        """
        Гарантирует наличие handler.config как объект с атрибутами.
        Это критично, т.к. много init-методов пишут настройки в handler.config.*.
        """
        cfg = getattr(self.handler, "config", None)
        if cfg is None:
            cfg = SimpleNamespace()
            self.handler.config = cfg
        return cfg

    @staticmethod
    def _redact_url(url: str) -> str:
        """
        Аккуратно скрываем userinfo (user:pass@) в URL для логов.
        """
        try:
            p = urlsplit(url)
            netloc = p.netloc
            if "@" in netloc:
                hostpart = netloc.split("@", 1)[1]
                netloc = f"[HIDDEN]@{hostpart}"
            return urlunsplit((p.scheme, netloc, p.path, p.query, p.fragment))
        except Exception:
            return "redis://[HIDDEN]"

    def _init_redis(self) -> None:
        """Инициализация подключений Redis."""
        redis_url_main = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
        redis_url_ticks = os.getenv("REDIS_TICKS_URL", "redis://redis-ticks:6379/0")

        self._init_redis_config(redis_url_main, redis_url_ticks)

    def _init_redis_config(self, redis_url_main: str, redis_url_ticks: str) -> None:
        """Настройка подключений Redis."""
        try:
            from redis import Redis
        except ImportError as e:
            raise RuntimeError("redis-py is required for OrderFlow") from e

        # Create connections
        self.handler.redis = Redis.from_url(redis_url_main, decode_responses=True)
        self.handler.redis_ticks = Redis.from_url(redis_url_ticks, decode_responses=True)

        # Test connections with retry logic for loading dataset
        self._test_redis_connection(self.handler.redis, "main", redis_url_main)
        self._test_redis_connection(self.handler.redis_ticks, "ticks", redis_url_ticks)

        # FAST client for tick-loop news/calendar reads (bounded latency)
        try:
            from news_pipeline.redis_fast import make_news_redis  # type: ignore
            redis_url_news = os.getenv("REDIS_NEWS_URL", redis_url_main)
            self.handler.redis_news = make_news_redis(redis_url=redis_url_news, max_connections=32)
        except Exception:
            # fail-open: если не получилось — tick-loop просто не будет обогащаться
            self.handler.redis_news = None

        # Тестировать fast client можно, но аккуратно (не увеличивать latency старта):
        # self._test_redis_connection(self.handler.redis_news, "news_fast", redis_url_main)

    def _test_redis_connection(self, redis_client, connection_type: str, redis_url: str) -> None:
        """Тест подключения Redis с логикой ретраев."""
        max_retries = 10
        base_delay = 1.0
        max_delay = 30.0

        for attempt in range(max_retries):
            try:
                redis_client.ping()
                self.logger.info("Connected to Redis %s: %s", connection_type, self._redact_url(redis_url))
                return
            except Exception as e:
                error_msg = str(e).lower()
                is_loading_error = "loading the dataset in memory" in error_msg

                if is_loading_error and attempt < max_retries - 1:
                    delay = min(base_delay * (2 ** attempt), max_delay)
                    self.logger.warning(
                        "Redis %s is loading dataset (attempt %d/%d), retrying in %.1fs: %s",
                        connection_type, attempt + 1, max_retries, delay, e
                    )
                    time.sleep(delay)
                    continue
                else:
                    if is_loading_error:
                        error_msg = f"Failed to connect to Redis {connection_type} after {max_retries} attempts - still loading dataset: {e}"
                    else:
                        error_msg = f"Failed to connect to Redis {connection_type}: {e}"
                    raise RuntimeError(error_msg) from e

    def _init_stream_config(self, symbol: str) -> None:
        """Инициализация конфигурации стримов."""
        cfg = self._ensure_config()
        self.handler.consumer_name_prefix = os.getenv("CONSUMER_NAME_PREFIX", "orderflow")
        self.handler.group = os.getenv("REDIS_CONSUMER_GROUP", "orderflow-group")

        # Stream names - read from environment variables with fallback to defaults
        self.handler.tick_stream = os.getenv(f"{symbol}_TICK_STREAM", f"ticks:{symbol}")
        self.handler.book_stream = os.getenv(f"{symbol}_BOOK_STREAM", f"book:{symbol}")
        self.handler.l3_stream = os.getenv(f"{symbol}_L3_STREAM", f"l3:{symbol}")

        # Параметры обработки - помещаем в конфиг для чтения в MainLoopService
        if not hasattr(cfg, "claim_min_idle_ms"):
            cfg.claim_min_idle_ms = int(os.getenv("CLAIM_MIN_IDLE_MS", "60000"))
        if not hasattr(cfg, "claim_count"):
            cfg.claim_count = int(os.getenv("CLAIM_COUNT", "100"))
        if not hasattr(cfg, "claim_interval_ms"):
            cfg.claim_interval_ms = int(os.getenv("CLAIM_INTERVAL_MS", "30000"))
        if not hasattr(cfg, "max_fail_retries"):
            cfg.max_fail_retries = int(os.getenv("MAX_FAIL_RETRIES", "3"))

        # Compatibility shim
        self.handler.max_fail_retries = int(getattr(cfg, "max_fail_retries", 3))

    def _init_notify_config(self) -> None:
        pass

    def _init_signal_processing_config(self) -> None:
        """
        ВАЖНО: писать и в handler.config, иначе downstream (DataProcessor/MainLoop)
        может не увидеть параметры.
        """
        cfg = self._ensure_config()

        cfg.delta_bucket_ms = int(os.getenv("DELTA_BUCKET_MS", "1000"))
        cfg.delta_window_ticks = int(os.getenv("DELTA_WINDOW_TICKS", "100"))
        cfg.max_zero_buckets = int(os.getenv("MAX_ZERO_BUCKETS", "10"))

        # optional shim для старого кода, если где-то читают handler.*
        self.handler.delta_bucket_ms = int(cfg.delta_bucket_ms)
        self.handler.delta_window_ticks = int(cfg.delta_window_ticks)
        self.handler.max_zero_buckets = int(cfg.max_zero_buckets)

    def _init_l2l3_config(self) -> None:
        """Инициализация конфигурации L2/L3."""
        cfg = self._ensure_config()

        # то, что реально читает data_processor (см. _update_l2_tick_staleness)
        cfg.l2_stale_ms = int(os.getenv("L2_STALE_MS", "2000"))
        cfg.l2_skew_tick_thr_ms = int(os.getenv("L2_SKEW_TICK_THR_MS", "5000"))

        # optional shim
        self.handler.l2_max_age_ms = int(os.getenv("L2_MAX_AGE_MS", "5000"))
        self.handler.l2_skew_threshold_ms = int(os.getenv("L2_SKEW_THRESHOLD_MS", "100"))

        # L3 configuration
        l3_alpha = float(os.getenv("L3_TAKER_RATE_EMA_ALPHA", "0.12"))

        L3Queue = self.handler.dependencies.l3_queue
        if L3Queue:
            try:
                self.handler.l3_queue = L3Queue(
                    bucket_ms=self.handler.delta_bucket_ms,
                    alpha=l3_alpha
                )
            except Exception as e:
                self.logger.warning("L3QueueEventsProxy init failed (fail-open): %s", e)
                self.handler.l3_queue = None
        else:
            self.handler.l3_queue = None

    def _init_burst_config(self) -> None:
        """Инициализация конфигурации детекции всплесков (идемпотентно)."""
        cfg = self._ensure_config()
        if getattr(self.handler, "_burst_inited", False):
            return
        self.handler._burst_inited = True

        cfg.imbalance_min = float(os.getenv("IMBALANCE_MIN", "0.20"))
        cfg.min_trades_breakout = int(os.getenv("MIN_TRADES_BREAKOUT", "20"))
        cfg.burst_ratio_min = float(os.getenv("BURST_RATIO_MIN", "1.6"))
        cfg.fano_min = float(os.getenv("FANO_MIN", "1.5"))
        cfg.flip_ratio_max = float(os.getenv("FLIP_RATIO_MAX", "0.70"))

        # optional shim
        self.handler.imbalance_min = float(cfg.imbalance_min)
        self.handler.min_trades_breakout = int(cfg.min_trades_breakout)
        self.handler.burst_ratio_min = float(cfg.burst_ratio_min)
        self.handler.fano_min = float(cfg.fano_min)
        self.handler.flip_ratio_max = float(cfg.flip_ratio_max)

        # Instance
        burst_half_life_short_ms = int(os.getenv("BURST_HALF_LIFE_SHORT_MS", "250"))
        burst_half_life_long_ms = int(os.getenv("BURST_HALF_LIFE_LONG_MS", "2000"))
        burst_fano_window_buckets = int(os.getenv("BURST_FANO_WINDOW_BUCKETS", "60"))
        burst_dt_alpha = float(os.getenv("BURST_DT_ALPHA", "0.05"))

        Burst = self.handler.dependencies.burst_tracker
        if Burst:
            try:
                self.handler.burst = Burst(
                    bucket_ms=self.handler.delta_bucket_ms,
                    half_life_short_ms=burst_half_life_short_ms,
                    half_life_long_ms=burst_half_life_long_ms,
                    fano_window_buckets=burst_fano_window_buckets,
                    dt_alpha=burst_dt_alpha,
                )
            except Exception as e:
                self.logger.warning("BurstinessTracker init failed: %s", e)
                self.handler.burst = None
        else:
            self.handler.burst = None

    def _init_caches(self) -> None:
        """Инициализация систем кеширования."""
        if not getattr(self.handler, "redis", None):
            self.handler._pivot_cache = None
            return

        try:
            from core.performance_optimizer import PivotPointsCache
            self.handler._pivot_cache = PivotPointsCache(self.handler.redis)
        except (ImportError, Exception) as e:
            self.logger.warning("Failed to initialize pivot cache: %s", e)
            self.handler._pivot_cache = None

    def _init_extrema(self) -> None:
        """Инициализация детекции экстремумов."""
        if not getattr(self.handler, "redis", None):
            self.handler._extrema_service = None
            return

        ExtremaPair = self.handler.dependencies.extrema_service
        if ExtremaPair:
            try:
                ServiceCls, ConfigCls = ExtremaPair
                config = ConfigCls(
                    min_bars_between_extremes=int(os.getenv("MIN_EXTREMA_DIST_TICKS", "50")),
                    min_move_bps=float(os.getenv("MIN_EXTREMA_MOVE_BPS", "20.0")),
                )
                self.handler._extrema_service = ServiceCls(config=config)
            except Exception as e:
                self.logger.warning("Failed to initialize extrema service: %s", e)
                self.handler._extrema_service = None
        else:
            self.handler._extrema_service = None

    def _init_execution_setup(self) -> None:
        """Инициализация настроек исполнения."""
        if not getattr(self.handler, "redis", None):
            self.handler._execution_setup = None
            return

        Repo = self.handler.dependencies.execution_setup
        if Repo:
            try:
                self.handler._execution_setup = Repo()
            except Exception as e:
                self.logger.warning("Failed to initialize execution setup: %s", e)
                self.handler._execution_setup = None
        else:
            self.handler._execution_setup = None

    def _init_outbox(self) -> None:
        """Инициализация outbox для публикации сигналов."""
        if not getattr(self.handler, "redis", None):
            self.handler._outbox_publisher = None
            return

        OutboxPair = self.handler.dependencies.outbox_publisher
        if OutboxPair:
            try:
                PubCls, OutboxCls = OutboxPair
                outbox = OutboxCls()
                self.handler._outbox_publisher = PubCls(
                    outbox=outbox,
                    source="orderflow",
                    strategy="orderflow"
                )
            except Exception as e:
                self.logger.warning("Failed to initialize outbox publisher: %s", e)
                self.handler._outbox_publisher = None
        else:
             self.handler._outbox_publisher = None

    def _init_gpu(self) -> None:
        """Инициализация GPU процессинга."""
        L2GPU = self.handler.dependencies.gpu_processor
        if L2GPU:
            try:
                self.handler.l2_gpu_processor = L2GPU(
                    symbol=self.handler.symbol,
                    batch_size=int(os.getenv("GPU_BATCH_SIZE", "1000")),
                    buffer_timeout_ms=int(os.getenv("GPU_BUFFER_TIMEOUT_MS", "1000")),
                )
            except Exception:
                self.handler.l2_gpu_processor = None
        else:
            self.handler.l2_gpu_processor = None

    def _init_regime(self) -> None:
        """Инициализация детекции режима рынка."""
        if not getattr(self.handler, "redis", None):
            self.handler._regime_service = None
            return

        RegimePair = self.handler.dependencies.regime_service
        if RegimePair:
            try:
                ServiceCls, ConfigCls = RegimePair
                try:
                    config = ConfigCls(
                        update_interval_ms=int(os.getenv("REGIME_UPDATE_INTERVAL_MS", "60000")),
                        stability_threshold=float(os.getenv("REGIME_STABILITY_THRESHOLD", "0.7")),
                    )
                except TypeError:
                    config = ConfigCls()  # type: ignore[call-arg]
                self.handler._regime_service = ServiceCls(regime_config=config)
            except Exception as e:
                self.logger.warning("Failed to initialize regime service: %s", e)
                self.handler._regime_service = None
        else:
            self.handler._regime_service = None

    def _init_execution(self) -> None:
        """Инициализация компонентов исполнения."""
        self.handler._signal_service = None
        self.handler._execution_planner = None
        self.handler._signal_repo = None
        self.handler._signal_bus = None
        self.handler._performance_tracker = None

        database_url = (os.getenv("ANALYTICS_DB_DSN") or os.getenv("DATABASE_URL"))
        if not database_url or not getattr(self.handler, "redis", None):
            return

        # Use dependencies
        d = self.handler.dependencies
        if d.signal_service and d.execution_planner and d.signal_repo and d.signal_bus and d.performance_tracker:
            try:
                # We need SymbolSetupConfig which is a model, might be imported or passed?
                # It was imported from signal_exec.models.
                # If we want to fully remove imports, we should add it to dependencies or use factory.
                # But it's a data model, less critical?
                # Ideally HandlerDependencies should have it.
                from signal_exec.models import (
                    SymbolSetupConfig,  # Keeping local import for data model for now or moving to top?
                )
                # Data models are usually fine. The issue is heavy imports.

                redis_url_main = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")

                basic_config = SymbolSetupConfig(
                    symbol=self.handler.symbol,
                    setup_type="orderflow",
                    expiry_bars=60,
                    min_stop_ticks=10,
                    max_stop_R=0.05,
                    atr_buffer_ratio=0.5,
                    entry_zone_min_R=0.01,
                    entry_zone_max_R=0.03,
                    default_tp_R=(1.0, 2.0, 3.0),
                    score_buckets=(0.4, 0.7, 0.85),
                    risk_multipliers=(0.5, 1.0, 1.5, 2.0)
                )
                setup_configs = {(self.handler.symbol, "orderflow"): basic_config}

                # Instantiate using injected classes
                self.handler._execution_planner = d.execution_planner(setup_configs)
                self.handler._signal_repo = d.signal_repo(database_url)
                self.handler._signal_bus = d.signal_bus(redis_url_main)
                self.handler._performance_tracker = d.performance_tracker(self.handler._signal_repo, bus=self.handler._signal_bus)
                self.handler._signal_service = d.signal_service(
                    self.handler._signal_repo,
                    self.handler._execution_planner,
                    self.handler._performance_tracker,
                    self.handler._signal_bus,
                )
            except Exception as e:
                self.logger.warning("Failed to initialize execution components: %s", e)

    def _init_calibration(self, local_calibration: LCStoreV2 | None = None) -> None:
        """Инициализация систем калибровки."""
        self.handler.local_calibration = local_calibration

    def _init_scoring(self) -> None:
        """Инициализация движков скоринга."""
        ScoringPair = self.handler.dependencies.scoring_engine
        if not ScoringPair:
            self.handler._scoring_engine = None
            return

        try:
            EngineCls, ConfigCls = ScoringPair
            cfg = ConfigCls(
                confidence_threshold=float(os.getenv("SCORING_CONFIDENCE_THRESHOLD", "0.5")),
                max_score_age_ms=int(os.getenv("MAX_SCORE_AGE_MS", "300000")),
            )

            if hasattr(self.handler, 'local_calibration') and self.handler.local_calibration is not None:
                self.handler._scoring_engine = EngineCls(
                    calib_store=self.handler.local_calibration,
                    config=cfg
                )
            else:
                self.handler._scoring_engine = None
        except Exception as e:
            self.logger.warning("Failed to init scoring engine (fail-open): %s", e)
            self.handler._scoring_engine = None

    def _init_unified_pipeline(self, unified_pipeline: Any | None = None) -> None:
        self.handler._unified_pipeline = unified_pipeline

    def _init_l2l3(self) -> None:
        self._init_l2l3_config()
        self._init_burst_config()

    def _init_analysis_state(self) -> None:
        self.handler.is_running = False
        self.handler.last_signal_ts = 0
        try:
            from signals.atr import ATR
            self.handler.atr_calculator = ATR(period=14)
        except ImportError:
            self.handler.atr_calculator = None
        self.handler.daily_pivots = None

    def initialize_all(
        self,
        symbol: str,
        config: Any | None = None,
        local_calibration: LCStoreV2 | None = None,
        unified_pipeline: Any | None = None,
    ) -> Infra:
        self.handler.symbol = symbol
        cfg = self._ensure_config()
        if config is not None:
            self.handler.config = config
            cfg = self._ensure_config()

        self._init_redis()
        self._init_stream_config(symbol)

        config_manager = ConfigManager(
            symbol,
            signal_stream_prefix=getattr(self.handler, "signal_stream_prefix", None),
            strategy_key="orderflow",
        )
        cache_service = CacheService(self.handler.redis, symbol)

        # важно: чтобы BaseOrderFlowHandler реально использовал один источник
        self.handler._cache_service = cache_service

        # прогрев pivots
        try:
            now_ms = get_ny_time_millis()
            cache_service.ensure_pivots_bundle(now_ms)
            self.logger.info("Pivots cache initialized for %s", symbol)
        except Exception as e:
            self.logger.warning("Failed to initialize pivots cache for %s: %s", symbol, e)

        self._init_notify_config()
        self._init_signal_processing_config()
        self._init_caches()
        self._init_extrema()
        self._init_execution_setup()
        self._init_outbox()
        self._init_gpu()
        self._init_regime()
        self._init_execution()
        self._init_calibration(local_calibration)
        self._init_scoring()
        self._init_unified_pipeline(unified_pipeline)
        self._init_l2l3()
        self._init_analysis_state()

        self.handler._error_handler = ErrorHandler(
            symbol,
            max_fail_retries=int(getattr(self.handler, "max_fail_retries", 3)),
        )

        return Infra(
            redis=self.handler.redis,
            redis_ticks=self.handler.redis_ticks,
            tick_stream=self.handler.tick_stream,
            book_stream=self.handler.book_stream,
            l3_stream=self.handler.l3_stream,
            group=self.handler.group,
            consumer_name_prefix=self.handler.consumer_name_prefix,
            cache_service=cache_service,
            config_manager=config_manager,
        )


from dataclasses import dataclass


@dataclass(frozen=True)
class Infra:
    redis: Any
    redis_ticks: Any
    tick_stream: str
    book_stream: str
    l3_stream: str
    group: str
    consumer_name_prefix: str
    cache_service: CacheService
    config_manager: ConfigManager

