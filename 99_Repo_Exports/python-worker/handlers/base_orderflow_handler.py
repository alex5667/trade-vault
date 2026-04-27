# handlers/base_orderflow_handler.py
# ПРИМЕЧАНИЕ (единый источник истины):
# - 1m бары: BarBuilder1m (внутри OrderFlowDataProcessor)
# - пивоты: CacheService (пакет {"ts_ms","date","hlc","pivots"} хранится в pivots:{symbol})
#
# ПРИМЕЧАНИЕ (инициализация пивотов):
# - Пивоты инициализируются автоматически при запуске handler через InitializationManager
# - Ежедневное обновление происходит автоматически через валидацию даты в ensure_pivots_bundle()
# - Ручное обновление доступно через метод refresh_pivots_cache()

from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import os
import time
import threading
import logging
from abc import ABC, abstractmethod
from collections import deque
from typing import Optional, Dict, Any, TYPE_CHECKING

from common.resiliency import safe_call_fail_open as _safe_call_fail_open

if TYPE_CHECKING:
    from signals.unified_pipeline import UnifiedSignalPipeline
    from services.l3_queue_events_proxy import L3QueueEventsProxy
    from services.queue_eta_estimator import QueueETAEvaluator
    from geometry.extrema import LocalExtremaService
    from signal_execution.setup_config import ExecutionSetupRepository
    from signals.signal_publisher import SignalPublisher
    from signal_scoring.engine import SignalScoringEngine
    from signal_exec import (
         SignalService, ExecutionPlanner, SignalPerformanceTracker,
         SignalRepository, SignalBus
    )
    from core.htf_levels import HTFLevelsProvider
else:
    HTFLevelsProvider = object

# -----------------------------
# HealthMetrics integration helpers
# -----------------------------
# Custom exceptions for better error handling
class HandlerError(Exception):
    """Базовое исключение для ошибок handler."""
    pass


class InvalidSymbolError(HandlerError):
    """Вызывается, когда символ недействителен."""
    pass


class MissingConfigError(HandlerError):
    """Вызывается, когда конфигурация отсутствует."""
    pass


class DependencyError(HandlerError):
    """Вызывается, когда необходимые зависимости недоступны."""
    pass



from core.instrument_config import OrderFlowConfig, SymbolSpecs, get_config
from common.log import setup_logger

from signals.atr import ATR


# Import from contexts (now with fixed imports)
from contexts import (
    OrderflowSignalContext, PublishResult
)

from liquidity_geometry import LiquidityGeometryAnalyzer
from pipeline_adapter import PipelineAdapter

# Lazy import to avoid circular dependency: from signal_scoring import SignalScoringEngine, ScoringConfig, SignalContext as ScoringSignalContext
from local_calibration.store import LocalCalibrationStore as LCStoreV2, eval_local_quantile
from .regime_service import (
    MarketRegimeService, RegimeState
)
from .data_parser import OrderFlowDataParser
from services.l3_queue_events_proxy import L3QueueEventsProxy
from .data_processor import OrderFlowDataProcessor
from .signal_generator import SignalGenerator
from .cache_service import CacheService
from .message_handler import MessageHandler
from .config_manager import ConfigManager
from .error_handler import ErrorHandler
from .initialization_manager import InitializationManager
from .data_extraction_service import DataExtractionService
from .signal_execution_service import SignalExecutionService
from .cooldown_service import CooldownService
# ПРИМЕЧАНИЕ: устаревание L2 обрабатывается в OrderFlowDataProcessor._update_l2_tick_staleness()
from .calibration_service import CalibrationService
from .session_service import SessionService
from .signal_processing_service import SignalProcessingService
from .main_loop_service import MainLoopService
from .atr_redis_publisher import AtrRedisPublisher
from .handler_dependencies import HandlerDependencies
from .lifecycle.state_manager import HandlerStateManager

# Runtime imports for services (moved from TYPE_CHECKING if used in logic)
try:
    from signals.signal_publisher import SignalPublisher
except ImportError:
    SignalPublisher = None

# ZoneType now imported from contexts

# Import context utilities from extracted module
from .context_helpers.context_utils import (
    to_float_or_nan as _to_float_or_nan,
    to_opt_float as _to_opt_float,
    ensure_levels,  # re-exported so callers can do: from handlers.base_orderflow_handler import ensure_levels
)


class BaseOrderFlowHandler(ABC):
    redis: Any  # Redis client
    redis_ticks: Any  # Redis client for ticks
    group: str  # Redis consumer group name
    consumer_name_prefix: str  # Consumer name prefix
    _cache_service: "CacheService"  # Cache service instance
    _error_handler: "ErrorHandler"  # Error handler instance
    _lock: threading.Lock  # Lock for thread safety
    _state_manager: "HandlerStateManager"  # State manager instance

    # Core Services
    _init_manager: "InitializationManager"
    _config_manager: "ConfigManager"
    _data_parser: "OrderFlowDataParser"
    _data_processor: "OrderFlowDataProcessor"
    _data_extraction: "DataExtractionService"
    _cooldown_service: "CooldownService"
    _calibration: "CalibrationService"
    _session: "SessionService"
    _signal_processing: "SignalProcessingService"
    _signal_generator: "SignalGenerator"
    _signal_execution: "SignalExecutionService"
    _pipeline_adapter: "PipelineAdapter"
    _message_handler: "MessageHandler"
    _main_loop: "MainLoopService"
    _atr_publisher: "AtrRedisPublisher"
    atr_calculator: Optional["ATR"]

    # Optional/Dynamic services
    _scoring_engine: Optional["SignalScoringEngine"]
    _regime_service: Optional["MarketRegimeService"]
    _extrema_service: Optional["LocalExtremaService"]
    _execution_setup: Optional["ExecutionSetupRepository"]
    _outbox_publisher: Optional["SignalPublisher"]
    l2_gpu_processor: Optional[Any]

    # Execution components
    _signal_service: Optional["SignalService"]
    _execution_planner: Optional["ExecutionPlanner"]
    _signal_repo: Optional["SignalRepository"]
    _signal_bus: Optional["SignalBus"]
    _performance_tracker: Optional["SignalPerformanceTracker"]

    # Other metadata
    health_metrics: Optional[Any]
    specs: "SymbolSpecs"
    config: "OrderFlowConfig"

    # State manager properties for backward compatibility
    @property
    def is_running(self) -> bool:
        """Dynamic read from state manager."""
        return self._state_manager.is_running

    @is_running.setter
    def is_running(self, value: bool) -> None:
        """Allow direct assignment for compatibility."""
        self._state_manager.is_running = value

    @property
    def _stop_event(self) -> threading.Event:
        """Dynamic read from state manager."""
        return self._state_manager._stop_event

    @_stop_event.setter
    def _stop_event(self, value: threading.Event) -> None:
        """Allow direct assignment for compatibility."""
        self._state_manager._stop_event = value

    @property
    def _lock(self) -> threading.Lock:
        """Dynamic read from state manager."""
        return self._state_manager._lock

    @property
    def _thread(self) -> Optional[threading.Thread]:
        """Dynamic read from state manager."""
        return self._state_manager._thread

    @_thread.setter
    def _thread(self, value: Optional[threading.Thread]) -> None:
        """Allow direct assignment for compatibility."""
        self._state_manager._thread = value

    @property
    def _start_time(self) -> float:
        """Dynamic read from state manager."""
        return self._state_manager._start_time

    """
    Надежный и эффективный базовый handler.

    Улучшения/принципы:
    - pending recovery: claim + обработка, ACK только на успех
    - различение transient infra ошибок vs poison сообщений (DLQ только для poison)
    - снижение нагрузки: robust-zscore/сигналы считаются только на границе delta-bucket
    - OBI только из book-stream; при stale book сбрасываем sustained/state
    - корректная статистика: published_signals увеличивается только при реальном publish в outbox
    - anti-chatter: cooldown по (kind, level_key) поверх min_signal_interval
    """

    DLQ_STREAM_ENV = "ORDERFLOW_DLQ_STREAM"
    DLQ_DEFAULT = "stream:dlq:orderflow"

    def __init__(
        self,
        symbol: str,
        config: Optional[OrderFlowConfig] = None,
        *,
        source_name: str = "OrderFlow",
        signal_stream_prefix: str = "signals:orderflow",
        htf_provider: Optional[HTFLevelsProvider] = None,
        local_calibration: Optional[LCStoreV2] = None,
        unified_pipeline: Optional["UnifiedSignalPipeline"] = None,
        health_metrics: Optional[object] = None,
        dependencies: Optional[HandlerDependencies] = None,
    ):
        # Валидация входных данных
        self._validate_inputs(symbol, config, source_name, signal_stream_prefix)

        # Сохранение входных параметров
        self.source_name = source_name
        self.signal_stream_prefix = signal_stream_prefix
        self._htf_provider = htf_provider
        self.local_calibration = local_calibration
        self._unified_pipeline = unified_pipeline
        self.health_metrics = health_metrics
        self.dependencies = dependencies or HandlerDependencies()

        # Базовая инициализация
        self.symbol = symbol
        self.config = config or get_config(symbol, use_env=True)

        # Initialize state manager first to ensure state is always available
        self._state_manager = HandlerStateManager()

        # Паттерн Builder: пошаговая инициализация
        self._initialize_basic_state()
        self._initialize_symbol_specs()
        self._initialize_infrastructure()
        self._initialize_services()
        self._initialize_ui_components()
        self._initialize_legacy_state()

        # Финальное логирование
        self._log_initialization_complete()

    @property
    def liq_max_age_ms(self) -> int:
        return self._liq_max_age_ms

    def _validate_inputs(
        self,
        symbol: str,
        config: Optional[OrderFlowConfig],
        source_name: str,
        signal_stream_prefix: str
    ) -> None:
        """Валидация входных параметров."""
        if not symbol or not isinstance(symbol, str) or len(symbol.strip()) == 0:
            raise InvalidSymbolError(f"Invalid symbol: '{symbol}'. Must be non-empty string.")

        if not source_name or not isinstance(source_name, str):
            raise ValueError(f"Invalid source_name: '{source_name}'. Must be non-empty string.")

        if not signal_stream_prefix or not isinstance(signal_stream_prefix, str):
            raise ValueError(f"Invalid signal_stream_prefix: '{signal_stream_prefix}'. Must be non-empty string.")

        # Config validation if provided
        if config is not None:
            self._validate_config(config)

    def _validate_config(self, config: OrderFlowConfig) -> None:
        """Валидация объекта конфигурации на согласованность."""
        if not hasattr(config, 'symbol') or not config.symbol:
            raise MissingConfigError("Config must have valid 'symbol' attribute.")

        # Validate critical thresholds
        required_attrs = ['main_z_threshold', 'breakout_z_threshold', 'obi_threshold', 'delta_bucket_ms']
        for attr in required_attrs:
            if hasattr(config, attr):
                value = getattr(config, attr)
                if attr.endswith('_threshold') and (not isinstance(value, (int, float)) or value < 0):
                    raise ValueError(f"Config.{attr} must be non-negative number, got: {value}")
                elif attr == 'delta_bucket_ms' and (not isinstance(value, int) or value <= 0):
                    raise ValueError(f"Config.{attr} must be positive integer, got: {value}")

        # Валидация логической согласованности
        if hasattr(config, 'main_z_threshold') and hasattr(config, 'breakout_z_threshold'):
            main_z = getattr(config, 'main_z_threshold', 0)
            breakout_z = getattr(config, 'breakout_z_threshold', 0)
            if breakout_z <= main_z:
                logging.warning(
                    f"Breakout Z threshold should be higher than main Z threshold: main={main_z}, breakout={breakout_z}"
                )

    def _initialize_basic_state(self) -> None:
        """Инициализация базового состояния handler."""
        self.logger = setup_logger(f"{self.__class__.__name__}:{self.symbol}")

        # State manager is now initialized in __init__ for guaranteed availability

        # Базовая конфигурация
        self.venue = "mt5"  # по умолчанию для базового orderflow
        self.timeframe = "1m"  # по умолчанию
        self.family = "orderflow"  # тип сигнала для контроля качества

        # читаем LIQ_MAX_AGE_MS один раз при инициализации
        self._liq_max_age_ms: int = int(os.getenv("LIQ_MAX_AGE_MS", "5000"))
        
        # Consolidate config: reading DEDUPLICATION_BUCKET_MS here
        self._dedup_bucket_ms: int = int(os.getenv("DEDUPLICATION_BUCKET_MS", "60000"))

        # Refactoring Phase 4: Use Injected LiquidityGeometryAnalyzer
        LGA = self.dependencies.liquidity_analyzer
        if LGA:
            self._liquidity_geometry = LGA(self._liq_max_age_ms)
        else:
            # Fallback for backward compatibility (or if dependency resolution failed)
            try:
                self._liquidity_geometry = LiquidityGeometryAnalyzer(self._liq_max_age_ms)
            except ImportError:
                 self.logger.warning("LiquidityGeometryAnalyzer not available (dependency missing)")
                 self._liquidity_geometry = None

        # Refactoring Phase 4: Use Injected ATR
        ATR_Class = self.dependencies.atr_indicator
        if ATR_Class:
             self.atr_calculator = ATR_Class(period=14)
        else:
             try:
                 from signals.atr import ATR
                 self.atr_calculator = ATR(period=14)
             except ImportError:
                 self.logger.warning("ATR calculator not available")
                 self.atr_calculator = None

    def _initialize_symbol_specs(self) -> None:
        """Инициализация специфичной для символа конфигурации."""
        self.specs = self._get_symbol_specs()

    def _initialize_infrastructure(self) -> None:
        """Инициализация базовой инфраструктуры (Redis, streams и т.д.)."""
        # Создаем менеджер инициализации и инициализируем подсистемы ядра первыми
        self._init_manager = InitializationManager(self)
        
        # Получаем явный объект инфраструктуры (Initialization Contract)
        infra = self._init_manager.initialize_all(self.symbol, self.config, self.local_calibration, self._unified_pipeline)
        
        # Присваиваем инфраструктуру хендлеру
        self.redis = infra.redis
        self.redis_ticks = infra.redis_ticks
        self.tick_stream = infra.tick_stream
        self.book_stream = infra.book_stream
        self.l3_stream = infra.l3_stream
        self.group = infra.group
        self.consumer_name_prefix = infra.consumer_name_prefix
        self._cache_service = infra.cache_service
        self._config_manager = infra.config_manager

        # Инициализация ATR publisher (требуется сервисам, инициализированным выше)

        self._atr_publisher = AtrRedisPublisher(self.redis, self.symbol)

        # Проверка того, что базовая инфраструктура инициализирована
        self._validate_infrastructure()

    def _validate_infrastructure(self) -> None:
        """Валидация доступности всей необходимой инфраструктуры."""
        if self.redis is None:
            raise DependencyError("Redis not initialized")
        if not hasattr(self, 'tick_stream') or not self.tick_stream:
            raise DependencyError("Tick stream not initialized")
        if not hasattr(self, 'book_stream') or not self.book_stream:
            raise DependencyError("Book stream not initialized")
        if not hasattr(self, 'l3_stream'):
            raise DependencyError("L3 stream not initialized")

    def health_check(self) -> Dict[str, Any]:
        """Delegate comprehensive health check to HealthMonitorService."""
        monitor = self.dependencies.health_monitor
        if monitor:
             return monitor.health_check(self)
        
        # Fallback if monitor not available (legacy)
        return {"status": "unknown", "message": "HealthMonitorService not initialized"}

    def _initialize_services(self) -> None:
        """Инициализация всех бизнес-сервисов."""
        # Обработка ошибок
        self._error_handler = ErrorHandler(self.symbol, max_fail_retries=int(getattr(self.config, "max_fail_retries", 3)))

        # ----- L3-lite queue-events proxy (дополнительные метрики)
        l3_alpha = float(os.getenv("L3_TAKER_RATE_EMA_ALPHA", "0.12"))
        _bucket_ms = int(getattr(self.config, "delta_bucket_ms", 1000) or 1000)
        self.l3_queue = L3QueueEventsProxy(bucket_ms=_bucket_ms, alpha=l3_alpha)

        # GPU Processor (Async Logging / Metrics)
        self.l2_gpu_processor = None
        if os.getenv("GPU_ENABLED", "0") == "1":
            try:
                from gpu.l2_processor import L2GPUProcessor
                self.l2_gpu_processor = L2GPUProcessor(self.symbol)
            except ImportError:
                self.logger.warning("GPU_ENABLED=1 but gpu.l2_processor not found")
            except Exception as e:
                self.logger.error(f"Failed to init L2GPUProcessor: {e}")

        # Сервисы обработки данных
        self._data_parser = OrderFlowDataParser(self.symbol, self.specs)
        self._data_processor = OrderFlowDataProcessor(
            self.symbol, self.specs, self.config,
            atr_publisher=self._atr_publisher,
            atr_calculator=self.atr_calculator,
            # NEW: health_metrics прокидываем прямо в DataProcessor,
            # чтобы on_tick(L2 freshness) вызывался в tick-loop.
            # Это заполняет orderflow:{symbol}:health_snapshot корректными данными.
            health_metrics=getattr(self, "health_metrics", None),
            l3_queue=self.l3_queue,
            l2_gpu_processor=self.l2_gpu_processor,
        )
        self._data_extraction = DataExtractionService(self.symbol)

        # Сервисы анализа
        # Phase 7: Use injected CooldownService class
        CooldownCls = self.dependencies.cooldown_service_cls
        if CooldownCls:
             self._cooldown_service = CooldownCls(self.symbol, self.redis)
        else:
             from .cooldown_service import CooldownService
             self._cooldown_service = CooldownService(self.symbol, self.redis)

        # Phase 7: Use injected CalibrationService class
        CalibrationCls = self.dependencies.calibration_service_cls
        if CalibrationCls:
             self._calibration = CalibrationCls(self.symbol, self.local_calibration, self.redis, self._config_manager, quantile_fn=eval_local_quantile)
        else:
             from .calibration_service import CalibrationService
             self._calibration = CalibrationService(self.symbol, self.local_calibration, self.redis, self._config_manager, quantile_fn=eval_local_quantile)
        self._session = SessionService(self.symbol, config=self._config_manager)

        # Генерация и исполнение сигналов
        self._initialize_signal_services()

        # Обработка сигналов (требует, чтобы signal_generator был инициализирован первым)
        self._signal_processing = SignalProcessingService(
            self.symbol,
            unified_pipeline=self._unified_pipeline,
            signal_generator=self._signal_generator,
            health_metrics=self.health_metrics,
            outbox=getattr(self._unified_pipeline, 'outbox', None) if self._unified_pipeline else None,
        )

    def _initialize_signal_services(self) -> None:
        """Инициализация сервисов генерации и исполнения сигналов."""
        # Инициализация генератора сигналов с publisher
        publisher = None
        if SignalPublisher and self._unified_pipeline:
            # Пытаемся получить publisher из unified_pipeline или создать новый
            publisher = getattr(self._unified_pipeline, 'publisher', None)
            if not publisher:
                # Создаем SignalPublisher с outbox из unified_pipeline
                outbox = getattr(self._unified_pipeline, 'outbox', None)
                if outbox:
                    publisher = SignalPublisher(outbox=outbox)

        self._signal_generator = SignalGenerator(
            self.symbol, self.config, publisher, self._cooldown_service,
            dedup_bucket_ms=self._dedup_bucket_ms,
            config_manager=self._config_manager,
            health_metrics=self.health_metrics
        )

        self._signal_execution = SignalExecutionService(
            self.symbol,
            signal_generator=self._signal_generator,
            config_manager=self._config_manager,
        )

        # Установка компонентов исполнения, если доступны
        self._signal_execution.set_execution_components(
            execution_planner=getattr(self, '_execution_planner', None),
            signal_repo=getattr(self, '_signal_repo', None),
            signal_bus=getattr(self, '_signal_bus', None),
            performance_tracker=getattr(self, '_performance_tracker', None),
        )

    def _initialize_ui_components(self) -> None:
        """Инициализация UI и компонентов мониторинга."""
        # Pipeline adapter
        self._pipeline_adapter = PipelineAdapter(self._unified_pipeline)

        # Инициализация обработчика сообщений и основного цикла (подключение health_metrics)
        self._message_handler = MessageHandler(
            symbol=self.symbol,
            tick_stream=self.tick_stream,
            book_stream=self.book_stream,
            l3_stream=self.l3_stream,
            data_parser=self._data_parser,
            data_processor=self._data_processor,
            config=self.config,
            health_metrics=self.health_metrics,
            on_bar_closed=self._on_1m_bar_closed,
            on_bucket_closed=self._on_signal_bucket_closed,
            error_handler=self._error_handler,
            on_l3_event=self._process_l3_event,
        )

        self._main_loop = MainLoopService(
            tick_stream=self.tick_stream,
            book_stream=self.book_stream,
            l3_stream=self.l3_stream,
            message_handler=self._message_handler,
            error_handler=self._error_handler,
            config=self.config,
            health_metrics=self.health_metrics,
            symbol=self.symbol,
        )



    def _initialize_legacy_state(self) -> None:
        """Инициализация устаревших переменных состояния для обратной совместимости."""
        # L2 warn throttling
        self._last_l2_warn_ms = 0

        # ----- L3-lite queue-events proxy (дополнительные метрики)
        # self.l3_queue now initialized in _initialize_services to be available for data_processor
        self.l3_eps = float(os.getenv("L3_EPS", "1e-9"))

        # ETA evaluator (depth / taker_rate -> time-to-fill proxy)
        self.eta_eval: Optional[QueueETAEvaluator] = None
        try:
            self.eta_eval = QueueETAEvaluator(eps=self.l3_eps)
        except Exception:
            self.eta_eval = None

        # Execution-quality gating (burstiness + OBI + ETA)
        self.exec_filters_enabled = os.getenv("EXEC_FILTERS_ENABLED", "true").lower() == "true"
        # optional ETA gates (seconds)
        self.eta_max_sec = float(os.getenv("ETA_MAX_SEC", "2.5"))

        # ----- OBI sustained quality
        self.obi_use_fraction = os.getenv("OBI_SUSTAINED_USE_FRACTION", "true").lower() == "true"
        self.obi_min_samples = int(os.getenv("OBI_SUSTAINED_MIN_SAMPLES", "3"))
        self.obi_min_fraction = float(os.getenv("OBI_SUSTAINED_MIN_FRACTION", "0.6"))

        # OBI state (legacy 5-depth)
        self._last_obi = 0.0
        self._last_obi_ts = 0

        # separate OBI deques for 5 and 20 (avg+sustained stability)
        self._obi_state_5 = deque(maxlen=64)
        self._obi_state_20 = deque(maxlen=256)
        self._last_obi_20 = 0.0
        self._last_obi_20_ts = 0

        # ATR уже инициализирован в _initialize_basic_state()

        # pivots
        self.daily_pivots: Optional[Dict[str, float]] = None
        self.last_pivot_date = None

        # bar range for weak progress (minute bar)
        self.bar_high = -1e9
        self.bar_low = 1e9
        self.bar_start_ts = 0

        # breakout cross - use previous evaluation price (bucket boundary), not every tick
        self._prev_eval_price: Optional[float] = None

        # snapshot
        self.snap_prefix = os.getenv("SNAP_PREFIX", "signal:snap:")
        self.snap_ttl = int(os.getenv("SNAP_TTL", "21600"))

        # counters
        # self.processed_ticks = 0  # Replaced by property below
        self.processed_books = 0
        self.published_signals = 0
        self.signal_count_long = 0
        self.signal_count_short = 0

        self.max_tick_lag_ms = int(os.getenv("MAX_TICK_LAG_MS", "5000"))

        # Initialize cum_delta regime window for slope calculation
        self._cumdelta_regime_window: deque[dict] = deque(maxlen=30)

        # Initialize regime state with default values
        self.regime_state = RegimeState(
            regime="unknown",
            confidence=0.0,
            last_update=time.time(),
            score=0.0,
        )

    @property
    def processed_ticks(self) -> int:
        """Returns total ticks processed by the main loop."""
        if hasattr(self, "_main_loop") and self._main_loop:
            return getattr(self._main_loop, "total_tick_cnt", 0)
        return 0

    def _log_initialization_complete(self) -> None:
        """Логирование успешной инициализации с ключевыми параметрами."""
        try:
            log_data = self._get_structured_init_data()
            self.logger.info(
                "Handler initialized: %(handler)s for %(symbol)s | "
                "source=%(source)s | streams: tick=%(tick_stream)s book=%(book_stream)s | "
                "thresholds: main_z=%(main_z).2f breakout_z=%(breakout_z).2f | "
                "bucket=%(bucket)dms | obi_threshold=%(obi_threshold).3f",
                log_data
            )
        except Exception as e:
            self.logger.warning(f"Failed to log initialization details: {e}")

    def _get_structured_init_data(self) -> Dict[str, Any]:
        """Получение структурированных данных для логирования инициализации."""
        return {
            "handler": self.__class__.__name__,
            "symbol": self.symbol,
            "source": self.source_name,
            "tick_stream": getattr(self, 'tick_stream', 'unknown'),
            "book_stream": getattr(self, 'book_stream', 'unknown'),
            "main_z": getattr(self.config, 'main_z_threshold', 0.0),
            "breakout_z": getattr(self.config, 'breakout_z_threshold', 0.0),
            "bucket": getattr(self.config, 'delta_bucket_ms', 60000),
            "obi_threshold": getattr(self.config, 'obi_threshold', 0.0),
            "unified_pipeline": self._unified_pipeline is not None,
            "cooldown_enabled": hasattr(self, '_cooldown_service') and self._cooldown_service is not None,
            "l3_enabled": self.l3_queue is not None,
            "burst_enabled": getattr(self, "burst", None) is not None,
            "gpu_enabled": getattr(self, "l2_gpu_processor", None) is not None,
            "extrema_enabled": getattr(self, "_extrema_service", None) is not None,
            "regime_enabled": getattr(self, "_regime_service", None) is not None,
            "calibration_enabled": getattr(self, "local_calibration", None) is not None,
            "scoring_enabled": getattr(self, "_scoring_engine", None) is not None,
        }

    def _log_structured(self, level: str, message: str, **kwargs) -> None:
        """Структурированное логирование сообщения с дополнительным контекстом."""
        extra_data = {
            "symbol": self.symbol,
            "handler": self.__class__.__name__,
            "timestamp": time.time(),
            **kwargs
        }

        log_message = f"{message} | symbol={self.symbol}"

        if level == "debug":
            self.logger.debug(log_message, extra=extra_data)
        elif level == "info":
            self.logger.info(log_message, extra=extra_data)
        elif level == "warning":
            self.logger.warning(log_message, extra=extra_data)
        elif level == "error":
            self.logger.error(log_message, extra=extra_data)
        elif level == "critical":
            self.logger.critical(log_message, extra=extra_data)


    @abstractmethod
    def _get_symbol_specs(self) -> SymbolSpecs:
        raise NotImplementedError

    def _get_calibrated_trailing_params(self) -> Dict[str, Any]:
        """
        Читает откалиброванные параметры из Redis symbol_specs.
        Возвращает параметры для трейлинга или fallback на значения из конфига.
        """
        return self._calibration.get_calibrated_trailing_params()

    def _get_min_confidence_for_symbol(self, symbol: str | None) -> float:
        """
        Возвращает минимальный порог confidence для символа.
        """
        return self._config_manager.get_min_confidence_for_symbol(symbol)

    # -------------------- lifecycle --------------------

    def start(self) -> None:
        """Start the orderflow handler thread with proper thread safety."""
        with self._lock:
            if self._thread and self._thread.is_alive():
                self.logger.warning("Orderflow thread already running for %s", self.symbol)
                return
            if self.is_running and not self._stop_event.is_set():
                self.logger.warning("Orderflow marked running but thread not alive: %s", self.symbol)
            
            # Clear stop event and create thread
            self._stop_event.clear()
            self._thread = threading.Thread(
                target=self._run_loop,
                daemon=True,
                name=f"orderflow:{self.symbol}",
            )
            
            # Fix Race Condition: Set flag BEFORE starting thread
            self.is_running = True
            
            try:
                self._thread.start()
                self.logger.info("Orderflow thread started for %s", self.symbol)
            except Exception as e:
                # Revert if start fails
                self.is_running = False
                self._stop_event.set()
                self._thread = None
                self.logger.error("Failed to start orderflow thread for %s: %s", self.symbol, e)
                raise

    def _run_loop(self) -> None:
        """Main processing loop - delegates to MainLoopService."""
        try:
            if not getattr(self, "_main_loop", None):
                self.logger.error("MainLoopService not initialized")
                return

            # Усиленные проверки зависимостей
            if not getattr(self, "group", None):
                raise RuntimeError("group not initialized")
            if not getattr(self, "consumer_name_prefix", None):
                raise RuntimeError("consumer_name_prefix not initialized")
            if getattr(self, "redis_ticks", None) is None:
                raise RuntimeError("redis_ticks not initialized")

            consumer_name = f"{self.consumer_name_prefix}-{os.getpid()}-{int(time.time())}"

            from core.redis_stream_consumer import SyncRedisStreamHelper
            consumer = SyncRedisStreamHelper(self.redis_ticks, self.group, consumer_name)

            # Обязательно используем метод run с stop_event для гарантированной остановки
            if not hasattr(self._main_loop, "run"):
                raise RuntimeError("MainLoopService must implement run(consumer, stop_event) for proper thread lifecycle management")

            self._main_loop.run(consumer, stop_event=self._stop_event)
        except Exception:
            self.logger.exception("Orderflow loop crashed: %s", self.symbol)
        finally:
            # гарантируем консистентность состояния
            with self._lock:
                self.is_running = False
                self._stop_event.set()

    def process_orderflow_signal(self, ctx: "OrderflowSignalContext", signal_type: str = "bar") -> "PublishResult":
        """
        Process signal context using the new unified signal processing architecture.
        """
        # Pass signal_type to the processing service
        result = self._signal_processing.process_orderflow_context(ctx, signal_type=signal_type)

        return result

    def _emit_health_metrics(self, ctx: Any) -> None:
        """
        Single Source of Truth для отправки метрик здоровья (tick/health).
        Делегирует HealthMonitorService или делает fallback.
        """
        hm = getattr(self, "health_metrics", None)
        if hm is None or ctx is None:
            return

        # 1. Делегируем монитору (Primary)
        if self.dependencies.health_monitor:
            try:
                self.dependencies.health_monitor.on_tick_health_emit(hm, self.symbol, ctx)
                return
            except Exception as e:
                self.logger.debug("health_monitor.on_tick_health_emit failed: %s", e)

        # 2. Fallback (Legacy/Internal)
        try:
            # Прямой доступ к слотам OrderflowSignalContext (быстрее getattr)
            try: l2_age_ms = _to_float_or_nan(ctx.l2_age_ms)
            except AttributeError: l2_age_ms = float("nan")
            
            try: z_score = _to_float_or_nan(ctx.z_delta)
            except AttributeError: z_score = float("nan")
            
            try: obi = _to_float_or_nan(ctx.obi)
            except AttributeError: obi = float("nan")
            
            try: obi_20 = _to_float_or_nan(ctx.obi_20)
            except AttributeError: obi_20 = float("nan")
            
            try: obi_sustained = bool(ctx.obi_sustained)
            except AttributeError: obi_sustained = False
            
            try: spread_bps = _to_float_or_nan(ctx.spread_bps)
            except AttributeError: spread_bps = float("nan")

            try: eta_fill_ms = _to_opt_float(ctx.eta_fill_ms if hasattr(ctx, 'eta_fill_ms') else None)
            except Exception: eta_fill_ms = None
            
            try: burst_ratio = _to_opt_float(ctx.burst_ratio)
            except AttributeError: burst_ratio = None
            
            try: imbalance_min = _to_opt_float(ctx.imbalance_min if hasattr(ctx, 'imbalance_min') else None)
            except AttributeError: imbalance_min = None

            try: l2_is_stale = bool(ctx.l2_is_stale)
            except AttributeError: l2_is_stale = True

            _safe_call_fail_open(
                getattr(self, "logger", None),
                key="health_metrics.on_tick",
                fn=hm.on_tick,
                kwargs=dict(
                    symbol=self.symbol,
                    l2_age_ms=l2_age_ms,
                    z_score=z_score,
                    obi=obi,
                    obi_20=obi_20,
                    obi_sustained=obi_sustained,
                    spread_bps=spread_bps,
                    eta_fill_ms=eta_fill_ms,
                    burst_ratio=burst_ratio,
                    imbalance_min=imbalance_min,
                    l2_is_stale=l2_is_stale,
                ),
            )
        except Exception:
            pass

    def _on_1m_bar_closed(self, bar: object) -> None:
        """
        Единственная точка входа в signal pipeline по событию закрытия 1m бара.
        Обновляет пивоты и генерирует бар-сигнал.
        """
        try:
            # Get bar timestamp for pivots validation - prefer close time for bar-signals
            event_ts_ms = int(
                getattr(bar, "ts_close", 0)
                or (int(getattr(bar, "ts_open", 0) or 0) + 60_000)
                or get_ny_time_millis()
            )

            # Ensure pivots are up-to-date (daily check inside CacheService)
            try:
                self._cache_service.ensure_pivots_bundle(event_ts_ms)
            except Exception as e:
                self.logger.debug("ensure_pivots_bundle failed: %s", e)

            # Get pivots from cache service
            pivots = None
            try:
                pivots = self._cache_service.get_pivots_bundle()
            except Exception as e:
                self.logger.debug("Failed to get pivots bundle: %s", e)

            # build_signal_ctx is in OrderFlowDataProcessor
            ctx = self._data_processor.build_signal_ctx(pivots=pivots)
            
            # ВАЖНО: фиксируем время события именно как close-time бара
            if hasattr(ctx, "ts"):
                ctx.ts = event_ts_ms

            # Attach session fields to context
            self._session.attach_to_ctx(ctx)
            # Apply calibration before signal processing
            self._calibration.calibrate_context(ctx)

            # Подаем метрики здоровья
            self._emit_health_metrics(ctx)

        except Exception as e:
            self.logger.warning("build_signal_ctx failed on bar close: %s", e)
            hm = getattr(self, "health_metrics", None)
            if hm:
                _safe_call_fail_open(
                    getattr(self, "logger", None),
                    key="health_metrics.on_signal_bar_failed.1m",
                    fn=hm.on_signal_bar_failed,
                    args=(self.symbol,),
                )
            return

        try:
            _ = self.process_orderflow_signal(ctx, "bar")
        except Exception as e:
            self.logger.warning("process_orderflow_signal failed: %s", e)
            hm = getattr(self, "health_metrics", None)
            if hm:
                _safe_call_fail_open(
                    getattr(self, "logger", None),
                    key="health_metrics.on_signal_bar_failed.1m.2",
                    fn=hm.on_signal_bar_failed,
                    args=(self.symbol,),
                )
            return

    def _process_l3_event(self, event: Any) -> None:
        """Обработка L3 события (проксирование в L3QueueEventsProxy)."""
        if self.l3_queue is None:
            return

        # Определяем side: buy -> 1, sell -> -1
        side_str = getattr(event, "side", "")
        side = 1 if side_str == "buy" else (-1 if side_str == "sell" else 0)

        if side != 0:
            self.l3_queue.on_trade(side=side, qty=getattr(event, "qty", 0.0))

    def _on_signal_bucket_closed(self, ts_ms: int) -> None:
        """
        Bucket-close сигнализация (best-effort).
        """
        try:
            # Ensure pivots are up-to-date
            try:
                self._cache_service.ensure_pivots_bundle(int(ts_ms))
            except Exception as e:
                self.logger.debug("ensure_pivots_bundle failed (bucket): %s", e)

            dp = self._data_processor
            bs = getattr(dp, "_bucket_state", None)
            if bs is None:
                return

            # Прямой доступ к слоту BucketState.price (быстрее getattr)
            last_price = float(bs.price or 0.0)
            if last_price <= 0.0:
                return

            # Get pivots from cache service
            pivots = None
            try:
                pivots = self._cache_service.get_pivots_bundle()
            except Exception as e:
                self.logger.debug("Failed to get pivots bundle: %s", e)

            # build_signal_ctx
            ctx = self._data_processor.build_signal_ctx(pivots=pivots)
            
            # ВАЖНО: фиксируем время события boundary-bucket
            if hasattr(ctx, "ts"):
                ctx.ts = int(ts_ms)

            # Attach session fields to context
            self._session.attach_to_ctx(ctx)
            # Apply calibration before signal processing
            self._calibration.calibrate_context(ctx)

            # Ensure geometry/liquidity context is updated before scoring
            if hasattr(self, "_update_geometry_liquidity_context"):
                self._update_geometry_liquidity_context(ctx)

            # Bucket boundary финализация + генерация сигналов

            # Подаем метрики здоровья
            self._emit_health_metrics(ctx)

            result = self.process_orderflow_signal(ctx, "bucket")

            # Track bucket event outcome
            hm = getattr(self, "health_metrics", None)
            if hm:
                processed = result is not None and bool(getattr(result, "sent", False))
                _safe_call_fail_open(
                    getattr(self, "logger", None),
                    key="health_metrics.on_bucket_event",
                    fn=hm.on_bucket_event,
                    args=(self.symbol,),
                    kwargs=dict(processed=processed, suppressed=False),
                )

        except Exception as e:
            self.logger.warning("bucket-close signal flow failed: %s", e)
            hm = getattr(self, "health_metrics", None)
            if hm:
                _safe_call_fail_open(
                    getattr(self, "logger", None),
                    key="health_metrics.on_signal_bucket_failed",
                    fn=hm.on_signal_bucket_failed,
                    args=(self.symbol,),
                )
            return

    def refresh_pivots_cache(self) -> bool:
        """
        Manually refresh the pivots cache.
        Returns True if refresh was successful, False otherwise.

        This is useful for:
        - Manual cache refresh after market events
        - Testing pivot calculations
        - Recovery from cache corruption
        """
        try:
            current_ts_ms = get_ny_time_millis()
            self._cache_service.ensure_pivots_bundle(current_ts_ms)

            # Log cache status
            bundle = self._cache_service.get_pivots_bundle()
            if bundle:
                pivot_count = len(bundle.get("pivots", {}))
                cache_date = bundle.get("date", "unknown")
                self.logger.info(f"Pivots cache refreshed: {pivot_count} levels for date {cache_date}")
                return True
            else:
                self.logger.warning("Pivots cache refresh completed but no bundle found")
                return False

        except Exception as e:
            self.logger.error(f"Failed to refresh pivots cache: {e}")
            return False

    def stop(self) -> None:
        """Stop the orderflow handler thread with proper thread safety."""
        th = None
        with self._lock:
            self.is_running = False
            self._stop_event.set()
            th = self._thread

        if th and th.is_alive():
            th.join(timeout=5.0)
            if th.is_alive():
                self.logger.warning("Orderflow thread did not stop within timeout")

        with self._lock:
            if self._thread is th:
                self._thread = None


