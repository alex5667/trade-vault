"""
MT5 Bridge - Complete Execution Engine for Signals

MT5-мост для автоматического исполнения сигналов из scanner_infra.
Читает ExecutionPlan из Redis Streams, исполняет ордера в MT5,
и публикует реальные результаты сделок обратно в Redis.

Components:
- Mt5ExecutionPlan: Модель плана исполнения для MT5
- Mt5Client: Обертка над MetaTrader5 API с поддержкой валюты счета
- PlanExecutor: Логика исполнения (TTD, entry zones, partials)
- PlansStreamConsumer: Чтение планов из Redis Streams
- ExecEventsPublisher: Публикация execution events
- Mt5DealsWatcher: Отслеживание реальных сделок MT5
- ExecCommandsConsumer: Обработка команд (CLOSE_REQUEST и т.п.)
- Main воркер: Склейка всего в один процесс

Usage:
    # Запуск моста
    python -m mt5_bridge.main

Environment Variables:
    MT5_LOGIN: MT5 account login (required)
    MT5_PASSWORD: MT5 account password (required)
    MT5_SERVER: MT5 server (required)
    REDIS_DSN: Redis connection string (default: redis://localhost:6379/0)
    MT5_SYMBOL_MAP: Symbol mapping JSON (optional)
    POLL_BLOCK_MS: Redis polling block time (default: 500)
    STEP_INTERVAL: Main loop interval (default: 0.2)
"""

from .models import Mt5ExecutionPlan, plan_from_dict

# Import MT5 components only if MetaTrader5 is available
try:
    from .mt5_client import Mt5Client, Mt5Config
    MT5_AVAILABLE = True
except ImportError:
    MT5_AVAILABLE = False
    Mt5Client = None
    Mt5Config = None

from .exec_events import ExecEventsPublisher, ExecutionEvent
from .executor import ActivePlanState, PlanExecutor
from .redis_consumer import PlansStreamConsumer

# Import MT5-dependent components only if MetaTrader5 is available
try:
    from .commands_consumer import ExecCommandsConsumer
    from .deals_watcher import Mt5DealsWatcher
    MT5_EXTENDED_AVAILABLE = True
except ImportError:
    MT5_EXTENDED_AVAILABLE = False
    Mt5DealsWatcher = None
    ExecCommandsConsumer = None

__version__ = "1.0.0"
__all__ = [
    "Mt5ExecutionPlan",
    "plan_from_dict",
    "PlanExecutor",
    "ActivePlanState",
    "PlansStreamConsumer",
    "ExecutionEvent",
    "ExecEventsPublisher",
]

# Add MT5 components only if available
if MT5_AVAILABLE:
    __all__.extend(["Mt5Client", "Mt5Config"])

# Add extended MT5 components only if available
if MT5_EXTENDED_AVAILABLE:
    __all__.extend(["Mt5DealsWatcher", "ExecCommandsConsumer"])
