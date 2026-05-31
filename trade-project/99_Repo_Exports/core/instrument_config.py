"""
Proxy module — Single Source of Truth.

Все определения перенесены в:
    python-worker/core/instrument_config.py

Docker volume: ./python-worker/core → /app/core (см. docker-compose-python-workers.yml)

Этот файл устарел и НЕ монтируется в production-контейнер.
НЕ редактируйте его. Редактируйте: python-worker/core/instrument_config.py
"""
try:
    from core.instrument_config import *  # noqa: F401, F403
    from core.instrument_config import (
        OrderFlowConfig,
        SymbolSpecs,
        get_config,
        get_specs,
        INSTRUMENT_CONFIGS,
        INSTRUMENT_SPECS,
        symbol_env_prefix,
        normalize_symbol,
    )
except ImportError:
    # Если запускается вне docker-контекста и python-worker не в sys.path —
    # делаем best-effort: добавляем путь и пробуем снова.
    import sys
    import os as _os
    _root = _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))
    _pw = _os.path.join(_root, "python-worker")
    if _pw not in sys.path:
        sys.path.insert(0, _pw)
    from core.instrument_config import *  # noqa: F401, F403
