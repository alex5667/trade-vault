"""
Proxy module — Single Source of Truth.

Все определения перенесены в:
    python-worker/core/instrument_config.py

Этот файл — тонкая обёртка. НЕ редактируйте его.
Редактируйте: core/instrument_config.py
"""
# Re-export всего публичного API через core, чтобы существующие импортеры
# (например, services/binance_executor.py) продолжали работать без изменений.
from core.instrument_config import *  # noqa: F401, F403
