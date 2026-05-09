#!/usr/bin/env python3
"""
Тонкая оболочка‑обёртка для стабильности импортов.
Полная реализация находится в `ticker_handler_impl.py`.
"""
from .ticker_handler_impl import TickerDataHandler

__all__ = [
    'TickerDataHandler',
]
