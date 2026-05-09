#!/usr/bin/env python3
"""
Тонкая оболочка‑обёртка для стабильности импортов.
Полная реализация находится в `pairs_handler_impl.py`.
"""
from .pairs_handler_impl import PairsDataHandler

__all__ = [
    'PairsDataHandler',
]
