#!/usr/bin/env python3
"""
Тонкая оболочка‑обёртка для стабильности импортов.
Полная реализация находится в `funding_handler_impl.py`.

Назначение:
- экспортировать `FundingDataHandler` без привязки к имени файла реализации
- сохранить стабильные импорты в других частях проекта
"""
from .funding_handler_impl import FundingDataHandler

__all__ = [
    'FundingDataHandler',
] 
