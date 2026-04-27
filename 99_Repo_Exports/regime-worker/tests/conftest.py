"""
tests/conftest.py — добавляем корень regime-worker в sys.path
чтобы тесты могли импортировать adx_atr, classify, quantiles без установки пакета.
"""
import sys
import os

# Добавляем корень /regime-worker/ в sys.path
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
