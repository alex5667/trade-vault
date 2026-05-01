#!/usr/bin/env python
# tools/dump_trailing_after_tp1_config.py
from __future__ import annotations
"""
Скрипт для вывода конфигурации trailing after TP1 по source/symbol.

Показывает:
- Для каких пар (source, symbol) реально включён after_tp1_enabled
- Какой offset_atr применяется
- Источник настройки (SymbolSpec / ENV / allowlist / global)
"""


import os
import sys
from typing import Dict, Tuple, List, Set, Optional

# Добавляем путь к корню проекта
# [AUTOGRAVITY CLEANUP] sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import redis

from domain.normalizers import canon_source, canon_symbol
from services.pnl_math import get_symbol_info, spec_from_symbol_info


REDIS_URL = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")


def _discover_pairs(r: redis.Redis) -> List[Tuple[str, str]]:
    """
    Находит пары (source, symbol) на основе stats:strategies + stats:symbols:<strategy>.
    """
    pairs: Set[Tuple[str, str]] = set()
    strategies = r.smembers("stats:strategies") or []

    mapping = {
        "ta": "TechnicalAnalysis",
        "orderflow": "OrderFlow",
        "cryptoorderflow": "CryptoOrderFlow",
        "aggregated": "AggregatedHub-V2",
    }

    for st in strategies:
        st_str = str(st)
        symbols = r.smembers(f"stats:symbols:{st_str}") or []
        for sym in symbols:
            sym_str = str(sym)
            src_raw = mapping.get(st_str.lower(), st_str)
            src = canon_source(src_raw)
            sy = canon_symbol(sym_str)
            pairs.add((src, sy))

    return sorted(pairs)


def _extract_spec_flag(spec) -> Optional[bool]:
    """
    Извлекает флаг из SymbolSpec:
    trailing_after_tp1_enabled / trailing_enabled.
    
    Если атрибут существует, возвращает его значение (даже если False).
    None возвращается только если атрибут отсутствует или значение не распознано.
    """
    for attr in ("trailing_after_tp1_enabled", "trailing_enabled"):
        if hasattr(spec, attr):
            val = getattr(spec, attr)
            # Если атрибут существует, используем его значение (даже если False)
            if isinstance(val, bool):
                return bool(val)
            if isinstance(val, (int, float)):
                return bool(val)
            if isinstance(val, str) and val.strip():
                # Пустая строка считается как "не задано"
                return val.lower() in ("1", "true", "yes", "on")
            # Если значение None или пустое - продолжаем поиск по другим атрибутам
    return None


def _get_spec_trailing_tp1_offset_atr(spec) -> Optional[float]:
    """
    Сырой trailing_tp1_offset_atr из SymbolSpec (если задан и > 0).
    """
    try:
        if not hasattr(spec, "trailing_tp1_offset_atr"):
            return None
        v = getattr(spec, "trailing_tp1_offset_atr", None)
        if v is None:
            return None
        v = float(v)
        if v <= 0:
            return None
        return v
    except Exception:
        return None


def _is_trailing_after_tp1_enabled_effective(
    source: str,
    symbol: str,
    spec,
) -> Tuple[bool, Dict[str, str]]:
    """
    Эффективный флаг after_tp1_enabled с приоритетами.

    SymbolSpec имеет ВЫСШИЙ ПРИОРИТЕТ (даже если значение False):
    1) SymbolSpec.trailing_after_tp1_enabled / trailing_enabled (из Redis) - ВЫСШИЙ ПРИОРИТЕТ
       Если в SymbolSpec явно задано False, оно перекрывает все ENV переменные
    2) ENV: TRAILING_AFTER_TP1_<SYMBOL> (используется только если SymbolSpec не задан)
    3) allowlist по source: TRAILING_AFTER_TP1_SOURCES (дефолт: CryptoOrderFlow)
    """
    symbol_up = (symbol or "").upper()
    source_norm = canon_source(source or "")

    meta: Dict[str, str] = {}

    # 1) spec flag (из Redis) - ВЫСШИЙ ПРИОРИТЕТ
    spec_flag = _extract_spec_flag(spec)
    if spec_flag is not None:
        meta["mode"] = "SPEC"
        meta["spec_flag"] = str(spec_flag)
        return bool(spec_flag), meta

    # 2) override по символу через ENV
    env_sym = os.getenv(f"TRAILING_AFTER_TP1_{symbol_up}")
    if env_sym is not None:
        enabled = env_sym.lower() in ("1", "true", "yes", "on")
        meta["mode"] = "ENV_SYMBOL"
        meta["env_sym"] = env_sym
        return enabled, meta

    # 3) allowlist по source
    sources_raw = os.getenv("TRAILING_AFTER_TP1_SOURCES", "CryptoOrderFlow")
    allowed_sources = {
        canon_source(s.strip())
        for s in sources_raw.split(",")
        if s.strip()
    }

    meta["mode"] = "ALLOWLIST"
    meta["allowlist"] = ",".join(sorted(allowed_sources)) if allowed_sources else ""
    enabled = bool(allowed_sources) and (source_norm in allowed_sources)
    return enabled, meta


def _resolve_trailing_tp1_offset_atr_effective(
    source: str,
    symbol: str,
    spec,
) -> Tuple[float, Dict[str, str]]:
    """
    Эффективный offset ATR с приоритетами.

    SymbolSpec имеет ВЫСШИЙ ПРИОРИТЕТ:
    1) spec.trailing_tp1_offset_atr (из Redis) - ВЫСШИЙ ПРИОРИТЕТ
    2) ENV: TRAILING_TP1_OFFSET_ATR_<SYMBOL> (используется только если SymbolSpec не задан)
    3) ENV: TRAILING_TP1_OFFSET_ATR_<SOURCE>
    4) глобальный TRAILING_TP1_OFFSET_ATR
    """
    symbol_up = (symbol or "").upper()
    source_norm = canon_source(source or "")

    meta: Dict[str, str] = {}

    # 1) spec (из Redis) - ВЫСШИЙ ПРИОРИТЕТ
    try:
        v = getattr(spec, "trailing_tp1_offset_atr", None)
        if v is not None:
            v_float = float(v)
            if v_float > 0:
                meta["mode"] = "SPEC"
                meta["spec_value"] = str(v_float)
                return v_float, meta
    except Exception:
        pass

    # 2) ENV по символу
    env_sym = os.getenv(f"TRAILING_TP1_OFFSET_ATR_{symbol_up}")
    if env_sym:
        try:
            v = float(env_sym)
            if v > 0:
                meta["mode"] = "ENV_SYMBOL"
                meta["env_sym"] = env_sym
                return v, meta
        except Exception:
            pass

    # 3) ENV по source
    env_src = os.getenv(f"TRAILING_TP1_OFFSET_ATR_{source_norm.upper()}")
    if env_src:
        try:
            v = float(env_src)
            if v > 0:
                meta["mode"] = "ENV_SOURCE"
                meta["env_src"] = env_src
                return v, meta
        except Exception:
            pass

    # 4) глобальный дефолт
    default_val = float(os.getenv("TRAILING_TP1_OFFSET_ATR", "0.6"))
    meta["mode"] = "GLOBAL"
    meta["global"] = str(default_val)
    return default_val, meta


def main() -> None:
    r = redis.from_url(REDIS_URL, decode_responses=True)

    pairs = _discover_pairs(r)
    if not pairs:
        print("⚠️ Не найдено пар source/symbol в stats:strategies / stats:symbols:*")
        return

    enabled_rows: List[str] = []
    disabled_rows: List[str] = []

    header = (
        f"{'SOURCE':20} "
        f"{'SYMBOL':10} "
        f"{'ENABLED':8} "
        f"{'OFFSET_ATR':10} "
        f"{'SPEC_ATR':10} "
        f"{'MODE':12} "
        f"{'DETAIL':20}"
    )
    sep = "-" * len(header)

    for source, symbol in pairs:
        info = get_symbol_info(symbol, r)
        spec = spec_from_symbol_info(info)

        enabled, meta_en = _is_trailing_after_tp1_enabled_effective(source, symbol, spec)
        offset, meta_off = _resolve_trailing_tp1_offset_atr_effective(source, symbol, spec)
        spec_off = _get_spec_trailing_tp1_offset_atr(spec)
        spec_off_str = f"{spec_off:.3f}" if spec_off is not None else "-"

        mode = meta_en.get("mode", "")
        detail = (
            meta_en.get("env_sym")
            or meta_en.get("spec_flag")
            or meta_en.get("allowlist")
            or meta_off.get("env_sym")
            or meta_off.get("env_src")
            or meta_off.get("spec_value")
            or meta_off.get("global", "")
        )

        row = (
            f"{source:20} "
            f"{symbol:10} "
            f"{str(enabled):8} "
            f"{offset:10.3f} "
            f"{spec_off_str:10} "
            f"{mode:12} "
            f"{detail:20}"
        )

        if enabled:
            enabled_rows.append(row)
        else:
            disabled_rows.append(row)

    print("=== Trailing after TP1: ENABLED ===")
    print(header)
    print(sep)
    for row in enabled_rows:
        print(row)

    print()
    print("=== Trailing after TP1: DISABLED ===")
    print(header)
    print(sep)
    for row in disabled_rows:
        print(row)


if __name__ == "__main__":
    main()

