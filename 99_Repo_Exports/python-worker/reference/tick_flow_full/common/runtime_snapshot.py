from __future__ import annotations
from utils.time_utils import get_ny_time_millis

"""
runtime_snapshot.py
===================
Убираем парсинг ENV/создание объектов из hot-path.

Идея:
  - RuntimeSnapshot.load() парсит нужные ENV один раз
  - далее в цикле только чтение self._runtime
  - мягкий refresh: раз в N секунд (или по сигналу, если добавите)
"""

import os
import time
from dataclasses import dataclass
from typing import Dict, Optional


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)) or default)
    except Exception:
        return float(default)


@dataclass(frozen=True)
class RuntimeSnapshot:
    loaded_ms: int

    # confidence gates
    min_conf_default: float
    min_conf_factor_default: float
    min_conf_by_symbol: Dict[str, float]
    min_conf_factor_by_symbol: Dict[str, float]

    # trace sampling (optional, но удобно держать рядом)
    trace_log_sample_rate: float

    @staticmethod
    def load() -> "RuntimeSnapshot":
        now_ms = get_ny_time_millis()

        min_conf_default = _env_float("MIN_CONF_DEFAULT", 70.0)
        min_conf_factor_default = _env_float("MIN_CONF_FACTOR_DEFAULT", 0.45)

        # Собираем MIN_CONF_{SYM}, MIN_CONF_FACTOR_{SYM}
        by_sym: Dict[str, float] = {}
        by_sym_cf: Dict[str, float] = {}
        try:
            for k, v in os.environ.items():
                if not k:
                    continue
                if k.startswith("MIN_CONF_") and k not in ("MIN_CONF_DEFAULT",):
                    sym = k[len("MIN_CONF_") :].strip().upper()
                    try:
                        by_sym[sym] = float(v)
                    except Exception:
                        pass
                if k.startswith("MIN_CONF_FACTOR_") and k not in ("MIN_CONF_FACTOR_DEFAULT",):
                    sym = k[len("MIN_CONF_FACTOR_") :].strip().upper()
                    try:
                        by_sym_cf[sym] = float(v)
                    except Exception:
                        pass
        except Exception:
            pass

        trace_log_sample_rate = _env_float("DECISION_TRACE_LOG_SAMPLE_RATE", 0.02)

        return RuntimeSnapshot(
            loaded_ms=now_ms,
            min_conf_default=float(min_conf_default),
            min_conf_factor_default=float(min_conf_factor_default),
            min_conf_by_symbol=by_sym,
            min_conf_factor_by_symbol=by_sym_cf,
            trace_log_sample_rate=float(trace_log_sample_rate),
        )

    def min_conf(self, symbol: str) -> float:
        s = (symbol or "").strip().upper()
        return float(self.min_conf_by_symbol.get(s, self.min_conf_default))

    def min_conf_factor(self, symbol: str) -> float:
        s = (symbol or "").strip().upper()
        return float(self.min_conf_factor_by_symbol.get(s, self.min_conf_factor_default))


class RuntimeRefresher:
    """
    Держатель snapshot + мягкий refresh по таймеру.
    """

    def __init__(self, *, refresh_every_s: float = 10.0) -> None:
        self._refresh_every_s = max(0.5, float(refresh_every_s))
        self._runtime = RuntimeSnapshot.load()
        self._next_refresh = time.monotonic() + self._refresh_every_s

    @property
    def runtime(self) -> RuntimeSnapshot:
        self._maybe_refresh()
        return self._runtime

    def _maybe_refresh(self) -> None:
        try:
            now = time.monotonic()
            if now >= self._next_refresh:
                self._runtime = RuntimeSnapshot.load()
                self._next_refresh = now + self._refresh_every_s
        except Exception:
            # fail-open
            return

# --- Compatibility Alias for Tests ---
class RuntimeSnapshotCache:
    """
    Simulates old static cache for tests,
    delegating to RuntimeRefresher or just loading once.
    """
    @staticmethod
    def from_env() -> "RuntimeRefresher":
        # Testing expects an object that acts like it has a snapshot or refresher
        return RuntimeRefresher(refresh_every_s=999999)