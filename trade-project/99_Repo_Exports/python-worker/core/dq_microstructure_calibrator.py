from __future__ import annotations

"""
DqMicrostructureCalibrator — rolling per-symbol staleness & spread DQ thresholds.

Проблема:
  signal_pipeline._build_gate_ctx использует глобальные константы:
    _cached_dq_book_stale_flag_ms   = 1500 ms  (одинаково для всех символов)
    _cached_dq_spread_wide_flag_bps = 12 bps
  BTC обновляет book каждые 5–20 ms → 1500 ms = 75× нормы (слишком мягко).
  PEPE/SHIB обновляет book каждые 200–500 ms → 1500 ms = 3× нормы (слишком жёстко).

Метод:
  - Streaming P² (Jain & Chlamtac) — O(1) память, 5 маркеров.
  - stale_threshold  = clamp(p99(book_stale_ms)  × STALE_MULT,  STALE_FLOOR_MS,  STALE_CEIL_MS)
  - spread_threshold = clamp(p95(spread_bps)      × SPREAD_MULT, SPREAD_FLOOR_BPS, SPREAD_CEIL_BPS)
  - Наблюдения: book_stale_ms > 0 и spread_bps в [SPREAD_FLOOR_BPS, SPREAD_CEIL_BPS].
  - Все наблюдения безусловны (не только при emit) — репрезентативная выборка.

Режимы и авто-промоут:
  - shadow (enforce=False, auto_promote=True, default):
      Накапливает, возвращает static defaults. Когда n ≥ min_samples И
      прошло ≥ auto_promote_min_hours с первого наблюдения → per-symbol
      авто-промоут: начинает выдавать калиброванные пороги без ручного ENV.
  - pure shadow (enforce=False, auto_promote=False):
      Только накапливает, никогда не применяет — для аудита/исследования.
  - force-enforce (enforce=True):
      Применяет калиброванные пороги сразу по достижении min_samples,
      игнорирует auto_promote и time guard.

Критерии авто-промоута (оба обязательны):
  1. n ≥ min_samples (default 200) — достаточно данных для стабильного P².
  2. elapsed ≥ auto_promote_min_hours (default 0.5h = 30 min) — исключает
     кратковременные всплески трафика в начале запуска.
  Промоут sticky: после достижения не откатывается (даже при рестарте,
  если состояние персистировано в Redis).

Redis-ключ: AUTOCAL_DQ_MICRO (HASH)
  field: SYMBOL.upper() → JSON {schema_version, kind, symbol, n, first_obs_ms,
                                  promoted, stale_p99_ms, spread_p95_bps,
                                  enforce, updated_ms}
"""

import json
import math
import time
from dataclasses import dataclass
from typing import Any

from core.quantile_p2 import P2Quantile

# ── Hard rails ──────────────────────────────────────────────────────────────
STALE_FLOOR_MS: float = 150.0      # BTC может коротко лагать; ниже — noise
STALE_CEIL_MS: float = 10_000.0    # 10s — явная DQ-проблема при любом символе
SPREAD_FLOOR_BPS: float = 0.5      # ниже — артефакт
SPREAD_CEIL_BPS: float = 500.0     # выше — outage-уровень

STALE_MULT: float = 3.0    # p99 × 3 = "явно нехарактерно"
SPREAD_MULT: float = 2.0   # p95 × 2 = "wider than normal"

# Статические defaults (fallback когда cold / shadow-mode)
DEFAULT_STALE_MS: float = 1500.0
DEFAULT_SPREAD_BPS: float = 12.0


@dataclass
class DqMicroThresholds:
    """Калиброванные (или статические) DQ-пороги для одного символа."""
    stale_flag_ms: float
    spread_wide_bps: float
    n: int
    src: str        # "static" | "calib_p99p95" | "calib_p99p95_auto"
    promoted: bool  # True = auto-promoted (enforce включён по критериям)


class DqMicrostructureCalibrator:
    """
    Online per-symbol DQ calibrator для stale_l2 / wide_spread флагов.
    Самостоятельно переходит в enforce per-symbol после прогрева (auto_promote).

    Использование (hot path):
        cal.observe(symbol="BTCUSDT", book_stale_ms=18, spread_bps=1.2)
        th = cal.thresholds("BTCUSDT")
        if book_stale_ms > th.stale_flag_ms: flags.append("stale_l2")
        if spread_bps     > th.spread_wide_bps: flags.append("wide_spread")
    """

    def __init__(
        self,
        *,
        min_samples: int = 200,
        enforce: bool = False,
        auto_promote: bool = True,
        auto_promote_min_hours: float = 0.5,
        default_stale_ms: float = DEFAULT_STALE_MS,
        default_spread_bps: float = DEFAULT_SPREAD_BPS,
        stale_mult: float = STALE_MULT,
        spread_mult: float = SPREAD_MULT,
    ) -> None:
        self.min_samples = min_samples
        self.enforce = enforce
        self.auto_promote = auto_promote
        self.auto_promote_min_hours = max(0.0, auto_promote_min_hours)
        self.default_stale_ms = default_stale_ms
        self.default_spread_bps = default_spread_bps
        self.stale_mult = stale_mult
        self.spread_mult = spread_mult

        # P² estimators per symbol
        self._st99: dict[str, P2Quantile] = {}   # p99 book_stale_ms
        self._sp95: dict[str, P2Quantile] = {}   # p95 spread_bps
        self._n: dict[str, int] = {}              # observation count per symbol

        # Auto-promote tracking
        self._first_obs_ms: dict[str, int] = {}  # первое наблюдение per symbol
        self._promoted: set[str] = set()         # символы, уже промоутированные

        self._shadow: dict[str, DqMicroThresholds] = {}

    # ── Public API ──────────────────────────────────────────────────────────

    def observe(
        self,
        *,
        symbol: str,
        book_stale_ms: float,
        spread_bps: float,
        now_ms: int | None = None,
    ) -> None:
        """
        Одно наблюдение. Вызывать безусловно (не только при stale).

        book_stale_ms — возраст последнего обновления book в ms; игнорируется если ≤ 0.
        spread_bps    — текущий спред; игнорируется если вне [SPREAD_FLOOR_BPS, SPREAD_CEIL_BPS].
        now_ms        — текущее время epoch ms; если None — берётся из time.time().
        """
        sym = (symbol or "").upper()
        if not sym:
            return
        counted = False

        bsm = float(book_stale_ms) if math.isfinite(float(book_stale_ms)) else 0.0
        if bsm > 0:
            self._get_q(self._st99, sym, 0.99).update(bsm)
            counted = True

        spr = float(spread_bps) if math.isfinite(float(spread_bps)) else 0.0
        if SPREAD_FLOOR_BPS <= spr <= SPREAD_CEIL_BPS:
            self._get_q(self._sp95, sym, 0.95).update(spr)
            counted = True

        if counted:
            self._n[sym] = self._n.get(sym, 0) + 1
            # Запоминаем момент первого наблюдения для time guard
            if sym not in self._first_obs_ms:
                ts = int(now_ms if now_ms is not None else time.time() * 1000)
                self._first_obs_ms[sym] = ts

    def thresholds(
        self,
        symbol: str,
        now_ms: int | None = None,
    ) -> DqMicroThresholds:
        """
        Вернуть пороги для символа.

        Логика enforce/shadow/auto-promote:
          force-enforce (self.enforce=True):   warm → calib, cold → static
          auto_promote=True:                   warm + time_ok → calib (sticky)
          pure shadow (auto_promote=False):    всегда static
        """
        sym = (symbol or "").upper()
        n = self._n.get(sym, 0)

        shadow = self._compute(sym, n)
        self._shadow[sym] = shadow

        enforced = self._is_enforced(sym, n, now_ms)
        if enforced and n >= self.min_samples:
            return DqMicroThresholds(
                stale_flag_ms=shadow.stale_flag_ms,
                spread_wide_bps=shadow.spread_wide_bps,
                n=n,
                src="calib_p99p95_auto" if (sym in self._promoted) else "calib_p99p95",
                promoted=(sym in self._promoted),
            )

        return DqMicroThresholds(
            stale_flag_ms=self.default_stale_ms,
            spread_wide_bps=self.default_spread_bps,
            n=n,
            src="static",
            promoted=False,
        )

    def stale_threshold(self, symbol: str, now_ms: int | None = None) -> float:
        """Shortcut для hot path."""
        return self.thresholds(symbol, now_ms).stale_flag_ms

    def spread_threshold(self, symbol: str, now_ms: int | None = None) -> float:
        """Shortcut для hot path."""
        return self.thresholds(symbol, now_ms).spread_wide_bps

    def is_promoted(self, symbol: str) -> bool:
        """True если символ уже в enforce через auto-promote."""
        return (symbol or "").upper() in self._promoted

    def shadow_thresholds(self, symbol: str) -> DqMicroThresholds | None:
        """Последнее shadow-предложение (всегда, независимо от enforce)."""
        return self._shadow.get((symbol or "").upper())

    # ── Persistence ─────────────────────────────────────────────────────────

    def dump_symbol_state(self, *, symbol: str, updated_ts_ms: int) -> dict[str, Any]:
        sym = (symbol or "").upper()
        return {
            "schema_version": 2,
            "kind": "dq_micro",
            "symbol": sym,
            "updated_ms": updated_ts_ms,
            "min_samples": self.min_samples,
            "enforce": self.enforce,
            "auto_promote": self.auto_promote,
            "auto_promote_min_hours": self.auto_promote_min_hours,
            "n": self._n.get(sym, 0),
            "first_obs_ms": self._first_obs_ms.get(sym, 0),
            "promoted": sym in self._promoted,
            "st99": (self._st99[sym].to_state() if sym in self._st99 else None),
            "sp95": (self._sp95[sym].to_state() if sym in self._sp95 else None),
        }

    def load_symbol_state(self, state: Any) -> None:
        """Восстановить per-symbol состояние из dump_symbol_state(). Fail-open."""
        try:
            if not isinstance(state, dict):
                return
            sym = str(state.get("symbol") or "").upper()
            if not sym:
                return
            ver = state.get("schema_version", 1)
            if ver not in (1, 2):
                return
            self._n[sym] = int(state.get("n", 0) or 0)
            first_ms = int(state.get("first_obs_ms", 0) or 0)
            if first_ms > 0:
                self._first_obs_ms[sym] = first_ms
            # Restore promoted flag (sticky across restarts)
            if state.get("promoted"):
                self._promoted.add(sym)
            for attr, key in (("_st99", "st99"), ("_sp95", "sp95")):
                raw = state.get(key)
                if isinstance(raw, dict):
                    getattr(self, attr)[sym] = P2Quantile.from_state(raw)
        except Exception:
            return

    @staticmethod
    def loads(raw: str) -> dict[str, Any] | None:
        try:
            d = json.loads(raw)
            return d if isinstance(d, dict) else None
        except Exception:
            return None

    # ── Internals ────────────────────────────────────────────────────────────

    def _is_enforced(self, sym: str, n: int, now_ms: int | None) -> bool:
        """Проверить, следует ли применять калиброванные пороги для этого символа."""
        # force-enforce: не зависит от auto-promote
        if self.enforce:
            return True
        # pure shadow: авто-промоут отключён
        if not self.auto_promote:
            return False
        # Уже промоутирован ранее (sticky)
        if sym in self._promoted:
            return True
        # Проверяем оба критерия
        if n < self.min_samples:
            return False
        # Время с первого наблюдения
        first_ms = self._first_obs_ms.get(sym, 0)
        if first_ms <= 0:
            return False
        ts = int(now_ms if now_ms is not None else time.time() * 1000)
        elapsed_h = (ts - first_ms) / 3_600_000.0
        if elapsed_h < self.auto_promote_min_hours:
            return False
        # Критерии выполнены — промоутируем (sticky)
        self._promoted.add(sym)
        return True

    def _get_q(self, m: dict[str, P2Quantile], sym: str, p: float) -> P2Quantile:
        q = m.get(sym)
        if q is None:
            q = P2Quantile(p=p)
            m[sym] = q
        return q

    def _p2_val(self, m: dict[str, P2Quantile], sym: str, default: float) -> float:
        q = m.get(sym)
        v = q.value() if q is not None else None
        if v is None or not math.isfinite(v) or v <= 0:
            return default
        return v

    def _compute(self, sym: str, n: int) -> DqMicroThresholds:
        if n < self.min_samples:
            return DqMicroThresholds(
                stale_flag_ms=self.default_stale_ms,
                spread_wide_bps=self.default_spread_bps,
                n=n,
                src="static",
                promoted=False,
            )

        raw_stale = self._p2_val(self._st99, sym, self.default_stale_ms)
        raw_spread = self._p2_val(self._sp95, sym, self.default_spread_bps)

        stale = raw_stale * self.stale_mult
        spread = raw_spread * self.spread_mult

        stale = max(STALE_FLOOR_MS, min(STALE_CEIL_MS, stale))
        spread = max(SPREAD_FLOOR_BPS, min(SPREAD_CEIL_BPS, spread))

        return DqMicroThresholds(
            stale_flag_ms=stale,
            spread_wide_bps=spread,
            n=n,
            src="calib_p99p95",
            promoted=False,
        )
