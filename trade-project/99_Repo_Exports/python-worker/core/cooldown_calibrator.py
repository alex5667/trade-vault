from __future__ import annotations

"""cooldown_calibrator.py

Адаптивный калибратор `SYMBOL_ENTRY_COOLDOWN_MS` для orchestrator.py.

Проблема
--------
`SYMBOL_ENTRY_COOLDOWN_MS=0` (отключён) — не используется, хотя на нестабильных
символах частые сигналы за короткий интервал систематически убыточны (overtrading).

Метод
-----
Отслеживаем per-symbol время между успешными эмитами сигналов (emit-to-emit interval).
q80 этого интервала → минимальный cooldown, позволяющий пропускать top-20% пауз.

Логика: если типичный интервал между сигналами q80 = 120 сек → cooldown=120 сек.
Это гарантирует, что не блокируем больше 20% сигналов в нормальных условиях.

Roadmap: "post-fill correlation → q80 inter-signal-loss interval". Это Phase 1:
простой q80 интервала. Phase 2 (когда накопится trades_closed): условный q80
только по убыточным сериям — более точный таргет для cooldown.

Инварианты
----------
- auto_enforce=True: per-symbol автопереключение после min_signals наблюдений.
- Hard rails: cooldown_ms ∈ [1_000ms, 300_000ms (5 min)].
- Warmup: min_signals=100 per symbol.
- Гистерезис: 2_000ms (2 сек).
"""

import math
from dataclasses import dataclass
from typing import Any

from core.quantile_p2 import P2Quantile

# ── hard rails ──────────────────────────────────────────────────────────────
COOLDOWN_FLOOR_MS: float = 1_000.0      # < 1 сек: нет смысла в cooldown
COOLDOWN_CEIL_MS: float = 300_000.0     # > 5 мин: слишком агрессивно

DEFAULT_COOLDOWN_MS: float = 0.0        # отключён по умолчанию
UPDATE_BAND_MS: float = 2_000.0         # гистерезис 2 сек


@dataclass
class CooldownThresholds:
    """
    cooldown_ms  — q80 inter-signal interval в ms для данного символа
    n            — число межсигнальных интервалов
    src          — "static" (0 ms, disabled) / "calib_q80"
    """
    cooldown_ms: float
    n: int
    src: str


class CooldownCalibrator:
    """
    Онлайн-калибратор cooldown_ms для SignalOrchestrator.

    Вызывающий код:
    1. При каждом emit сигнала: `observe(symbol, emit_ts_ms)`.
    2. При проверке cooldown: `thresholds(symbol).cooldown_ms`.

    auto_enforce=True: автопереключение как только накопилось min_signals.
    """

    def __init__(
        self,
        *,
        min_signals: int = 100,
        enforce: bool = False,
        auto_enforce: bool = True,
        update_band_ms: float = UPDATE_BAND_MS,
    ) -> None:
        self.min_signals = min_signals
        self.enforce = enforce
        self.auto_enforce = auto_enforce
        self.update_band_ms = update_band_ms

        # P² q80 per-symbol на inter-signal intervals (ms)
        self._q80: dict[str, P2Quantile] = {}
        # Последний emit timestamp per symbol
        self._last_emit_ms: dict[str, float] = {}
        # Количество интервалов (не сигналов) per symbol
        self._n: dict[str, int] = {}
        self._committed: dict[str, float] = {}
        self._shadow: dict[str, CooldownThresholds] = {}

    # ── публичный API ────────────────────────────────────────────────────────

    def observe(self, *, symbol: str, emit_ts_ms: float) -> None:
        """
        Зарегистрировать emit сигнала. Первый emit — только запоминаем ts.
        Каждый последующий — вычисляем и накапливаем интервал.
        """
        sym = _norm(symbol)
        if not math.isfinite(emit_ts_ms) or emit_ts_ms <= 0:
            return

        if sym in self._last_emit_ms:
            interval_ms = emit_ts_ms - self._last_emit_ms[sym]
            if COOLDOWN_FLOOR_MS <= interval_ms <= COOLDOWN_CEIL_MS:
                self._get_q80(sym).update(interval_ms)
                self._n[sym] = self._n.get(sym, 0) + 1

        self._last_emit_ms[sym] = emit_ts_ms

    def thresholds(
        self,
        *,
        symbol: str,
        default_cooldown_ms: float = DEFAULT_COOLDOWN_MS,
    ) -> CooldownThresholds:
        """
        Вернуть cooldown_ms для данного символа.

        0.0 (default) → cooldown отключён.
        Калиброванное значение возвращается только после прогрева.
        """
        sym = _norm(symbol)
        n = self._n.get(sym, 0)

        shadow = self._compute(sym, n, default_cooldown_ms)
        self._shadow[sym] = shadow

        warm = n >= self.min_signals
        effective_enforce = self.enforce or (self.auto_enforce and warm)
        if not effective_enforce:
            return CooldownThresholds(cooldown_ms=default_cooldown_ms, n=n, src="static")

        prev = self._committed.get(sym, default_cooldown_ms)
        new_val = shadow.cooldown_ms

        if abs(new_val - prev) >= self.update_band_ms:
            self._committed[sym] = new_val
        else:
            new_val = prev

        return CooldownThresholds(cooldown_ms=new_val, n=n, src="calib_q80")

    def shadow_thresholds(self, *, symbol: str) -> CooldownThresholds | None:
        return self._shadow.get(_norm(symbol))

    def n(self, symbol: str) -> int:
        return self._n.get(_norm(symbol), 0)

    # ── персистентность ──────────────────────────────────────────────────────

    def dump_symbol_state(self, *, symbol: str, updated_ts_ms: int) -> dict[str, Any]:
        sym = _norm(symbol)
        return {
            "v": 1, "kind": "cooldown", "symbol": sym,
            "updated_ts_ms": updated_ts_ms, "min_signals": self.min_signals,
            "enforce": self.enforce, "auto_enforce": self.auto_enforce,
            "n": self._n.get(sym, 0),
            "last_emit_ms": self._last_emit_ms.get(sym),
            "committed": self._committed.get(sym),
            "q80": (self._q80[sym].to_state() if sym in self._q80 else None),
        }

    def load_symbol_state(self, state: Any) -> None:
        try:
            if not isinstance(state, dict) or state.get("kind") != "cooldown":
                return
            sym = str(state.get("symbol") or "na").lower()
            self.min_signals = int(state.get("min_signals", self.min_signals) or self.min_signals)
            self._n[sym] = int(state.get("n", 0) or 0)
            if state.get("last_emit_ms") is not None:
                self._last_emit_ms[sym] = float(state["last_emit_ms"])
            if state.get("committed") is not None:
                self._committed[sym] = float(state["committed"])
            if q80_raw := state.get("q80"):
                self._q80[sym] = P2Quantile.from_state(q80_raw)
        except Exception:
            pass

    # ── вспомогательные ──────────────────────────────────────────────────────

    def _get_q80(self, sym: str) -> P2Quantile:
        if sym not in self._q80:
            self._q80[sym] = P2Quantile(p=0.80)
        return self._q80[sym]

    def _compute(self, sym: str, n: int, default: float) -> CooldownThresholds:
        if n < self.min_signals:
            return CooldownThresholds(cooldown_ms=default, n=n, src="static")
        raw = self._q80[sym].value() if sym in self._q80 else None
        val = _clamp(raw, default)
        return CooldownThresholds(cooldown_ms=val, n=n, src="calib_q80")


def _norm(s: str | None) -> str:
    return (s or "na").strip().lower()

def _clamp(val: float | None, default: float) -> float:
    if val is None or not math.isfinite(val):
        return default
    return max(COOLDOWN_FLOOR_MS, min(COOLDOWN_CEIL_MS, val))
