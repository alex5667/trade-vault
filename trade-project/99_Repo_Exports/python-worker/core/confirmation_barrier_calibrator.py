from __future__ import annotations

"""
Confirmation Barrier Calibrator — адаптивный OBI threshold для L2ConfirmBreakout /
L2ConfirmAbsorption per (symbol × kind).

Проблема: hardcoded breakout_imbalance_min=1.15 / absorption_imbalance_min=1.20
игнорируют разброс OBI per symbol. Для SOL 1.15 может быть шумом, для BTC — сильным сигналом.

Метод (P0 автокалибраторов):
  - rolling ring buffer per (symbol × kind) с time-window (default 168h = 7 days);
  - observe(symbol, kind, obi, ts_ms) — вызывается когда L2Confirm.check() -> ok=True;
  - threshold_for(symbol, kind, now_ms, quantile=0.80) → q80(OBI | confirmed);
  - Интуиция: нижние 20% OBI-значений при confirmation отсекаются — они шумовые подтверждения;
  - Иерархический fallback: (sym, kind) → (*, kind) → hardcoded default per kind;
  - Гистерезис и jump-limit для стабильности;
  - Warmup guard: минимум min_samples значений для выхода из warmup;
  - snapshot() / load_state() — персистентность через Redis.

Defaults:
  BREAKOUT_DEFAULT  = 1.15   # L2ConfirmCfg.breakout_imbalance_min
  ABSORPTION_DEFAULT = 1.20  # L2ConfirmCfg.absorption_imbalance_min
  OBI floor = 1.01, ceil = 3.0 (sanity rails)
"""

import math
from collections import deque
from dataclasses import dataclass, field
from typing import Any


# ── defaults matching hardcoded L2ConfirmCfg values ─────────────────────────
BREAKOUT_DEFAULT: float = 1.15
ABSORPTION_DEFAULT: float = 1.20
_KIND_DEFAULTS: dict[str, float] = {
    "breakout": BREAKOUT_DEFAULT,
    "absorption": ABSORPTION_DEFAULT,
}
_FALLBACK_DEFAULT: float = 1.15  # for unknown kinds

OBI_FLOOR: float = 1.01   # нельзя калиброваться ниже (OBI <= 1 = нет дисбаланса)
OBI_CEIL: float = 3.0     # сanity upper bound
MIN_HYSTERESIS: float = 0.02   # не обновлять threshold если delta < этого
MAX_JUMP: float = 0.10         # max однократный сдвиг threshold


def _quantile(xs: list[float], q: float) -> float:
    """Linear-interpolated quantile на отсортированной копии."""
    if not xs:
        return 0.0
    a = sorted(xs)
    n = len(a)
    if n == 1:
        return a[0]
    q = min(1.0, max(0.0, q))
    idx = q * (n - 1)
    lo = math.floor(idx)
    hi = min(math.ceil(idx), n - 1)
    if lo == hi:
        return a[lo]
    w = idx - lo
    return a[lo] * (1.0 - w) + a[hi] * w


@dataclass
class _Sample:
    ts_ms: int
    obi: float


@dataclass
class _BinState:
    """Rolling buffer + committed threshold for one (symbol, kind) bin."""
    samples: deque[_Sample] = field(default_factory=deque)
    committed_tau: float | None = None   # None = warmup / не калиброван
    last_apply_ms: int = 0


class ConfirmationBarrierCalibrator:
    """Rolling OBI quantile calibrator для confirmation barriers.

    Thread-safety: NOT thread-safe — protect with asyncio lock if needed.
    Stateless relative to wall-clock; now_ms передаётся в threshold_for().
    """

    def __init__(
        self,
        *,
        window_ms: int = 168 * 3600 * 1000,  # 7 days
        min_samples: int = 30,               # warmup guard
        quantile: float = 0.80,
        hysteresis: float = MIN_HYSTERESIS,
        max_jump: float = MAX_JUMP,
        obi_floor: float = OBI_FLOOR,
        obi_ceil: float = OBI_CEIL,
    ) -> None:
        self.window_ms = window_ms
        self.min_samples = max(1, min_samples)
        self.quantile = quantile
        self.hysteresis = hysteresis
        self.max_jump = max_jump
        self.obi_floor = obi_floor
        self.obi_ceil = obi_ceil

        # key: (symbol, kind)
        self._bins: dict[tuple[str, str], _BinState] = {}

    # ── observation ───────────────────────────────────────────────────────────

    def observe(self, symbol: str, kind: str, obi: float, ts_ms: int) -> None:
        """Record OBI value at confirmation time.

        Call only when L2Confirm.check() returned ok=True.
        obi = actual imbalance ratio (e.g. bid_not/ask_not for breakout up).
        """
        if not math.isfinite(obi) or obi <= 0.0:
            return
        obi = max(self.obi_floor, min(self.obi_ceil, obi))
        key = (_norm_sym(symbol), _norm_kind(kind))
        if key not in self._bins:
            self._bins[key] = _BinState()
        b = self._bins[key]
        b.samples.append(_Sample(ts_ms=ts_ms, obi=obi))
        self._prune(b, now_ms=ts_ms)

    # ── query ─────────────────────────────────────────────────────────────────

    def threshold_for(
        self,
        symbol: str,
        kind: str,
        now_ms: int,
        *,
        enforce: bool = False,
    ) -> float:
        """Return calibrated threshold for (symbol, kind).

        When enforce=False (shadow mode): возвращает hardcoded default.
        Falls back to hardcoded default when bin is cold or in warmup.

        Иерархия:
          1. (symbol, kind) — если warmed up
          2. (*, kind) — cross-symbol aggregate, если warmed up
          3. _KIND_DEFAULTS[kind]
        """
        default = _KIND_DEFAULTS.get(kind, _FALLBACK_DEFAULT)
        if not enforce:
            return default

        # try (symbol, kind)
        sym = _norm_sym(symbol)
        knd = _norm_kind(kind)
        t = self._committed_for(sym, knd, now_ms)
        if t is not None:
            return t

        # try (*, kind) aggregate
        t = self._committed_for("*", knd, now_ms)
        if t is not None:
            return t

        return default

    def shadow_threshold_for(self, symbol: str, kind: str, now_ms: int) -> float | None:
        """Proposed threshold без применения — для логирования/метрик в shadow mode."""
        sym = _norm_sym(symbol)
        knd = _norm_kind(kind)
        self._maybe_update_committed(sym, knd, now_ms)
        self._maybe_update_committed("*", knd, now_ms)
        t = self._committed_for(sym, knd, now_ms)
        if t is not None:
            return t
        return self._committed_for("*", knd, now_ms)

    def sample_counts(self) -> dict[tuple[str, str], int]:
        """Return number of samples per (symbol, kind) bin — for metrics."""
        return {k: len(b.samples) for k, b in self._bins.items()}

    # ── snapshot / restore ────────────────────────────────────────────────────

    def snapshot(self) -> dict[str, Any]:
        out: dict[str, Any] = {
            "schema_version": 1,
            "window_ms": self.window_ms,
            "min_samples": self.min_samples,
            "quantile": self.quantile,
            "bins": {},
        }
        for (sym, knd), b in self._bins.items():
            bin_key = f"{sym}:{knd}"
            out["bins"][bin_key] = {
                "committed_tau": b.committed_tau,
                "last_apply_ms": b.last_apply_ms,
                "n": len(b.samples),
            }
        return out

    def load_state(self, state: dict[str, Any]) -> None:
        """Restore committed_tau values from snapshot. Clears sample buffers
        (они эфемерны; только committed_tau персистируется)."""
        if not isinstance(state, dict):
            return
        bins_data = state.get("bins") or {}
        for bin_key, bdata in bins_data.items():
            if ":" not in bin_key:
                continue
            sym, _, knd = bin_key.partition(":")
            key = (_norm_sym(sym), _norm_kind(knd))
            if key not in self._bins:
                self._bins[key] = _BinState()
            b = self._bins[key]
            raw_tau = bdata.get("committed_tau")
            if raw_tau is not None:
                try:
                    tau = float(raw_tau)
                    if math.isfinite(tau) and self.obi_floor <= tau <= self.obi_ceil:
                        b.committed_tau = tau
                except (TypeError, ValueError):
                    pass
            b.last_apply_ms = int(bdata.get("last_apply_ms", 0) or 0)

    # ── internals ─────────────────────────────────────────────────────────────

    def _prune(self, b: _BinState, now_ms: int) -> None:
        cutoff = now_ms - self.window_ms
        while b.samples and b.samples[0].ts_ms < cutoff:
            b.samples.popleft()

    def _maybe_update_committed(self, sym: str, knd: str, now_ms: int) -> None:
        """Recompute and possibly update committed_tau for a bin."""
        key = (sym, knd)
        if key not in self._bins:
            return
        b = self._bins[key]
        self._prune(b, now_ms)

        if len(b.samples) < self.min_samples:
            return  # warmup

        obis = [s.obi for s in b.samples]
        proposed = _quantile(obis, self.quantile)
        proposed = max(self.obi_floor, min(self.obi_ceil, proposed))

        if b.committed_tau is None:
            # первичный комит
            b.committed_tau = proposed
            b.last_apply_ms = now_ms
            return

        delta = proposed - b.committed_tau
        if abs(delta) < self.hysteresis:
            return  # гистерезис

        # cap jump
        capped = b.committed_tau + max(-self.max_jump, min(self.max_jump, delta))
        b.committed_tau = round(capped, 4)
        b.last_apply_ms = now_ms

    def _committed_for(self, sym: str, knd: str, now_ms: int) -> float | None:
        key = (sym, knd)
        if key not in self._bins:
            return None
        b = self._bins[key]
        self._maybe_update_committed(sym, knd, now_ms)
        return b.committed_tau

    def _rebuild_aggregate(self, knd: str, now_ms: int) -> None:
        """Rebuild '*' bin from all per-symbol bins with same kind."""
        agg_key = ("*", knd)
        all_samples: list[_Sample] = []
        for (sym, k), b in self._bins.items():
            if k == knd and sym != "*":
                self._prune(b, now_ms)
                all_samples.extend(b.samples)

        if not all_samples:
            return
        all_samples.sort(key=lambda s: s.ts_ms)

        if agg_key not in self._bins:
            self._bins[agg_key] = _BinState()
        agg = self._bins[agg_key]
        agg.samples = deque(all_samples)
        self._prune(agg, now_ms)


def _norm_sym(s: str) -> str:
    return (s or "").strip().upper() or "*"


def _norm_kind(k: str) -> str:
    k = (k or "").strip().lower()
    return k if k else "*"
