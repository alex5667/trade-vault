from __future__ import annotations

"""
LiquidityRegimeService
----------------------
Единый "risk overlay" по ликвидности/книге.

Цели:
1) Свести spread/depth/book_rate в один liq_score (0..1) и режим:
   - normal / thin / stressed
2) В stressed автоматически эскалировать строгость StrongGate (need -> 3).
3) Делать это детерминированно (только от входных данных книги + ts_ms).

Ключевая идея:
Вы просили "генерировать пороги из текущих дефолтов". Мы берём:
 - dist_bp_threshold   -> базовая допустимая дистанция/спред (в bp)
 - book_rate_min_hz/warn_hz -> ожидаемая частота обновления книги
 - dn_tier1_usd        -> грубый прокси "масштаб ликвидности" (для depth floors)

При этом runtime.config уже использует эти дефолты (и ваши overrides), поэтому
мы читаем их из cfg и только при отсутствии делаем fallback на instrument_config.
"""

from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple


def _clamp01(x: float) -> float:
    if x < 0.0:
        return 0.0
    if x > 1.0:
        return 1.0
    return x


def _ramp(x: float, lo: float, hi: float) -> float:
    """0 при x<=lo, 1 при x>=hi, линейно между."""
    if hi <= lo:
        return 0.0
    return _clamp01((x - lo) / (hi - lo))


def _sym_class(symbol: str) -> str:
    s = (symbol or "").upper()
    if s in ("BTCUSDT", "ETHUSDT"):
        return "majors"
    if s in ("SOLUSDT", "BNBUSDT"):
        return "large"
    # эвристика "мемов": либо явные, либо 1000*
    if s.startswith("1000") or any(x in s for x in ("PEPE", "SHIB", "FLOKI", "BONK")):
        return "memes"
    return "mid"


@dataclass(frozen=True)
class LiquidityThresholds:
    # spread in bp
    spread_warn_bp: float
    spread_crit_bp: float
    # depth in USD (min(bid5, ask5) * mid)
    depth_warn_usd: float
    depth_crit_usd: float
    # book rate in Hz
    rate_min_hz: float
    rate_warn_hz: float
    rate_crit_hz: float
    # regime boundaries for liq_score
    thin_score: float
    stressed_score: float


@dataclass(frozen=True)
class LiquidityRegimeEvent:
    ts_ms: int
    score: float
    regime: str  # normal|thin|stressed
    spread_bps: float
    depth_min_5_usd: float
    book_rate_hz: float


class LiquidityRegimeService:
    def __init__(self, *, symbol: str, cfg: Dict[str, Any]) -> None:
        self.symbol = str(symbol)
        self.cfg = cfg
        self._thr = self._build_thresholds(symbol=self.symbol, cfg=cfg)
        self._last: Optional[LiquidityRegimeEvent] = None

    def thresholds(self) -> LiquidityThresholds:
        return self._thr

    def last(self) -> Optional[LiquidityRegimeEvent]:
        return self._last

    def update(
        self,
        *,
        ts_ms: int,
        spread_bps: float,
        depth_min_5_usd: float,
        book_rate_hz: float,
    ) -> LiquidityRegimeEvent:
        """
        Возвращает LiquidityRegimeEvent. Внутри не модифицирует входы и не "лечит" время.
        Если ts_ms немонотонный — просто перезаписываем last (determinism: ваш pipeline уже
        умеет quarantine bad time; здесь fail-open).
        """
        thr = self._thr
        sp = float(spread_bps or 0.0)
        dep = float(depth_min_5_usd or 0.0)
        hz = float(book_rate_hz or 0.0)

        # 1) spread_score: чем больше спред, тем хуже
        # score=1 при sp<=warn, 0 при sp>=crit
        spread_score = 1.0 - _ramp(sp, thr.spread_warn_bp, thr.spread_crit_bp)

        # 2) depth_score: чем глубже, тем лучше
        # score=0 при dep<=crit, 1 при dep>=warn
        depth_score = _ramp(dep, thr.depth_crit_usd, thr.depth_warn_usd)

        # 3) rate_score: score=0 при hz<=crit, 1 при hz>=min
        rate_score = _ramp(hz, thr.rate_crit_hz, thr.rate_min_hz)

        # Веса: depth важнее (проскальзывание/исполнение), потом rate, потом spread.
        score = (
            0.42 * depth_score
            + 0.35 * rate_score
            + 0.23 * spread_score
        )
        score = _clamp01(score)

        if score < thr.stressed_score:
            regime = "stressed"
        elif score < thr.thin_score:
            regime = "thin"
        else:
            regime = "normal"

        ev = LiquidityRegimeEvent(
            ts_ms=int(ts_ms),
            score=float(score),
            regime=str(regime),
            spread_bps=float(sp),
            depth_min_5_usd=float(dep),
            book_rate_hz=float(hz),
        )
        self._last = ev
        return ev

    # ---------------- internals ----------------
    def _build_thresholds(self, *, symbol: str, cfg: Dict[str, Any]) -> LiquidityThresholds:
        # Pull defaults from cfg; fallback to instrument_config if missing.
        dist_bp = float(cfg.get("dist_bp_threshold", 0.0) or 0.0)
        br_min = float(cfg.get("book_rate_min_hz", 0.0) or 0.0)
        br_warn = float(cfg.get("book_rate_warn_hz", 0.0) or 0.0)
        dn_t1 = float(cfg.get("dn_tier1_usd", 0.0) or 0.0)

        if dist_bp <= 0 or br_min <= 0 or br_warn <= 0 or dn_t1 <= 0:
            try:
                from core.instrument_config import (
                    get_default_book_rate_settings,
                    get_default_dist_bp_threshold,
                    get_default_delta_tiers,
                )
                if dist_bp <= 0:
                    dist_bp = float(get_default_dist_bp_threshold(symbol) or 0.0)
                if br_min <= 0 or br_warn <= 0:
                    s = get_default_book_rate_settings(symbol) or {}
                    br_min = float(s.get("book_rate_min_hz", br_min) or br_min)
                    br_warn = float(s.get("book_rate_warn_hz", br_warn) or br_warn)
                if dn_t1 <= 0:
                    t = get_default_delta_tiers(symbol) or {}
                    dn_t1 = float(t.get("tier1", dn_t1) or dn_t1)
            except Exception:
                pass

        # spread: warn/crit derived from dist threshold (мультипликаторы можно оверрайдить)
        sp_warn_mult = float(cfg.get("liq_spread_warn_mult", 1.5) or 1.5)
        sp_crit_mult = float(cfg.get("liq_spread_crit_mult", 3.0) or 3.0)
        spread_warn = max(1.0, dist_bp * sp_warn_mult)
        spread_crit = max(spread_warn + 1.0, dist_bp * sp_crit_mult)

        # rate: crit ниже warn (чтобы отличать "просели" от "умерли")
        rate_min = max(0.5, br_min)
        rate_warn = max(0.25, br_warn)
        rate_crit = max(0.10, float(cfg.get("liq_book_rate_crit_hz", 0.0) or (0.5 * rate_warn)))

        # depth: базовая привязка к dn_tier1_usd + floors по классу
        cls = _sym_class(symbol)
        if cls == "majors":
            floor_warn, floor_crit = 200_000.0, 80_000.0
        elif cls == "large":
            floor_warn, floor_crit = 80_000.0, 30_000.0
        elif cls == "memes":
            floor_warn, floor_crit = 25_000.0, 8_000.0
        else:
            floor_warn, floor_crit = 45_000.0, 15_000.0

        dep_warn_mult = float(cfg.get("liq_depth_warn_mult", 0.35) or 0.35)
        dep_crit_mult = float(cfg.get("liq_depth_crit_mult", 0.18) or 0.18)

        depth_warn = max(floor_warn, dn_t1 * dep_warn_mult)
        depth_crit = max(floor_crit, dn_t1 * dep_crit_mult)
        if depth_warn <= depth_crit:
            depth_warn = depth_crit * 1.8

        thin_score = float(cfg.get("liq_thin_score", 0.60) or 0.60)
        stressed_score = float(cfg.get("liq_stressed_score", 0.35) or 0.35)
        # sanity
        thin_score = _clamp01(thin_score)
        stressed_score = _clamp01(stressed_score)
        if thin_score <= stressed_score:
            thin_score = min(0.80, stressed_score + 0.20)

        return LiquidityThresholds(
            spread_warn_bp=float(spread_warn),
            spread_crit_bp=float(spread_crit),
            depth_warn_usd=float(depth_warn),
            depth_crit_usd=float(depth_crit),
            rate_min_hz=float(rate_min),
            rate_warn_hz=float(rate_warn),
            rate_crit_hz=float(rate_crit),
            thin_score=float(thin_score),
            stressed_score=float(stressed_score),
        )
