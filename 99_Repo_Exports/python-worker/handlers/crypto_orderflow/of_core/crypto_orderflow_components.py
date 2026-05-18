from __future__ import annotations

import json
import math
import time

# Wall-clock bounds for normalize_ts_ms validation
_NORM_7D_MS: int = 7 * 24 * 3_600_000
_NORM_1M_MS: int = 60_000
import contextlib
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from contexts import MarketRegime  # type: ignore
from handlers.tick_parser import Tick  # type: ignore

from ..types.crypto_orderflow_handler_types import RegimeFeatures, RegimeSample
from core.htf_proximity_calibrator import HtfProximityCalibrator  # type: ignore[import]


def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        v = float(x)
    except Exception:
        return default
    if not math.isfinite(v):
        return default
    return float(v)


def _clamp(x: float, lo: float, hi: float) -> float:
    return float(min(hi, max(lo, x)))


def _safe_pos_float(x: Any) -> float | None:
    try:
        v = float(x)
    except Exception:
        return None
    if not math.isfinite(v) or v <= 0.0:
        return None
    return float(v)


# =========================
    try:
        v = float(x)
    except Exception:
        return default
    if not math.isfinite(v):
        return default
    return float(v)


# =========================
# 1) TickParser (Парсер тиков)
# =========================

@dataclass
class TickParserStats:
    total: int = 0
    bad: int = 0
    last_bad_reason: str = ""

    @property
    def bad_rate(self) -> float:
        if self.total <= 0:
            return 0.0
        return float(self.bad) / float(self.total)


def _to_str(x: Any) -> str:
    if x is None:
        return ""
    if isinstance(x, (bytes, bytearray)):
        return x.decode("utf-8", errors="ignore")
    return str(x)


def _parse_bool(v: Any) -> bool | None:
    if v is None:
        return None
    if isinstance(v, bool):
        return v
    s = _to_str(v).strip().lower()
    if s in {"true", "1", "yes", "y"}:
        return True
    if s in {"false", "0", "no", "n"}:
        return False
    return None


def normalize_ts_ms(ts: int, now_ms: int = 0) -> int:
    """
    Нормализация ts → epoch_ms с проверкой порядка величины + wall-clock bounds.

    Порядок нормализации (epoch_ms текущей эры ≈ 1.7e12):
      - ts <  1e10  → секунды  → *1000
      - ts <  1e13  → миллисекунды → без изменений
      - ts >= 1e13  → микросекунды (от сторонних прокси) → //1000

    После нормализации результат проверяется против [now-7d, now+1min].
    Любая аномалия → 0. Caller обязан трактовать 0 как bad tick.

    Args:
        ts:     raw timestamp (int, любой масштаб)
        now_ms: текущий epoch_ms (0 = auto через time.time())
    """
    if ts <= 0:
        return 0

    # --- нормализация по порядку величины ---
    # Текущий epoch_ms ≈ 1.7e12: любое значение >= 1e13 не является
    # действительным ms (это ~год 2286+), трактуем как µs.
    if ts >= 10_000_000_000_000:   # >= 1e13 → µs
        ts_ms = int(ts // 1000)
    elif ts < 10_000_000_000:      # < 1e10  → seconds
        ts_ms = int(ts * 1000)
    else:                          # 1e10 .. 1e13 → ms
        ts_ms = int(ts)

    # --- wall-clock bounds validation ---
    _now = now_ms if now_ms > 0 else int(time.time() * 1000)
    if ts_ms <= 0 or ts_ms < _now - _NORM_7D_MS or ts_ms > _now + _NORM_1M_MS:
        return 0

    return ts_ms


class TickParser:
    """
    Нормализация:
      - ts (сек/мс/мкс) -> мс
      - bid/ask/last/volume/flags
      - is_buyer_maker (плоские алиасы или вложенный json)
    Метрики:
      - bad tick rate (доля некорректных тиков)
    """

    def __init__(self) -> None:
        self.stats = TickParserStats()

    def parse(self, fields: dict[str, Any]) -> Tick | None:
        self.stats.total += 1
        if not fields:
            self.stats.bad += 1
            self.stats.last_bad_reason = "empty_fields"
            return None

        tick_json: dict[str, Any] | None = None

        try:
            if "data" in fields:
                raw_s = _to_str(fields.get("data"))
                tick_json = json.loads(raw_s) if raw_s else {}
                ts = int(float(tick_json.get("ts", 0) or 0))  # type: ignore
                bid = float(tick_json.get("bid", 0) or 0)  # type: ignore
                ask = float(tick_json.get("ask", 0) or 0)  # type: ignore
                last = float(tick_json.get("last", 0) or 0)  # type: ignore
                volume = float(tick_json.get("volume", 0) or 0)  # type: ignore
                flags = int(float(tick_json.get("flags", 0) or 0))  # type: ignore
            else:
                ts = int(float(fields.get("ts", 0) or 0))
                bid = float(fields.get("bid", 0) or 0)
                ask = float(fields.get("ask", 0) or 0)
                last = float(fields.get("last", 0) or 0)
                volume = float(fields.get("volume", 0) or 0)
                flags = int(float(fields.get("flags", 0) or 0))
        except Exception:
            self.stats.bad += 1
            self.stats.last_bad_reason = "parse_error" # ошибка парсинга
            return None

        ts = normalize_ts_ms(ts)
        if ts <= 0 or bid <= 0 or ask <= 0 or ask < bid:
            self.stats.bad += 1
            self.stats.last_bad_reason = "invalid_prices_or_ts"
            return None

        ibm = _parse_bool(fields.get("is_buyer_maker"))
        if ibm is None:
            ibm = _parse_bool(fields.get("m"))  # алиас
        if ibm is None and tick_json and isinstance(tick_json, dict):
            for k in ("is_buyer_maker", "isBuyerMaker", "buyerMaker", "m"):
                if k in tick_json:
                    ibm = _parse_bool(tick_json.get(k))
                    break
            # вложенный json в строке (редко)
            if ibm is None and isinstance(tick_json.get("data"), str):
                try:
                    nested = json.loads(tick_json["data"])
                    for k in ("is_buyer_maker", "isBuyerMaker", "buyerMaker", "m"):
                        if k in nested:
                            ibm = _parse_bool(nested.get(k))
                            break
                except Exception:
                    pass

        return Tick(ts=ts, bid=bid, ask=ask, last=last, volume=volume, flags=flags, is_buyer_maker=ibm)


# =========================
# 2) MicrostructureEngine (Движок микроструктуры)
# =========================

@dataclass(frozen=True)
class MicroSnapshot:
    spread_bps: float = 0.0
    realized_bps: float = 0.0
    realized_ema_bps: float = 0.0
    adverse_ratio_ema: float = 0.0
    market_mode: str = "mixed"


@dataclass
class _PendingMid:
    ts: int
    mid_at_trade: float
    side: int  # +1 taker-buy, -1 taker-sell


class RealizedSpreadTracker:
    """
    Легковесный трекер реализованного спреда (post-factum):
      сохраняет mid в момент сделки -> через горизонт вычисляет realized_bps.
    """
    __slots__ = (
        "horizon_ms",
        "alpha",
        "max_pending",
        "pending",
        "_head",
        "last_realized_bps",
        "realized_ema_bps",
        "adverse_ratio_ema",
        "dropped_pending",
        "settled",
    )

    def __init__(self, horizon_ms: int = 2000, alpha: float = 0.08, max_pending: int = 5000):
        self.horizon_ms = max(50, int(horizon_ms))
        self.alpha = max(0.01, min(0.5, float(alpha)))
        self.max_pending = max(100, int(max_pending))
        self.pending: list[_PendingMid] = []
        self._head = 0
        self.last_realized_bps = 0.0
        self.realized_ema_bps = 0.0
        self.adverse_ratio_ema = 0.0
        self.dropped_pending = 0
        self.settled = 0

    def update(self, *, ts: int, bid: float, ask: float, is_trade: bool, side: int) -> tuple[float, float, float, float]:
        if ts <= 0 or bid <= 0 or ask <= 0 or ask < bid:
            return 0.0, self.last_realized_bps, self.realized_ema_bps, self.adverse_ratio_ema

        mid_now = 0.5 * (bid + ask)
        if mid_now <= 0:
            return 0.0, self.last_realized_bps, self.realized_ema_bps, self.adverse_ratio_ema

        spread_bps = (ask - bid) / mid_now * 10_000.0

        cutoff = ts - self.horizon_ms
        head = self._head
        pend = self.pending

        while head < len(pend):
            p = pend[head]
            if p.ts > cutoff:
                break
            if p.mid_at_trade > 0:
                realized = p.side * (mid_now - p.mid_at_trade) / p.mid_at_trade * 10_000.0
                self.last_realized_bps = float(realized)
                a = self.alpha
                self.realized_ema_bps = (1.0 - a) * self.realized_ema_bps + a * self.last_realized_bps
                adverse = 1.0 if realized < 0.0 else 0.0
                self.adverse_ratio_ema = (1.0 - a) * self.adverse_ratio_ema + a * adverse
            self.settled += 1
            head += 1

        self._head = head

        if is_trade and side in (-1, 1):
            if (len(self.pending) - self._head) >= self.max_pending:
                self._head += 1
                self.dropped_pending += 1
            self.pending.append(_PendingMid(ts=ts, mid_at_trade=mid_now, side=side))

        if self._head > 2000 and self._head > (len(self.pending) // 2):
            self.pending = self.pending[self._head :]
            self._head = 0

        return float(spread_bps), self.last_realized_bps, self.realized_ema_bps, self.adverse_ratio_ema


def _is_trade_tick(tick: Tick) -> bool:
    return bool(tick.flags & 1) or bool(tick.last and tick.volume and tick.volume > 0)


class MicrostructureEngine:
    def __init__(
        self,
        *,
        rs: RealizedSpreadTracker,
        momo_thr_bps: float,
        meanrev_thr_bps: float,
        momo_adverse_max: float,
        meanrev_adverse_min: float,
        taker_side_fn: Callable[[Tick], int],
        mode_ema_alpha: float = 0.02,
    ) -> None:
        self.rs = rs
        self.momo_thr_bps = float(momo_thr_bps)
        self.meanrev_thr_bps = float(meanrev_thr_bps)
        self.momo_adverse_max = float(momo_adverse_max)
        self.meanrev_adverse_min = float(meanrev_adverse_min)
        self._taker_side = taker_side_fn
        self._mode_ema_alpha = float(mode_ema_alpha)
        self._mode_momo_ema = 0.0
        self._mode_counts = {"momentum": 0, "range": 0, "mixed": 0}
        self.last = MicroSnapshot()

    def _market_mode(self, realized_ema_bps: float, adverse_ratio_ema: float) -> str:
        if realized_ema_bps >= self.momo_thr_bps and adverse_ratio_ema <= self.momo_adverse_max:
            return "momentum"
        if realized_ema_bps <= self.meanrev_thr_bps and adverse_ratio_ema >= self.meanrev_adverse_min:
            return "range"
        return "mixed"

    def on_tick(self, tick: Tick) -> MicroSnapshot:
        if not tick.bid or not tick.ask:
            return self.last

        is_trade = _is_trade_tick(tick)
        side = self._taker_side(tick)

        spread_bps, last_realized_bps, realized_ema_bps, adverse_ratio_ema = self.rs.update(
            ts=int(tick.ts),
            bid=float(tick.bid),
            ask=float(tick.ask),
            is_trade=is_trade,
            side=side,
        )

        mode = self._market_mode(float(realized_ema_bps), float(adverse_ratio_ema))

        if mode in self._mode_counts:
            self._mode_counts[mode] += 1
        x = 1.0 if mode == "momentum" else 0.0
        a = float(self._mode_ema_alpha)
        self._mode_momo_ema = (1.0 - a) * self._mode_momo_ema + a * x

        self.last = MicroSnapshot(
            spread_bps=float(spread_bps),
            realized_bps=float(last_realized_bps),
            realized_ema_bps=float(realized_ema_bps),
            adverse_ratio_ema=float(adverse_ratio_ema),
            market_mode=str(mode),
        )
        return self.last

    def attach_to_ctx(self, ctx: Any) -> None:
        s = self.last
        ctx.spread_bps = float(s.spread_bps)
        ctx.realized_bps = float(s.realized_bps)
        ctx.realized_ema_bps = float(s.realized_ema_bps)
        ctx.adverse_ratio_ema = float(s.adverse_ratio_ema)
        ctx.market_mode = str(s.market_mode)




# =========================
# 3) RegimeDetector (Детектор режима рынка)
# =========================

@dataclass
class RegimeDetectorCfg:
    # пороги
    regime_trend_threshold: float = 0.35
    regime_range_threshold: float = -0.35
    regime_window_size: int = 240

    # веса (сохраняем семантику хендлера)
    atr_weight: float = 0.0
    delta_weight: float = 0.0
    vwap_dev_weight: float = 0.0
    daily_open_dev_weight: float = 0.0
    daily_open_cross_weight: float = 0.0
    htf_level_weight: float = 0.0
    weak_progress_weight: float = 0.0
    session_weight: float = 0.0

    # HTF proximity: адаптивные мультипликаторы (DEFAULT = data_models.py:43-44)
    htf_near_mult: float = 0.20
    htf_far_mult: float = 0.80
    htf_near_bps_fallback: float = 10.0
    htf_far_bps_fallback: float = 40.0


class RegimeDetector:
    """
    Детектор режима рынка:
      - может использовать существующую логику хендлера (feature_extractor/history_updater),
        чтобы НЕ менять семантику;
      - если extractor не передан — откат на микро market_mode.
    """

    def __init__(
        self,
        cfg: RegimeDetectorCfg,
        *,
        daily_open_cross_freq_provider: Callable[[str], float | None] | None = None,
        htf_levels_provider: Callable[[str], Any] | None = None,
        now_provider: Callable[[], float] | None = None,
    ) -> None:
        self.cfg = cfg
        self._daily_open_cross_freq_provider = daily_open_cross_freq_provider
        self._htf_levels_provider = htf_levels_provider
        self._now = now_provider or time.time
        self._history: dict[str, deque[RegimeSample]] = {}
        self._htf_prox_calib = HtfProximityCalibrator(auto_enforce=True)  # type: ignore[misc]

    def _hist(self, symbol: str) -> deque[RegimeSample]:
        h = self._history.get(symbol)
        if h is None:
            h = deque(maxlen=int(self.cfg.regime_window_size))
            self._history[symbol] = h
        return h

    def _acc(self, score: float, wsum: float, val: float | None, w: float) -> tuple[float, float]:
        if val is None:
            return score, wsum
        v = _safe_float(val, default=0.0)
        if not math.isfinite(v) or w <= 0.0:
            return score, wsum
        return score + w * v, wsum + w

    def _weighted_score(self, feats: RegimeFeatures) -> tuple[float, dict[str, Any]]:
        cfg = self.cfg
        score = 0.0
        wsum = 0.0
        score, wsum = self._acc(score, wsum, feats.atr_bias, cfg.atr_weight)
        score, wsum = self._acc(score, wsum, feats.delta_dir_bias, cfg.delta_weight)
        score, wsum = self._acc(score, wsum, feats.vwap_dev_bias, cfg.vwap_dev_weight)
        score, wsum = self._acc(score, wsum, feats.daily_open_dev_bias, cfg.daily_open_dev_weight)
        score, wsum = self._acc(score, wsum, feats.daily_open_cross_bias, cfg.daily_open_cross_weight)
        score, wsum = self._acc(score, wsum, feats.htf_prox_bias, cfg.htf_level_weight)
        score, wsum = self._acc(score, wsum, feats.weak_progress_bias, cfg.weak_progress_weight)
        score, wsum = self._acc(score, wsum, feats.session_bias, cfg.session_weight)

        if wsum <= 0.0:
            return 0.0, {"wsum": 0.0, "reason": "no_weights_or_missing_features"}
        return float(score / wsum), {
            "wsum": float(wsum),
            # raw
            "vwap_dev_bps": feats.vwap_dev_bps,
            "daily_open_dev_bps": feats.daily_open_dev_bps,
            "daily_open_cross_freq": feats.daily_open_cross_freq,
            "htf_level_dist_bps": feats.htf_level_dist_bps,
            "atr_bias": feats.atr_bias,
            "delta_dir_bias": feats.delta_dir_bias,
            "vwap_dev_bias": feats.vwap_dev_bias,
            "daily_open_dev_bias": feats.daily_open_dev_bias,
            "daily_open_cross_bias": feats.daily_open_cross_bias,
            "htf_prox_bias": feats.htf_prox_bias,
            "weak_progress_bias": feats.weak_progress_bias,
            "session_bias": feats.session_bias,
        }

    def update_history(self, ctx: Any) -> None:
        """Перенос _update_regime_history из хендлера в детектор."""
        symbol = getattr(ctx, "symbol", None)
        price = getattr(ctx, "last_price", None) or getattr(ctx, "price", None)
        vwap = getattr(ctx, "vwap", None)
        daily_open = getattr(ctx, "daily_open", None)

        if symbol is None:
            return
        p = _safe_pos_float(price)
        if p is None:
            return

        now = getattr(ctx, "ts_utc", None)
        ts = float(now) if (now is not None and math.isfinite(float(now))) else float(self._now())

        # сторона VWAP
        vwap_side = 0
        vv = _safe_pos_float(vwap)
        if vv is not None:
            diff_v = p - vv
            if diff_v > 0.0:
                vwap_side = 1
            elif diff_v < 0.0:
                vwap_side = -1

        # сторона daily_open
        daily_open_side = 0
        oo = _safe_pos_float(daily_open)
        if oo is not None:
            diff_o = p - oo
            if diff_o > 0.0:
                daily_open_side = 1
            elif diff_o < 0.0:
                daily_open_side = -1

        self._hist(symbol).append(
            RegimeSample(
                ts=ts,
                price=p,
                vwap_side=vwap_side,
                daily_open_side=daily_open_side,
                vol_total=0.0,  # заполнитель
                notional=0.0,  # заполнитель
                bar_index=None,
            )
        )

    def _fallback_daily_open_cross_freq(self, symbol: str) -> float | None:
        """
        Фолбэк, если нет внешнего провайдера:
          частота пересечений daily_open ~= доля смен знака daily_open_side на окне.
        """
        hist = self._history.get(symbol)
        if not hist or len(hist) < 3:
            return None
        recent = list(hist)[-50:]
        sides = [s.daily_open_side for s in recent if s.daily_open_side != 0]
        if len(sides) < 3:
            return None
        crosses = 0
        prev = sides[0]
        for cur in sides[1:]:
            if cur != prev:
                crosses += 1
            prev = cur
        denom = max(1, len(sides) - 1)
        return float(crosses / denom)

    def compute_features(self, ctx: Any) -> RegimeFeatures:
        """Перенос _compute_regime_features из хендлера в детектор."""
        symbol = getattr(ctx, "symbol", None)
        price = getattr(ctx, "last_price", None) or getattr(ctx, "price", None)
        vwap = getattr(ctx, "vwap", None)
        daily_open = getattr(ctx, "daily_open", None)
        atr_14_bps = getattr(ctx, "atr_14_bps", None)
        weak_progress_raw = getattr(ctx, "weak_progress_raw", None)

        if symbol is None:
            return RegimeFeatures()
        p = _safe_pos_float(price)
        if p is None:
            return RegimeFeatures()

        sym = symbol

        # 1) Расстояние до VWAP в bps
        vwap_dev_bps: float | None = None
        vv = _safe_pos_float(vwap)
        if vv is not None:
            vwap_dev_bps = float(abs(p - vv) / p * 10_000.0)

        # 2) Расстояние до daily_open в bps
        daily_open_dev_bps: float | None = None
        oo = _safe_pos_float(daily_open)
        if oo is not None:
            daily_open_dev_bps = float(abs(p - oo) / oo * 10_000.0)

        # 3) Частота пересечений daily_open
        daily_open_cross_freq: float | None = None
        if self._daily_open_cross_freq_provider is not None:
            try:
                daily_open_cross_freq = self._daily_open_cross_freq_provider(sym)
            except Exception:
                daily_open_cross_freq = None
        if daily_open_cross_freq is None:
            daily_open_cross_freq = self._fallback_daily_open_cross_freq(sym)
        if daily_open_cross_freq is not None:
            daily_open_cross_freq = _clamp(_safe_float(daily_open_cross_freq, 0.0), 0.0, 1.0)

        # 4) Расстояние до HTF уровней
        htf_level_dist_bps: float | None = None
        if self._htf_levels_provider is not None:
            try:
                htf_levels = self._htf_levels_provider(sym)
            except Exception:
                htf_levels = None
            if htf_levels is not None:
                levels = []
                for k in ("pdh", "pdl", "pdm"):
                    if hasattr(htf_levels, k):
                        lv = _safe_pos_float(getattr(htf_levels, k))
                        if lv is not None:
                            levels.append(lv)
                if levels:
                    htf_level_dist_bps = float(min(abs(p - lv) / p * 10_000.0 for lv in levels))

        # 5) bias'ы на основе сырых метрик

        # ATR bias: высокая волатильность -> тренд (+1), низкая -> рендж (-1)
        atr_bias: float | None = None
        ab = _safe_float(atr_14_bps, default=float("nan"))
        if math.isfinite(ab):
            atr_bias = _clamp((ab - 50.0) / 50.0, -1.0, 1.0)

        # Delta direction bias - получаем из истории
        delta_dir_bias: float | None = None
        hist = self._history.get(sym)
        if hist and len(hist) >= 3:
            recent = list(hist)[-10:]
            sides = [s.vwap_side for s in recent if s.vwap_side != 0]
            if sides:
                pos_count = sum(1 for s in sides if s > 0)
                neg_count = sum(1 for s in sides if s < 0)
                total = pos_count + neg_count
                if total > 0:
                    delta_dir_bias = _clamp((pos_count - neg_count) / total, -1.0, 1.0)

        # VWAP deviation bias: близко -> рендж (-1), далеко -> тренд (+1)
        vwap_dev_bias: float | None = None
        if vwap_dev_bps is not None and math.isfinite(vwap_dev_bps):
            vwap_dev_bias = _clamp((vwap_dev_bps - 25.0) / 75.0, -1.0, 1.0)

        # Daily open deviation bias
        daily_open_dev_bias: float | None = None
        if daily_open_dev_bps is not None and math.isfinite(daily_open_dev_bps):
            daily_open_dev_bias = _clamp((daily_open_dev_bps - 25.0) / 75.0, -1.0, 1.0)

        # Daily open cross bias: частые пересечения -> рендж (-1), редкие -> тренд (+1)
        daily_open_cross_bias: float | None = None
        if daily_open_cross_freq is not None and math.isfinite(daily_open_cross_freq):
            daily_open_cross_bias = _clamp(1.0 - 2.0 * daily_open_cross_freq, -1.0, 1.0)

        # HTF proximity bias: близко к уровням -> +1, далеко -> -1
        # Адаптивная формула через HtfProximityCalibrator (q20/q80 per-symbol).
        htf_prox_bias: float | None = None
        if htf_level_dist_bps is not None and math.isfinite(htf_level_dist_bps):
            atr_val = _safe_float(atr_14_bps, default=0.0)
            if atr_val > 0.0:
                self._htf_prox_calib.observe(
                    symbol=sym, dist_bps=htf_level_dist_bps, daily_atr_bps=atr_val)
                th = self._htf_prox_calib.thresholds(symbol=sym)
                near_bps = th.near_mult * atr_val
                far_bps = th.far_mult * atr_val
            else:
                near_bps = self.cfg.htf_near_bps_fallback
                far_bps = self.cfg.htf_far_bps_fallback
            span = far_bps - near_bps
            if span > 0.0:
                htf_prox_bias = _clamp(
                    1.0 - 2.0 * (htf_level_dist_bps - near_bps) / span, -1.0, 1.0)
            else:
                htf_prox_bias = _clamp(1.0 - (htf_level_dist_bps / 50.0), -1.0, 1.0)

        # Weak progress bias: слабый прогресс -> рендж (-1), сильный -> тренд (+1)
        weak_progress_bias: float | None = None
        wp = _safe_float(weak_progress_raw, default=float("nan"))
        if math.isfinite(wp):
            weak_progress_bias = _clamp((wp - 0.5) * 2.0, -1.0, 1.0)

        session_bias: float | None = None

        return RegimeFeatures(
            # raw
            vwap_dev_bps=vwap_dev_bps,
            daily_open_dev_bps=daily_open_dev_bps,
            daily_open_cross_freq=daily_open_cross_freq,
            htf_level_dist_bps=htf_level_dist_bps,
            # bias
            atr_bias=atr_bias,
            delta_dir_bias=delta_dir_bias,
            vwap_dev_bias=vwap_dev_bias,
            daily_open_dev_bias=daily_open_dev_bias,
            daily_open_cross_bias=daily_open_cross_bias,
            htf_prox_bias=htf_prox_bias,
            weak_progress_bias=weak_progress_bias,
            session_bias=session_bias,
        )

    def detect(self, ctx: Any) -> MarketRegime:
        # 1) update history (fail-open)
        with contextlib.suppress(Exception):
            self.update_history(ctx)

        # 2) compute features (fail-open)
        breakdown: dict[str, Any] = {}
        try:
            feats = self.compute_features(ctx)
        except Exception:
            feats = RegimeFeatures()
            breakdown["features_error"] = True

        # 3) score
        score, b = self._weighted_score(feats)
        breakdown.update(b)

        # 4) если веса или фичи отсутствуют -> откат к скорингу по микро-режиму для сохранения поведения
        if float(breakdown.get("wsum") or 0.0) <= 0.0:
            mm = str(getattr(ctx, "market_mode", "mixed") or "mixed").lower()
            if mm.startswith("momentum"):
                score = +0.6
            elif mm.startswith("mean"):
                score = -0.6
            else:
                score = 0.0
            breakdown["micro_mode"] = mm
            breakdown["fallback_micro_mode"] = True

        ctx.market_regime_score = float(score)
        if score >= float(self.cfg.regime_trend_threshold):
            regime = MarketRegime.TREND
        elif score <= float(self.cfg.regime_range_threshold):
            regime = MarketRegime.RANGE
        else:
            regime = MarketRegime.MIXED
        ctx.market_regime = regime
        ctx.regime_features = breakdown
        return regime


# =========================
# 4) Confirmations (Подтверждения)
# =========================

@dataclass(frozen=True)
class ConfirmationResult:
    ok: bool
    code: str = "ok"
    details: dict[str, Any] | None = None

    @property
    def veto(self) -> bool:
        return not self.ok

    @property
    def reason_code(self) -> str:
        return self.code


class L2ConfirmBreakout:
    def __init__(
        self,
        *,
        require_obi20: bool,
        mp_min_bps: float,
        wall_max_dist_bps: float,
        dep_min: float,
        ref_max: float,
        impact_max: float,
        use_l3: bool,
        l3_ctr_max: float,
        l3_rate_min: float,
        l3_eta_max: float,
    ) -> None:
        self.require_obi20 = bool(require_obi20)
        self.mp_min_bps = float(mp_min_bps)
        self.wall_max_dist_bps = float(wall_max_dist_bps)
        self.dep_min = float(dep_min)
        self.ref_max = float(ref_max)
        self.impact_max = float(impact_max)
        self.use_l3 = bool(use_l3)
        self.l3_ctr_max = float(l3_ctr_max)
        self.l3_rate_min = float(l3_rate_min)
        self.l3_eta_max = float(l3_eta_max)

    def check(self, ctx: Any, *, dir_up: bool) -> ConfirmationResult:
        if self.require_obi20:
            if not bool(getattr(ctx, "obi_sustained_20", False)):
                return ConfirmationResult(False, "obi20_not_sustained")
            if float(getattr(ctx, "obi_avg_20", 0.0) or 0.0) * (1.0 if dir_up else -1.0) <= 0:
                return ConfirmationResult(False, "obi20_wrong_sign")

        mp = float(getattr(ctx, "microprice_shift_bps_20", 0.0) or 0.0)
        if dir_up and mp < self.mp_min_bps:
            return ConfirmationResult(False, "microprice_shift_too_small")
        if (not dir_up) and mp > -self.mp_min_bps:
            return ConfirmationResult(False, "microprice_shift_too_small")

        if dir_up and bool(getattr(ctx, "wall_ask", False)) and float(getattr(ctx, "wall_ask_dist_bps", 0.0) or 0.0) <= self.wall_max_dist_bps:
            return ConfirmationResult(False, "wall_near_ask")
        if (not dir_up) and bool(getattr(ctx, "wall_bid", False)) and float(getattr(ctx, "wall_bid_dist_bps", 0.0) or 0.0) <= self.wall_max_dist_bps:
            return ConfirmationResult(False, "wall_near_bid")

        if float(getattr(ctx, "depletion_score", 0.0) or 0.0) < self.dep_min:
            return ConfirmationResult(False, "depletion_low")
        if float(getattr(ctx, "refill_score", 0.0) or 0.0) > self.ref_max:
            return ConfirmationResult(False, "refill_too_high")

        if float(getattr(ctx, "impact_proxy", 0.0) or 0.0) > self.impact_max:
            return ConfirmationResult(False, "impact_too_high")

        if self.use_l3:
            if dir_up:
                ctr = float(getattr(ctx, "cancel_to_trade_ask", 0.0) or 0.0)
                rate = float(getattr(ctx, "taker_buy_rate_ema", 0.0) or 0.0)
                eta = float(getattr(ctx, "eta_fill_ask_sec", 0.0) or 0.0)
            else:
                ctr = float(getattr(ctx, "cancel_to_trade_bid", 0.0) or 0.0)
                rate = float(getattr(ctx, "taker_sell_rate_ema", 0.0) or 0.0)
                eta = float(getattr(ctx, "eta_fill_bid_sec", 0.0) or 0.0)

            if self.l3_ctr_max > 0 and ctr >= self.l3_ctr_max and (self.l3_rate_min <= 0 or rate < self.l3_rate_min):
                return ConfirmationResult(False, "l3_pulled_liquidity")
            if self.l3_eta_max > 0 and eta > self.l3_eta_max and (self.l3_rate_min <= 0 or rate < self.l3_rate_min):
                return ConfirmationResult(False, "l3_eta_too_high")

        return ConfirmationResult(True, "ok")


class L2ConfirmAbsorption:
    def __init__(
        self,
        *,
        require_fresh_l2: bool,
        refill_min: float,
        wall_max_dist_bps: float,
        use_micro_proxy: bool,
        micro_adverse_min: float,
        micro_realized_ema_max: float,
        use_l3: bool,
        l3_rate_min: float,
    ) -> None:
        self.require_fresh_l2 = bool(require_fresh_l2)
        self.refill_min = float(refill_min)
        self.wall_max_dist_bps = float(wall_max_dist_bps)
        self.use_micro_proxy = bool(use_micro_proxy)
        self.micro_adverse_min = float(micro_adverse_min)
        self.micro_realized_ema_max = float(micro_realized_ema_max)
        self.use_l3 = bool(use_l3)
        self.l3_rate_min = float(l3_rate_min)

    def check(self, ctx: Any, *, dir_up: bool) -> ConfirmationResult:
        if self.require_fresh_l2 and bool(getattr(ctx, "l2_is_stale", True)):
            return ConfirmationResult(False, "l2_stale")

        mp = float(getattr(ctx, "microprice_shift_bps_20", 0.0) or 0.0)
        mp_contra = (mp < 0.0) if dir_up else (mp > 0.0)

        wall_here = False
        if dir_up:
            wall_here = bool(getattr(ctx, "wall_ask", False)) and float(getattr(ctx, "wall_ask_dist_bps", 0.0) or 0.0) <= self.wall_max_dist_bps
        else:
            wall_here = bool(getattr(ctx, "wall_bid", False)) and float(getattr(ctx, "wall_bid_dist_bps", 0.0) or 0.0) <= self.wall_max_dist_bps

        refill = float(getattr(ctx, "refill_score", 0.0) or 0.0) >= self.refill_min
        weak = bool(getattr(ctx, "weak_progress", False)) or bool(getattr(ctx, "weakProgress", False))

        micro_proxy = False
        if self.use_micro_proxy:
            rema = float(getattr(ctx, "realized_ema_bps", 0.0) or 0.0)
            adv = float(getattr(ctx, "adverse_ratio_ema", 0.0) or 0.0)
            if (rema <= self.micro_realized_ema_max) and (adv >= self.micro_adverse_min):
                micro_proxy = True

        ok = bool(weak or refill or wall_here or mp_contra or micro_proxy)
        if not ok:
            return ConfirmationResult(False, "no_absorption_evidence")

        if self.use_l3 and self.l3_rate_min > 0:
            if dir_up:
                rate = float(getattr(ctx, "taker_buy_rate_ema", 0.0) or 0.0)
            else:
                rate = float(getattr(ctx, "taker_sell_rate_ema", 0.0) or 0.0)
            if rate < self.l3_rate_min:
                return ConfirmationResult(False, "l3_taker_rate_too_low")

        return ConfirmationResult(True, "ok")


class TouchFilter:
    def __init__(self, enabled: bool, kinds: set[str]) -> None:
        self.enabled = bool(enabled)
        self.kinds = set(kinds or set())

    def check(self, ctx: Any, *, signal_kind: str, side: str) -> ConfirmationResult:
        if not self.enabled or signal_kind not in self.kinds:
            return ConfirmationResult(True, "ok")
        if bool(getattr(ctx, "touch_is_stale", True)):
            return ConfirmationResult(True, "touch_stale_skip_filter")

        ask_tag = str(getattr(ctx, "touch_ask_tag", "none"))
        bid_tag = str(getattr(ctx, "touch_bid_tag", "none"))

        if side == "LONG" and ask_tag == "refill":
            return ConfirmationResult(False, "touch_block_ask_refill")
        if side == "SHORT" and bid_tag == "refill":
            return ConfirmationResult(False, "touch_block_bid_refill")
        return ConfirmationResult(True, "ok")


# =========================
# 5) Scoring (Скоринг)
# =========================

@dataclass(frozen=True)
class ScoreResult:
    raw_score: float
    conf_factor: float
    final_score: float
    confidence_pct: float
    parts: dict[str, Any]


def _clamp01(x: float) -> float:
    if not math.isfinite(x):
        return 0.0
    return max(0.0, min(1.0, x))


class ScoreModel:
    """
    Один вход -> один выход:
      conf_factor ∈ [0..1]
      final_score = raw_score * conf_factor
      confidence_pct — отдельная калиброванная метрика для UI/фильтров (0..100)
    """

    def __init__(
        self,
        *,
        conf_scorer: Callable[[Any, str], tuple[float, dict[str, Any]]],
        kind_normalizer: Callable[[Any], str],
        confidence_pct_k: float = 100.0,
    ) -> None:
        self._conf_scorer = conf_scorer
        self._norm_kind = kind_normalizer
        self._k = float(confidence_pct_k) if math.isfinite(float(confidence_pct_k)) and float(confidence_pct_k) > 0 else 100.0

    def _confidence_pct_from_final(self, final_score: float) -> float:
        v = abs(float(final_score)) * (self._k / 1.0)
        return float(max(0.0, min(100.0, v)))

    def score(self, ctx: Any, *, raw_score: float, signal_kind: Any) -> ScoreResult:
        kind = self._norm_kind(signal_kind)
        conf, parts = self._conf_scorer(ctx, kind)
        # обратная совместимость: scorer может вернуть pct
        cf = float(conf)
        if cf > 1.0:
            cf = cf / 100.0
        cf = _clamp01(cf)

        fs = float(raw_score) * cf
        cpct = self._confidence_pct_from_final(fs)

        # прикрепляем к контексту для дальнейшего использования
        ctx.raw_score = float(raw_score)
        ctx.conf_factor = float(cf)
        ctx.final_score = float(fs)
        ctx.confidence_pct = float(cpct)
        ctx.confidence_parts = parts or {}

        return ScoreResult(
            raw_score=float(raw_score),
            conf_factor=float(cf),
            final_score=float(fs),
            confidence_pct=float(cpct),
            parts=parts or {},
        )


# =========================
# 6) Emitter (Эмиттер сигналов)
# =========================

class Emitter:
    def __init__(
        self,
        *,
        manual_signal_enabled: bool,
        manual_signal_stream: str,
        audit_level: str = "full",
        audit_max_bytes: int = 12_000,
    ) -> None:
        self.manual_signal_enabled = bool(manual_signal_enabled)
        self.manual_signal_stream = (manual_signal_stream or "")
        self.audit_level = (audit_level or "full").strip().lower()
        self.audit_max_bytes = int(audit_max_bytes or 0)

    def _estimate_json_bytes(self, obj: Any) -> int:
        try:
            s = json.dumps(obj, ensure_ascii=False, separators=(",", ":"))
            return len(s.encode("utf-8"))
        except Exception:
            return 0

    def _shrink_manual_payload(self, mp: dict[str, Any]) -> dict[str, Any]:
        mp = dict(mp)
        mp.pop("metadata", None)
        if self.audit_level == "compact":
            mp.pop("indicators", None)
        return mp

    def extend_outbox_envelope(
        self,
        envelope: dict[str, Any],
        *,
        signal: Any,
        ctx: Any,
        build_audit_full: Callable[[Any], dict[str, Any]],
        build_audit_compact: Callable[[Any], dict[str, Any]],
    ) -> None:
        if not self.manual_signal_enabled or not self.manual_signal_stream:
            return

        manual_payload = {
            "sid": getattr(signal, "sid", ""),
            "ts": getattr(signal, "ts", envelope.get("ts", 0)),
            "symbol": getattr(signal, "symbol", ""),
            "side": getattr(signal, "side", ""),
            "entry": getattr(signal, "entry", None),
            "sl": getattr(signal, "sl", None),
            "tp_levels": getattr(signal, "tp_levels", None),
            "lot": getattr(signal, "lot", None),
            "reason": getattr(signal, "reason", ""),
            "source": "crypto-orderflow",
            "confidence": getattr(signal, "confidence", getattr(ctx, "confidence_pct", 0.0)),
            "atr": getattr(signal, "atr", getattr(ctx, "atr", None)),
            "trail_after_tp1": getattr(signal, "trail_after_tp1", False),
            "trail_profile": getattr(signal, "trail_profile", ""),
            "indicators": getattr(signal, "indicators", {}) or {},
            "metadata": getattr(signal, "metadata", None) or {},
            "audit_context": build_audit_full(ctx) if self.audit_level != "compact" else build_audit_compact(ctx),
        }

        if self.audit_max_bytes > 0:
            sz = self._estimate_json_bytes(manual_payload)
            if sz > self.audit_max_bytes:
                manual_payload = self._shrink_manual_payload(manual_payload)
                sz2 = self._estimate_json_bytes(manual_payload)
                if sz2 > self.audit_max_bytes:
                    manual_payload["audit_context"] = build_audit_compact(ctx)
                    manual_payload.pop("indicators", None)

        envelope.setdefault("meta", {})
        envelope.setdefault("targets", {})
        envelope["meta"]["manual_stream"] = self.manual_signal_stream
        envelope["targets"]["manual_payload"] = manual_payload
