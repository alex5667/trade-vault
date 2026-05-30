"""
Adaptive TP1 Policy v1 (Plan 3, 2026-05-29)

Wybiera TP1 distance, который максимизирует EV_R (expected R-multiple),
а не просто увеличивает win-rate.

Принцип:

    EV_R(TP1_R) = P_hit(TP1_R) * TP1_R - (1 - P_hit(TP1_R)) * 1R - cost_R

Цель — argmax EV_R(TP1_R) на сетке кандидатов.

Безопасность по дизайну:
  - master switch TP1_ADAPTIVE_ENABLED=0 (off) по умолчанию
  - TP1_ADAPTIVE_MODE=shadow|paper|enforce (default shadow)
  - в shadow режиме .apply=False, baseline TP1 не подменяется
  - probability curve должен быть откалиброванным (не raw confidence)
  - min_samples, calibration_ok, min_ev_delta — все защиты включены
  - tp1_dist никогда не превышает MAX_RR и не уходит ниже MIN_RR / min_tp1_bps
  - safety floor: TP1 (рассчитанный из ATR/MFE/MAE EmpiricalLevels) не обходится —
    мы решаем только верхний слой, и compute_levels продолжает применять
    EDGE_LEVELS_MIN_TP1_BPS и tp1_min_rr

Ничего не пишет в Redis/PG. Только возвращает решение.
"""

from __future__ import annotations

from dataclasses import dataclass, field
import math
import os
from typing import Any


# ---------------------------------------------------------------------------
# Defaults (можно переопределять через ENV; см. config/crypto-of-common.env)
# ---------------------------------------------------------------------------

_DEFAULT_GRID = "0.65,0.80,1.00,1.15,1.30,1.50"
_DEFAULT_MIN_RR = 0.80
_DEFAULT_MAX_RR = 1.50
_DEFAULT_MIN_TP1_BPS = 8.0
_DEFAULT_MIN_SAMPLES = 200
_DEFAULT_MIN_EV_DELTA_R = 0.05
_DEFAULT_FEE_BPS = 4.0
_DEFAULT_COST_BUFFER_BPS = 4.0


# ---------------------------------------------------------------------------
# Result dataclass
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class AdaptiveTP1Decision:
    """
    Результат AdaptiveTP1Policy.

    Контракт:
      - .enabled = True если master switch включен;
      - .apply = True ТОЛЬКО если кандидат выбран И режим paper/enforce;
      - в shadow .apply=False даже при найденном кандидате (только метрики);
      - .reason — стабильный reason-code (см. tp1_adaptive_* в плане).
    """
    enabled: bool
    apply: bool
    mode: str
    reason: str
    tp1_dist: float | None
    tp1_rr: float | None
    p_hit: float | None
    p_hit_baseline: float | None
    ev_baseline_r: float
    ev_adaptive_r: float
    ev_delta_r: float
    cost_r: float
    samples: int = 0
    baseline_rr: float | None = None
    grid_evaluated: tuple[float, ...] = field(default_factory=tuple)

    @property
    def is_shadow(self) -> bool:
        return self.mode == "shadow"

    @property
    def is_enforce(self) -> bool:
        return self.mode == "enforce"


# ---------------------------------------------------------------------------
# ENV helpers
# ---------------------------------------------------------------------------


def _env_on(name: str, default: str = "0") -> bool:
    return (os.getenv(name, default) or "").strip().lower() in {"1", "true", "yes", "on"}


def _env_float(name: str, default: float) -> float:
    try:
        v = os.getenv(name, "")
        if v is None or str(v).strip() == "":
            return default
        return float(v)
    except Exception:
        return default


def _env_int(name: str, default: int) -> int:
    try:
        v = os.getenv(name, "")
        if v is None or str(v).strip() == "":
            return default
        return int(float(v))
    except Exception:
        return default


def _env_str(name: str, default: str) -> str:
    v = os.getenv(name, "")
    if v is None or str(v).strip() == "":
        return default
    return str(v).strip().lower()


def _parse_grid(raw: str) -> list[float]:
    out: list[float] = []
    for x in (raw or "").split(","):
        try:
            v = float(x.strip())
            if math.isfinite(v) and v > 0:
                out.append(v)
        except Exception:
            pass
    return sorted(set(out))


# ---------------------------------------------------------------------------
# EV formula (full-exit; partial-exit может усложнить позже)
# ---------------------------------------------------------------------------


def ev_full_exit_r(*, p_hit: float, tp_rr: float, cost_r: float) -> float:
    """
    Pure function, чистый EV в R-multiple при полном выходе на TP.

    EV_R = p_hit * tp_rr - (1 - p_hit) * 1R - cost_R

    cost_R уже выражен в R (т.е. costs_bps / stop_bps).
    """
    p = max(0.0, min(1.0, float(p_hit)))
    return p * float(tp_rr) - (1.0 - p) * 1.0 - float(cost_r)


# ---------------------------------------------------------------------------
# Probability curve extraction
# ---------------------------------------------------------------------------


def _extract_prob_curve(ctx: Any) -> dict[str, float] | None:
    """
    Извлекает калиброванную кривую P_hit(TP1_R) из ctx.

    Поддерживаемые формы:
      - ctx.tp1_hit_prob_by_rr = {"0.65": 0.90, "1.00": 0.80, ...}

    Возвращает None если кривая не предоставлена или невалидна.

    IMPORTANT: ctx.confidence / ctx.p_edge / ctx.tp1_hit_prob_by_rr НЕ являются
    взаимозаменяемыми. Нам нужна именно ОТКАЛИБРОВАННАЯ оценка P(hit TP1 | rr)
    из replay/empirical, а не сырая модельная вероятность.
    """
    curve = getattr(ctx, "tp1_hit_prob_by_rr", None)
    if not isinstance(curve, dict) or not curve:
        return None
    out: dict[str, float] = {}
    for k, v in curve.items():
        try:
            rr = float(k)
            p = float(v)
            if math.isfinite(rr) and rr > 0 and 0.0 <= p <= 1.0:
                out[f"{rr:.2f}"] = p
        except Exception:
            continue
    return out or None


def _interp_prob(curve: dict[str, float], rr: float) -> float | None:
    """
    Lookup p_hit(rr) с exact match приоритетом и линейной интерполяцией
    между соседними точками сетки. Возвращает None если rr выходит за края.
    """
    key = f"{rr:.2f}"
    if key in curve:
        return float(curve[key])
    try:
        keys = sorted(float(k) for k in curve.keys())
    except Exception:
        return None
    if not keys or rr < keys[0] or rr > keys[-1]:
        return None
    lo = max(k for k in keys if k <= rr)
    hi = min(k for k in keys if k >= rr)
    if hi <= lo:
        return float(curve[f"{lo:.2f}"])
    p_lo = float(curve[f"{lo:.2f}"])
    p_hi = float(curve[f"{hi:.2f}"])
    w = (rr - lo) / (hi - lo)
    return p_lo + w * (p_hi - p_lo)


# ---------------------------------------------------------------------------
# Main API
# ---------------------------------------------------------------------------


def choose_adaptive_tp1(
    *,
    ctx: Any,
    entry: float,
    stop_dist: float,
    baseline_tp1_dist: float,
    symbol: str = "",
    kind: str = "",
    regime: str = "",
) -> AdaptiveTP1Decision:
    """
    Главная точка входа. Возвращает AdaptiveTP1Decision.

    Никогда не бросает исключений; на любом плохом входе → skip.

    Параметры:
      ctx: SignalContext (нужен .tp1_hit_prob_by_rr, .tp1_prob_samples,
                          .tp1_calibration_ok, .spread_bps, .slippage_ema_bps)
      entry, stop_dist, baseline_tp1_dist: уже посчитанные baseline дистанции
                                            (в единицах цены, positive)
      symbol/kind/regime: только для логирования/метрик; не влияют на решение
    """
    mode = _env_str("TP1_ADAPTIVE_MODE", "shadow")
    if mode not in {"off", "shadow", "paper", "enforce"}:
        mode = "shadow"

    # master kill switch
    if not _env_on("TP1_ADAPTIVE_ENABLED", "0") or mode == "off":
        return AdaptiveTP1Decision(
            enabled=False, apply=False, mode=mode,
            reason="tp1_adaptive_skip_disabled",
            tp1_dist=None, tp1_rr=None, p_hit=None, p_hit_baseline=None,
            ev_baseline_r=0.0, ev_adaptive_r=0.0, ev_delta_r=0.0,
            cost_r=0.0, samples=0, baseline_rr=None, grid_evaluated=(),
        )

    # input sanity
    try:
        entry_f = float(entry)
        stop_dist_f = float(stop_dist)
        baseline_tp1_dist_f = float(baseline_tp1_dist)
    except Exception:
        return _skip(mode, "tp1_adaptive_skip_bad_levels")
    if entry_f <= 0.0 or stop_dist_f <= 0.0 or baseline_tp1_dist_f <= 0.0:
        return _skip(mode, "tp1_adaptive_skip_bad_levels")

    curve = _extract_prob_curve(ctx)
    if curve is None:
        return _skip(mode, "tp1_adaptive_skip_no_prob_curve")

    samples = int(getattr(ctx, "tp1_prob_samples", 0) or 0)
    min_samples = _env_int("TP1_ADAPTIVE_MIN_SAMPLES", _DEFAULT_MIN_SAMPLES)
    if samples < min_samples:
        return _skip(mode, "tp1_adaptive_skip_low_samples", samples=samples)

    require_cal = _env_on("TP1_ADAPTIVE_REQUIRE_CALIBRATION_OK", "1")
    calibration_ok = bool(int(getattr(ctx, "tp1_calibration_ok", 0) or 0) == 1)
    if require_cal and not calibration_ok:
        return _skip(mode, "tp1_adaptive_skip_uncalibrated", samples=samples)

    # ranges
    min_rr = _env_float("TP1_ADAPTIVE_MIN_RR", _DEFAULT_MIN_RR)
    max_rr = _env_float("TP1_ADAPTIVE_MAX_RR", _DEFAULT_MAX_RR)
    if not (math.isfinite(min_rr) and math.isfinite(max_rr)) or min_rr <= 0 or max_rr < min_rr:
        return _skip(mode, "tp1_adaptive_skip_bad_levels", samples=samples)
    min_tp1_bps = _env_float("TP1_ADAPTIVE_MIN_TP1_BPS", _DEFAULT_MIN_TP1_BPS)
    min_ev_delta = _env_float("TP1_ADAPTIVE_MIN_EV_DELTA_R", _DEFAULT_MIN_EV_DELTA_R)

    # costs
    fee_bps = _env_float("TAKER_FEE_BPS", _DEFAULT_FEE_BPS)
    buffer_bps = _env_float("TP1_ADAPTIVE_COST_BUFFER_BPS", _DEFAULT_COST_BUFFER_BPS)
    include_slip = _env_on("TP1_ADAPTIVE_INCLUDE_SLIPPAGE", "1")
    spread_bps = float(getattr(ctx, "spread_bps", 0.0) or 0.0)
    slip_bps = float(getattr(ctx, "slippage_ema_bps", 0.0) or 0.0) if include_slip else 0.0

    stop_bps = stop_dist_f / entry_f * 10_000.0
    if not math.isfinite(stop_bps) or stop_bps <= 0:
        return _skip(mode, "tp1_adaptive_skip_bad_levels", samples=samples)
    cost_bps = fee_bps + spread_bps / 2.0 + slip_bps + buffer_bps
    cost_r = cost_bps / max(stop_bps, 1e-9)

    baseline_rr = baseline_tp1_dist_f / stop_dist_f
    p_baseline = _interp_prob(curve, baseline_rr)
    if p_baseline is None:
        # baseline TP1 за пределами grid; используем fallback k=tp1_hit_prob если есть
        fallback = getattr(ctx, "tp1_hit_prob", None)
        if fallback is None:
            return _skip(mode, "tp1_adaptive_skip_no_prob_curve", samples=samples)
        try:
            p_baseline = float(fallback)
        except Exception:
            return _skip(mode, "tp1_adaptive_skip_no_prob_curve", samples=samples)
        if not (0.0 <= p_baseline <= 1.0):
            return _skip(mode, "tp1_adaptive_skip_no_prob_curve", samples=samples)
    ev_base = ev_full_exit_r(p_hit=p_baseline, tp_rr=baseline_rr, cost_r=cost_r)

    grid_raw = os.getenv("TP1_ADAPTIVE_RR_GRID", _DEFAULT_GRID)
    grid = _parse_grid(grid_raw) or _parse_grid(_DEFAULT_GRID)
    grid_eval: list[float] = []

    best_rr = baseline_rr
    best_p = p_baseline
    best_ev = ev_base
    chose_clamped = ""

    for rr_raw in grid:
        # clamp в [min_rr, max_rr] (track clamp для reason)
        rr = max(min_rr, min(max_rr, rr_raw))
        tp1_bps = stop_bps * rr
        if tp1_bps < min_tp1_bps:
            continue
        p = _interp_prob(curve, rr)
        if p is None:
            continue
        ev = ev_full_exit_r(p_hit=p, tp_rr=rr, cost_r=cost_r)
        grid_eval.append(rr)
        if ev > best_ev:
            best_rr = rr
            best_p = p
            best_ev = ev
            if rr_raw < min_rr - 1e-9:
                chose_clamped = "tp1_adaptive_clamped_min_rr"
            elif rr_raw > max_rr + 1e-9:
                chose_clamped = "tp1_adaptive_clamped_max_rr"
            else:
                chose_clamped = ""

    ev_delta = best_ev - ev_base

    # nothing better than baseline
    if best_rr == baseline_rr and best_p == p_baseline:
        return AdaptiveTP1Decision(
            enabled=True, apply=False, mode=mode,
            reason="tp1_adaptive_skip_low_ev_delta",
            tp1_dist=None, tp1_rr=None, p_hit=best_p, p_hit_baseline=p_baseline,
            ev_baseline_r=ev_base, ev_adaptive_r=best_ev, ev_delta_r=ev_delta,
            cost_r=cost_r, samples=samples, baseline_rr=baseline_rr,
            grid_evaluated=tuple(grid_eval),
        )

    # EV delta safety margin
    if ev_delta < min_ev_delta:
        return AdaptiveTP1Decision(
            enabled=True, apply=False, mode=mode,
            reason="tp1_adaptive_skip_low_ev_delta",
            tp1_dist=None, tp1_rr=None, p_hit=best_p, p_hit_baseline=p_baseline,
            ev_baseline_r=ev_base, ev_adaptive_r=best_ev, ev_delta_r=ev_delta,
            cost_r=cost_r, samples=samples, baseline_rr=baseline_rr,
            grid_evaluated=tuple(grid_eval),
        )

    # tiny-tp1 floor
    if stop_bps * best_rr < min_tp1_bps:
        return AdaptiveTP1Decision(
            enabled=True, apply=False, mode=mode,
            reason="tp1_adaptive_skip_tiny_tp1",
            tp1_dist=None, tp1_rr=None, p_hit=best_p, p_hit_baseline=p_baseline,
            ev_baseline_r=ev_base, ev_adaptive_r=best_ev, ev_delta_r=ev_delta,
            cost_r=cost_r, samples=samples, baseline_rr=baseline_rr,
            grid_evaluated=tuple(grid_eval),
        )

    # apply iff paper/enforce
    apply = mode in {"paper", "enforce"}
    reason = chose_clamped or ("tp1_adaptive_apply" if apply else "tp1_adaptive_shadow")
    tp1_dist = best_rr * stop_dist_f

    return AdaptiveTP1Decision(
        enabled=True, apply=apply, mode=mode, reason=reason,
        tp1_dist=tp1_dist, tp1_rr=best_rr,
        p_hit=best_p, p_hit_baseline=p_baseline,
        ev_baseline_r=ev_base, ev_adaptive_r=best_ev, ev_delta_r=ev_delta,
        cost_r=cost_r, samples=samples, baseline_rr=baseline_rr,
        grid_evaluated=tuple(grid_eval),
    )


def _skip(mode: str, reason: str, *, samples: int = 0) -> AdaptiveTP1Decision:
    return AdaptiveTP1Decision(
        enabled=True, apply=False, mode=mode, reason=reason,
        tp1_dist=None, tp1_rr=None, p_hit=None, p_hit_baseline=None,
        ev_baseline_r=0.0, ev_adaptive_r=0.0, ev_delta_r=0.0,
        cost_r=0.0, samples=samples, baseline_rr=None, grid_evaluated=(),
    )
