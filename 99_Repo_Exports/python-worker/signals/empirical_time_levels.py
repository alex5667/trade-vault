from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, List, Optional, Set


def _canon(x: Any) -> str:
    return (str(x or "").strip().lower() or "na")


def _canon_sym(x: Any) -> str:
    return (str(x or "").strip().upper().replace("/", "").replace("-", "") or "NA")


def _env_bool(name: str, default: bool) -> bool:
    v = (os.getenv(name, "1" if default else "0") or "").strip().lower()
    return v in {"1", "true", "yes", "on"}


def _env_int(name: str, default: int) -> int:
    try:
        return int(float(os.getenv(name, str(default)) or default))
    except Exception:
        return int(default)


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)) or default)
    except Exception:
        return float(default)


def _parse_csv_ints(s: str) -> List[int]:
    out: List[int] = []
    for part in (s or "").split(","):
        p = part.strip()
        if not p:
            continue
        try:
            out.append(int(float(p)))
        except Exception:
            pass
    return out


def _parse_csv_strs(s: str) -> List[str]:
    out: List[str] = []
    for part in (s or "").split(","):
        p = part.strip().lower()
        if p:
            out.append(p)
    return out


def _clamp(x: float, lo: float, hi: float) -> float:
    if hi > 0 and x > hi:
        return hi
    if lo > 0 and x < lo:
        return lo
    return x


def _percentile(xs: List[float], q: float) -> Optional[float]:
    """
    Simple percentile without numpy. q in [0,1].
    """
    if not xs:
        return None
    ys = sorted(float(x) for x in xs if x is not None)
    if not ys:
        return None
    qv = max(0.0, min(1.0, float(q)))
    if len(ys) == 1:
        return float(ys[0])
    idx = qv * (len(ys) - 1)
    lo = int(idx)
    hi = min(lo + 1, len(ys) - 1)
    w = idx - lo
    return float(ys[lo] * (1.0 - w) + ys[hi] * w)


def _nearest_bucket(buckets: List[int], t_ms: int) -> Optional[int]:
    if not buckets:
        return None
    tt = int(t_ms)
    best = None
    best_d = None
    for b in buckets:
        bb = int(b)
        if bb <= 0:
            continue
        d = abs(bb - tt)
        if best is None or best_d is None or d < best_d:
            best = bb
            best_d = d
    return best


@dataclass(frozen=True)
class EmpiricalTimeLevelsConfig:
    enabled: bool
    use_regime_dim: bool
    min_n_alive: int
    q_mfe: float
    q_mae: float
    buckets_minutes: List[int]

    # NEW: Double-T (TTD quantile selector)
    ttd_q_slow: float           # default 0.50 (median)
    ttd_q_fast: float           # default 0.25
    fast_regimes: Set[str]      # regimes where we use q25 instead of q50

    # NEW: TP/SL clamp (bps) to avoid insane levels on small/noisy samples
    tp1_min_bps: float
    tp1_max_bps: float
    sl_min_bps: float
    sl_max_bps: float

    # NEW: survival-aware SL
    min_n_total: int            # denominator stability
    survive_min: float          # p_survive(T) >= survive_min

    @classmethod
    def from_env(cls) -> "EmpiricalTimeLevelsConfig":
        mins = _parse_csv_ints(os.getenv("EMP_TIME_BUCKETS_MINUTES", "1,2,3,5,8,13,21,34,45"))
        return cls(
            enabled=_env_bool("EMP_TIME_LEVELS_RUNTIME_ENABLED", True),
            use_regime_dim=_env_bool("EMP_LEVELS_USE_REGIME_DIM", True),
            min_n_alive=max(20, _env_int("EMP_TIME_LEVELS_MIN_N", 120)),
            q_mfe=max(0.0, min(1.0, _env_float("EMP_TIME_LEVELS_Q_MFE", 0.60))),
            q_mae=max(0.0, min(1.0, _env_float("EMP_TIME_LEVELS_Q_MAE", 0.80))),
            buckets_minutes=[m for m in mins if m > 0],

            ttd_q_slow=max(0.0, min(1.0, _env_float("EMP_TIME_TTD_Q_SLOW", 0.50))),
            ttd_q_fast=max(0.0, min(1.0, _env_float("EMP_TIME_TTD_Q_FAST", 0.25))),
            fast_regimes=set(_parse_csv_strs(os.getenv(
                "EMP_TIME_TTD_FAST_REGIMES",
                "expansion,trending_bull,trending_bear,trend"
            ))),

            tp1_min_bps=max(0.0, _env_float("EMP_TIME_TP1_MIN_BPS", 10.0)),
            tp1_max_bps=max(0.0, _env_float("EMP_TIME_TP1_MAX_BPS", 500.0)),
            sl_min_bps=max(0.0, _env_float("EMP_TIME_SL_MIN_BPS", 10.0)),
            sl_max_bps=max(0.0, _env_float("EMP_TIME_SL_MAX_BPS", 800.0)),

            min_n_total=max(20, _env_int("EMP_TIME_LEVELS_MIN_N_TOTAL", 120)),
            survive_min=max(0.0, min(1.0, _env_float("EMP_TIME_LEVELS_SURVIVE_MIN", 0.25))),
        )

    def buckets_ms(self) -> List[int]:
        return [m * 60_000 for m in self.buckets_minutes]


@dataclass(frozen=True)
class EmpiricalTimeLevelsResult:
    ok: bool
    ttd_median_ms: int
    bucket_ms: int
    n_alive: int
    tp1_bps: float
    sl_bps: float
    notes: str = ""


class RedisEmpiricalTimeLevelsProvider:
    def __init__(self, redis_client: Any, cfg: EmpiricalTimeLevelsConfig):
        self.redis = redis_client
        self.cfg = cfg

    def _key(self, *, kind: str, symbol: str, tf: str, regime: str, suffix: str) -> str:
        k = _canon(kind)
        s = _canon_sym(symbol)
        t = _canon(tf)
        r = _canon(regime) if self.cfg.use_regime_dim else "na"
        return f"statsbuf:{k}:{s}:{t}:{r}:{suffix}"

    def _lrange_floats(self, key: str) -> List[float]:
        try:
            xs = self.redis.lrange(key, 0, -1) or []
        except Exception:
            return []
        out: List[float] = []
        for v in xs:
            try:
                if isinstance(v, (bytes, bytearray)):
                    v = v.decode("utf-8")
                f = float(v)
                if f > 0:
                    out.append(f)
            except Exception:
                pass
        return out

    def _median_ttd_ms(self, *, kind: str, symbol: str, tf: str, regime: str) -> Optional[int]:
        # existing list already stores ttd for tp1_hit trades (your pipeline behavior)
        key = self._key(kind=kind, symbol=symbol, tf=tf, regime=regime, suffix="ttd_ms")
        xs = self._lrange_floats(key)
        if len(xs) < 5:
            return None
        med = _percentile(xs, 0.50)
        return int(med or 0) if med is not None else None

    def _ttd_quantile_ms(self, *, kind: str, symbol: str, tf: str, regime: str, q: float) -> Optional[int]:
        key = self._key(kind=kind, symbol=symbol, tf=tf, regime=regime, suffix="ttd_ms")
        xs = self._lrange_floats(key)
        if len(xs) < 5:
            return None
        v = _percentile(xs, q)
        return int(v) if v is not None and v > 0 else None

    def get_levels(self, *, kind: str, symbol: str, tf: str, regime: str) -> EmpiricalTimeLevelsResult:
        if not self.cfg.enabled or self.redis is None:
            return EmpiricalTimeLevelsResult(ok=False, ttd_median_ms=0, bucket_ms=0, n_alive=0, tp1_bps=0.0, sl_bps=0.0, notes="disabled")

        # -----------------------------------------------------------------
        # NEW: Double-T selector:
        #   - slow/default: T = q50(TTD_tp1)
        #   - fast regimes (trend/expansion): T = q25(TTD_tp1)
        # This improves calibration for momentum regimes where TP1 is hit faster.
        # Fail-open fallback: if q25 not available -> use q50.
        # -----------------------------------------------------------------
        r_key = _canon(regime)
        use_fast = (r_key in self.cfg.fast_regimes)
        ttd_q = self.cfg.ttd_q_fast if use_fast else self.cfg.ttd_q_slow
        ttd_ms = self._ttd_quantile_ms(kind=kind, symbol=symbol, tf=tf, regime=regime, q=ttd_q)
        if not ttd_ms:
            ttd_ms = self._ttd_quantile_ms(kind=kind, symbol=symbol, tf=tf, regime=regime, q=self.cfg.ttd_q_slow)
        if not ttd_ms:
            return EmpiricalTimeLevelsResult(ok=False, ttd_median_ms=0, bucket_ms=0, n_alive=0, tp1_bps=0.0, sl_bps=0.0, notes="no_ttd_quantile")

        b = _nearest_bucket(self.cfg.buckets_ms(), int(ttd_ms))
        if not b:
            return EmpiricalTimeLevelsResult(ok=False, ttd_median_ms=int(ttd_ms), bucket_ms=0, n_alive=0, tp1_bps=0.0, sl_bps=0.0, notes="no_bucket")

        # n_alive is approximated by alive list length (trimmed buffer)
        alive_key = self._key(kind=kind, symbol=symbol, tf=tf, regime=regime, suffix=f"alive_t{b}")
        try:
            n_alive = int(self.redis.llen(alive_key) or 0)
        except Exception:
            n_alive = 0
        if n_alive < self.cfg.min_n_alive:
            return EmpiricalTimeLevelsResult(ok=False, ttd_median_ms=int(ttd_ms), bucket_ms=int(b), n_alive=int(n_alive), tp1_bps=0.0, sl_bps=0.0, notes="insufficient_n_alive")

        # -----------------------------------------------------------------
        # NEW: survival-aware SL
        # p_survive(T) ≈ n_alive(T) / n_total_trades (same sliding window buffer)
        # This prevents "tight SL" calibrated on tiny survivor subset.
        # -----------------------------------------------------------------
        trades_key = self._key(kind=kind, symbol=symbol, tf=tf, regime=regime, suffix="trades")
        try:
            n_total = int(self.redis.llen(trades_key) or 0)
        except Exception:
            n_total = 0
        if n_total < self.cfg.min_n_total:
            return EmpiricalTimeLevelsResult(ok=False, ttd_median_ms=int(ttd_ms), bucket_ms=int(b), n_alive=int(n_alive), tp1_bps=0.0, sl_bps=0.0, notes="insufficient_n_total")
        p_survive = float(n_alive) / float(max(1, n_total))
        if p_survive < self.cfg.survive_min:
            return EmpiricalTimeLevelsResult(ok=False, ttd_median_ms=int(ttd_ms), bucket_ms=int(b), n_alive=int(n_alive), tp1_bps=0.0, sl_bps=0.0, notes=f"low_survival p={p_survive:.3f}")

        mfe_key = self._key(kind=kind, symbol=symbol, tf=tf, regime=regime, suffix=f"mfe_bps_t{b}")
        mae_key = self._key(kind=kind, symbol=symbol, tf=tf, regime=regime, suffix=f"mae_bps_t{b}")
        mfe = self._lrange_floats(mfe_key)
        mae = self._lrange_floats(mae_key)
        tp1_bps = float(_percentile(mfe, self.cfg.q_mfe) or 0.0)
        sl_bps = float(_percentile(mae, self.cfg.q_mae) or 0.0)
        if tp1_bps <= 0 or sl_bps <= 0:
            return EmpiricalTimeLevelsResult(ok=False, ttd_median_ms=int(ttd_ms), bucket_ms=int(b), n_alive=int(n_alive), tp1_bps=float(tp1_bps), sl_bps=float(sl_bps), notes="bad_quantiles")

        # -----------------------------------------------------------------
        # NEW: clamp TP/SL in bps to prevent insane levels on small/noisy samples.
        # -----------------------------------------------------------------
        tp1_bps = _clamp(tp1_bps, self.cfg.tp1_min_bps, self.cfg.tp1_max_bps)
        sl_bps = _clamp(sl_bps, self.cfg.sl_min_bps, self.cfg.sl_max_bps)

        return EmpiricalTimeLevelsResult(ok=True, ttd_median_ms=int(ttd_ms), bucket_ms=int(b), n_alive=int(n_alive), tp1_bps=float(tp1_bps), sl_bps=float(sl_bps), notes="")
