from __future__ import annotations

import os
import math
from dataclasses import dataclass
from typing import Any, Optional, Protocol, List, Dict


def _env_bool(name: str, default: bool) -> bool:
    v = (os.getenv(name, "1" if default else "0") or "").strip().lower()
    return v in {"1", "true", "yes", "on"}


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)) or default)
    except Exception:
        return float(default)


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)) or default)
    except Exception:
        return int(default)


def _clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def _norm_symbol(sym: str) -> str:
    return (sym or "").strip().upper().replace("/", "").replace("-", "")


def _norm_kind(kind: str) -> str:
    return (kind or "").strip().lower()


def _norm_regime(reg: Any) -> str:
    if reg is None:
        return "na"
    if isinstance(reg, str):
        s = reg.strip().lower()
        return s if s else "na"
    s = str(getattr(reg, "name", None) or getattr(reg, "value", None) or reg).strip().lower()
    return s if s else "na"


def _quantile(sorted_xs: List[float], q: float) -> Optional[float]:
    """
    Квантиль ближайшего ранга (nearest-rank) на уже отсортированных значениях.
    q в [0,1]. Возвращает None, если пусто.
    """
    if not sorted_xs:
        return None
    q = _clamp(float(q), 0.0, 1.0)
    n = len(sorted_xs)
    # nearest-rank: ceil(q*n) - 1
    idx = int(math.ceil(q * n) - 1)
    idx = max(0, min(n - 1, idx))
    return float(sorted_xs[idx])


def _parse_csv_ints(s: str) -> List[int]:
    out: List[int] = []
    for part in (s or "").split(","):
        p = part.strip()
        if not p:
            continue
        try:
            out.append(int(p))
        except Exception:
            pass
    return out


def _time_buckets_ms_from_env() -> List[int]:
    """
    Buckets used by writer (StatsAggregator) and reader (this module).
    ENV is minutes to keep it readable for operators.
    """
    mins = _parse_csv_ints(os.getenv("EMP_TIME_BUCKETS_MINUTES", "1,2,3,5,8,13,21,34,45"))
    ms = [m * 60_000 for m in mins if m and m > 0]
    ms.sort()
    return ms


def _select_bucket_ceil(target_ms: int, buckets_ms: List[int]) -> Optional[int]:
    """
    Pick the smallest bucket >= target (ceil).
    Rationale:
      - MFE@T is non-decreasing over time (max favorable excursion).
      - MAE@T is non-decreasing over time (max adverse excursion in abs).
    So ceil(target) is a stable approximation to "at time T" (never earlier).
    """
    if target_ms <= 0 or not buckets_ms:
        return None
    for b in buckets_ms:
        if b >= target_ms:
            return int(b)
    return int(buckets_ms[-1])  # if target beyond max bucket


def _regime_is_fast(regime: str) -> bool:
    """
    Fast-T mode is enabled for regimes where "time to TP1" is typically shorter.
    Default keywords are intentionally broad and substring-based (robust to naming).
    """
    rg = (regime or "").strip().lower()
    if not rg:
        return False
    kws = [x.strip().lower() for x in (os.getenv("EMP_TTD_FAST_REGIME_KEYWORDS", "trend,expansion") or "").split(",") if x.strip()]
    if not kws:
        kws = ["trend", "expansion"]
    return any(k in rg for k in kws)


def _clamp_bps(v: float, vmin: float, vmax: float) -> float:
    try:
        x = float(v)
    except Exception:
        return float(vmin)
    if math.isnan(x) or math.isinf(x):
        return float(vmin)
    return float(max(vmin, min(vmax, x)))

def to_floats(xs: List[Any]) -> List[float]:
    """
    Convert list of values to floats, handling bytes decoding.
    Reused from existing code (no duplicates).
    """
    out: List[float] = []
    for x in xs or []:
        try:
            if isinstance(x, (bytes, bytearray)):
                x = x.decode("utf-8", "ignore")
            out.append(float(x))
        except Exception:
            pass
    return out

def _decode_hgetall(h: Dict[Any, Any]) -> Dict[str, str]:
    """
    Redis may return bytes keys/values. Normalize to {str: str}.
    Fail-open: best-effort decoding.
    """
    out: Dict[str, str] = {}
    for k, v in (h or {}).items():
        try:
            if isinstance(k, (bytes, bytearray)):
                k = k.decode("utf-8", "ignore")
            if isinstance(v, (bytes, bytearray)):
                v = v.decode("utf-8", "ignore")
            out[str(k)] = str(v)
        except Exception:
            pass
    return out



@dataclass(frozen=True)
class EmpiricalLevelStats:
    samples: int
    mfe_tp1_bps_q60: float
    mae_to_tp1_bps_q80: float
    ttd_tp1_ms_median: int = 0


class EmpiricalStatsProvider(Protocol):
    def get_level_stats(self, *, symbol: str, kind: str, regime: str, samples: int = 0) -> Optional[EmpiricalLevelStats]:
        ...


@dataclass(frozen=True)
class EmpiricalLevelsConfig:
    enabled: bool
    min_samples: int
    blend_alpha: float

    tp1_min_rr: float

    tp1_atr_min: float
    tp1_atr_max: float
    stop_atr_min: float
    stop_atr_max: float

    tp1_bps_min: float
    tp1_bps_max: float
    stop_bps_min: float
    stop_bps_max: float

    @classmethod
    def from_env(cls) -> "EmpiricalLevelsConfig":
        return cls(
            enabled=_env_bool("LEVELS_EMPIRICAL_ENABLED", False),
            min_samples=_env_int("LEVELS_EMPIRICAL_MIN_SAMPLES", 80),
            blend_alpha=_clamp(_env_float("LEVELS_EMPIRICAL_BLEND_ALPHA", 0.7), 0.0, 1.0),

            tp1_min_rr=_env_float("LEVELS_EMPIRICAL_TP1_MIN_RR", 1.1),

            tp1_atr_min=_env_float("LEVELS_EMPIRICAL_TP1_ATR_MIN", 0.35),
            tp1_atr_max=_env_float("LEVELS_EMPIRICAL_TP1_ATR_MAX", 2.00),
            stop_atr_min=_env_float("LEVELS_EMPIRICAL_STOP_ATR_MIN", 0.30),
            stop_atr_max=_env_float("LEVELS_EMPIRICAL_STOP_ATR_MAX", 2.50),

            tp1_bps_min=_env_float("LEVELS_EMPIRICAL_TP1_BPS_MIN", 8.0),
            tp1_bps_max=_env_float("LEVELS_EMPIRICAL_TP1_BPS_MAX", 400.0),
            stop_bps_min=_env_float("LEVELS_EMPIRICAL_STOP_BPS_MIN", 8.0),
            stop_bps_max=_env_float("LEVELS_EMPIRICAL_STOP_BPS_MAX", 700.0),
        )


@dataclass(frozen=True)
class EmpiricalSuggestion:
    stop_dist: float
    tp1_dist: float
    source: str
    samples: int
    ttd_tp1_ms: int = 0


class EmpiricalLevels:
    """
    Конвертирует эмпирические распределения MFE/MAE в дистанции SL/TP1 (в единицах цены).
    Безопасно по дизайну:
      - требует min_samples
      - смешивает с baseline
      - ограничивает по ATR-мультипликаторам и лимитам bps
      - принуждает к минимальному RR для TP1
    """
    def __init__(self, cfg: EmpiricalLevelsConfig, provider: Optional[EmpiricalStatsProvider]):
        self.cfg = cfg
        self.provider = provider

    @classmethod
    def from_env(cls, provider: Optional[EmpiricalStatsProvider]) -> "EmpiricalLevels":
        return cls(EmpiricalLevelsConfig.from_env(), provider)

    def suggest(
        self,
        *,
        symbol: str,
        kind: str,
        regime: Any,
        entry: float,
        atr: float,
        baseline_stop_dist: float,
        baseline_tp1_dist: float,
    ) -> Optional[EmpiricalSuggestion]:
        if not self.cfg.enabled or self.provider is None:
            return None
        if entry <= 0.0 or not math.isfinite(entry):
            return None

        sym = _norm_symbol(symbol)
        kd = _norm_kind(kind)
        rg = _norm_regime(regime)

        st = self.provider.get_level_stats(symbol=sym, kind=kd, regime=rg, samples=0)
        if st is None or int(st.samples) < int(self.cfg.min_samples):
            return None

        tp1_bps = _clamp(float(st.mfe_tp1_bps_q60), self.cfg.tp1_bps_min, self.cfg.tp1_bps_max)
        stop_bps = _clamp(float(st.mae_to_tp1_bps_q80), self.cfg.stop_bps_min, self.cfg.stop_bps_max)

        tp1_dist_emp = entry * tp1_bps / 10_000.0
        stop_dist_emp = entry * stop_bps / 10_000.0

        atr_f = float(atr) if atr and atr > 0 and math.isfinite(float(atr)) else 0.0
        if atr_f > 0:
            tp1_dist_emp = _clamp(tp1_dist_emp, self.cfg.tp1_atr_min * atr_f, self.cfg.tp1_atr_max * atr_f)
            stop_dist_emp = _clamp(stop_dist_emp, self.cfg.stop_atr_min * atr_f, self.cfg.stop_atr_max * atr_f)

        a = float(self.cfg.blend_alpha)
        tp1_dist = a * tp1_dist_emp + (1.0 - a) * float(baseline_tp1_dist)
        stop_dist = a * stop_dist_emp + (1.0 - a) * float(baseline_stop_dist)

        # Enforce minimal RR for TP1
        min_rr = max(0.1, float(self.cfg.tp1_min_rr))
        if stop_dist > 0.0:
            need = stop_dist * min_rr
            if tp1_dist < need:
                tp1_dist = need

        if not (tp1_dist > 0 and stop_dist > 0 and math.isfinite(tp1_dist) and math.isfinite(stop_dist)):
            return None

        return EmpiricalSuggestion(
            stop_dist=float(stop_dist),
            tp1_dist=float(tp1_dist),
            source="empirical_blend",
            samples=int(st.samples),
            ttd_tp1_ms=int(st.ttd_tp1_ms_median or 0),
        )


def read_empirical_level_stats(
    redis: Any,
    *,
    symbol: str,
    kind: str,
    regime: str,
    tf: str,
    use_regime_dim: bool,
    buf_max: int,
    samples: int,
) -> Optional[EmpiricalLevelStats]:
    """
    Reader that supports BOTH:
      A) legacy buffers:
         statsbuf:{kind}:{symbol}:{tf}:{rg}:mfe_bps / mae_bps / ttd_ms
      B) time-bucket buffers (NEW):
         statsbuf:{kind}:{symbol}:{tf}:{rg}:mfe_bps_t{bucket_ms}
         statsbuf:{kind}:{symbol}:{tf}:{rg}:mae_bps_t{bucket_ms}
         and survival counters:
         statscnt:{kind}:{symbol}:{tf}:{rg}:survival  (HASH: total, alive_t{bucket_ms})
    It is fail-open:
      - if time-bucket data missing/insufficient -> fallback to legacy buffers
      - if everything missing -> return None (caller should fallback to compute_levels/RR/ATR)
    """
    sym = str(symbol or "").strip().upper()
    kd = str(kind or "").strip().lower()
    rg = (str(regime or "").strip().lower() or "na") if use_regime_dim else "na"
    tf_s = str(tf or "").strip().lower() or "1m"

    def buf_key(metric: str) -> str:
        return f"statsbuf:{kd}:{sym}:{tf_s}:{rg}:{metric}"

    # -----------------------------
    # Load TTD buffer (legacy key).
    # -----------------------------
    try:
        ttd_raw = redis.lrange(buf_key("ttd_ms"), 0, max(int(buf_max), 1) - 1) or []
    except Exception:
        ttd_raw = []
    ttd_vals = sorted([int(float(x)) for x in to_floats(ttd_raw) if float(x) > 0])
    ttd_med = int(_quantile([float(x) for x in ttd_vals], 0.50) or 0.0) if ttd_vals else 0
    ttd_q25 = int(_quantile([float(x) for x in ttd_vals], 0.25) or 0.0) if ttd_vals else 0

    # ---------------------------------------------
    # Decide target T (median or fast-q25 by regime).
    # ---------------------------------------------
    use_fast = _env_bool("EMP_TTD_FAST_IF_REGIME", True)
    ttd_target = ttd_med
    if use_fast and _regime_is_fast(rg) and ttd_q25 > 0:
        ttd_target = ttd_q25

    # ---------------------------------------------
    # Try NEW time-bucket buffers first (if enabled).
    # ---------------------------------------------
    use_time = _env_bool("EMP_TIME_SNAPSHOT_READ", True)
    buckets_ms = _time_buckets_ms_from_env() if use_time else []
    bucket_ms = _select_bucket_ceil(int(ttd_target), buckets_ms) if use_time else None

    # Optional: survival gate (p_survive(T) >= S_MIN).
    try:
        survive_min = float(os.getenv("EMP_SURVIVE_MIN", "0") or "0")
    except Exception:
        survive_min = 0.0

    def _survival_ok() -> bool:
        if survive_min <= 0.0 or not bucket_ms:
            return True
        try:
            h = redis.hgetall(f"statscnt:{kd}:{sym}:{tf_s}:{rg}:survival") or {}
        except Exception:
            return False
        # decode bytes -> str (redis-py often returns bytes)
        hh: Dict[str, str] = {}
        for k, v in (h or {}).items():
            try:
                if isinstance(k, (bytes, bytearray)):
                    k = k.decode("utf-8", "ignore")
                if isinstance(v, (bytes, bytearray)):
                    v = v.decode("utf-8", "ignore")
                hh[str(k)] = str(v)
            except Exception:
                pass
        try:
            total = int(float(hh.get("total", "0") or "0"))
            alive = int(float(hh.get(f"alive_t{int(bucket_ms)}", "0") or "0"))
        except Exception:
            return False
        if total <= 0:
            return False
        p = float(alive) / float(total)
        return p >= float(survive_min)

    def _read_quantiles_from(metric_mfe: str, metric_mae: str) -> Optional[EmpiricalLevelStats]:
        try:
            mfe_raw = redis.lrange(buf_key(metric_mfe), 0, max(int(buf_max), 1) - 1) or []
            mae_raw = redis.lrange(buf_key(metric_mae), 0, max(int(buf_max), 1) - 1) or []
        except Exception:
            return None
        mfe = sorted([x for x in to_floats(mfe_raw) if x > 0])
        mae = sorted([x for x in to_floats(mae_raw) if x > 0])
        if len(mfe) < 5 or len(mae) < 5:
            return None
        mfe_q60 = _quantile(mfe, 0.60)
        mae_q80 = _quantile(mae, 0.80)
        if mfe_q60 is None or mae_q80 is None:
            return None
        # Samples fallback (same semantics as your current code).
        s = int(samples) if int(samples) > 0 else max(len(mfe), len(mae))
        return EmpiricalLevelStats(
            samples=int(s),
            mfe_tp1_bps_q60=_clamp_bps(float(mfe_q60), tp1_min, tp1_max),
            mae_to_tp1_bps_q80=_clamp_bps(float(mae_q80), sl_min, sl_max),
            ttd_tp1_ms_median=int(ttd_med or 0),
        )

    # Time-bucket path (MFE@T / MAE@T)
    if use_time and bucket_ms and _survival_ok():
        st = _read_quantiles_from(f"mfe_bps_t{int(bucket_ms)}", f"mae_bps_t{int(bucket_ms)}")
        if st is not None:
            return st

    # Fallback: legacy global buffers
    return _read_quantiles_from("mfe_bps", "mae_bps")


class RedisEmpiricalStatsProvider:
    """
    Читает эмпирические распределения из Redis.

    Ваш текущий StatsAggregator хранит только суммы:
      total_trades, sum_mfe, sum_mae, sum_duration_ms
    Этого НЕ достаточно для квантилей.

    Поэтому этот провайдер ожидает дополнительные ограниченные буферы (списки):
      statsbuf:{kind}:{symbol}:{tf}:{regime}:mfe_bps
      statsbuf:{kind}:{symbol}:{tf}:{regime}:mae_bps
      statsbuf:{kind}:{symbol}:{tf}:{regime}:ttd_ms

    Если буферы отсутствуют/пусты -> возвращает None (fail-open).
    """
    def __init__(self, redis_client: Any, *, tf: str = "1m", buf_max: int = 300, use_regime_dim: bool = True, cache_ttl_sec: float = 60.0):
        self.redis = redis_client
        self.tf = (tf or "1m").strip()
        self.buf_max = int(buf_max)
        self.use_regime_dim = bool(use_regime_dim)
        self.cache_ttl_sec = float(cache_ttl_sec)
        self._cache: Dict[str, Tuple[float, Optional[EmpiricalLevelStats]]] = {}

    def _stats_key(self, symbol: str, kind: str, regime: str) -> str:
        # Существующий хеш итогов (уже в вашем проекте):
        # stats:{kind}:{symbol}:{tf}
        # Опциональное измерение режима:
        # stats:{kind}:{symbol}:{tf}:{regime}
        if self.use_regime_dim:
            return f"stats:{kind}:{symbol}:{self.tf}:{regime}"
        return f"stats:{kind}:{symbol}:{self.tf}"

    def _buf_key(self, symbol: str, kind: str, regime: str, metric: str) -> str:
        rg = regime if self.use_regime_dim else "na"
        return f"statsbuf:{kind}:{symbol}:{self.tf}:{rg}:{metric}"

    def get_level_stats(self, symbol: str, kind: str, regime: str, samples: int = 0) -> Optional["EmpiricalLevelStats"]:
        sym = str(symbol or "").strip().upper()
        kd = str(kind or "").strip().lower()
        rg = str(regime or "").strip().lower()
        if not sym or not kd:
            return None

        # --- Cache Check ---
        if self.cache_ttl_sec > 0:
            cache_key = f"{sym}|{kd}|{rg}|{samples}"
            now = time.time()
            if cache_key in self._cache:
                ts, val = self._cache[cache_key]
                if now - ts < self.cache_ttl_sec:
                    return val

        try:
            # ---------------------------------------------------------------
            # 1) Always read TTD buffer (median is the target horizon T).
            # ---------------------------------------------------------------
            ttd_key = self._buf_key(sym, kd, rg, "ttd_ms")
            ttd_raw = self.redis.lrange(ttd_key, 0, self.buf_max - 1) or []
            ttd_vals = []
            for x in to_floats(ttd_raw):
                try:
                    v = int(float(x))
                    if v > 0:
                        ttd_vals.append(v)
                except Exception:
                    pass
            ttd_vals.sort()
            ttd_med = _quantile([float(x) for x in ttd_vals], 0.50) if ttd_vals else 0.0
            ttd_q25 = _quantile([float(x) for x in ttd_vals], 0.25) if ttd_vals else 0.0

            def _read_quantiles(mfe_metric: str, mae_metric: str) -> Optional["EmpiricalLevelStats"]:
                mfe_raw = self.redis.lrange(self._buf_key(sym, kd, rg, mfe_metric), 0, self.buf_max - 1) or []
                mae_raw = self.redis.lrange(self._buf_key(sym, kd, rg, mae_metric), 0, self.buf_max - 1) or []

                mfe = sorted([v for v in to_floats(mfe_raw) if v > 0])
                mae = sorted([v for v in to_floats(mae_raw) if v > 0])
                if len(mfe) < 5 or len(mae) < 5:
                    return None

                mfe_q60 = _quantile(mfe, 0.60)
                mae_q80 = _quantile(mae, 0.80)
                if mfe_q60 is None or mae_q80 is None:
                    return None

                s = int(samples) if int(samples) > 0 else max(len(mfe), len(mae))
                return EmpiricalLevelStats(
                    samples=int(s),
                    mfe_tp1_bps_q60=float(mfe_q60),
                    mae_to_tp1_bps_q80=float(mae_q80),
                    ttd_tp1_ms_median=int(ttd_med or 0),
                )

            result = None
            # ---------------------------------------------------------------
            # 2) STRICT time-snapshot mode:
            #    Use MFE@T and MAE@T where T = median(TTD_tp1).
            # ---------------------------------------------------------------
            if _env_bool("EMP_TIME_SNAPSHOTS_READ", True) and ttd_med and float(ttd_med) > 0.0:
                t_target = int(float(ttd_med))
                if _env_bool("EMP_TTD_FAST_IF_REGIME", True) and _regime_is_fast(rg) and ttd_q25 and float(ttd_q25) > 0.0:
                    t_target = int(float(ttd_q25))
                bset = _time_buckets_ms_from_env()
                bucket_ms = _select_bucket_ceil(t_target, bset)
                if bucket_ms:
                    # Optional survival hard gate
                    try:
                        survive_min = float(os.getenv("EMP_SURVIVE_MIN", "0") or "0")
                    except Exception:
                        survive_min = 0.0
                    
                    survival_ok = True
                    if survive_min > 0.0:
                        rg_seg = rg if self.use_regime_dim else "na"
                        surv_key = f"statscnt:{kd}:{sym}:{self.tf}:{rg_seg}:survival"
                        h = _decode_hgetall(self.redis.hgetall(surv_key) or {})
                        try:
                            total = int(float(h.get("total", "0") or "0"))
                            alive = int(float(h.get(f"alive_t{int(bucket_ms)}", "0") or "0"))
                            p_survive = float(alive) / float(total) if total > 0 else 0.0
                        except Exception:
                            p_survive = 0.0
                        if p_survive < float(survive_min):
                            survival_ok = False
                    
                    if survival_ok:
                        st = _read_quantiles(f"mfe_bps_t{int(bucket_ms)}", f"mae_bps_t{int(bucket_ms)}")
                        if st is not None:
                            result = st

            # ---------------------------------------------------------------
            # 3) Fallback: original (global) buffers
            # ---------------------------------------------------------------
            if result is None:
                result = _read_quantiles("mfe_bps", "mae_bps")
            
            # --- Update Cache ---
            if self.cache_ttl_sec > 0:
                self._cache[cache_key] = (time.time(), result)
            
            return result
            
        except Exception:
            return None

