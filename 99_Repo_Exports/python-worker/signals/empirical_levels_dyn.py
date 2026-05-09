from __future__ import annotations

import math
import os
from dataclasses import dataclass
from typing import Any

from utils.time_utils import get_ny_time_millis
import contextlib

# =====================================================================================
# Динамические уровни на основе эмпирических буферов (MFE/MAE/TTD).
#
# Источник данных (записывается StatsAggregator.update_stats()):
#   statsbuf:{kind}:{symbol}:{tf}:{regime}:mfe_bps  -> СПИСОК до ~300 последних значений (bps)
#   statsbuf:{kind}:{symbol}:{tf}:{regime}:mae_bps  -> СПИСОК до ~300 последних значений (bps)
#   statsbuf:{kind}:{symbol}:{tf}:{regime}:ttd_ms   -> СПИСОК до ~300 последних значений (ms)
#
# Цель:
#   TP1_bps = quantile(MFE_bps, q_tp1)   (по умолчанию q_tp1=0.60)
#   SL_bps  = quantile(MAE_bps, q_sl)    (по умолчанию q_sl=0.80)
#   TTD_ms  = quantile(TTD_ms,  q_ttd)   (по умолчанию q_ttd=0.50 median)
#
# Затем конвертируем bps -> цены:
#   LONG : tp1 = entry * (1 + tp1_bps/10000), sl = entry * (1 - sl_bps/10000)
#   SHORT: tp1 = entry * (1 - tp1_bps/10000), sl = entry * (1 + sl_bps/10000)
#
# Правила Fail-open:
#   - Если недостаточно сэмплов => ничего не делать.
#   - Любая ошибка redis => ничего не делать.
#   - Любая ошибка математики => ничего не делать.
#
# Производительность:
#   - LRANGE(0..-1) для 3 ключей может быть дорого для каждого сигнала => короткий in-process кэш.
# =====================================================================================


def _env_bool(name: str, default: bool) -> bool:
    v = (os.getenv(name, "1" if default else "0") or "").strip().lower()
    return v in {"1", "true", "yes", "on"}


def _env_int(name: str, default: int) -> int:
    try:
        return int(float(os.getenv(name, str(default)) or default))
    except Exception:
        return default


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)) or default)
    except Exception:
        return default


def _canon_symbol(sym: Any) -> str:
    s = (sym or "").strip().upper()
    return s.replace("/", "").replace("-", "")


def _canon_kind(kind: Any) -> str:
    return ((kind or "").strip().lower() or "na")


def _canon_tf(tf: Any) -> str:
    return ((tf or "").strip().lower() or "na")


def _canon_regime(r: Any) -> str:
    return ((r or "").strip().lower() or "na")


def _decode(x: Any) -> str:
    if isinstance(x, (bytes, bytearray)):
        try:
            return x.decode("utf-8")
        except Exception:
            return ""
    return str(x)


def _to_pos_floats(xs: list[Any]) -> list[float]:
    out: list[float] = []
    for x in xs:
        try:
            v = float(_decode(x).strip())
        except Exception:
            continue
        if math.isfinite(v) and v > 0.0:
            out.append(float(v))
    return out


def _to_pos_ints(xs: list[Any]) -> list[int]:
    out: list[int] = []
    for x in xs:
        try:
            v = int(float(_decode(x).strip()))
        except Exception:
            continue
        if v > 0:
            out.append(int(v))
    return out


def _percentile_sorted(sorted_vals: list[float], q: float) -> float:
    """
    Детерминированный перцентиль с линейной интерполяцией.
    q в диапазоне [0..1].
    """
    n = len(sorted_vals)
    if n == 0:
        return float("nan")
    if n == 1:
        return float(sorted_vals[0])
    qq = max(0.0, min(1.0, float(q)))
    # position in [0..n-1]
    pos = qq * (n - 1)
    lo = int(math.floor(pos))
    hi = int(math.ceil(pos))
    if lo == hi:
        return float(sorted_vals[lo])
    w = pos - lo
    return float(sorted_vals[lo] * (1.0 - w) + sorted_vals[hi] * w)


def _percentile(vals: list[float], q: float) -> float:
    sv = sorted(vals)
    return _percentile_sorted(sv, q)


@dataclass(frozen=True)
class EmpiricalLevelsConfig:
    enabled: bool
    min_n: int
    q_tp1: float
    q_sl: float
    q_ttd: float
    use_regime_dim: bool
    fallback_to_na_regime: bool
    cache_ms: int
    max_bps: float
    min_bps: float
    max_ttd_ms: int

    @classmethod
    def from_env(cls) -> EmpiricalLevelsConfig:
        return cls(
            enabled=_env_bool("EMP_LEVELS_ENABLED", False),
            min_n=max(10, _env_int("EMP_LEVELS_MIN_N", 60)),
            q_tp1=max(0.05, min(0.95, _env_float("EMP_LEVELS_TP1_Q", 0.60))),
            q_sl=max(0.05, min(0.95, _env_float("EMP_LEVELS_SL_Q", 0.80))),
            q_ttd=max(0.05, min(0.95, _env_float("EMP_LEVELS_TTD_Q", 0.50))),
            use_regime_dim=_env_bool("EMP_LEVELS_USE_REGIME_DIM", True),
            fallback_to_na_regime=_env_bool("EMP_LEVELS_FALLBACK_TO_NA", True),
            cache_ms=max(0, _env_int("EMP_LEVELS_CACHE_MS", 2000)),
            max_bps=max(10.0, _env_float("EMP_LEVELS_MAX_BPS", 2500.0)),
            min_bps=max(0.0, _env_float("EMP_LEVELS_MIN_BPS", 5.0)),
            max_ttd_ms=max(1_000, _env_int("EMP_LEVELS_MAX_TTD_MS", 6 * 60 * 60 * 1000)),
        )

    def key(self, *, kind: str, symbol: str, tf: str, regime: str, metric: str) -> str:
        k = _canon_kind(kind)
        s = _canon_symbol(symbol) or "NA"
        t = _canon_tf(tf)
        r = _canon_regime(regime)
        if not self.use_regime_dim:
            r = "na"
        return f"statsbuf:{k}:{s}:{t}:{r}:{metric}"


@dataclass(frozen=True)
class EmpiricalLevelsResult:
    tp1_bps: float
    sl_bps: float
    ttd_ms: int
    n_mfe: int
    n_mae: int
    n_ttd: int
    regime_used: str


class RedisEmpiricalLevelsProvider:
    """
    Небольшой кэширующий ридер над буферами Redis LIST.
    """

    def __init__(self, redis_client: Any, cfg: EmpiricalLevelsConfig):
        self.redis = redis_client
        self.cfg = cfg
        # кэш: key_prefix -> (ts_ms, result)
        self._cache: dict[str, tuple[int, EmpiricalLevelsResult | None]] = {}

    def _now_ms(self) -> int:
        return get_ny_time_millis()

    def _get_cached(self, cache_key: str) -> EmpiricalLevelsResult | None:
        if self.cfg.cache_ms <= 0:
            return None
        it = self._cache.get(cache_key)
        if not it:
            return None
        ts, res = it
        if self._now_ms() - ts <= self.cfg.cache_ms:
            return res
        return None

    def _put_cached(self, cache_key: str, res: EmpiricalLevelsResult | None) -> None:
        if self.cfg.cache_ms <= 0:
            return
        self._cache[cache_key] = (self._now_ms(), res)

    def get(self, *, kind: str, symbol: str, tf: str, regime: str) -> EmpiricalLevelsResult | None:
        """
        Читать буферы и вычислять квантили.
        Возвращает None, если недостаточно данных.
        """
        if not self.cfg.enabled:
            return None
        if self.redis is None:
            return None

        # ключ кэша должен отражать запрошенный режим; для fallback кэшируем по "эффективному" regime_used
        req_regime = _canon_regime(regime)
        cache_key = f"{_canon_kind(kind)}:{_canon_symbol(symbol)}:{_canon_tf(tf)}:{req_regime}"
        cached = self._get_cached(cache_key)
        if cached is not None:
            return cached

        def try_regime(rg: str) -> EmpiricalLevelsResult | None:
            k_mfe = self.cfg.key(kind=kind, symbol=symbol, tf=tf, regime=rg, metric="mfe_bps")
            k_mae = self.cfg.key(kind=kind, symbol=symbol, tf=tf, regime=rg, metric="mae_bps")
            k_ttd = self.cfg.key(kind=kind, symbol=symbol, tf=tf, regime=rg, metric="ttd_ms")
            try:
                mfe_raw = self.redis.lrange(k_mfe, 0, -1)
                mae_raw = self.redis.lrange(k_mae, 0, -1)
                ttd_raw = self.redis.lrange(k_ttd, 0, -1)
            except Exception:
                return None

            mfe = _to_pos_floats(list(mfe_raw or []))
            mae = _to_pos_floats(list(mae_raw or []))
            ttd = _to_pos_ints(list(ttd_raw or []))

            # требуем достаточно сэмплов для TP и SL; TTD опционально, но желательно
            if len(mfe) < self.cfg.min_n or len(mae) < self.cfg.min_n:
                return None

            tp1_bps = _percentile(mfe, self.cfg.q_tp1)
            sl_bps = _percentile(mae, self.cfg.q_sl)
            if not math.isfinite(tp1_bps) or not math.isfinite(sl_bps):
                return None

            # ограничить безопасными диапазонами bps (избегать абсурдных значений на плохих данных)
            tp1_bps = max(self.cfg.min_bps, min(self.cfg.max_bps, float(tp1_bps)))
            sl_bps = max(self.cfg.min_bps, min(self.cfg.max_bps, float(sl_bps)))

            ttd_ms = 0
            if len(ttd) >= max(10, self.cfg.min_n // 2):
                try:
                    ttd_q = _percentile([float(x) for x in ttd], self.cfg.q_ttd)
                    ttd_ms = int(max(0.0, min(float(self.cfg.max_ttd_ms), float(ttd_q))))
                except Exception:
                    ttd_ms = 0

            return EmpiricalLevelsResult(
                tp1_bps=float(tp1_bps),
                sl_bps=float(sl_bps),
                ttd_ms=int(ttd_ms),
                n_mfe=len(mfe),
                n_mae=len(mae),
                n_ttd=len(ttd),
                regime_used=_canon_regime(rg),
            )

        # основная попытка: запрошенный режим
        res = try_regime(req_regime)
        # fallback: na корзина, если специфичный для режима разрежен
        if res is None and self.cfg.fallback_to_na_regime and self.cfg.use_regime_dim:
            res = try_regime("na")

        self._put_cached(cache_key, res)
        return res


def _parse_csv_floats(s: Any) -> list[float]:
    if s is None:
        return []
    if isinstance(s, (list, tuple)):
        out: list[float] = []
        for x in s:
            with contextlib.suppress(Exception):
                out.append(float(x))
        return out
    if isinstance(s, str):
        out: list[float] = []
        for part in s.split(","):
            p = part.strip()
            if not p:
                continue
            with contextlib.suppress(Exception):
                out.append(float(p))
        return out
    try:
        return [float(s)]
    except Exception:
        return []


def apply_empirical_levels_to_ctx(
    ctx: Any,
    *,
    side: str,
    entry_price: float,
    atr: float,
    risk_cfg: dict[str, Any],
    emp: EmpiricalLevelsResult,
    logger: Any | None = None,
) -> bool:
    """
    Применяет эмпирические TP1/SL к контексту и перестраивает tp_levels согласованно.

    Дизайн:
      - Мы всегда устанавливаем ctx.tp1_price и ctx.sl_price из эмпирических bps.
      - Мы перестраиваем ctx.tp_levels для режима RR используя *новую дистанцию стопа*.
      - Для режима ATR мы сохраняем tp2/tp3 из существующих ctx.tp_levels, если они есть
        (режим ATR независим от SL; изменение SL иначе нарушило бы семантику).

    Fail-open:
      Любое исключение => вернуть False.
    """
    try:
        s = (side or "").strip().upper()
        is_long = s in {"LONG", "BUY"}
        entry = float(entry_price)
        if not math.isfinite(entry) or entry <= 0.0:
            return False

        tp1_bps = float(emp.tp1_bps)
        sl_bps = float(emp.sl_bps)
        if not math.isfinite(tp1_bps) or not math.isfinite(sl_bps):
            return False

        # цены из bps
        tp1 = entry * (1.0 + tp1_bps / 10_000.0) if is_long else entry * (1.0 - tp1_bps / 10_000.0)
        sl = entry * (1.0 - sl_bps / 10_000.0) if is_long else entry * (1.0 + sl_bps / 10_000.0)

        # проверка: sl должен быть "защитным", tp1 должен быть "прибыльным"
        if is_long and not (sl < entry < tp1):
            return False
        if (not is_long) and not (tp1 < entry < sl):
            return False

        stop_dist = abs(entry - sl)
        if stop_dist <= 0.0:
            return False

        tp_mode = (risk_cfg.get("TP_MODE", "RR") or "RR").strip().upper()
        rr_list = _parse_csv_floats(risk_cfg.get("TP_RR", "1,2,3"))
        atr_mults = _parse_csv_floats(risk_cfg.get("TP_ATR_MULTS", "0.6,1.0,1.5"))

        # Build tp_levels
        tp_levels: list[float] = []
        if tp_mode == "RR" and rr_list:
            # согласованные уровни RR с новым SL
            for rr in rr_list[:3]:
                try:
                    r = float(rr)
                except Exception:
                    continue
                if r <= 0:
                    continue
                lvl = entry + r * stop_dist if is_long else entry - r * stop_dist
                tp_levels.append(float(lvl))
            if not tp_levels:
                tp_levels = [tp1]
        elif tp_mode == "ATR" and atr_mults and math.isfinite(atr) and atr > 0.0:
            # ATR-mode: уровни TP независимы от SL; сохраняем согласованность с ATR.
            # Всё ещё принудительно ставим tp1 на эмпирический (более мощно для качества).
            # Для tp2/tp3 мы либо используем существующие ctx.tp_levels (если есть),
            # либо вычисляем из мультипликаторов ATR.
            tp_levels = [tp1]
            # предпочитаем существующие базовые tp для 2/3, чтобы избежать неожиданных скачков
            base = getattr(ctx, "tp_levels", None)
            if isinstance(base, list) and len(base) >= 3:
                with contextlib.suppress(Exception):
                    tp_levels.extend([float(base[1]), float(base[2])])
            if len(tp_levels) < 3:
                for m in atr_mults[1:3]:
                    try:
                        mm = float(m)
                    except Exception:
                        continue
                    if mm <= 0:
                        continue
                    move = atr * mm
                    lvl = entry + move if is_long else entry - move
                    tp_levels.append(float(lvl))
        else:
            # неизвестный режим -> как минимум применить tp1
            tp_levels = [tp1]

        # записать поля (тот же контракт, что и level_enricher)
        ctx.entry_price = entry
        ctx.sl_price = sl
        ctx.tp1_price = float(tp_levels[0])
        ctx.tp_levels = [float(x) for x in tp_levels]
        ctx.stop_dist = stop_dist
        # aliases
        ctx.entry = entry
        ctx.sl = sl
        ctx.tp1 = float(tp_levels[0])

        # диагностика: какая эмпирическая статистика использовалась
        ctx.emp_levels_used = True
        ctx.emp_tp1_bps = float(tp1_bps)
        ctx.emp_sl_bps = float(sl_bps)
        ctx.emp_ttd_ms = int(emp.ttd_ms)
        ctx.emp_regime_used = str(emp.regime_used)
        ctx.emp_n_mfe = int(emp.n_mfe)
        ctx.emp_n_mae = int(emp.n_mae)
        ctx.emp_n_ttd = int(emp.n_ttd)

        if logger is not None:
            with contextlib.suppress(Exception):
                logger.debug(
                    "emp_levels applied: side=%s entry=%.8f sl=%.8f tp1=%.8f tp_mode=%s "
                    "mfe_q=%.2f sl_q=%.2f ttd_q=%.2f n(mfe=%d,mae=%d,ttd=%d) regime=%s",
                    s,
                    entry,
                    sl,
                    float(tp_levels[0]),
                    tp_mode,
                    float(tp1_bps),
                    float(sl_bps),
                    float(emp.ttd_ms),
                    int(emp.n_mfe),
                    int(emp.n_mae),
                    int(emp.n_ttd),
                    str(emp.regime_used),
                )

        return True
    except Exception:
        return False
