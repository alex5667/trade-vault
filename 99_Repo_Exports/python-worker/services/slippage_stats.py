from __future__ import annotations

import math
import os
import time
from dataclasses import dataclass
from typing import Any


def _env_bool(name: str, default: bool) -> bool:
    v = (os.getenv(name, "1" if default else "0") or "").strip().lower()
    return v in {"1", "true", "yes", "on"}


def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        f = float(x)
        return f if math.isfinite(f) else default
    except Exception:
        return default


def _canon_tf(x: Any) -> str:
    """
    Минимальная нормализация tf (без зависимости от canon_tf),
    чтобы ключи EMA были стабильными.
    """
    try:
        s = (x or "").strip().lower()
        return s if s else "na"
    except Exception:
        return "na"


def _canon(s: Any) -> str:
    try:
        v = (s or "").strip().lower()
        return v if v else "na"
    except Exception:
        return "na"


def session_from_ts_ms(ts_ms: int) -> str:
    """
    Простая (и самодостаточная) классификация "сессии" по часу UTC.
    Это deliberately lightweight: достаточно для ключа EMA.

    Если у вас уже есть session_service.py и вы хотите полное соответствие — скажите,
    я переподключу сюда вашу функцию без изменения протокола ключей.
    """
    try:
        ts = int(ts_ms)
        if ts <= 0:
            return "na"
        h = int(time.gmtime(ts / 1000.0).tm_hour)
        # Грубые окна:
        if 0 <= h < 7:
            return "asian"
        if 7 <= h < 13:
            return "europe"
        if 13 <= h < 20:
            return "us"
        return "overnight"
    except Exception:
        return "na"


@dataclass(frozen=True)
class SlippageEmaConfig:
    enabled: bool
    alpha: float
    ttl_sec: int
    min_samples: int
    key_prefix: str
    # ---------------------------------------------------------------------
    # NEW: optional "kind" dimension.
    #
    # Goal:
    #   EMA keys can be separated per signal kind (execution differs by kind).
    #
    # Back-compat rule (critical):
    #   - The BASE key format (without kind suffix) is preserved.
    #   - We append ":{kind}" ONLY when kind is provided AND kind != "na".
    #
    # This means:
    #   * Existing stored keys keep working as-is.
    #   * Rollout can be incremental: only some kinds start writing suffix keys.
    # ---------------------------------------------------------------------
    use_kind_dim: bool = True

    @staticmethod
    def from_env() -> SlippageEmaConfig:
        enabled = _env_bool("SLIPPAGE_EMA_ENABLED", True)
        try:
            alpha = float(os.getenv("SLIPPAGE_EMA_ALPHA", "0.05"))
        except Exception:
            alpha = 0.05
        alpha = max(0.001, min(0.5, alpha))  # защитный диапазон
        try:
            ttl_sec = int(os.getenv("SLIPPAGE_EMA_TTL_SEC", str(60 * 60 * 24 * 30)))
        except Exception:
            ttl_sec = 60 * 60 * 24 * 30
        ttl_sec = max(0, ttl_sec)
        try:
            min_samples = int(os.getenv("SLIPPAGE_EMA_MIN_SAMPLES", "20"))
        except Exception:
            min_samples = 20
        min_samples = max(0, min_samples)
        key_prefix = (os.getenv("SLIPPAGE_EMA_KEY_PREFIX", "slipema:") or "slipema:")
        use_kind_dim = _env_bool("SLIPPAGE_EMA_USE_KIND_DIM", True)
        return SlippageEmaConfig(
            enabled=enabled,
            alpha=float(alpha),
            ttl_sec=int(ttl_sec),
            min_samples=int(min_samples),
            key_prefix=key_prefix,
            use_kind_dim=bool(use_kind_dim),
        )


def _key(
    cfg: SlippageEmaConfig,
    *,
    symbol: str,
    venue: str,
    session: str,
    tf: str,
    kind: str | None = None,
) -> str:
    """
    Key format:
      BASE:  slipema:{symbol}:{venue}:{session}:{tf}
      KIND:  slipema:{symbol}:{venue}:{session}:{tf}:{kind}   (ONLY if kind != "na")

    Why conditional suffix (kind != "na"):
      - Preserves existing stored keys (no migration needed).
      - Allows gradual rollout: only selected kinds start writing suffix keys.
      - If kind missing -> we read/write BASE (legacy format).
    """
    base = f"{cfg.key_prefix}{_canon(symbol)}:{_canon(venue)}:{_canon(session)}:{_canon_tf(tf)}"
    if not cfg.use_kind_dim:
        return base
    kk = _canon(kind)
    if kk and kk != "na":
        return f"{base}:{kk}"
    return base


def update_slippage_ema(
    redis_client: Any,
    *,
    cfg: SlippageEmaConfig,
    symbol: str,
    venue: str,
    session: str,
    tf: str,
    kind: str | None = None,
    now_ms: int,
    realized_slippage_bps: float,
    realized_spread_bps: float = 0.0,
) -> None:
    """
    Пишем EMA в Redis:
      key: slipema:{symbol}:{venue}:{session}
      hash fields:
        - samples
        - ema_slippage_bps
        - ema_spread_bps
        - last_ts_ms

    Важно:
      - значения должны быть >=0 и finite
      - fail-open: любые ошибки => просто пропускаем
    """
    if not cfg.enabled or redis_client is None:
        return

    x = _safe_float(realized_slippage_bps, 0.0)
    if x <= 0:
        return
    sp = _safe_float(realized_spread_bps, 0.0)
    if sp < 0:
        sp = 0.0

    k = _key(cfg, symbol=symbol, venue=venue, session=session, tf=tf, kind=kind)

    try:
        # читаем текущее состояние
        h = redis_client.hgetall(k) or {}
        # hgetall может вернуть bytes — приводим
        def _getf(name: str, default: float) -> float:
            v = h.get(name)
            if isinstance(v, (bytes, bytearray)):
                v = v.decode("utf-8", "ignore")
            try:
                f = float(v)  # type: ignore
                return f if math.isfinite(f) else default
            except Exception:
                return default

        def _geti(name: str, default: int) -> int:
            v = h.get(name)
            if isinstance(v, (bytes, bytearray)):
                v = v.decode("utf-8", "ignore")
            try:
                return int(float(v))  # type: ignore
            except Exception:
                return default

        samples = _geti("samples", 0)
        ema = _getf("ema_slippage_bps", 0.0)
        ema_sp = _getf("ema_spread_bps", 0.0)

        a = float(cfg.alpha)
        ema_new = x if samples <= 0 or ema <= 0 else ((1.0 - a) * ema + a * x)
        ema_sp_new = sp if samples <= 0 or ema_sp <= 0 else ((1.0 - a) * ema_sp + a * sp) if sp > 0 else ema_sp

        pipe = redis_client.pipeline(transaction=False)
        pipe.hset(k, mapping={
            "samples": int(samples + 1),
            "ema_slippage_bps": float(ema_new),
            "ema_spread_bps": float(ema_sp_new),
            "last_ts_ms": int(now_ms),
        })
        if cfg.ttl_sec > 0:
            pipe.expire(k, int(cfg.ttl_sec))
        pipe.execute()
    except Exception:
        return


def read_slippage_ema(
    redis_client: Any,
    *,
    cfg: SlippageEmaConfig,
    symbol: str,
    venue: str,
    session: str,
    tf: str,
    kind: str | None = None,
) -> dict[str, float] | None:
    """
    Чтение EMA (для gate).
    Возвращает None, если нет данных/ошибка/недостаточно сэмплов.
    """
    if not cfg.enabled or redis_client is None:
        return None
    # If kind is provided (and kind != "na"), we try kind-specific key first.
    # If it doesn't exist, we fall back to BASE key (legacy) automatically
    # by recomputing with kind=None.
    k = _key(cfg, symbol=symbol, venue=venue, session=session, tf=tf, kind=kind)
    try:
        h = redis_client.hgetall(k) or {}
        if not h and kind is not None:
            # fallback to legacy base key
            k2 = _key(cfg, symbol=symbol, venue=venue, session=session, tf=tf, kind=None)
            if k2 != k:
                h = redis_client.hgetall(k2) or {}
        def _to_str(v: Any) -> str:
            if isinstance(v, (bytes, bytearray)):
                return v.decode("utf-8", "ignore")
            return str(v)
        samples = int(float(_to_str(h.get("samples") or "0")))
        if cfg.min_samples > 0 and samples < cfg.min_samples:
            return None
        ema = float(_to_str(h.get("ema_slippage_bps") or "0"))
        ema_sp = float(_to_str(h.get("ema_spread_bps") or "0"))
        if not math.isfinite(ema) or ema <= 0:
            return None
        if not math.isfinite(ema_sp) or ema_sp < 0:
            ema_sp = 0.0
        return {"ema_slippage_bps": float(ema), "ema_spread_bps": float(ema_sp), "samples": float(samples)}
    except Exception:
        return None
