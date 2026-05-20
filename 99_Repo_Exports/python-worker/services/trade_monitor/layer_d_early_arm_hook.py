from __future__ import annotations

"""layer_d_early_arm_hook.py

Layer D production hook — inline на каждом тике после обновления MFE.

Контракт:
  Hook(pos, ts_ms, redis_client) → может XADD одну запись в
    trail:arm:requests stream при выполнении условий early-arm:
      1. pos.trailing_started == False (idempotency)
      2. pos.entry_price > 0 и pos.max_favorable_price корректен
      3. mfe_R >= ARM_THRESHOLD (default 0.5)
      4. Symbol в allowlist (canary_symbols из of_gate:layer_d:*)
      5. Mode != off (читается из Redis + ENV)
      6. HMAC bundle верифицируется

Минимальный production-risk:
  - Default OFF (LAYER_D_EARLY_ARM_MODE=off).
  - Любая ошибка не прерывает on_tick (логирует и возвращается).
  - В режиме shadow: не пишет в stream, только метрика+лог.
  - В режиме enforce: XADD в trail:arm:requests (один раз per position).
  - Per-position idempotency: tracks `pos._layer_d_arm_sent`.

Реальный pickup arm-event'а — в tp_hit_trailing_orchestrator (отдельный PR).
"""

import hashlib
import hmac
import json
import logging
import os
import time
from typing import Any

logger = logging.getLogger(__name__)

# Prometheus метрики — lazy init чтобы избежать дублирующих регистраций.
_metrics_ready = False
g_arm_fired = None  # type: ignore
g_arm_blocked = None  # type: ignore
g_mfe_observed = None  # type: ignore
c_errors = None  # type: ignore


def _init_metrics() -> None:
    global _metrics_ready, g_arm_fired, g_arm_blocked, g_mfe_observed, c_errors
    if _metrics_ready:
        return
    try:
        from prometheus_client import Counter, Gauge  # type: ignore
        g_arm_fired = Counter("layer_d_arm_fired_total",
                              "Early-arm requests emitted", ["mode", "symbol"])
        g_arm_blocked = Counter("layer_d_arm_blocked_total",
                                "Hook calls that didn't fire", ["reason"])
        g_mfe_observed = Gauge("layer_d_mfe_r_last", "Last observed mfe_R", ["symbol"])
        c_errors = Counter("layer_d_arm_errors_total", "errors", ["where"])
        _metrics_ready = True
    except Exception:
        _metrics_ready = True  # avoid retry loop


def _env(k: str, d: str = "") -> str:
    return os.environ.get(k, d)


def _env_f(k: str, d: float) -> float:
    try:
        return float(_env(k, str(d)))
    except Exception:
        return d


# ------------------------------------------------------------------------------
# Layer D state per scanner-python-worker — cached config from Redis (TTL).
# ------------------------------------------------------------------------------
class _Config:
    """Кешированная конфигурация Layer D, читается из Redis с TTL."""
    def __init__(self) -> None:
        self.mode: str = "off"
        self.canary_symbols: set[str] = set()
        self.arm_threshold_r: float = _env_f("OF_LAYER_D_ARM_THRESHOLD_R", 0.25)
        self.cache_ts: float = 0.0
        self.ttl: float = _env_f("LAYER_D_CONFIG_CACHE_TTL_SEC", 5.0)

    def maybe_refresh(self, redis_client: Any) -> None:
        now = time.monotonic()
        if (now - self.cache_ts) < self.ttl:
            return
        self.cache_ts = now

        # ENV default mode
        env_mode = (_env("OF_LAYER_D_EARLY_ARM_MODE", "off") or "off").lower().strip()
        if env_mode not in ("off", "shadow", "enforce"):
            env_mode = "off"

        # Redis hot-override + canary symbols + HMAC verify
        if redis_client is None:
            self.mode = env_mode
            self.canary_symbols = set()
            return

        prefix = _env("LAYER_D_GATES_KEY_PREFIX", "of_gate")
        secret = _env("LAYER_D_HMAC_SECRET", "") \
                 or _env("LAYERS_CAL_HMAC_SECRET", "") \
                 or _env("RECS_HMAC_SECRET", "")

        try:
            r_mode = redis_client.get(f"{prefix}:layer_d:mode") or ""
            symbols = redis_client.get(f"{prefix}:layer_d:canary_symbols") or ""
            bundle = redis_client.get(f"{prefix}:layer_d:bundle") or ""
            sig = redis_client.get(f"{prefix}:layer_d:bundle_sig") or ""
            mode_override = redis_client.get(f"{prefix}:enforce_mode_override") or ""
        except Exception:
            r_mode = symbols = bundle = sig = mode_override = ""

        # HMAC verify (требуется только для canary/prod)
        r_mode_s = str(r_mode).lower().strip()
        if r_mode_s in ("canary", "prod") and bundle and sig and secret:
            try:
                rec = json.loads(bundle)
                canon = json.dumps(rec, sort_keys=True, separators=(",", ":")).encode()
                expected = hmac.new(secret.encode(), canon, hashlib.sha256).hexdigest()
                if not hmac.compare_digest(expected, str(sig)):
                    logger.warning("layer_d: HMAC invalid → downgrade to off")
                    r_mode_s = "off"
            except Exception:
                r_mode_s = "off"
        elif r_mode_s in ("canary", "prod") and not (bundle and sig):
            r_mode_s = "off"

        # mode_override (enforce_mode_override) — глобальный hot override
        # (применяется и к Layer A/B/C, но Layer D отдельно canary-gated).
        # Здесь используем only если layer_d уже в canary/prod.
        if r_mode_s in ("canary", "prod"):
            override = str(mode_override).lower().strip()
            if override in ("off", "shadow", "enforce"):
                # Внешний override может только "off" или "enforce" Layer D.
                # "shadow" не применим — Layer D отдельно canary-gated.
                if override == "off":
                    r_mode_s = "off"

        # Effective mode:
        # - layer_d Redis = off → no-op
        # - layer_d Redis = canary/prod → используем ENV-mode (off/shadow/enforce)
        if r_mode_s == "off":
            self.mode = "off"
        else:
            self.mode = env_mode

        self.canary_symbols = {s.strip().upper()
                               for s in str(symbols).split(",") if s.strip()}
        self.arm_threshold_r = _env_f("OF_LAYER_D_ARM_THRESHOLD_R", 0.25)


_CFG = _Config()


def _sign_request(payload: dict[str, Any], secret: str) -> str:
    blob = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    return hmac.new(secret.encode(), blob, hashlib.sha256).hexdigest()


def evaluate_and_emit(pos: Any, ts_ms: int, redis_client: Any) -> bool:
    """Main entry point. Возвращает True если был emit arm-request.

    Идемпотентно: per-position attribute `_layer_d_arm_sent` гарантирует
    что arm-request отправляется не более 1 раза за жизнь позиции.

    Safe-by-default: любая ошибка → False (никогда не roll on_tick).
    """
    _init_metrics()
    try:
        _CFG.maybe_refresh(redis_client)
        mode = _CFG.mode

        if mode == "off":
            return False

        # Идемпотентность
        if getattr(pos, "_layer_d_arm_sent", False):
            if g_arm_blocked is not None:
                g_arm_blocked.labels(reason="already_sent").inc()
            return False
        # Не делать ничего, если трейлинг уже армирован (TP1 или другим путём).
        if getattr(pos, "trailing_started", False) or getattr(pos, "trailing_active", False):
            if g_arm_blocked is not None:
                g_arm_blocked.labels(reason="already_armed").inc()
            return False

        symbol = str(getattr(pos, "symbol", "") or "").upper()
        if not symbol:
            return False

        # Canary allowlist (если непустой)
        if _CFG.canary_symbols and symbol not in _CFG.canary_symbols:
            if g_arm_blocked is not None:
                g_arm_blocked.labels(reason="symbol_not_canary").inc()
            return False

        entry_price = float(getattr(pos, "entry_price", 0.0) or 0.0)
        if entry_price <= 0:
            return False

        peak_price = float(getattr(pos, "max_favorable_price", 0.0) or 0.0)
        if peak_price <= 0:
            return False

        direction = str(getattr(pos, "direction",
                                getattr(pos, "side", "")) or "").upper()

        # mfe in bps (sign-aware)
        if direction == "LONG":
            mfe_bps = (peak_price - entry_price) / entry_price * 10_000.0
        elif direction == "SHORT":
            mfe_bps = (entry_price - peak_price) / entry_price * 10_000.0
        else:
            return False

        # one_R in bps: попробуем взять из pos. Иначе считаем по SL-distance.
        one_r_bps = float(getattr(pos, "one_r_bps", 0.0) or 0.0)
        if one_r_bps <= 0:
            sl_price = float(getattr(pos, "sl_price", 0.0) or 0.0)
            if sl_price > 0:
                if direction == "LONG":
                    one_r_bps = (entry_price - sl_price) / entry_price * 10_000.0
                else:
                    one_r_bps = (sl_price - entry_price) / entry_price * 10_000.0
        if one_r_bps <= 0:
            if g_arm_blocked is not None:
                g_arm_blocked.labels(reason="no_one_r").inc()
            return False

        mfe_r = mfe_bps / one_r_bps
        if g_mfe_observed is not None:
            g_mfe_observed.labels(symbol=symbol).set(mfe_r)

        if mfe_r < _CFG.arm_threshold_r:
            if g_arm_blocked is not None:
                g_arm_blocked.labels(reason="below_threshold").inc()
            return False

        # Шлём arm-request
        signal_id = str(getattr(pos, "signal_id", "")
                        or getattr(pos, "sid", "")
                        or getattr(pos, "pos_id", "") or "")
        if not signal_id:
            if g_arm_blocked is not None:
                g_arm_blocked.labels(reason="no_signal_id").inc()
            return False

        payload = {
            "signal_id": signal_id,
            "symbol": symbol,
            "side": direction,
            "mfe_r": round(mfe_r, 4),
            "mfe_bps": round(mfe_bps, 2),
            "one_r_bps": round(one_r_bps, 2),
            "ts_ms": int(ts_ms),
            "source": "MFE_EARLY_ARM",
            "arm_threshold_r": _CFG.arm_threshold_r,
        }

        secret = _env("LAYER_D_HMAC_SECRET", "") \
                 or _env("LAYERS_CAL_HMAC_SECRET", "") \
                 or _env("RECS_HMAC_SECRET", "") or "CHANGE_ME"
        sig = _sign_request(payload, secret)

        if mode == "shadow":
            logger.info(
                "[LAYER-D SHADOW] would arm sid=%s symbol=%s mfe_R=%.3f",
                signal_id, symbol, mfe_r,
            )
            try:
                setattr(pos, "_layer_d_arm_sent", True)  # idempotency even in shadow
            except Exception:
                pass
            if g_arm_fired is not None:
                g_arm_fired.labels(mode="shadow", symbol=symbol).inc()
            return False  # shadow не блокирует и не отправляет

        # enforce: XADD в trail:arm:requests
        try:
            redis_client.xadd(
                _env("LAYER_D_ARM_STREAM", "trail:arm:requests"),
                {
                    "payload": json.dumps(payload, sort_keys=True),
                    "sig": sig,
                },
                maxlen=10_000,
                approximate=True,
            )
            try:
                setattr(pos, "_layer_d_arm_sent", True)
            except Exception:
                pass
            if g_arm_fired is not None:
                g_arm_fired.labels(mode="enforce", symbol=symbol).inc()
            logger.info(
                "[LAYER-D ENFORCE] arm-request emitted sid=%s symbol=%s mfe_R=%.3f",
                signal_id, symbol, mfe_r,
            )
            return True
        except Exception as ex:
            logger.warning(f"layer_d xadd failed: {ex}")
            if c_errors is not None:
                c_errors.labels(where="xadd").inc()
            return False

    except Exception as ex:
        logger.debug(f"layer_d hook error: {ex}")
        if c_errors is not None:
            c_errors.labels(where="hook").inc()
        return False
