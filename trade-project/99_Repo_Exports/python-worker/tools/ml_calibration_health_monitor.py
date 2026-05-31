from __future__ import annotations

import json
import os
from typing import Any

import redis

from utils.time_utils import get_ny_time_millis
from core.redis_keys import RedisStreams as RS


def now_ms() -> int:
    return get_ny_time_millis()


def _safe_loads(s: Any) -> dict[str, Any]:
    """
    Safe JSON loads with fallback to empty dict.
    """
    try:
        return json.loads(s) if s else {}
    except Exception:
        return {}


def _f(x: Any, d: float = 0.0) -> float:
    """
    Safe float conversion with default.
    """
    try:
        return float(x)
    except Exception:
        return d


def main() -> None:
    """
    ML Calibration Health Monitor
    
    Reads champion cfg from Redis, checks calibration metrics (ECE/Brier/LogLoss),
    and sends alert to Telegram if thresholds are exceeded.
    
    ENV:
        REDIS_URL: Redis connection URL
        ML_CFG_CHAMPION_KEY: Redis key for champion config (default: cfg:ml_confirm:champion)
        NOTIFY_TELEGRAM_STREAM: Redis stream for Telegram notifications (default: notify:telegram)
        ML_CALIB_ECE_MAX: Maximum allowed ECE (default: 0.06)
        ML_CALIB_BRIER_MAX: Maximum allowed Brier score (default: 0.22)
        ML_CALIB_LOGLOSS_MAX: Maximum allowed LogLoss (default: 0.65)
    """
    redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
    champion_key = os.getenv("ML_CFG_CHAMPION_KEY", "cfg:ml_confirm:champion")
    notify_stream = os.getenv("NOTIFY_TELEGRAM_STREAM", RS.NOTIFY_TELEGRAM)

    ece_max = float(os.getenv("ML_CALIB_ECE_MAX", "0.06") or 0.06)
    brier_max = float(os.getenv("ML_CALIB_BRIER_MAX", "0.22") or 0.22)
    logloss_max = float(os.getenv("ML_CALIB_LOGLOSS_MAX", "0.65") or 0.65)

    r = redis.Redis.from_url(redis_url, decode_responses=True)
    cfg = _safe_loads(r.get(champion_key))
    mets = cfg.get("calibration_metrics", {}) if isinstance(cfg.get("calibration_metrics", {}), dict) else {}

    ece = _f(mets.get("ece_cal", mets.get("ece_raw", 0.0)), 0.0)
    brier = _f(mets.get("brier_cal", mets.get("brier_raw", 0.0)), 0.0)
    ll = _f(mets.get("logloss_cal", mets.get("logloss_raw", 0.0)), 0.0)

    alerts = []
    if ece > ece_max:
        alerts.append(f"ECE={ece:.4f} > {ece_max:.4f}")
    if brier > brier_max:
        alerts.append(f"Brier={brier:.4f} > {brier_max:.4f}")
    if ll > logloss_max:
        alerts.append(f"LogLoss={ll:.4f} > {logloss_max:.4f}")

    if not alerts:
        return

    msg = (
        "ML CALIBRATION HEALTH ALERT\n"
        f"ece_cal={ece:.4f} brier_cal={brier:.4f} logloss_cal={ll:.4f}\n"
        f"thresholds: ece<={ece_max:.4f}, brier<={brier_max:.4f}, logloss<={logloss_max:.4f}\n"
        "reasons: " + ", ".join(alerts)
    )
    import html
    msg = html.escape(msg)
    r.xadd(notify_stream, {"type": "alert", "subtype": "ml_calibration", "ts_ms": str(now_ms()), "text": msg}, maxlen=200000, approximate=True)


if __name__ == "__main__":
    main()
















