from __future__ import annotations
from core.redis_keys import RedisStreams as RS

"""
ML Confirm Config Watchdog.

Периодически проверяет наличие cfg:ml_confirm:champion в Redis.
Если ключ отсутствует или содержит невалидный JSON — автоматически
восстанавливает конфигурацию через init_ml_confirm_on_startup.ensure_ml_confirm_config().

ENV:
    REDIS_URL                   — Redis URL (default: redis://redis-worker-1:6379/0)
    ML_CONFIRM_MODE             — если OFF, пропускаем (default: SHADOW)
    ML_CFG_CHAMPION_KEY         — ключ champion (default: cfg:ml_confirm:champion)
    NOTIFY_TELEGRAM_STREAM      — стрим Telegram (default: notify:telegram)
    ML_WATCHDOG_INTERVAL_SEC    — интервал опроса сек (default: 300)
    ML_WATCHDOG_DRY_RUN         — если 1, не пишем в Redis (default: 0)

Метрика:
    при каждом restore пишем XADD notify:telegram предупреждение.
"""

import json
import logging
import os
import sys
import time

import redis

from utils.time_utils import get_ny_time_millis

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [ml_cfg_watchdog] %(message)s",
)
log = logging.getLogger("ml_confirm_cfg_watchdog")


def _now_ms() -> int:
    return get_ny_time_millis()


def _notify(r: redis.Redis, text: str) -> None:
    stream = os.getenv("NOTIFY_TELEGRAM_STREAM", RS.NOTIFY_TELEGRAM)
    try:
        r.xadd(
            stream,
            {"type": "report", "text": text, "ts": str(_now_ms())},
            maxlen=200_000,
            approximate=True,
        )
    except Exception as exc:
        log.warning("Failed to send Telegram notification: %s", exc)


def _check_champion_key(r: redis.Redis, champion_key: str) -> bool:
    """Return True if champion key exists and contains valid non-empty JSON dict."""
    try:
        raw = r.get(champion_key)
        if not raw:
            return False
        d = json.loads(raw)
        return isinstance(d, dict) and bool(d)
    except Exception:
        return False


def _wait_for_redis(url: str, max_wait_sec: int = 120) -> redis.Redis:
    """Create Redis connection, retrying until ready (handles BusyLoadingError)."""
    deadline = time.monotonic() + max_wait_sec
    attempt = 0
    while True:
        attempt += 1
        try:
            r = redis.Redis.from_url(
                url, decode_responses=True,
                socket_connect_timeout=5, socket_timeout=5,
            )
            r.ping()
            return r
        except redis.exceptions.BusyLoadingError:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise
            wait = min(5.0, remaining)
            log.warning("Redis loading dataset (attempt %d), retry in %.0fs…", attempt, wait)
            time.sleep(wait)
        except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError) as exc:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise
            wait = min(5.0, remaining)
            log.warning("Redis not ready (%s, attempt %d), retry in %.0fs…", exc, attempt, wait)
            time.sleep(wait)


def run_watchdog_cycle(
    r: redis.Redis,
    *,
    champion_key: str,
    mode: str,
    dry_run: bool = False,
) -> bool | None:
    """
    Run one watchdog cycle.

    Returns:
        True  — key was absent and restore succeeded
        False — key was absent but restore failed (no model found)
        None  — key present and valid (no action needed)
    """
    if mode.upper() == "OFF":
        log.debug("ML_CONFIRM_MODE=OFF, skipping watchdog check.")
        return None

    if _check_champion_key(r, champion_key):
        log.debug("Champion key %s is present and valid. OK.", champion_key)
        return None

    log.warning(
        "Champion key %s is MISSING or invalid! Attempting auto-restore…",
        champion_key,
    )

    if dry_run:
        log.info("DRY_RUN mode — skipping actual restore.")
        return None

    # Import and call the init function
    try:
        # Add python-worker to path if needed (works both in-container and in tests)
        sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        from tools.init_ml_confirm_on_startup import ensure_ml_confirm_config  # noqa: PLC0415
    except ImportError:
        log.error("Cannot import init_ml_confirm_on_startup — check PYTHONPATH.")
        return False

    try:
        success = ensure_ml_confirm_config()
    except Exception as exc:
        log.error("ensure_ml_confirm_config raised: %s", exc)
        success = False

    if success:
        log.info("Auto-restore succeeded for champion key %s", champion_key)
        try:
            raw = r.get(champion_key)
            d = json.loads(raw or "{}")
            model_path = d.get("model_path", "?")
            kind = d.get("kind", "?")
        except Exception:
            model_path = "?"
            kind = "?"
        _notify(
            r,
            f"⚠️ <b>ML CFG WATCHDOG: key restored</b>\n"
            f"<code>{champion_key}</code> was missing — auto-restored.\n"
            f"kind=<code>{kind}</code> model=<code>{model_path}</code>\n"
            f"<i>Check why the key disappeared (Redis restart / flush?).</i>",
        )
    else:
        log.error(
            "Auto-restore FAILED for %s — no model found on disk? "
            "Check /var/lib/trade/ml_models/ and /var/lib/trade/of_reports/models/",
            champion_key,
        )
        _notify(
            r,
            f"🚨 <b>ML CFG WATCHDOG: restore FAILED</b>\n"
            f"<code>{champion_key}</code> is missing and no model found on disk.\n"
            f"Manual intervention required!",
        )

    return success


def main() -> None:
    redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
    mode = os.getenv("ML_CONFIRM_MODE", "SHADOW")
    champion_key = os.getenv("ML_CFG_CHAMPION_KEY", "cfg:ml_confirm:champion")
    interval_sec = int(os.getenv("ML_WATCHDOG_INTERVAL_SEC", "300") or 300)
    dry_run = os.getenv("ML_WATCHDOG_DRY_RUN", "0") == "1"

    log.info(
        "Starting ml_confirm_cfg_watchdog: mode=%s key=%s interval=%ds dry_run=%s",
        mode, champion_key, interval_sec, dry_run,
    )

    r = _wait_for_redis(redis_url)

    while True:
        try:
            run_watchdog_cycle(r, champion_key=champion_key, mode=mode, dry_run=dry_run)
        except Exception as exc:
            log.error("Unexpected error in watchdog cycle: %s", exc, exc_info=True)

        log.debug("Sleeping %ds until next check…", interval_sec)
        time.sleep(interval_sec)


if __name__ == "__main__":
    main()
