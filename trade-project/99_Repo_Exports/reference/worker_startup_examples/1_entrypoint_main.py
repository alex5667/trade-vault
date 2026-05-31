"""
Главная точка входа python-worker: инициализация, ожидание Redis,
запуск обработчиков и graceful shutdown.
"""

import asyncio
import logging
import os
import socket
import threading
import time
import traceback as _traceback
import warnings

from prometheus_client import start_http_server

from core.app_init import print_startup_banner, print_startup_message, print_shutdown_message
from core.redis_utils import wait_for_redis
from core.ws_manager import WebSocketManager
from handlers.signal_processor import SignalProcessor
from tools.init_ml_confirm_on_startup import ensure_ml_confirm_config

# Suppress annoying NVIDIA driver warnings when running on CPU nodes.
warnings.filterwarnings("ignore", message=".*NVIDIA Driver not detected.*")
warnings.filterwarnings("ignore", message=".*CUDA initialization.*")

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Process-level constants (read once at import time)
# ---------------------------------------------------------------------------
_NOTIFY_STREAM: str = os.getenv("NOTIFY_STREAM", "notify:telegram")
_HOSTNAME: str = socket.gethostname()
_OF_ALERT_ENABLE: bool = (
    os.getenv("OF_CRASH_ALERT_ENABLE", "1").strip().lower() in ("1", "true", "yes", "on")
)


def _notify_telegram_sync(text: str) -> None:
    """Best-effort: отправляет сообщение в notify:telegram через sync Redis.

    Никогда не бросает исключение — это watchdog, он не должен ломать процесс.
    """
    if not _OF_ALERT_ENABLE:
        return
    try:
        import redis as _redis_sync
        r = _redis_sync.from_url(
            os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0"),
            socket_connect_timeout=3,
            socket_timeout=3,
        )
        r.xadd(_NOTIFY_STREAM, {"text": text}, maxlen=5000)
        r.close()
    except Exception as exc:  # noqa: BLE001
        print(f"[watchdog] Failed to send Telegram alert: {exc}", flush=True)


def run_orderflow_service() -> None:
    """Runs CryptoOrderflowService in a separate asyncio loop."""
    print("🚀 [Main] Starting OrderFlowService thread...", flush=True)
    start_ts = time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime())
    loop = None
    try:
        from services.crypto_orderflow_service import CryptoOrderflowService  # noqa: PLC0415
        print("✅ [Main] CryptoOrderflowService imported", flush=True)

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
        ticks_url = os.getenv("REDIS_TICKS_URL") or os.getenv("REDIS_URL_TICKS")

        service = CryptoOrderflowService(redis_dsn=redis_url, ticks_dsn=ticks_url)
        print("🚀 [Main] Running CryptoOrderflowService.run_forever()...", flush=True)
        loop.run_until_complete(service.run_forever())

        _notify_telegram_sync(
            f"⚠️ *CryptoOrderflowService остановлен*\n"
            f"🖥 Host: `{_HOSTNAME}`\n"
            f"🕐 Старт: {start_ts}\n"
            f"🕑 Стоп: {time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime())}\n"
            f"ℹ️ Причина: нормальная остановка (run\\_forever завершён)"
        )

    except Exception as exc:  # noqa: BLE001
        tb = _traceback.format_exc()
        short_err = str(exc)[:200]
        print(f"❌ [Main] CryptoOrderflowService failed: {exc}", flush=True)
        print(tb, flush=True)
        _notify_telegram_sync(
            f"🚨 *CryptoOrderflowService УПАЛ* 🚨\n"
            f"🖥 Host: `{_HOSTNAME}`\n"
            f"🕐 Старт: {start_ts}\n"
            f"🕑 Сбой: {time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime())}\n"
            f"❌ Ошибка: `{short_err}`\n"
            f"⚠️ Сигналы и виртуальные сделки *не создаются*!"
        )
    finally:
        print("🛑 [Main] CryptoOrderflowService thread finished", flush=True)
        if loop is not None:
            try:
                loop.close()
            except Exception:  # noqa: BLE001
                pass


def _of_watchdog(of_thread: threading.Thread, check_interval_sec: int = 60) -> None:
    """Watchdog-поток: проверяет, жив ли of_thread.

    Если умер — шлёт алерт в Telegram. Намеренно не перезапускает поток —
    для авторестарта используй Docker restart policy.
    """
    time.sleep(30)  # Дать сервису время на нормальный старт.
    alerted = False
    while True:
        time.sleep(check_interval_sec)
        if not of_thread.is_alive():
            if not alerted:
                _notify_telegram_sync(
                    f"🔴 *[WATCHDOG] OrderFlow поток мёртв* 🔴\n"
                    f"🖥 Host: `{_HOSTNAME}`\n"
                    f"🕐 Время: {time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime())}\n"
                    f"⚠️ Сигналы и сделки *не создаются*!\n"
                    f"🔄 Перезапусти контейнер: `docker restart scanner-python-worker`"
                )
                alerted = True
        else:
            alerted = False  # Вернулся — сброс флага.


def main() -> None:
    """Главная функция приложения."""
    metrics_port = int(os.getenv("METRICS_PORT", "8000"))
    start_http_server(metrics_port)

    print_startup_banner()
    print_startup_message()
    wait_for_redis()
    ensure_ml_confirm_config()

    # Start CryptoOrderflowService in background thread.
    of_thread = threading.Thread(
        target=run_orderflow_service,
        name="OrderFlowService",
        daemon=True,
    )
    of_thread.start()

    # Watchdog: следит за of_thread и шлёт алерт если упал.
    watchdog_interval = int(os.getenv("OF_WATCHDOG_INTERVAL_SEC", "60"))
    wd_thread = threading.Thread(
        target=_of_watchdog,
        args=(of_thread, watchdog_interval),
        name="OrderFlowWatchdog",
        daemon=True,
    )
    wd_thread.start()

    ws_manager = WebSocketManager()
    signal_processor = SignalProcessor(ws_manager.update_ws_connections)
    signal_processor.start_all()

    try:
        signal_processor.wait_forever()
    finally:
        print_shutdown_message()


if __name__ == "__main__":
    main()
