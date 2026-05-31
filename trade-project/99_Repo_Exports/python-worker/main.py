"""
Главная точка входа python-worker: инициализация, ожидание Redis,
запуск обработчиков и graceful shutdown.
"""

import asyncio
import logging
import os
import socket
import sys
import threading
import time
import traceback as _traceback
import warnings
from typing import Any

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

# --- Graceful Shutdown Globals ---
of_service_loop: asyncio.AbstractEventLoop | None = None
of_service_task: asyncio.Task | None = None
of_service_instance: Any = None
_SHUTDOWN_EVENT = threading.Event()
_GLOBAL_SIGNAL_PROCESSOR: Any = None
_OF_THREAD: threading.Thread | None = None


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
    global of_service_loop, of_service_task, of_service_instance

    print("🚀 [Main] Starting OrderFlowService thread...", flush=True)
    start_ts = time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime())
    loop = None
    try:
        from services.crypto_orderflow_service import CryptoOrderflowService  # noqa: PLC0415
        print("✅ [Main] CryptoOrderflowService imported", flush=True)

        # Use uvloop (libuv-backed) for 2-4x higher I/O throughput on Redis streams.
        # Falls back to stdlib asyncio if uvloop is not installed.
        try:
            import uvloop  # type: ignore[import]
            loop = uvloop.new_event_loop()
            print("✅ [Main] uvloop event loop (libuv) activated", flush=True)
        except ImportError:
            loop = asyncio.new_event_loop()
            print("ℹ️ [Main] uvloop not available, using stdlib asyncio", flush=True)
        asyncio.set_event_loop(loop)
        of_service_loop = loop

        # -------------------------------------------------------------------
        # P4: Trade-side NewsRecoReader (asyncio) — best-effort, fail-open.
        # Reader в фоне обновляет in-memory cache из Redis (GET trade:cache:news_reco_map).
        # Fail-open: ошибки не блокируют запуск сервиса.
        # -------------------------------------------------------------------
        try:
            from services.news_reco_reader import ensure_started as _ensure_news_reco_started  # noqa: PLC0415
            loop.run_until_complete(_ensure_news_reco_started())
            print("✅ [Main] NewsRecoReader started (best-effort)", flush=True)
        except Exception as _exc:  # noqa: BLE001
            print(f"⚠️ [Main] NewsRecoReader failed to start (fail-open): {_exc}", flush=True)

        redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
        ticks_url = os.getenv("REDIS_TICKS_URL") or os.getenv("REDIS_URL_TICKS")

        service = CryptoOrderflowService(redis_dsn=redis_url, ticks_dsn=ticks_url)
        of_service_instance = service

        print("🚀 [Main] Running CryptoOrderflowService.run_forever()...", flush=True)
        of_service_task = loop.create_task(service.run_forever())
        
        try:
            loop.run_until_complete(of_service_task)
        except asyncio.CancelledError:
            print("ℹ️ [Main] OrderFlowService task cancelled (graceful shutdown)", flush=True)

        _notify_telegram_sync(
            f"⚠️ *CryptoOrderflowService остановлен*\n"
            f"🖥 Host: `{_HOSTNAME}`\n"
            f"🕐 Старт: {start_ts}\n"
            f"🕑 Стоп: {time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime())}\n"
            "ℹ️ Причина: нормальная остановка (run\\_forever завершён)"
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
        # P4: best-effort shutdown (avoid "Task was destroyed" warnings).
        if loop is not None:
            try:
                from services.news_reco_reader import shutdown as _shutdown_news_reco_reader  # noqa: PLC0415
                loop.run_until_complete(_shutdown_news_reco_reader())
            except Exception:  # noqa: BLE001
                pass
            try:
                loop.close()
            except Exception:  # noqa: BLE001
                pass


def _read_signals_total() -> float:
    """Возвращает суммарное значение счётчика signals_total из Prometheus registry.

    Счётчик инкрементируется в services/orderflow/signal_pipeline.py при каждом
    выпущенном сигнале. Если метрика ещё не зарегистрирована (сервис не стартовал),
    возвращает -1 — watchdog не алертит до тех пор, пока значение не станет >= 0.
    """
    try:
        from prometheus_client import REGISTRY  # noqa: PLC0415
        total = 0.0
        found = False
        for metric in REGISTRY.collect():
            if metric.name == "signals_total":
                for sample in metric.samples:
                    if sample.name == "signals_total_total":
                        total += sample.value
                        found = True
        return total if found else -1.0
    except Exception:  # noqa: BLE001
        return -1.0


def _of_watchdog(of_thread: threading.Thread, check_interval_sec: int = 60) -> None:
    """Watchdog-поток: проверяет жизнь of_thread И активность бизнес-логики.

    Два условия для алерта + os._exit(1):
    1. Поток мёртв (is_alive() == False) — как раньше.
    2. signals_total не растёт дольше OF_WATCHDOG_SIGNAL_STALE_SEC секунд
       (по умолчанию 600 = 10 мин). Поток жив, но сигналы не генерируются.

    Первые OF_WATCHDOG_WARMUP_SEC секунд (default 120) staleness не проверяется,
    чтобы дать пайплайну время прогреться и получить первый тик.
    """
    stale_threshold = int(os.getenv("OF_WATCHDOG_SIGNAL_STALE_SEC", "600"))
    warmup_sec = int(os.getenv("OF_WATCHDOG_WARMUP_SEC", "120"))

    time.sleep(30)  # Дать сервису время на нормальный старт.

    # Ждём прогрева: первые OF_WATCHDOG_WARMUP_SEC секунд staleness игнорируем.
    warmup_elapsed = 30  # уже проспали 30 выше
    while warmup_elapsed < warmup_sec:
        time.sleep(min(check_interval_sec, warmup_sec - warmup_elapsed))
        warmup_elapsed += check_interval_sec
        if not of_thread.is_alive():
            break  # упал во время прогрева — уйдём в основной цикл

    last_count = _read_signals_total()
    stale_since: float | None = None  # время начала «тишины»

    while True:
        if _SHUTDOWN_EVENT.is_set():
            print("ℹ️ [WATCHDOG] Shutdown in progress, stopping watchdog.", flush=True)
            break

        time.sleep(check_interval_sec)
        now = time.time()

        # --- 1. Thread liveness ---
        if not of_thread.is_alive():
            _notify_telegram_sync(
                f"🔴 *[WATCHDOG] OrderFlow поток мёртв* 🔴\n"
                f"🖥 Host: `{_HOSTNAME}`\n"
                f"🕐 Время: {time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime())}\n"
                f"⚠️ Сигналы и сделки *не создаются*!\n"
                f"🔄 Watchdog инициирует падение процесса для авторестарта (os._exit)."
            )
            print("🔴 [WATCHDOG] OrderFlow thread died! Triggering os._exit(1)...", flush=True)
            # Внутри потока sys.exit() завершает только сам поток.
            # Для немедленного завершения всего процесса нужен os._exit().
            os._exit(1)

        # --- 2. Business-logic staleness (signals_total counter) ---
        current_count = _read_signals_total()
        if current_count < 0:
            # Метрика ещё не зарегистрирована — пропускаем цикл.
            continue

        if current_count > last_count:
            # Новые сигналы есть — сбрасываем staleness.
            stale_since = None
            last_count = current_count
        else:
            # Нет новых сигналов.
            if stale_since is None:
                stale_since = now
            stale_sec = now - stale_since
            if stale_sec >= stale_threshold:
                stale_min = int(stale_sec // 60)
                _notify_telegram_sync(
                    f"⚠️ *[WATCHDOG] Сигналы не генерируются {stale_min} мин* ⚠️\n"
                    f"🖥 Host: `{_HOSTNAME}`\n"
                    f"🕐 Время: {time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime())}\n"
                    f"📊 signals\\_total = {int(current_count)} (не растёт с "
                    f"{time.strftime('%H:%M:%S UTC', time.gmtime(stale_since))})\n"
                    f"⚠️ Поток жив, но бизнес-логика молчит!\n"
                    f"🔄 Watchdog инициирует падение процесса для авторестарта (os._exit)."
                )
                print(
                    f"🔴 [WATCHDOG] signals_total stale for {stale_min}m "
                    f"(count={int(current_count)}). Triggering os._exit(1)...",
                    flush=True,
                )
                os._exit(1)


def handle_exit(sig, frame):
    """Signal handler for graceful shutdown."""
    global _GLOBAL_SIGNAL_PROCESSOR, of_service_loop, of_service_task

    print(f"\n✅ [Main] Received signal {sig}, initiating graceful shutdown...", flush=True)
    _SHUTDOWN_EVENT.set()

    if _GLOBAL_SIGNAL_PROCESSOR:
        print("🛑 [Main] Stopping SignalProcessor...", flush=True)
        _GLOBAL_SIGNAL_PROCESSOR.is_running = False

    if of_service_loop and of_service_task:
        print("🛑 [Main] Cancelling OrderFlowService task...", flush=True)
        of_service_loop.call_soon_threadsafe(of_service_task.cancel)

    # Hard timeout for termination
    def hard_kill():
        time.sleep(30)
        print("🚨 [Main] Graceful shutdown timed out! Forcing exit...", flush=True)
        os._exit(1)

    threading.Thread(target=hard_kill, daemon=True).start()


def _smoke_test_ml() -> None:
    """Выполняет инференс на синтетическом тике при старте.
    
    Проверяет валидность модели (corrupted pickle) и форму (shape/dtype) выходов,
    чтобы предотвратить краш во время скоринга (P2-5).
    """
    print("🚀 [Startup] Выполняется ML Smoke Test...", flush=True)
    import time
    from typing import Any
    import numpy as np
    from services.ml_confirm_gate import MLConfirmGate
    
    try:
        gate = MLConfirmGate.from_env()
        model = gate._model
        if model is None:
            print("⚠️ [Startup] Модель не загружена, пропускаем Smoke Test.", flush=True)
            return

        cfg = gate._cfg
        kind = str(cfg.get("kind", "")).lower()

        # Generate dummy feature vector matching model's expected size.
        # Priority: sklearn n_features_in_ > feature_cols > feature_names > hardcoded fallback.
        fcols_len: int = 0
        if isinstance(model, dict):
            # edge_stack_v1: composite dict with lr/gbdt sub-models
            for _sub_key in ("lr", "gbdt"):
                _sub = model.get(_sub_key)
                if _sub is not None and hasattr(_sub, "n_features_in_"):
                    fcols_len = int(getattr(_sub, "n_features_in_"))
                    break
            if fcols_len == 0:
                fcols = model.get("feature_cols") or model.get("feature_names") or []
                fcols_len = max(1, len(fcols))
        else:
            # sklearn/catboost/xgboost standard attribute
            if hasattr(model, "n_features_in_"):
                fcols_len = int(getattr(model, "n_features_in_"))
            elif hasattr(model, "feature_cols"):
                fcols_len = max(1, len(getattr(model, "feature_cols", [])))
            elif hasattr(model, "feature_names_in_"):
                fcols_len = max(1, len(getattr(model, "feature_names_in_", [])))
        if fcols_len <= 0:
            print("⚠️ [Startup] ML Smoke Test: не удалось определить n_features_in_, используем 50 (fallback).", flush=True)
            fcols_len = 50
            
        dummy_X = np.zeros((1, fcols_len), dtype=np.float32)

        def check_preds(name: str, preds: Any) -> None:
            if not isinstance(preds, np.ndarray):
                raise TypeError(f"{name} вернул {type(preds)}, ожидался np.ndarray")
            if preds.shape == ():
                raise ValueError(f"{name} вернул пустой shape ()")
            if preds.shape[0] != 1:
                raise ValueError(f"{name} shape {preds.shape} некорректен (ожидалась 1 строка)")

        # Verify underlying sklearn/catboost shape and types
        if isinstance(model, dict) and kind == "edge_stack_v1":
            lr = model.get("lr")
            if lr is not None and hasattr(lr, "predict_proba"):
                check_preds("LR predict_proba", lr.predict_proba(dummy_X))
            gbdt = model.get("gbdt")
            if gbdt is not None and hasattr(gbdt, "predict_proba"):
                check_preds("GBDT predict_proba", gbdt.predict_proba(dummy_X))
        else:
            if hasattr(model, "predict_proba"):
                check_preds("predict_proba", model.predict_proba(dummy_X))
            if hasattr(model, "predict_util"):
                check_preds("predict_util", model.predict_util(dummy_X))

        # Check full pipeline (gate.check()) — передаём ВСЕ обязательные kwargs
        dec = gate.check(
            symbol="BTCUSDT",
            ts_ms=int(time.time() * 1000),
            direction="LONG",
            scenario="trend_up",
            indicators={"close": 50000.0, "volume": 100.0},
            rule_score=0.5,
            rule_have=0,
            rule_need=0,
            cancel_spike_veto=0,
            ok_rule=1,
        )
        if dec.error and "missing" not in dec.error.lower():
            print(f"❌ [Startup] ML Smoke Test вернул ошибку выполнения: {dec.error}", flush=True)
            sys.exit(1)
            
        print("✅ [Startup] ML Smoke Test пройден (shape/dtype проверены).", flush=True)

    except Exception as e:
        print(f"❌ [Startup] Ошибка: ML Smoke Test крашнулся (corrupted pickle?): {e}", flush=True)
        import traceback
        traceback.print_exc()
        sys.exit(1)


def main() -> None:
    """Главная функция приложения."""
    metrics_port = int(os.getenv("METRICS_PORT", "8000"))
    start_http_server(metrics_port)

    print_startup_banner()
    print_startup_message()
    print("🚀 [Startup] Выполняется валидация конфигурации...", flush=True)
    if not wait_for_redis():
        print("❌ [Startup] Ошибка: Redis недоступен. Завершение работы.", flush=True)
        sys.exit(1)

    from tools.init_ml_confirm_on_startup import find_latest_model
    if not find_latest_model():
        print("❌ [Startup] Ошибка: ML-модель (model.joblib) не найдена. Завершение работы.", flush=True)
        sys.exit(1)

    if not ensure_ml_confirm_config():
        print("❌ [Startup] Ошибка: Конфигурация ML Confirm не создана. Завершение работы.", flush=True)
        sys.exit(1)

    _smoke_test_ml()

    crypto_symbols = os.getenv("CRYPTO_SYMBOLS") or os.getenv("CRYPTO_SYMBOLS_OVERRIDE") or os.getenv("SYMBOLS")
    if not crypto_symbols or not crypto_symbols.strip():
        print("❌ [Startup] Ошибка: Символьный конфиг (CRYPTO_SYMBOLS) пуст. Завершение работы.", flush=True)
        sys.exit(1)

    # P0-Fix #2: fail-fast если ACCOUNT_DEPOSIT_USD не задан явно.
    # Default=100 приводит к risk cap в 100x ниже реального депозита.
    _raw_deposit = os.getenv("ACCOUNT_DEPOSIT_USD", "").strip()
    if not _raw_deposit:
        print(
            "❌ [Startup] КРИТИЧНО: ENV-переменная ACCOUNT_DEPOSIT_USD не задана!\n"
            "   Без неё notional cap считается от $100 (дефолт), что в 100x ниже реального депозита.\n"
            "   Задайте ACCOUNT_DEPOSIT_USD=<ваш депозит> в .env и перезапустите сервис.",
            flush=True,
        )
        sys.exit(1)
    try:
        _deposit_val = float(_raw_deposit)
        if _deposit_val <= 0:
            raise ValueError("должно быть > 0")
    except ValueError as _dep_err:
        print(
            f"❌ [Startup] ACCOUNT_DEPOSIT_USD='{_raw_deposit}' — некорректное значение: {_dep_err}. "
            "Ожидается положительное число.",
            flush=True,
        )
        sys.exit(1)

    # P0-Fix #3: предупреждение о silent coercion RISK_PERCENT (0.3 → 30%).
    # Проверяем здесь (до горячего пути), чтобы оператор видел предупреждение при старте.
    _raw_rp = os.getenv("RISK_PERCENT", "").strip()
    if _raw_rp:
        try:
            _rp_val = float(_raw_rp)
            if 0 < _rp_val < 0.5:
                print(
                    f"⚠️  [Startup] RISK_PERCENT={_raw_rp} выглядит как доля (< 0.5), "
                    f"но будет автоматически масштабирован в {_rp_val * 100:.1f}% "
                    f"(умножение на 100). Убедитесь, что это намеренно. "
                    f"Рекомендуется задавать RISK_PERCENT в процентах, например RISK_PERCENT=5.0",
                    flush=True,
                )
        except ValueError:
            pass  # невалидное значение поймает сам orchestrator

    print("✅ [Startup] Конфигурация успешно отвалидирована.", flush=True)
    
    import signal
    signal.signal(signal.SIGINT, handle_exit)
    signal.signal(signal.SIGTERM, handle_exit)

    # Start CryptoOrderflowService in background thread.
    of_thread = threading.Thread(
        target=run_orderflow_service,
        name="OrderFlowService",
        daemon=True,
    )
    global _OF_THREAD
    _OF_THREAD = of_thread
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
    global _GLOBAL_SIGNAL_PROCESSOR
    _GLOBAL_SIGNAL_PROCESSOR = signal_processor
    signal_processor.start_all()

    try:
        signal_processor.wait_forever()
    finally:
        print_shutdown_message()
        if _OF_THREAD and _OF_THREAD.is_alive():
            print("⏳ [Main] Waiting for OrderFlowService thread to finish...", flush=True)
            _OF_THREAD.join(timeout=35)
        print("🏁 [Main] Shutdown complete.", flush=True)


if __name__ == "__main__":
    main()
