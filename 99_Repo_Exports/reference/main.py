"""
Главная точка входа python-worker: инициализация, ожидание Redis, запуск обработчиков и graceful shutdown.
"""
from core.app_init import print_startup_banner, print_startup_message, print_shutdown_message
from core.redis_utils import wait_for_redis
from core.ws_manager import WebSocketManager
from tools.init_ml_confirm_on_startup import ensure_ml_confirm_config
from handlers.signal_processor import SignalProcessor
from prometheus_client import start_http_server
import os
import warnings
import threading
import asyncio
import logging

# Suppress annoying NVIDIA driver warnings when running on CPU nodes
warnings.filterwarnings("ignore", message=".*NVIDIA Driver not detected.*")
warnings.filterwarnings("ignore", message=".*CUDA initialization.*")

def run_orderflow_service():
    """Runs CryptoOrderflowService in a separate asyncio loop."""
    print("🚀 [Main] Starting OrderFlowService thread...", flush=True)
    try:
        from services.crypto_orderflow_service import CryptoOrderflowService
        print("✅ [Main] CryptoOrderflowService imported", flush=True)
        
        # Setup new loop for this thread
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
        ticks_url = os.getenv("REDIS_TICKS_URL") or os.getenv("REDIS_URL_TICKS")
        
        service = CryptoOrderflowService(redis_dsn=redis_url, ticks_dsn=ticks_url)
        print("🚀 [Main] Running CryptoOrderflowService.run_forever()...", flush=True)
        
        loop.run_until_complete(service.run_forever())
    except Exception as e:
        print(f"❌ [Main] CryptoOrderflowService failed: {e}", flush=True)
        import traceback
        traceback.print_exc()
    finally:
        print("🛑 [Main] CryptoOrderflowService thread finished", flush=True)
        try:
            loop.close()
        except:
            pass

def main():
    """Главная функция приложения"""
    # Start Prometheus server
    metrics_port = int(os.getenv("METRICS_PORT", "8000"))
    start_http_server(metrics_port)
    
    # Инициализация
    print_startup_banner()
    print_startup_message()
    
    # Ожидание готовности Redis
    wait_for_redis()

    # Ensure ML Confirm configuration exists
    ensure_ml_confirm_config()
    
    # Start CryptoOrderflowService in background thread
    of_thread = threading.Thread(target=run_orderflow_service, name="OrderFlowService", daemon=True)
    of_thread.start()
    
    # Создание менеджера WebSocket
    ws_manager = WebSocketManager()
    
    # Создание и запуск обработчика сигналов
    signal_processor = SignalProcessor(ws_manager.update_ws_connections)
    signal_processor.start_all()
    
    # Ожидание завершения работы
    try:
        signal_processor.wait_forever()
    finally:
        print_shutdown_message()


if __name__ == "__main__":
    main()
