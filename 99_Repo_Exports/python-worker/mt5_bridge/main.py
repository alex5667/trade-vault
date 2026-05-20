#!/usr/bin/env python3
from __future__ import annotations
from core.redis_keys import RedisStreams as RS

"""
MT5 Bridge Main Worker

Основной воркер MT5-моста. Читает ExecutionPlan из Redis Streams
и автоматически исполняет сигналы в MetaTrader5 терминале.

Запуск:
    python -m mt5_bridge.main

Environment Variables:
    MT5_LOGIN: MT5 account login (required)
    MT5_PASSWORD: MT5 account password (required)
    MT5_SERVER: MT5 server (required)
    REDIS_DSN: Redis connection string (default: redis://localhost:6379/0)
    MT5_SYMBOL_MAP: JSON string with symbol mapping (optional)
    POLL_BLOCK_MS: Block time for Redis polling (default: 500)
    POLL_COUNT: Max messages per poll (default: 20)
    STEP_INTERVAL: Seconds between execution steps (default: 0.2)
"""


import json
import os
import signal
import sys
import time

from .commands_consumer import ExecCommandsConsumer
from .deals_watcher import Mt5DealsWatcher
from .exec_events import ExecEventsPublisher
from .executor import PlanExecutor
from .mt5_client import Mt5Client, Mt5Config
from .redis_consumer import PlansStreamConsumer


def load_symbol_map() -> dict[str, str]:
    """
    Загружает маппинг символов из MT5_SYMBOL_MAP environment variable.

    Returns:
        dict: Маппинг символов, по умолчанию пустой
    """
    symbol_map_str = os.environ.get("MT5_SYMBOL_MAP", "{}")
    try:
        return json.loads(symbol_map_str)
    except json.JSONDecodeError:
        print(f"[mt5_bridge] Warning: Invalid MT5_SYMBOL_MAP JSON: {symbol_map_str}")
        return {}


def create_mt5_config() -> Mt5Config:
    """
    Создает конфигурацию MT5 из environment variables.

    Returns:
        Mt5Config: Конфигурация для MT5 подключения

    Raises:
        ValueError: Если отсутствуют обязательные переменные
    """
    required_vars = ["MT5_LOGIN", "MT5_PASSWORD", "MT5_SERVER"]
    missing = [var for var in required_vars if not os.environ.get(var)]

    if missing:
        raise ValueError(f"Missing required environment variables: {missing}")

    return Mt5Config(
        login=int(os.environ["MT5_LOGIN"]),
        password=os.environ["MT5_PASSWORD"],
        server=os.environ["MT5_SERVER"],
        symbol_map=load_symbol_map(),
    )


def main():
    """
    Основной цикл MT5-моста.

    1. Подключается к MT5 терминалу
    2. Начинает читать планы из Redis Streams
    3. Исполняет сигналы согласно планам
    4. Обрабатывает сигнал завершения
    """
    # MT5 kill switch (2026-05-19): refuse to start unless MT5_ENABLED=1.
    # Whole module preserved for re-enable; set MT5_ENABLED=1 to restore.
    from core.mt5_kill_switch import mt5_enabled
    if not mt5_enabled():
        sys.stderr.write(
            "mt5_bridge: MT5_ENABLED=0 (default) — refusing to start.\n"
            "  Set MT5_ENABLED=1 to re-enable the MT5 bridge.\n"
        )
        sys.exit(0)
    print("[mt5_bridge] Starting MT5 Bridge...")

    # Конфигурация из environment
    redis_dsn = os.environ.get("REDIS_DSN", "redis://localhost:6379/0")
    poll_block_ms = int(os.environ.get("POLL_BLOCK_MS", "500"))
    poll_count = int(os.environ.get("POLL_COUNT", "20"))
    step_interval = float(os.environ.get("STEP_INTERVAL", "0.2"))

    try:
        # Создаем MT5 конфигурацию и подключаемся
        mt5_cfg = create_mt5_config()
        mt5_client = Mt5Client(mt5_cfg)
        print("[mt5_bridge] ✅ MT5 connected successfully")

        # --- Redis streams ---
        plans_consumer = PlansStreamConsumer(redis_dsn)
        print(f"[mt5_bridge] ✅ Plans consumer initialized (stream: {plans_consumer.stream_key})")

        exec_publisher = ExecEventsPublisher(
            redis_dsn,
            stream_key=RS.SIGNAL_EXEC_EVENTS,
        )
        print("[mt5_bridge] ✅ Exec events publisher initialized")

        # --- executor (вход по зонам) ---
        executor = PlanExecutor(mt5_client, bar_seconds=60)
        print("[mt5_bridge] ✅ Plan executor initialized")

        # --- watcher фактических сделок ---
        deals_watcher = Mt5DealsWatcher(
            mt5_client=mt5_client,
            publisher=exec_publisher,
            history_window_minutes=60 * 24,  # при старте смотрим сутки назад
        )
        print("[mt5_bridge] ✅ Deals watcher initialized")

        # --- commands consumer (опционально) ---
        commands_consumer = ExecCommandsConsumer(redis_dsn, mt5_client)
        print("[mt5_bridge] ✅ Commands consumer initialized")

        print("[mt5_bridge] 🚀 Bridge started - waiting for signals...")

        # Флаг для graceful shutdown
        running = True

        def signal_handler(signum, frame):
            nonlocal running
            print(f"\n[mt5_bridge] Received signal {signum}, shutting down gracefully...")
            running = False

        # Регистрируем обработчики сигналов
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        # Основной цикл
        while running:
            try:
                # 1) забираем новые ExecutionPlan'ы
                plans = plans_consumer.poll(block_ms=poll_block_ms, count=poll_count)

                for plan in plans:
                    print(f"[mt5_bridge] 📋 New plan: {plan.signal_id} {plan.symbol} {plan.side} "
                          f"zone=({plan.entry_zone_low:.2f}..{plan.entry_zone_high:.2f}) "
                          f"stop={plan.stop_price:.2f} size={plan.position_size_lots:.3f} "
                          f"expiry={plan.expiry_bars} bars")
                    executor.add_plan(plan)

                # 2) шаг execution-логики (TTD + entry-zone)
                executor.step()

                # 3) учёт фактических сделок → stream:signals:exec_events
                deals_watcher.step()

                # 4) обработка команд (CLOSE_REQUEST и т.п.)
                commands_consumer.step()

                # 5) выводим статистику каждые 60 секунд
                if int(time.time()) % 60 == 0:
                    active = executor.get_active_plans_count()
                    entered = executor.get_entered_positions_count()
                    print(f"[mt5_bridge] 📊 Status: {active} active plans, {entered} positions entered")

                # 6) небольшая пауза, чтобы не жрать CPU
                time.sleep(step_interval)

            except Exception as e:
                print(f"[mt5_bridge] ❌ Error in main loop: {e}")
                time.sleep(1)  # Пауза перед продолжением

        print("[mt5_bridge] 🛑 Shutdown complete")

    except ValueError as e:
        print(f"[mt5_bridge] ❌ Configuration error: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"[mt5_bridge] ❌ Fatal error: {e}")
        sys.exit(1)
    finally:
        # Корректное завершение MT5
        try:
            if 'mt5_client' in locals():
                mt5_client.shutdown()
                print("[mt5_bridge] ✅ MT5 connection closed")
        except Exception:
            pass


if __name__ == "__main__":
    main()
