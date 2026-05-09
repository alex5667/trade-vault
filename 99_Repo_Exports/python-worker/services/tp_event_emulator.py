from utils.time_utils import get_ny_time_millis
from core.redis_keys import RedisStreams as RS

# -*- coding: utf-8 -*-
"""
Эмулятор событий TP/SL для тестирования системы трейлинга.

Этот модуль можно использовать для:
1. Тестирования tp_event_listener без реального MT5
2. Симуляции различных сценариев (TP1->TP2->TP3, TP1->SL и т.д.)
3. Проверки работы оркестратора трейлинга

Использование:
    python -m services.tp_event_emulator --sid signal--123 --scenario tp1_then_tp2
"""

import argparse
import os
import sys
import time
from pathlib import Path
from typing import Any

import redis

# Добавляем путь к python-worker в PYTHONPATH
_worker_path = Path(__file__).parent.parent
if str(_worker_path) not in sys.path:
    sys.path.insert(0, str(_worker_path))

from common.log import setup_logger

log = setup_logger("tp_event_emulator")


class TPEventEmulator:
    """
    Эмулятор событий TP/SL для тестирования.
    """

    def __init__(self, redis_url: str = None):
        """
        Args:
            redis_url: URL Redis (если None, берётся из REDIS_URL env)
        """
        self.redis_url = redis_url or os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
        self.r = redis.from_url(self.redis_url, decode_responses=True)
        self.events_stream = os.getenv("TP_EVENTS_STREAM", RS.EVENTS_TRADES)

        log.info("✅ TPEventEmulator initialized: redis=%s stream=%s",
                 self.redis_url, self.events_stream)

    def emit_event(self, event: dict[str, Any]) -> str:
        """
        Отправить событие в Redis stream.
        
        Args:
            event: Словарь с данными события
            
        Returns:
            ID сообщения в stream
        """
        msg_id = self.r.xadd(self.events_stream, event, maxlen=50000)
        log.info("📡 Event emitted: %s (id=%s)", event.get("event_type"), msg_id)
        return msg_id

    def emit_tp1_hit(
        self,
        sid: str,
        symbol="",
        price: float = 2769.9,
        position_id: str = "1234567"
    ) -> str:
        """Эмитировать TP1_HIT событие."""
        event = {
            "event_type": "TP1_HIT",
            "sid": sid,
            "symbol": symbol,
            "position_id": position_id,
            "ticket": position_id,
            "price": str(price),
            "ts": str(get_ny_time_millis()),
            "source": "emulator"
        }
        return self.emit_event(event)

    def emit_tp2_hit(
        self,
        sid: str,
        symbol="",
        price: float = 2773.1,
        position_id: str = "1234567"
    ) -> str:
        """Эмитировать TP2_HIT событие."""
        event = {
            "event_type": "TP2_HIT",
            "sid": sid,
            "symbol": symbol,
            "position_id": position_id,
            "ticket": position_id,
            "price": str(price),
            "ts": str(get_ny_time_millis()),
            "source": "emulator"
        }
        return self.emit_event(event)

    def emit_tp3_hit(
        self,
        sid: str,
        symbol="",
        price: float = 2776.3,
        position_id: str = "1234567"
    ) -> str:
        """Эмитировать TP3_HIT событие."""
        event = {
            "event_type": "TP3_HIT",
            "sid": sid,
            "symbol": symbol,
            "position_id": position_id,
            "ticket": position_id,
            "price": str(price),
            "ts": str(get_ny_time_millis()),
            "source": "emulator"
        }
        return self.emit_event(event)

    def emit_sl_hit(
        self,
        sid: str,
        symbol="",
        price: float = 2758.7,
        position_id: str = "1234567"
    ) -> str:
        """Эмитировать SL_HIT событие."""
        event = {
            "event_type": "SL_HIT",
            "sid": sid,
            "symbol": symbol,
            "position_id": position_id,
            "ticket": position_id,
            "price": str(price),
            "ts": str(get_ny_time_millis()),
            "source": "emulator"
        }
        return self.emit_event(event)

    def run_scenario(self, scenario: str, sid: str, symbol=""):
        """
        Запустить сценарий тестирования.
        
        Args:
            scenario: Название сценария
            sid: ID сигнала
            symbol: Символ
        """
        log.info("🎬 Running scenario: %s for %s", scenario, sid)

        if scenario == "tp1_only":
            self.emit_tp1_hit(sid, symbol)

        elif scenario == "tp1_then_tp2":
            self.emit_tp1_hit(sid, symbol)
            time.sleep(2)
            self.emit_tp2_hit(sid, symbol)

        elif scenario == "tp1_then_tp2_then_tp3":
            self.emit_tp1_hit(sid, symbol)
            time.sleep(2)
            self.emit_tp2_hit(sid, symbol)
            time.sleep(2)
            self.emit_tp3_hit(sid, symbol)

        elif scenario == "tp1_then_sl":
            self.emit_tp1_hit(sid, symbol)
            time.sleep(2)
            self.emit_sl_hit(sid, symbol)

        elif scenario == "direct_sl":
            self.emit_sl_hit(sid, symbol)

        else:
            log.error("❌ Unknown scenario: %s", scenario)
            return

        log.info("✅ Scenario completed: %s", scenario)


def main():
    """Entry point."""
    parser = argparse.ArgumentParser(description="TP Event Emulator")
    parser.add_argument("--sid", required=True, help="Signal ID")
    parser.add_argument("--symbol", help="Symbol (default: )")
    parser.add_argument(
        "--scenario",
        choices=[
            "tp1_only",
            "tp1_then_tp2",
            "tp1_then_tp2_then_tp3",
            "tp1_then_sl",
            "direct_sl"
        ],
        default="tp1_only",
        help="Test scenario"
    )

    args = parser.parse_args()

    log.info("=" * 80)
    log.info("TP Event Emulator")
    log.info("=" * 80)
    log.info("SID: %s", args.sid)
    log.info("Symbol: %s", args.symbol)
    log.info("Scenario: %s", args.scenario)
    log.info("=" * 80)

    emulator = TPEventEmulator()
    emulator.run_scenario(args.scenario, args.sid, args.symbol)


if __name__ == "__main__":
    main()

