"""
Сервис обновления ADX/DI/ATR по 1-минутным свечам.

1. Периодически читает последнюю свечу из `candles:{symbol}:1m`.
2. Обновляет состояние Wilder и пишет результат в `adx:{symbol}`.

Использует функции из `regime-worker.adx_atr`, чтобы не дублировать расчёты.
"""

import logging
import os
import time
from typing import Any, Dict, List

import redis

from regime_worker.adx_atr import WilderState, update_adx_atr


REDIS_URL = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
CANDLE_KEY_TMPL = "candles:{symbol}:1m"
ADX_KEY_TMPL = "adx:{symbol}"
DEFAULT_SYMBOLS = [s.strip().upper() for s in os.getenv("ADX_SYMBOLS", "BTCUSDT,ETHUSDT").split(",") if s.strip()]
TICK_INTERVAL_SEC = float(os.getenv("ADX_UPDATE_INTERVAL_SEC", "1.0"))
WILDER_PERIOD = int(os.getenv("ADX_WILDER_PERIOD", "14"))

log = logging.getLogger("adx-updater")
logging.basicConfig(level=logging.INFO)


class AdxUpdater:
    def __init__(self, redis_client: redis.Redis, symbols: List[str]):
        self.r = redis_client
        self.symbols = symbols
        self.states: Dict[str, WilderState] = {s: WilderState() for s in symbols}

    def _decode_hash(self, raw: Dict[Any, Any]) -> Dict[str, str]:
        decoded: Dict[str, str] = {}
        for k, v in raw.items():
            key = k.decode() if isinstance(k, bytes) else k
            val = v.decode() if isinstance(v, bytes) else v
            decoded[key] = val
        return decoded

    def _load_last_candle(self, symbol: str) -> Dict[str, str]:
        key = CANDLE_KEY_TMPL.format(symbol=symbol)
        try:
            data = self.r.hgetall(key)
        except Exception:
            return {}
        if not data:
            return {}
        return self._decode_hash(data)

    def _parse_float(self, source: Dict[str, str], key: str, fallback: float) -> float:
        value = source.get(key)
        if value is None:
            return fallback
        try:
            return float(value)
        except (TypeError, ValueError):
            return fallback

    def tick(self) -> None:
        for symbol in self.symbols:
            candle = self._load_last_candle(symbol)
            if not candle:
                continue

            o = self._parse_float(candle, "open", 0.0)
            h = self._parse_float(candle, "high", o)
            l = self._parse_float(candle, "low", o)
            c = self._parse_float(candle, "close", o)

            prev_high = self._parse_float(candle, "prev_high", h)
            prev_low = self._parse_float(candle, "prev_low", l)
            prev_close = self._parse_float(candle, "prev_close", o)

            state = self.states[symbol]
            state, result = update_adx_atr(state, h, l, c, prev_high, prev_low, prev_close, n=WILDER_PERIOD)
            self.states[symbol] = state

            if result is None:
                continue

            payload = {
                "atr": result["atr"]
                "plusDI": result["plusDI"]
                "minusDI": result["minusDI"]
                "adx": result["adx"]
                "ts": int(time.time())
            }

            try:
                self.r.hset(ADX_KEY_TMPL.format(symbol=symbol), mapping=payload)
            except Exception as exc:
                log.warning("Failed to write ADX for %s: %s", symbol, exc)


def main() -> None:
    client = redis.Redis.from_url(REDIS_URL, decode_responses=False)
    symbols = DEFAULT_SYMBOLS or ["BTCUSDT", "ETHUSDT"]
    updater = AdxUpdater(client, symbols)

    while True:
        updater.tick()
        time.sleep(TICK_INTERVAL_SEC)


if __name__ == "__main__":
    main()

