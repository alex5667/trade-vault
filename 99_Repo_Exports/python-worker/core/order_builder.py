from utils.time_utils import get_ny_time_millis
import time
from typing import Any, Dict


class OrderBuilder:
    """
    Формирует команду для `orders:queue` на основе сохранённого сигнала.

    Приоритет источников объёма:
      1. `config:orderflow:<symbol>` — `base_qty`, `qty`, `volume`, `order_qty`
      2. `profiles:trailing:<profile>` — `default_qty`
      3. Fallback: 0.01
    """

    def __init__(self, redis_core):
        self.r = redis_core

    def _load_hash(self, key: str) -> Dict[str, Any]:
        try:
            data = self.r.hgetall(key)
        except Exception:
            return {}

        if not data:
            return {}

        decoded: Dict[str, Any] = {}
        for raw_key, raw_value in data.items():
            key = raw_key.decode() if isinstance(raw_key, bytes) else raw_key
            value = raw_value.decode() if isinstance(raw_value, bytes) else raw_value
            decoded[key] = value
        return decoded

    def _resolve_quantity(self, signal: Dict[str, Any]) -> float:
        symbol = signal["symbol"]
        cfg = self._load_hash(f"config:orderflow:{symbol}")

        for candidate in ("base_qty", "qty", "volume", "order_qty"):
            if candidate in cfg:
                try:
                    return float(cfg[candidate])
                except (TypeError, ValueError):
                    continue

        profile_name = signal.get("trail_profile") or "default"
        profile = self._load_hash(f"profiles:trailing:{profile_name}")
        default_qty = profile.get("default_qty")
        if default_qty is not None:
            try:
                return float(default_qty)
            except (TypeError, ValueError):
                pass

        return 0.01

    def build_order_from_signal(self, signal: Dict[str, Any]) -> Dict[str, Any]:
        symbol = signal["symbol"]
        direction = str(signal.get("direction", "")).upper()
        side = "buy" if direction == "LONG" else "sell"
        price = signal.get("entry")
        sid = signal["sid"]

        qty = self._resolve_quantity(signal)
        order_id = f"order-{sid}-{get_ny_time_millis()}"

        payload = {
            "id": order_id,
            "sid": sid,
            "symbol": symbol,
            "type": "market" if price in (None, 0, "", "0") else "limit",
            "side": side,
            "qty": qty,
            "price": price,
            "source": "order_builder_v2",
            "idempotency_key": order_id,
            "metadata": {
                "from_signal": True,
                "signal_confidence": signal.get("confidence"),
                "trail_after_tp1": signal.get("trail_after_tp1"),
                "trail_profile": signal.get("trail_profile"),
            },
        }

        # Add SL/TP to payload
        if "sl" in signal:
            payload["sl"] = signal["sl"]
        if "tp_levels" in signal:
            payload["tp_levels"] = signal["tp_levels"]

        return payload

