"""
MT5 Bridge Models

Модели данных для MT5-моста - упрощенные версии для исполнения сигналов.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List


@dataclass
class Mt5ExecutionPlan:
    """
    Упрощённая модель ExecutionPlan для MT5-моста.

    Здесь только то, что нужно для открытия и сопровождения сделок в MT5.
    Содержит всю необходимую информацию для:
    - Проверки time-to-decay (TTL по expiry_bars)
    - Входа в позицию при попадании цены в entry zone
    - Разбиения позиции по частичным выходам (partials + tp_levels)
    - Управления стоп-лоссами
    """

    signal_id: str
    symbol: str
    side: str           # "long" / "short"
    ts_signal: datetime
    price_at_signal: float

    entry_zone_low: float
    entry_zone_high: float
    stop_price: float

    tp_levels: List[float]          # абсолютные цены TP
    partials: List[float]           # доли объёма: [0.5, 0.5] и т.п.

    risk_usd: float
    position_size_lots: float       # объём в лотах для MT5
    expiry_bars: int

    created_at: datetime

    # --- удобные свойства для логики ---

    @property
    def is_long(self) -> bool:
        """True если это длинная позиция (покупка)."""
        return self.side.lower() in ("long", "buy")

    @property
    def is_short(self) -> bool:
        """True если это короткая позиция (продажа)."""
        return self.side.lower() in ("short", "sell")

    @property
    def ttl_seconds(self) -> float:
        """
        Time-to-live в секундах: expiry_bars * 60 (предполагая 1m бары).
        После истечения TTL сигнал не исполняется.
        """
        return self.expiry_bars * 60.0

    @property
    def is_expired(self) -> bool:
        """
        Проверяет, истек ли TTL сигнала относительно текущего времени.
        """
        now = datetime.now(timezone.utc)
        elapsed = (now - self.ts_signal).total_seconds()
        return elapsed > self.ttl_seconds

    def price_in_entry_zone(self, price: float) -> bool:
        """
        Проверяет, находится ли цена в entry zone для входа в позицию.
        """
        low = min(self.entry_zone_low, self.entry_zone_high)
        high = max(self.entry_zone_low, self.entry_zone_high)
        return low <= price <= high


def plan_from_dict(data: dict) -> Mt5ExecutionPlan:
    """
    Конвертирует dict из Redis payload["plan"] в Mt5ExecutionPlan.

    data — это dict из SignalBus._plan_to_dict() или аналогичный формат.

    Формат данных:
    {
        "signal_id": "XAU_2025-12-15_12:34:56"
        "symbol": "XAUUSD"
        "setup_type": "volatility_spike"
        "side": "long"
        "ts_signal": "2025-12-15T12:34:56.123456+00:00"
        "price_at_signal": 2615.3
        "entry_zone_low": 2610.0
        "entry_zone_high": 2616.0
        "stop_price": 2600.0
        "tp_levels": [2625.0, 2640.0]
        "partials": [0.5, 0.5]
        "pos_risk_R": 1.0
        "risk_usd": 100.0
        "position_size": 0.2,  # в лотах
        "expiry_bars": 3
        "created_at": "2025-12-15T12:34:56.123456+00:00"
        "meta": {}
    }
    """

    def parse_dt(s: str) -> datetime:
        """
        Парсит ISO 8601 строку в datetime с timezone.
        Если timezone отсутствует, предполагаем UTC.
        """
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt

    return Mt5ExecutionPlan(
        signal_id=data["signal_id"]
        symbol=data["symbol"]
        side=data["side"]
        ts_signal=parse_dt(data["ts_signal"])
        price_at_signal=float(data["price_at_signal"])
        entry_zone_low=float(data["entry_zone_low"])
        entry_zone_high=float(data["entry_zone_high"])
        stop_price=float(data["stop_price"])
        tp_levels=[float(x) for x in data.get("tp_levels", [])]
        partials=[float(x) for x in data.get("partials", [])] or [1.0]
        risk_usd=float(data["risk_usd"])
        position_size_lots=float(data["position_size"])
        expiry_bars=int(data.get("expiry_bars", 3))
        created_at=parse_dt(data["created_at"])
    )
