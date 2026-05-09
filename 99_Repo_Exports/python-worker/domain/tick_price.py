# domain/tick_price.py
from __future__ import annotations

from domain.models import Side, Tick


def build_tick(raw: dict) -> Tick | None:
    symbol = (raw.get("symbol") or "").strip().upper()
    if not symbol:
        return None

    from domain.time_utils import normalize_ts_ms
    ts_raw = raw.get("ts") or raw.get("timestamp") or 0
    ts_ms = normalize_ts_ms(int(float(ts_raw)) if ts_raw else 0)

    bid = float(raw.get("bid") or 0.0)
    ask = float(raw.get("ask") or 0.0)
    last = float(raw.get("last") or 0.0)
    price = float(raw.get("price") or 0.0)

    mid = 0.0
    if bid > 0 and ask > 0:
        mid = (bid + ask) / 2.0
    elif last > 0:
        mid = last
    elif price > 0:
        mid = price

    if mid <= 0:
        return None

    return Tick(symbol=symbol, ts_ms=ts_ms, bid=bid, ask=ask, last=last, price=price, mid=mid)


def trigger_prices(tick: Tick, side: Side) -> tuple[float, float, float]:
    """
    Возвращает (tp_trigger_price, sl_trigger_price, reference_mid)
    Важно:
      LONG: закрытие/TP/SL исполняются SELL → смотрим BID если есть
      SHORT: закрытие/TP/SL исполняются BUY  → смотрим ASK если есть
    """
    mid = tick.mid

    if side == "LONG":
        tp_px = tick.bid if tick.bid > 0 else mid
        sl_px = tick.bid if tick.bid > 0 else mid
        return tp_px, sl_px, mid

    # SHORT
    tp_px = tick.ask if tick.ask > 0 else mid
    sl_px = tick.ask if tick.ask > 0 else mid
    return tp_px, sl_px, mid

