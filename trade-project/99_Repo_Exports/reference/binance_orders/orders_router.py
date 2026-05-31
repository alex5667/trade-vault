#!/usr/bin/env python3
"""
Orders Router - Telegram callbacks to orders queue.

Consumes callback events from bot:callbacks stream and routes them to
orders:queue for execution by MT5 OrderExecutor.

Uses signal snapshots (signal:snap:{sid}) to get full trade context.

Workflow:
    bot:callbacks → orders_router → orders:queue → MT5 OrderExecutor
"""

import math
import os
import json
import time
import redis
from typing import Dict, Optional, Any

from symbol_specs_store import SymbolSpecsStore, SymbolSpecs


# Configuration
REDIS_URL = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
CALLBACKS_STREAM = os.getenv("CALLBACKS_STREAM", "bot:callbacks")
# Router is used for MT5 by default.
ORDERS_QUEUE = os.getenv("ORDERS_QUEUE_MT5") or os.getenv("ORDERS_QUEUE") or "orders:queue:mt5"
SNAP_PREFIX = os.getenv("SNAP_PREFIX", "signal:snap:")
GROUP = os.getenv("ORDERS_ROUTER_GROUP", "orders-router-group")
CONSUMER = os.getenv("ORDERS_ROUTER_CONSUMER", "orders-router-1")


def _parse_float(value: Any) -> Optional[float]:
    """Безопасно преобразует значение к float."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _decimals_from_point(point: float) -> int:
    """Определяет количество знаков после запятой исходя из размера пункта."""
    if point <= 0:
        return 2
    decimals = 0
    scaled = point
    while not math.isclose(scaled, round(scaled), rel_tol=0.0, abs_tol=1e-9) and decimals < 12:
        scaled *= 10
        decimals += 1
    if decimals == 0 and point < 1:
        decimals = int(round(-math.log10(point)))
    return max(0, min(decimals, 12))


def _round_price(price: float, point: float) -> float:
    """Округляет цену до ближайшего допустимого шага."""
    if point <= 0:
        return round(price, 5)
    decimals = _decimals_from_point(point)
    steps = round(price / point)
    return round(steps * point, decimals)


def _ensure_min_distance(entry: Optional[float], target: Optional[float], side: str, min_distance: float) -> Optional[float]:
    """Возвращает цену, отстоящую от entry не менее чем на min_distance."""
    if entry is None or target is None or min_distance <= 0:
        return target
    gap = target - entry
    if side.upper() == "LONG":
        if gap > -min_distance:
            return entry - min_distance
        return target
    if gap < min_distance:
        return entry + min_distance
    return target


def _filter_tp_levels(entry: Optional[float], tps: list, side: str, min_distance: float) -> list:
    """Фильтрует TP уровни, оставляя только корректные значения."""
    if entry is None or not tps:
        return []
    filtered = []
    for raw_tp in tps:
        tp_val = _parse_float(raw_tp)
        if tp_val is None:
            continue
        distance = abs(tp_val - entry)
        if distance < min_distance:
            continue
        if side.upper() == "LONG" and tp_val <= entry:
            continue
        if side.upper() == "SHORT" and tp_val >= entry:
            continue
        filtered.append(tp_val)
    return filtered


def get_snapshot(r: redis.Redis, sid: str) -> Optional[Dict]:
    """
    Get signal snapshot from Redis.
    
    Args:
        r: Redis client
        sid: Signal ID
        
    Returns:
        Signal snapshot dict or None
    """
    snap = r.get(SNAP_PREFIX + sid)
    if not snap:
        return None
    
    try:
        return json.loads(snap)
    except json.JSONDecodeError:
        return None


def route_open(r: redis.Redis, parts: list) -> None:
    """
    Route 'open' callback to orders queue.
    
    Format: open:<side>:<lot>:<sid>
    """
    if len(parts) < 4:
        return
    
    side, lot, sid = parts[1], parts[2], parts[3]
    sid = sid.strip()
    if not sid:
        print(f"⚠️  Ignoring open callback without sid: {parts}")
        return
    
    # Get signal snapshot
    snap = get_snapshot(r, sid)

    symbol = "XAUUSD"
    entry_price: Optional[float] = None
    sl_price: Optional[float] = None
    tp_levels = []
    atr_value: Optional[float] = None
    note = ""

    if snap:
        symbol = snap.get("symbol", symbol)
        entry_price = _parse_float(snap.get("price"))
        risk_data = snap.get("risk") or {}
        sl_price = _parse_float(risk_data.get("sl"))
        tp_levels = risk_data.get("tp_levels") or [
            risk_data.get("tp1"),
            risk_data.get("tp2"),
            risk_data.get("tp3"),
        ]
        atr_value = _parse_float(risk_data.get("atr"))
        note = snap.get("note", "")

    payload = {
        "action": "open",
        "side": side,
        "lot": float(lot),
        "sid": sid,
        "timestamp": int(time.time() * 1000),
        "symbol": symbol,
    }

    specs_store = SymbolSpecsStore(r)
    specs: SymbolSpecs = specs_store.get(symbol)
    min_distance = abs(specs.point * specs.min_stop_points)
    decimals = _decimals_from_point(specs.point)

    corrected = False
    original_sl = sl_price
    original_tps = list(tp_levels) if tp_levels else []

    if entry_price is not None:
        entry_price = _round_price(entry_price, specs.point)
        payload["entry"] = entry_price

    if atr_value is not None:
        payload["atr"] = atr_value

    if note:
        payload["note"] = note

    if sl_price is not None and entry_price is not None:
        adjusted_sl = _ensure_min_distance(entry_price, sl_price, side, min_distance)
        adjusted_sl = _round_price(adjusted_sl, specs.point)
        if not math.isclose(adjusted_sl, sl_price, rel_tol=0.0, abs_tol=10 ** (-decimals)):
            corrected = True
        sl_price = adjusted_sl
        payload["sl"] = sl_price

    filtered_tp = _filter_tp_levels(entry_price, tp_levels, side, min_distance)
    rounded_tp = [_round_price(tp, specs.point) for tp in filtered_tp]
    if rounded_tp:
        if len(rounded_tp) != len(original_tps) or any(
            not math.isclose(a or 0.0, b, rel_tol=0.0, abs_tol=10 ** (-decimals))
            for a, b in zip(original_tps[: len(rounded_tp)], rounded_tp)
        ):
            corrected = True
        payload["tp_levels"] = rounded_tp

    if corrected:
        print(
            f"⚙️  Коррекция SL/TP для sid={sid[:20]}... "
            f"(symbol={symbol}, min_distance={min_distance:.5f})"
        )

    # Push to queue
    r.lpush(ORDERS_QUEUE, json.dumps(payload))
    print(f"✅ Routed: open {side} {lot} lot (sid={sid[:20]}...)")


def route_sltp(r: redis.Redis, parts: list) -> None:
    """
    Route 'sltp:set' callback to orders queue.
    
    Format: sltp:set:<sid>
    """
    if len(parts) < 3:
        return
    
    sid = parts[2].strip()
    if not sid:
        print(f"⚠️  Ignoring sltp callback without sid: {parts}")
        return
    
    # Get signal snapshot
    snap = get_snapshot(r, sid)
    if not snap:
        print(f"⚠️  No snapshot for sid={sid[:20]}...")
        return
    
    symbol = snap.get("symbol", "XAUUSD")
    entry_price = _parse_float(snap.get("price"))
    side = snap.get("side", "LONG")
    risk_data = snap.get("risk") or {}
    sl_price = _parse_float(risk_data.get("sl"))
    tp_levels = risk_data.get("tp_levels") or [
        risk_data.get("tp1"),
        risk_data.get("tp2"),
        risk_data.get("tp3"),
    ]

    specs_store = SymbolSpecsStore(r)
    specs: SymbolSpecs = specs_store.get(symbol)
    min_distance = abs(specs.point * specs.min_stop_points)
    decimals = _decimals_from_point(specs.point)

    corrected = False
    original_sl = sl_price
    original_tps = list(tp_levels) if tp_levels else []

    if sl_price is not None and entry_price is not None:
        adjusted_sl = _ensure_min_distance(entry_price, sl_price, side, min_distance)
        adjusted_sl = _round_price(adjusted_sl, specs.point)
        if not math.isclose(adjusted_sl, sl_price, rel_tol=0.0, abs_tol=10 ** (-decimals)):
            corrected = True
        sl_price = adjusted_sl

    filtered_tp = _filter_tp_levels(entry_price, tp_levels, side, min_distance)
    rounded_tp = [_round_price(tp, specs.point) for tp in filtered_tp]
    if len(rounded_tp) != len(original_tps):
        if rounded_tp or original_tps:
            corrected = True
    else:
        if any(
            not math.isclose((orig or 0.0), new, rel_tol=0.0, abs_tol=10 ** (-decimals))
            for orig, new in zip(original_tps, rounded_tp)
        ):
            corrected = True

    payload = {
        "action": "modify",
        "sid": sid,
        "symbol": symbol,
        "timestamp": int(time.time() * 1000)
    }

    if sl_price is not None:
        payload["sl"] = sl_price
    if rounded_tp:
        payload["tp_levels"] = rounded_tp

    if corrected:
        print(
            f"⚙️  Коррекция SL/TP (modify) для sid={sid[:20]}... "
            f"(symbol={symbol}, min_distance={min_distance:.5f})"
        )

    # Push to queue
    r.lpush(ORDERS_QUEUE, json.dumps(payload))
    print(f"✅ Routed: modify SL/TP (sid={sid[:20]}...)")


def route_size(r: redis.Redis, parts: list) -> None:
    """
    Route 'size' callback to orders queue.
    
    Format: size:<multiplier>:<sid>
    """
    if len(parts) < 3:
        return
    
    mult, sid = parts[1], parts[2].strip()
    if not sid:
        print(f"⚠️  Ignoring size callback without sid: {parts}")
        return
    
    # Get signal snapshot
    snap = get_snapshot(r, sid)
    if not snap:
        print(f"⚠️  No snapshot for sid={sid[:20]}...")
        return
    
    # Calculate new lot
    original_lot = float(snap.get("lot", 0.1))
    new_lot = original_lot * float(mult)
    
    payload = {
        "action": "resize",
        "sid": sid,
        "symbol": snap.get("symbol", "XAUUSD"),
        "lot": round(new_lot, 2),
        "original_lot": original_lot,
        "multiplier": float(mult),
        "timestamp": int(time.time() * 1000)
    }
    
    # Push to queue
    r.lpush(ORDERS_QUEUE, json.dumps(payload))
    print(f"✅ Routed: resize x{mult} (sid={sid[:20]}...)")


def route_cancel(r: redis.Redis, parts: list) -> None:
    """
    Route 'cancel' callback to orders queue.
    
    Format: cancel::<sid>
    """
    if len(parts) < 2:
        return
    
    sid = parts[-1].strip()
    if not sid:
        print(f"⚠️  Ignoring cancel callback without sid: {parts}")
        return
    
    payload = {
        "action": "cancel",
        "sid": sid,
        "timestamp": int(time.time() * 1000)
    }
    
    # Push to queue
    r.lpush(ORDERS_QUEUE, json.dumps(payload))
    print(f"✅ Routed: cancel (sid={sid[:20]}...)")


def main():
    """Main entry point."""
    print(f"🔀 Orders Router starting...")
    print(f"   Callbacks: {CALLBACKS_STREAM}")
    print(f"   Orders Queue: {ORDERS_QUEUE}")
    print(f"   Snapshot Prefix: {SNAP_PREFIX}")
    print(f"   Group: {GROUP}")
    print(f"   Consumer: {CONSUMER}")
    print()
    
    # Connect to Redis
    r = redis.from_url(REDIS_URL, decode_responses=True)
    
    # Create consumer group
    try:
        r.xgroup_create(CALLBACKS_STREAM, GROUP, id='0', mkstream=True)
        print(f"✅ Created consumer group: {GROUP}")
    except redis.ResponseError:
        print(f"✅ Consumer group already exists: {GROUP}")
    
    print(f"📊 Listening for callbacks...")
    print()
    
    routed_count = 0
    
    # Main loop
    while True:
        msgs = r.xreadgroup(
            GROUP,
            CONSUMER,
            {CALLBACKS_STREAM: ">"},
            count=50,
            block=2000
        )
        
        for stream, entries in msgs or []:
            for msg_id, fields in entries:
                try:
                    cb = fields.get("callback", "")
                    parts = cb.split(":")
                    
                    if not parts:
                        continue
                    
                    action = parts[0]
                    
                    # Route based on action
                    if action == "open":
                        route_open(r, parts)
                        routed_count += 1
                    
                    elif action == "sltp":
                        route_sltp(r, parts)
                        routed_count += 1
                    
                    elif action == "size":
                        route_size(r, parts)
                        routed_count += 1
                    
                    elif action == "cancel":
                        route_cancel(r, parts)
                        routed_count += 1
                    
                    else:
                        print(f"⚠️  Unknown action: {action}")
                    
                    if routed_count % 10 == 0:
                        print(f"📊 Total routed: {routed_count}")
                
                except Exception as e:
                    print(f"❌ Error routing callback: {e}")
                
                finally:
                    # Always ACK
                    r.xack(stream, GROUP, msg_id)


if __name__ == "__main__":
    main()

