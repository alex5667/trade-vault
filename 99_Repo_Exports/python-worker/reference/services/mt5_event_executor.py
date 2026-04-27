from utils.time_utils import get_ny_time_millis
# -*- coding: utf-8 -*-
"""
MT5 Event Executor - Приём и классификация событий от MT5 EA.

Принимает вебхуки от MT5 (POST /events/mt5) и:
1. Классифицирует события (TP1/TP2/TP3/SL/OPEN/CLOSE)
2. Обновляет состояние сделки (trade:state:{sid})
3. Публикует события в streams (events:trades)
4. Интеграция с trade_events_logger для trade_back

Интегрировано с scanner_infra:
- FastAPI для HTTP endpoint
- Redis для состояния и событий
- trade_events_logger для полного логирования
- Graceful shutdown
- Health checks
- Prometheus metrics
"""

import os
import json
import time
from typing import Any, Dict, List, Optional

import redis
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from common.log import setup_logger

# Import trade_events_logger если доступен
try:
    from services.trade_events_logger import TradeEventsLogger
    HAS_EVENTS_LOGGER = True
except ImportError:
    HAS_EVENTS_LOGGER = False
    TradeEventsLogger = None

# Import for robust R-mult calculation
try:
    from services.pnl_math import get_symbol_info, spec_from_symbol_info
except ImportError:
    pass

log = setup_logger("mt5_event_executor")

# ═══════════════════════════════════════════════════════════════
# Configuration
# ═══════════════════════════════════════════════════════════════

REDIS_URL = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
EVENT_STREAM = os.getenv("TRADE_EVENTS_STREAM", "events:trades")
SIGNAL_PREFIX = os.getenv("SIGNAL_PREFIX", "signals:")
TRADE_STATE_PREFIX = os.getenv("TRADE_STATE_PREFIX", "trade:state:")

# Допуск по цене для определения TP/SL (в единицах инструмента)
PRICE_TOLERANCE = float(os.getenv("PRICE_TOLERANCE", "0.5"))

# Redis connection
r = redis.from_url(REDIS_URL, decode_responses=True)

# Trade events logger
events_logger = None
if HAS_EVENTS_LOGGER:
    events_logger = TradeEventsLogger(REDIS_URL)
    log.info("✅ Trade events logger initialized")

# FastAPI app
app = FastAPI(
    title="MT5 Event Executor",
    version="1.0.0",
    description="Receives and classifies trading events from MT5 EA"
)

# ═══════════════════════════════════════════════════════════════
# Pydantic Models
# ═══════════════════════════════════════════════════════════════

class MT5Event(BaseModel):
    """
    Событие от MT5 EA.
    
    Формат соответствует OnTradeTransaction в MT5.
    """
    symbol: str
    deal: int
    position: int
    type: int              # Type сделки (DEAL_TYPE_BUY=0, DEAL_TYPE_SELL=1)
    price: float
    profit: float
    comment: Optional[str] = None    # sid сигнала
    volume: Optional[float] = None   # Объём сделки
    ts: Optional[int] = None         # Timestamp (если MT5 не шлёт - ставим сервером)


# ═══════════════════════════════════════════════════════════════
# Helper Functions
# ═══════════════════════════════════════════════════════════════

def load_signal(sid: str) -> Optional[Dict[str, Any]]:
    """Загрузить исходный сигнал из Redis."""
    try:
        raw = r.get(f"{SIGNAL_PREFIX}{sid}")
        if not raw:
            log.warning("Signal not found in Redis: %s", sid)
            return None
        return json.loads(raw)
    except Exception as e:
        log.error("Error loading signal %s: %s", sid, str(e))
        return None


def load_trade_state(sid: str) -> Dict[str, Any]:
    """
    Загрузить состояние сделки из Redis.
    
    Если не существует - создаёт новое.
    """
    key = f"{TRADE_STATE_PREFIX}{sid}"
    
    if not r.exists(key):
        # Создаём новый state
        return {
            "sid": sid,
            "tp1_hit": False,
            "tp2_hit": False,
            "tp3_hit": False,
            "sl_hit": False,
            "opened_at": None,
            "closed_at": None,
            "last_event_ts": None,
            "pnl_realized": 0.0,
            "events": [],
            "volume_opened": 0.0,
            "volume_closed": 0.0
        }
    
    try:
        data = r.get(key)
        return json.loads(data)
    except Exception as e:
        log.error("Error loading trade state %s: %s", sid, str(e))
        return load_trade_state.__defaults__[0]  # Новый state


def save_trade_state(state: Dict[str, Any], ttl: int = 604800):
    """
    Сохранить состояние сделки в Redis.
    
    Args:
        state: Состояние сделки
        ttl: TTL в секундах (default 7 дней)
    """
    sid = state["sid"]
    key = f"{TRADE_STATE_PREFIX}{sid}"
    
    try:
        r.set(key, json.dumps(state), ex=ttl)
        log.debug("Trade state saved: %s", sid)
    except Exception as e:
        log.error("Error saving trade state %s: %s", sid, str(e))


def append_event_to_stream(evt: Dict[str, Any]):
    """Добавить событие в Redis stream для trade_back."""
    try:
        # Конвертируем все поля в строки для Redis
        stream_data = {}
        for key, value in evt.items():
            if isinstance(value, (dict, list)):
                stream_data[key] = json.dumps(value)
            else:
                stream_data[key] = str(value)
        
        msg_id = r.xadd(EVENT_STREAM, stream_data, maxlen=10000)
        log.debug("Event published to stream: %s (id=%s)", evt.get("event_type"), msg_id)
    except Exception as e:
        log.error("Error publishing to stream: %s", str(e))


def price_close_enough(price: float, target: float, tolerance: float = None) -> bool:
    """Проверка попадания цены в уровень с допуском."""
    if tolerance is None:
        tolerance = PRICE_TOLERANCE
    return abs(price - target) <= tolerance


def classify_fill(event: MT5Event, signal: Optional[Dict[str, Any]]) -> Dict[str, str]:
    """
    Классифицировать событие MT5.
    
    Args:
        event: Событие от MT5
        signal: Исходный сигнал из Redis
        
    Returns:
        Dict с event_type и reason
    """
    result = {
        "event_type": "UNKNOWN",
        "reason": "not_classified"
    }
    
    if signal is None:
        result["event_type"] = "UNKNOWN"
        result["reason"] = "signal_not_found"
        return result
    
    side = signal.get("side", "LONG")
    sl = float(signal.get("sl", 0.0))
    tp_levels: List[float] = signal.get("tp_levels", [])
    price = event.price
    
    # OPEN - определяем по нулевой прибыли
    if abs(event.profit) < 0.01:
        result["event_type"] = "POSITION_OPENED"
        result["reason"] = "zero_profit_detected"
        return result
    
    # SL - проверяем срабатывание stop loss
    if sl > 0:
        if side == "LONG" and price <= sl + PRICE_TOLERANCE:
            result["event_type"] = "SL_HIT"
            result["reason"] = f"price {price:.2f} <= sl {sl:.2f}"
            return result
        elif side == "SHORT" and price >= sl - PRICE_TOLERANCE:
            result["event_type"] = "SL_HIT"
            result["reason"] = f"price {price:.2f} >= sl {sl:.2f}"
            return result
    
    # TP levels - проверяем достижение take profit'ов
    if tp_levels:
        for idx, tp in enumerate(tp_levels[:3], 1):  # Максимум 3 уровня
            if side == "LONG":
                if price >= tp - PRICE_TOLERANCE:
                    result["event_type"] = f"TP{idx}_HIT"
                    result["reason"] = f"price {price:.2f} >= tp{idx} {tp:.2f}"
                    return result
            else:  # SHORT
                if price <= tp + PRICE_TOLERANCE:
                    result["event_type"] = f"TP{idx}_HIT"
                    result["reason"] = f"price {price:.2f} <= tp{idx} {tp:.2f}"
                    return result
    
    # Если прибыль отрицательная но не SL - возможно manual close
    if event.profit < 0:
        result["event_type"] = "POSITION_CLOSED"
        result["reason"] = "negative_profit_manual_close"
    elif event.profit > 0:
        result["event_type"] = "POSITION_CLOSED"
        result["reason"] = "positive_profit_manual_close"
    
    return result


# ═══════════════════════════════════════════════════════════════
# API Endpoints
# ═══════════════════════════════════════════════════════════════

@app.post("/events/mt5")
def receive_mt5_event(event: MT5Event):
    """
    Приём события от MT5 EA.
    
    MT5 шлёт POST на этот endpoint при каждой сделке.
    """
    # 1. Получаем sid из комментария
    sid = event.comment
    if not sid:
        log.warning("Event without comment (sid): %s", event.model_dump())
        raise HTTPException(400, "comment (sid) is required")
    
    log.info(
        "📥 MT5 event: sid=%s symbol=%s price=%.2f profit=%.2f",
        sid, event.symbol, event.price, event.profit
    )
    
    # 2. Загружаем исходный сигнал
    signal = load_signal(sid)
    
    # 3. Классифицируем событие
    classified = classify_fill(event, signal)
    event_type = classified["event_type"]
    reason = classified.get("reason", "")
    
    log.info(
        "🎯 Classified as: %s (reason: %s)",
        event_type, reason
    )
    
    # 4. Загружаем текущее состояние сделки
    state = load_trade_state(sid)
    
    # Timestamp
    now_ms = event.ts or get_ny_time_millis()
    
    # 5. Обновляем state
    state_event = {
        "ts": now_ms,
        "event_type": event_type,
        "price": event.price,
        "profit": event.profit,
        "deal": event.deal,
        "position": event.position,
        "volume": event.volume or 0.0,
        "reason": reason
    }
    
    state["events"].append(state_event)
    state["last_event_ts"] = now_ms
    
    # Обработка по типу события
    if event_type == "POSITION_OPENED":
        state["opened_at"] = now_ms
        if event.volume:
            state["volume_opened"] += event.volume
    
    elif event_type == "TP1_HIT":
        state["tp1_hit"] = True
        state["pnl_realized"] = round(state.get("pnl_realized", 0.0) + event.profit, 2)
        if event.volume:
            state["volume_closed"] += event.volume
        
        # Логируем через TradeEventsLogger
        if events_logger:
            events_logger.log_tp1_hit(
                sid=sid,
                symbol=event.symbol,
                price=event.price,
                position_id=str(event.position),
                lot=event.volume,
                source="mt5"
            )
    
    elif event_type == "TP2_HIT":
        state["tp2_hit"] = True
        state["pnl_realized"] = round(state.get("pnl_realized", 0.0) + event.profit, 2)
        if event.volume:
            state["volume_closed"] += event.volume
        
        if events_logger:
            events_logger.log_tp2_hit(
                sid=sid,
                symbol=event.symbol,
                price=event.price,
                position_id=str(event.position),
                lot=event.volume,
                source="mt5"
            )
    
    elif event_type == "TP3_HIT":
        state["tp3_hit"] = True
        state["pnl_realized"] = round(state.get("pnl_realized", 0.0) + event.profit, 2)
        if event.volume:
            state["volume_closed"] += event.volume
        
        if events_logger:
            events_logger.log_tp3_hit(
                sid=sid,
                symbol=event.symbol,
                price=event.price,
                position_id=str(event.position),
                lot=event.volume,
                source="mt5"
            )
    
    elif event_type == "SL_HIT":
        state["sl_hit"] = True
        state["closed_at"] = now_ms
        state["pnl_realized"] = round(state.get("pnl_realized", 0.0) + event.profit, 2)
        if event.volume:
            state["volume_closed"] += event.volume
        
        # Определяем причину SL
        sl_reason = "normal_sl"
        if state.get("tp1_hit"):
            sl_reason = "tp1_then_sl"  # Критичная метрика!
        
        if events_logger:
            events_logger.log_sl_hit(
                sid=sid,
                symbol=event.symbol,
                price=event.price,
                position_id=str(event.position),
                lot=event.volume,
                source="mt5",
                reason=sl_reason
            )
    
    elif event_type == "POSITION_CLOSED":
        state["closed_at"] = now_ms
        state["pnl_realized"] = round(state.get("pnl_realized", 0.0) + event.profit, 2)
        if event.volume:
            state["volume_closed"] += event.volume
        
        # Определяем причину закрытия
        if state.get("tp3_hit"):
            close_reason = "tp3"
        elif state.get("tp2_hit"):
            close_reason = "tp2"
        elif state.get("tp1_hit"):
            close_reason = "tp1"
        elif event.profit < 0:
            close_reason = "loss_manual"
        else:
            close_reason = "profit_manual"
        
        if events_logger:
            # Extract AB data from signal payload or top-level if available
            ab_arm = str(signal.get("ab_arm") or (signal.get("payload") or {}).get("ab_arm") or "A")
            ab_group = str(signal.get("ab_group") or (signal.get("payload") or {}).get("ab_group") or "default")
            ab_key = str(signal.get("ab_key") or (signal.get("payload") or {}).get("ab_key") or "")
            regime = str(signal.get("regime") or (signal.get("payload") or {}).get("regime") or "na")
            zone_id = str(signal.get("zone_id") or (signal.get("payload") or {}).get("zone_id") or "")

            # Calculate R-multiple
            r_mult = 0.0
            risk_usd = 0.0
            try:
                # 1. Try explicit risk from signal
                risk_usd = float(signal.get("risk_usd") or (signal.get("payload") or {}).get("risk_usd") or 0.0)
                
                # 2. Fallback: calculate from SL distance
                if risk_usd <= 0 and "spec_from_symbol_info" in globals():
                    try:
                        entry_px = float(signal.get("entry_price") or 0.0)
                        sl_px = float(signal.get("sl") or 0.0)
                        lot = float(event.volume or 0.0)
                        side = str(signal.get("side") or (signal.get("payload") or {}).get("side") or "LONG")
                        
                        if entry_px > 0 and sl_px > 0 and lot > 0:
                            # Use new get_symbol_info which supports redis fallback locally
                            spec_info = get_symbol_info(event.symbol, r)
                            spec = spec_from_symbol_info(spec_info)
                            risk_usd = spec.risk_money(entry_px, sl_px, lot, side, symbol=event.symbol)
                    except Exception:
                        pass
                
                if risk_usd > 1e-9:
                    r_mult = event.profit / risk_usd
            except Exception:
                pass

            # A3: POSITION_CLOSED must carry join-critical fields (sid/order_id/ts_fill_ms/qty/fee_bps/side/venue).
            # We keep it fail-open and route any contract violations to DLQ inside TradeEventsLogger.
            close_ts_ms = int(event.ts or now_ms or get_ny_time_millis())
            # Normalize side from signal payload
            side_norm = str(
                signal.get("side")
                or (signal.get("payload") or {}).get("side")
                or "LONG"
            ).upper()
            # For MT5 we map deal id as order_id (unique per fill), position id remains position_id.
            events_logger.log_position_closed(
                sid=sid,
                symbol=event.symbol,
                close_price=float(event.price),
                pnl=float(event.profit),
                position_id=str(event.position),
                lot=float(event.volume or 0.0),
                source="mt5",
                close_reason=close_reason,
                # A3 time contract: use exchange close timestamp as ts
                ts_ms=close_ts_ms,
                exit_ts_ms=close_ts_ms,
                # A3 join-critical exec fields
                order_id=str(event.deal),  # deal id is unique per fill in MT5
                side=side_norm,
                venue="mt5",
                qty=float(event.volume or 0.0),
                fee_bps=0.0,  # MT5 does not report fees separately
                # Legacy AB/regime fields go via metadata (backward compat via **legacy_kwargs)
                ab_arm=ab_arm,
                ab_group=ab_group,
                ab_key=ab_key,
                regime=regime,
                zone_id=zone_id,
                payload={
                    "risk_usd": float(risk_usd),
                    "r_mult": float(r_mult),
                    "exit_ts_ms": int(close_ts_ms),
                    "ts_fill_ms": int(close_ts_ms),
                    "order_id": str(event.deal),
                    "qty": float(event.volume or 0.0),
                    "side": side_norm,
                    "venue": "mt5",
                    "fee_bps": 0.0,
                }
            )
    
    # 6. Сохраняем обновлённый state
    save_trade_state(state)
    
    # 7. Публикуем в stream для trade_back
    stream_event = {
        "sid": sid,
        "symbol": event.symbol,
        "event_type": event_type,
        "price": event.price,
        "profit": event.profit,
        "deal": event.deal,
        "position": event.position,
        "ts": now_ms,
        "reason": reason,
        "state": state  # Полное состояние для анализа
    }
    append_event_to_stream(stream_event)
    
    log.info(
        "✅ Event processed: %s for %s (pnl_total=%.2f)",
        event_type, sid, state["pnl_realized"]
    )
    
    return {
        "ok": True,
        "sid": sid,
        "event_type": event_type,
        "reason": reason,
        "state": {
            "tp1_hit": state["tp1_hit"],
            "tp2_hit": state["tp2_hit"],
            "tp3_hit": state["tp3_hit"],
            "sl_hit": state["sl_hit"],
            "pnl_realized": state["pnl_realized"]
        }
    }


@app.get("/health")
def health_check():
    """Health check endpoint."""
    try:
        r.ping()
        redis_ok = True
    except Exception:
        redis_ok = False
    
    return {
        "status": "healthy" if redis_ok else "unhealthy",
        "redis": "connected" if redis_ok else "disconnected",
        "events_logger": HAS_EVENTS_LOGGER,
        "timestamp": get_ny_time_millis()
    }


@app.get("/stats")
def get_stats():
    """Статистика обработанных событий."""
    try:
        # Подсчитываем количество событий в stream
        stream_len = r.xlen(EVENT_STREAM)
        
        # Количество сделок в состоянии
        trade_states = len(r.keys(f"{TRADE_STATE_PREFIX}*"))
        
        return {
            "events_in_stream": stream_len,
            "trade_states": trade_states,
            "events_logger_stats": events_logger.get_stats() if events_logger else {}
        }
    except Exception as e:
        log.error("Error getting stats: %s", str(e))
        return {"error": str(e)}


@app.get("/signal/{sid}/state")
def get_signal_state(sid: str):
    """Получить состояние сделки по sid."""
    state = load_trade_state(sid)
    
    if not state or not state.get("opened_at"):
        raise HTTPException(404, f"Trade state not found for {sid}")
    
    return state


@app.get("/signal/{sid}/events")
def get_signal_events(sid: str):
    """Получить все события по сигналу."""
    if events_logger:
        events = events_logger.get_signal_events(sid)
        return {"sid": sid, "events": events, "count": len(events)}
    else:
        # Fallback: читаем из state
        state = load_trade_state(sid)
        return {"sid": sid, "events": state.get("events", []), "count": len(state.get("events", []))}


# ═══════════════════════════════════════════════════════════════
# Main Entry Point
# ═══════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import uvicorn
    
    host = os.getenv("MT5_EXECUTOR_HOST", "0.0.0.0")
    port = int(os.getenv("MT5_EXECUTOR_PORT", "8091"))
    
    log.info("=" * 80)
    log.info("MT5 Event Executor Service")
    log.info("=" * 80)
    log.info("Host: %s", host)
    log.info("Port: %d", port)
    log.info("Redis: %s", REDIS_URL)
    log.info("Events stream: %s", EVENT_STREAM)
    log.info("Events logger: %s", "enabled" if HAS_EVENTS_LOGGER else "disabled")
    log.info("=" * 80)
    
    uvicorn.run(
        app,
        host=host,
        port=port,
        log_level="info"
    )

