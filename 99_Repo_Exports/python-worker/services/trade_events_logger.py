from utils.time_utils import get_ny_time_millis
# -*- coding: utf-8 -*-
"""
Trade Events Logger - Логирование всех торговых событий для trade_back.

Записывает все события (TP1_HIT, TP2_HIT, TRAILING_MOVE, SL_HIT) в:
1. Redis hash: trade:events:{sid} - полная история событий по сигналу
2. Redis stream: events:trades - глобальный поток событий

Для trade_back это позволит:
- Рассчитать winrate/ROC
- Анализировать "как далеко мы смогли утащить"
- Строить графики движения SL
- Оценивать эффективность профилей трейлинга

Интегрировано с scanner_infra:
- Redis streams и hashes
- TTL для автоочистки
- Структурированный формат
- Полнота данных для анализа
"""

import json
import os
import time
import hashlib
import redis
from typing import Dict, Any, Optional, List
from dataclasses import dataclass, asdict
from copy import deepcopy

from common.log import setup_logger

# A3: строгий контракт для POSITION_CLOSED + DLQ-хелпер (fail-open) — V2 API
try:
    from services.posttrade.trade_events_contract import (
        normalize_position_closed_event,
        validate_position_closed_event,
    )
    from services.posttrade.redis_stream_dlq import publish_dlq_sync
except Exception:  # pragma: no cover — fallback for direct-run
    from posttrade.trade_events_contract import normalize_position_closed_event, validate_position_closed_event  # type: ignore
    from posttrade.redis_stream_dlq import publish_dlq_sync  # type: ignore

log = setup_logger("trade_events_logger")


@dataclass(slots=True)
class TradeEvent:
    """
    Структура торгового события.
    
    Attributes:
        event_type: Тип события (TP1_HIT, TP2_HIT, TRAILING_MOVE, SL_HIT, etc)
        sid: Signal ID
        symbol: Символ ( BTCUSD, etc)
        ts: Timestamp в миллисекундах
        price: Цена события (опционально)
        new_sl: Новый SL (для TRAILING_MOVE)
        new_tp: Новый TP (опционально)
        position_id: ID позиции MT5
        lot: Объём
        pnl: P&L (для закрытия позиции)
        profile: Профиль трейлинга (для TRAILING_MOVE)
        source: Источник события (mt5, paper_executor, backtest)
        metadata: Дополнительные данные
    """
    event_type: str
    sid: str
    symbol: str
    ts: int
    price: Optional[float] = None
    new_sl: Optional[float] = None
    new_tp: Optional[float] = None
    position_id: Optional[str] = None
    lot: Optional[float] = None
    pnl: Optional[float] = None
    profile: Optional[str] = None
    source: str = "unknown"
    v: int = 1
    metadata: Optional[Dict[str, Any]] = None
    payload: Optional[Dict[str, Any]] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Конвертация в dict для Redis."""
        data = asdict(self)
        # Убираем None значения
        return {k: v for k, v in data.items() if v is not None}


from copy import deepcopy

# ------------------------------
# Helpers (unit-testable)
# ------------------------------
def _merge_close_metadata(
    *,
    close_reason: Optional[str],
    ab_arm: Optional[str] = None,
    ab_group: Optional[str] = None,
    ab_key: Optional[str] = None,
    arm_ver: Optional[int] = None,
    regime: Optional[str] = None,
    zone_id: Optional[str] = None,
    base: Optional[Dict[str, Any]] = None,
) -> Optional[Dict[str, Any]]:
    """
    Build metadata for POSITION_CLOSED in a backward-compatible way.
    Returns None if nothing to attach.
    """
    md: Dict[str, Any] = deepcopy(base) if isinstance(base, dict) else {}
    if close_reason:
        md["close_reason"] = str(close_reason)
    if ab_arm:
        md["ab_arm"] = str(ab_arm)
    if ab_group:
        md["ab_group"] = str(ab_group)
    if ab_key:
        md["ab_key"] = str(ab_key)
    if arm_ver is not None:
        try:
            md["arm_ver"] = int(arm_ver)
        except Exception:
            md["arm_ver"] = str(arm_ver)
    if regime:
        md["regime"] = str(regime)
    if zone_id:
        md["zone_id"] = str(zone_id)
    return md or None


class TradeEventsLogger:
    """
    Логгер торговых событий для trade_back анализа.
    
    Пишет события в:
    1. trade:events:{sid} - list с полной историей по сигналу
    2. events:trades - stream со всеми событиями
    3. trade:timeline:{sid} - sorted set для временной последовательности
    """
    
    def __init__(self, redis_url: Optional[str] = None):
        """
        Args:
            redis_url: URL Redis (если None, берётся из REDIS_URL env)
        """
        self.redis_url = redis_url or os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
        # P2: ConnectionPool вместо голого from_url() — ограничиваем количество соединений
        _max_conn = int(os.getenv("TRADE_EVENTS_REDIS_MAX_CONNECTIONS", "20"))
        _pool = redis.ConnectionPool.from_url(
            self.redis_url,
            decode_responses=True,
            max_connections=_max_conn,
            socket_keepalive=True,
            health_check_interval=60,
        )
        self.r = redis.Redis(connection_pool=_pool)
        
        # Конфигурация
        self.events_stream = os.getenv("TRADE_EVENTS_STREAM", "events:trades")
        # ВАЖНО: maxlen конфигурируется через ENV для координации с archiver
        # archiver читает из stream и ack'ает -> можно держать меньший буфер
        # если archiver отстает, события накапливаются до maxlen, затем trimming
        self.events_stream_maxlen = int(os.getenv("TRADE_EVENTS_MAXLEN", "200000"))
        self.events_ttl = int(os.getenv("TRADE_EVENTS_TTL", "604800"))  # 7 дней
        self.per_sid_list_maxlen = int(os.getenv("TRADE_EVENTS_PER_SID_MAXLEN", "10000"))
        self.idempo_ttl_days = int(os.getenv("TRADE_EVENTS_IDEMPOTENCY_TTL_DAYS", "14"))
        self.idempo_ttl_sec = self.idempo_ttl_days * 86400

        # A3: DLQ-конфигурация для битых POSITION_CLOSED событий (fail-open)
        self.dlq_stream = os.getenv("TRADE_EVENTS_DLQ_STREAM", "events:trades:dlq")
        self.dlq_maxlen = int(os.getenv("TRADE_EVENTS_DLQ_MAXLEN", "200000"))
        # TTL for bad-event idempotency key (prevents DLQ spam for repeated bad events)
        self.bad_idempo_ttl_sec = int(os.getenv("TRADE_EVENTS_BAD_IDEMPO_TTL_SEC", "86400"))  # 1d
        
        # Статистика
        self.stats = {
            "events_written": 0,
            "tp1_hits": 0,
            "tp2_hits": 0,
            "tp3_hits": 0,
            "sl_hits": 0,
            "trailing_moves": 0,
            "trailing_started": 0,
            "position_opened": 0,
            "position_closed": 0
        }
        
        log.info(
            "✅ TradeEventsLogger initialized | stream=%s ttl=%ds",
            self.events_stream, self.events_ttl
        )
    
    @staticmethod
    def _mk_event_id(e: TradeEvent) -> str:
        """Создает уникальный ID события для идемпотентности."""
        base = f"{e.event_type}|{e.sid}|{e.ts}|{e.price or ''}|{e.lot or ''}|{e.pnl or ''}|{e.position_id or ''}"
        return hashlib.sha1(base.encode("utf-8"), usedforsecurity=False).hexdigest()

    def log_event(self, event: TradeEvent) -> str:
        """
        Записать событие в Redis с идемпотентностью.

        Пишет в:
          1) events:trades (stream)      — онлайн шина событий для downstream
          2) trade:events:{sid} (list)   — история по сигналу (полная, без trimming тяжелых полей)
          3) trade:timeline:{sid} (zset) — временной индекс

        Особый случай: POSITION_CLOSED
        -----------------------------
        Для закрытия позиции downstream критичны поля:
          sid, symbol, ts/exit_ts_ms (epoch ms), side, order_id, qty, fee_bps, risk_usd, r_mult.

        Поэтому POSITION_CLOSED проходит строгую нормализацию/валидацию
        (см. services.posttrade.trade_events_contract V2). Если событие не соответствует
        контракту — оно уходит в DLQ stream (TRADE_EVENTS_DLQ_STREAM) и НЕ блокирует торговый поток.
        Плохое событие деплицируется отдельным ключом (bad_idempo_ttl_sec) чтобы не спамить DLQ.
        """
        try:
            event_id = self._mk_event_id(event)

            # --- Build stream payload (dict) ---
            event_dict = event.to_dict()
            extra_payload = event_dict.pop("payload", None)

            stream_payload: Dict[str, Any] = {"event_id": event_id, **event_dict}
            if isinstance(extra_payload, dict):
                stream_payload.update(extra_payload)

            # Ensure meta alias exists (backward compatibility for consumers)
            if "metadata" in stream_payload and "meta" not in stream_payload:
                stream_payload["meta"] = stream_payload.get("metadata")

            # POSITION_CLOSED: validate before idempotency check.
            # Failure → DLQ, using a separate bad-event idempo key (not the main one).
            if event.event_type == "POSITION_CLOSED":
                # Deduplicate repeated bad events (avoid DLQ spam)
                bad_key = f"idempo:trade_event_bad:{event_id}"
                try:
                    if self.r.get(bad_key):
                        return event_id  # already sent to DLQ
                except Exception:
                    pass

                normalized, errs = normalize_position_closed_event(stream_payload)
                if errs:
                    publish_dlq_sync(
                        self.r,
                        dlq_stream=self.dlq_stream,
                        reason="position_closed_contract_violation",
                        error=";".join(errs),
                        src_stream=self.events_stream,
                        src_entry_id="*",  # before xadd — entry_id not yet known
                        payload=stream_payload,
                        maxlen=self.dlq_maxlen,
                    )
                    log.warning(
                        "⚠️  POSITION_CLOSED rejected by contract, routed to DLQ | sid=%s errs=%s",
                        event.sid, errs,
                    )
                    try:
                        # Mark bad event so we don't spam DLQ on retry
                        self.r.set(bad_key, "1", nx=True, ex=self.bad_idempo_ttl_sec)
                    except Exception:
                        pass
                    return event_id  # fail-open: return event_id, do NOT crash

                stream_data = dict(normalized)  # already strings (V2 API)
            else:
                # Generic event: stringify values for Redis Stream
                stream_data: Dict[str, str] = {}
                for key, value in stream_payload.items():
                    if isinstance(value, (dict, list)):
                        stream_data[key] = json.dumps(value, ensure_ascii=False)
                    else:
                        stream_data[key] = str(value)

                if "metadata" in stream_data and "meta" not in stream_data:
                    stream_data["meta"] = stream_data["metadata"]

            # Idempotency: only for events we actually accept.
            idempo_key = f"idempo:trade_event:{event_id}"
            if not self.r.set(idempo_key, "1", nx=True, ex=self.idempo_ttl_sec):
                return event_id  # already written

            # P2: Strip heavy analytics fields from stream — они раздувают events:trades до 4KB/entry.
            # Полный payload лежит в PostgreSQL через stream-archiver и в trade:events:{sid} Redis list.
            _STREAM_STRIP_FIELDS = frozenset({
                "config_snapshot", "calibrated_specs", "indicators_snapshot",
                "trail_profile_config", "evidence", "feature_vector",
                "trail_profile", "raw_signal", "signal_payload",
            })
            stripped_stream_data = {k: v for k, v in stream_data.items() if k not in _STREAM_STRIP_FIELDS}

            # 1) Stream (online bus)
            self.r.xadd(self.events_stream, stripped_stream_data, maxlen=self.events_stream_maxlen, approximate=True)

            # 2) Per-sid list (full dict, nested payload preserved for offline analysis)
            full_storage_payload = {"event_id": event_id, **event_dict}
            if extra_payload:
                full_storage_payload["payload"] = extra_payload

            events_key = f"trade:events:{event.sid}"
            per_list = json.dumps(full_storage_payload, ensure_ascii=False)
            self.r.rpush(events_key, per_list)
            self.r.ltrim(events_key, -self.per_sid_list_maxlen, -1)
            self.r.expire(events_key, self.events_ttl)

            # 3) Timeline zset (full payload)
            timeline_key = f"trade:timeline:{event.sid}"
            timeline_value = json.dumps(full_storage_payload, ensure_ascii=False)
            self.r.zadd(timeline_key, {timeline_value: float(event.ts)})
            self.r.expire(timeline_key, self.events_ttl)

            self.stats["events_written"] += 1
            event_type_key = event.event_type.lower().replace("_hit", "_hits")
            if event_type_key in self.stats:
                self.stats[event_type_key] += 1

            log.debug("📝 Event logged: %s for %s (event_id=%s)", event.event_type, event.sid, event_id)
            return event_id

        except Exception as e:
            log.error(
                "❌ Failed to log event %s for %s: %s",
                getattr(event, "event_type", "?"), getattr(event, "sid", "?"), str(e)
            )
            return ""

    def log_tp1_hit(self, sid: str, symbol: str, price: float, position_id: Optional[str] = None, lot: Optional[float] = None, source: str = "mt5") -> str:
        event = TradeEvent(event_type="TP1_HIT", sid=sid, symbol=symbol, ts=get_ny_time_millis(), price=price, position_id=position_id, lot=lot, source=source)
        return self.log_event(event)

    def log_tp2_hit(self, sid: str, symbol: str, price: float, position_id: Optional[str] = None, lot: Optional[float] = None, source: str = "mt5") -> str:
        event = TradeEvent(event_type="TP2_HIT", sid=sid, symbol=symbol, ts=get_ny_time_millis(), price=price, position_id=position_id, lot=lot, source=source)
        return self.log_event(event)

    def log_tp3_hit(self, sid: str, symbol: str, price: float, position_id: Optional[str] = None, lot: Optional[float] = None, source: str = "mt5") -> str:
        event = TradeEvent(event_type="TP3_HIT", sid=sid, symbol=symbol, ts=get_ny_time_millis(), price=price, position_id=position_id, lot=lot, source=source)
        return self.log_event(event)

    def log_sl_hit(self, sid: str, symbol: str, price: float, position_id: Optional[str] = None, lot: Optional[float] = None, source: str = "mt5", reason: Optional[str] = None) -> str:
        metadata = {"reason": reason} if reason else None
        event = TradeEvent(event_type="SL_HIT", sid=sid, symbol=symbol, ts=get_ny_time_millis(), price=price, position_id=position_id, lot=lot, source=source, metadata=metadata)
        return self.log_event(event)

    def log_trailing_move(self, sid: str, symbol: str, new_sl: float, current_price: Optional[float] = None, profile: str = "unknown", position_id: Optional[str] = None, source: str = "tp_hit_trailing_orchestrator", distance_from_entry: Optional[float] = None, atr: Optional[float] = None) -> str:
        metadata = {}
        if distance_from_entry is not None: metadata["distance_from_entry"] = distance_from_entry
        if atr is not None: metadata["atr"] = atr
        if current_price is not None: metadata["current_price"] = current_price
        event = TradeEvent(event_type="TRAILING_MOVE", sid=sid, symbol=symbol, ts=get_ny_time_millis(), new_sl=new_sl, profile=profile, position_id=position_id, source=source, metadata=metadata if metadata else None)
        return self.log_event(event)

    def log_trailing_started(self, sid: str, symbol: str, profile: str, initial_sl: Optional[float] = None, tp1_price: Optional[float] = None, position_id: Optional[str] = None, source: str = "tp_hit_trailing_orchestrator") -> str:
        metadata = {}
        if initial_sl is not None: metadata["initial_sl"] = initial_sl
        if tp1_price is not None: metadata["tp1_price"] = tp1_price
        event = TradeEvent(event_type="TRAILING_STARTED", sid=sid, symbol=symbol, ts=get_ny_time_millis(), profile=profile, position_id=position_id, source=source, metadata=metadata if metadata else None)
        return self.log_event(event)

    def log_position_opened(self, sid: str, symbol: str, price: float, lot: float, sl: float, tp_levels: List[float], position_id: Optional[str] = None, source: str = "mt5") -> str:
        event = TradeEvent(event_type="POSITION_OPENED", sid=sid, symbol=symbol, ts=get_ny_time_millis(), price=price, lot=lot, position_id=position_id, source=source, metadata={"sl": sl, "tp_levels": tp_levels, "entry": price})
        return self.log_event(event)

    def log_position_closed(
        self,
        sid: str,
        symbol: str,
        close_price: float,
        pnl: float,
        position_id: Optional[str] = None,
        lot: Optional[float] = None,
        source: str = "mt5",
        close_reason: Optional[str] = None,
        # A3: explicit time + fill join fields
        ts_ms: Optional[int] = None,
        exit_ts_ms: Optional[int] = None,
        order_id: Optional[str] = None,
        side: Optional[str] = None,
        venue: Optional[str] = None,
        qty: Optional[float] = None,
        fee_bps: Optional[float] = None,
        bid_at_fill: Optional[float] = None,
        ask_at_fill: Optional[float] = None,
        mid_at_fill: Optional[float] = None,
        # Existing: structured metadata and extra payload
        metadata: Optional[Dict[str, Any]] = None,
        payload: Optional[Dict[str, Any]] = None,
        extra_payload: Optional[Dict[str, Any]] = None,
        # Backward-compatible sink for older callsites that pass ab_arm/regime/etc as kwargs
        # (previously these were explicit positional args that caused TypeError).
        **legacy_kwargs: Any,
    ) -> str:
        """
        Записать событие POSITION_CLOSED.

        Contract notes (важно для downstream):
        - sid: join key (обязателен)
        - ts/exit_ts_ms/ts_fill_ms: epoch ms (детерминированный — из ts_ms/exit_ts_ms)
        - side (LONG/SHORT), order_id, qty, fee_bps: required for TCA/execution joins

        Fail-open:
        - Мы никогда не бросаем исключение из этого метода.
        - Валидация контракта происходит в log_event(); при нарушениях событие уйдет в DLQ.
        """
        # --- metadata ---
        md: Dict[str, Any] = {}
        if isinstance(metadata, dict):
            md.update(deepcopy(metadata))

        # Accept legacy fields into metadata in a deterministic way
        # (mt5_event_executor historically passed ab_arm/regime/zone_id as direct kwargs).
        for k in ("ab_arm", "ab_group", "ab_key", "regime", "zone_id", "scenario", "scenario_v4"):
            if k in legacy_kwargs and legacy_kwargs.get(k) is not None:
                md.setdefault(k, legacy_kwargs.get(k))

        if close_reason and "close_reason" not in md:
            md["close_reason"] = str(close_reason)

        # --- payload (root-expanded in stream via log_event) ---
        pl: Dict[str, Any] = {}
        if isinstance(payload, dict):
            pl.update(deepcopy(payload))
        if isinstance(extra_payload, dict):
            for k, v in extra_payload.items():
                if k not in pl:
                    pl[k] = v

        # A3: Explicit time + fill-join fields (override payload if caller passed via args)
        if exit_ts_ms is not None and "exit_ts_ms" not in pl:
            pl["exit_ts_ms"] = int(exit_ts_ms)
        if "ts_fill_ms" not in pl:
            # Prefer explicit ts_ms, otherwise exit_ts_ms, otherwise filled below
            if ts_ms is not None:
                pl["ts_fill_ms"] = int(ts_ms)
            elif exit_ts_ms is not None:
                pl["ts_fill_ms"] = int(exit_ts_ms)

        # order_id / qty / side / venue
        if order_id is not None:
            pl["order_id"] = str(order_id)
        if "order_id" not in pl and position_id:
            # Fallback: use position_id for MT5 (still joinable)
            pl["order_id"] = str(position_id)

        if qty is not None:
            pl["qty"] = float(qty)
        if "qty" not in pl and lot is not None:
            pl["qty"] = float(lot)

        if fee_bps is not None:
            pl["fee_bps"] = float(fee_bps)

        if side is not None:
            pl["side"] = str(side).upper()
        if "side" not in pl and "direction" in pl:
            pl["side"] = str(pl.get("direction")).upper()

        if venue is not None:
            pl["venue"] = str(venue)
        if "venue" not in pl:
            pl["venue"] = str(source or "unknown")

        # Optional BBO-at-fill for TCA refinements
        if bid_at_fill is not None:
            pl["bid_at_fill"] = float(bid_at_fill)
        if ask_at_fill is not None:
            pl["ask_at_fill"] = float(ask_at_fill)
        if mid_at_fill is not None:
            pl["mid_at_fill"] = float(mid_at_fill)

        # --- event timestamp ---
        ts_final = int(ts_ms) if ts_ms is not None else None
        if ts_final is None and exit_ts_ms is not None:
            ts_final = int(exit_ts_ms)
        if ts_final is None:
            ts_final = get_ny_time_millis()

        # Fallback to 0.0 PnL if missing to satisfy contract
        if pnl is None:
            pnl = 0.0

        event = TradeEvent(
            event_type="POSITION_CLOSED",
            sid=sid,
            symbol=symbol,
            ts=int(ts_final),  # A3: deterministic timestamp
            price=close_price,
            pnl=float(pnl),
            position_id=position_id,
            lot=lot,
            source=source,
            metadata=(md or None),
            payload=(pl or None),
        )
        return self.log_event(event)
    
    def get_signal_events(self, sid: str) -> List[Dict[str, Any]]:
        """
        Получить все события по сигналу.
        
        Args:
            sid: Signal ID
            
        Returns:
            List событий в хронологическом порядке
        """
        try:
            events_key = f"trade:events:{sid}"
            events_json = self.r.lrange(events_key, 0, -1)
            
            events = []
            for event_json in events_json:
                try:
                    events.append(json.loads(event_json))
                except json.JSONDecodeError:
                    continue
            
            return events
            
        except Exception as e:
            log.error("Failed to get events for %s: %s", sid, str(e))
            return []
    
    def get_trailing_history(self, sid: str) -> List[Dict[str, Any]]:
        """
        Получить историю движения trailing stop.
        
        Возвращает все TRAILING_MOVE события для анализа
        "как далеко мы смогли утащить".
        
        Args:
            sid: Signal ID
            
        Returns:
            List TRAILING_MOVE событий
        """
        all_events = self.get_signal_events(sid)
        return [e for e in all_events if e.get("event_type") == "TRAILING_MOVE"]
    
    def calculate_signal_outcome(self, sid: str) -> Optional[Dict[str, Any]]:
        """
        Рассчитать итоговый результат сигнала.
        
        Анализирует все события и возвращает:
        - Достигнутые TP (tp1_hit, tp2_hit, tp3_hit)
        - Был ли SL
        - Максимальное движение trailing
        - Итоговый P&L
        - Время жизни сделки
        
        Args:
            sid: Signal ID
            
        Returns:
            Dict с результатами или None
        """
        events = self.get_signal_events(sid)
        if not events:
            return None
        
        outcome = {
            "sid": sid,
            "position_opened": False,
            "tp1_hit": False,
            "tp2_hit": False,
            "tp3_hit": False,
            "sl_hit": False,
            "trailing_started": False,
            "trailing_moves": 0,
            "max_sl": None,
            "min_sl": None,
            "final_pnl": None,
            "lifetime_ms": 0,
            "close_reason": None
        }
        
        first_ts = None
        last_ts = None
        
        for event in events:
            event_type = event.get("event_type")
            ts = event.get("ts", 0)
            
            if first_ts is None or ts < first_ts:
                first_ts = ts
            if last_ts is None or ts > last_ts:
                last_ts = ts
            
            # Обрабатываем по типам
            if event_type == "POSITION_OPENED":
                outcome["position_opened"] = True
                
            elif event_type == "TP1_HIT":
                outcome["tp1_hit"] = True
                
            elif event_type == "TP2_HIT":
                outcome["tp2_hit"] = True
                
            elif event_type == "TP3_HIT":
                outcome["tp3_hit"] = True
                
            elif event_type == "SL_HIT":
                outcome["sl_hit"] = True
                
            elif event_type == "TRAILING_STARTED":
                outcome["trailing_started"] = True
                
            elif event_type == "TRAILING_MOVE":
                outcome["trailing_moves"] += 1
                new_sl = event.get("new_sl")
                
                if new_sl is not None:
                    if outcome["max_sl"] is None or new_sl > outcome["max_sl"]:
                        outcome["max_sl"] = new_sl
                    if outcome["min_sl"] is None or new_sl < outcome["min_sl"]:
                        outcome["min_sl"] = new_sl
                
            elif event_type == "POSITION_CLOSED":
                pnl = event.get("pnl")
                if pnl is not None:
                    outcome["final_pnl"] = pnl
                
                metadata = event.get("metadata", {})
                if isinstance(metadata, str):
                    try:
                        metadata = json.loads(metadata)
                    except (ValueError, json.JSONDecodeError):
                        metadata = {}
                
                outcome["close_reason"] = metadata.get("close_reason")
        
        # Рассчитываем время жизни
        if first_ts and last_ts:
            outcome["lifetime_ms"] = last_ts - first_ts
        
        return outcome
    
    def get_stats(self) -> Dict[str, int]:
        """Получить статистику logger."""
        return self.stats.copy()
    
    def log_stats(self):
        """Вывести статистику в лог."""
        log.info(
            "📊 Events Logger Stats: total=%d tp1=%d tp2=%d tp3=%d sl=%d trailing_moves=%d",
            self.stats["events_written"],
            self.stats["tp1_hits"],
            self.stats["tp2_hits"],
            self.stats["tp3_hits"],
            self.stats["sl_hits"],
            self.stats["trailing_moves"]
        )


if __name__ == "__main__":
    # Тестирование
    logger = TradeEventsLogger()
    
    test_sid = f"test-signal-{int(time.time())}"
    
    print(f"\n=== Testing TradeEventsLogger with {test_sid} ===\n")
    
    # Симулируем торговый цикл
    logger.log_position_opened(
        sid=test_sid,
        symbol="",
        price=2765.5,
        lot=0.03,
        sl=2758.7,
        tp_levels=[2769.9, 2773.1, 2776.3]
    )
    
    time.sleep(0.1)
    
    logger.log_tp1_hit(
        sid=test_sid,
        symbol="",
        price=2769.9,
        lot=0.015  # 50% от позиции
    )
    
    time.sleep(0.1)
    
    logger.log_trailing_started(
        sid=test_sid,
        symbol="",
        profile="rocket_v1",
        initial_sl=2758.7,
        tp1_price=2769.9
    )
    
    time.sleep(0.1)
    
    # Несколько движений трейлинга
    for i, new_sl in enumerate([2762.0, 2764.5, 2767.2, 2769.0]):
        logger.log_trailing_move(
            sid=test_sid,
            symbol="",
            new_sl=new_sl,
            current_price=new_sl + 5.0,
            profile="rocket_v1",
            distance_from_entry=(new_sl - 2758.7)
        )
        time.sleep(0.05)
    
    logger.log_tp2_hit(
        sid=test_sid,
        symbol="",
        price=2773.1,
        lot=0.01  # 30% от позиции
    )
    
    time.sleep(0.1)
    
    logger.log_position_closed(
        sid=test_sid,
        symbol="",
        close_price=2771.5,
        pnl=150.25,
        lot=0.005,
        close_reason="trailing_stop"
    )
    
    # Показываем результаты
    print("\n=== Signal Events ===")
    events = logger.get_signal_events(test_sid)
    for i, event in enumerate(events, 1):
        print(f"{i}. {event['event_type']:20} @ {event.get('price', 'N/A'):>8} | new_sl={event.get('new_sl', 'N/A')}")
    
    print("\n=== Trailing History ===")
    trailing = logger.get_trailing_history(test_sid)
    for i, event in enumerate(trailing, 1):
        print(f"{i}. SL moved to {event['new_sl']:.2f}")
    
    print("\n=== Signal Outcome ===")
    outcome = logger.calculate_signal_outcome(test_sid)
    if outcome:
        print(json.dumps(outcome, indent=2))
    
    print("\n=== Logger Stats ===")
    logger.log_stats()
    
    print("\n✅ Test complete")

