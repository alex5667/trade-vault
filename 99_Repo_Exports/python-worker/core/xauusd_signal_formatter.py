from utils.time_utils import get_ny_time_millis
# -*- coding: utf-8 -*-
"""
 Signal Formatter - Единый формат сообщений по  для всех генерирующих сервисов.

Senior Go/Python Developer + Senior Trading Systems Analyst
40 лет совместного опыта
"""

from dataclasses import dataclass
from typing import List, Dict, Any, Optional
from datetime import datetime, timezone
import time
import html


@dataclass
class XAUUSDSignal:
    """Стандартная структура сигнала """
    sid: str                    # Signal ID
    symbol: str                 # Символ ()
    side: str                   # Направление (LONG/SHORT)
    entry: float                # Цена входа
    sl: float                   # Stop Loss
    tp_levels: List[float]      # Take Profits
    lot: float                  # Объем
    source: str                 # Источник сигнала (OrderFlow/TechnicalAnalysis/AggregatedHub)
    reason: str                 # Причина сигнала
    confidence: float           # Уверенность (0-100)
    atr: float                  # ATR
    ts: int                     # Timestamp в миллисекундах
    indicators: Optional[Dict[str, Any]] = None  # Дополнительные индикаторы
    trail_after_tp1: bool = False  # Включить трейлинг после TP1
    trail_profile: str = "rocket_v1"  # Профиль трейлинга (rocket_v1, lock_and_trail, wide_swing, etc)
    expires_at: Optional[int] = None  # Timestamp expiration if applicable


class XAUUSDSignalFormatter:
    """
    Единый форматировщик сигналов .
    
    Обеспечивает консистентность сообщений по всем сервисам:
    - python-worker/handlers/xau_orderflow_handler.py
    - signal-generator/signal_generator.py
    - hub/aggregated_signal_hub_pro.py
    - hub/aggregated_signal_hub.py
    - python-worker/aggregated_signal_hub.py
    """
    
    # Эмодзи для разных источников
    SOURCE_EMOJI = {
        "OrderFlow": "💥",
        "TechnicalAnalysis": "📊",
        "AggregatedHub": "🎯",
        "AggregatedHub-Pro": "🚀",
        "Hub": "🎯",
    }
    
    # Эмодзи для направления
    DIRECTION_EMOJI = {
        "LONG": "🟢",
        "SHORT": "🔴"
    }
    
    @classmethod
    def format_telegram_message(cls, signal: XAUUSDSignal, include_indicators: bool = True) -> str:
        """
        Форматирует сигнал для Telegram с полной информацией и временем.
        
        Args:
            signal: Структура сигнала
            include_indicators: Включать ли технические индикаторы
            
        Returns:
            Отформатированное сообщение для Telegram
        """
        # Эмодзи для сигнала
        source_emoji = cls.SOURCE_EMOJI.get(signal.source, "🚨")
        direction_emoji = cls.DIRECTION_EMOJI.get(signal.side, "⚪")
        
        # Время сигнала в UTC
        dt = datetime.fromtimestamp(signal.ts / 1000, tz=timezone.utc)
        time_str = dt.strftime('%H:%M:%S %d.%m.%Y UTC')
        
        # Рассчитываем Risk/Reward ratios
        stop_dist = abs(signal.entry - signal.sl)
        rr_parts = []
        for i, tp in enumerate(signal.tp_levels[:3], 1):
            tp_dist = abs(tp - signal.entry)
            rr = tp_dist / max(stop_dist, 0.01)
            rr_parts.append(f"TP{i} {tp:.2f} (RR {rr:.1f})")
        
        tp_str = "; ".join(rr_parts)
        
        # Формируем основное сообщение
        message_parts = [
            f"{source_emoji} {direction_emoji}  {signal.side} @ {signal.entry:.2f}, Volume {signal.lot:.2f} lot",
        ]
        
        # Добавляем причину/контекст если есть
        if signal.reason:
            # Извлекаем ключевые метрики из reason для краткости
            reason_short = html.escape(str(signal.reason)).replace("; ", " | ")
            message_parts.append(f"📝 {reason_short}")
        
        # Уровни риска
        message_parts.append(f"🛑 SL {signal.sl:.2f} | {tp_str}")
        
        # Время сигнала (ОБЯЗАТЕЛЬНО!)
        message_parts.append(f"🕐 {time_str}")
        
        # Источник и ID
        message_parts.append(f"🔧 Source: {html.escape(str(signal.source))} | ID: {html.escape(str(signal.sid))}")
        
        # Дополнительные индикаторы (если требуется)
        if include_indicators and signal.indicators:
            ind_parts = []
            
            # Z-score delta
            if "z" in signal.indicators or "z_delta" in signal.indicators:
                z_val = signal.indicators.get("z") or signal.indicators.get("z_delta")
                if z_val and abs(z_val) > 1.0:
                    ind_parts.append(f"Z={z_val:.1f}")
            
            # ATR
            if signal.atr > 0:
                ind_parts.append(f"ATR={signal.atr:.2f}")
            
            # Confidence
            if signal.confidence > 0:
                ind_parts.append(f"Conf={signal.confidence:.0f}%")
            
            if ind_parts:
                message_parts.append(f"📊 {' | '.join(ind_parts)}")
        
        return "\n".join(message_parts)
    
    @classmethod
    def format_redis_payload(cls, signal: XAUUSDSignal) -> Dict[str, Any]:
        """
        Форматирует сигнал для Redis stream (notify:telegram).
        
        Args:
            signal: Структура сигнала
            
        Returns:
            Payload для Redis xadd
        """
        payload = {
            "text": cls.format_telegram_message(signal, include_indicators=True),
            "sid": signal.sid,
            "symbol": signal.symbol,
            "source": signal.source,
            "side": signal.side,
            "entry": f"{signal.entry:.2f}",
            "price": f"{signal.entry:.2f}",
            "lot": f"{signal.lot:.2f}",
            "sl": f"{signal.sl:.2f}",
            "tp_levels": [round(tp, 2) for tp in signal.tp_levels],
            "atr": f"{signal.atr:.4f}",
            "confidence": f"{signal.confidence:.1f}",
            "reason": signal.reason,
            "ts": str(signal.ts),
            "indicators": signal.indicators or {},
            "trail_after_tp1": signal.trail_after_tp1,
            "trail_profile": signal.trail_profile
        }
        if signal.expires_at is not None:
            payload["expires_at"] = str(signal.expires_at)
        return payload
    
    @classmethod
    def format_audit_payload(cls, signal: XAUUSDSignal, extra_context: Optional[Dict] = None) -> Dict[str, Any]:
        """
        Форматирует сигнал для audit stream (полный контекст для аналитики).
        
        Args:
            signal: Структура сигнала
            extra_context: Дополнительный контекст
            
        Returns:
            Payload для audit stream
        """
        payload = {
            "sid": signal.sid,
            "symbol": signal.symbol,
            "source": signal.source,
            "ts": signal.ts,
            "side": signal.side,
            "entry": signal.entry,
            "sl": signal.sl,
            "tp_levels": signal.tp_levels,
            "lot": signal.lot,
            "confidence": signal.confidence,
            "atr": signal.atr,
            "reason": signal.reason,
            "indicators": signal.indicators or {},
            "trail_after_tp1": signal.trail_after_tp1,
            "trail_profile": signal.trail_profile
        }
        if signal.expires_at is not None:
            payload["expires_at"] = signal.expires_at

        
        if extra_context:
            payload["context"] = extra_context
        
        return payload
    
    @classmethod
    def format_order_payload(cls, signal: XAUUSDSignal) -> Dict[str, Any]:
        """
        Форматирует сигнал для /orders/push endpoint.
        
        Args:
            signal: Структура сигнала
            
        Returns:
            Payload для API запроса
        """
        return {
            "sid": signal.sid,
            "symbol": signal.symbol,
            "source": signal.source,
            "side": signal.side,
            "lot": round(signal.lot, 2),
            "entry": round(signal.entry, 2),
            "sl": round(signal.sl, 2),
            "tp_levels": [round(tp, 2) for tp in signal.tp_levels]
        }
    
    @classmethod
    def create_signal_id(cls, side: str, price: float, ts: Optional[int] = None) -> str:
        """
        Создаёт уникальный ID сигнала.
        
        Args:
            side: Направление (LONG/SHORT)
            price: Цена
            ts: Timestamp в миллисекундах (если None, используется текущее время)
            
        Returns:
            Signal ID в формате: {timestamp}:{side}:{price_normalized}
        """
        if ts is None:
            ts = get_ny_time_millis()
        
        price_normalized = int(price * 100)
        return f"{ts}:{side}:{price_normalized}"


# Примеры использования
if __name__ == "__main__":
    # Создаём тестовый сигнал
    test_signal = XAUUSDSignal(
        sid="1730000000000:SHORT:400718",
        symbol="",
        side="SHORT",
        entry=4007.18,
        sl=4007.78,
        tp_levels=[4006.58, 4005.98, 4005.38],
        lot=0.10,
        source="OrderFlow",
        reason="Extreme delta activity",
        confidence=85.0,
        atr=0.60,
        ts=1730000000000,
        indicators={"z": -6.5, "atr": 0.60}
    )
    
    # Форматируем для Telegram
    telegram_msg = XAUUSDSignalFormatter.format_telegram_message(test_signal)
    print("=== TELEGRAM MESSAGE ===")
    print(telegram_msg)
    print()
    
    # Форматируем для Redis
    redis_payload = XAUUSDSignalFormatter.format_redis_payload(test_signal)
    print("=== REDIS PAYLOAD ===")
    import json
    print(json.dumps(redis_payload, indent=2, ensure_ascii=False))

