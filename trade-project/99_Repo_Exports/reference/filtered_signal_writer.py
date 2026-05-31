# filtered_signal_writer.py
"""
Filtered Signal Writer - буферизация/дедуп/кулдаун + интеграция риск-сайзера + отправка в /orders/push.
"""
from __future__ import annotations
from dataclasses import dataclass
from typing import Dict, List, Optional
import os
import time

try:
    import redis
except ImportError:
    redis = None

from common.log import setup_logger
from symbol_specs_store import SymbolSpecsStore, SymbolSpecs
from risk_position_sizer import size_and_bracket, SymbolSpecs as RiskSpecs
from account_client import get_balance
from order_push_dispatcher import post_order, publish_telegram

log = setup_logger("signal_writer")

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

@dataclass
class WriterConfig:
    """Конфигурация writer."""
    symbol: str = "XAUUSD"
    min_confidence: float = 60.0
    cooldown_sec: int = 300
    atr_fallback: float = 3.0
    risk_pct: float = 1.0
    sl_mult: float = 1.5
    tp_mults: List[float] = None

class FilteredSignalWriter:
    """
    Буферизация/дедуп/кулдаун + интеграция риск-сайзера + отправка в /orders/push.
    """
    
    def __init__(self, r=None, cfg: Optional[WriterConfig] = None):
        if r is not None:
            self.r = r
        else:
            if not redis:
                raise RuntimeError("redis-py не установлен")
            self.r = redis.Redis.from_url(REDIS_URL, decode_responses=True)
        
        self.cfg = cfg or WriterConfig()
        if self.cfg.tp_mults is None:
            self.cfg.tp_mults = [2.0, 3.0, 4.0]
        
        self.last_ts: Dict[str, int] = {}
        self.specs_store = SymbolSpecsStore(self.r)
        self.specs_store.ensure_default(self.cfg.symbol)

    def _cooldown_ok(self, sid: str) -> bool:
        """Проверить можно ли отправить сигнал (прошел ли cooldown)."""
        now = int(time.time())
        ts = self.last_ts.get(sid, 0)
        if now - ts < self.cfg.cooldown_sec:
            return False
        self.last_ts[sid] = now
        return True

    def _risk_specs(self, symbol: str) -> RiskSpecs:
        """Получить спецификации инструмента для risk sizing."""
        s: SymbolSpecs = self.specs_store.get(symbol)
        return RiskSpecs(
            symbol=s.symbol,
            point=s.point,
            tick_value_per_lot=s.tick_value_per_lot,
            lot_step=s.lot_step,
            min_lot=s.min_lot,
            max_lot=s.max_lot,
        )

    def write(self, signal: Dict) -> Optional[Dict]:
        """
        Обработать и отправить сигнал.
        
        Args:
            signal: {
                "sid": "...",
                "symbol": "XAUUSD",
                "side": "LONG"/"SHORT",
                "confidence": 0..100,
                "entry": float|None (market),
                "atr": float|None,
                "context": {...}  # reason, cluster, of/ta parts
            }
        
        Returns:
            Dict с результатом или None если сигнал отфильтрован
        """
        if signal.get("symbol") != self.cfg.symbol:
            return None
        
        conf = float(signal.get("confidence", 0.0))
        if conf < self.cfg.min_confidence:
            log.debug("Signal filtered: confidence %.1f < %.1f", conf, self.cfg.min_confidence)
            return None
        
        sid = signal.get("sid") or f"{self.cfg.symbol}:{int(time.time())}"
        if not self._cooldown_ok(sid):
            log.debug("Signal filtered: cooldown not passed for sid=%s", sid)
            return None

        # параметры риска
        atr = float(signal.get("atr") or self.cfg.atr_fallback)
        side = signal["side"].upper()
        entry = float(signal.get("entry") or 0.0)  # для MARKET просто используем текущий mid в gateway/EA

        balance = get_balance()
        rspec = self._risk_specs(self.cfg.symbol)
        lot, sl, tp_levels = size_and_bracket(
            side=side,
            entry=entry,
            atr=atr,
            balance=balance,
            spec=rspec,
            risk_pct=self.cfg.risk_pct,
            sl_mult=self.cfg.sl_mult,
            tp_mults=self.cfg.tp_mults
        )

        payload = {
            "action": "open",
            "symbol": self.cfg.symbol,
            "side": side,
            "lot": lot,
            "sl": sl,
            "tp_levels": tp_levels,
            "sid": sid
        }
        
        try:
            resp = post_order(payload)
            text = f"📤 {self.cfg.symbol} {side} lot={lot} SL={sl} TP={tp_levels} conf={conf:.1f}"
            publish_telegram(text, self.r)
            log.info("Order pushed: %s", payload)
            return {"ok": True, "resp": resp, "payload": payload}
        except Exception as e:
            error_msg = f"❌ push failed: {e}"
            publish_telegram(error_msg, self.r)
            log.error("Order push failed: %s", e)
            return {"ok": False, "error": str(e), "payload": payload}
