# -*- coding: utf-8 -*-
"""
FilteredSignalWriter — финальный буфер и публикация.
"""

from dataclasses import dataclass
from typing import Dict, Any, List, Optional, Tuple
import time
import logging
import requests
import redis

from infra.config import Config
from risk.position_sizer import PositionSizer, SymbolSpecs
from dispatch.order_push_dispatcher import OrderPushDispatcher
from specs.symbol_specs_repo import SymbolSpecsRepo, SymbolSpecsModel


@dataclass
class FinalSignal:
    sid: str
    symbol: str
    side: str
    price: float
    sl: float
    tp_levels: List[float]
    lot: float
    confidence: float
    reason: str


class FilteredSignalWriter:
    def __init__(
        self,
        r: redis.Redis,
        cfg: Config,
        logger: logging.Logger,
        dispatcher: OrderPushDispatcher,
    ):
        self.r = r
        self.cfg = cfg
        self.log = logger
        self.dispatcher = dispatcher
        self.last_ts: float = 0.0
        self.cooldown_skip_count: int = 0  # Счётчик пропущенных сигналов из-за cooldown

    def _can_emit(self) -> bool:
        return (time.time() - self.last_ts) >= self.cfg.cooldown_sec

    def _get_balance(self) -> Optional[float]:
        try:
            url = f"{self.cfg.gateway_url}{self.cfg.balance_path}"
            resp = requests.get(url, timeout=1.0)
            if resp.ok:
                j = resp.json()
                return float(j.get("balance"))
        except Exception:
            return None
        return None

    def _get_specs(self) -> SymbolSpecs:
        # Используем Redis-репозиторий с фолбэком на ENV/config
        repo = SymbolSpecsRepo(self.r, key_tpl="symbol_specs:{SYMBOL}")
        fb = SymbolSpecsModel(
            symbol=self.cfg.symbol,
            point=self.cfg.point,
            tick_value_per_lot=self.cfg.tick_value_per_lot,
            min_lot=self.cfg.min_lot,
            max_lot=self.cfg.max_lot,
            lot_step=self.cfg.lot_step,
            contract_size=0.0,
            price_decimals=1,
            volume_decimals=2,
        )
        s = repo.get(self.cfg.symbol, fb)
        # Адаптация к классу сайзера
        return SymbolSpecs(
            point=s.point,
            tick_value_per_lot=s.tick_value_per_lot,
            min_lot=s.min_lot,
            max_lot=s.max_lot,
            lot_step=s.lot_step,
        )

    def _select_lot_and_sl_tp(
        self, side: str, entry: float, atr: float
    ) -> Tuple[float, float, List[float]]:
        specs = self._get_specs()
        bal = self._get_balance() or 10_000.0
        ps = PositionSizer(specs)
        lot, stop_dist = ps.size_by_atr(bal, self.cfg.risk_pct, atr, self.cfg.atr_sl_mult)
        if side == "LONG":
            sl = entry - stop_dist
            tps = [entry + atr*m for m in self.cfg.atr_tp_mults]
        else:
            sl = entry + stop_dist
            tps = [entry - atr*m for m in self.cfg.atr_tp_mults]
        return (lot, sl, [round(x, 2) for x in tps])

    def _notify_text(self, fs: FinalSignal, atr: float) -> str:
        from datetime import datetime
        utc_time = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')
        
        rr = []
        stop_dist = abs(fs.price - fs.sl)
        for t in fs.tp_levels:
            rr.append(round(abs(t - fs.price) / max(stop_dist, 1e-9), 2))
        rr_str = "; ".join(f"TP{i+1} {fs.tp_levels[i]:.2f} (RR {rr[i]})" for i in range(len(fs.tp_levels)))
        emoji = "🚀" if fs.side == "LONG" else "🧊"
        return (
            f"{emoji} {fs.symbol} {fs.side} @ {fs.price:.2f}, Vol {fs.lot:.2f} lot. "
            f"Meta-signal (conf {fs.confidence:.2f})\n"
            f"📍 Entry: {fs.price:.2f} | 🔧 Source: AggregatedHub\n"
            f"SL {fs.sl:.2f} | {rr_str} | ATR {atr:.2f}\n"
            f"🕐 {utc_time}\n"
            f"Причина: {fs.reason}"
        )

    def write_and_push(
        self, symbol: str, side: str, entry: float, atr: float, confidence: float, reason: str, source: str = "AggregatedHub"
    ) -> Optional[FinalSignal]:
        if atr <= 0 or entry <= 0:
            self.log.debug("skip emit: bad atr/entry")
            return None
        if not self._can_emit():
            self.cooldown_skip_count += 1
            if self.cooldown_skip_count % 10000 == 0:
                self.log.debug("skip emit: cooldown [skipped %d times]", self.cooldown_skip_count)
            return None

        lot, sl, tps = self._select_lot_and_sl_tp(side, entry, atr)
        sid = f"{int(time.time()*1000)}:{side}:{int(entry*100)}"
        fs = FinalSignal(
            sid=sid,
            symbol=symbol,
            side=side,
            price=entry,
            sl=sl,
            tp_levels=tps,
            lot=lot,
            confidence=confidence,
            reason=reason,
        )

        text = self._notify_text(fs, atr)
        try:
            self.r.xadd(self.cfg.notify_stream, {
                "text": text, 
                "sid": fs.sid,
                "source": source,  # Источник сигнала
                "entry": str(round(fs.price, 2))  # Точка входа
            }, maxlen=1000)
        except Exception:
            pass

        push_body = {
            "sid": fs.sid,
            "symbol": fs.symbol,
            "source": source,  # Источник сигнала
            "side": fs.side,
            "lot": round(fs.lot, 2),
            "entry": round(fs.price, 2),  # Точка входа
            "sl": round(fs.sl, 2),
            "tp_levels": fs.tp_levels,
        }
        ok = False
        self.log.info("Skipping order push (disabled): %s | payload=%s", fs.sid, push_body)
        self.last_ts = time.time()
        self.log.info("Meta-signal %s sent=%s", fs.sid, ok)
        return fs


