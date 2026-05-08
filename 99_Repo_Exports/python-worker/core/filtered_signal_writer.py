from utils.time_utils import get_ny_time_millis
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
from core.xauusd_signal_formatter import XAUUSDSignalFormatter, XAUUSDSignal
from core.redis_keys import RedisKeyPrefixes as RK


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

    def _notify_text(self, fs: FinalSignal, atr: float, source: str = "AggregatedHub") -> str:
        """УСТАРЕЛО: используйте XAUUSDSignalFormatter вместо этого метода"""
        # Создаём сигнал в стандартном формате
        ts = get_ny_time_millis()
        signal = XAUUSDSignal(
            sid=fs.sid,
            symbol=fs.symbol,
            side=fs.side,
            entry=fs.price,
            sl=fs.sl,
            tp_levels=fs.tp_levels,
            lot=fs.lot,
            source=source,
            reason=fs.reason,
            confidence=fs.confidence * 100,  # Convert 0-1 to 0-100
            atr=atr,
            ts=ts,
            indicators={"confidence": fs.confidence}
        )
        return XAUUSDSignalFormatter.format_telegram_message(signal)

    def write_and_push(
        self, 
        symbol: str, 
        side: str, 
        entry: float, 
        atr: float, 
        confidence: float, 
        reason: str, 
        source: str = "AggregatedHub",
        trail_after_tp1: bool = False,
        trail_profile: str = "rocket_v1",
        ttl: int = 86400
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
        ts = get_ny_time_millis()
        sid = XAUUSDSignalFormatter.create_signal_id(side, entry, ts)
        
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

        # Используем единый форматировщик для 
        xauusd_signal = XAUUSDSignal(
            sid=sid,
            symbol=symbol,
            side=side,
            entry=entry,
            sl=sl,
            tp_levels=tps,
            lot=lot,
            source=source,
            reason=reason,
            confidence=confidence * 100,  # Convert 0-1 to 0-100
            atr=atr,
            ts=ts,
            indicators={"confidence": confidence},
            trail_after_tp1=trail_after_tp1,
            trail_profile=trail_profile,
            expires_at=ts + ttl * 1000
        )
        
        # Публикуем в notify stream с единым форматом
        try:
            redis_payload = XAUUSDSignalFormatter.format_redis_payload(xauusd_signal)
            # Конвертируем для Redis
            redis_data = {}
            for k, v in redis_payload.items():
                if isinstance(v, (dict, list)):
                    import json
                    redis_data[k] = json.dumps(v)
                else:
                    redis_data[k] = str(v)
            
            notify_counter_key = getattr(
                self.cfg,
                "notify_signal_counter_key",
                RK.NOTIFY_SIGNAL_COUNTER
            )
            notify_every_n = getattr(self.cfg, "notify_signal_every_n", 1) or 1
            if notify_every_n < 1:
                notify_every_n = 1
            send_to_notify = True
            counter_value = None
            try:
                counter_value = self.r.incr(notify_counter_key)
            except Exception as counter_err:
                self.log.warning(
                    "⚠️ Failed to increment notify signal counter %s: %s",
                    notify_counter_key,
                    counter_err
                )
            if (
                counter_value is not None
                and notify_every_n > 1
                and counter_value % notify_every_n != 0
            ):
                send_to_notify = False
                self.log.debug(
                    "🔕 Skipping telegram notify for signal %s (counter=%s, every_n=%s)",
                    sid,
                    counter_value,
                    notify_every_n
                )
            
            if send_to_notify:
                self.r.xadd(self.cfg.notify_stream, redis_data, maxlen=100000, approximate=True)
                self.log.info("📨 Signal published to %s: %s", self.cfg.notify_stream, sid)
        except Exception as e:
            self.log.warning("Failed to publish to notify stream: %s", e)
        
        # ✅ Публикуем также в signals:aggregated:SYMBOL для Signal Performance Tracker
        try:
            import json as json_lib
            signal_stream = f"signals:aggregated:{symbol}"
            audit_payload = {
                "sid": xauusd_signal.sid,
                "symbol": xauusd_signal.symbol,
                "side": xauusd_signal.side,
                "entry": xauusd_signal.entry,
                "sl": xauusd_signal.sl,
                "tp_levels": xauusd_signal.tp_levels,
                "lot": xauusd_signal.lot,
                "source": source,  # AggregatedHub-V2
                "atr": atr,
                "confidence": confidence,
                "ts": get_ny_time_millis(),
                "trail_after_tp1": trail_after_tp1,
                "trail_profile": trail_profile,
                "expires_at": xauusd_signal.expires_at
            }
            self.r.xadd(signal_stream, {"data": json_lib.dumps(audit_payload)}, maxlen=100000, approximate=True)
            self.log.debug("📨 Signal published to %s for tracking", signal_stream)
        except Exception as e:
            self.log.warning("Failed to publish to signal stream for tracking: %s", e)

        # ✅ Сохраняем сигнал в Redis для TP1 Trailing Orchestrator
        try:
            import json as json_lib
            signal_key = f"signals:{sid}"
            signal_data = {
                "sid": sid,
                "symbol": symbol,
                "side": side,
                "entry": entry,
                "sl": sl,
                "tp_levels": tps,
                "lot": lot,
                "source": source,
                "atr": atr,
                "confidence": confidence,
                "reason": reason,
                "ts": ts,
                "trail_after_tp1": trail_after_tp1,
                "trail_profile": trail_profile,
                "expires_at": xauusd_signal.expires_at
            }
            self.r.set(signal_key, json_lib.dumps(signal_data), ex=ttl)  # Dynamic TTL
            self.log.debug("💾 Signal saved to Redis: %s with ttl %s", signal_key, ttl)
        except Exception as e:
            self.log.warning("Failed to save signal to Redis: %s", e)
        
        # Отправка ордеров во внешнюю систему временно отключена
        push_body = XAUUSDSignalFormatter.format_order_payload(xauusd_signal)
        ok = False
        self.log.info("🚫 Order push skipped (disabled): %s | payload=%s", fs.sid, push_body)
        
        self.last_ts = time.time()
        self.log.info("✅ Meta-signal %s sent=%s, source=%s", fs.sid, ok, source)
        return fs


