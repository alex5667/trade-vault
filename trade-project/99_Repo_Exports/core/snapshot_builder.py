# -*- coding: utf-8 -*-
"""
SnapshotBuilder — собирает срез рынка.
"""

from dataclasses import dataclass
from typing import Dict, Any, List, Optional, Tuple
import json
import time
import requests
import logging
import redis

from infra.redis_client import try_get_json
from infra.config import Config


@dataclass
class Tick:
    ts: int
    bid: float
    ask: float
    last: float


class SnapshotBuilder:
    def __init__(self, r: redis.Redis, cfg: Config, logger: logging.Logger):
        self.r = r
        self.cfg = cfg
        self.log = logger

    def _key(self, tpl: str, symbol: str) -> str:
        return tpl.replace("{SYMBOL}", symbol)

    def _get_last_tick(self, symbol: str) -> Optional[Tick]:
        last_key = self._key(self.cfg.last_tick_key_tpl, symbol)
        obj = try_get_json(self.r, last_key)
        if obj:
            try:
                return Tick(
                    ts=int(obj.get("ts") or 0),
                    bid=float(obj.get("bid") or 0),
                    ask=float(obj.get("ask") or 0),
                    last=float(obj.get("last") or 0),
                )
            except Exception:
                pass
        stream = self._key(self.cfg.tick_stream_tpl, symbol)
        try:
            res = self.r.xrevrange(stream, max="+", min="-", count=1)
            if res:
                _, fields = res[0]
                data = json.loads(fields.get("data", "{}"))
                return Tick(
                    ts=int(data.get("ts") or 0),
                    bid=float(data.get("bid") or 0),
                    ask=float(data.get("ask") or 0),
                    last=float(data.get("last") or 0),
                )
        except Exception as e:
            self.log.warning("xrevrange fail: %s", e)
        return None

    def _get_pivots(self) -> Dict[str, float]:
        piv = try_get_json(self.r, self.cfg.pivots_key) or {}
        out: Dict[str, float] = {}
        for k, v in piv.items():
            try:
                out[k] = float(v)
            except Exception:
                pass
        return out

    def _get_dom_levels(self, symbol: str, depth: int = 10) -> List[Dict[str, float]]:
        key = self._key(self.cfg.dom_levels_key_tpl, symbol)
        arr = try_get_json(self.r, key)
        if not isinstance(arr, list):
            return []
        levels = []
        for x in arr[:depth]:
            try:
                levels.append({
                    "price": float(x.get("price")),
                    "bid": float(x.get("bid", 0)),
                    "ask": float(x.get("ask", 0)),
                })
            except Exception:
                continue
        return levels

    def _get_atr_redis(self, symbol: str) -> Optional[float]:
        """
        Получает ATR из Redis (приоритет 1).
        
        Читает ключ ta:last:atr:{symbol} в формате JSON:
        {"atr": 3.5, "period": 14, "method": "wilder", "tf": "1m", "source": "py", "ts": ...}
        """
        try:
            # Основной ключ от atr-worker (JSON формат)
            key = f"ta:last:atr:{symbol}"
            val = self.r.get(key)
            if val:
                try:
                    atr_data = json.loads(val)
                    atr = float(atr_data.get("atr", 0))
                    if atr > 0:
                        self.log.debug(f"✅ ATR from Redis: {atr:.4f} (source={atr_data.get('source', '?')})")
                        return atr
                except (json.JSONDecodeError, KeyError, ValueError) as e:
                    self.log.debug(f"Failed to parse ATR JSON from {key}: {e}")
            
            # Fallback: legacy форматы
            for legacy_key in [f"atr:val:{symbol}:1m", f"atr:{symbol}:1m"]:
                try:
                    val = self.r.get(legacy_key)
                    if val:
                        atr = float(val)
                        if atr > 0:
                            self.log.debug(f"✅ ATR from Redis legacy: {atr:.4f} (key={legacy_key})")
                            return atr
                except Exception:
                    continue
        except Exception as e:
            self.log.debug(f"Redis ATR error: {e}")
        return None

    def _get_atr_gateway(self, symbol: str) -> Optional[float]:
        try:
            url = f"{self.cfg.gateway_url}{self.cfg.runtime_atr_path}?symbol={symbol}&period={self.cfg.atr_period}"
            resp = requests.get(url, timeout=1.5)
            if resp.ok:
                data = resp.json()
                return float(data.get("atr") or 0)
        except Exception:
            return None
        return None

    def _get_atr_local(self, symbol: str) -> Optional[float]:
        key = self._key(self.cfg.ohlc_m1_list_tpl, symbol)
        try:
            N = self.cfg.atr_period + 25
            vals = self.r.lrange(key, -N, -1)
            import math
            c: List[Tuple[float, float, float, float]] = []
            for v in vals:
                o = json.loads(v)
                c.append((float(o["open"]), float(o["high"]), float(o["low"]), float(o["close"])) )
            if len(c) < self.cfg.atr_period + 1:
                return None
            trs = []
            for i in range(1, len(c)):
                _, h, l, cprev = c[i-1]
                _, h1, l1, c1 = c[i]
                tr = max(h1-l1, abs(h1-cprev), abs(l1-cprev))
                trs.append(tr)
            p = self.cfg.atr_period
            seed = sum(trs[:p]) / p
            atr = seed
            for tr in trs[p:]:
                atr = (atr*(p-1) + tr) / p
            return float(atr)
        except Exception as e:
            self.log.warning("Local ATR error: %s", e)
            return None

    def build(self, symbol: str, with_dom_depth: int = 10) -> Dict[str, Any]:
        tick = self._get_last_tick(symbol)
        pivots = self._get_pivots()
        
        # ATR приоритеты: 1) Redis → 2) Gateway API → 3) Local calculation
        atr = self._get_atr_redis(symbol)
        if atr is None:
            atr = self._get_atr_gateway(symbol)
        if atr is None:
            atr = self._get_atr_local(symbol)

        snap: Dict[str, Any] = {
            "symbol": symbol,
            "ts": int(time.time() * 1000),
            "tick": {"ts": 0, "bid": 0.0, "ask": 0.0, "last": 0.0},
            "pivots": pivots,
            "atr": float(atr or 0.0),
            "dom": self._get_dom_levels(symbol, with_dom_depth),
        }
        if tick:
            snap["tick"] = {"ts": tick.ts, "bid": tick.bid, "ask": tick.ask, "last": tick.last}
        return snap


