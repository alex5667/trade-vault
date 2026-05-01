from utils.time_utils import get_ny_time_millis
# -*- coding: utf-8 -*-
"""
ATR cache / reader for multiple legacy keys.

We keep this file "read-optimized" because production uses many key shapes:
  - ATR:{symbol}:{TF} hash (tracker-style)
  - atr:{symbol}:{tf} string
  - atr:val:{symbol}:{tf} string (legacy mirror)
  - atr:json:{symbol}:{tf} json (includes ts)
  - ta:last:atr:{symbol} json (includes tf + ts)

This module provides:
  - get_with_meta(): returns (atr, meta) best-effort
  - get_candidates(): returns list of candidates for sanity calibrator (source selector)
"""
import json
import os
import time
from typing import Any, Dict, List, Optional, Tuple
import redis
from core.redis_client import get_redis


def _f(x: Any, d: float = 0.0) -> float:
    try:
        if x is None:
            return d
        return float(x)
    except Exception:
        return d


def _i(x: Any, d: int = 0) -> int:
    try:
        if x is None:
            return d
        return int(float(x))
    except Exception:
        return d


class ATRCache:
    """
    Класс для работы с кэшем ATR в Redis.
    Поддерживает чтение из множества легаси-ключей.
    """
    
    def __init__(self, ttl: int = 3600):
        url = os.getenv("ATR_REDIS_URL")
        if url:
            self.redis_client = redis.from_url(url, decode_responses=True)
        else:
            self.redis_client = get_redis()
        self.ttl = ttl
    
    def get(self, symbol: str, timeframe: str) -> Optional[float]:
        atr, _meta = self.get_with_meta(symbol=symbol, timeframe=timeframe)
        return atr

    def _pttl_ms(self, key: str) -> int:
        try:
            v = self.redis_client.pttl(key)
            return int(v if isinstance(v, int) else -1)
        except Exception:
            return -1

    def get_candidates(self, *, symbol: str, timeframe: str, now_ms: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Return ALL candidates with meta for sanity selection.
        Candidates are dictionaries:
          {atr, src, key, tf, ts_ms, age_ms, has_ts}
        """
        out: List[Dict[str, Any]] = []
        sym = str(symbol or "").upper()
        tf_raw = str(timeframe or "1m")
        tf_norm = self._normalize_tracker_tf(tf_raw)
        nm = int(now_ms) if (now_ms is not None) else get_ny_time_millis()

        # 1) Tracker hash: ATR:{SYM}:{TFN} fields: atr, lastCloseTime (if any)
        tracker_key = f"ATR:{sym}:{tf_norm}"
        try:
            v_atr, v_ts = self.redis_client.hmget(tracker_key, "atr", "lastCloseTime")
            if v_atr:
                atr = float(v_atr)
                ts_ms = 0
                try:
                    ts_ms = int(float(v_ts)) if v_ts else 0
                except Exception:
                    ts_ms = 0
                age = int(max(0, nm - ts_ms)) if ts_ms > 0 else 0
                out.append({"atr": atr, "src": "tracker_hash", "key": tracker_key, "tf": tf_norm, "ts_ms": ts_ms, "age_ms": age, "has_ts": 1 if ts_ms > 0 else 0})
        except Exception:
            pass

        # 2) atr:{sym}:{tf} string
        key2 = f"atr:{sym}:{tf_raw}"
        try:
            raw = self.redis_client.get(key2)
            if raw:
                atr = float(raw)
                # strings have no ts; best-effort: use TTL to hint freshness (not deterministic)
                pttl = self._pttl_ms(key2)
                out.append({"atr": atr, "src": "atr_string", "key": key2, "tf": tf_norm, "ts_ms": 0, "age_ms": 0, "has_ts": 0, "pttl_ms": pttl})
        except Exception:
            pass

        # 3) atr:val:{sym}:{tf} string mirror
        key2b = f"atr:val:{sym}:{tf_raw}"
        try:
            raw = self.redis_client.get(key2b)
            if raw:
                atr = float(raw)
                pttl = self._pttl_ms(key2b)
                out.append({"atr": atr, "src": "atr_val", "key": key2b, "tf": tf_norm, "ts_ms": 0, "age_ms": 0, "has_ts": 0, "pttl_ms": pttl})
        except Exception:
            pass

        # 4) atr:json:{sym}:{tf} json (atr + ts)
        key3 = f"atr:json:{sym}:{tf_raw}"
        try:
            raw = self.redis_client.get(key3)
            if raw:
                d = json.loads(raw)
                atr = float(d.get("atr", 0.0) or 0.0)
                ts_ms = int(d.get("ts", 0) or 0)
                if atr > 0:
                    age = int(max(0, nm - ts_ms)) if ts_ms > 0 else 0
                    out.append({"atr": atr, "src": "atr_json", "key": key3, "tf": tf_norm, "ts_ms": ts_ms, "age_ms": age, "has_ts": 1 if ts_ms > 0 else 0})
        except Exception:
            pass

        # 5) ta:last:atr:{sym} json (atr + tf + ts)
        last_key = f"ta:last:atr:{sym}"
        try:
            raw = self.redis_client.get(last_key)
            if raw:
                d = json.loads(raw)
                atr = float(d.get("atr", 0.0) or 0.0)
                ts_ms = int(d.get("ts", 0) or 0)
                tf0 = str(d.get("tf", "") or "").upper()
                if atr > 0:
                    age = int(max(0, nm - ts_ms)) if ts_ms > 0 else 0
                    out.append({"atr": atr, "src": "ta_last", "key": last_key, "tf": tf0 if tf0 else tf_norm, "ts_ms": ts_ms, "age_ms": age, "has_ts": 1 if ts_ms > 0 else 0})
        except Exception:
            pass

        return out

    def get_with_meta(self, symbol: str, timeframe: Optional[str] = None, now_ms: Optional[int] = None, prefer_src: str = "") -> Tuple[Optional[float], dict]:
        """
        Returns (atr_value, meta) where meta contains:
          {src, tf, ts_ms, age_ms}
        Uses cfg:atr_tf:{sym} when tf is None.
        """
        sym = str(symbol)
        if timeframe is None:
            tf = str(self.redis_client.get(f"cfg:atr_tf:{sym}") or "").strip() or None
        else:
            tf = str(timeframe)

        # Prefer selected meta if exists
        meta_raw = self.redis_client.get(f"cfg:atr_sel_meta:{sym}")
        if meta_raw:
            try:
                meta = json.loads(meta_raw) if isinstance(meta_raw, str) else json.loads(meta_raw.decode("utf-8","ignore"))
                atr = _f(meta.get("atr", None), 0.0)
                ts_ms = _i(meta.get("ts_ms", None), 0)
                nm = int(now_ms) if (now_ms is not None) else get_ny_time_millis()
                age_ms = max(0, nm - ts_ms) if ts_ms > 0 else 0
                meta["age_ms"] = age_ms
                if atr > 0:
                    return atr, meta
            except Exception:
                pass

        # Fallback: try direct keys by tf if provided
        if tf:
            # Example: atr:val:{sym}:{tf}
            raw_val = self.redis_client.get(f"atr:val:{sym}:{tf}") or self.redis_client.get(f"atr:{sym}:{tf}")
            if raw_val:
                atr = _f(raw_val, 0.0)
                if atr > 0:
                    ts_ms = _i(self.redis_client.get(f"atr:val:{sym}:{tf}:ts_ms") or self.redis_client.get(f"atr:{sym}:{tf}:ts_ms"), 0)
                    nm = int(now_ms) if (now_ms is not None) else get_ny_time_millis()
                    return atr, {"src": "atr_string", "tf": tf, "ts_ms": ts_ms, "age_ms": max(0, nm - ts_ms) if ts_ms > 0 else 0}
            # Also try other candidate sources
            candidates = self.get_candidates(symbol=sym, timeframe=tf, now_ms=now_ms)
            for cand in candidates:
                cand_atr = float(cand.get("atr", 0.0) or 0.0)
                if cand_atr > 0:
                    return cand_atr, {
                        "src": str(cand.get("src", "atr_string")),
                        "tf": str(cand.get("tf", tf)),
                        "ts_ms": int(cand.get("ts_ms", 0) or 0),
                        "age_ms": int(cand.get("age_ms", 0) or 0),
                    }

        return None, {"src": "none", "tf": "na", "ts_ms": 0, "age_ms": 0}

    def set(self, symbol: str, timeframe: str, atr_value: float) -> bool:
        """
        Сохраняет ATR в кэш. (legacy support)
        """
        try:
            if atr_value <= 0:
                return False
            
            primary_key = f"atr:{symbol}:{timeframe}"
            self.redis_client.set(primary_key, str(atr_value), ex=self.ttl)
            # Legacy compatibility
            self.redis_client.set(f"atr:val:{symbol}:{timeframe}", str(atr_value), ex=self.ttl)
            
            return True
        except Exception:
            return False

    def delete(self, symbol: str, timeframe: str) -> bool:
        try:
            key = f"atr:{symbol}:{timeframe}"
            self.redis_client.delete(key)
            return True
        except Exception:
            return False
    
    def clear_all(self) -> int:
        try:
            pattern = "atr:*"
            keys = list(self.redis_client.scan_iter(match=pattern, count=10000))
            if not keys: return 0
            deleted = self.redis_client.delete(*keys)
            return deleted
        except Exception:
            return 0

    @staticmethod
    def _normalize_tracker_tf(tf: str) -> str:
        if not tf:
            return "M1"
        tf_map = {
            "1m": "M1", "m1": "M1",
            "5m": "M5", "m5": "M5",
            "15m": "M15", "m15": "M15",
            "30m": "M30", "m30": "M30",
            "1h": "H1", "h1": "H1",
            "4h": "H4", "h4": "H4",
            "1d": "D1", "d1": "D1",
        }
        key = tf.strip().lower()
        return tf_map.get(key, tf.strip().upper())

# Глобальный экземпляр для использования
_atr_cache_instance = None
def get_atr_cache() -> ATRCache:
    global _atr_cache_instance
    if _atr_cache_instance is None:
        _atr_cache_instance = ATRCache()
    return _atr_cache_instance
