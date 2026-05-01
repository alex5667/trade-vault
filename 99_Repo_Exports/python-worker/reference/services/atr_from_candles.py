from utils.time_utils import get_ny_time_millis
#!/usr/bin/env python3
"""
ATR Calculator from Unified Candles Stream.

Consumes `candles:data` (unified kline stream) and computes ATR(14) per (symbol, tf)
using Wilder's smoothing algorithm. Stores results to Redis keys:
  - atr:val:{symbol}:{tf} -> float value
  - atr:json:{symbol}:{tf} -> JSON with metadata

Uses consumer group for at-most-once delivery.
"""

import os
import sys
import json
import time
from pathlib import Path
import redis
from collections import defaultdict
from typing import Callable, Dict, Optional, Set, Tuple
import numpy as np


def _resolve_regime_worker_path() -> str:
    """Determine path to regime-worker module for ADX calculations."""
    env_path = os.getenv("REGIME_WORKER_PATH")
    if env_path:
        return env_path
    base_dir = Path(__file__).resolve().parent.parent  # /app
    candidate = base_dir / "regime-worker"
    return str(candidate)


REGIME_WORKER_PATH = _resolve_regime_worker_path()
if REGIME_WORKER_PATH not in sys.path:
    sys.path.append(REGIME_WORKER_PATH)

from adx_atr import WilderState, update_adx_atr  # type: ignore  # noqa: E402


# Configuration
REDIS_URL = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
CANDLES_STREAM = os.getenv("CANDLES_STREAM", "candles:data")
GROUP = os.getenv("ATR_GROUP", "atr-worker-group")
CONSUMER = os.getenv("ATR_CONSUMER", "atr-worker-1")
PERIOD = int(os.getenv("ATR_PERIOD", "14"))


def _normalize_symbol(value: str) -> str:
    return value.strip().upper()


def _normalize_tf(value: str) -> str:
    return value.strip().lower()


DEFAULT_ATR_SYMBOLS = {_normalize_symbol(s) for s in os.getenv("ATR_SYMBOLS", "XAUUSD").split(",") if s.strip()}
DEFAULT_ATR_TFS = {_normalize_tf(s) for s in os.getenv("ATR_TFS", "1m,5m,15m,1d").split(",") if s.strip()}

# Dynamic config (optional)
CONFIG_REFRESH_INTERVAL_SEC = float(os.getenv("CONFIG_REFRESH_INTERVAL_SEC", "5"))
ATR_SYMBOLS_KEY = os.getenv("ATR_SYMBOLS_KEY", "atr:config:symbols")
ATR_TFS_KEY = os.getenv("ATR_TFS_KEY", "atr:config:tfs")
ADX_SYMBOLS_KEY = os.getenv("ADX_SYMBOLS_KEY", "adx:config:symbols")
ADX_TFS_KEY = os.getenv("ADX_TFS_KEY", "adx:config:tfs")

# ADX configuration (optional)
DEFAULT_ADX_SYMBOLS = {_normalize_symbol(s) for s in os.getenv("ADX_SYMBOLS", "BTCUSDT,ETHUSDT").split(",") if s.strip()}
DEFAULT_ADX_TFS = {_normalize_tf(s) for s in os.getenv("ADX_TFS", "1m").split(",") if s.strip()}
ADX_PERIOD = int(os.getenv("ADX_PERIOD", str(PERIOD)))
ADX_KEY_TEMPLATE = os.getenv("ADX_KEY_TEMPLATE", "adx:{symbol}")


def true_range(h: float, l: float, prev_close: Optional[float]) -> float:
    """
    Calculate True Range.
    
    TR = max(H-L, |H-PC|, |L-PC|) where PC is previous close
    
    Args:
        h: High price
        l: Low price
        prev_close: Previous close price (None for first bar)
        
    Returns:
        True Range value
    """
    if prev_close is None:
        return h - l
    return max(h - l, abs(h - prev_close), abs(l - prev_close))


class ATRState:
    """
    Wilder's ATR calculation state for a single (symbol, timeframe) pair.
    
    Uses Wilder's smoothing:
    - First ATR = simple average of first PERIOD TRs
    - Subsequent: ATR = (ATR_prev * (PERIOD-1) + TR) / PERIOD
    
    ✅ GPU Support: использует GPU для вычисления True Range если доступен
    """
    
    # ✅ GPU Support: lazy initialization
    _gpu_service_cache = None
    
    @classmethod
    def _get_gpu_service(cls):
        """Получить GPU сервис (lazy initialization)"""
        if cls._gpu_service_cache is None:
            try:
                from services.gpu_compute_service import get_gpu_service
                cls._gpu_service_cache = get_gpu_service()
            except Exception:
                cls._gpu_service_cache = None
        return cls._gpu_service_cache
    
    def __init__(self, period: int = 14):
        """Initialize ATR state."""
        self.period = period
        self.prev_close: Optional[float] = None
        self.value: Optional[float] = None
        self.count = 0
        self.tr_sum = 0.0  # For initial simple average
        # ✅ GPU Support: буфер для батч-обработки
        self.tr_buffer: List[float] = []
    
    def feed(self, h: float, l: float, c: float) -> Optional[float]:
        """
        Feed a new candle and update ATR with optional GPU acceleration.
        
        Args:
            h: High price
            l: Low price
            c: Close price
            
        Returns:
            ATR value (None until initialized)
        """
        # ✅ GPU Support: используем GPU для вычисления True Range если доступен
        gpu_service = self._get_gpu_service()
        if gpu_service and gpu_service.is_gpu_available() and self.prev_close is not None:
            try:
                # Используем GPU для вычисления True Range
                highs = np.array([self.prev_close if self.prev_close else h, h], dtype=np.float32)
                lows = np.array([self.prev_close if self.prev_close else l, l], dtype=np.float32)
                closes = np.array([self.prev_close if self.prev_close else c, c], dtype=np.float32)
                tr_values = gpu_service.compute_atr_batch(highs, lows, closes, period=1)
                tr = float(tr_values[-1]) if len(tr_values) > 0 else true_range(h, l, self.prev_close)
            except Exception:
                tr = true_range(h, l, self.prev_close)
        else:
            tr = true_range(h, l, self.prev_close)
        
        self.prev_close = c
        self.count += 1
        
        if self.value is None:
            # Initialization phase: collect PERIOD TRs
            self.tr_sum += tr
            if self.count == self.period:
                # Initialize with simple average
                self.value = self.tr_sum / self.period
        else:
            # Wilder's smoothing
            self.value = (self.value * (self.period - 1) + tr) / self.period
        
        return self.value


class DynamicSet:
    """Utility to keep Redis-backed dynamic filters in sync."""

    def __init__(
        self,
        name: str,
        key: str,
        default: Set[str],
        normalizer: Callable[[str], str],
    ) -> None:
        self.name = name
        self.key = key or ""
        self.normalizer = normalizer
        self.default = {normalizer(v) for v in default}
        self.active: Set[str] = set(self.default)
        self.should_filter = bool(self.active)

    def refresh(self, redis_client: redis.Redis) -> bool:
        """Refresh state from Redis. Returns True if configuration changed."""
        source = "defaults"
        new_active = set(self.default)
        new_should_filter = bool(new_active)

        if self.key:
            try:
                exists = bool(redis_client.exists(self.key))
                if exists:
                    members = redis_client.smembers(self.key)
                    normalized = {self.normalizer(m) for m in members if m}
                    new_active = normalized
                    new_should_filter = True  # explicit set => enforce filter
                    source = "redis"
                else:
                    # fall back to defaults
                    source = "defaults"
            except redis.RedisError as exc:
                print(f"⚠️  Unable to load {self.name} from Redis ({self.key}): {exc}")
                return False

        changed = (new_active != self.active) or (new_should_filter != self.should_filter)
        if changed:
            self.active = new_active
            self.should_filter = new_should_filter
            printable = ", ".join(sorted(new_active)) if new_active else "∅"
            print(f"🔁 {self.name} -> {printable} ({source})")
        return changed

    def allows(self, value: Optional[str]) -> bool:
        """Check whether a value passes the filter."""
        if not self.should_filter:
            return True
        if value is None:
            return False
        normalized = self.normalizer(value)
        return normalized in self.active


def main():
    """Main entry point."""
    print(f"🔧 ATR Calculator starting...")
    print(f"   Stream: {CANDLES_STREAM}")
    print(f"   Group: {GROUP}")
    print(f"   Consumer: {CONSUMER}")
    print(f"   Period: {PERIOD}")
    print(
        f"   Default ATR symbols: {sorted(DEFAULT_ATR_SYMBOLS) if DEFAULT_ATR_SYMBOLS else 'ALL'}"
    )
    print(
        f"   Default ATR timeframes: {sorted(DEFAULT_ATR_TFS) if DEFAULT_ATR_TFS else 'ALL'}"
    )
    if DEFAULT_ADX_SYMBOLS:
        print(f"   Default ADX symbols: {sorted(DEFAULT_ADX_SYMBOLS)}")
        print(
            f"   Default ADX timeframes: {sorted(DEFAULT_ADX_TFS) if DEFAULT_ADX_TFS else 'ALL'}"
        )
    else:
        print("   ADX: disabled (no symbols configured)")
    print(f"   Config refresh interval: {CONFIG_REFRESH_INTERVAL_SEC:.2f}s")
    print()
    
    # Connect to Redis with retry logic
    max_retries = 20  # Увеличено для ожидания загрузки большого dataset из RDB
    retry_delay = 3.0
    r = None
    
    for attempt in range(max_retries):
        try:
            r = redis.from_url(
                REDIS_URL,
                decode_responses=True,
                health_check_interval=30,
                socket_timeout=30,
            )
            # Проверяем подключение
            r.ping()
            print(f"✅ Connected to Redis: {REDIS_URL}")
            break
        except (redis.exceptions.BusyLoadingError, redis.exceptions.ConnectionError) as e:
            if attempt < max_retries - 1:
                print(f"⚠️ Redis not ready (attempt {attempt + 1}/{max_retries}): {e}")
                print(f"   Retrying in {retry_delay}s...")
                time.sleep(retry_delay)
                retry_delay *= 1.5  # Exponential backoff
            else:
                print(f"❌ Failed to connect to Redis after {max_retries} attempts: {e}")
                raise
        except Exception as e:
            print(f"❌ Unexpected error connecting to Redis: {e}")
            raise
    
    if r is None:
        raise RuntimeError("Failed to establish Redis connection")

    def ensure_consumer_group(redis_client, stream_key, group_name):
        """Ensure consumer group exists, creating it if necessary."""
        local_retries = 5
        local_delay = 1.0
        
        for attempt in range(local_retries):
            try:
                # Try to create the group
                redis_client.xgroup_create(stream_key, group_name, id='0', mkstream=True)
                print(f"✅ Created consumer group: {group_name}")
                return True
            except redis.exceptions.BusyLoadingError:
                print(f"⚠️ Redis is loading dataset (ensure_consumer_group)")
                time.sleep(local_delay)
                local_delay *= 1.5
                continue
            except redis.exceptions.ResponseError as e:
                error_str = str(e).upper()
                if "BUSYGROUP" in error_str:
                    # Group already exists - check if it's really there
                    try:
                        groups = redis_client.xinfo_groups(stream_key)
                        if any(g.get("name") == group_name for g in groups):
                            print(f"ℹ️ Consumer group {group_name} confirmed active")
                            return True
                    except Exception as check_err:
                        print(f"⚠️ Failed to verify consumer group existence: {check_err}")
                    
                    # If we got BUSYGROUP, it essentially means it exists or we can't create it due to name collision
                    # Assuming it exists is safe enough usually
                    return True
                else:
                    print(f"⚠️ Error creating consumer group {group_name}: {e}")
                    time.sleep(local_delay)
                    local_delay *= 1.5
            except Exception as e:
                print(f"❌ Unexpected error in ensure_consumer_group: {e}")
                time.sleep(local_delay)
        
        return False
    
    # Initialize consumer group
    if not ensure_consumer_group(r, CANDLES_STREAM, GROUP):
         raise RuntimeError(f"Failed to ensure consumer group {GROUP}")
    
    # ATR states per (symbol, timeframe)
    atr_states = defaultdict(lambda: ATRState(period=PERIOD))
    # ADX states per (symbol, timeframe)
    adx_states: Dict[Tuple[str, str], WilderState] = defaultdict(WilderState)
    prev_candles: Dict[Tuple[str, str], Dict[str, float]] = {}

    atr_symbols_set = DynamicSet("ATR symbols", ATR_SYMBOLS_KEY, DEFAULT_ATR_SYMBOLS, _normalize_symbol)
    atr_tfs_set = DynamicSet("ATR timeframes", ATR_TFS_KEY, DEFAULT_ATR_TFS, _normalize_tf)
    adx_symbols_set = DynamicSet("ADX symbols", ADX_SYMBOLS_KEY, DEFAULT_ADX_SYMBOLS, _normalize_symbol)
    adx_tfs_set = DynamicSet("ADX timeframes", ADX_TFS_KEY, DEFAULT_ADX_TFS, _normalize_tf)

    def prune_states() -> None:
        for key in list(atr_states.keys()):
            sym, tf = key
            if atr_symbols_set.should_filter and sym not in atr_symbols_set.active:
                atr_states.pop(key, None)
                continue
            if atr_tfs_set.should_filter and tf not in atr_tfs_set.active:
                atr_states.pop(key, None)
                continue

        for key in list(adx_states.keys()):
            sym, tf = key
            if adx_symbols_set.should_filter and sym not in adx_symbols_set.active:
                adx_states.pop(key, None)
                prev_candles.pop(key, None)
                continue
            if adx_tfs_set.should_filter and tf not in adx_tfs_set.active:
                adx_states.pop(key, None)
                prev_candles.pop(key, None)
                continue

        for key in list(prev_candles.keys()):
            sym, tf = key
            if atr_symbols_set.should_filter and sym not in atr_symbols_set.active:
                prev_candles.pop(key, None)
                continue
            if atr_tfs_set.should_filter and tf not in atr_tfs_set.active:
                prev_candles.pop(key, None)

        if not adx_symbols_set.should_filter:
            if adx_states:
                adx_states.clear()
            if prev_candles:
                prev_candles.clear()

    last_refresh = 0.0

    def refresh_config(force: bool = False) -> None:
        nonlocal last_refresh
        now_mon = time.monotonic()
        if not force and (now_mon - last_refresh) < CONFIG_REFRESH_INTERVAL_SEC:
            return
        last_refresh = now_mon
        changed = False
        for dyn in (atr_symbols_set, atr_tfs_set, adx_symbols_set, adx_tfs_set):
            if dyn.refresh(r):
                changed = True
        if changed:
            prune_states()

    refresh_config(force=True)
    
    print(f"📊 Listening for candles...")
    print()
    
    # Main loop
    while True:
        try:
            refresh_config()
            msgs = r.xreadgroup(
                GROUP,
                CONSUMER,
                {CANDLES_STREAM: ">"},
                count=200,
                block=1000
            )
            
            for stream, entries in msgs or []:
                for msg_id, fields in entries:
                    try:
                        # Extract symbol and timeframe
                        sym = _normalize_symbol(fields.get("symbol") or "")
                        tf_raw = fields.get("tf") or fields.get("timeframe")
                        tf = _normalize_tf(tf_raw) if tf_raw else None

                        if not sym or tf is None:
                            continue

                        if not atr_symbols_set.allows(sym):
                            continue
                        if not atr_tfs_set.allows(tf):
                            continue
                        
                        # Get payload
                        payload = fields.get("payload") or fields.get("data")
                        if not payload:
                            continue
                        
                        # Parse candle data
                        try:
                            d = json.loads(payload)
                        except json.JSONDecodeError:
                            print(f"⚠️  Invalid JSON in payload: {payload[:100]}")
                            continue
                        
                        # Extract OHLC
                        h = float(d.get("high") or d.get("h") or 0.0)
                        l = float(d.get("low") or d.get("l") or 0.0)
                        c = float(d.get("close") or d.get("c") or 0.0)
                        
                        if h <= 0 or l <= 0 or c <= 0:
                            continue
                        
                        now_ms = get_ny_time_millis()
                        
                        # Update ATR state
                        key = (sym, tf)
                        val = atr_states[key].feed(h, l, c)
                        
                        # Store to Redis if initialized AND valid (must be > 0)
                        if val is not None and val > 0:
                            # Store float value
                            r.set(f"atr:{sym}:{tf}", f"{val:.8f}")
                            # Legacy format for compatibility
                            r.set(f"atr:val:{sym}:{tf}", f"{val:.8f}")
                            
                            # Store JSON metadata (legacy format)
                            meta = {
                                "symbol": sym,
                                "tf": tf,
                                "atr": val,
                                "period": PERIOD,
                                "close": c,
                                "count": atr_states[key].count,
                                "ts": now_ms,
                            }
                            r.set(f"atr:json:{sym}:{tf}", json.dumps(meta))
                            
                            # Store explicit timestamp keys for legacy atr_string lookups
                            r.set(f"atr:{sym}:{tf}:ts_ms", str(now_ms), ex=120)
                            r.set(f"atr:val:{sym}:{tf}:ts_ms", str(now_ms), ex=120)
                            
                            # Store in go-gateway format (ta:last:atr:SYMBOL) with 2 minute TTL
                            gw_meta = {
                                "atr": val,
                                "period": PERIOD,
                                "method": "wilder",
                                "tf": tf if tf else "M1",
                                "source": "py",
                                "ts": now_ms,
                            }
                            r.setex(f"ta:last:atr:{sym}", 120, json.dumps(gw_meta))
                            
                            if atr_states[key].count % 100 == 0:
                                print(f"✅ {sym}:{tf} ATR={val:.8f} (count={atr_states[key].count})")
                        elif val is not None and val <= 0:
                            # Log warning for invalid ATR values
                            if atr_states[key].count % 100 == 0:
                                print(f"⚠️  {sym}:{tf} ATR={val:.8f} is invalid (<=0), skipping storage (count={atr_states[key].count})")

                        # Update ADX/DI if enabled
                        if adx_symbols_set.should_filter and adx_symbols_set.allows(sym) and adx_tfs_set.allows(tf):
                            o = float(d.get("open") or d.get("o") or 0.0)
                            if o <= 0:
                                prev_candles.pop(key, None)
                            else:
                                prev = prev_candles.get(key)
                                if prev is not None:
                                    state = adx_states[key]
                                    state, res = update_adx_atr(
                                        state,
                                        h,
                                        l,
                                        c,
                                        prev["high"],
                                        prev["low"],
                                        prev["close"],
                                        n=ADX_PERIOD,
                                    )
                                    adx_states[key] = state
                                    if res:
                                        payload = {
                                            "atr": res["atr"],
                                            "plusDI": res["plusDI"],
                                            "minusDI": res["minusDI"],
                                            "adx": res["adx"],
                                            "tf": tf,
                                            "period": ADX_PERIOD,
                                            "ts": now_ms,
                                        }
                                        r.hset(ADX_KEY_TEMPLATE.format(symbol=sym), mapping=payload)
                                prev_candles[key] = {
                                    "open": o,
                                    "high": h,
                                    "low": l,
                                    "close": c,
                                }
                    
                    except Exception as e:
                        print(f"❌ Error processing message: {e}")
                    
                    finally:
                        # Always ACK message
                        r.xack(stream, GROUP, msg_id)
        
        except redis.exceptions.ResponseError as e:
            error_str = str(e).upper()
            if "NOGROUP" in error_str:
                print(f"⚠️ Consumer group {GROUP} missing (NOGROUP). Attempting to recreate...")
                if ensure_consumer_group(r, CANDLES_STREAM, GROUP):
                    print("✅ Successfully recreated consumer group. Resuming...")
                else:
                    print("❌ Failed to recreate consumer group. Sleeping 5s...")
                    time.sleep(5)
            else:
                print(f"⚠️ Redis response error in main loop: {e}. sleeping 5s...")
                time.sleep(5)

        except redis.RedisError as e:
            print(f"⚠️ Redis error in main loop: {e}. sleeping 5s...")
            time.sleep(5)
        except Exception as e:
            print(f"❌ Unexpected error in main loop: {e}. sleeping 5s...")
            time.sleep(5)


if __name__ == "__main__":
    main()

