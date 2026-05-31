from __future__ import annotations
import os, json, time, signal, sys
from dataclasses import dataclass
from typing import Dict, List, Optional

import redis
from redis.exceptions import RedisError, ConnectionError as RedisConnectionError
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
SYMBOL    = os.getenv("SYMBOL", "XAUUSD")
SYMBOLS_RAW = os.getenv("SYMBOLS", "")
PAPER_SYMBOLS_MODE = os.getenv("PAPER_SYMBOLS_MODE", "env").lower()
PAPER_SYMBOLS_REFRESH_SEC = int(os.getenv("PAPER_SYMBOLS_REFRESH_SEC", "30"))
PAPER_SYMBOLS_SCAN_COUNT = int(os.getenv("PAPER_SYMBOLS_SCAN_COUNT", "500"))
PAPER_SYMBOLS_MAX = int(os.getenv("PAPER_SYMBOLS_MAX", "200"))

def _parse_symbols(raw: str) -> List[str]:
    if raw:
        return [s.strip().upper() for s in raw.split(",") if s.strip()]
    return [str(SYMBOL).upper()]

SYMBOLS = _parse_symbols(SYMBOLS_RAW)

ORDERS_STREAM = os.getenv("PAPER_ORDERS_STREAM", "paper:orders")
DEALS_STREAM  = os.getenv("PAPER_DEALS_STREAM",  "paper:deals")
PARQUET_PATH  = os.getenv("PAPER_PARQUET", "/data/paper_trades.parquet")

PAPER_GROUP_ENV = os.getenv("PAPER_GROUP", "")
PAPER_CONSUMER_ENV = os.getenv("PAPER_CONSUMER", f"paper-{int(time.time())}")

TICK_STREAM = os.getenv("TICK_STREAM", "")
TICK_STREAM_PREFIX = os.getenv("TICK_STREAM_PREFIX", "stream:tick_")
if TICK_STREAM and len(SYMBOLS) == 1:
    TICK_STREAMS = {SYMBOLS[0]: TICK_STREAM}
else:
    TICK_STREAMS = {
        sym: os.getenv(f"TICK_STREAM_{sym}", f"{TICK_STREAM_PREFIX}{sym}")
        for sym in SYMBOLS
    }

def create_redis_connection(max_retries: int = 10, retry_delay: int = 2) -> redis.Redis:
    """Create Redis connection with retry logic"""
    for attempt in range(max_retries):
        try:
            print(f"[PaperExecutor] Connecting to Redis at {REDIS_URL} (attempt {attempt + 1}/{max_retries})...", flush=True)
            r = redis.Redis.from_url(
                REDIS_URL, 
                decode_responses=True,
                socket_connect_timeout=5,
                socket_keepalive=True,
                health_check_interval=30,
                max_connections=10
            )
            # Test connection
            r.ping()
            print(f"[PaperExecutor] Successfully connected to Redis!", flush=True)
            return r
        except (RedisError, RedisConnectionError, Exception) as e:
            print(f"[PaperExecutor] Redis connection attempt {attempt + 1} failed: {e}", flush=True)
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
            else:
                print(f"[PaperExecutor] Failed to connect to Redis after {max_retries} attempts", flush=True)
                raise
    raise RedisConnectionError("Failed to establish Redis connection")

def _discover_symbols(r: redis.Redis) -> List[str]:
    """Discover symbols by scanning tick streams in Redis."""
    prefix = str(TICK_STREAM_PREFIX)
    pattern = f"{prefix}*"
    out: List[str] = []
    seen = set()
    cursor = 0
    while True:
        cursor, keys = r.scan(cursor=cursor, match=pattern, count=PAPER_SYMBOLS_SCAN_COUNT)
        for key in keys or []:
            if not isinstance(key, str):
                try:
                    key = key.decode("utf-8", errors="ignore")
                except Exception:
                    continue
            if not key.startswith(prefix):
                continue
            sym = key[len(prefix):].strip().upper()
            if not sym or sym in seen:
                continue
            seen.add(sym)
            out.append(sym)
            if len(out) >= PAPER_SYMBOLS_MAX:
                return out
        if cursor == 0:
            break
    return out

@dataclass
class Leg:
    tp: float
    vol: float
    hit: bool = False
    ts_hit: Optional[int] = None

@dataclass
class PaperPosition:
    sid: str
    symbol: str
    side: str      # LONG/SHORT
    entry: float   # для market можно подставить первый mid после ордера
    sl: float
    legs: List[Leg]
    lot: float
    opened_ts: int
    closed: bool = False
    closed_ts: Optional[int] = None

class PaperExecutor:
    def __init__(self):
        self.r = create_redis_connection()
        self.positions: Dict[str, PaperPosition] = {}
        self.paper_symbols_mode = PAPER_SYMBOLS_MODE
        self.symbols: List[str] = []
        self.symbol_set: set[str] = set()
        self.tick_streams: Dict[str, str] = {}
        self.group = PAPER_GROUP_ENV or ""
        self.consumer = PAPER_CONSUMER_ENV
        self._last_symbol_refresh = 0.0
        self._refresh_symbols(force=True)
        if not self.group:
            if len(self.symbols) == 1:
                self.group = f"paper-{self.symbols[0].lower()}-grp"
            else:
                self.group = "paper-multi-grp"
        # Consumer group will be created when needed in _ingest_order

        self.stop = False
        signal.signal(signal.SIGINT, self._sig)
        signal.signal(signal.SIGTERM, self._sig)
        mode = self.paper_symbols_mode.upper()
        print(f"[PaperExecutor] Initialized - mode={mode} symbols={','.join(self.symbols)}", flush=True)

    def _refresh_symbols(self, force: bool = False) -> None:
        now = time.time()
        if not force and self.paper_symbols_mode != "auto":
            return
        if not force and PAPER_SYMBOLS_REFRESH_SEC > 0 and now - self._last_symbol_refresh < PAPER_SYMBOLS_REFRESH_SEC:
            return

        self._last_symbol_refresh = now
        symbols: List[str] = []
        if self.paper_symbols_mode == "auto":
            try:
                symbols = _discover_symbols(self.r)
            except Exception as exc:
                print(f"[PaperExecutor] Symbol discovery failed: {exc}", flush=True)

        if not symbols:
            symbols = [s.upper() for s in _parse_symbols(SYMBOLS_RAW)]
        if not symbols:
            symbols = [str(SYMBOL).upper()]

        if symbols == self.symbols:
            return

        self.symbols = symbols
        self.symbol_set = set(symbols)
        if len(self.symbols) == 1 and TICK_STREAM:
            self.tick_streams = {self.symbols[0]: TICK_STREAM}
        else:
            self.tick_streams = {
                sym: os.getenv(f"TICK_STREAM_{sym}", f"{TICK_STREAM_PREFIX}{sym}")
                for sym in self.symbols
            }
        print(f"[PaperExecutor] Symbols refreshed: {','.join(self.symbols)}", flush=True)

    def _sig(self, *a): self.stop = True

    def _mid(self, tick: Dict) -> float:
        bid = float(tick.get("bid", 0) or 0.0)
        ask = float(tick.get("ask", 0) or 0.0)
        last = float(tick.get("last", 0) or 0.0)
        if bid and ask:
            return (bid + ask)/2.0
        return last

    def _append_deal(self, pos: PaperPosition, kind: str, price: float, ts: int, note: str):
        # Map to SignalPerformanceTracker expected event types
        kind_map = {
            "OPEN": "ENTRY_FILLED",
            "SL": "STOP_HIT",
            "TP": "TP_HIT",
            "CLOSE": "MANUAL_EXIT"
        }
        event_type = kind_map.get(kind, kind)

        # Write to DEALS_STREAM (configured to events:trades usually)
        # We include BOTH formats for compatibility:
        # 1. Native paper executor fields (kind, sid)
        # 2. Standard tracker fields (event_type, signal_id)
        
        payload = {
            # Legacy
            "sid": pos.sid, "symbol": pos.symbol, "side": pos.side,
            "kind": kind, "price": round(price,2), "ts": ts, "note": note,
            
            # Standard (for SignalPerformanceTracker)
            "event_type": event_type,
            "signal_id": pos.sid,
            # Tracker might expect ISO timestamp or handle int. 
            # Ideally standard bus uses 'ts' as string or int.
            # We keep 'ts' as int from argument.
        }
        
        self.r.xadd(DEALS_STREAM, {
            "data": json.dumps(payload)
        }, maxlen=50000)

    def _write_parquet(self):
        rows = []
        for p in self.positions.values():
            rr_hits = sum(1 for l in p.legs if l.hit)
            rows.append({
                "sid": p.sid, "symbol": p.symbol, "side": p.side,
                "entry": p.entry, "sl": p.sl, "tp_hits": rr_hits,
                "lot": p.lot, "opened_ts": p.opened_ts, "closed": p.closed,
                "closed_ts": p.closed_ts,
            })
        if not rows: return
        table = pa.Table.from_pandas(pd.DataFrame(rows))
        pq.write_table(table, PARQUET_PATH)

    def _ingest_order(self):
        """
        Consume orders from Redis list using BRPOP (blocking right pop).
        Producers use LPUSH, so we use BRPOP to maintain FIFO order.
        """
        try:
            # Use BRPOP to consume from the list (timeout=1 second)
            # Returns: (key, value) or None if timeout
            result = self.r.brpop(ORDERS_STREAM, timeout=1)
            if not result:
                return
            
            _, raw = result
            try:
                payload = json.loads(raw)
                payload_symbol = str(payload.get("symbol") or "").upper()
                
                # Symbol filtering (if configured)
                if self.symbol_set and payload_symbol and payload_symbol not in self.symbol_set:
                    # Different symbol - skip (no way to re-queue in list model)
                    return
                
                pos = self._payload_to_pos(payload)
                self.positions[pos.sid] = pos
                print(f"[PaperExecutor] New position: {pos.sid} {pos.side} @ {pos.entry} SL={pos.sl}", flush=True)

                # Emit OPEN event immediately if entry is determined (Limit/Stop/Market with price)
                # This ensures SignalPerformanceTracker transitions from PENDING to ACTIVE
                if pos.entry > 0:
                    self._append_deal(pos, "OPEN", pos.entry, int(time.time()*1000), "open-simulation")

            except json.JSONDecodeError as e:
                print(f"[PaperExecutor] Invalid JSON in order: {e}", flush=True)
            except Exception as e:
                print(f"[PaperExecutor] Error processing order: {e}", flush=True)

        except (RedisError, RedisConnectionError) as e:
            print(f"[PaperExecutor] Redis connection error in _ingest_order: {e}", flush=True)
        except Exception as e:
            print(f"[PaperExecutor] Unexpected error in _ingest_order: {e}", flush=True)

    def _payload_to_pos(self, p: Dict) -> PaperPosition:
        symbol = str(p.get("symbol") or self.symbols[0]).upper()
        sid = p.get("sid") or f"{symbol}:{int(time.time()*1000)}"
        
        # Normalize side
        raw_side = str(p.get("side", "")).upper()
        side_map = {"BUY": "LONG", "SELL": "SHORT", "LONG": "LONG", "SHORT": "SHORT"}
        side = side_map.get(raw_side, raw_side) # Default to raw if unknown, but normally should be LONG/SHORT
        if side not in ("LONG", "SHORT"):
             print(f"[PaperExecutor] ⚠️ Unknown side '{raw_side}' in order: {json.dumps(p)}", flush=True)

        # Robust SL handling
        if "sl" not in p:
            print(f"[PaperExecutor] ⚠️ MISSING 'sl' in order: {json.dumps(p)}", flush=True)
            # Default SL if missing (simulation safe-guard)
            entry_p = float(p.get("entry", 0.0))
            if entry_p > 0 and side == "LONG":
                sl = entry_p * 0.9  # 10% default
            elif entry_p > 0 and side == "SHORT":
                sl = entry_p * 1.1  # 10% default
            else:
                sl = 0.0
        else:
            sl = float(p["sl"])
            
        tps = [float(x) for x in p.get("tp_levels", [])]
        lot = float(p.get("lot", 0.01))
        entry = float(p.get("entry", 0.0))
        legs = [Leg(tp=x, vol=round(lot * w, 3)) for x, w in zip(tps, self._weights(len(tps)))]
        return PaperPosition(sid=sid, symbol=symbol, side=side, entry=entry, sl=sl,
                             legs=legs, lot=lot, opened_ts=int(time.time()*1000))

    def _weights(self, n: int) -> List[float]:
        if n<=0: return []
        if n==1: return [1.0]
        if n==2: return [0.6, 0.4]
        if n>=3: return [0.5, 0.3, 0.2][:n]

    def _apply_tick(self, symbol: str, tick: Dict):
        mid = self._mid(tick)
        ts = int(tick.get("ts", int(time.time()*1000)))
        for pos in list(self.positions.values()):
            if pos.symbol != symbol:
                continue
            if pos.closed: continue
            if pos.entry <= 0.0:
                pos.entry = mid
                self._append_deal(pos, "OPEN", pos.entry, ts, "market-fill")

            if pos.side == "LONG" and mid <= pos.sl:
                self._append_deal(pos, "SL", pos.sl, ts, "stop out")
                pos.closed, pos.closed_ts = True, ts
            elif pos.side == "SHORT" and mid >= pos.sl:
                self._append_deal(pos, "SL", pos.sl, ts, "stop out")
                pos.closed, pos.closed_ts = True, ts
            if pos.closed: continue

            for lg in pos.legs:
                if lg.hit: continue
                if pos.side == "LONG" and mid >= lg.tp:
                    lg.hit, lg.ts_hit = True, ts
                    self._append_deal(pos, "TP", lg.tp, ts, f"leg vol={lg.vol}")
                elif pos.side == "SHORT" and mid <= lg.tp:
                    lg.hit, lg.ts_hit = True, ts
                    self._append_deal(pos, "TP", lg.tp, ts, f"leg vol={lg.vol}")

            if all(lg.hit for lg in pos.legs):
                pos.closed, pos.closed_ts = True, ts
                self._append_deal(pos, "CLOSE", mid, ts, "all TPs done")

    def run(self):
        last_id = "$"
        reconnect_attempts = 0
        max_reconnect_attempts = 5
        
        print(f"[PaperExecutor] Starting main loop...", flush=True)
        
        while not self.stop:
            try:
                if self.paper_symbols_mode == "auto":
                    self._refresh_symbols()
                self._ingest_order()
                for symbol in self.symbols:
                    stream = self.tick_streams.get(symbol)
                    if not stream:
                        continue
                    msgs = self.r.xrevrange(stream, max="+", min="-", count=1)
                    if msgs:
                        _, kv = msgs[0]
                        try:
                            data = json.loads(kv.get("data") or "{}")
                        except Exception:
                            data = kv
                        self._apply_tick(symbol, data)
                time.sleep(0.05)
                reconnect_attempts = 0  # Reset on success
            except (RedisError, RedisConnectionError) as e:
                reconnect_attempts += 1
                print(f"[PaperExecutor] Redis connection error (attempt {reconnect_attempts}): {e}", flush=True)
                if reconnect_attempts >= max_reconnect_attempts:
                    print(f"[PaperExecutor] Too many reconnection attempts, exiting...", flush=True)
                    break
                try:
                    self.r = create_redis_connection()
                except Exception as reconnect_error:
                    print(f"[PaperExecutor] Reconnection failed: {reconnect_error}", flush=True)
                    time.sleep(2)
            except Exception as e:
                print(f"[PaperExecutor] Unexpected error in main loop: {e}", flush=True)
                time.sleep(0.2)

        print(f"[PaperExecutor] Shutting down, writing parquet...", flush=True)
        self._write_parquet()
        print(f"[PaperExecutor] Shutdown complete", flush=True)

if __name__ == "__main__":
    PaperExecutor().run()


