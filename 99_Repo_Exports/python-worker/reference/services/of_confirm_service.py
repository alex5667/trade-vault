from utils.time_utils import get_ny_time_millis
"""
OFConfirmService (Variant A)

Decoupled service for Order Flow confirmation.
Reads:
  - events:delta_spike (from crypto_orderflow_service)
  - events:microbar_closed (from crypto_orderflow_service)
  - config:orderflow:<symbol> (Redis Hash)

Writes:
  - signals:of:confirm (output stream)

Logic:
  - Maintains minimal state (regime, sweep, reclaim, pressure) from events.
  - Uses OFConfirmEngine to validate spikes.
  - Generates confirmation signals.
"""

import asyncio
from utils.task_manager import safe_create_task

import json
import logging
import os
import signal
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import redis.asyncio as aioredis
from prometheus_client import Counter, start_http_server

from core.crypto_orderflow_detectors import (
    DeltaSpikeDetector, OBIDetector, IcebergDetector, AbsorptionDetector
)
from core.of_confirm_engine import OFConfirmEngine
from core.pressure_tracker import PressureTracker
from core.redis_stream_consumer import AsyncRedisStreamHelper
from core.instrument_config import get_config

# Metrics
signals_processed_total = Counter("of_confirm_signals_processed_total", "Total signals processed", ["symbol", "status"])
confirm_signals_total = Counter("of_confirm_signals_out_total", "Total confirmed signals published", ["symbol"])
events_received_total = Counter("of_confirm_events_received_total", "Total events received", ["type", "symbol"])

# Logging
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO")
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("of_confirm_service")


@dataclass
class SymbolState:
    symbol: str
    
    # Detectors (Independent Mode)
    delta_detector: Optional[DeltaSpikeDetector] = None
    obi_detector: Optional[OBIDetector] = None
    iceberg_detector: Optional[IcebergDetector] = None
    absorption_detector: Optional[AbsorptionDetector] = None
    
    # State tracking
    last_regime: str = "na"
    last_sweep: Optional[Any] = None
    last_reclaim: Optional[Any] = None
    last_wp: Optional[Any] = None
    last_div: Optional[Any] = None
    
    # Pressure tracker (local calculation based on incoming spikes)
    pressure: PressureTracker = field(default_factory=PressureTracker)
    
    # Caches
    last_obi_event: Optional[Dict[str, Any]] = None
    last_iceberg_event: Optional[Dict[str, Any]] = None
    
    # Config cache
    config: Dict[str, Any] = field(default_factory=dict)
    config_last_fetch: float = 0.0

    # Dynamic cfg accumulator (similar to SymbolRuntime)
    dynamic_cfg: Dict[str, Any] = field(default_factory=dict)

    # Dummy attributes to satify OFConfirmEngine runtime interface
    last_bar: Any = None 
    book_churn_hi: int = 0
    cont_ctx_ts_ms: int = 0


class OFConfirmService:
    def __init__(self):
        self.redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
        self.redis: Optional[aioredis.Redis] = None
        self.consumer_group = os.getenv("CONSUMER_GROUP", "of_confirm_group")
        self.consumer_name = os.getenv("HOSTNAME", "of_confirm_worker")
        
        # Streams
        self.stream_spikes = "events:delta_spike"
        # Legacy shared stream (kept for migration / dual-write)
        self.stream_bars_legacy = os.getenv("MICROBAR_LEGACY_STREAM", "events:microbar_closed")
        # Per-symbol stream template (preferred when split enabled)
        self.stream_bars_template = os.getenv("MICROBAR_PER_SYMBOL_STREAM_TEMPLATE", "events:microbar_closed:{sym}")
        self.microbar_symbols_set = os.getenv("MICROBAR_SYMBOLS_SET", "events:microbar_closed:symbols")
        self.microbar_split = os.getenv("MICROBAR_SPLIT_STREAMS_ENABLE", "0") == "1"
        # Backward compatible alias
        self.stream_bars = self.stream_bars_legacy
        # Output stream
        self.stream_out = os.getenv("OF_CONFIRM_STREAM", "signals:of:confirm")

        # Optional: enable actual microbar consumption (was previously unused)
        self.bars_enable = os.getenv("OF_CONFIRM_BARS_ENABLE", "0") == "1"
        self.bars_max_streams = int(os.getenv("OF_CONFIRM_BARS_MAX_STREAMS", "200"))
        
        self.states: Dict[str, SymbolState] = {}
        self.engine = OFConfirmEngine(version=3)
        self.running = True

    async def start(self):
        logger.info(f"Starting OFConfirmService... streams={self.stream_spikes},{self.stream_bars}")
        self.redis = aioredis.from_url(self.redis_url, decode_responses=True)
        
        # Wait for Redis to be ready (BusyLoadingError)
        from core.redis_client import wait_for_redis_async
        if not await wait_for_redis_async(self.redis):
            logger.error("❌ Redis is not ready after wait. Exiting.")
            return

        
        # Ensure consumer group exists
        for stream in [self.stream_spikes, self.stream_bars_legacy]:
            try:
                await self.redis.xgroup_create(stream, self.consumer_group, mkstream=True)
            except aioredis.ResponseError as e:
                if "BUSYGROUP" not in str(e):
                    logger.warning(f"Error creating group for {stream}: {e}")

        # If split streams are enabled, create groups lazily on discovered per-symbol streams.
        if self.microbar_split:
            await self._ensure_microbar_groups()

        # Start metrics server
        start_http_server(int(os.getenv("PROMETHEUS_PORT", 8003)))

        # Consumers
        tasks = [
            safe_create_task(self._consume_ticks())
            safe_create_task(self._consume_books())
            safe_create_task(self._consume_bars())
            safe_create_task(self._config_refresher())
        ]
        
        await asyncio.gather(*tasks)

    async def _consume_ticks(self):
        logger.info("Started raw tick consumer loop")
        # We use a pattern to subscribe to all crypto ticks
        pattern = "ticks:crypto:binance_futures:*"

        # ------------------------------------------------------------------
        # Optional: microbar stream consumption (split-stream aware)
        # This service historically did not consume microbars even though it created groups.
        # To keep behavior stable, it is guarded behind OF_CONFIRM_BARS_ENABLE=1.
        # ------------------------------------------------------------------
        if getattr(self, "bars_enable", False) and not getattr(self, "_bars_task", None):
            self._bars_task = safe_create_task(self._consume_microbars())
        
        while self.running:
            try:
                # Use PUBSUB for ticks (low latency)
                pubsub = self.redis.pubsub()
                await pubsub.psubscribe(pattern)
                
                async for message in pubsub.listen():
                    if not self.running: break
                    if message["type"] != "pmessage": continue
                    
                    channel = message["channel"]
                    # Channel format: ticks:crypto:binance_futures:<symbol>
                    try:
                        symbol = channel.split(":")[-1]
                        await self._process_tick(symbol, message["data"])
                    except Exception as e:
                        logger.error(f"Tick process error: {e}")
                        
            except Exception as e:
                logger.error(f"Tick consumer connection error: {e}")
                await asyncio.sleep(1)

        # stop bars task
        t = getattr(self, "_bars_task", None)
        if t:
            try:
                t.cancel()
            except Exception:
                pass

    async def _consume_books(self):
        logger.info("Started book stream consumer loop")
        # Stream pattern: stream:book_l2_top5:<symbol>
        # We need to discover active streams or rely on config. 
        # For simplicity in this variant, we'll scan for active book streams or use XREADGroup on known keys if dynamic.
        # But standard redis streams for books are usually per-symbol. 
        # We will use a dedicated consumer group on a fixed list or pattern if supported.
        # Since Redis Streams don't support PSUBSCRIBE pattern matching for groups in the same way
        # we often use a loop to sync active symbols. For now, we iterate known active symbols.
        
        while self.running:
            try:
                # 1. Get active symbols from SymbolState or Config
                symbols = list(self.states.keys())
                if not symbols:
                    await asyncio.sleep(1)
                    continue
                
                streams = {f"stream:book_l2_top5:{s}": ">" for s in symbols}
                
                # Ensure groups exist (lazy)
                for s in streams:
                    try:
                        await self.redis.xgroup_create(s, self.consumer_group, mkstream=True)
                    except aioredis.ResponseError as e:
                        if "BUSYGROUP" not in str(e): pass
                
                helper = AsyncRedisStreamHelper(self.redis, self.consumer_group, self.consumer_name)
                events = await helper.read(streams, count=50, block=1000)
                
                for stream_name, messages in events:
                    symbol = stream_name.split(":")[-1]
                    for msg_id, fields in messages:
                        await self._process_book(symbol, fields)
                        await self.redis.xack(stream_name, self.consumer_group, msg_id)
                        
            except Exception as e:
                logger.error(f"Book consumer error: {e}")
                await asyncio.sleep(1)

    async def _consume_bars(self):
        """Consumer for events:microbar_closed stream."""
        logger.info("Started microbar_closed stream consumer loop")
        
        while self.running:
            try:
                helper = AsyncRedisStreamHelper(self.redis, self.consumer_group, self.consumer_name)
                events = await helper.read({self.stream_bars: ">"}, count=100, block=1000)
                
                for stream_name, messages in events:
                    if stream_name != self.stream_bars:
                        continue
                    
                    for msg_id, fields in messages:
                        try:
                            await self._process_bar(fields)
                            await self.redis.xack(self.stream_bars, self.consumer_group, msg_id)
                        except Exception as e:
                            logger.error(f"Error processing bar message {msg_id}: {e}")
                            # ACK even on error to avoid poison pills
                            try:
                                await self.redis.xack(self.stream_bars, self.consumer_group, msg_id)
                            except Exception:
                                pass
                            
            except aioredis.ResponseError as e:
                msg = str(e)
                if "NOGROUP" in msg:
                    logger.warning(f"Consumer group missing for {self.stream_bars}, recreating...")
                    try:
                        await self.redis.xgroup_create(self.stream_bars, self.consumer_group, mkstream=True)
                    except Exception as create_err:
                        logger.error(f"Failed to recreate group: {create_err}")
                else:
                    logger.error(f"Bar consumer error: {e}")
                await asyncio.sleep(1)
            except Exception as e:
                logger.error(f"Bar consumer error: {e}")
                await asyncio.sleep(1)

    async def _process_tick(self, symbol: str, data: str):
        try:
            state = self._get_state(symbol)
            # Parse tick (assuming JSON or specific format)
            # Check if format is JSON or specialized
            try:
                tick = json.loads(data)
            except (ValueError, json.JSONDecodeError):
                # Fallback / skip if not json
                return

            tick_ts = int(tick.get("T", 0)) # Binance format often T=time
            price = float(tick.get("p", 0.0))
            qty = float(tick.get("q", 0.0))
            is_buyer_maker = bool(tick.get("m", False))
            
            # Feed Delta Detector
            # Detector expects dict with price, qty, is_buyer_maker
            # Adjust input format to what DeltaSpikeDetector expects (often internal fmt)
            # Based on crypto_orderflow_service, it calls push(tick_payload)
            
            # We construct a normalized tick payload
            norm_tick = {
                "ts": tick_ts
                "price": price
                "qty": qty
                "is_buyer_maker": is_buyer_maker
            }
            
            # 1. Delta Spike
            if state.delta_detector:
                spike_event = state.delta_detector.push(norm_tick)
                if spike_event:
                    await self._check_spike(symbol, state, spike_event)
            
            # 2. Absorption (needs trade stream)
            if state.absorption_detector:
                # Detector requires more complex state (CVD, etc). 
                # For MVP Variant C, we focus on Delta Spikes. 
                # If absorption is needed, we push to it.
                pass 

        except Exception:
            pass

    async def _process_book(self, symbol: str, fields: Dict[str, Any]):
        try:
            state = self._get_state(symbol)
            # Parse L2 Top5
            payload = fields.get("payload")
            if not payload: return
            book = json.loads(payload)
            
            # Feed OBI
            if state.obi_detector:
                # Detector.push(timestamp, bids, asks)
                # book format expected: {ts:..., b:[[p,q],...], a:[...]}
                bids = book.get("b", [])
                asks = book.get("a", [])
                ts = book.get("ts", 0)
                obi_res = state.obi_detector.push(ts, bids, asks)
                if obi_res:
                    state.last_obi_event = obi_res
            
            # Feed Iceberg
            if state.iceberg_detector:
                # Iceberg detection usually needs L2 updates + trades context
                # Simpler version might work on snapshot updates
                pass
                
        except Exception:
            pass

    async def _check_spike(self, symbol: str, state: SymbolState, spike: Dict[str, Any]):
        """
        Runs OFConfirmEngine when a local spike is detected.
        """
        try:
            events_received_total.labels(type="local_spike", symbol=symbol).inc()
            
            ts_ms = spike.get("ts_ms", get_ny_time_millis())
            state.pressure.on_raw_trigger(ts_ms=ts_ms)
            
            delta_z = spike.get("delta_z", 0.0)
            
            indicators = {
                "now_ts_ms": ts_ms
                "delta": spike.get("delta", 0.0)
                "delta_z": delta_z
            }
            
            # Run engine
            of_confirm, decision = self.engine.build(
                symbol=symbol
                tf="tick"
                direction=spike.get("direction", "none")
                tick_ts_ms=ts_ms
                price=spike.get("price", 0.0)
                delta_z=delta_z
                runtime=state
                cfg=state.config
                indicators=indicators
            )
            
            status = "skipped"
            if of_confirm and of_confirm.ok:
                status = "confirmed"
                out_payload = of_confirm.to_dict()
                out_payload["generated_at"] = get_ny_time_millis()
                await self.redis.xadd(
                    self.stream_out
                    {"payload": json.dumps(out_payload)}
                    maxlen=50000
                    approximate=True
                )
                confirm_signals_total.labels(symbol=symbol).inc()
            
            signals_processed_total.labels(symbol=symbol, status=status).inc()

        except Exception as e:
            logger.error(f"Check spike failed: {e}")

    async def _ensure_microbar_groups(self) -> List[str]:
        """
        Ensure consumer group exists for per-symbol microbar streams.
        Returns the list of active stream keys.
        """
        # If template has no {sym}, treat it as a single stream key
        if "{sym}" not in self.stream_bars_template:
            keys = [self.stream_bars_template]
        else:
            keys: List[str] = []
            cursor = 0
            seen = 0
            while True:
                cursor, batch = await self.redis.sscan(self.microbar_symbols_set, cursor=cursor, count=10000)
                for s in batch or []:
                    sym = s.decode("utf-8", "ignore") if isinstance(s, bytes) else str(s)
                    if sym:
                        keys.append(self.stream_bars_template.format(sym=sym))
                        seen += 1
                        if seen >= self.bars_max_streams:
                            cursor = 0
                            break
                if int(cursor) == 0:
                    break

        # create group on each stream (mkstream=True so empty streams don't crash)
        for k in keys:
            try:
                await self.redis.xgroup_create(k, self.consumer_group, mkstream=True)
            except Exception:
                # group exists / stream exists => ignore
                pass
        self._microbar_streams = keys
        return keys

    async def _poll_microbars_once(self) -> int:
        """
        Poll one batch of microbar_closed from split streams using XREADGROUP.
        This is guarded behind OF_CONFIRM_BARS_ENABLE=1 to avoid changing behavior by default.
        """
        if not getattr(self, "bars_enable", False):
            return 0
        keys = getattr(self, "_microbar_streams", None) or (await self._ensure_microbar_groups())
        if not keys:
            return 0

        # XREADGROUP supports reading from multiple streams in one call.
        # Use '>' to read new messages for this group.
        stream_map: Dict[str, str] = {k: ">" for k in keys}
        try:
            resp = await self.redis.xreadgroup(
                groupname=self.consumer_group
                consumername=self.consumer_name
                streams=stream_map
                count=int(os.getenv("OF_CONFIRM_BARS_BATCH", "200"))
                block=int(os.getenv("OF_CONFIRM_BARS_BLOCK_MS", "500"))
            )
        except Exception:
            return 0

        n = 0
        for stream, entries in resp or []:
            for msg_id, fields in entries or []:
                n += 1
                try:
                    # Existing hook (previously unused) — now becomes the single place
                    # to parse/process microbar payloads.
                    await self._process_bar(msg_id, fields, stream=stream)
                    # ACK only if processing succeeded
                    await self.redis.xack(stream, self.consumer_group, msg_id)
                except Exception:
                    # On failure: do not ack => message remains pending for retry
                    pass
        return n

    async def _process_bar(self, msg_id: str = None, fields: Dict[str, Any] = None, stream: str = None):
        """
        Process a microbar message. Supports both legacy (fields dict) and new (msg_id, fields, stream) signatures.
        """
        try:
            # Handle legacy call signature (fields only)
            if fields is None and isinstance(msg_id, dict):
                fields = msg_id
                msg_id = None
                stream = None
            
            payload_str = fields.get("payload")
            if not payload_str:
                return
            
            bar = json.loads(payload_str)
            symbol = bar.get("symbol")
            if not symbol: 
                return

            state = self._get_state(symbol)
            events_received_total.labels(type="microbar_closed", symbol=symbol).inc()

            # Sync state
            if "regime" in bar:
                state.last_regime = bar["regime"]
            
            if "sweep" in bar and bar["sweep"]:
                # Reconstruct simple sweep object
                from types import SimpleNamespace
                sw = bar["sweep"]
                state.last_sweep = SimpleNamespace(
                    kind=sw.get("kind")
                    ts_ms=sw.get("ts_ms")
                    # Add defaults if engine needs them
                    direction_bias="NONE" 
                )
                
            if "reclaim" in bar and bar["reclaim"]:
                from types import SimpleNamespace
                rc = bar["reclaim"]
                state.last_reclaim = SimpleNamespace(
                    hold_bars=rc.get("hold_bars")
                    ts_ms=rc.get("ts_ms")
                    direction_bias="NONE"
                )
            
            if "weak_progress" in bar:
                 from types import SimpleNamespace
                 # Engine checks: getattr(runtime.last_wp, "weak_any", False)
                 state.last_wp = SimpleNamespace(weak_any=bool(bar["weak_progress"]))

            if "last_div_kind" in bar and bar["last_div_kind"]:
                from types import SimpleNamespace
                state.last_div = SimpleNamespace(kind=bar["last_div_kind"])

        except Exception as e:
            logger.error(f"Failed to process bar: {e}")

    async def _consume_microbars(self):
        """Background loop to read microbar_closed from Redis Streams via XREADGROUP.

        Uses per-symbol streams when MICROBAR_SPLIT_STREAMS_ENABLE=1.
        """
        logger.info("Started microbar stream consumer loop")
        refresh_sec = int(os.getenv("OF_CONFIRM_BARS_REFRESH_SEC", "30"))
        last_refresh_ms = 0

        while self.running:
            try:
                now_ms = get_ny_time_millis()
                if (now_ms - last_refresh_ms) >= (refresh_sec * 1000):
                    # update list of active streams / ensure groups exist
                    try:
                        await self._ensure_microbar_groups()
                    except Exception:
                        pass
                    last_refresh_ms = now_ms

                n = await self._poll_microbars_once()
                # If nothing read, avoid tight loop when block_ms is small
                if n <= 0:
                    await asyncio.sleep(0.05)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Microbar consumer error: {e}")
                await asyncio.sleep(1)

    async def _poll_microbars_loop(self):
        """Main loop for polling microbars from split streams."""
        logger.info("Started microbar split-streams polling loop")
        while self.running:
            try:
                n = await self._poll_microbars_once()
                if n == 0:
                    # No messages, sleep briefly to avoid busy-wait
                    await asyncio.sleep(0.1)
            except Exception as e:
                logger.error(f"Error in microbar polling loop: {e}")
                await asyncio.sleep(1)

    async def _config_refresher(self):
        """Periodically refresh config for active symbols and discover new ones"""
        while self.running:
            try:
                # 1. Discover symbols from Env and Redis
                env_syms = os.getenv("SYMBOLS", "").split(",")
                symbols = set([s.strip().upper() for s in env_syms if s.strip()])
                
                try:
                    redis_syms = await self.redis.smembers("crypto:symbols")
                    if redis_syms:
                        symbols.update([s.upper() for s in redis_syms])
                except Exception:
                    pass
                
                # 2. Initialize state for new symbols
                for sym in symbols:
                    if sym not in self.states:
                        self._get_state(sym)

                # 3. Refresh config for existing states
                active_symbols = list(self.states.keys())
                for symbol in active_symbols:
                    state = self.states[symbol]
                    if time.time() - state.config_last_fetch > 60:
                        raw_cfg_obj = get_config(symbol)
                        if raw_cfg_obj:
                            from dataclasses import asdict
                            state.config = asdict(raw_cfg_obj)
                        state.config_last_fetch = time.time()
                await asyncio.sleep(10)
            except Exception as e:
                logger.error(f"Config refresh failed: {e}")
                await asyncio.sleep(10)

    def _get_state(self, symbol: str) -> SymbolState:
        if symbol not in self.states:
            # 1. Load config synchronously for init (or use defaults)
            cfg_obj = get_config(symbol, use_env=True)
            config = {}
            if cfg_obj:
                from dataclasses import asdict
                config = asdict(cfg_obj)
            
            # 2. Params
            # Delta
            delta_win = int(config.get("delta_window_ticks", 140))
            delta_z = float(config.get("delta_z_threshold", 3.0))
            
            # OBI
            obi_thr = float(config.get("obi_threshold", 0.35))
            obi_dur = float(config.get("obi_min_duration", 1.5))
            
            # Iceberg
            ice_ref = int(config.get("iceberg_refresh_count", 3))
            ice_dur = float(config.get("iceberg_min_duration", 1.0))
            
            # Absorption (optional for MVP)
            
            # 3. Create State with Detectors
            state = SymbolState(symbol=symbol)
            state.config = config
            
            state.delta_detector = DeltaSpikeDetector(
                window=delta_win
                z_threshold=delta_z
            )
            state.obi_detector = OBIDetector(
                threshold=obi_thr
                hold_secs=obi_dur
            )
            state.iceberg_detector = IcebergDetector(
                min_refresh=ice_ref
                min_duration=ice_dur
            )
            
            self.states[symbol] = state
            logger.info(f"Initialized state for {symbol} (Variant C)")
            
        return self.states[symbol]

    async def shutdown(self):
        self.running = False
        if self.redis:
            await self.redis.aclose()

if __name__ == "__main__":
    service = OFConfirmService()
    
    def handle_sigterm(*args):
        safe_create_task(service.shutdown())
    
    signal.signal(signal.SIGTERM, handle_sigterm)
    
    try:
        asyncio.run(service.start())
    except KeyboardInterrupt:
        pass
