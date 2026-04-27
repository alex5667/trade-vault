import asyncio
import json
import os
import time
import logging
from collections import deque
from dataclasses import dataclass, field
from typing import Dict

from core.redis_client import get_redis
from core.redis_keys import RedisStreams as RS
from services.telegram.telegram_client import TelegramClient

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("ATRSanityMonitor")

@dataclass
class SymbolState:
    errors: deque = field(default_factory=deque)  # stores timestamps of errors
    last_alert_ts: float = 0.0

class ATRSanityMonitor:
    def __init__(self):
        self.redis = get_redis()
        self.telegram = TelegramClient.from_env()
        
        # Config
        self.enabled = bool(int(os.getenv("ATR_MONITOR_ENABLE", "1")))
        self.stream_key = os.getenv("ATR_MONITOR_STREAM", RS.CRYPTO_RAW)
        self.consumer_group = os.getenv("ATR_MONITOR_GROUP", "atr_monitor_group")
        self.consumer_name = os.getenv("ATR_MONITOR_CONSUMER", f"mon-{os.getpid()}")
        
        # Alert logic settings
        self.window_sec = float(os.getenv("ATR_MONITOR_WINDOW_SEC", "60"))
        self.threshold_count = int(os.getenv("ATR_MONITOR_THRESHOLD_COUNT", "3"))
        self.cooldown_sec = float(os.getenv("ATR_MONITOR_COOLDOWN_SEC", "300"))
        
        self.states: Dict[str, SymbolState] = {}
        logger.info(f"Initialized ATRSanityMonitor: enabled={self.enabled}, window={self.window_sec}s, threshold={self.threshold_count}, cooldown={self.cooldown_sec}s")

    async def ensure_group(self):
        try:
            await self.redis.xgroup_create(self.stream_key, self.consumer_group, mkstream=True)
            logger.info(f"Created consumer group {self.consumer_group}")
        except Exception as e:
            if "BUSYGROUP" in str(e):
                pass
            else:
                logger.warning(f"Error creating group: {e}")

    def clean_old_errors(self, state: SymbolState, now: float):
        """Remove errors outside the window."""
        cutoff = now - self.window_sec
        while state.errors and state.errors[0] < cutoff:
            state.errors.popleft()

    async def process_signal(self, payload: dict):
        if not payload:
            return
            
        # Check if sanity check failed
        is_bad = False
        try:
            # atr_sanity_bad is explicitly 1 if bad
            is_bad = int(payload.get("atr_sanity_bad", 0) or 0) == 1
        except Exception:
            pass
            
        if not is_bad:
            return

        symbol = payload.get("symbol", "unknown")
        reason = payload.get("atr_sanity_reason", "unknown")
        
        now = time.time()
        if symbol not in self.states:
            self.states[symbol] = SymbolState()
        
        state = self.states[symbol]
        state.errors.append(now)
        self.clean_old_errors(state, now)
        
        count = len(state.errors)
        logger.warning(f"Detected bad ATR for {symbol}: {reason} (count={count}/{self.threshold_count} in {self.window_sec}s)")
        
        if count >= self.threshold_count:
            if now - state.last_alert_ts >= self.cooldown_sec:
                await self.send_alert(symbol, reason, count)
                state.last_alert_ts = now

    async def send_alert(self, symbol: str, reason: str, count: int):
        if not self.telegram:
            logger.warning("Telegram client not configured, skipping alert")
            return
            
        msg = (
            f"⚠️ <b>ATR Sanity Alert</b>\n"
            f"Symbol: <code>{symbol}</code>\n"
            f"Reason: {reason}\n"
            f"Count: {count} errors in {self.window_sec}s\n"
            f"Status: <b>DEGRADED</b>"
        )
        logger.info(f"Sending telegram alert for {symbol}")
        # TelegramClient.send_text is failing-open and synchronous (requests), 
        # but here we are in async context. ideally we wrap it to not block loop, 
        # but for low-freq alerts it's acceptable or we run in executor.
        try:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, self.telegram.send_text, msg)
        except Exception as e:
            logger.error(f"Failed to send telegram: {e}")

    async def run(self):
        if not self.enabled:
            logger.info("Service disabled via env")
            while True:
                await asyncio.sleep(3600)
                
        await self.ensure_group()
        logger.info(f"Listening on {self.stream_key}...")
        
        while True:
            try:
                # Read new messages
                streams = await self.redis.xreadgroup(
                    self.consumer_group,
                    self.consumer_name,
                    {self.stream_key: ">"},
                    count=10,
                    block=2000
                )
                
                if not streams:
                    continue
                    
                for stream_name, messages in streams:
                    for msg_id, data in messages:
                        # process
                        try:
                            # data contains log payload, usually valid JSON or dict
                            # If it comes from xadd, it is a dict of strings
                            # We expect standard payload structure
                            # Usually our signals are flattened or nested JSON?
                            # Looking at crypto_orderflow_service, it emits dict.
                            # Redis stream data is dict[bytes, bytes] or dict[str, str] (decode_responses=True)
                            payload = data
                            if "json" in payload:
                                # sometimes payload is packed in "json" field
                                try:
                                    payload = json.loads(payload["json"])
                                except (ValueError, json.JSONDecodeError):
                                    pass
                            
                            await self.process_signal(payload)
                            
                            # Ack
                            await self.redis.xack(self.stream_key, self.consumer_group, msg_id)
                        except Exception as e:
                            logger.error(f"Error processing message {msg_id}: {e}")
                            
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Stream loop error: {e}")
                await asyncio.sleep(1)

if __name__ == "__main__":
    monitor = ATRSanityMonitor()
    try:
        asyncio.run(monitor.run())
    except KeyboardInterrupt:
        pass
