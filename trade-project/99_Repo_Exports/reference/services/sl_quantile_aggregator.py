#!/usr/bin/env python3
"""
SL Quantile Aggregator Service.

Consumes: trades:post_sl (from PostSlAnalyzer)
Aggregates: post_sl_req_buffer_atr
Outputs: Redis keys slq:{symbol}:{side}:{regime} -> JSON snapshot
"""

import os
import sys
import time
import json
import signal
from collections import deque, defaultdict
from typing import Dict, Any
import numpy as np
import redis

# Ensure we can import from project root
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.log import setup_logger

logger = setup_logger("SlQuantileAggregator")

# --- Config ---
REDIS_URL = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
INPUT_STREAM = os.getenv("POST_SL_STREAM", "trades:post_sl")
GROUP_NAME = os.getenv("SLQ_GROUP", "slq-aggregator-group")
CONSUMER_NAME = os.getenv("SLQ_CONSUMER", "slq-worker-1")

# Aggregation settings
MAX_SAMPLES = int(os.getenv("SLQ_MAX_SAMPLES", "1000"))
MIN_SAMPLES_FOR_WRITE = int(os.getenv("SLQ_MIN_SAMPLES_WRITE", "20"))
WRITE_EVERY_SEC = int(os.getenv("SLQ_WRITE_EVERY_SEC", "10"))
TTL_SEC = int(os.getenv("SLQ_TTL_SEC", "604800")) # 7 days

class SlQuantileAggregator:
    def __init__(self):
        self.running = False
        self.redis = redis.from_url(REDIS_URL, decode_responses=True)
        
        # Buckets: key -> deque of float (req_buffer_atr)
        # key = f"{symbol}:{side}:{regime}"
        self.buckets: Dict[str, deque] = defaultdict(lambda: deque(maxlen=MAX_SAMPLES))
        
        # Also track TP1 hits for hit-rate calc
        # key -> deque of bool (tp1_hit)
        self.buckets_hits: Dict[str, deque] = defaultdict(lambda: deque(maxlen=MAX_SAMPLES))
        
        self._ensure_group()
        logger.info(f"SlQuantileAggregator initialized. MaxSamples={MAX_SAMPLES}")

    def _ensure_group(self):
        try:
            self.redis.xgroup_create(INPUT_STREAM, GROUP_NAME, id="0", mkstream=True)
            logger.info(f"Created group {GROUP_NAME} for {INPUT_STREAM}")
        except redis.exceptions.ResponseError as e:
            if "BUSYGROUP" in str(e):
                pass
            else:
                logger.error(f"Failed to create group: {e}")

    def start(self):
        self.running = True
        logger.info("Starting aggregator loop...")
        
        # 0. Recover pending messages (at-least-once delivery)
        self._recover_pending()
        
        last_flush = time.time()
        
        while self.running:
            try:
                # 1. Consume
                self._poll_stream()
                
                # 2. Periodic writes
                now = time.time()
                if now - last_flush > WRITE_EVERY_SEC:
                    self._flush_aggregates()
                    last_flush = now
                    
                time.sleep(0.01)
                
            except Exception as e:
                logger.error(f"Error in main loop: {e}", exc_info=True)
                time.sleep(1)

    def stop(self):
        self.running = False
        logger.info("Stopping SlQuantileAggregator...")

    def _recover_pending(self):
        """
        Recover pending messages that were delivered but not ACKed.
        Uses XAUTOCLAIM if available, or manual XPENDING+XCLAIM.
        """
        logger.info(f"Recovering pending messages for {GROUP_NAME}...")
        start_id = "0-0"
        total_recovered = 0
        
        while True:
            try:
                # Redis 6.2+ XAUTOCLAIM: key group consumer min_idle_time start_id count
                # Returns: [next_start_id, [messages]]
                # min_idle_time=1000ms (only claim if idle for >1s)
                try:
                    res = self.redis.execute_command(
                        "XAUTOCLAIM", INPUT_STREAM, GROUP_NAME, CONSUMER_NAME, "1000", start_id, "COUNT", "100"
                    )
                except redis.exceptions.ResponseError as e:
                    if str(e).startswith("NOGROUP"):
                        logger.warning(f"Consumer group {GROUP_NAME} missing during recovery, recreating...")
                        self._ensure_group()
                        continue
                    raise e
                    
                if not res:
                    break
                    
                # XAUTOCLAIM can return 2 or 3 elements depending on version
                next_start_id = res[0]
                msgs = res[1]
                
                if not msgs:
                    # No more pending messages to claim
                    if next_start_id == "0-0":
                        break
                    start_id = next_start_id
                    continue
                
                for msg_id, fields in msgs:
                    if self._process_msg(fields):
                        self.redis.xack(INPUT_STREAM, GROUP_NAME, msg_id)
                    else:
                        # Could not process, but we claimed it. 
                        # If invalid data, we should probably ACK to drop it from pending
                        # to avoid infinite loops. Logging it as error.
                        logger.warning(f"Dropping invalid pending msg {msg_id}: {fields}")
                        self.redis.xack(INPUT_STREAM, GROUP_NAME, msg_id)
                        
                total_recovered += len(msgs)
                start_id = next_start_id
                
            except Exception as e:
                logger.error(f"Error in pending recovery: {e}")
                break
                
        if total_recovered > 0:
            logger.info(f"Recovered {total_recovered} pending messages")

    def _poll_stream(self):
        try:
            entries = self.redis.xreadgroup(
                GROUP_NAME, CONSUMER_NAME, {INPUT_STREAM: ">"}, count=100, block=1000
            )
        except redis.exceptions.ResponseError as e:
            if str(e).startswith("NOGROUP"):
                logger.warning(f"Consumer group {GROUP_NAME} missing, recreating...")
                self._ensure_group()
                return
            logger.error(f"XREADGROUP failed: {e}")
            time.sleep(1)
            return
        except Exception as e:
            logger.error(f"XREADGROUP failed: {e}")
            time.sleep(1)
            return

        if not entries:
            return

        for stream, msgs in entries:
            for msg_id, fields in msgs:
                try:
                    # Process & ACK only if processed (or explicitly dropped inside)
                    if self._process_msg(fields):
                        self.redis.xack(INPUT_STREAM, GROUP_NAME, msg_id)
                    else:
                        # Logic: if malformed field -> Log & ACK to clear.
                        # Do not keep poison pills in pending.
                        # _process_msg logs the reason.
                        self.redis.xack(INPUT_STREAM, GROUP_NAME, msg_id)
                except Exception as e:
                    logger.error(f"Failed processing {msg_id}: {e}")
                    # Don't ACK on crash, might be transient capability issue

    def _process_msg(self, fields: Dict[str, Any]) -> bool:
        """
        Returns True if processed successfully (valid data).
        Returns False if data was invalid/missing critical fields.
        """
        # 1. Strict extraction & normalization
        symbol = str(fields.get("symbol", "")).strip().upper()
        if not symbol:
            return False

        side_raw = str(fields.get("side", "")).strip().upper()
        if side_raw not in ("LONG", "SHORT"):
            # If strictly required -> fail
            # logger.warning(f"Skipping msg without valid side: {side_raw}")
            return False
            
        regime = str(fields.get("regime", "na")).strip().lower()
        if not regime: 
            regime = "na"

        # 2. Extract Metrics
        try:
            req_buf = float(fields.get("post_sl_req_buffer_atr", 0.0))
            tp1_hit = int(fields.get("post_sl_tp1_hit", 0))
        except (ValueError, TypeError):
            # Malformed numbers
            return False
            
        # 3. Data Quality Check (Finite)
        if not np.isfinite(req_buf) or req_buf < 0:
            # Junk value (e.g. NaN or negative distance? Negative distance shouldn't happen by logic)
            return False

        # 4. Aggregate
        key = f"{symbol}:{side_raw}:{regime}"
        self.buckets[key].append(req_buf)
        self.buckets_hits[key].append(tp1_hit)
        
        return True

    def _flush_aggregates(self):
        # Write stats to Redis keys slq:{key}
        # Iterate snapshot of keys to avoid modification during iteration issues if we were threading (we are single threaded here)
        
        for key, buf in self.buckets.items():
            if len(buf) < MIN_SAMPLES_FOR_WRITE:
                continue
            
            # buffer of hits
            hits = self.buckets_hits[key]
            
            # Calc stats
            # q50, q75, q90, q95
            arr = np.array(buf)
            q_vals = np.percentile(arr, [50, 75, 90, 95])
            
            tp1_rate = sum(hits) / len(hits) if hits else 0.0
            
            payload = {
                "n": len(buf),
                "sl_buffer_atr_q50": float(q_vals[0]),
                "sl_buffer_atr_q75": float(q_vals[1]),
                "sl_buffer_atr_q90": float(q_vals[2]),
                "sl_buffer_atr_q95": float(q_vals[3]),
                "post_sl_tp1_hit_rate": tp1_rate,
                "ts_ms": int(time.time() * 1000)
            }
            
            redis_key = f"slq:{key}"
            self.redis.set(redis_key, json.dumps(payload), ex=TTL_SEC)
            
        # logger.info(f"Flushed stats for {len(self.buckets)} buckets")

if __name__ == "__main__":
    service = SlQuantileAggregator()
    
    def signal_handler(sig, frame):
        service.stop()
        
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    service.start()
