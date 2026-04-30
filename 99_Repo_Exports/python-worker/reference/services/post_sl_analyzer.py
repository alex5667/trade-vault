from utils.time_utils import get_ny_time_millis
#!/usr/bin/env python3
"""
Post-SL Analyzer Service.

Analyzes trades that hit Stop Loss to determine if they would have reached TP1
within a reasonable time horizon if the SL had been wider.
Tracks "Post-SL" price action using 1m candles.

Inputs:
  - Redis Stream: trades:closed (filter for close_reason=SL)
  - Redis Stream: candles:data (1m candles for symbols)

Outputs:
  - Redis Stream: trades:post_sl
    - post_sl_tp1_hit: bool
    - post_sl_tp1_time_ms: int
    - post_sl_end_reason: str
    ...
"""

import os
import math
import hashlib
import sys
import time
import json
import signal
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any, Tuple, Callable, Union
import redis

# Ensure we can import from project root
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.log import setup_logger
from services.trade_closed_hydrator import hydrate_trade_closed

logger = setup_logger("PostSlAnalyzer")

# --- Configuration ---
REDIS_URL = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")

# Streams
TRADES_STREAM = os.getenv("TRADES_CLOSED_STREAM", "trades:closed")
CANDLES_STREAM = os.getenv("CANDLES_STREAM", "candles:data")
OUTPUT_STREAM = os.getenv("POST_SL_STREAM", "trades:post_sl")

# Consumer Groups
TRADES_GROUP = os.getenv("POST_SL_TRADES_GROUP", "post-sl-trades-group")
TRADES_CONSUMER = os.getenv("POST_SL_TRADES_CONSUMER", "post-sl-worker-1")
CANDLES_GROUP = os.getenv("POST_SL_CANDLES_GROUP", "post-sl-candles-group")
CANDLES_CONSUMER = os.getenv("POST_SL_CANDLES_CONSUMER", "post-sl-worker-1")

# Analysis Params
ENABLE_TRACKING = os.getenv("POST_SL_TRACK_ENABLE", "1") == "1"
MAX_BARS = int(os.getenv("POST_SL_MAX_BARS", "120"))  # Start with 2h horizon
ATR_CAP = float(os.getenv("POST_SL_ATR_CAP", "2.0"))  # 2.0 * ATR strict cap
TP1_EPS_BPS = float(os.getenv("POST_SL_TP1_EPS_BPS", "2.0")) # 2 bps tolerance


# --- Data Structures ---

def _norm_side(x) -> str:
    try:
        # int side
        if isinstance(x, (int, float)):
            return "LONG" if int(x) > 0 else "SHORT"
        s = str(x).strip().upper()
        if s in {"LONG", "BUY", "B"}:
            return "LONG"
        if s in {"SHORT", "SELL", "S"}:
            return "SHORT"
        if s in {"1"}:
            return "LONG"
        if s in {"-1"}:
            return "SHORT"
    except Exception:
        pass
    return "NA"


def _norm_regime(x) -> str:
    try:
        if x is None:
            return "na"
        s = str(x).strip().lower()
        if s == "none":
            return "na"
        return s or "na"
    except Exception:
        return "na"

@dataclass
class TrackState:
    trade_id: str
    symbol: str
    direction: str  # "LONG" or "SHORT"
    entry_price: float
    sl_price: float
    tp1_price: float
    start_ts_ms: int
    atr_entry: float
    regime: str = "na"
    
    # Tracking State
    max_favorable: float = field(init=False)
    min_favorable: float = field(init=False)
    bars_seen: int = 0
    max_mfe_atr: float = 0.0
    
    def __post_init__(self):
        self.max_favorable = self.entry_price
        self.min_favorable = self.entry_price

    @property
    def risk_dist(self) -> float:
        return abs(self.entry_price - self.sl_price)


class PostSlAnalyzer:
    def __init__(self):
        self.running = False
        self.redis = redis.from_url(REDIS_URL, decode_responses=True)
        # Active tracks: symbol -> List[TrackState]
        self.tracks: Dict[str, List[TrackState]] = defaultdict(list)
        
        # Ensure groups exist
        self._ensure_group(TRADES_STREAM, TRADES_GROUP)
        self._ensure_group(CANDLES_STREAM, CANDLES_GROUP)
        
        # init finish_meta controls (sampling + bounds) once (avoid getenv in hot path)
        self._init_finish_meta_controls()

        logger.info(f"PostSlAnalyzer initialized. MaxBars={MAX_BARS}, AtrCap={ATR_CAP}")

    # -----------------------------
    # finish_meta controls (ENV)
    # -----------------------------
    # Payload hard limits (applied only on finish path)
    DEFAULT_FINISH_META_MAX_CHARS = 2048
    DEFAULT_FINISH_META_MAX_DEPTH = 4
    DEFAULT_FINISH_META_MAX_ITEMS = 64
    DEFAULT_FINISH_META_MAX_STR = 256

    # Sampling defaults
    # - TP1 is usually the most important => keep meta always
    DEFAULT_SAMPLE_TP1 = 1.0
    DEFAULT_SAMPLE_ATR_CAP = 0.10
    DEFAULT_SAMPLE_TIME_CAP = 0.01
    DEFAULT_SAMPLE_DEFAULT = 0.05

    # finish_meta type: dict or lazy builder (called only if sampling keeps meta)
    FinishMetaT = Union[Dict[str, Any], Callable[[], Dict[str, Any]]]

    def _init_finish_meta_controls(self) -> None:
        """
        Read ENV once. Call in __init__ and in unit-tests when using __new__.
        """
        self._finish_meta_max_chars = int(os.getenv("POSTSL_FINISH_META_MAX_CHARS", str(self.DEFAULT_FINISH_META_MAX_CHARS)))
        self._finish_meta_max_depth = int(os.getenv("POSTSL_FINISH_META_MAX_DEPTH", str(self.DEFAULT_FINISH_META_MAX_DEPTH)))
        self._finish_meta_max_items = int(os.getenv("POSTSL_FINISH_META_MAX_ITEMS", str(self.DEFAULT_FINISH_META_MAX_ITEMS)))
        self._finish_meta_max_str = int(os.getenv("POSTSL_FINISH_META_MAX_STR", str(self.DEFAULT_FINISH_META_MAX_STR)))

        def _f01(key: str, default: float) -> float:
            raw = os.getenv(key, "")
            try:
                v = float(raw) if raw else float(default)
            except Exception:
                v = float(default)
            if v < 0.0:
                v = 0.0
            if v > 1.0:
                v = 1.0
            return v

        self._sample_p_tp1 = _f01("POSTSL_FINISH_META_SAMPLE_TP1", self.DEFAULT_SAMPLE_TP1)
        self._sample_p_atr_cap = _f01("POSTSL_FINISH_META_SAMPLE_ATR_CAP", self.DEFAULT_SAMPLE_ATR_CAP)
        self._sample_p_time_cap = _f01("POSTSL_FINISH_META_SAMPLE_TIME_CAP", self.DEFAULT_SAMPLE_TIME_CAP)
        self._sample_p_default = _f01("POSTSL_FINISH_META_SAMPLE_DEFAULT", self.DEFAULT_SAMPLE_DEFAULT)

        raw_tag = os.getenv("POSTSL_FINISH_META_SAMPLE_TAGS", "")
        self._finish_meta_sample_tags = raw_tag.strip().lower() in ("1", "true", "yes", "y", "on")

        # clamp bounds defensively
        self._finish_meta_max_chars = max(256, min(self._finish_meta_max_chars, 65536))
        self._finish_meta_max_depth = max(1, min(self._finish_meta_max_depth, 16))
        self._finish_meta_max_items = max(8, min(self._finish_meta_max_items, 1024))
        self._finish_meta_max_str = max(32, min(self._finish_meta_max_str, 8192))

    @staticmethod
    def _stable_u01(seed: str) -> float:
        """
        Deterministic u in [0,1) from a stable hash (replay-friendly).
        """
        h = hashlib.sha1(seed.encode("utf-8")).digest()
        x = int.from_bytes(h[:8], "big", signed=False)  # 64-bit
        return x / float(1 << 64)

    def _finish_meta_sample_p(self, reason: str) -> float:
        """
        Per-reason sampling probability.
        Uses cached values from _init_finish_meta_controls.
        """
        r = (reason or "").strip().lower()
        if r == "tp1_hit":
            return self._sample_p_tp1
        if r == "atr_cap":
            return self._sample_p_atr_cap
        if r == "time_cap":
            return self._sample_p_time_cap
        return self._sample_p_default

    def _want_finish_meta(self, track: "TrackState", reason: str) -> Tuple[bool, float, float]:
        """
        Returns (want_meta, p, u). Deterministic for same (trade_id,start_ts,reason).
        """
        p = self._finish_meta_sample_p(reason)
        if p <= 0.0:
            return False, p, 0.0
        if p >= 1.0:
            return True, p, 0.0
        seed = f"{track.trade_id}|{track.start_ts_ms}|{reason}"
        u = self._stable_u01(seed)
        return (u < p), p, u

    def _want_finish_meta_bool(self, track: "TrackState", reason: str) -> bool:
        """
        Hot-loop helper: returns only the boolean decision.
        Keeps deterministic sampling logic centralized in _want_finish_meta().
        """
        want, _, _ = self._want_finish_meta(track, reason)
        return want

    @staticmethod
    def _json_sanitize(obj: Any, *, depth: int, max_depth: int, max_items: int, max_str: int) -> Any:
        if depth > max_depth:
            return "<max_depth>"
        if obj is None or isinstance(obj, (bool, int, float, str)):
            if isinstance(obj, float) and not math.isfinite(obj):
                return None
            if isinstance(obj, str) and len(obj) > max_str:
                return obj[:max_str]
            return obj
        try:
            if hasattr(obj, "item"):
                return PostSlAnalyzer._json_sanitize(obj.item(), depth=depth+1, max_depth=max_depth, max_items=max_items, max_str=max_str)
        except Exception:
            pass
        if isinstance(obj, dict):
            out: Dict[str, Any] = {}
            n = 0
            for k, v in obj.items():
                if n >= max_items:
                    out["<truncated>"] = True
                    break
                ks = str(k)
                if len(ks) > max_str:
                    ks = ks[:max_str]
                out[ks] = PostSlAnalyzer._json_sanitize(v, depth=depth+1, max_depth=max_depth, max_items=max_items, max_str=max_str)
                n += 1
            return out
        if isinstance(obj, (list, tuple)):
            out_list = []
            for v in obj[:max_items]:
                out_list.append(PostSlAnalyzer._json_sanitize(v, depth=depth+1, max_depth=max_depth, max_items=max_items, max_str=max_str))
            if len(obj) > max_items:
                out_list.append("<truncated>")
            return out_list
        s = str(obj)
        if len(s) > max_str:
            s = s[:max_str]
        return s

    def _safe_finish_meta_json(self, finish_meta: Dict[str, Any]) -> Tuple[Optional[str], int, int]:
        try:
            sanitized = self._json_sanitize(
                finish_meta
                depth=0
                max_depth=self._finish_meta_max_depth
                max_items=self._finish_meta_max_items
                max_str=self._finish_meta_max_str
            )
            s = json.dumps(sanitized, ensure_ascii=False, separators=(",", ":"))
        except Exception:
            return None, 0, 0
        orig_len = len(s)
        if orig_len > self._finish_meta_max_chars:
            return s[: self._finish_meta_max_chars], 1, orig_len
        return s, 0, orig_len

    def _ensure_group(self, stream: str, group: str):
        max_retries = 15
        for i in range(max_retries):
            try:
                self.redis.xgroup_create(stream, group, id="0", mkstream=True)
                logger.info(f"Created consumer group {group} for {stream}")
                return
            except redis.exceptions.ResponseError as e:
                err_str = str(e)
                if "BUSYGROUP" in err_str:
                    return
                if "LOADING" in err_str:
                    wait_time = 2.0
                    logger.warning(f"Redis is loading data, retrying group {group} creation in {wait_time}s... ({i+1}/{max_retries})")
                    time.sleep(wait_time)
                    continue
                
                logger.error(f"Failed to create group {group}: {e}")
                return

    # -----------------------------
    # Helpers: finish-condition telemetry
    # -----------------------------
    @staticmethod
    def _tp1_hit_bool(direction: str, bar_h: float, bar_l: float, tp1_price: float, eps_bps: float) -> Tuple[bool, float, float]:
        """
        Returns:
          hit: bool
          eps_val: absolute epsilon in price units
          trigger_px: bar extreme used for comparison (HIGH for LONG, LOW for SHORT)
        """
        eps_val = tp1_price * (eps_bps * 1e-4)
        if direction == "LONG":
            trigger_px = bar_h
            hit = trigger_px >= (tp1_price - eps_val)
        else:
            trigger_px = bar_l
            hit = trigger_px <= (tp1_price + eps_val)
        return hit, eps_val, trigger_px

    @staticmethod
    def _tp1_hit_details(direction: str, tp1_price: float, eps_bps: float, eps_val: float, trigger_px: float) -> Dict[str, Any]:
        """
        Build compact meta for TP1 hit. Intended for telemetry/debug, JSON-safe.
        dist_bps_signed:
          LONG: (tp1 - trigger)/tp1*1e4  (<=0 means reached/overshoot)
          SHORT: (trigger - tp1)/tp1*1e4 (<=0 means reached/overshoot)
        """
        if tp1_price <= 0:
            dist_bps_signed = None
        else:
            if direction == "LONG":
                dist_bps_signed = (tp1_price - trigger_px) / tp1_price * 10_000.0
            else:
                dist_bps_signed = (trigger_px - tp1_price) / tp1_price * 10_000.0

        # threshold price used for comparison (explicit, for deterministic audit)
        thr_px = (tp1_price - eps_val) if direction == "LONG" else (tp1_price + eps_val)

        return {
            "tp1_price": float(tp1_price)
            "tp1_eps_bps": float(eps_bps)
            "tp1_eps_val": float(eps_val)
            "tp1_trigger_px": float(trigger_px)
            "tp1_threshold_px": float(thr_px)
            "tp1_dist_bps_signed": float(dist_bps_signed) if dist_bps_signed is not None else None
            "direction": str(direction)
        }

    @staticmethod
    def _atr_cap_details(direction: str, sl_price: float, atr_entry: float, atr_cap_mult: float, bar_h: float, bar_l: float) -> Dict[str, Any]:
        """
        Telemetry for ATR cap condition.
        For LONG: cap_level = SL - ATR_CAP*ATR_entry, breach if bar_low <= cap_level
        For SHORT: cap_level = SL + ATR_CAP*ATR_entry, breach if bar_high >= cap_level
        """
        dist = atr_cap_mult * atr_entry
        if direction == "LONG":
            cap_level = sl_price - dist
            extreme = bar_l
            breach = cap_level - extreme  # >=0 means breached below cap_level
        else:
            cap_level = sl_price + dist
            extreme = bar_h
            breach = extreme - cap_level  # >=0 means breached above cap_level

        return {
            "sl_price": float(sl_price)
            "atr_entry": float(atr_entry)
            "atr_cap_mult": float(atr_cap_mult)
            "atr_cap_dist": float(dist)
            "atr_cap_level_px": float(cap_level)
            "bar_extreme_px": float(extreme)
            "atr_cap_breach_px": float(breach)
            "direction": str(direction)
        }

    @staticmethod
    def _time_cap_details(bars_seen: int, max_bars: int) -> Dict[str, Any]:
        return {"bars_seen": int(bars_seen), "max_bars": int(max_bars)}

    def start(self):
        self.running = True
        logger.info("Starting analysis loop...")
        
        last_log = time.time()
        
        while self.running:
            try:
                # 1. Read new trades
                self._poll_trades()
                
                # 2. Read new candles (drivers of analysis)
                self._poll_candles()
                
                # 3. Cleanup / Stats
                now = time.time()
                if now - last_log > 60:
                    total_active = sum(len(v) for v in self.tracks.values())
                    if total_active > 0:
                        logger.info(f"Stats: Tracking {total_active} active post-SL trades across {len(self.tracks)} symbols")
                    last_log = now
                    
                time.sleep(0.01) # Avoid tight loop cpu spin
                
            except Exception as e:
                logger.error(f"Error in main loop: {e}", exc_info=True)
                time.sleep(1)

    def stop(self):
        self.running = False
        logger.info("Stopping PostSlAnalyzer...")

    def _poll_trades(self):
        """Read closed trades from Redis Stream."""
        try:
            entries = self.redis.xreadgroup(
                TRADES_GROUP, TRADES_CONSUMER, {TRADES_STREAM: ">"}, count=50, block=10
            )
        except redis.exceptions.ResponseError as e:
            if str(e).startswith("NOGROUP"):
                logger.warning(f"Consumer group {TRADES_GROUP} missing, recreating...")
                self._ensure_group(TRADES_STREAM, TRADES_GROUP)
                return
            raise e
        
        if not entries:
            return

        for stream, msgs in entries:
            for msg_id, fields in msgs:
                try:
                    self._handle_new_trade(msg_id, fields)
                except Exception as e:
                    logger.error(f"Failed handling trade {msg_id}: {e}")
                finally:
                    self.redis.xack(TRADES_STREAM, TRADES_GROUP, msg_id)

    def _handle_new_trade(self, msg_id: str, fields: Dict[str, Any]):
        # Hydrate for safety (ensure number formats etc)
        # Note: hydrate_trade_closed might be expensive if we do it for ALL trades, 
        # but filtering close_reason is cheap.
        
        close_reason = fields.get("close_reason", "")
        if close_reason != "SL" and "SL" not in close_reason:
            # Not an SL trade, ignore
            return

        if not ENABLE_TRACKING:
            return

        # Hydrate to get full prices float conversion
        t = hydrate_trade_closed(self.redis, fields, require_closed=False)
        
        symbol = t.get("symbol")
        if not symbol: 
            return

        # Create TrackState
        direction = str(t.get("direction", "")).upper()
        if direction not in ("LONG", "SHORT"):
            return

        try:
            entry = float(t.get("entry_price", 0))
            sl = float(t.get("sl_price", 0))
            tp1 = float(t.get("tp1_price", 0))
            atr = float(t.get("atr_entry", 0))
            exit_ts = int(t.get("exit_ts_ms", 0) or t.get("closed_time", 0))
        except (ValueError, TypeError):
            return

        if entry <= 0 or sl <= 0 or tp1 <= 0:
            return

        regime = str(t.get("regime", "na"))

        track = TrackState(
            trade_id=str(t.get("trade_id") or msg_id)
            symbol=symbol
            direction=direction
            entry_price=entry
            sl_price=sl
            tp1_price=tp1
            start_ts_ms=exit_ts
            atr_entry=atr
            regime=regime
        )
        
        self.tracks[symbol].append(track)
        # logger.debug(f"Started tracking post-SL for {symbol} trade {track.trade_id}")

    def _poll_candles(self):
        """Read 1m candles and update active tracks."""
        try:
            entries = self.redis.xreadgroup(
                CANDLES_GROUP, CANDLES_CONSUMER, {CANDLES_STREAM: ">"}, count=100, block=10
            )
        except redis.exceptions.ResponseError as e:
            if str(e).startswith("NOGROUP"):
                logger.warning(f"Consumer group {CANDLES_GROUP} missing, recreating...")
                self._ensure_group(CANDLES_STREAM, CANDLES_GROUP)
                return
            raise e
        
        if not entries:
            return

        for stream, msgs in entries:
            for msg_id, fields in msgs:
                try:
                    self._handle_candle(fields)
                except Exception as e:
                    logger.error(f"Failed handling candle {msg_id}: {e}")
                finally:
                    self.redis.xack(CANDLES_STREAM, CANDLES_GROUP, msg_id)

    def _handle_candle(self, fields: Dict[str, Any]):
        # Parse Unified Candle Format
        # We need symbol, tf=1m, and valid OHLC
        
        # 1. Symbol/TF check
        sym = fields.get("symbol")
        tf = fields.get("tf") or fields.get("timeframe")
        
        if not sym or str(tf).lower() not in ("1m", "m1"):
            # We only track on 1m bars for standard granularity
            return
            
        # 2. Payload check
        payload = fields.get("payload") or fields.get("data")
        if not payload:
            return
            
        try:
            d = json.loads(payload)
        except json.JSONDecodeError:
            return

        # Extract OHLC
        try:
            h = float(d.get("high") or d.get("h", 0))
            l = float(d.get("low") or d.get("l", 0))
            # c = float(d.get("close") or d.get("c", 0)) 
            # We mostly need High/Low for validation
        except (ValueError, TypeError):
            return

        ts_ms = int(d.get("ts", 0) or 0)
        
        # Update tracks for this symbol
        if sym in self.tracks:
            self._update_symbol_tracks(sym, h, l, ts_ms)

    def _update_symbol_tracks(self, symbol: str, bar_h: float, bar_l: float, bar_ts_ms: int):
        active = self.tracks[symbol]
        finished_indices = []
        
        for i, track in enumerate(active):
            # Skip if candle is OLDER than trade exit (should not happen in real-time but possible in replays)
            if bar_ts_ms < track.start_ts_ms:
                continue

            track.bars_seen += 1
            
            # --- Update local extrema ---
            track.max_favorable = max(track.max_favorable, bar_h)
            track.min_favorable = min(track.min_favorable, bar_l)
            
            # --- Check Conditions ---
            
            # 1. TP1 Hit?
            is_tp_hit, eps_val, trigger_px = self._tp1_hit_bool(
                track.direction, bar_h, bar_l, track.tp1_price, TP1_EPS_BPS
            )
            
            if is_tp_hit:
                meta = self._tp1_hit_details(track.direction, track.tp1_price, TP1_EPS_BPS, eps_val, trigger_px)
                self._finish_track(track, "tp1_hit", bar_ts_ms, finish_meta=meta)
                finished_indices.append(i)
                continue

            # 2. Time Cap?
            if track.bars_seen >= MAX_BARS:
                self._finish_track(
                    track
                    "time_cap"
                    bar_ts_ms
                    finish_meta=lambda: self._time_cap_details(track.bars_seen, MAX_BARS)
                )
                finished_indices.append(i)
                continue

            # 3. ATR Cap (Market moved too far against)
            # Cap logic:
            # LONG: Low <= SL - (ATR * Cap)
            # SHORT: High >= SL + (ATR * Cap)
            if track.atr_entry > 0:
                dist = ATR_CAP * track.atr_entry
                is_atr_cap = False
                if track.direction == "LONG":
                    if bar_l <= (track.sl_price - dist):
                        is_atr_cap = True
                else:
                    if bar_h >= (track.sl_price + dist):
                        is_atr_cap = True
                
                if is_atr_cap:
                    self._finish_track(
                        track
                        "atr_cap"
                        bar_ts_ms
                        finish_meta=lambda: self._atr_cap_details(track.direction, track.sl_price, track.atr_entry, ATR_CAP, bar_h, bar_l)
                    )
                    finished_indices.append(i)
                    continue

        # Remove finished
        if finished_indices:
            # remove from back to front to preserve indices
            for i in sorted(finished_indices, reverse=True):
                active.pop(i)

    def _finish_track(self, track: TrackState, reason: str, end_ts_ms: int, finish_meta: Optional[FinishMetaT] = None):
        # Calculate metrics
        tp1_hit = (reason == "tp1_hit")
        time_to_tp1 = (end_ts_ms - track.start_ts_ms) if tp1_hit else None
        
        # MFE calculation (favorable excursion from Entry)
        # LONG: max_high - entry
        # SHORT: entry - min_low
        mfe_money = 0.0
        if track.direction == "LONG":
            mfe_money = track.max_favorable - track.entry_price
        else:
            mfe_money = track.entry_price - track.min_favorable
        
        mfe_r = 0.0
        if track.risk_dist > 0:
            mfe_r = mfe_money / track.risk_dist
            
        mfe_atr = 0.0
        if track.atr_entry > 0:
            mfe_atr = mfe_money / track.atr_entry

        # Calculate Required Buffer (MAE beyond SL)
        # LONG: How much below SL did it go?
        # SHORT: How much above SL did it go?
        req_buffer_money = 0.0
        if track.direction == "LONG":
            # If min_favorable < sl_price, that diff is what we needed
            if track.min_favorable < track.sl_price:
                req_buffer_money = track.sl_price - track.min_favorable
        else:
            # If max_favorable > sl_price
            if track.max_favorable > track.sl_price:
                req_buffer_money = track.max_favorable - track.sl_price
        
        req_buffer_atr = 0.0
        if track.atr_entry > 0:
            req_buffer_atr = req_buffer_money / track.atr_entry

        now_ms = get_ny_time_millis()
        result = {
            "trade_id": str(track.trade_id)
            "symbol": str(track.symbol).upper()
            "side": _norm_side(track.direction)
            "regime": _norm_regime(track.regime)
            "post_sl_tp1_hit": int(tp1_hit)
            "post_sl_tp1_time_ms": int(time_to_tp1) if time_to_tp1 is not None else -1
            "post_sl_end_reason": str(reason or "")
            "post_sl_bars_observed": int(track.bars_seen)
            "post_sl_mfe_r": float(mfe_r)
            "post_sl_mfe_atr": float(mfe_atr)
            "post_sl_req_buffer_atr": float(req_buffer_atr)
            "event_ts_ms": int(track.start_ts_ms or 0)
            "end_ts_ms": int(end_ts_ms or 0)
            "ingest_ts_ms": now_ms
            "ts": now_ms  # Legacy validation
        }
        
        # finish_meta sampling + lazy builder
        if finish_meta:
            want, p, _ = self._want_finish_meta(track, reason)
            if want:
                meta_dict = finish_meta() if callable(finish_meta) else finish_meta
                s, trunc, orig_len = self._safe_finish_meta_json(meta_dict)
                if s is not None:
                    result["finish_meta"] = s
                    result["finish_meta_trunc"] = int(trunc)
                    result["finish_meta_len"] = int(orig_len)
            else:
                if self._finish_meta_sample_tags:
                    result["finish_meta_sampled_out"] = 1
                    result["finish_meta_sample_p"] = float(p)
        
        # Publish to output stream
        try:
            self.redis.xadd(OUTPUT_STREAM, result, maxlen=10000)
            logger.info(f"Analyzed {track.symbol} {track.trade_id}: hit={tp1_hit}, reason={reason}, bars={track.bars_seen}")
        except Exception as e:
            logger.error(f"Failed to publish result for {track.trade_id}: {e}")

if __name__ == "__main__":
    service = PostSlAnalyzer()
    
    def signal_handler(sig, frame):
        service.stop()
        
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    service.start()
