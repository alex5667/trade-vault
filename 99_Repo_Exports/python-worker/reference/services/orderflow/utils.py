from typing import Any, Dict, List, Optional, Callable
import json
import zlib
from datetime import datetime, timezone
from services.orderflow.configuration import _safe_float, _safe_int

def hour_of_week_utc(ts_ms: int) -> int:
    dt = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)
    return dt.weekday() * 24 + dt.hour  # 0..167

def session_utc(ts_ms: int) -> str:
    """
    UTC sessions (simple and stable; no overlaps)
    ASIA: 00:00 - 08:00
    EU:   08:00 - 14:00
    NY:   14:00 - 21:00
    OFF:  21:00 - 00:00
    """
    h = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).hour
    if 0 <= h < 8:
        return "ASIA"
    if 8 <= h < 14:
        return "EU"
    if 14 <= h < 21:
        return "NY"
    return "OFF"

def fmt_utc_dow_hour(ts_ms: int) -> str:
    dt = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)
    return dt.strftime("%a %H:00 UTC")


def _fields_to_dict(fields: Any) -> Dict[str, str]:
    """Fast stream fields normalizer.

    Optimized for decode_responses=True Redis clients (common case):
      - If fields is already a dict with str keys → return as-is (zero copy).
      - Falls back to slow bytes-decode path for raw/legacy clients.
      - Handles flat list/tuple [k1, v1, k2, v2, ...] format from RESP.
    """
    if not fields:
        return {}
    if isinstance(fields, dict):
        # Fast path: str keys (decode_responses=True) — no copy needed
        try:
            first_key = next(iter(fields))
            if isinstance(first_key, str):
                return fields  # type: ignore[return-value]
        except StopIteration:
            return {}
        # Slow path: bytes keys (raw client without decode_responses)
        return {
            (k.decode("utf-8") if isinstance(k, bytes) else str(k)):
            (v.decode("utf-8") if isinstance(v, bytes) else str(v))
            for k, v in fields.items()
        }
    if isinstance(fields, (list, tuple)):
        # Flat alternating [k, v, k, v, ...] from raw RESP
        it = iter(fields)
        return {
            (k.decode("utf-8") if isinstance(k, bytes) else str(k)):
            (v.decode("utf-8") if isinstance(v, bytes) else str(v))
            for k, v in zip(it, it)
        }
    return {}


def _normalize_epoch_ms(v) -> int:
    """Helper for robust epoch ms extraction with heuristics for sec/ms/us."""
    try:
        if v is None or isinstance(v, bool):
            return 0
        if isinstance(v, str):
            v = v.strip()
            if not v: return 0
            v = float(v) if "." in v else int(v)
        if isinstance(v, float):
            v = int(v)
        if not isinstance(v, (int, float)):
            return 0
        x = int(v)
        if x <= 0: return 0
        # sec -> ms heuristic
        if x < 100_000_000_000:     # < 1e11 => likely seconds
            return x * 1000
        # us -> ms heuristic
        if x >= 100_000_000_000_000:  # >= 1e14 => microseconds
            return x // 1000
        return x  # milliseconds
    except Exception:
        return 0

def redis_stream_id_ts_ms(msg_id: Any) -> int:
    """Parse Redis Stream entry id (e.g. '1700000000000-0') -> epoch ms."""
    try:
        if msg_id is None:
            return 0
        if isinstance(msg_id, bytes):
            msg_id = msg_id.decode("utf-8", errors="ignore")
        s = str(msg_id).strip()
        if not s:
            return 0
        # Stream ID is '<ms>-<seq>'
        ms_part = s.split("-", 1)[0]
        return int(ms_part)
    except Exception:
        return 0

def _score_candidate(ind: Dict[str, Any]) -> float:
    """
    Score used to pick best signal inside cooldown burst.
    Cheap + monotonic:
      - of_confirm_score (0..1) dominates
      - then |delta_z|
      - then obi/ice confirmations
    """
    try: of_sc = float(ind.get("of_confirm_score", 0.0) or 0.0)
    except Exception: of_sc = 0.0
    try: dz = abs(float(ind.get("delta_z", 0.0) or 0.0))
    except Exception: dz = 0.0
    try: obi = float(ind.get("obi_stable_secs", 0.0) or 0.0)
    except Exception: obi = 0.0
    try: ice = float(ind.get("iceberg_strict", 0.0) or 0.0)
    except Exception: ice = 0.0
    return 2.0 * of_sc + 0.30 * dz + 0.15 * min(3.0, obi) + 0.50 * ice

def _calc_pressure_sps(ts_list: List[int], now_ms: int, window_ms: int = 60_000) -> float:
    """
    Candidates/sec in the last window_ms.
    Deterministic (uses tick_ts).
    """
    if not ts_list:
        return 0.0
    cutoff = now_ms - window_ms
    n = 0
    for t in reversed(ts_list):
        if t < cutoff:
            break
        n += 1
    return float(n) / float(max(1, window_ms // 1000))

def _cooldown_ms_for(runtime, *, scenario: str, now_ms: int) -> int:
    """
    Scenario-aware cooldown:
      - reversal and continuation can have different budgets
      - in thin/news/illiquid or wide spread => stricter (longer)
      - under pressure_hi => slightly longer (avoid chasing noise)
    Deterministic: uses runtime.last_regime / runtime.last_spread_bps (updated on microbar close).
    """
    cfg = getattr(runtime, "config", {}) or {}
    scn = (scenario or "").strip().lower()
    # base cooldowns
    cd_rev = int(cfg.get("cooldown_reversal_sec", cfg.get("signal_cooldown_sec", 30)) or 30) * 1000
    cd_con = int(cfg.get("cooldown_continuation_sec", cfg.get("signal_cooldown_sec", 30)) or 30) * 1000
    cd = cd_rev if scn == "reversal" else cd_con

    # regime multiplier
    rg = str(getattr(runtime, "last_regime", "na") or "na").lower()
    if rg in ("thin", "news", "illiquid"):
        mul = float(cfg.get("cooldown_mul_thin", 1.6) or 1.6)
        cd = int(cd * mul)

    # spread multiplier (if spread wide => slow down)
    try:
        sp = float(getattr(runtime, "last_spread_bps", 0.0) or 0.0)
        sp_hi = float(cfg.get("cooldown_spread_hi_bp", 18.0) or 18.0)
        if sp > 0 and sp >= sp_hi:
            mul = float(cfg.get("cooldown_mul_wide_spread", 1.4) or 1.4)
            cd = int(cd * mul)
    except Exception:
        pass

    # pressure multiplier (only when pressure_hi)
    try:
        if int(getattr(runtime, "pressure_hi", 0) or 0) == 1:
            mul = float(cfg.get("cooldown_mul_pressure_hi", 1.25) or 1.25)
            cd = int(cd * mul)
    except Exception:
        pass

    # clamp
    cd_min = int(cfg.get("cooldown_min_ms", 1000) or 1000)
    cd_max = int(cfg.get("cooldown_max_ms", 120000) or 120000)
    if cd < cd_min: cd = cd_min
    if cd > cd_max: cd = cd_max
    return int(cd)

def _should_sample(ts_ms: int, rate: float) -> bool:
    """
    Deterministic sampling for audits.
    rate: 0..1. Example 0.05 => 5%
    """
    try:
        r = float(rate)
        if r <= 0: return False
        if r >= 1: return True
        # deterministic hash on ts_ms
        return (int(ts_ms) % 10_000) < int(r * 10_000)
    except Exception:
        return False


def _compute_tick_uid(
    *, symbol: str, trade_id: Optional[int], ts_ms: int, price_src: Any, qty_src: Any
    side: str, is_buyer_maker: Any, stream_id: Optional[str] = None
) -> str:
    """Deterministic tick UID for dedup across retries/replays.

    Preference order:
    - if trade_id is present and >0: {symbol}:{trade_id}
    - else if stream_id is present: {symbol}:mid{stream_id}  (stable for Redis retries/PEL)
    - else: crc32 over stable string components (symbol|ts_ms|price|qty|side|bm)

    Notes:
    - We intentionally use raw (pre-float) price/qty sources when available to avoid
      float formatting drift across languages.
    - stream_id is the Redis stream entry id ("<ms>-<seq>").
    """
    try:
        sym = (symbol or "").upper()
        if not sym:
            return "UNKNOWN:h00000000"

        # Prefer trade_id if available
        if trade_id is not None:
            try:
                tid = int(trade_id)
                if tid > 0:
                    return f"{sym}:{tid}"
            except Exception:
                pass

        sid = str(stream_id or "").strip()
        if sid:
            return f"{sym}:mid{sid}"

        s_side = (side or "").upper()

        bm_val = is_buyer_maker
        if bm_val is None:
            bm = ""
        elif isinstance(bm_val, bool):
            bm = "1" if bm_val else "0"
        else:
            bm = "1" if bool(bm_val) else "0"

        ps = "" if price_src is None else str(price_src)
        qs = "" if qty_src is None else str(qty_src)
        base = f"{sym}|{int(ts_ms or 0)}|{ps}|{qs}|{s_side}|{bm}"
        h = zlib.crc32(base.encode("utf-8", errors="replace")) & 0xFFFFFFFF
        return f"{sym}:h{h:08x}"
    except Exception:
        return f"{(symbol or '').upper()}:h00000000"


def _dedup_seen_uid(uid: str, ring, uid_set, window: int) -> bool:
    """Ring-buffer deduplication guard for tick UIDs.

    Returns True (duplicate) if uid has been seen within the current window.
    Otherwise records uid into ring + set and returns False.

    Args:
        uid:     stable tick identifier string
        ring:    collections.deque used as ring buffer (maxlen auto-managed)
        uid_set: set mirroring the ring for O(1) lookup
        window:  max number of recent UIDs to remember (ring capacity)
    """
    if not uid:
        return False
    if uid in uid_set:
        return True
    # Maintain ring size
    if window > 0 and len(ring) >= window:
        evicted = ring.popleft()
        uid_set.discard(evicted)
    ring.append(uid)
    uid_set.add(uid)
    return False


def _parse_tick_payload(
    payload: Any, default_symbol: str = "", log: Optional[Callable] = None
) -> Optional[Dict[str, Any]]:
    """Normalize tick payload into a stable schema used across the pipeline.

    Key points (schema v2-ish):
      - side is NEVER defaulted to BUY. Missing/invalid side => "UNKNOWN".
      - side_conf indicates source: explicit | maker | unknown
      - is_buyer_maker is preserved from source when present; not derived from side.
      - trade_id best-effort extraction (Binance aggTrade: t/a)
      - tick_uid best-effort stable id for dedupe (trade_id > stream_id > hash).
        stream_id is not available here; consumer may overwrite tick_uid with stream_id-aware uid.
    """
    if payload is None:
        return None

    merged: Dict[str, Any] = {}
    try:
        if isinstance(payload, (bytes, bytearray)):
            payload = payload.decode("utf-8", errors="ignore")
        if isinstance(payload, str):
            merged = json.loads(payload)
        elif isinstance(payload, dict):
            merged = payload
        else:
            merged = dict(payload) if isinstance(payload, (list, tuple)) else {}
    except Exception:
        return None

    # Handle nested data field
    if "data" in merged:
        try:
            nested = json.loads(merged["data"]) if isinstance(merged["data"], str) else merged["data"]
            if isinstance(nested, dict):
                merged = {**merged, **nested}
        except Exception:
            pass

    # timestamp (epoch ms)
    ts_ms = _normalize_epoch_ms(
        merged.get("ts_ms") or
        merged.get("ts") or
        merged.get("event_time") or
        merged.get("E") or
        merged.get("T") or
        merged.get("time") or
        merged.get("written_at")
    )

    # symbol
    symbol = (
        merged.get("symbol") or merged.get("s") or merged.get("pair") or default_symbol
    )

    # raw price/qty sources (keep raw for UID stability)
    price_src = merged.get("price") or merged.get("p") or merged.get("last") or merged.get("mid")
    qty_src = merged.get("qty") or merged.get("q") or merged.get("volume")

    # trade id (optional)
    tid_raw = (
        merged.get("trade_id") or merged.get("tradeId") or merged.get("t") or merged.get("a") or
        merged.get("id") or merged.get("tradeID")
    )
    trade_id_val: Optional[int] = None
    try:
        if tid_raw is not None:
            tv = int(float(str(tid_raw).strip()))
            if tv > 0:
                trade_id_val = tv
    except Exception:
        trade_id_val = None

    # is_buyer_maker (Binance semantics: True => taker SELL, False => taker BUY)
    def _coerce_bool_maybe(v: Any) -> Optional[bool]:
        if v is None:
            return None
        if isinstance(v, bool):
            return v
        if isinstance(v, (int, float)):
            iv = int(v)
            if iv in (0, 1):
                return bool(iv)
            return None
        if isinstance(v, str):
            s = v.strip().lower()
            if s in ("1", "true", "yes", "on"):
                return True
            if s in ("0", "false", "no", "off"):
                return False
            return None
        return None

    is_buyer_maker = merged.get("is_buyer_maker")
    if is_buyer_maker is None:
        is_buyer_maker = merged.get("m")  # Binance isBuyerMaker
    is_buyer_maker = _coerce_bool_maybe(is_buyer_maker)

    # side normalization (NO BUY default)
    side_raw_val = merged.get("side") or merged.get("trade_side")
    side_raw = None if side_raw_val is None else str(side_raw_val)

    side = ""
    side_conf = "unknown"

    if side_raw_val is not None:
        s = str(side_raw_val).strip().upper()
        if s in ("BUY", "SELL"):
            side = s
            side_conf = "explicit"

    if (not side) and (is_buyer_maker is not None):
        side = "SELL" if bool(is_buyer_maker) else "BUY"
        side_conf = "maker"

    if not side:
        side = "UNKNOWN"
        side_conf = "unknown"

    tick: Dict[str, Any] = {
        "symbol": symbol
        "ts": int(ts_ms or 0),      # legacy epoch ms (keep)
        "ts_ms": int(ts_ms or 0),   # payload time (may be coerced later)
        "event_ts_ms": int(ts_ms or 0),  # payload event time (may be coerced later)
        "price": _safe_float(price_src)
        "last": _safe_float(merged.get("last"))
        "bid": _safe_float(merged.get("bid"))
        "ask": _safe_float(merged.get("ask"))
        "qty": qty_src
        "side": side
        "side_raw": side_raw
        "side_conf": side_conf
        "is_buyer_maker": is_buyer_maker
        "trade_id": trade_id_val
        "raw": merged
        "written_at": datetime.now(timezone.utc).isoformat()
        "tick_uid": ""
        "ts_source": "payload" if int(ts_ms or 0) > 0 else "missing"
    }

    # normalize qty to float and bail if invalid
    try:
        qty = float(tick.get("qty", 0.0) or 0.0)
    except (TypeError, ValueError):
        qty = 0.0

    if qty <= 0:
        return None

    tick["qty"] = qty

    # Deterministic UID for dedup (prefer trade_id; consumer may overwrite with stream_id-aware uid)
    tick["tick_uid"] = _compute_tick_uid(
        symbol=str(tick.get("symbol") or "")
        trade_id=trade_id_val
        ts_ms=int(tick.get("ts_ms") or 0)
        price_src=price_src
        qty_src=qty_src
        side=str(tick.get("side") or "")
        is_buyer_maker=tick.get("is_buyer_maker")
    )

    bid = _safe_float(tick.get("bid"))
    ask = _safe_float(tick.get("ask"))
    if bid and ask:
        tick["mid"] = (bid + ask) / 2.0
    else:
        tick["mid"] = _safe_float(tick.get("price"))

    return tick


def _parse_book_payload(payload: Dict[str, Any], symbol: str) -> Dict[str, Any]:
    if "data" in payload:
        try:
            nested = json.loads(payload["data"])
        except json.JSONDecodeError:
            nested = {}
    else:
        nested = {}

    merged = {**payload, **nested}
    # Ensure bids/asks are list of [price, qty]
    bids = merged.get("bids") or []
    if isinstance(bids, str):
        try:
            bids = json.loads(bids)
        except Exception:
            bids = []
            
    asks = merged.get("asks") or []
    if isinstance(asks, str):
        try:
            asks = json.loads(asks)
        except Exception:
            asks = []

    ts_ms = _normalize_epoch_ms(
        merged.get("ts_ms") or 
        merged.get("ts") or 
        merged.get("E") or 
        merged.get("event_time") or 
        merged.get("T") or 
        merged.get("time") or 
        merged.get("written_at")
    )
    
    return {
        "symbol": symbol
        "ts_ms": ts_ms
        # Binance: depthUpdate has both firstUpdateId ("U") and lastUpdateId ("u").
        # Spot depthUpdate: {"U": firstUpdateId, "u": lastUpdateId, ...}
        # Futures depthUpdate: same keys.
        # partial depth snapshots (@depth5/@depth10/@depth20) typically do NOT have U/u.
        "U": _safe_int(merged.get("U") or merged.get("firstUpdateId"))
        "u": _safe_int(merged.get("u") or merged.get("lastUpdateId")),  # Binance specific
        "bids": bids
        "asks": asks
        "written_at": _safe_int(merged.get("written_at"))
    }

class LogSampler:
    def __init__(self, sample_rate: int = 1):
        self.n = int(sample_rate)
        self.counts = {}

    def should_log(self, key: str) -> bool:
        if self.n <= 1:
            return True
        c = self.counts.get(key, 0)
        self.counts[key] = c + 1
        return (c % self.n) == 0

# ---------------------------------------------------------------------------
# Session / time bucketing helpers (UTC)
# ---------------------------------------------------------------------------
def hour_of_week_utc(ts_ms: int) -> int:
    """0..167, UTC hour-of-week."""
    from datetime import datetime, timezone
    dt = datetime.fromtimestamp(float(ts_ms) / 1000.0, tz=timezone.utc)
    return int(dt.weekday() * 24 + dt.hour)

def session_utc(ts_ms: int) -> str:
    """Simple UTC sessions (stable, no overlaps)."""
    from datetime import datetime, timezone
    h = datetime.fromtimestamp(float(ts_ms) / 1000.0, tz=timezone.utc).hour
    if 0 <= h < 8:
        return "ASIA"
    if 8 <= h < 14:
        return "EU"
    if 14 <= h < 21:
        return "NY"
    return "OFF"

def fmt_utc_dow_hour(ts_ms: int) -> str:
    from datetime import datetime, timezone
    dt = datetime.fromtimestamp(float(ts_ms) / 1000.0, tz=timezone.utc)
    return dt.strftime("%a %H:00 UTC")


class LogSamplerFactory:
    _samplers = {}
    @staticmethod
    def get_sampler(key: str, rate: int) -> 'LogSampler':
        if key not in LogSamplerFactory._samplers:
            LogSamplerFactory._samplers[key] = LogSampler(sample_rate=rate)
        return LogSamplerFactory._samplers[key]
