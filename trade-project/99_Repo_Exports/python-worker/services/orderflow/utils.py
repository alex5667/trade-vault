import os
import zlib
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

import msgpack
import orjson

from services.orderflow.configuration import _safe_float, _safe_int


def hour_of_week_utc(ts_ms: int) -> int:  # type: ignore
    dt = datetime.fromtimestamp(ts_ms / 1000.0, tz=UTC)
    return dt.weekday() * 24 + dt.hour  # 0..167

def session_utc(ts_ms: int) -> str:  # type: ignore
    """
    UTC sessions (simple and stable; no overlaps)
    ASIA: 00:00 - 08:00
    EU:   08:00 - 14:00
    NY:   14:00 - 21:00
    OFF:  21:00 - 00:00
    """
    h = datetime.fromtimestamp(ts_ms / 1000.0, tz=UTC).hour
    if 0 <= h < 8:
        return "ASIA"
    if 8 <= h < 14:
        return "EU"
    if 14 <= h < 21:
        return "NY"
    return "OFF"

def fmt_utc_dow_hour(ts_ms: int) -> str:  # type: ignore
    dt = datetime.fromtimestamp(ts_ms / 1000.0, tz=UTC)
    return dt.strftime("%a %H:00 UTC")


def _fields_to_dict(fields: dict[Any, Any]) -> dict[str, str]:
    """Helper to convert redis stream fields (bytes) to dict (str)."""
    if not fields:
        return {}
    return {
        k.decode("utf-8") if isinstance(k, bytes) else str(k):
        v.decode("utf-8") if isinstance(v, bytes) else str(v)
        for k, v in fields.items()
    }

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

        # REMOVED sec -> ms heuristic to enforce explicit `bad_ts_unit` detection downstream

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

def _score_candidate(ind: dict[str, Any]) -> float:
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

def _calc_pressure_sps(ts_list: list[int], now_ms: int, window_ms: int = 60_000) -> float:
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

def _safe_float(v, default=0.0):
    """Safe float conversion with bounds checking."""
    try:
        f = float(v) if v is not None else default
        return f if f >= 0 else default  # reject negative multipliers
    except (TypeError, ValueError):
        return default

def _safe_int(v, default=0):
    """Safe int conversion with bounds checking."""
    try:
        i = int(float(v)) if v is not None else default
        return max(0, i)  # reject negative values
    except (TypeError, ValueError):
        return default

def _burst_min_gap_floor_ms(runtime, *, cur_dir: str) -> int:
    """Resolve burst-min-gap floor (ms) from ENV with per-symbol/direction overrides.

    Resolution order (longest match wins):
      1. BURST_MIN_GAP_SEC_<PREFIX>_<DIR>   (e.g. BURST_MIN_GAP_SEC_PEPE_LONG)
      2. BURST_MIN_GAP_SEC_<PREFIX>        (e.g. BURST_MIN_GAP_SEC_PEPE)
      3. BURST_MIN_GAP_SEC_<DIR>           (e.g. BURST_MIN_GAP_SEC_LONG)
      4. BURST_MIN_GAP_SEC                 (global)

    PREFIX = symbol with leading 1000/`USDT`/`USDC`/`USD` stripped.
    Returns 0 when no env override is set.

    LONG-in-downtrend extra multiplier: when cur_dir=LONG and regime is
    `trending_bear`, multiply the resolved floor by `BURST_MIN_GAP_DOWNTREND_MUL`
    (default 1.0 = off). Encourages slower LONG cadence when the trend is bearish.

    Re-added 2026-05-26 (P1.7) — previously rolled back 2026-05-19.
    """
    symbol = str(getattr(runtime, "symbol", "") or "").upper()
    prefix = symbol
    for tail in ("USDT", "USDC", "USD"):
        if prefix.endswith(tail):
            prefix = prefix[: -len(tail)]
            break
    if prefix.startswith("1000"):
        prefix = prefix[4:]
    dr = (cur_dir or "").upper().strip()

    candidates = []
    if prefix and dr:
        candidates.append(f"BURST_MIN_GAP_SEC_{prefix}_{dr}")
    if prefix:
        candidates.append(f"BURST_MIN_GAP_SEC_{prefix}")
    if dr:
        candidates.append(f"BURST_MIN_GAP_SEC_{dr}")
    candidates.append("BURST_MIN_GAP_SEC")

    sec = 0
    for name in candidates:
        raw = os.environ.get(name)
        if raw is None or raw == "":
            continue
        try:
            v = int(float(raw))
            if v > 0:
                sec = v
                break
        except (TypeError, ValueError):
            continue

    if sec <= 0:
        return 0

    # LONG-in-downtrend multiplier
    if dr == "LONG":
        rg = str(getattr(runtime, "last_regime", "na") or "na").lower()
        if rg in ("trending_bear", "downtrend"):
            try:
                mul = float(os.environ.get("BURST_MIN_GAP_DOWNTREND_MUL", "1.0") or 1.0)
                if mul > 1.0:
                    sec = int(sec * mul)
            except (TypeError, ValueError):
                pass

    return sec * 1000


def _cooldown_ms_for(runtime, *, scenario: str, now_ms: int, new_dir: str = "") -> int:
    """
    Scenario-aware cooldown with directional reversal penalty:
      - reversal and continuation can have different budgets
      - directional reversal (LONG→SHORT or SHORT→LONG) gets extra penalty (anti-whipsaw)
      - in thin/news/illiquid/stressed or wide spread => stricter (longer)
      - under pressure_hi => slightly longer (avoid chasing noise)
    Deterministic: uses runtime.last_regime / runtime.last_spread_bps (updated on microbar close).
    """
    cfg = getattr(runtime, "config", {}) or {}
    scn = (scenario or "").strip().lower()
    # base cooldowns (with validation)
    cd_rev_sec = _safe_int(cfg.get("cooldown_reversal_sec", cfg.get("signal_cooldown_sec", 30)), 30)
    cd_con_sec = _safe_int(cfg.get("cooldown_continuation_sec", cfg.get("signal_cooldown_sec", 30)), 30)
    cd_rev = int(cd_rev_sec) * 1000
    cd_con = int(cd_con_sec) * 1000
    cd = cd_rev if scn == "reversal" else cd_con

    # ── Directional reversal penalty (anti-whipsaw) ──
    # If new signal direction is opposite to last emitted direction → multiply cooldown
    # This prevents LONG→SHORT→LONG churn within minutes
    last_dir = str(getattr(runtime, "last_emit_dir", "NONE") or "NONE").upper()
    cur_dir = (new_dir or "").strip().upper()
    if cur_dir and last_dir not in ("NONE", "") and cur_dir != last_dir:
        dir_mul = _safe_float(cfg.get("cooldown_reversal_dir_mul", 3.0), 3.0)
        cd = int(cd * dir_mul)

    # regime multiplier (thin/news/illiquid)
    rg = str(getattr(runtime, "last_regime", "na") or "na").lower()
    if rg in ("thin", "news", "illiquid"):
        mul = _safe_float(cfg.get("cooldown_mul_thin", 1.6), 1.6)
        cd = int(cd * mul)

    # stressed liquidity multiplier (liq_score < 0.35 typically)
    liq_regime = str(getattr(runtime, "liq_regime", "normal") or "normal").lower()
    if liq_regime == "stressed":
        mul = _safe_float(cfg.get("cooldown_mul_stressed", 1.8), 1.8)
        cd = int(cd * mul)

    # spread multiplier (if spread wide => slow down)
    sp = _safe_float(getattr(runtime, "last_spread_bps", 0.0), 0.0)
    sp_hi = _safe_float(cfg.get("cooldown_spread_hi_bp", 18.0), 18.0)
    if sp > 0 and sp >= sp_hi:
        mul = _safe_float(cfg.get("cooldown_mul_wide_spread", 1.4), 1.4)
        cd = int(cd * mul)

    # pressure multiplier (only when pressure_hi)
    if int(getattr(runtime, "pressure_hi", 0) or 0) == 1:
        mul = _safe_float(cfg.get("cooldown_mul_pressure_hi", 1.25), 1.25)
        cd = int(cd * mul)

    # P1.7 — Burst min-gap floor applied AFTER all multipliers but BEFORE clamp.
    # SHADOW mode (BURST_MIN_GAP_SHADOW=1, default 0): compute the floor and stash
    # the would-veto delta on runtime for downstream indicators, but DON'T lift cd.
    floor_ms = _burst_min_gap_floor_ms(runtime, cur_dir=cur_dir)
    if floor_ms > 0:
        shadow = (os.environ.get("BURST_MIN_GAP_SHADOW", "0").strip().lower()
                  in ("1", "true", "yes", "on"))
        try:
            # Record would-veto state for indicators; no behavioural change.
            runtime.burst_gate_would_veto = 1 if floor_ms > cd else 0
            runtime.burst_gate_floor_ms = floor_ms
        except Exception:
            pass

        if not shadow and floor_ms > cd:
            cd = floor_ms

    # clamp to safe range
    cd_min = _safe_int(cfg.get("cooldown_min_ms", 1000), 1000)
    cd_max = _safe_int(cfg.get("cooldown_max_ms", 300000), 300000)
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
    *, symbol: str, trade_id: int | None, ts_ms: int, price_src: Any, qty_src: Any,
    side: str, is_buyer_maker: Any, stream_id: str | None = None
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

        sid = (stream_id or "").strip()
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
    payload: Any, default_symbol="", log: Callable | None = None
) -> dict[str, Any] | None:
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

    merged: dict[str, Any] = {}
    try:
        use_msgpack = os.getenv("USE_MSGPACK", "false").lower() == "true"
        if isinstance(payload, (bytes, bytearray)):
            if use_msgpack:
                try:
                    merged = msgpack.unpackb(payload, raw=False)
                except Exception:
                    payload = payload.decode("utf-8", errors="ignore")
                    merged = orjson.loads(payload)
            else:
                payload = payload.decode("utf-8", errors="ignore")
                merged = orjson.loads(payload)
        elif isinstance(payload, str):
            merged = orjson.loads(payload)
        elif isinstance(payload, dict):
            merged = payload
        else:
            merged = dict(payload) if isinstance(payload, (list, tuple)) else {}
    except Exception:
        return None

    # Handle nested data field
    if "data" in merged:
        try:
            nested = orjson.loads(merged["data"]) if isinstance(merged["data"], str) else merged["data"]
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
    trade_id_val: int | None = None
    try:
        if tid_raw is not None:
            tv = int(float(str(tid_raw).strip()))
            if tv > 0:
                trade_id_val = tv
    except Exception:
        trade_id_val = None

    # is_buyer_maker (Binance semantics: True => taker SELL, False => taker BUY)
    def _coerce_bool_maybe(v: Any) -> bool | None:
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

    tick: dict[str, Any] = {
        "symbol": symbol,
        "ts": int(ts_ms or 0),      # legacy epoch ms (keep)
        "ts_ms": int(ts_ms or 0),   # payload time (may be coerced later)
        "event_ts_ms": int(ts_ms or 0),  # payload event time (may be coerced later)
        "price": _safe_float(price_src),
        "last": _safe_float(merged.get("last")),
        "bid": _safe_float(merged.get("bid")),
        "ask": _safe_float(merged.get("ask")),
        "qty": qty_src,
        "side": side,
        "side_raw": side_raw,
        "side_conf": side_conf,
        "is_buyer_maker": is_buyer_maker,
        "trade_id": trade_id_val,
        "raw": merged,
        "written_at": datetime.now(UTC).isoformat(),
        "tick_uid": "",
        "ts_source": "payload" if int(ts_ms or 0) > 0 else "missing",
    }

    # normalize qty to float and bail if invalid
    qty = _safe_float(tick.get("qty"))

    if qty <= 0:
        return None

    tick["qty"] = qty

    if side == "BUY":
        tick["direction"] = "LONG"
        tick["aggressor_sign"] = 1
        tick["counted_in_delta"] = True
        tick["qty_signed"] = qty
    elif side == "SELL":
        tick["direction"] = "SHORT"
        tick["aggressor_sign"] = -1
        tick["counted_in_delta"] = True
        tick["qty_signed"] = -qty
    else:
        tick["direction"] = "NONE"
        tick["aggressor_sign"] = 0
        tick["counted_in_delta"] = False
        tick["qty_signed"] = 0.0

    # Deterministic UID for dedup (prefer trade_id; consumer may overwrite with stream_id-aware uid)
    tick["tick_uid"] = _compute_tick_uid(
        symbol=(tick.get("symbol") or ""),
        trade_id=trade_id_val,
        ts_ms=int(tick.get("ts_ms") or 0),
        price_src=price_src,
        qty_src=qty_src,
        side=(tick.get("side") or ""),
        is_buyer_maker=tick.get("is_buyer_maker"),
    )

    bid = _safe_float(tick.get("bid"))
    ask = _safe_float(tick.get("ask"))
    if bid and ask:
        tick["mid"] = (bid + ask) / 2.0
    else:
        tick["mid"] = _safe_float(tick.get("price"))

    return tick


def _parse_book_payload(payload: dict[str, Any], symbol: str) -> dict[str, Any]:
    if "data" in payload:
        use_msgpack = os.getenv("USE_MSGPACK", "false").lower() == "true"
        if use_msgpack and isinstance(payload["data"], (bytes, bytearray)):
            try:
                nested = msgpack.unpackb(payload["data"], raw=False)
            except Exception:
                try:
                    nested = orjson.loads(payload["data"])
                except Exception:
                    nested = {}
        else:
            try:
                nested = orjson.loads(payload["data"])
            except Exception:
                nested = {}
    else:
        nested = {}

    merged = {**payload, **nested}
    # Ensure bids/asks are list of [price, qty]
    bids = merged.get("bids") or []
    if isinstance(bids, str):
        try:
            bids = orjson.loads(bids)
        except Exception:
            bids = []

    asks = merged.get("asks") or []
    if isinstance(asks, str):
        try:
            asks = orjson.loads(asks)
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
        "symbol": symbol,
        "ts_ms": ts_ms,
        "u": _safe_int(merged.get("u") or merged.get("lastUpdateId")), # Binance specific
        "bids": bids,
        "asks": asks,
        "written_at": _safe_int(merged.get("written_at")),
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
    """0..167, UTC hour-of-week. Returns -1 for invalid (zero/negative) timestamps."""
    if ts_ms <= 0:
        return -1
    from datetime import datetime
    dt = datetime.fromtimestamp(float(ts_ms) / 1000.0, tz=UTC)
    return int(dt.weekday() * 24 + dt.hour)

def session_utc(ts_ms: int) -> str:
    """Simple UTC sessions (stable, no overlaps). Returns 'na' for invalid timestamps."""
    if ts_ms <= 0:
        return "na"
    from datetime import datetime
    h = datetime.fromtimestamp(float(ts_ms) / 1000.0, tz=UTC).hour
    if 0 <= h < 8:
        return "ASIA"
    if 8 <= h < 14:
        return "EU"
    if 14 <= h < 21:
        return "NY"
    return "OFF"

def fmt_utc_dow_hour(ts_ms: int) -> str:
    from datetime import datetime
    dt = datetime.fromtimestamp(float(ts_ms) / 1000.0, tz=UTC)
    return dt.strftime("%a %H:00 UTC")


class LogSamplerFactory:
    _samplers = {}
    @staticmethod
    def get_sampler(key: str, rate: int) -> 'LogSampler':
        if key not in LogSamplerFactory._samplers:
            LogSamplerFactory._samplers[key] = LogSampler(sample_rate=rate)
        return LogSamplerFactory._samplers[key]
