# data_parser.py
"""
Data parsing functionality extracted from base_orderflow_handler.py
"""

from __future__ import annotations
from utils.time_utils import get_ny_time_millis

from typing import Optional, Dict, Any, Tuple, List
import time
import json
from datetime import datetime, timezone

# Safe imports
try:
    from contexts import Tick, SimpleL2Snapshot, L2Level
except ImportError:
    # Fallback definitions for testing
    class Tick:
        def __init__(self, ts: int, bid: float, ask: float, last: float, volume: float, flags: int, is_buyer_maker: Optional[bool]):
            self.ts = ts
            self.bid = bid
            self.ask = ask
            self.last = last
            self.volume = volume
            self.flags = flags
            self.is_buyer_maker = is_buyer_maker

    class L2Level:
        def __init__(self, price: float, size: float):
            self.price = price
            self.size = size

    class SimpleL2Snapshot:
        def __init__(self, bids: List[L2Level], asks: List[L2Level], ts_ms: int, mid: float,
                     best_bid: float, best_ask: float, depth_bid_5: float, depth_ask_5: float,
                     depth_bid_20: float, depth_ask_20: float):
            self.bids = bids
            self.asks = asks
            self.ts_ms = ts_ms
            self.mid = mid
            self.best_bid = best_bid
            self.best_ask = best_ask
            self.depth_bid_5 = depth_bid_5
            self.depth_ask_5 = depth_ask_5
            self.depth_bid_20 = depth_bid_20
            self.depth_ask_20 = depth_ask_20


EPOCH_MS_MIN = 946684800000  # 2000-01-01
EPOCH_MS_FUTURE_SKEW = 86_400_000  # +1 day


def _ensure_epoch_ms(ts: Any, *, now_ms: Optional[int] = None) -> Optional[int]:
    """
    Strict epoch-ms validation.
    Rejects minutes-of-day, small offsets, non-epoch timestamps.
    """
    if ts is None:
        return None
    if now_ms is None:
        now_ms = get_ny_time_millis()

    # bytes -> str
    if isinstance(ts, (bytes, bytearray)):
        ts = ts.decode("utf-8", errors="ignore")

    # str -> number/iso
    if isinstance(ts, str):
        s = ts.strip()
        # numeric?
        try:
            v = float(s)
            ts = int(v)
        except Exception:
            # iso?
            try:
                iso = s.replace("Z", "+00:00")
                dt = datetime.fromisoformat(iso)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return int(dt.timestamp() * 1000)
            except Exception:
                return None

    if isinstance(ts, (int, float)):
        ts_i = int(ts)

        # seconds epoch plausible range (>= 2001-09-09)
        if 1_000_000_000 <= ts_i < 1_000_000_000_000:
            ts_i *= 1000

        # hard epoch-ms plausibility window (>= 2001-09-09)
        if ts_i < 1_000_000_000_000:
            return None
        if ts_i > now_ms + 86_400_000:      # future > 1 day
            return None

        return ts_i

    return None


# Backward compatibility alias
def _ensure_ts_ms(ts: Any, *, now_ms: Optional[int] = None, fallback_now: bool = False) -> Optional[int]:
    """Legacy wrapper for backward compatibility."""
    result = _ensure_epoch_ms(ts, now_ms=now_ms)
    if result is None and fallback_now:
        return int(now_ms or get_ny_time_millis())
    return result


def _parse_bool(v: Any) -> Optional[bool]:
    if v is None:
        return None
    if isinstance(v, bool):
        return v
    if isinstance(v, (bytes, bytearray)):
        v = v.decode("utf-8", errors="ignore")
    if isinstance(v, str):
        s = v.strip().lower()
        if s in ("true", "1", "yes", "y"):
            return True
        if s in ("false", "0", "no", "n"):
            return False
    return None


def _maybe_json(v: Any) -> Any:
    """If v is JSON string -> json.loads(v), else return v."""
    if isinstance(v, (bytes, bytearray)):
        v = v.decode("utf-8", errors="ignore")
    if isinstance(v, str):
        s = v.strip()
        if s and (s[0] in "[{" and s[-1] in "]}"):
            try:
                return json.loads(s)
            except Exception:
                return v
    return v


class OrderFlowDataParser:
    """
    Parser for incoming orderflow data (ticks, books, L3 events).
    """

    def __init__(self, symbol: str, specs: Any, logger: Any = None):
        self.symbol = symbol
        self.specs = specs
        self.logger = logger  # optional

    def _parse_tick(self, fields: Dict[str, Any]) -> Optional[Tick]:
        try:
            data = fields
            if "data" in fields:
                decoded = _maybe_json(fields.get("data"))
                if isinstance(decoded, dict):
                    data = decoded

            now_ms = get_ny_time_millis()
            ts_ms = _ensure_ts_ms(data.get("ts") or data.get("ts_ms") or fields.get("ts"), now_ms=now_ms, fallback_now=True)

            last = data.get("last", data.get("price"))
            if last is None:
                return None

            bid = data.get("bid")
            ask = data.get("ask")

            last_f = float(last)
            if last_f <= 0:
                return None

            # bid/ask optional: fallback to last
            bid_f = float(bid) if bid is not None else last_f
            ask_f = float(ask) if ask is not None else last_f

            # if crossed -> fallback to last/last (do not drop the tick)
            if bid_f <= 0 or ask_f <= 0 or ask_f < bid_f:
                bid_f = last_f
                ask_f = last_f

            volume = data.get("volume", data.get("qty", 0.0)) or 0.0
            flags = data.get("flags", 0) or 0

            # binance часто: "m" / "isBuyerMaker"
            is_buyer_maker = _parse_bool(data.get("is_buyer_maker"))
            if is_buyer_maker is None:
                is_buyer_maker = _parse_bool(data.get("m") or data.get("isBuyerMaker") or data.get("buyer_is_maker"))

            return Tick(
                ts=int(ts_ms),  # type: ignore
                bid=float(bid_f),
                ask=float(ask_f),
                last=float(last_f),
                volume=float(volume) if volume is not None else 0.0,
                flags=int(flags),
                is_buyer_maker=is_buyer_maker,
            )
        except Exception as e:
            if self.logger:
                self.logger.debug("Failed to parse tick: %s | fields=%s", e, fields)
            return None

    def _parse_book(self, fields: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        try:
            data = fields
            if "data" in fields:
                decoded = _maybe_json(fields.get("data"))
                if isinstance(decoded, dict):
                    data = decoded

            ts_ms = _ensure_ts_ms(data.get("ts") or data.get("ts_ms") or fields.get("ts") or fields.get("ts_ms"),
                                  fallback_now=True)

            bids_data = _maybe_json(data.get("bids") if isinstance(data, dict) else None) or _maybe_json(fields.get("bids"))
            asks_data = _maybe_json(data.get("asks") if isinstance(data, dict) else None) or _maybe_json(fields.get("asks"))

            if not isinstance(bids_data, list) or not isinstance(asks_data, list):
                return None

            def _lvl(it: Any) -> Optional[Tuple[float, float]]:
                if isinstance(it, (list, tuple)) and len(it) >= 2:
                    return float(it[0]), float(it[1])
                if isinstance(it, dict):
                    p = it.get("price", it.get("p"))
                    q = it.get("size", it.get("qty", it.get("q")))
                    if p is None or q is None:
                        return None
                    return float(p), float(q)
                return None

            bids: List[L2Level] = []
            for it in bids_data[:20]:
                lv = _lvl(it)
                if not lv:
                    continue
                price, size = lv
                if price > 0 and size > 0:
                    bids.append(L2Level(price=price, size=size))

            asks: List[L2Level] = []
            for it in asks_data[:20]:
                lv = _lvl(it)
                if not lv:
                    continue
                price, size = lv
                if price > 0 and size > 0:
                    asks.append(L2Level(price=price, size=size))

            if not bids or not asks:
                return None

            bids.sort(key=lambda x: x.price, reverse=True)
            asks.sort(key=lambda x: x.price)

            best_bid = bids[0].price
            best_ask = asks[0].price
            if best_ask <= 0 or best_bid <= 0 or best_ask < best_bid:
                return None

            mid = (best_bid + best_ask) / 2.0

            def _sum_depth(levels: List[L2Level], n: int) -> float:
                return float(sum(l.size for l in levels[: min(n, len(levels))]))

            snapshot = SimpleL2Snapshot(
                bids=bids, asks=asks, ts_ms=int(ts_ms), mid=float(mid),
                best_bid=float(best_bid), best_ask=float(best_ask),
                depth_bid_5=_sum_depth(bids, 5), depth_ask_5=_sum_depth(asks, 5),
                depth_bid_20=_sum_depth(bids, 20), depth_ask_20=_sum_depth(asks, 20),
            )
            return {"snapshot": snapshot, "ts_ms": int(ts_ms), "raw_data": fields}
        except Exception as e:
            if self.logger:
                self.logger.debug("Failed to parse book: %s | fields=%s", e, fields)
            return None

    def _parse_l3_event(self, fields: Dict[str, Any]) -> Optional[Any]:
        try:
            data = fields
            if "data" in fields:
                decoded = _maybe_json(fields.get("data"))
                if isinstance(decoded, dict):
                    data = decoded

            ts_ms = _ensure_ts_ms(data.get("ts") or data.get("ts_ms") or fields.get("ts") or fields.get("ts_ms"), fallback_now=True)
            if ts_ms is None:
                ts_ms = get_ny_time_millis()

            side = (data.get("side") or "").lower()
            if side in ("buy", "bid", "b"):
                side = "buy"
            elif side in ("sell", "ask", "s"):
                side = "sell"
            else:
                return None

            price = float(data.get("price", 0.0))
            qty = float(data.get("qty", data.get("quantity", data.get("size", 0.0))) or 0.0)
            if price <= 0 or qty <= 0:
                return None

            # лучше явный dataclass, но оставим как есть
            L3LiteEvent = type("L3LiteEvent", (), {})
            ev = L3LiteEvent()
            ev.ts_ms = int(ts_ms)
            ev.kind = data.get("kind", "unknown")
            ev.side = side
            ev.price = price
            ev.qty = qty
            return ev

        except Exception as e:
            if self.logger:
                self.logger.debug("Failed to parse L3 event: %s | fields=%s", e, fields)
            return None
