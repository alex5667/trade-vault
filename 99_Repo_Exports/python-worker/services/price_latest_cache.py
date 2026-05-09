from __future__ import annotations

import math
import os
from typing import Any

from domain.time_utils import normalize_ts_ms


def _safe_float(x: Any, d: float = 0.0) -> float:
    try:
        v = float(x)
        if not math.isfinite(v):
            return d
        return v
    except Exception:
        return d


def write_price_latest(
    redis_client: Any,
    *,
    symbol: str,
    ts_ms: Any,
    bid: Any = None,
    ask: Any = None,
    last: Any = None,
    mid: Any | None = None,
    venue: str = "na",
) -> None:
    """
    Fail-open latest price cache for cross-symbol features (SMT/coherence, drift alarm).

    Key:
      price:latest:{SYMBOL}
    Fields (all strings; stable for Redis hash):
      mid, ts_ms, venue
      bid, ask, last (optional; useful for audits)
      spread_bps (optional; computed if bid/ask/mid valid)

    Timestamp policy:
      - normalize_ts_ms() is the single source of truth:
        * seconds -> ms (x1000)
        * invalid -> 0 (then we skip write)
    """
    try:
        if redis_client is None:
            return

        enabled = (os.getenv("PRICE_LATEST_ENABLED", "1") or "1").strip().lower()
        if enabled in ("0", "false", "no", "off"):
            return

        sym = ((symbol or "").strip().upper())
        if not sym:
            return

        tsm = int(normalize_ts_ms(ts_ms))
        if tsm <= 0:
            # hard fail-open: do not pollute keys with invalid timestamps
            return

        b = _safe_float(bid, 0.0)
        a = _safe_float(ask, 0.0)
        l = _safe_float(last, 0.0)

        m = _safe_float(mid, 0.0) if mid is not None else 0.0
        if m <= 0.0:
            # Prefer true mid from bid/ask if available; else fallback to last.
            if b > 0.0 and a > 0.0 and a >= b:
                m = 0.5 * (a + b)
            elif l > 0.0:
                m = l
            else:
                # No usable price -> skip
                return

        # Compute spread_bps if possible (best-effort)
        spread_bps = 0.0
        try:
            if b > 0.0 and a > 0.0 and a >= b and m > 0.0:
                spread_bps = float((a - b) / m * 10_000.0)
                if not math.isfinite(spread_bps) or spread_bps < 0.0:
                    spread_bps = 0.0
        except Exception:
            spread_bps = 0.0

        mapping: dict[str, str] = {
            "mid": f"{float(m):.10f}",
            "ts_ms": str(int(tsm)),
            "venue": (venue or "na"),
        }

        # Optional raw fields for debugging/audit. Controlled via env (default on).
        store_raw = (os.getenv("PRICE_LATEST_STORE_RAW", "1") or "1").strip().lower() not in ("0", "false", "no", "off")
        if store_raw:
            if b > 0.0:
                mapping["bid"] = f"{float(b):.10f}"
            if a > 0.0:
                mapping["ask"] = f"{float(a):.10f}"
            if l > 0.0:
                mapping["last"] = f"{float(l):.10f}"
            if spread_bps > 0.0:
                mapping["spread_bps"] = f"{float(spread_bps):.6f}"

        redis_client.hset(f"price:latest:{sym}", mapping=mapping)

        # Optional TTL (FakeRedis may not support expire/pexpire -> guard it).
        try:
            ttl_ms = int(float(os.getenv("PRICE_LATEST_TTL_MS", "0")))
        except Exception:
            ttl_ms = 0
        if ttl_ms > 0:
            try:
                if hasattr(redis_client, "pexpire"):
                    redis_client.pexpire(f"price:latest:{sym}", ttl_ms)
                elif hasattr(redis_client, "expire"):
                    redis_client.expire(f"price:latest:{sym}", int(max(1, ttl_ms // 1000)))
            except Exception:
                pass
    except Exception:
        # fail-open: never break ingest
        return
