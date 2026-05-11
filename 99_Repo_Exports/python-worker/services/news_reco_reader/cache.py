from __future__ import annotations

"""services.news_reco_reader.cache

In-memory TTL cache for trade-side news recommendations (reco).

The cache is updated by an asyncio background reader, and accessed on the
hot-path by Stage-5 gates/policies without any IO.

Contract (Redis map value)
--------------------------
The trade-side cache consumer is expected to write a single Redis key:

    trade:cache:news_reco_map

with JSON:

    {
      "schema_ver": "v1",
      "ts_ms": 1710000000000,
      "reco": {
        "BTCUSDT": {"expires_ms": 1710000005000, ...},
        "ETHUSDT": {"expires_ms": 1710000006000, ...}
      }
    }

Only "reco" and per-symbol "expires_ms" are strictly required. Everything
else is treated as opaque payload (forward compatible).
"""

import json
from dataclasses import dataclass
from typing import Any

from utils.time_utils import get_ny_time_millis


def now_ms() -> int:
    return get_ny_time_millis()


def sanitize_symbol(sym: str) -> str:
    sym = (sym or "").strip().upper()
    # Defensive: keep only common chars to avoid memory abuse via hostile keys.
    # Binance-like symbols are [A-Z0-9], but we allow ':' and '-' for internal.
    out = []
    for ch in sym:
        if ch.isalnum() or ch in (":", "-", "_"):
            out.append(ch)
    sym = "".join(out)
    return sym[:40]  # hard cap


@dataclass
class NewsRecoSnapshot:
    symbol: str
    expires_ms: int
    received_ts_ms: int
    payload: dict[str, Any]


class NewsRecoCache:
    """Simple TTL cache with size cap and deterministic eviction."""

    def __init__(self, max_symbols: int = 2000) -> None:
        self._max_symbols = max(10, int(max_symbols))
        self._by_symbol: dict[str, NewsRecoSnapshot] = {}

    @property
    def size(self) -> int:
        return len(self._by_symbol)

    def get(self, symbol: str, *, now: int | None = None) -> NewsRecoSnapshot | None:
        nowv = now if now is not None else now_ms()
        sym = sanitize_symbol(symbol)
        snap = self._by_symbol.get(sym)
        if snap is None:
            return None
        if snap.expires_ms <= nowv:
            # Expired — delete and fail-open.
            self._by_symbol.pop(sym, None)
            return None
        return snap

    def sweep_expired(self, *, now: int | None = None) -> int:
        nowv = now if now is not None else now_ms()
        to_del = [s for s, v in self._by_symbol.items() if v.expires_ms <= nowv]
        for s in to_del:
            self._by_symbol.pop(s, None)
        return len(to_del)

    def update_from_map_json(self, raw_json: str, *, now: int | None = None) -> tuple[int, int, int]:
        """Parse and apply a map JSON.

        Returns (updated, skipped_invalid, expired_dropped).
        """
        nowv = now if now is not None else now_ms()
        try:
            obj = json.loads(raw_json)
        except Exception as exc:
            raise ValueError(f"invalid json: {exc}") from exc

        if not isinstance(obj, dict):
            raise ValueError("map is not an object")

        reco = obj.get("reco")
        ts_ms = obj.get("ts_ms")

        if reco is None or not isinstance(reco, dict):
            raise ValueError("map.reco missing or not an object")

        updated = 0
        invalid = 0
        expired = 0

        for k, v in reco.items():
            sym = sanitize_symbol(str(k))
            if not sym:
                invalid += 1
                continue

            if not isinstance(v, dict):
                invalid += 1
                continue

            exp = v.get("expires_ms")
            try:
                exp_ms = int(exp)  # type: ignore
            except Exception:
                invalid += 1
                continue

            if exp_ms <= nowv:
                expired += 1
                # Don't store expired values.
                self._by_symbol.pop(sym, None)
                continue

            payload = dict(v)
            payload.setdefault("symbol", sym)
            if ts_ms is not None:
                payload.setdefault("map_ts_ms", ts_ms)

            self._by_symbol[sym] = NewsRecoSnapshot(
                symbol=sym,
                expires_ms=exp_ms,
                received_ts_ms=nowv,
                payload=payload,
            )
            updated += 1

        # Enforce size cap deterministically: evict earliest expiry first.
        if len(self._by_symbol) > self._max_symbols:
            items = sorted(self._by_symbol.items(), key=lambda kv: kv[1].expires_ms)
            to_evict = len(items) - self._max_symbols
            for i in range(to_evict):
                self._by_symbol.pop(items[i][0], None)

        return updated, invalid, expired

    def as_dict(self, *, now: int | None = None) -> dict[str, dict[str, Any]]:
        nowv = now if now is not None else now_ms()
        out: dict[str, dict[str, Any]] = {}
        for sym, snap in list(self._by_symbol.items()):
            if snap.expires_ms <= nowv:
                self._by_symbol.pop(sym, None)
                continue
            out[sym] = snap.payload
        return out
