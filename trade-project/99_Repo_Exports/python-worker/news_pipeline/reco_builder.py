"""news_pipeline.reco_builder

Builds and writes trade:cache:news_reco_map from classified news events.

Responsibilities:
- Accept a ClassifyResult + timing info
- Compute per-symbol reco entries (action, risk_factor_bps, expires_ms)
- Merge with existing map (new events override by grade_id, never downgrade)
- Write atomic JSON to Redis key trade:cache:news_reco_map
- Always fail-open: Redis write errors must NOT raise

Redis contract (consumed by services/news_reco_reader/):
    trade:cache:news_reco_map = {
        "schema_ver": "news_reco_map_v1",
        "ts_ms": <int>,
        "producer": "news-analyzer",
        "reco": {
            "BTCUSDT": {
                "symbol": "BTCUSDT",
                "action": "block",
                "risk_factor_bps": 0,
                "grade_id": 5,
                "confidence": 0.86,
                "reason_code": "macro_high_impact_cpi",
                "source_event_id": "...",
                "sentiment": "risk_off",
                "asof_ts_ms": <int>,
                "expires_ms": <int>
            }
        }
    }
"""
from __future__ import annotations

import json
import logging
from typing import Any

from .classifier import ClassifyResult, risk_factor_bps_for_action

log = logging.getLogger(__name__)

RECO_MAP_KEY = "trade:cache:news_reco_map"
SCHEMA_VER   = "news_reco_map_v1"


def _compute_expires_ms(
    *,
    now_ts_ms: int,
    event_ts_ms: int,
    post_sec: int,
) -> int:
    """expires_ms = max(now + post_sec, event_ts_ms + post_sec)."""
    base = max(now_ts_ms, event_ts_ms) if event_ts_ms > 0 else now_ts_ms
    return int(base + post_sec * 1000)


def build_reco_entries(
    *,
    result: ClassifyResult,
    now_ts_ms: int,
    event_ts_ms: int = 0,
    source_event_id: str = "",
    confidence: float = 1.0,
    active_symbols: tuple[str, ...] | None = None,
) -> dict[str, dict[str, Any]]:
    """Return per-symbol reco dict (not yet merged with existing map).

    active_symbols: if provided, expands the '*crypto' sentinel to real symbols.
    """
    if not result.matched or result.grade_id == 0:
        return {}

    action = result.default_action
    rfb = risk_factor_bps_for_action(action, result.grade_id)
    expires_ms = _compute_expires_ms(
        now_ts_ms=now_ts_ms,
        event_ts_ms=event_ts_ms,
        post_sec=result.post_sec,
    )

    symbols: list[str] = []
    for sym in result.symbols:
        if sym == "*crypto":
            if active_symbols:
                symbols.extend(active_symbols)
        else:
            symbols.append(sym)

    if not symbols:
        return {}

    entry_base: dict[str, Any] = {
        "action": action,
        "risk_factor_bps": rfb,
        "grade_id": result.grade_id,
        "confidence": round(float(confidence), 4),
        "reason_code": result.reason_code,
        "source_event_id": source_event_id,
        "sentiment": result.sentiment,
        "asof_ts_ms": now_ts_ms,
        "expires_ms": expires_ms,
    }

    return {sym: {"symbol": sym, **entry_base} for sym in symbols}


def merge_reco_map(
    existing: dict[str, dict[str, Any]],
    new_entries: dict[str, dict[str, Any]],
    *,
    now_ts_ms: int,
) -> dict[str, dict[str, Any]]:
    """Merge new entries into existing map.

    Rules:
    - Drop expired entries from existing.
    - New entry wins if grade_id >= existing grade_id.
    - Never allow a lower-grade event to downgrade an active block.
    """
    merged: dict[str, dict[str, Any]] = {}

    # Carry forward non-expired existing
    for sym, entry in existing.items():
        if int(entry.get("expires_ms", 0)) > now_ts_ms:
            merged[sym] = entry

    # Apply new entries
    for sym, new_entry in new_entries.items():
        existing_entry = merged.get(sym)
        if existing_entry is None:
            merged[sym] = new_entry
            continue
        existing_grade = int(existing_entry.get("grade_id", 0))
        new_grade = int(new_entry.get("grade_id", 0))
        if new_grade >= existing_grade:
            merged[sym] = new_entry

    return merged


class RecoMapWriter:
    """Thread-safe (single-threaded) writer for trade:cache:news_reco_map."""

    def __init__(
        self,
        *,
        redis_client: Any,
        map_key: str = RECO_MAP_KEY,
        ttl_sec: int = 7200,
    ) -> None:
        self._r = redis_client
        self._map_key = map_key
        self._ttl_sec = int(ttl_sec)

    def _read_existing(self) -> dict[str, dict[str, Any]]:
        try:
            raw = self._r.get(self._map_key)
            if not raw:
                return {}
            obj = json.loads(raw)
            reco = obj.get("reco")
            if isinstance(reco, dict):
                return reco
        except Exception:
            pass
        return {}

    def apply(
        self,
        *,
        result: ClassifyResult,
        now_ts_ms: int,
        event_ts_ms: int = 0,
        source_event_id: str = "",
        confidence: float = 1.0,
        active_symbols: tuple[str, ...] | None = None,
    ) -> int:
        """Build, merge, and write reco map.

        Returns number of symbols written. 0 means no-op (grade=0 or Redis error).
        """
        new_entries = build_reco_entries(
            result=result,
            now_ts_ms=now_ts_ms,
            event_ts_ms=event_ts_ms,
            source_event_id=source_event_id,
            confidence=confidence,
            active_symbols=active_symbols,
        )
        if not new_entries:
            return 0

        existing = self._read_existing()
        merged = merge_reco_map(existing, new_entries, now_ts_ms=now_ts_ms)

        payload = {
            "schema_ver": SCHEMA_VER,
            "ts_ms": now_ts_ms,
            "producer": "news-analyzer",
            "reco": merged,
        }
        try:
            self._r.set(
                self._map_key,
                json.dumps(payload, ensure_ascii=False, separators=(",", ":")),
                ex=self._ttl_sec,
            )
            log.debug(
                "reco_map written symbols=%d event_type=%s grade=%d",
                len(new_entries),
                result.event_type,
                result.grade_id,
            )
        except Exception as exc:
            log.warning("reco_map write failed: %r", exc)
            return 0

        return len(new_entries)
