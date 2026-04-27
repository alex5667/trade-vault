"""
Phase 2.1 — ATR Candidate Cache Writer.

Lightweight writer called from TA/aggregator path after ATR(tf) is recomputed.
Publishes to the canonical Redis key: ta:last:atr:{symbol}:{tf_label}

Usage (call from ohlc_aggregator / crypto_htf_aggregator after ATR update):

    from services.atr_candidate_cache_writer import publish_atr_candidate
    publish_atr_candidate(symbol="BTCUSDT", tf_label="1m", atr_value=250.0, ts_ms=now_ms)

tf_label values: 15s / 30s / 1m / 3m / 5m / 15m
"""
from __future__ import annotations

import json
import logging
import os
from typing import Optional

logger = logging.getLogger(__name__)

try:
    import redis as _redis_lib
except ImportError:  # pragma: no cover
    _redis_lib = None  # type: ignore

_R: Optional[object] = None
_REDIS_URL = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
_TTL_SEC = int(os.getenv("ATR_HORIZON_CANDIDATE_TTL_SEC", "86400") or 86400)
_ENABLED = os.getenv("ATR_HORIZON_CANDIDATE_WRITER_ENABLE", "1") == "1"

# Valid tf_label values — guard against accidental key pollution.
_VALID_TF_LABELS = frozenset({"15s", "30s", "1m", "3m", "5m", "15m"})


def _redis() -> Optional[object]:
    global _R
    if _redis_lib is None:
        return None
    if _R is None:
        try:
            _R = _redis_lib.Redis.from_url(_REDIS_URL, decode_responses=True)
        except Exception as exc:
            logger.debug("atr_candidate_cache_writer: Redis connect failed: %s", exc)
            _R = None
    return _R


def publish_atr_candidate(
    symbol: str,
    tf_label: str,
    atr_value: float,
    ts_ms: int,
) -> bool:
    """
    Write ATR candidate into Redis TA cache after each candle close.

    Returns True on success, False on any failure (fail-open, never raises).

    Args:
        symbol:    Normalised symbol e.g. "BTCUSDT".
        tf_label:  Canonical label e.g. "1m", "5m", "15m".
        atr_value: ATR value > 0.
        ts_ms:     Candle close timestamp in epoch milliseconds.
    """
    if not _ENABLED:
        return False
    try:
        symbol = str(symbol or "").upper()
        tf_label = str(tf_label or "").lower().strip()
        if not symbol or tf_label not in _VALID_TF_LABELS:
            return False
        atr_value = float(atr_value)
        if not (atr_value > 0.0):
            return False
        ts_ms = int(ts_ms)
        r = _redis()
        if r is None:
            return False
        payload = {
            "v": 1,
            "symbol": symbol,
            "tf": tf_label,
            "atr": atr_value,
            "ts_ms": ts_ms,
        }
        key = f"ta:last:atr:{symbol}:{tf_label}"
        getattr(r, "set")(
            key,
            json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True),
            ex=_TTL_SEC,
        )
        return True
    except Exception as exc:
        logger.debug("atr_candidate_cache_writer: publish failed: %s", exc)
        return False
