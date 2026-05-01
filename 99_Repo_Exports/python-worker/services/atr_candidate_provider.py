from __future__ import annotations
"""
Phase 2.1 — ATR Candidate Provider.

Collects multi-TF ATR candidates from:
  1. signal[indicators]     (fastest, no I/O)
  2. signal / signal[meta]  (payload fallback)
  3. Redis TA cache (MGET batch, canonical key: ta:last:atr:{symbol}:{tf_label})
  4. Redis fallback key:     atr:json:{symbol}:{tf_label}

Returns a normalised {tf_ms: dict} map suitable for atr_runtime_selector.

Design contracts:
  - Fail-open on every step; no exception propagates upward.
  - MGET batch = single round-trip for all allowed TFs.
  - Freshness enforced by max_age_ms per candidate.
  - Canonical tf_label: 15s / 30s / 1m / 3m / 5m / 15m.
"""

import json
import math
import os
import time
from dataclasses import dataclass, asdict
from typing import Any, Dict, List, Optional, Tuple

try:
    import redis as _redis_lib
except ImportError:  # pragma: no cover
    _redis_lib = None  # type: ignore

try:
    from prometheus_client import Counter, Histogram
except Exception:  # pragma: no cover
    Counter = None  # type: ignore
    Histogram = None  # type: ignore


# ---------------------------------------------------------------------------
# Prometheus metrics
# ---------------------------------------------------------------------------

_M_COLLECT_TOTAL = Counter(
    "trade_atr_candidate_collect_total",
    "ATR candidate provider: candidates collected",
    ["source"],
) if Counter is not None else None

_M_MISSING_TOTAL = Counter(
    "trade_atr_candidate_collect_missing_total",
    "ATR candidate provider: TF missing after collect",
    ["tf_ms"],
) if Counter is not None else None

_M_AGE_HIST = Histogram(
    "trade_atr_candidate_age_ms_hist",
    "ATR candidate age_ms at collection time",
    ["tf_ms", "source"],
    buckets=[0, 1_000, 5_000, 15_000, 30_000, 60_000, 120_000, 300_000, 900_000],
) if Histogram is not None else None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _safe_int(v: Any, default: int = 0) -> int:
    try:
        return int(v)
    except Exception:
        return default


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        x = float(v)
        return x if math.isfinite(x) else default
    except Exception:
        return default


def _ensure_dict(v: Any) -> Dict[str, Any]:
    return dict(v) if isinstance(v, dict) else {}


def _parse_allowed_tfs() -> List[int]:
    raw = str(
        os.getenv("ATR_HORIZON_ALLOWED_TFS_MS", "15000,30000,60000,180000,300000,900000") or ""
    ).strip()
    out: List[int] = []
    for p in raw.split(","):
        try:
            x = int(p.strip())
            if x > 0:
                out.append(x)
        except Exception:
            pass
    return sorted(set(out)) or [60000]


# Canonical label map: tf_ms → Redis tf_label
_TF_ALIAS: Dict[int, str] = {
    15000: "15s",
    30000: "30s",
    60000: "1m",
    180000: "3m",
    300000: "5m",
    900000: "15m",
}


def _tf_alias(tf_ms: int) -> str:
    return _TF_ALIAS.get(tf_ms, str(tf_ms))


# ---------------------------------------------------------------------------
# Candidate dataclass
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class ATRCandidate:
    tf_ms: int
    value: float
    ts_ms: int
    age_ms: int
    source: str  # "indicators"|"payload"|"redis_ta_last"|"redis_atr_json"|"legacy"


# ---------------------------------------------------------------------------
# Provider
# ---------------------------------------------------------------------------

class ATRCandidateProvider:
    """
    Collects ATR candidates from all available sources and normalises them
    into a {tf_ms: ATRCandidate_dict} map.

    Priority (first hit wins per TF):
      1. signal["indicators"]   — in-process TA result
      2. signal / signal["meta"] — payload keys
      3. Redis TA cache          — ta:last:atr:{symbol}:{tf_label}
      4. Redis fallback          — atr:json:{symbol}:{tf_label}
    """

    def __init__(self, redis_url: Optional[str] = None) -> None:
        self.redis_url: str = redis_url or os.getenv(
            "REDIS_URL", "redis://redis-worker-1:6379/0"
        )
        self.max_age_ms: int = _safe_int(
            os.getenv("ATR_HORIZON_CANDIDATE_MAX_AGE_MS", "300000"), 300000
        )
        self.redis_enable: bool = os.getenv("ATR_HORIZON_CANDIDATE_REDIS_ENABLE", "1") == "1"
        self.redis_mget_enable: bool = os.getenv("ATR_HORIZON_CANDIDATE_MGET_ENABLE", "1") == "1"
        self.allowed_tfs: List[int] = _parse_allowed_tfs()
        self._r: Optional[Any] = None  # Redis client (lazy)

    # ------------------------------------------------------------------
    # Redis access
    # ------------------------------------------------------------------

    def _redis(self) -> Optional[Any]:
        if not self.redis_enable or _redis_lib is None:
            return None
        if self._r is None:
            try:
                self._r = _redis_lib.Redis.from_url(self.redis_url, decode_responses=True)
            except Exception:
                self._r = None
        return self._r

    # ------------------------------------------------------------------
    # Source 1: indicators (inprocess)
    # ------------------------------------------------------------------

    def _from_indicators(
        self, tf_ms: int, indicators: Dict[str, Any], now_ms: int
    ) -> Optional[ATRCandidate]:
        alias = _tf_alias(tf_ms)
        # Value key candidates (first non-zero wins)
        val_keys = [
            f"atr_{alias}", f"atr_tf_{alias}",
            f"atr_{tf_ms}", f"atr_tf_{tf_ms}",
        ]
        value = 0.0
        for k in val_keys:
            if k in indicators:
                v = _safe_float(indicators[k], 0.0)
                if v > 0.0:
                    value = v
                    break
        if value <= 0.0:
            return None
        # Timestamp key candidates
        ts_keys = [
            f"atr_ts_ms_{alias}", f"atr_tf_ts_ms_{alias}",
            f"atr_ts_ms_{tf_ms}", f"atr_tf_ts_ms_{tf_ms}",
        ]
        ts_ms = 0
        for k in ts_keys:
            if k in indicators:
                x = _safe_int(indicators[k], 0)
                if x > 0:
                    ts_ms = x
                    break
        if ts_ms <= 0:
            ts_ms = now_ms
        return ATRCandidate(
            tf_ms=tf_ms,
            value=value,
            ts_ms=ts_ms,
            age_ms=max(0, now_ms - ts_ms),
            source="indicators",
        )

    # ------------------------------------------------------------------
    # Source 2: payload / meta (same key patterns, different dicts)
    # ------------------------------------------------------------------

    def _from_payload(
        self, tf_ms: int, signal: Dict[str, Any], now_ms: int
    ) -> Optional[ATRCandidate]:
        meta = _ensure_dict(signal.get("meta"))
        # Search signal, meta, signal[indicators] (already checked as source 1)
        for d in (signal, meta):
            c = self._from_indicators(tf_ms, d, now_ms)
            if c is not None:
                return ATRCandidate(
                    tf_ms=c.tf_ms, value=c.value,
                    ts_ms=c.ts_ms, age_ms=c.age_ms,
                    source="payload",
                )
        return None

    # ------------------------------------------------------------------
    # Source 3+4: Redis (single MGET batch)
    # ------------------------------------------------------------------

    def _redis_key_pair(self, symbol: str, tf_ms: int) -> Tuple[str, str]:
        label = _tf_alias(tf_ms)
        return (
            f"ta:last:atr:{symbol}:{label}",   # primary
            f"atr:json:{symbol}:{label}",       # fallback
        )

    def _from_redis_batch(self, symbol: str, now_ms: int) -> Dict[int, ATRCandidate]:
        """One MGET call covering primary + fallback keys for all allowed TFs."""
        r = self._redis()
        if r is None:
            return {}

        symbol = symbol.upper()
        # Build ordered key list: primary key first, then fallback
        keys: List[str] = []
        key_meta: List[Tuple[int, str]] = []  # (tf_ms, raw_key)
        for tf in self.allowed_tfs:
            pk, fk = self._redis_key_pair(symbol, tf)
            keys.append(pk)
            key_meta.append((tf, pk))
            keys.append(fk)
            key_meta.append((tf, fk))

        results: Dict[int, ATRCandidate] = {}

        if self.redis_mget_enable:
            try:
                vals = r.mget(keys)
                # Accumulate: first hit per TF wins (primary before fallback)
                for (tf, key), raw in zip(key_meta, vals):
                    if tf in results or not raw:
                        continue
                    try:
                        obj = json.loads(raw)
                        atr = _safe_float(obj.get("atr"), 0.0)
                        ts = _safe_int(obj.get("ts_ms"), 0)
                        if atr > 0.0 and ts > 0:
                            src = "redis_ta_last" if key.startswith("ta:last:atr:") else "redis_atr_json"
                            results[tf] = ATRCandidate(
                                tf_ms=tf,
                                value=atr,
                                ts_ms=ts,
                                age_ms=max(0, now_ms - ts),
                                source=src,
                            )
                    except Exception:
                        continue
                return results
            except Exception:
                pass  # fall through to per-key GET

        # Fallback: individual GET per key pair (only on MGET failure)
        for tf in self.allowed_tfs:
            if tf in results:
                continue
            pk, fk = self._redis_key_pair(symbol, tf)
            for key in (pk, fk):
                try:
                    raw = r.get(key)
                    if not raw:
                        continue
                    obj = json.loads(raw)
                    atr = _safe_float(obj.get("atr"), 0.0)
                    ts = _safe_int(obj.get("ts_ms"), 0)
                    if atr > 0.0 and ts > 0:
                        src = "redis_ta_last" if key.startswith("ta:last:atr:") else "redis_atr_json"
                        results[tf] = ATRCandidate(
                            tf_ms=tf, value=atr, ts_ms=ts,
                            age_ms=max(0, now_ms - ts), source=src,
                        )
                        break
                except Exception:
                    continue
        return results

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def collect(
        self,
        *,
        signal: Dict[str, Any],
        symbol: str,
        now_ms: int,
    ) -> Dict[int, Dict[str, Any]]:
        """
        Collect multi-TF ATR candidates.

        Returns:
            {tf_ms: asdict(ATRCandidate)} — sorted by tf_ms, freshness-filtered.
        """
        signal = _ensure_dict(signal)
        indicators = _ensure_dict(signal.get("indicators"))

        out: Dict[int, ATRCandidate] = {}

        # 1. indicators (fastest, no I/O)
        for tf in self.allowed_tfs:
            c = self._from_indicators(tf, indicators, now_ms)
            if c and c.value > 0.0 and c.age_ms <= self.max_age_ms:
                out[tf] = c

        # 2. payload / meta (no I/O)
        for tf in self.allowed_tfs:
            if tf in out:
                continue
            c = self._from_payload(tf, signal, now_ms)
            if c and c.value > 0.0 and c.age_ms <= self.max_age_ms:
                out[tf] = c

        # 3+4. Redis (single MGET batch)
        try:
            redis_map = self._from_redis_batch(symbol=str(symbol or "").upper(), now_ms=now_ms)
            for tf, c in redis_map.items():
                if tf not in out and c.value > 0.0 and c.age_ms <= self.max_age_ms:
                    out[tf] = c
        except Exception:
            pass

        # Emit metrics per candidate
        for tf, c in out.items():
            _emit_collect_metrics(tf, c)

        # Emit missing metrics for TFs without a candidate
        for tf in self.allowed_tfs:
            if tf not in out:
                if _M_MISSING_TOTAL is not None:
                    try:
                        _M_MISSING_TOTAL.labels(tf_ms=str(tf)).inc()
                    except Exception:
                        pass

        return {
            tf: asdict(c)
            for tf, c in sorted(out.items(), key=lambda x: x[0])
        }


def _emit_collect_metrics(tf: int, c: ATRCandidate) -> None:
    if _M_COLLECT_TOTAL is not None:
        try:
            _M_COLLECT_TOTAL.labels(source=c.source).inc()
        except Exception:
            pass
    if _M_AGE_HIST is not None:
        try:
            _M_AGE_HIST.labels(tf_ms=str(tf), source=c.source).observe(c.age_ms)
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Singleton accessor
# ---------------------------------------------------------------------------

_PROVIDER: Optional[ATRCandidateProvider] = None


def get_atr_candidate_provider() -> ATRCandidateProvider:
    global _PROVIDER
    if _PROVIDER is None:
        _PROVIDER = ATRCandidateProvider()
    return _PROVIDER
