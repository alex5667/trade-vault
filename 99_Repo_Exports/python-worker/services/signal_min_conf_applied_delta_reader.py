"""signal_min_conf_applied_delta_reader.py — reads cfg:gate_value_autocal:applied:*.

Per-group min_conf delta override, written by `gate_value_autocalibrator_v1`
when its phase reaches RELAX_APPLIED and the rollout governor has flipped
`cfg:gva:enforce=1`. The delta is applied AFTER all other min_conf
calculations (ENV, per-symbol, signal_min_conf autocal reader) and clamped
to the floor/ceiling from the same payload.

Each key is a separate Redis STRING:
  cfg:gate_value_autocal:applied:{kind}|{symbol}|{horizon_ms}
  → {schema_version, ts_ms, group_key, phase, min_conf_delta,
     min_conf_floor, min_conf_ceiling, reason, llm_summary, sig?}

Caching strategy: TTL cache per-group_key, default 60s refresh, 600s stale.
Wildcards: NO. Each (kind, symbol, horizon_ms) is queried explicitly. If a
key is missing the reader returns None — caller falls back to base min_conf.

Disabled by default (AUTOCAL_APPLIED_DELTA_READ_ENABLED=0). Fail-open.
HMAC verification optional (GVA_HMAC_SECRET / RECS_HMAC_SECRET).
"""

from __future__ import annotations

import hashlib
import hmac
import json
import logging
import os
import threading
import time
from dataclasses import dataclass
from typing import Any

logger = logging.getLogger(__name__)

_DEFAULT_REFRESH_MS = 60_000
_DEFAULT_STALE_MS = 10 * 60 * 1000
_DEFAULT_KEY_PREFIX = "cfg:gate_value_autocal:applied"


def _env(k: str, d: str = "") -> str:
    return os.environ.get(k, d) or d


def _env_bool(k: str, d: bool) -> bool:
    raw = _env(k, "")
    if not raw:
        return d
    return raw.strip().lower() in ("1", "true", "yes", "on")


def _env_int(k: str, d: int) -> int:
    try:
        return int(_env(k, str(d)))
    except Exception:
        return d


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        import math
        f = float(v)
        return f if math.isfinite(f) else default
    except (TypeError, ValueError):
        return default


@dataclass(frozen=True)
class AppliedDelta:
    """Resolved per-group delta. Floor/ceiling are absolute min_conf bounds (0..1)."""
    min_conf_delta: float
    min_conf_floor: float
    min_conf_ceiling: float
    phase: str
    ts_ms: int


@dataclass
class _CacheEntry:
    delta: AppliedDelta | None  # None = "key absent at last refresh"
    fetched_ms: int


class AppliedDeltaReader:
    """TTL-cached reader for cfg:gate_value_autocal:applied:{group_key}.

    Thread-safe. Returns None for missing/stale/HMAC-fail entries — caller
    keeps the base min_conf.
    """

    def __init__(
        self,
        redis_client: Any,
        *,
        key_prefix: str = _DEFAULT_KEY_PREFIX,
        refresh_ms: int = _DEFAULT_REFRESH_MS,
        stale_ms: int = _DEFAULT_STALE_MS,
        hmac_secret: str = "",
    ) -> None:
        self._redis = redis_client
        self._prefix = key_prefix
        self._refresh_ms = max(5_000, refresh_ms)
        self._stale_ms = max(self._refresh_ms, stale_ms)
        self._hmac_secret = hmac_secret
        self._lock = threading.Lock()
        self._cache: dict[str, _CacheEntry] = {}

    @staticmethod
    def _build_group_key(kind: str, symbol: str, horizon_ms: int) -> str:
        k = (kind or "").strip()
        s = (symbol or "").strip().upper()
        try:
            h = int(horizon_ms or 0)
        except (TypeError, ValueError):
            h = 0
        if not k or not s:
            return ""
        return f"{k}|{s}|{h}"

    def _redis_key(self, group_key: str) -> str:
        return f"{self._prefix}:{group_key}"

    def _verify_hmac(self, payload: dict[str, Any]) -> bool:
        if not self._hmac_secret:
            return True
        sig = payload.get("sig")
        if not sig:
            # HMAC required but missing — fail-closed for this entry.
            return False
        body = {k: v for k, v in payload.items() if k != "sig"}
        data = json.dumps(body, sort_keys=True, separators=(",", ":")).encode("utf-8")
        expected = hmac.new(
            self._hmac_secret.encode("utf-8"), data, hashlib.sha256
        ).hexdigest()
        return hmac.compare_digest(expected, str(sig))

    def _parse_payload(self, raw: Any) -> AppliedDelta | None:
        if not raw:
            return None
        try:
            if isinstance(raw, bytes):
                raw = raw.decode("utf-8")
            payload = json.loads(raw)
        except Exception:
            return None
        if not isinstance(payload, dict):
            return None
        if not self._verify_hmac(payload):
            logger.debug("applied_delta HMAC mismatch — ignored")
            return None
        delta = _safe_float(payload.get("min_conf_delta"), 0.0)
        floor = _safe_float(payload.get("min_conf_floor"), 0.30)
        ceiling = _safe_float(payload.get("min_conf_ceiling"), 0.85)
        phase = str(payload.get("phase") or "")
        ts_ms = int(payload.get("ts_ms") or 0)
        # Sanity: floor/ceiling must form a valid range; delta within plausible bounds.
        if not (0.0 <= floor < ceiling <= 1.0):
            return None
        if abs(delta) > 0.5:  # paranoid cap; autocal max step is ~0.05
            return None
        return AppliedDelta(
            min_conf_delta=delta,
            min_conf_floor=floor,
            min_conf_ceiling=ceiling,
            phase=phase,
            ts_ms=ts_ms,
        )

    def _refresh_one(self, group_key: str, *, now_ms: int) -> _CacheEntry:
        try:
            raw = self._redis.get(self._redis_key(group_key))
        except Exception as e:
            logger.debug("applied_delta redis get failed for %s: %s", group_key, e)
            return _CacheEntry(delta=None, fetched_ms=now_ms)
        return _CacheEntry(delta=self._parse_payload(raw), fetched_ms=now_ms)

    def get_delta(
        self,
        *,
        kind: str,
        symbol: str,
        horizon_ms: int,
    ) -> AppliedDelta | None:
        """Return the applied delta or None (no override).

        None means: no key, parse error, HMAC mismatch, stale entry, or
        delta == 0. Caller should apply NO change to base min_conf.
        """
        group_key = self._build_group_key(kind, symbol, horizon_ms)
        if not group_key:
            return None
        now_ms = int(time.time() * 1000)

        entry = self._cache.get(group_key)
        if entry is None or (now_ms - entry.fetched_ms) >= self._refresh_ms:
            with self._lock:
                entry = self._cache.get(group_key)
                if entry is None or (now_ms - entry.fetched_ms) >= self._refresh_ms:
                    entry = self._refresh_one(group_key, now_ms=now_ms)
                    self._cache[group_key] = entry

        delta = entry.delta
        if delta is None:
            return None

        # Stale guard: payload.ts_ms much older than stale_ms → drop
        if delta.ts_ms > 0:
            age_ms = now_ms - delta.ts_ms
            if age_ms > self._stale_ms:
                return None

        if delta.min_conf_delta == 0.0:
            return None
        return delta


# ── Module singleton ────────────────────────────────────────────────────────

_READER: AppliedDeltaReader | None = None
_READER_LOCK = threading.Lock()


def _make_reader() -> AppliedDeltaReader | None:
    if not _env_bool("AUTOCAL_APPLIED_DELTA_READ_ENABLED", False):
        return None
    try:
        import redis  # type: ignore
        url = _env(
            "AUTOCAL_APPLIED_DELTA_REDIS_URL",
            _env("REDIS_URL", "redis://redis-worker-1:6379/0"),
        )
        prefix = _env("AUTOCAL_APPLIED_DELTA_KEY_PREFIX", _DEFAULT_KEY_PREFIX)
        secret = (
            _env("GVA_HMAC_SECRET", "")
            or _env("RECS_HMAC_SECRET", "")
            or _env("LAYERS_CAL_HMAC_SECRET", "")
        )
        client = redis.from_url(url, decode_responses=False)
        return AppliedDeltaReader(
            client,
            key_prefix=prefix,
            refresh_ms=_env_int("AUTOCAL_APPLIED_DELTA_REFRESH_MS", _DEFAULT_REFRESH_MS),
            stale_ms=_env_int("AUTOCAL_APPLIED_DELTA_STALE_MS", _DEFAULT_STALE_MS),
            hmac_secret=secret,
        )
    except Exception as e:
        logger.debug("applied_delta reader init fail: %s", e)
        return None


def get_reader() -> AppliedDeltaReader | None:
    global _READER
    if _READER is not None:
        return _READER
    with _READER_LOCK:
        if _READER is None:
            _READER = _make_reader()
        return _READER
