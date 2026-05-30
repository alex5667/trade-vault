from __future__ import annotations

"""tp1_hit_prob_reader.py — read-side adapter for tp1-phit publisher.

Loads `autocal:tp1_phit:state` (written by
`orderflow_services/tp1_hit_prob_publisher_v1.py`) and exposes a per-call
lookup that performs the (symbol, kind, regime, direction) → curve fallback
hierarchy *inside* the reader, so callers don't repeat that logic.

Design:
  - Disabled by default (`TP1_PHIT_READ_ENABLED=0`); fail-open.
  - TTL cache (30s default) — Redis GET on miss only.
  - HMAC verify if `TP1_PHIT_HMAC_SECRET` (or `RECS_HMAC_SECRET`/
    `LAYERS_CAL_HMAC_SECRET`) set; mismatch → ignore snapshot.
  - Per-bucket `passes` flag must be 1 to surface the curve to consumers.
  - `attach_tp1_phit_to_ctx(ctx, ...)` writes:
        ctx.tp1_hit_prob_by_rr   = {"0.65": 0.61, ...}
        ctx.tp1_prob_samples     = n_total
        ctx.tp1_calibration_ok   = 0|1
    fail-open: missing/disabled → ctx is not modified.

Used by `signals/level_enricher.py` to seed ctx before AdaptiveTP1Policy.
"""

import hashlib
import hmac
import json
import logging
import os
import threading
import time
from typing import Any

from core.tp1_hit_prob_cdf import lookup_phit_curve

logger = logging.getLogger(__name__)

_DEFAULT_KEY = "autocal:tp1_phit:state"
_DEFAULT_REFRESH_MS = 30_000
_DEFAULT_STALE_MS = 6 * 60 * 60 * 1000  # 6h — publisher window is 168h, slow-moving


def _env(k: str, d: str = "") -> str:
    return os.environ.get(k, d)


def _env_bool(k: str, d: bool) -> bool:
    raw = _env(k, "")
    if not raw:
        return d
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _env_int(k: str, d: int) -> int:
    try:
        return int(_env(k, str(d)))
    except Exception:
        return d


class Tp1PhitReader:
    """TTL-cached Redis snapshot reader. Thread-safe."""

    def __init__(
        self,
        redis_client: Any,
        *,
        redis_key: str = _DEFAULT_KEY,
        refresh_ms: int = _DEFAULT_REFRESH_MS,
        stale_ms: int = _DEFAULT_STALE_MS,
        hmac_secret: str = "",
    ) -> None:
        self._redis = redis_client
        self._key = redis_key
        self._refresh_ms = max(1000, int(refresh_ms))
        self._stale_ms = max(self._refresh_ms, int(stale_ms))
        self._hmac_secret = hmac_secret
        self._lock = threading.Lock()
        self._buckets: dict[str, dict[str, Any]] = {}
        self._snapshot_ts_ms: int = 0
        self._last_refresh_ms: int = 0

    def _maybe_refresh(self) -> None:
        now_ms = int(time.time() * 1000)
        if (now_ms - self._last_refresh_ms) < self._refresh_ms:
            return
        with self._lock:
            if (now_ms - self._last_refresh_ms) < self._refresh_ms:
                return
            self._last_refresh_ms = now_ms
            try:
                raw = self._redis.get(self._key)
                if not raw:
                    return
                if isinstance(raw, (bytes, bytearray)):
                    raw = raw.decode()
                data = json.loads(raw)
                if self._hmac_secret and "sig" in data:
                    expected_sig = data.pop("sig")
                    canon = json.dumps(
                        data, sort_keys=True, separators=(",", ":")
                    ).encode()
                    actual = hmac.new(
                        self._hmac_secret.encode(), canon, hashlib.sha256
                    ).hexdigest()
                    if not hmac.compare_digest(actual, str(expected_sig)):
                        logger.warning("tp1_phit reader: HMAC mismatch — ignoring")
                        return
                self._buckets = data.get("buckets") or {}
                self._snapshot_ts_ms = int(data.get("ts_ms") or 0)
            except Exception as e:
                logger.debug("tp1_phit reader: refresh fail (fail-open): %s", e)

    def _fresh(self) -> bool:
        if not self._buckets:
            return False
        age_ms = int(time.time() * 1000) - self._snapshot_ts_ms
        return age_ms <= self._stale_ms

    def get_bucket(
        self,
        *,
        symbol: str,
        kind: str,
        regime: str,
        direction: str,
        require_pass: bool = True,
    ) -> dict[str, Any] | None:
        """Return matching bucket dict via fallback hierarchy, or None."""
        self._maybe_refresh()
        if not self._fresh():
            return None
        return lookup_phit_curve(
            self._buckets,
            symbol=symbol,
            kind=kind,
            regime=regime,
            direction=direction,
            require_pass=require_pass,
        )


# Module singleton
_READER: Tp1PhitReader | None = None
_READER_LOCK = threading.Lock()


def _make_reader() -> Tp1PhitReader | None:
    if not _env_bool("TP1_PHIT_READ_ENABLED", False):
        return None
    try:
        import redis  # type: ignore

        url = _env(
            "TP1_PHIT_READ_REDIS_URL",
            _env("TP1_PHIT_REDIS_URL", _env("REDIS_URL", "redis://redis-worker-1:6379/0")),
        )
        client = redis.from_url(url, decode_responses=True)
        secret = (
            _env("TP1_PHIT_HMAC_SECRET", "")
            or _env("RECS_HMAC_SECRET", "")
            or _env("LAYERS_CAL_HMAC_SECRET", "")
        )
        return Tp1PhitReader(
            client,
            redis_key=_env("TP1_PHIT_STATE_KEY", _DEFAULT_KEY),
            refresh_ms=_env_int("TP1_PHIT_READ_REFRESH_MS", _DEFAULT_REFRESH_MS),
            stale_ms=_env_int("TP1_PHIT_READ_STALE_MS", _DEFAULT_STALE_MS),
            hmac_secret=secret,
        )
    except Exception as e:
        logger.debug("tp1_phit reader: init fail (fail-open): %s", e)
        return None


def get_reader() -> Tp1PhitReader | None:
    """Lazy singleton. Returns None when disabled or unavailable."""
    global _READER
    if _READER is not None:
        return _READER
    with _READER_LOCK:
        if _READER is None:
            _READER = _make_reader()
        return _READER


def reset_reader_for_tests() -> None:
    """Test-only: drop module singleton so a fresh reader is built on next access."""
    global _READER
    with _READER_LOCK:
        _READER = None


def attach_tp1_phit_to_ctx(
    ctx: Any,
    *,
    symbol: str,
    kind: str,
    regime: str,
    direction: str,
    reader: Tp1PhitReader | None = None,
) -> bool:
    """Populate ctx.tp1_hit_prob_by_rr / tp1_prob_samples / tp1_calibration_ok.

    Returns True iff ctx was modified. Fail-open: any error returns False
    without touching ctx (consumer treats missing fields as "skip_no_prob_curve").
    """
    try:
        rd = reader if reader is not None else get_reader()
        if rd is None:
            return False
        bucket = rd.get_bucket(
            symbol=symbol,
            kind=kind,
            regime=regime,
            direction=direction,
            require_pass=True,
        )
        if not bucket:
            return False
        curve = bucket.get("curve") or {}
        if not isinstance(curve, dict) or not curve:
            return False
        # Normalise key format to "X.XX" and validate values; reader is the
        # last guard before consumer.
        norm_curve: dict[str, float] = {}
        for k, v in curve.items():
            try:
                rr = float(k)
                p = float(v)
                if 0.0 <= p <= 1.0:
                    norm_curve[f"{rr:.2f}"] = p
            except Exception:
                continue
        if not norm_curve:
            return False
        try:
            ctx.tp1_hit_prob_by_rr = norm_curve
            ctx.tp1_prob_samples = int(bucket.get("n_total") or 0)
            ctx.tp1_calibration_ok = int(bucket.get("calibration_ok") or 0)
        except Exception:
            return False
        return True
    except Exception:
        return False
