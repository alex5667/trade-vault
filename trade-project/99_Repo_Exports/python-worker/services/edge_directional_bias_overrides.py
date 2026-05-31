from __future__ import annotations

"""edge_directional_bias_overrides.py — read-side adapter for the EDB autocal.

Loads `autocal:edge_directional_bias:state` (written by
`orderflow_services/edge_directional_bias_autocal_v1`) and exposes per-bucket
bias overrides for EdgeCostGate's `_apply_directional_bias`.

Design:
  - Disabled by default (`AUTOCAL_EDGE_DIRECTIONAL_BIAS_READ_ENABLED=0`).
  - TTL cache (60s default) — Redis GET on miss only. Thread-safe.
  - HMAC verify if a secret is configured; mismatch → snapshot ignored.
  - Fail-open everywhere: any error → returns None and the caller falls
    back to ENV-driven static bias values.

Public surface (module-level):
  get_bias_override(direction, regime, countertrend) -> float | None
    Returns the bias value the autocalibrator advises for this cell.
    None means "no override" — caller must fall back to ENV.
"""

import hashlib
import hmac
import json
import logging
import os
import threading
import time
from typing import Any

logger = logging.getLogger(__name__)

# Reader-side observability — pairs with the autocal's
# `edb_ac_bucket_bias_value` gauge to detect "autocal published a bias but
# hot path never applied it" (the audit's failure mode for the loop being
# broken). Counter results: hit | miss | stale | disabled | not_countertrend
# | unsupported_direction | bucket_missing | observe_phase | error.
try:
    from prometheus_client import Counter, Gauge  # type: ignore

    _override_read_total = Counter(
        "edge_directional_bias_override_read_total",
        "EDB override reads by hot path",
        ["result"],
    )
    _override_value_gauge = Gauge(
        "edge_directional_bias_override_value",
        "Last bias value applied per (direction, regime)",
        ["direction", "regime"],
    )
except Exception:  # boundary fail-open: missing client lib must not crash hot path
    Counter = Gauge = None
    _override_read_total = None
    _override_value_gauge = None


def _emit_read_result(result: str) -> None:
    if _override_read_total is not None:
        try:
            _override_read_total.labels(result=result).inc()
        except Exception:
            pass


def _emit_value_gauge(direction: str, regime: str, value: float) -> None:
    if _override_value_gauge is not None:
        try:
            _override_value_gauge.labels(
                direction=(direction or "")[:8],
                regime=(regime or "")[:24],
            ).set(value)
        except Exception:
            pass

_DEFAULT_KEY = "autocal:edge_directional_bias:state"
_DEFAULT_REFRESH_MS = 60_000
_DEFAULT_STALE_MS = 6 * 60 * 60 * 1000  # 6 hours

_REGIME_ALIASES = {
    "uptrend": "trending_bull",
    "trending_up": "trending_bull",
    "trending": "trending_bull",
    "downtrend": "trending_bear",
    "trending_down": "trending_bear",
    "mixed": "range",
}


def _env(k: str, d: str = "") -> str:
    return os.environ.get(k, d)


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


def _normalize_regime(raw: str) -> str:
    s = (raw or "").strip().lower()
    if s in {"", "na", "unknown", "none"}:
        return ""
    return _REGIME_ALIASES.get(s, s)


class EdgeDirectionalBiasReader:
    """TTL-cached Redis snapshot reader. Thread-safe, fail-open."""

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
        self._refresh_ms = max(1000, refresh_ms)
        self._stale_ms = max(self._refresh_ms, stale_ms)
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
                        logger.warning(
                            "edge_directional_bias overrides: HMAC mismatch — ignoring"
                        )
                        return
                self._buckets = data.get("buckets") or {}
                self._snapshot_ts_ms = int(data.get("ts_ms") or 0)
            except Exception as e:
                logger.debug(
                    "edge_directional_bias overrides: refresh fail (fail-open): %s", e
                )

    def _fresh(self) -> bool:
        if not self._buckets:
            return False
        if not self._snapshot_ts_ms:
            return False
        age_ms = int(time.time() * 1000) - self._snapshot_ts_ms
        return age_ms <= self._stale_ms

    def get_bias_override(
        self,
        *,
        direction: str,
        regime: str,
        countertrend: bool,
    ) -> float | None:
        """Return calibrator-advised bias for (direction × regime).

        Returns None when:
          - countertrend=False (autocal only covers counter-trend cells)
          - snapshot stale / missing
          - bucket not tracked
          - phase is OBSERVE (no override needed — equals ENV default)

        On phase=ROLLED_BACK explicitly returns 0.0 so the calibrator can
        override an otherwise non-zero ENV default to neutralise the cell.
        """
        if not countertrend:
            _emit_read_result("not_countertrend")
            return None
        self._maybe_refresh()
        if not self._buckets:
            _emit_read_result("miss")
            return None
        if not self._fresh():
            _emit_read_result("stale")
            return None
        d = (direction or "").strip().upper()
        if d not in ("LONG", "SHORT"):
            _emit_read_result("unsupported_direction")
            return None
        r = _normalize_regime(regime)
        if not r:
            _emit_read_result("miss")
            return None
        key = f"{d}|{r}"
        b = self._buckets.get(key)
        if not isinstance(b, dict):
            _emit_read_result("bucket_missing")
            return None
        phase = str(b.get("phase") or "OBSERVE")
        if phase == "OBSERVE":
            _emit_read_result("observe_phase")
            return None
        try:
            v = float(b.get("bias_value") or 0.0)
            import math as _m

            if not _m.isfinite(v):
                _emit_read_result("error")
                return None
            # Clamp to safe range — autocal should never produce >0.20, but
            # defence-in-depth costs nothing.
            clamped = max(0.0, min(0.20, v))
            _emit_read_result("hit")
            _emit_value_gauge(d, r, clamped)
            return clamped
        except Exception:
            _emit_read_result("error")
            return None

    def get_snapshot_meta(self) -> dict[str, Any]:
        """Inspection helper: ts/age/sizes for shadow-mode metrics."""
        self._maybe_refresh()
        now_ms = int(time.time() * 1000)
        return {
            "ts_ms": self._snapshot_ts_ms,
            "age_ms": (now_ms - self._snapshot_ts_ms) if self._snapshot_ts_ms else None,
            "buckets_n": len(self._buckets),
            "fresh": self._fresh(),
        }


_READER: EdgeDirectionalBiasReader | None = None
_READER_LOCK = threading.Lock()


def _make_reader() -> EdgeDirectionalBiasReader | None:
    if not _env_bool("AUTOCAL_EDGE_DIRECTIONAL_BIAS_READ_ENABLED", False):
        return None
    try:
        import redis  # type: ignore

        url = _env(
            "AUTOCAL_EDGE_DIRECTIONAL_BIAS_REDIS_URL",
            _env("REDIS_URL", "redis://redis-worker-1:6379/0"),
        )
        client = redis.from_url(url, decode_responses=True)
        secret = (
            _env("EDB_AC_HMAC_SECRET", "")
            or _env("RECS_HMAC_SECRET", "")
            or _env("LAYERS_CAL_HMAC_SECRET", "")
        )
        return EdgeDirectionalBiasReader(
            client,
            redis_key=_env("AUTOCAL_EDGE_DIRECTIONAL_BIAS_KEY", _DEFAULT_KEY),
            refresh_ms=_env_int(
                "AUTOCAL_EDGE_DIRECTIONAL_BIAS_REFRESH_MS", _DEFAULT_REFRESH_MS
            ),
            stale_ms=_env_int(
                "AUTOCAL_EDGE_DIRECTIONAL_BIAS_STALE_MS", _DEFAULT_STALE_MS
            ),
            hmac_secret=secret,
        )
    except Exception as e:
        logger.debug(
            "edge_directional_bias overrides: reader init fail (fail-open): %s", e
        )
        return None


def get_reader() -> EdgeDirectionalBiasReader | None:
    """Lazy singleton. None when disabled or unavailable."""
    global _READER
    if _READER is not None:
        return _READER
    with _READER_LOCK:
        if _READER is None:
            _READER = _make_reader()
        return _READER


def reset_reader_for_tests() -> None:
    """Test helper: force re-init on next get_reader() call."""
    global _READER
    with _READER_LOCK:
        _READER = None


def get_bias_override(
    *,
    direction: str,
    regime: str,
    countertrend: bool,
) -> float | None:
    """Module-level convenience accessor. Returns None on any failure."""
    r = get_reader()
    if r is None:
        _emit_read_result("disabled")
        return None
    try:
        return r.get_bias_override(
            direction=direction, regime=regime, countertrend=countertrend
        )
    except Exception:
        _emit_read_result("error")
        return None
