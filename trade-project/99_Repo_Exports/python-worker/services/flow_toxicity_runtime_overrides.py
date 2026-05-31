from __future__ import annotations

"""flow_toxicity_runtime_overrides.py — read-side adapter for flow toxicity autocalibrator.

Loads HGETALL `autocal:flow_toxicity:state` (written by
`orderflow_services/flow_toxicity_calibrator_v1.py`) with TTL cache and exposes
`get_thresholds(symbol)` returning (thr_z, thr_vpin) for use inside
signal_pipeline.check_flow_toxicity call.

Design:
  - Disabled by default (`AUTOCAL_FLOW_TOX_READ_ENABLED=0`); fail-open → (0.0, 0.0).
  - TTL cache (30s default) — Redis HGETALL on miss only.
  - Stale guard: if snapshot older than STALE_MS, returns (0.0, 0.0) fail-open.
  - Per-symbol committed_z / committed_vpin from dump_state format (v1).
"""

import json
import logging
import os
import threading
import time
from typing import Any

logger = logging.getLogger(__name__)

_DEFAULT_KEY = "autocal:flow_toxicity:state"
_DEFAULT_REFRESH_MS = 30_000
_DEFAULT_STALE_MS = 10 * 60 * 1000  # 10 min


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


class FlowToxicityThresholdReader:
    """TTL-cached per-symbol threshold reader. Thread-safe."""

    def __init__(
        self,
        redis_client: Any,
        *,
        redis_key: str = _DEFAULT_KEY,
        refresh_ms: int = _DEFAULT_REFRESH_MS,
        stale_ms: int = _DEFAULT_STALE_MS,
    ) -> None:
        self._redis = redis_client
        self._key = redis_key
        self._refresh_ms = max(1000, refresh_ms)
        self._stale_ms = max(self._refresh_ms, stale_ms)
        self._lock = threading.Lock()
        # {symbol_upper: {"committed_z": float, "committed_vpin": float, "n": int, ...}}
        self._snapshot: dict[str, dict[str, Any]] = {}
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
                raw = self._redis.hgetall(self._key)
                if not raw:
                    return
                parsed: dict[str, dict[str, Any]] = {}
                for sym_raw, state_raw in raw.items():
                    try:
                        sym = sym_raw.decode("utf-8") if isinstance(sym_raw, bytes) else str(sym_raw)
                        sym = sym.strip().upper()
                        state_str = state_raw.decode("utf-8") if isinstance(state_raw, bytes) else str(state_raw)
                        state = json.loads(state_str)
                        if isinstance(state, dict):
                            parsed[sym] = state
                    except Exception:
                        pass
                self._snapshot = parsed
            except Exception as e:
                logger.debug("flow_tox overrides: refresh fail (fail-open): %s", e)

    def get_thresholds(self, symbol: str) -> tuple[float, float]:
        """Return (thr_z, thr_vpin) for symbol; (0.0, 0.0) if cold/stale/disabled.

        thr_z   — ofi_norm_z p95 committed threshold (0.0 = gate disabled)
        thr_vpin — vpin_cdf p95 committed threshold   (0.0 = gate disabled)
        """
        self._maybe_refresh()
        sym = (symbol or "").strip().upper()
        state = self._snapshot.get(sym)
        if not state:
            return 0.0, 0.0

        # Staleness guard
        updated_ms = int(state.get("updated_ts_ms") or 0)
        if updated_ms > 0:
            age_ms = int(time.time() * 1000) - updated_ms
            if age_ms > self._stale_ms:
                return 0.0, 0.0

        thr_z = _safe_float(state.get("committed_z"), 0.0)
        thr_vpin = _safe_float(state.get("committed_vpin"), 0.0)
        return thr_z, thr_vpin

    def n(self, symbol: str) -> int:
        """Число наблюдений для символа из последнего снапшота."""
        self._maybe_refresh()
        sym = (symbol or "").strip().upper()
        state = self._snapshot.get(sym)
        if not state:
            return 0
        return int(state.get("n") or 0)


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        if v is None:
            return default
        f = float(v)
        import math
        return f if math.isfinite(f) else default
    except (TypeError, ValueError):
        return default


# ── Module singleton ──────────────────────────────────────────────────────────

_READER: FlowToxicityThresholdReader | None = None
_READER_LOCK = threading.Lock()


def _make_reader() -> FlowToxicityThresholdReader | None:
    if not _env_bool("AUTOCAL_FLOW_TOX_READ_ENABLED", False):
        return None
    try:
        import redis  # type: ignore
        url = _env(
            "AUTOCAL_FLOW_TOX_REDIS_URL",
            _env("REDIS_URL", "redis://redis-worker-1:6379/0"),
        )
        client = redis.from_url(url, decode_responses=False)
        return FlowToxicityThresholdReader(
            client,
            redis_key=_env("AUTOCAL_FLOW_TOX_KEY", _DEFAULT_KEY),
            refresh_ms=_env_int("AUTOCAL_FLOW_TOX_REFRESH_MS", _DEFAULT_REFRESH_MS),
            stale_ms=_env_int("AUTOCAL_FLOW_TOX_STALE_MS", _DEFAULT_STALE_MS),
        )
    except Exception as e:
        logger.debug("flow_tox overrides: reader init fail (fail-open): %s", e)
        return None


def get_reader() -> FlowToxicityThresholdReader | None:
    """Lazy singleton. None when disabled or unavailable."""
    global _READER
    if _READER is not None:
        return _READER
    with _READER_LOCK:
        if _READER is None:
            _READER = _make_reader()
        return _READER


def get_thresholds(symbol: str) -> tuple[float, float]:
    """Convenience: return (thr_z, thr_vpin) or (0.0, 0.0) if reader disabled."""
    rdr = get_reader()
    if rdr is None:
        return 0.0, 0.0
    try:
        return rdr.get_thresholds(symbol)
    except Exception:
        return 0.0, 0.0
