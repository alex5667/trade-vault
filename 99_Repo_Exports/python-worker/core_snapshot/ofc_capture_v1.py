from __future__ import annotations
"""OFC_CAPTURE v1: deterministic sampling + NDJSON capture to stable storage.

Why this exists (Train==Serve / golden replay):
  - Golden replay parity requires that we can re-run OFConfirmEngine.build() on
    *the same inputs*.
  - Runtime already knows the full inputs (indicators + minimal runtime snapshot
    + stateful gates state). This module provides a safe, fail-open mechanism
    to persist those inputs + outputs.

Design goals:
  - Deterministic sampling (stable, restart-safe): same (symbol, ts_ms, direction)
    always results in the same capture decision.
  - Low overhead: append-only files, one file per (day, policy_hash, pid),
    optional fsync throttling.
  - JSON-safe: everything is encoded as primitives/dicts; unknown types -> str().

Security note:
  - These captures may contain sensitive strategy/evidence details. Store them
    on trusted machines only and apply retention policies.
"""

from utils.time_utils import get_ny_time_millis

import json
import os
import socket
import time
import hashlib
from dataclasses import asdict, is_dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple


# ---------------------------------------------------------------------------
# B7: observability sidecar
#
# Runtime workers may not expose /metrics. We persist lightweight capture stats
# under <capture_dir>/_state/ for a standalone exporter to scrape.
# ---------------------------------------------------------------------------


def _env_bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    v = str(v).strip().lower()
    return v in ("1", "true", "yes", "y", "on")


def _env_int(name: str, default: int) -> int:
    v = os.getenv(name)
    if v is None:
        return default
    try:
        return int(str(v).strip())
    except Exception:
        return default


def _env_str(name: str, default: str) -> str:
    v = os.getenv(name)
    return str(v) if v is not None else default


def _utc_yyyymmdd(ts_ms: int) -> str:
    try:
        dt = datetime.fromtimestamp(max(0, int(ts_ms)) / 1000.0, tz=timezone.utc)
    except Exception:
        dt = datetime.now(tz=timezone.utc)
    return dt.strftime("%Y%m%d")


def _json_safe(obj: Any) -> Any:
    """Make an object JSON-safe (primitives/dicts/lists).

    This is intentionally conservative: unknown objects are stringified.
    """
    if obj is None:
        return None
    if isinstance(obj, (str, int, float, bool)):
        return obj
    if is_dataclass(obj):
        try:
            return _json_safe(asdict(obj))
        except Exception:
            return str(obj)
    if isinstance(obj, dict):
        out: Dict[str, Any] = {}
        for k, v in obj.items():
            try:
                kk = str(k)
            except Exception:
                kk = "<key>"
            out[kk] = _json_safe(v)
        return out
    if isinstance(obj, (list, tuple)):
        return [_json_safe(x) for x in obj]
    try:
        return str(obj)
    except Exception:
        return "<unserializable>"


def _stable_u64(key: str, seed: str) -> int:
    """Deterministic 64-bit hash from (seed, key)."""
    h = hashlib.blake2b(digest_size=16)
    h.update(seed.encode("utf-8", errors="ignore"))
    h.update(b"\x00")
    h.update(key.encode("utf-8", errors="ignore"))
    d = h.digest()
    return int.from_bytes(d[:8], "big", signed=False)


def should_sample(*, stable_key: str, sample_ppm: int, seed: str) -> bool:
    """Return True if this event should be captured.

    Args:
        stable_key: Stable identifier for the event.
        sample_ppm: Sampling probability in parts-per-million.
        seed: Namespace seed.
    """
    try:
        ppm = int(sample_ppm)
    except Exception:
        ppm = 0
    if ppm <= 0:
        return False
    if ppm >= 1_000_000:
        return True
    x = _stable_u64(stable_key, seed)
    return (x % 1_000_000) < ppm


class NDJSONRotatingWriter:
    """Append-only NDJSON writer with size/time rotation.

    Writes one file per (day, policy_hash, pid). That avoids cross-process
    locking. Rotation is local to the process.
    """

    def __init__(
        self,
        *,
        base_dir: str,
        max_bytes: int = 256 * 1024 * 1024,
        rotate_sec: int = 3600,
        fsync_every_n: int = 0,
    ) -> None:
        self.base_dir = str(base_dir)
        self.max_bytes = int(max_bytes)
        self.rotate_sec = int(rotate_sec)
        self.fsync_every_n = int(fsync_every_n)
        self._host = socket.gethostname()[:48]
        self._pid = os.getpid()
        self._seq = 0
        self._fh = None
        self._path = None
        self._opened_at = 0.0
        self._n = 0

        # ---- B7 local stats sidecar (best-effort)
        self._stats_dir = os.path.join(self.base_dir, "_state")
        try:
            os.makedirs(self._stats_dir, exist_ok=True)
        except Exception:
            pass
        self._stats_path = os.path.join(self._stats_dir, f"ofc_capture_stats_{self._host}-{self._pid}.json")
        self._stats = {
            "schema": "ofc_capture_stats_v1",
            "host": self._host,
            "pid": int(self._pid),
            "started_ts_ms": get_ny_time_millis(),
            "written_total": 0,
            "bytes_total": 0,
            "errors_total": 0,
            "sampled_out_total": 0,
            "last_write_ts_ms": 0,
            "last_error_ts_ms": 0,
            "last_error": "",
            "last_path": "",
        }
        self._stats_flush_every_n = 0
        self._stats_flush_sec = 0
        self._stats_last_flush = 0.0

    def configure_stats_flush(self, *, flush_every_n: int, flush_sec: int) -> None:
        self._stats_flush_every_n = int(flush_every_n)
        self._stats_flush_sec = int(flush_sec)

    def _stats_flush(self) -> None:
        """Best-effort atomic stats flush."""
        try:
            self._stats["updated_ts_ms"] = get_ny_time_millis()
            tmp = self._stats_path + ".tmp"
            with open(tmp, "w", encoding="utf-8") as fh:
                fh.write(json.dumps(self._stats, ensure_ascii=False, separators=(",", ":")))
            os.replace(tmp, self._stats_path)
            self._stats_last_flush = time.monotonic()
        except Exception:
            pass

    def _mk_path(self, *, day: str, policy_hash: str) -> str:
        ph = (policy_hash or "unknown")[:32]
        d = os.path.join(self.base_dir, day, f"policy_{ph}")
        os.makedirs(d, exist_ok=True)
        name = f"decisions-{self._host}-{self._pid}-{self._seq:04d}.ndjson"
        return os.path.join(d, name)

    def _need_rotate(self, *, path: str) -> bool:
        if self._fh is None or self._path != path:
            return True
        now = time.monotonic()
        if self.rotate_sec > 0 and (now - self._opened_at) >= float(self.rotate_sec):
            return True
        try:
            if self.max_bytes > 0 and os.path.exists(path):
                if os.path.getsize(path) >= self.max_bytes:
                    return True
        except Exception:
            return False
        return False

    def _open(self, *, path: str) -> None:
        # Close previous
        try:
            if self._fh is not None:
                self._fh.flush()
                self._fh.close()
        except Exception:
            pass

        # Ensure directory exists
        os.makedirs(os.path.dirname(path), exist_ok=True)

        # Line-buffered append
        self._fh = open(path, "a", encoding="utf-8", buffering=1)
        self._path = path
        self._opened_at = time.monotonic()
        self._n = 0

        # ---- B7 local stats sidecar (best-effort)
        self._stats_dir = os.path.join(self.base_dir, "_state")
        try:
            os.makedirs(self._stats_dir, exist_ok=True)
        except Exception:
            pass
        self._stats_path = os.path.join(self._stats_dir, f"ofc_capture_stats_{self._host}-{self._pid}.json")
        self._stats = {
            "schema": "ofc_capture_stats_v1",
            "host": self._host,
            "pid": int(self._pid),
            "started_ts_ms": get_ny_time_millis(),
            "written_total": 0,
            "bytes_total": 0,
            "errors_total": 0,
            "sampled_out_total": 0,
            "last_write_ts_ms": 0,
            "last_error_ts_ms": 0,
            "last_error": "",
            "last_path": "",
        }
        self._stats_flush_every_n = 0
        self._stats_flush_sec = 0
        self._stats_last_flush = 0.0

    def write(self, *, day: str, policy_hash: str, record: Dict[str, Any]) -> Optional[str]:
        path = self._mk_path(day=day, policy_hash=policy_hash)
        if self._need_rotate(path=path):
            # rotate by incrementing seq only when the day+policy stays same
            if self._path is not None and os.path.dirname(self._path) == os.path.dirname(path):
                self._seq += 1
                path = self._mk_path(day=day, policy_hash=policy_hash)
            self._open(path=path)

        if self._fh is None:
            self._stats["errors_total"] = int(self._stats.get("errors_total", 0)) + 1
            self._stats["last_error_ts_ms"] = get_ny_time_millis()
            self._stats["last_error"] = "writer_not_open"
            return None

        try:
            line = json.dumps(record, ensure_ascii=False, separators=(",", ":"))
            self._fh.write(line + "\n")
            self._n += 1

            # ---- B7 stats
            self._stats["written_total"] = int(self._stats.get("written_total", 0)) + 1
            self._stats["bytes_total"] = int(self._stats.get("bytes_total", 0)) + int(len(line) + 1)
            self._stats["last_write_ts_ms"] = get_ny_time_millis()
            self._stats["last_path"] = str(path)
            if self.fsync_every_n > 0 and (self._n % self.fsync_every_n) == 0:
                try:
                    self._fh.flush()
                    os.fsync(self._fh.fileno())
                except Exception:
                    pass
            # Stats flush throttling: every N writes or every T seconds.
            do_flush = False
            if self._stats_flush_every_n > 0 and (self._n % self._stats_flush_every_n) == 0:
                do_flush = True
            if (not do_flush) and self._stats_flush_sec > 0:
                now = time.monotonic()
                if (now - float(self._stats_last_flush)) >= float(self._stats_flush_sec):
                    do_flush = True
            if do_flush:
                self._stats_flush()

            return path
        except Exception:
            self._stats["errors_total"] = int(self._stats.get("errors_total", 0)) + 1
            self._stats["last_error_ts_ms"] = get_ny_time_millis()
            self._stats["last_error"] = "write_exception"
            self._stats_flush()
            return None


_WRITER: Optional[NDJSONRotatingWriter] = None


def _get_writer(cfg2: Dict[str, Any]) -> NDJSONRotatingWriter:
    global _WRITER
    if _WRITER is not None:
        return _WRITER

    base_dir = str(cfg2.get("ofc_capture_dir") or _env_str("OFC_CAPTURE_DIR", "/var/lib/scanner/ofc_capture"))
    max_bytes = int(cfg2.get("ofc_capture_max_bytes") or _env_int("OFC_CAPTURE_MAX_BYTES", 256 * 1024 * 1024))
    rotate_sec = int(cfg2.get("ofc_capture_rotate_sec") or _env_int("OFC_CAPTURE_ROTATE_SEC", 3600))
    fsync_every_n = int(cfg2.get("ofc_capture_fsync_every_n") or _env_int("OFC_CAPTURE_FSYNC_EVERY_N", 0))

    _WRITER = NDJSONRotatingWriter(
        base_dir=base_dir,
        max_bytes=max_bytes,
        rotate_sec=rotate_sec,
        fsync_every_n=fsync_every_n,
    )
    return _WRITER


def capture_enabled(cfg2: Dict[str, Any]) -> bool:
    # Backward compatible env name: OFC_CAPTURE
    if int(cfg2.get("ofc_capture_enable", 0) or 0) == 1:
        return True
    if _env_bool("OFC_CAPTURE_ENABLE", False):
        return True
    if _env_bool("OFC_CAPTURE", False):
        return True
    return False


def maybe_capture_ofc_v1(
    *,
    engine: Any,
    runtime: Any,
    indicators: Dict[str, Any],
    cfg2: Dict[str, Any],
    ofc: Any,
    dec: Any,
    now_ts_ms: int,
) -> Optional[str]:
    """Capture one decision record (fail-open). Returns path if written."""

    if not capture_enabled(cfg2):
        return None

    # deterministic sampling
    sample_ppm = int(cfg2.get("ofc_capture_sample_ppm") or _env_int("OFC_CAPTURE_SAMPLE_PPM", 1000))
    seed = str(cfg2.get("ofc_capture_seed") or _env_str("OFC_CAPTURE_SEED", "ofc_cap_v1"))

    symbol = str(getattr(ofc, "symbol", "") or indicators.get("symbol") or "")
    direction = str(getattr(ofc, "direction", "") or indicators.get("direction") or "")
    ts_ms = int(getattr(ofc, "ts_ms", 0) or indicators.get("event_ts_ms") or now_ts_ms or 0)
    stable_key = f"{symbol}|{direction}|{ts_ms}"

    if not should_sample(stable_key=stable_key, sample_ppm=sample_ppm, seed=seed):
        # B7: track sampling to validate expected capture rate.
        try:
            wr = _get_writer(cfg2)
            if getattr(wr, "_stats", None) is not None:
                wr._stats["sampled_out_total"] = int(wr._stats.get("sampled_out_total", 0)) + 1  # type: ignore[attr-defined]
        except Exception:
            pass
        return None

    # group by policy hash if available (B4)
    policy_hash = str(indicators.get("dq_policy_hash", "") or "unknown")
    manifest_hash = str(indicators.get("dq_policy_feature_manifest_hash_v1", "") or "")

    # runtime + gate state for deterministic replay
    try:
        rt_snap = engine.export_runtime_snapshot(runtime, indicators)
    except Exception:
        rt_snap = {"schema": 0}

    try:
        gate_state = engine.export_gate_state(symbol=symbol) if hasattr(engine, "export_gate_state") else {}
    except Exception:
        gate_state = {}

    # minimal dec snapshot (best-effort)
    dec_snap: Dict[str, Any] = {}
    try:
        if dec is not None:
            for k in ("scenario", "need", "have", "score", "gate_bits"):
                v = getattr(dec, k, None)
                if isinstance(v, (str, int, float, bool)) or v is None:
                    dec_snap[k] = v
    except Exception:
        dec_snap = {}

    record = {
        "schema": "ofc_capture_v1",
        "ts_ms": ts_ms,
        "symbol": symbol,
        "direction": direction,
        "policy_hash": policy_hash,
        "manifest_hash": manifest_hash,
        "sample_ppm": int(sample_ppm),
        "stable_key": stable_key,
        "now_ts_ms": int(now_ts_ms),
        # payload
        "indicators": _json_safe(indicators),
        "of_confirm_v3": _json_safe(ofc),
        "decision": _json_safe(dec_snap),
        "runtime_snapshot": _json_safe(rt_snap),
        "gate_state": _json_safe(gate_state),
    }

    day = _utc_yyyymmdd(ts_ms)
    wr = _get_writer(cfg2)
    return wr.write(day=day, policy_hash=policy_hash, record=record)


__all__ = [
    "capture_enabled",
    "maybe_capture_ofc_v1",
    "should_sample",
    "NDJSONRotatingWriter",
]


# B7: exporter sidecar contract
# - Stats files: <OFC_CAPTURE_DIR>/_state/ofc_capture_stats_<host>-<pid>.json
# - Schema: ofc_capture_stats_v1
