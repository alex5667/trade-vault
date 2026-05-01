#!/usr/bin/env python3
from __future__ import annotations
"""
Auto-apply Tick Gate Exporter

Exports the current auto-apply block state (set by tick-gate blocker) to Prometheus.

Keys (default prefix = cfg:suggestions:entry_policy:auto_apply_block):
  {prefix}:tick_gate              -> "1" if blocked (optional TTL)
  {prefix}:tick_gate:meta         -> JSON blob (best-effort)
  {prefix}:tick_gate:ts_ms        -> ts of last decision

ENV:
  REDIS_URL                         redis://host:6379/0
  AUTO_APPLY_BLOCK_PREFIX           key prefix (default as above)
  AUTO_APPLY_EXPORTER_PORT          default 9115
  AUTO_APPLY_EXPORTER_POLL_S        default 5
  AUTO_APPLY_REASON_LABEL_MODE      collapse|allow|skip (default collapse)
  AUTO_APPLY_REASON_ALLOWLIST       comma separated reasons (optional)
"""


import json
import os
import time
from typing import Any, Dict, Optional, Tuple

from prometheus_client import Gauge, Counter, start_http_server


def _env(name: str, default: str) -> str:
    v = os.getenv(name)
    return default if v is None or str(v).strip() == "" else str(v).strip()


def parse_json_maybe(s: Any) -> Dict[str, Any]:
    if s is None:
        return {}
    if isinstance(s, dict):
        return s
    try:
        if isinstance(s, (bytes, bytearray)):
            s = s.decode("utf-8", errors="replace")
        s = str(s)
        if not s:
            return {}
        obj = json.loads(s)
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


def normalize_reason(raw: Optional[str], mode: str, allowlist: Optional[set]) -> str:
    """
    Normalize reason into a safe label:
      - collapse: unknown -> __other__
      - allow: pass-through sanitized (still collapses if allowlist provided)
      - skip: return "" (caller should not label)
    """
    r = (raw or "").strip()
    if not r:
        r = "unknown"
    r = r.replace(" ", "_")
    if len(r) > 48:
        r = r[:48]
    if mode == "skip":
        return ""
    if allowlist and r not in allowlist:
        return "__other__" if mode in ("collapse", "allow") else r
    return r


def read_block_state(rds, prefix: str) -> Tuple[bool, Dict[str, Any], int]:
    """
    Returns: (blocked, meta_dict, ts_ms)
    """
    k_block = f"{prefix}:tick_gate"
    k_meta = f"{prefix}:tick_gate:meta"
    k_ts = f"{prefix}:tick_gate:ts_ms"

    blocked = False
    meta: Dict[str, Any] = {}
    ts_ms = 0
    try:
        v = rds.get(k_block)
        if v is not None:
            if isinstance(v, (bytes, bytearray)):
                v = v.decode("utf-8", errors="replace")
            blocked = str(v).strip() == "1"
    except Exception:
        blocked = False

    try:
        meta_raw = rds.get(k_meta)
        meta = parse_json_maybe(meta_raw)
    except Exception:
        meta = {}

    try:
        t = rds.get(k_ts)
        if t is not None:
            if isinstance(t, (bytes, bytearray)):
                t = t.decode("utf-8", errors="replace")
            ts_ms = int(float(str(t)))
    except Exception:
        ts_ms = 0
    return blocked, meta, ts_ms


def main() -> int:
    prefix = _env("AUTO_APPLY_BLOCK_PREFIX", "cfg:suggestions:entry_policy:auto_apply_block")
    port = int(_env("AUTO_APPLY_EXPORTER_PORT", "9115"))
    poll_s = float(_env("AUTO_APPLY_EXPORTER_POLL_S", "5"))
    reason_mode = _env("AUTO_APPLY_REASON_LABEL_MODE", "collapse").lower()
    allow_raw = _env("AUTO_APPLY_REASON_ALLOWLIST", "")
    allow = {x.strip() for x in allow_raw.split(",") if x.strip()} or None

    g_blocked = Gauge("auto_apply_tick_gate_blocked", "Auto-apply blocked by tick gate (0/1)", ["reason"])
    g_meta_age = Gauge("auto_apply_tick_gate_block_meta_age_seconds", "Age of last block decision meta (seconds)")
    g_last = Gauge("auto_apply_tick_gate_exporter_last_scrape_ts_seconds", "Last successful scrape timestamp")
    c_errors = Counter("auto_apply_tick_gate_exporter_errors_total", "Exporter errors total")

    start_http_server(port)

    try:
        import redis  # type: ignore
    except Exception as e:
        raise SystemExit(f"redis not available: {e}")

    redis_url = _env("REDIS_URL", _env("CRYPTO_NOTIFY_REDIS_URL", "redis://localhost:6379/0"))
    rds = redis.Redis.from_url(redis_url, decode_responses=False)

    while True:
        try:
            blocked, meta, ts_ms = read_block_state(rds, prefix)
            now = time.time()
            g_last.set(now)

            reason_raw = (
                meta.get("pinned_reason")
                or meta.get("reason")
                or meta.get("fail_reason")
                or meta.get("status")
            )
            reason = normalize_reason(str(reason_raw) if reason_raw is not None else None, reason_mode, allow)
            if reason_mode == "skip":
                g_blocked.labels(reason="__skipped__").set(1.0 if blocked else 0.0)
            else:
                g_blocked.labels(reason=reason).set(1.0 if blocked else 0.0)

            if ts_ms > 0:
                g_meta_age.set(max(0.0, now - (ts_ms / 1000.0)))
            else:
                g_meta_age.set(0.0)
        except Exception:
            c_errors.inc()
        time.sleep(poll_s)


if __name__ == "__main__":
    raise SystemExit(main())
