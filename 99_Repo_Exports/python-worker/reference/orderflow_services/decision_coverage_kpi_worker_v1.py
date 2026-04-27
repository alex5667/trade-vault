#!/usr/bin/env python3
"""
Decision coverage KPI worker (P66-ish).

Consumes `decisions:final` stream and maintains a low-cardinality rolling 24h window:
  - decision_last_ts_ms
  - decision_n_24h
  - decision_regime_n_24h{ok|warn|block|unknown}

Writes state to Redis hash (read by exporter):
  metrics:decision_coverage:state

Design goals:
  - deterministic time (use decision_ts_ms if present; fallback to stream id / now)
  - bounded cardinality (no symbol labels)
  - low overhead (bucketed per-minute counts + rolling window subtract on minute advance)
"""

from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import json
import os
import socket
import time
from dataclasses import dataclass
from typing import Any, Dict, Tuple, Optional, List


def _env(name: str, default: str = "") -> str:
    return (os.getenv(name) or default).strip()


def _now_ms() -> int:
    return get_ny_time_millis()


def _i(v: Any, d: int = 0) -> int:
    try:
        return int(float(v))
    except Exception:
        return d


def _minute(ts_ms: int) -> int:
    """Convert millisecond timestamp to whole-minute bucket id."""
    return int(ts_ms // 60000)


def _parse_json_maybe(v: Any) -> Any:
    """Try to parse string as JSON; return as-is if not JSON-like."""
    if v is None:
        return None
    if isinstance(v, (dict, list)):
        return v
    s = str(v)
    s0 = s.strip()
    if not s0:
        return s
    if (s0.startswith("{") and s0.endswith("}")) or (s0.startswith("[") and s0.endswith("]")):
        try:
            return json.loads(s0)
        except Exception:
            return s
    return s


def _state_norm(v: Any) -> str:
    """Normalize DQ/drift state to one of: ok|warn|block|unknown."""
    s = str(v or "").strip().lower()
    if s in ("ok", "warn", "block"):
        return s
    return "unknown"


def _regime_from_states(dq_state: Any, drift_state: Any) -> str:
    """
    Derive low-cardinality regime from dq_state and drift_state.
    Priority: block > warn > ok > unknown.
    """
    dq = _state_norm(dq_state)
    dr = _state_norm(drift_state)
    if dq == "block" or dr == "block":
        return "block"
    if dq == "warn" or dr == "warn":
        return "warn"
    if dq == "ok" and dr == "ok":
        return "ok"
    return "unknown"


def _decision_ts_ms(fields: Dict[str, Any], stream_id: str) -> int:
    """
    Extract decision timestamp (ms) from payload fields.
    Priority: explicit ms fields > seconds fields (normalized) > stream id > now.
    """
    # Prefer explicit decision timestamp fields
    for k in ("decision_ts_ms", "ts_ms", "ts", "decision_ts"):
        if k in fields:
            ts = _i(fields.get(k), 0)
            if ts > 0:
                # normalize seconds -> ms if needed (heuristic: < 10B ms → seconds)
                if ts < 10_000_000_000:
                    ts *= 1000
                return ts
    # Stream id is like "1700000000000-0"
    try:
        return int(stream_id.split("-", 1)[0])
    except Exception:
        return _now_ms()


@dataclass
class Cfg:
    redis_url: str
    stream: str
    group: str
    consumer: str
    block_ms: int
    count: int
    window_minutes: int
    bucket_prefix: str
    bucket_ttl_s: int
    state_key: str
    claim_idle_ms: int
    sleep_on_idle_s: float
    rebuild_gap_minutes: int


def load_cfg() -> Cfg:
    return Cfg(
        redis_url=_env("REDIS_URL", "redis://redis-worker-1:6379/0"),
        stream=_env("DECISIONS_FINAL_STREAM", "decisions:final"),
        group=_env("DECISION_COVERAGE_CG", "decision_coverage_kpi_v1"),
        consumer=_env("DECISION_COVERAGE_CONSUMER", socket.gethostname()),
        block_ms=_i(_env("DECISION_COVERAGE_BLOCK_MS", "5000"), 5000),
        count=_i(_env("DECISION_COVERAGE_READ_COUNT", "200"), 200),
        window_minutes=_i(_env("DECISION_COVERAGE_WINDOW_MINUTES", "1440"), 1440),
        bucket_prefix=_env("DECISION_COVERAGE_BUCKET_PREFIX", "kpi:decision_coverage:bucket:"),
        bucket_ttl_s=_i(_env("DECISION_COVERAGE_BUCKET_TTL_S", str(86400 * 3)), 86400 * 3),
        state_key=_env("DECISION_COVERAGE_STATE_KEY", "metrics:decision_coverage:state"),
        claim_idle_ms=_i(_env("DECISION_COVERAGE_CLAIM_IDLE_MS", "60000"), 60000),
        sleep_on_idle_s=float(_env("DECISION_COVERAGE_SLEEP_ON_IDLE_S", "0.2") or 0.2),
        rebuild_gap_minutes=_i(_env("DECISION_COVERAGE_REBUILD_GAP_MINUTES", "10"), 10),
    )


def _bucket_key(cfg: Cfg, minute_id: int) -> str:
    return f"{cfg.bucket_prefix}{minute_id}"


def _ensure_group(r, cfg: Cfg) -> None:
    """Create consumer group if missing. MKSTREAM ensures stream exists."""
    try:
        r.xgroup_create(name=cfg.stream, groupname=cfg.group, id="0-0", mkstream=True)
    except Exception:
        # BUSYGROUP — already exists; or stream already present
        pass


def _hget_counts(r, key: str) -> Dict[str, int]:
    """Read per-minute bucket hash and return integer counts per regime."""
    d = r.hgetall(key) or {}
    out: Dict[str, int] = {}
    for k in ("ok", "warn", "block", "unknown", "total"):
        out[k] = _i(d.get(k), 0)
    return out


def _bootstrap_state(r, cfg: Cfg, now_ms: int) -> Tuple[int, Dict[str, int], int]:
    """
    Rebuild rolling window sums from per-minute buckets stored in Redis.
    Returns (cur_minute, rolling_counts, last_ts_ms).
    Called on startup and after large time gaps (rebuild_gap_minutes).
    """
    cur_min = _minute(now_ms)
    start_min = cur_min - cfg.window_minutes + 1

    rolling = {"ok": 0, "warn": 0, "block": 0, "unknown": 0, "total": 0}
    # best-effort last_ts from existing state hash
    st = r.hgetall(cfg.state_key) or {}
    last_ts_ms = _i(st.get("last_ts_ms"), 0)

    # Pipeline to read all per-minute buckets in one RTT
    pipe = r.pipeline()
    for m in range(start_min, cur_min + 1):
        k = _bucket_key(cfg, m)
        pipe.hmget(k, ["ok", "warn", "block", "unknown", "total"])
    rows = pipe.execute()

    for row in rows:
        if not row:
            continue
        ok = _i(row[0], 0)
        warn = _i(row[1], 0)
        block = _i(row[2], 0)
        unk = _i(row[3], 0)
        total = _i(row[4], ok + warn + block + unk)
        rolling["ok"] += ok
        rolling["warn"] += warn
        rolling["block"] += block
        rolling["unknown"] += unk
        rolling["total"] += total

    # Persist bootstrap result immediately so exporter has fresh state
    r.hset(
        cfg.state_key,
        mapping={
            "cur_minute": str(cur_min),
            "rolling_ok": str(rolling["ok"]),
            "rolling_warn": str(rolling["warn"]),
            "rolling_block": str(rolling["block"]),
            "rolling_unknown": str(rolling["unknown"]),
            "rolling_total": str(rolling["total"]),
            "last_ts_ms": str(last_ts_ms),
            "updated_ts_ms": str(now_ms),
        },
    )
    return cur_min, rolling, last_ts_ms


def _advance_window(r, cfg: Cfg, from_min: int, to_min: int, rolling: Dict[str, int]) -> int:
    """
    Advance window minute pointer from `from_min` to `to_min`,
    subtracting outgoing minute buckets at the tail of the window.
    If gap is too large (>= rebuild_gap_minutes), do a full rebuild instead.
    Returns updated cur_minute (in-place modifies rolling dict).
    """
    if to_min <= from_min:
        return from_min
    gap = to_min - from_min
    if gap >= cfg.rebuild_gap_minutes:
        # Large gap: rebuild is cheaper than iterating many buckets
        cur_min, new_roll, _ = _bootstrap_state(r, cfg, _now_ms())
        rolling.update(new_roll)
        return cur_min

    cur = from_min
    for m in range(from_min + 1, to_min + 1):
        # Subtract the bucket that just fell outside the window
        out_m = m - cfg.window_minutes
        out_key = _bucket_key(cfg, out_m)
        out_counts = _hget_counts(r, out_key)
        for k in ("ok", "warn", "block", "unknown", "total"):
            rolling[k] = max(0, int(rolling.get(k, 0)) - int(out_counts.get(k, 0)))
        cur = m
    return cur


def _decode_fields(raw: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize field keys to str."""
    out: Dict[str, Any] = {}
    for k, v in (raw or {}).items():
        out[str(k)] = v
    return out


def _process_one(
    r,
    cfg: Cfg,
    stream_id: str,
    fields: Dict[str, Any],
    cur_min: int,
    rolling: Dict[str, int],
    last_ts_ms: int,
) -> Tuple[int, int]:
    """
    Process a single stream message:
      1. Extract timestamp and regime.
      2. Advance window minute pointer if needed.
      3. Increment per-minute bucket and rolling totals.
      4. Write updated state hash.
    Returns (new_cur_min, new_last_ts_ms).
    """
    ts_ms = _decision_ts_ms(fields, stream_id)
    last_ts_ms = max(last_ts_ms, ts_ms)

    m = _minute(ts_ms)
    # Silently drop very old messages (beyond window) — still ACK to clear PEL
    if m < cur_min - cfg.window_minutes:
        return cur_min, last_ts_ms

    if m > cur_min:
        cur_min = _advance_window(r, cfg, cur_min, m, rolling)

    # Extract dq_state / drift_state from fields; handle nested JSON objects
    dq_state = _parse_json_maybe(fields.get("dq_state", "unknown"))
    drift_state = _parse_json_maybe(fields.get("drift_state", "unknown"))
    if isinstance(dq_state, dict) and "state" in dq_state:
        dq_state = dq_state.get("state")
    if isinstance(drift_state, dict) and "state" in drift_state:
        drift_state = drift_state.get("state")

    regime = _regime_from_states(dq_state, drift_state)

    # Atomic pipeline: update bucket + rolling state in one round-trip
    bkey = _bucket_key(cfg, m)
    pipe = r.pipeline()
    pipe.hincrby(bkey, regime, 1)
    pipe.hincrby(bkey, "total", 1)
    pipe.expire(bkey, cfg.bucket_ttl_s)

    rolling[regime] = int(rolling.get(regime, 0)) + 1
    rolling["total"] = int(rolling.get("total", 0)) + 1

    pipe.hset(
        cfg.state_key,
        mapping={
            "cur_minute": str(cur_min),
            "rolling_ok": str(int(rolling["ok"])),
            "rolling_warn": str(int(rolling["warn"])),
            "rolling_block": str(int(rolling["block"])),
            "rolling_unknown": str(int(rolling["unknown"])),
            "rolling_total": str(int(rolling["total"])),
            "last_ts_ms": str(int(last_ts_ms)),
            "updated_ts_ms": str(_now_ms()),
        },
    )
    pipe.execute()
    return cur_min, last_ts_ms


def main() -> int:
    cfg = load_cfg()
    import redis  # type: ignore

    r = redis.Redis.from_url(cfg.redis_url, decode_responses=True)
    
    while True:
        try:
            r.ping()
            _ensure_group(r, cfg)
            # Bootstrap rolling counts from persisted per-minute buckets
            cur_min, rolling, last_ts_ms = _bootstrap_state(r, cfg, _now_ms())
            break
        except (redis.exceptions.BusyLoadingError, redis.exceptions.ConnectionError) as e:
            print(f"Redis not ready ({type(e).__name__}), retrying in 1s...")
            time.sleep(1.0)

    last_claim_ms = 0
    while True:
        # Periodic XAUTOCLAIM to recover stale PEL entries (other dead consumers)
        now_ms = _now_ms()
        if now_ms - last_claim_ms > cfg.claim_idle_ms:
            last_claim_ms = now_ms
            try:
                res = r.xautoclaim(
                    cfg.stream,
                    cfg.group,
                    cfg.consumer,
                    min_idle_time=cfg.claim_idle_ms,
                    start_id="0-0",
                    count=cfg.count,
                )
                msgs = res[1] if isinstance(res, (list, tuple)) and len(res) >= 2 else []
                for mid, mfields in msgs:
                    fields = _decode_fields(mfields)
                    cur_min, last_ts_ms = _process_one(r, cfg, str(mid), fields, cur_min, rolling, last_ts_ms)
                    r.xack(cfg.stream, cfg.group, mid)
            except Exception:
                pass

        # Main read loop: block until new messages arrive
        try:
            data = r.xreadgroup(
                groupname=cfg.group,
                consumername=cfg.consumer,
                streams={cfg.stream: ">"},
                count=cfg.count,
                block=cfg.block_ms,
            )
        except Exception:
            time.sleep(1.0)
            continue

        if not data:
            time.sleep(cfg.sleep_on_idle_s)
            continue

        for _stream, msgs in data:
            for mid, mfields in msgs:
                try:
                    fields = _decode_fields(mfields)
                    cur_min, last_ts_ms = _process_one(r, cfg, str(mid), fields, cur_min, rolling, last_ts_ms)
                except Exception:
                    pass
                try:
                    r.xack(cfg.stream, cfg.group, mid)
                except Exception:
                    pass


if __name__ == "__main__":
    raise SystemExit(main())
