#!/usr/bin/env python3
from __future__ import annotations

"""
P66: Policy mode KPI worker

Consumes `decisions:final` stream and maintains rolling 24h counters:
  - counts by regime (ok|warn|block|unknown) x effective_mode (active|shadow|block|unknown)
  - mismatch counters:
      * block_regime_effective_not_block
      * warn_regime_effective_active

Writes state to Redis hash:
  metrics:policy_mode:state

Low-cardinality, deterministic time, per-minute buckets with window subtract.

Architecture:
  - Consumer group on decisions:final (CG: policy_mode_kpi_p66_v1)
  - Per-minute hash buckets: kpi:policy_mode:bucket:<minute_id>
  - Rolling 24h count maintained in Redis hash: metrics:policy_mode:state
  - On startup: re-bootstraps rolling totals by summing last 1440 buckets
  - On idle: claims stale PEL messages (autoclaim) to avoid processing gaps
"""

import json
import os
import socket
import time
from dataclasses import dataclass
from typing import Any

from utils.time_utils import get_ny_time_millis


def _env(name: str, default: str = "") -> str:
    return (os.getenv(name) or default).strip()


def _now_ms() -> int:
    return get_ny_time_millis()


def _i(v: Any, d: int = 0) -> int:
    """Safe int cast via float."""
    try:
        return int(float(v))
    except Exception:
        return d


def _minute(ts_ms: int) -> int:
    """Convert epoch ms to minute bucket id."""
    return int(ts_ms // 60000)


def _parse_json_maybe(v: Any) -> Any:
    """
    Try to parse JSON if the value looks like a JSON object or array.
    Returns the original value as-is otherwise.
    """
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


def _norm_state(v: Any) -> str:
    """Normalize dq_state/drift_state value to ok|warn|block|unknown."""
    s = (v or "").strip().lower()
    if s in ("ok", "warn", "block"):
        return s
    return "unknown"


def _regime_from_states(dq_state: Any, drift_state: Any) -> str:
    """
    Compute composite regime from dq_state and drift_state.
    Priority: block > warn > ok > unknown
    """
    dq = _norm_state(dq_state)
    dr = _norm_state(drift_state)
    if dq == "block" or dr == "block":
        return "block"
    if dq == "warn" or dr == "warn":
        return "warn"
    if dq == "ok" and dr == "ok":
        return "ok"
    return "unknown"


def _norm_mode(v: Any) -> str:
    """
    Normalize effective mode value to active|shadow|block|unknown.
    Accept multiple naming variants from different pipeline versions.
    """
    s = (v or "").strip().lower()
    # active variants
    if s in ("active", "live", "on"):
        return "active"
    # shadow variants
    if s in ("shadow", "paper", "dry", "dry_run"):
        return "shadow"
    # block variants (fail-closed)
    if s in ("block", "off", "disabled", "freeze"):
        return "block"
    return "unknown"


def _decision_ts_ms(fields: dict[str, Any], stream_id: str) -> int:
    """
    Extract decision timestamp from message fields.
    Falls back to stream ID (Redis XADD timestamp part) if no explicit field found.
    Normalizes seconds -> ms if the value is suspiciously small.
    """
    for k in ("decision_ts_ms", "ts_ms", "ts", "decision_ts"):
        if k in fields:
            ts = _i(fields.get(k), 0)
            if ts > 0:
                # heuristic: if < 10_000_000_000 it's likely seconds, not ms
                if ts < 10_000_000_000:
                    ts *= 1000
                return ts
    # fall back to Redis stream ID timestamp part
    try:
        return int(str(stream_id).split("-", 1)[0])
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
    window_minutes: int       # rolling window width (default 1440 = 24h)
    bucket_prefix: str        # Redis key prefix for per-minute buckets
    bucket_ttl_s: int         # TTL for bucket keys (3d default)
    state_key: str            # Redis hash with rolled-up state
    claim_idle_ms: int        # PEL autoclaim threshold
    sleep_on_idle_s: float    # sleep when no new messages
    rebuild_gap_minutes: int  # if clock jumped > this many minutes, full rebuild


def load_cfg() -> Cfg:
    return Cfg(
        redis_url=_env("REDIS_URL", "redis://redis-worker-1:6379/0"),
        stream=_env("DECISIONS_FINAL_STREAM", "decisions:final"),
        group=_env("POLICY_MODE_CG", "policy_mode_kpi_p66_v1"),
        consumer=_env("POLICY_MODE_CONSUMER", socket.gethostname()),
        block_ms=_i(_env("POLICY_MODE_BLOCK_MS", "5000"), 5000),
        count=_i(_env("POLICY_MODE_READ_COUNT", "200"), 200),
        window_minutes=_i(_env("POLICY_MODE_WINDOW_MINUTES", "1440"), 1440),
        bucket_prefix=_env("POLICY_MODE_BUCKET_PREFIX", "kpi:policy_mode:bucket:"),
        bucket_ttl_s=_i(_env("POLICY_MODE_BUCKET_TTL_S", str(86400 * 3)), 86400 * 3),
        state_key=_env("POLICY_MODE_STATE_KEY", "metrics:policy_mode:state"),
        claim_idle_ms=_i(_env("POLICY_MODE_CLAIM_IDLE_MS", "60000"), 60000),
        sleep_on_idle_s=float(_env("POLICY_MODE_SLEEP_ON_IDLE_S", "0.2") or 0.2),
        rebuild_gap_minutes=_i(_env("POLICY_MODE_REBUILD_GAP_MINUTES", "10"), 10),
    )


def _bucket_key(cfg: Cfg, minute_id: int) -> str:
    return f"{cfg.bucket_prefix}{minute_id}"


def _ensure_group(r, cfg: Cfg) -> None:
    """Create consumer group if it doesn't exist yet. mkstream creates stream if absent."""
    try:
        r.xgroup_create(name=cfg.stream, groupname=cfg.group, id="0-0", mkstream=True)
    except Exception:
        pass  # BUSYGROUP error means the group already exists — ignore


# All per-minute counters stored per bucket and in rolling state
FIELDS = [
    "ok_active", "ok_shadow", "ok_block", "ok_unknown",
    "warn_active", "warn_shadow", "warn_block", "warn_unknown",
    "block_active", "block_shadow", "block_block", "block_unknown",
    "unknown_active", "unknown_shadow", "unknown_block", "unknown_unknown",
    "total",
    "mismatch_block_regime_effective_not_block",
    "mismatch_warn_regime_effective_active",
]


def _hget_counts(r, key: str) -> dict[str, int]:
    """Read all tracked fields from a bucket hash; missing fields default to 0."""
    d = r.hgetall(key) or {}
    out: dict[str, int] = {}
    for k in FIELDS:
        out[k] = _i(d.get(k), 0)
    return out


def _bootstrap_state(r, cfg: Cfg, now_ms: int) -> tuple[int, dict[str, int], int]:
    """
    Bootstrap (or re-bootstrap) rolling totals from last window_minutes bucket keys.
    Called once at startup and when a large time gap is detected.
    Returns (cur_minute, rolling_dict, last_ts_ms).
    """
    cur_min = _minute(now_ms)
    start_min = cur_min - cfg.window_minutes + 1
    rolling = dict.fromkeys(FIELDS, 0)
    # try to recover last_ts_ms from existing state
    st = r.hgetall(cfg.state_key) or {}
    last_ts_ms = _i(st.get("last_ts_ms"), 0)

    # batch-read all window buckets via pipeline
    pipe = r.pipeline()
    for m in range(start_min, cur_min + 1):
        pipe.hmget(_bucket_key(cfg, m), FIELDS)
    rows = pipe.execute()
    for row in rows:
        if not row:
            continue
        for idx, k in enumerate(FIELDS):
            rolling[k] += _i(row[idx], 0) if idx < len(row) else 0

    # persist rebuilt state
    r.hset(
        cfg.state_key,
        mapping={
            "cur_minute": str(cur_min),
            **{f"rolling_{k}": str(int(v)) for k, v in rolling.items()},
            "last_ts_ms": str(int(last_ts_ms)),
            "updated_ts_ms": str(int(now_ms)),
        },
    )
    return cur_min, rolling, last_ts_ms


def _advance_window(r, cfg: Cfg, from_min: int, to_min: int, rolling: dict[str, int]) -> int:
    """
    Advance the rolling window from from_min to to_min by subtracting expired buckets.
    If the gap is too large (rebuild_gap_minutes), do a full bootstrap instead.
    Mutates rolling in-place. Returns new current minute.
    """
    if to_min <= from_min:
        return from_min
    gap = to_min - from_min
    if gap >= cfg.rebuild_gap_minutes:
        # large gap — full rebuild is more reliable than subtract-only
        cur_min, new_roll, _ = _bootstrap_state(r, cfg, _now_ms())
        rolling.update(new_roll)
        return cur_min
    # incremental subtract: drop buckets that just fell out of the window
    cur = from_min
    for m in range(from_min + 1, to_min + 1):
        out_m = m - cfg.window_minutes  # minute that expires as we advance
        out_counts = _hget_counts(r, _bucket_key(cfg, out_m))
        for k in FIELDS:
            rolling[k] = max(0, int(rolling.get(k, 0)) - int(out_counts.get(k, 0)))
        cur = m
    return cur


def _decode_fields(raw: dict[str, Any]) -> dict[str, Any]:
    """Normalize Redis message field dict to str keys."""
    return {str(k): v for k, v in (raw or {}).items()}


def _process_one(
    r,
    cfg: Cfg,
    stream_id: str,
    fields: dict[str, Any],
    cur_min: int,
    rolling: dict[str, int],
    last_ts_ms: int,
) -> tuple[int, int]:
    """
    Process a single decisions:final message:
    1. Extract timestamp and determine minute bucket.
    2. Skip if older than the rolling window start.
    3. Advance window if message is newer than current minute.
    4. Parse dq_state/drift_state → regime.
    5. Parse policy_effective_mode/effective_mode/policy_mode → effective_mode.
    6. Increment bucket hash and rolling counters.
    7. Persist updated rolling state to Redis.

    Returns (cur_min, last_ts_ms).
    """
    ts_ms = _decision_ts_ms(fields, stream_id)
    last_ts_ms = max(last_ts_ms, ts_ms)
    m = _minute(ts_ms)

    # skip messages older than the window (too late to include)
    if m < cur_min - cfg.window_minutes:
        return cur_min, last_ts_ms

    # advance window if message is ahead of cur_min
    if m > cur_min:
        cur_min = _advance_window(r, cfg, cur_min, m, rolling)

    # ── Parse dq/drift states ────────────────────────────────────────────────
    dq_state = _parse_json_maybe(fields.get("dq_state", "unknown"))
    drift_state = _parse_json_maybe(fields.get("drift_state", "unknown"))
    # handle nested {"state": "ok"} objects written by some pipeline versions
    if isinstance(dq_state, dict) and "state" in dq_state:
        dq_state = dq_state.get("state")
    if isinstance(drift_state, dict) and "state" in drift_state:
        drift_state = drift_state.get("state")
    regime = _regime_from_states(dq_state, drift_state)

    # ── Parse effective mode ─────────────────────────────────────────────────
    # Check several field names in priority order (different pipeline versions)
    eff = fields.get("policy_effective_mode")
    if eff is None:
        eff = fields.get("effective_mode")
    if eff is None:
        eff = fields.get("policy_mode")
    effective_mode = _norm_mode(eff)

    # ── Compute matrix cell key ──────────────────────────────────────────────
    cell = f"{regime}_{effective_mode}"
    if cell not in rolling:
        # safety fallback if some new combo appears
        cell = "unknown_unknown"

    # ── Compute mismatch flags ───────────────────────────────────────────────
    # Critical: block regime must always have effective_mode=block (fail-closed)
    mism_block_not_block = 1 if (regime == "block" and effective_mode != "block") else 0
    # Warning: warn regime with active mode means we're still trading in degraded state
    mism_warn_active = 1 if (regime == "warn" and effective_mode == "active") else 0

    # ── Atomic bucket update + state write via pipeline ──────────────────────
    bkey = _bucket_key(cfg, m)
    pipe = r.pipeline()
    pipe.hincrby(bkey, cell, 1)
    pipe.hincrby(bkey, "total", 1)
    if mism_block_not_block:
        pipe.hincrby(bkey, "mismatch_block_regime_effective_not_block", 1)
    if mism_warn_active:
        pipe.hincrby(bkey, "mismatch_warn_regime_effective_active", 1)
    pipe.expire(bkey, cfg.bucket_ttl_s)

    # update rolling in-memory state
    rolling[cell] = rolling.get(cell, 0) + 1
    rolling["total"] = rolling.get("total", 0) + 1
    if mism_block_not_block:
        rolling["mismatch_block_regime_effective_not_block"] = (
            rolling.get("mismatch_block_regime_effective_not_block", 0) + 1
        )
    if mism_warn_active:
        rolling["mismatch_warn_regime_effective_active"] = (
            rolling.get("mismatch_warn_regime_effective_active", 0) + 1
        )

    # persist full rolling state atomically with bucket update
    pipe.hset(
        cfg.state_key,
        mapping={
            "cur_minute": str(cur_min),
            **{f"rolling_{k}": str(int(v)) for k, v in rolling.items()},
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
    _ensure_group(r, cfg)

    # Bootstrap rolling totals from existing buckets
    cur_min, rolling, last_ts_ms = _bootstrap_state(r, cfg, _now_ms())
    last_claim_ms = 0

    while True:
        now_ms = _now_ms()

        # ── PEL autoclaim: recover stale messages from crashed consumers ────
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
                # xautoclaim returns [next_id, [[id, fields], ...], [deleted_ids]]
                msgs = res[1] if isinstance(res, (list, tuple)) and len(res) >= 2 else []
                for mid, mfields in msgs:
                    cur_min, last_ts_ms = _process_one(
                        r, cfg, str(mid), _decode_fields(mfields), cur_min, rolling, last_ts_ms
                    )
                    r.xack(cfg.stream, cfg.group, mid)
            except Exception:
                pass  # Redis unavailable or version doesn't support xautoclaim

        # ── Normal read from consumer group ─────────────────────────────────
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
                    cur_min, last_ts_ms = _process_one(
                        r, cfg, str(mid), _decode_fields(mfields), cur_min, rolling, last_ts_ms
                    )
                except Exception:
                    pass  # process_one errors are non-fatal; ack anyway
                try:
                    r.xack(cfg.stream, cfg.group, mid)
                except Exception:
                    pass


if __name__ == "__main__":
    raise SystemExit(main())
