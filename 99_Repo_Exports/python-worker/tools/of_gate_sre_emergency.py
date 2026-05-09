from __future__ import annotations

from domain.evidence_keys import MetaKeys
from core.redis_keys import RedisStreams as RS

"""Emergency safety: auto-disable execution (ENTRY_POLICY_SHADOW=1) on critical drifts.

Reads metrics:of_gate for a time window, and if critical thresholds are exceeded,
automatically applies bundle:
  - SET cfg:entry_policy:overrides:* (JSON merge → ENTRY_POLICY_SHADOW=1)
  - Optionally: HSET config:orderflow:<SYM> meta_model_mode=SHADOW

Sends Telegram notification with rollback button (via recs_callback_worker).

Usage:
  python -m tools.of_gate_sre_emergency
  (reads ENV vars for thresholds, cooldown, override keys)
"""

import hashlib
import hmac
import json
import os
import secrets
from collections import Counter
from typing import Any

import redis

from common.log import setup_logger
from common.redis_errors import retry_redis_operation
from core.ok_fields import get_scenario, get_ts_ms, parse_ok_fields
from utils.time_utils import get_ny_time_millis

logger = setup_logger("OfGateSreEmergency")


def now_ms() -> int:
    """Returns current timestamp in milliseconds (epoch)."""
    return get_ny_time_millis()


def pctl(xs: list[float], q: float) -> float:
    """Computes percentile q (0.0-1.0) from sorted list xs."""
    if not xs:
        return 0.0
    xs = sorted(xs)
    i = int(round((len(xs) - 1) * q))
    i = max(0, min(len(xs) - 1, i))
    return float(xs[i])


def _f(x: Any, d: float = 0.0) -> float:
    """Converts value to float with default."""
    try:
        return float(x)
    except Exception:
        return d


def _i(x: Any, d: int = 0) -> int:
    """Converts value to int with default."""
    try:
        return int(float(x))
    except Exception:
        return d


def sign(bundle_id: str, secret: str) -> str:
    """Generates short HMAC signature for bundle_id (8 hex characters)."""
    d = hmac.new(secret.encode("utf-8"), bundle_id.encode("utf-8"), hashlib.sha256).hexdigest()
    return d[:8]


def read_window_xrevrange(r: redis.Redis, stream: str, since_ms: int, *, max_scan: int = 250_000) -> list[dict[str, Any]]:
    """
    Reads messages from Redis stream in reverse chronological order (newest first),
    stopping when timestamp < since_ms.
    
    Args:
        r: Redis client
        stream: Stream name
        since_ms: Minimum timestamp (epoch ms)
        max_scan: Maximum number of messages to scan
        
    Returns:
        List of message dicts (chronological order, oldest first)
    """
    rows: list[dict[str, Any]] = []
    last_id = "+"
    scanned = 0
    while scanned < max_scan:
        batch = r.xrevrange(stream, max=last_id, min="-", count=2000)
        if not batch:
            break
        if len(batch) == 1 and batch[0][0] == last_id:
            break
        for msg_id, fields in batch:
            scanned += 1
            if msg_id == last_id:
                continue
            last_id = msg_id
            ts = get_ts_ms(fields)
            if ts and ts < since_ms:
                scanned = max_scan
                break
            row = dict(fields)
            row["_ts_ms"] = ts
            rows.append(row)
    rows.reverse()
    return rows


def compute_stats(rows: list[dict[str, Any]]) -> dict[str, Any]:
    """
    Computes statistics from metrics rows.
    
    Returns:
        Dict with:
        - n: total count
        - ok_rate: fraction of ok=1
        - soft_rate: fraction of ok_soft=1
        - lat_p99_us: 99th percentile latency (microseconds)
        - exec_p90: 90th percentile exec_risk_norm
        - meta_veto_rate: fraction of meta_veto=1 among rows with meta_p>=0
        - scenario_top: top 6 scenarios by count
    """
    n = len(rows)
    if n == 0:
        return {"n": 0}

    ok = 0
    soft = 0
    lat = []
    ex = []
    meta_veto = 0
    meta_n = 0
    scen = Counter()

    for r in rows:
        ok_i, soft_i = parse_ok_fields(r)
        ok += 1 if ok_i == 1 else 0
        soft += 1 if soft_i == 1 else 0
        lat.append(_f(r.get("latency_us", 0.0), 0.0))
        ex.append(_f(r.get("exec_risk_norm", 0.0), 0.0))

        mp = _f(r.get(MetaKeys.P, -1.0), -1.0)
        if mp >= 0.0:
            meta_n += 1
            meta_veto += 1 if _i(r.get(MetaKeys.VETO, 0), 0) == 1 else 0

        sc = get_scenario(r) or "na"
        scen[sc] += 1

    return {
        "n": n,
        "ok_rate": ok / n if n > 0 else 0.0,
        "soft_rate": soft / n if n > 0 else 0.0,
        "lat_p99_us": pctl(lat, 0.99),
        "exec_p90": pctl(ex, 0.90),
        "meta_veto_rate": (meta_veto / meta_n) if meta_n else 0.0,
        "scenario_top": scen.most_common(6),
    },


def merge_entry_policy_override(old: str, shadow_field: str) -> str:
    """
    Merges entry policy override JSON, setting shadow_field="1".
    
    old may be empty or invalid json; result is JSON with overrides[shadow_field]="1".
    
    Args:
        old: Existing override JSON string (may be empty)
        shadow_field: Field name to set to "1" (e.g. "ENTRY_POLICY_SHADOW")
        
    Returns:
        JSON string with merged overrides
    """
    base = {"version": 1, "overrides": {}}
    try:
        if old:
            d = json.loads(old)
            if isinstance(d, dict):
                base.update({k: d.get(k) for k in ("version",) if k in d})
                ov = d.get("overrides")
                if isinstance(ov, dict):
                    base["overrides"] = ov
    except Exception:
        pass

    base.setdefault("overrides", {})
    base["overrides"][shadow_field] = "1"
    return json.dumps(base, ensure_ascii=False, separators=(",", ":"))


def _retry_redis_operation(operation, max_retries: int = 10, operation_name: str = "Redis operation"):
    """Retry Redis operation with exponential backoff and jitter on BusyLoadingError.
    
    Wrapper around common.redis_errors.retry_redis_operation for backward compatibility.
    """
    return retry_redis_operation(
        operation=operation,
        operation_name=operation_name,
        max_retries=max_retries,
        base_delay=1.0,
        max_delay=30.0,
        logger_instance=logger,
    )


def apply_bundle_auto(r: redis.Redis, ops: list[dict[str, str]], meta: dict[str, Any], who: str, ttl: int, secret: str) -> tuple[str, str]:
    """
    Automatically applies bundle (without preview/confirm flow).
    
    Creates bundle, applies operations, stores audit, sets status APPLIED.
    Returns bundle_id and signature for rollback button.
    
    Args:
        r: Redis client
        ops: List of operation dicts {op, key, field (optional), value}
        meta: Bundle metadata
        who: Creator identifier
        ttl: TTL in seconds
        secret: HMAC secret for signing
        
    Returns:
        (bundle_id, signature) tuple
    """
    bundle_id = secrets.token_hex(6)
    sig = sign(bundle_id, secret)

    bundle = {"id": bundle_id, "created_ms": now_ms(), "ttl_sec": ttl, "who": who, "ops": ops, "meta": meta}

    # Retry Redis operations on BusyLoadingError
    _retry_redis_operation(
        lambda: r.set(f"recs:bundle:{bundle_id}", json.dumps(bundle, ensure_ascii=False, separators=(",", ":")), ex=ttl),
        operation_name="Bundle set"
    )
    _retry_redis_operation(
        lambda: r.set(f"recs:status:{bundle_id}", "PENDING", ex=ttl),
        operation_name="Status set"
    )

    # audit old + apply
    audit = []
    pipe = r.pipeline()
    for op in ops:
        if op["op"] == "SET":
            key = op["key"]
            newv = op["value"]
            old = _retry_redis_operation(
                lambda: r.get(key),
                operation_name=f"GET {key}"
            )
            audit.append({
                "op": "SET",
                "key": key,
                "old": ("" if old is None else str(old)),
                "old_null": (1 if old is None else 0),
                "new": newv
            })
            pipe.set(key, newv)
        elif op["op"] == "HSET":
            key = op["key"]
            field = op["field"]
            newv = op["value"]
            old = _retry_redis_operation(
                lambda: r.hget(key, field),
                operation_name=f"HGET {key}:{field}"
            )
            audit.append({
                "op": "HSET",
                "key": key,
                "field": field,
                "old": ("" if old is None else str(old)),
                "old_null": (1 if old is None else 0),
                "new": newv
            })
            pipe.hset(key, field, newv)

    _retry_redis_operation(
        lambda: pipe.execute(),
        operation_name="Pipeline execute"
    )

    ts = now_ms()
    for a in audit:
        a["ts_ms"] = ts
        a["who"] = who
        _retry_redis_operation(
            lambda: r.rpush(f"recs:audit:{bundle_id}", json.dumps(a, ensure_ascii=False, separators=(",", ":"))),
            operation_name=f"RPUSH audit {bundle_id}"
        )
    _retry_redis_operation(
        lambda: r.expire(f"recs:audit:{bundle_id}", ttl),
        operation_name=f"EXPIRE audit {bundle_id}"
    )
    _retry_redis_operation(
        lambda: r.set(f"recs:status:{bundle_id}", "APPLIED", ex=ttl),
        operation_name="Status APPLIED set"
    )
    return bundle_id, sig


def main() -> None:
    """Main entry point: check metrics, apply emergency shadow if critical."""
    redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
    r = redis.Redis.from_url(redis_url, decode_responses=True)

    stream = os.getenv("OF_GATE_METRICS_STREAM", "metrics:of_gate")
    window_min = float(os.getenv("EMERG_WINDOW_MIN", "10") or 10)
    max_scan = int(os.getenv("EMERG_MAX_SCAN", "250000") or 250000)

    # Critical thresholds
    lat_p99_max = float(os.getenv("EMERG_LAT_P99_US_MAX", "8000") or 8000)
    exec_p90_max = float(os.getenv("EMERG_EXEC_P90_MAX", "0.92") or 0.92)
    soft_max = float(os.getenv("EMERG_SOFT_MAX", "0.60") or 0.60)

    cooldown_sec = int(os.getenv("EMERG_COOLDOWN_SEC", "3600") or 3600)
    cooldown_key = os.getenv("EMERG_COOLDOWN_KEY", "sre:of_gate:emergency:last_ms")

    # Emergency actions
    override_keys = [x.strip() for x in os.getenv("ENTRY_POLICY_OVERRIDE_KEYS", "cfg:entry_policy:overrides:A").split(",") if x.strip()]
    shadow_field = os.getenv("ENTRY_POLICY_SHADOW_FIELD", "ENTRY_POLICY_SHADOW")
    disable_meta = int(os.getenv("EMERG_DISABLE_META_ENFORCE", "1") or 1)
    canary_syms = [s.strip().upper() for s in os.getenv("CANARY_SYMBOLS", "BTCUSDT,ETHUSDT").split(",") if s.strip()]
    cfg_prefix = os.getenv("CFG_HASH_PREFIX", "config:orderflow:")

    secret = os.getenv("RECS_HMAC_SECRET", "CHANGE_ME")
    ttl = int(os.getenv("RECS_TTL_SEC", "86400") or 86400)

    # Helper function to retry Redis operations on BusyLoadingError
    def _redis_get_with_retry(key: str, max_retries: int = 10) -> str | None:
        """Get Redis key with retry on BusyLoadingError."""
        try:
            return retry_redis_operation(
                operation=lambda: r.get(key),
                operation_name="redis_get",
                max_retries=max_retries,
                base_delay=1.0,
                max_delay=30.0,
                on_final_failure=lambda e: None,  # Return None on final failure
                logger_instance=logger,
            )
        except Exception:
            return None

    # cooldown check
    last = _i(_redis_get_with_retry(cooldown_key) or "0", 0)
    if last and (now_ms() - last) < cooldown_sec * 1000:
        logger.debug("Emergency safety in cooldown (last=%d, now=%d, cooldown=%d)", last, now_ms(), cooldown_sec * 1000)
        return

    since_ms = now_ms() - int(window_min * 60_000)
    # Retry read_window_xrevrange with exponential backoff and jitter on BusyLoadingError
    try:
        rows = retry_redis_operation(
            operation=lambda: read_window_xrevrange(r, stream, since_ms, max_scan=max_scan),
            operation_name="read_window_xrevrange",
            max_retries=10,
            base_delay=1.0,
            max_delay=30.0,
            on_final_failure=lambda e: [],  # Return empty list on final failure
            logger_instance=logger,
        )
    except Exception:
        rows = []
    st = compute_stats(rows)

    if st["n"] < 20:
        logger.debug("Not enough metrics: n=%d < 20", st["n"])
        return

    # Check critical thresholds
    critical = []
    if st["lat_p99_us"] > lat_p99_max:
        critical.append(f"lat_p99_us={st['lat_p99_us']:.0f}>{lat_p99_max:.0f}")
    if st["exec_p90"] > exec_p90_max:
        critical.append(f"exec_p90={st['exec_p90']:.2f}>{exec_p90_max:.2f}")
    if st["soft_rate"] > soft_max:
        critical.append(f"soft_rate={st['soft_rate']:.2f}>{soft_max:.2f}")

    if not critical:
        logger.debug("No critical thresholds exceeded: lat_p99=%.0f, exec_p90=%.2f, soft=%.2f",
                     st["lat_p99_us"], st["exec_p90"], st["soft_rate"])
        return

    # Build operations
    ops: list[dict[str, str]] = []
    for k in override_keys:
        old = _redis_get_with_retry(k) or ""
        newv = merge_entry_policy_override(old, shadow_field)
        ops.append({"op": "SET", "key": k, "value": newv})

    if disable_meta == 1:
        for sym in canary_syms:
            ops.append({"op": "HSET", "key": f"{cfg_prefix}{sym}", "field": "meta_model_mode", "value": "SHADOW"})
            # emergency: also reset share to 0.00 (even if mode=ENFORCE somehow remains)
            ops.append({"op": "HSET", "key": f"{cfg_prefix}{sym}", "field": "meta_enforce_share", "value": "0.00"})

    bundle_id, sig = apply_bundle_auto(
        r,
        ops=ops,
        meta={"kind": "emergency_shadow", "critical": critical, "stats": st},
        who="of_gate_sre_emergency",
        ttl=ttl,
        secret=secret,
    )

    _retry_redis_operation(
        lambda: r.set(cooldown_key, str(now_ms()), ex=cooldown_sec),
        operation_name="Cooldown set"
    )

    buttons = [[{"text": "↩ Rollback", "callback": f"recs:rollback:{bundle_id}:{sig}"}]]
    msg = (
        "<b>EMERGENCY SAFETY</b>\n"
        f"reason=<code>{','.join(critical)}</code>\n"
        f"id=<code>{bundle_id}</code>\n"
        f"action=<code>ENTRY_POLICY_SHADOW=1</code> (+ meta->SHADOW)\n"
        f"stats=<code>{json.dumps(st, ensure_ascii=False)}</code>"
    )
    _retry_redis_operation(
        lambda: r.xadd(
            os.getenv("NOTIFY_TELEGRAM_STREAM", RS.NOTIFY_TELEGRAM),
            {
                "type": "report",
                "text": msg,
                "buttons": json.dumps(buttons, ensure_ascii=False, separators=(",", ":")),
                "ts": str(now_ms())
            },
            maxlen=200000,
            approximate=True
        ),
        operation_name="XADD notify:telegram"
    )

    logger.warning("Emergency shadow applied: bundle_id=%s, critical=%s", bundle_id, critical)
    raise SystemExit(2)


if __name__ == "__main__":
    main()

