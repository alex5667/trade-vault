from __future__ import annotations

import json
import os
import time
from typing import Any

import redis

from common.ml_labeling import compute_y_and_r_from_closed
from common.redis_errors import retry_redis_operation
from core.redis_keys import STREAM_RETENTION
from core.redis_keys import RedisStreams as RS
from utils.time_utils import get_ny_time_millis
import contextlib


def now_ms() -> int:
    """Returns current timestamp in milliseconds (epoch)."""
    return get_ny_time_millis()


def _f(x: Any, d: float = 0.0) -> float:
    """Safe float conversion with default."""
    try:
        return float(x)
    except Exception:
        return d


def _i(x: Any, d: int = 0) -> int:
    """Safe int conversion with default."""
    try:
        return int(float(x))
    except Exception:
        return d


def _as_str(x: Any) -> str:
    try:
        return str(x)
    except Exception:
        return ""


def get_bucket_from_dec(dec: dict[str, Any]) -> str:
    """Extract bucket from decision cache, default to 'other'."""
    b = _as_str(dec.get("bucket") or "").lower()
    return b if b else "other"


def load_decision(r: redis.Redis, sid: str) -> dict[str, Any] | None:
    """Load ML decision cache from Redis key ml:dec:{sid}."""
    raw = r.get(f"ml:dec:{sid}")
    if not raw:
        return None
    try:
        d = json.loads(raw)
        return d if isinstance(d, dict) else None
    except Exception:
        return None


def _emit_aux_metric(r: redis.Redis, kind: str, fields: dict[str, Any]) -> None:
    try:
        payload = {"ts_ms": str(now_ms()), "kind": str(kind)}
        payload.update({k: _as_str(v) for k, v in fields.items() if v is not None})
        retry_redis_operation(
            operation=lambda: r.xadd(
                "metrics:ml_outcome_emitter",
                payload,
                maxlen=200000,
                approximate=True,
            ),
            operation_name="xadd_metrics_aux",
            max_retries=3,
        )
    except Exception:
        pass


def main() -> None:
    """ML Outcome Emitter Service.

    Reads POSITION_CLOSED events from trades:closed stream,
    joins with ml:dec:{sid} cache to get p_edge and bucket,
    writes outcome metrics to metrics:ml_outcome stream.

    ENV:
        REDIS_URL - Redis connection URL
        TRADES_CLOSED_STREAM - Source stream (default: trades:closed)
        ML_OUTCOME_METRICS_STREAM - Output stream (default: metrics:ml_outcome)
        ML_OUTCOME_GROUP - Consumer group name (default: ml_outcome)
        ML_OUTCOME_CONSUMER - Consumer name (default: c-{pid})
        ML_OUTCOME_BLOCK_MS - XREADGROUP block timeout (default: 5000)
        ML_OUTCOME_COUNT - XREADGROUP count (default: 200)
        ML_OUTCOME_R_MIN - R-multiple threshold for y=1 (default: 0.50)
        ML_OUTCOME_EMIT_MISSING - if '1', emit outcome rows even when ml:dec missing (p_edge=-1)
        ML_OUTCOME_MISSING_DEDUP_TTL_SEC - dedup TTL for missing_decision (default: 3600)
    """

    redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
    r = redis.Redis.from_url(redis_url, decode_responses=True)

    src_stream = os.getenv("TRADES_CLOSED_STREAM", RS.TRADES_CLOSED)
    out_stream = os.getenv("ML_OUTCOME_METRICS_STREAM", RS.ML_OUTCOME_METRICS)

    group = os.getenv("ML_OUTCOME_GROUP", "ml_outcome")
    consumer = os.getenv("ML_OUTCOME_CONSUMER", f"c-{os.getpid()}")

    block_ms = int(os.getenv("ML_OUTCOME_BLOCK_MS", "5000") or 5000)
    count = int(os.getenv("ML_OUTCOME_COUNT", "200") or 200)

    r_min = float(os.getenv("ML_OUTCOME_R_MIN", "0.50") or 0.50)
    emit_missing = (os.getenv("ML_OUTCOME_EMIT_MISSING", "0") or "0").strip() == "1"
    missing_ttl = int(os.getenv("ML_OUTCOME_MISSING_DEDUP_TTL_SEC", "3600") or 3600)

    # Ensure consumer group exists
    try:
        retry_redis_operation(
            operation=lambda: r.xgroup_create(src_stream, group, id="0-0", mkstream=True),
            operation_name="xgroup_create",
            max_retries=5,
        )
    except Exception:
        # Group already exists, ignore
        pass

    processed_total = 0
    missing_decision_total = 0
    skipped_total = 0
    loop_start = time.time()

    while True:
        loop_iter_start = time.time()
        try:
            resp = retry_redis_operation(
                operation=lambda: r.xreadgroup(group, consumer, {src_stream: ">"}, count=count, block=block_ms),
                operation_name="xreadgroup",
                max_retries=10,
            )
        except Exception:
            time.sleep(1)
            continue

        if not resp:
            continue

        for _, entries in resp:
            for msg_id, fields in entries:
                # Filter non-close events if present
                et = _as_str(fields.get("event_type") or "").upper()
                if et and et != "POSITION_CLOSED":
                    skipped_total += 1
                    with contextlib.suppress(Exception):
                        retry_redis_operation(
                            operation=lambda: r.xack(src_stream, group, msg_id),
                            operation_name="xack_skipped",
                            max_retries=3,
                        )
                    continue

                sid = _as_str(fields.get("sid") or "")
                if not sid:
                    skipped_total += 1
                    with contextlib.suppress(Exception):
                        retry_redis_operation(
                            operation=lambda: r.xack(src_stream, group, msg_id),
                            operation_name="xack_no_sid",
                            max_retries=3,
                        )
                    continue

                symbol = _as_str(fields.get("symbol") or "").upper()
                ts_ms = _i(fields.get("exit_ts_ms", fields.get("ts_ms", now_ms())), now_ms())

                # Outcome from closed payload (shared labeling)
                y, r_mult, label_src = compute_y_and_r_from_closed(fields, r_min=r_min)

                # Load decision cache
                dec = load_decision(r, sid)
                if not dec:
                    missing_decision_total += 1

                    # Dedup missing_decision notifications by sid (handles restarts/pending replay)
                    dedup_key = f"ml:outcome:missing:{sid}"
                    try:
                        ok = retry_redis_operation(
                            operation=lambda: r.set(dedup_key, "1", nx=True, ex=missing_ttl),
                            operation_name="set_missing_dedup",
                            max_retries=3,
                        )
                        if ok:
                            _emit_aux_metric(r, "missing_decision", {"sid": sid, "symbol": symbol})
                    except Exception:
                        pass

                    if not emit_missing:
                        with contextlib.suppress(Exception):
                            retry_redis_operation(
                                operation=lambda: r.xack(src_stream, group, msg_id),
                                operation_name="xack_missing",
                                max_retries=3,
                            )
                        continue

                # Extract decision fields (or sentinels)
                p_edge = _f((dec or {}).get("p_edge", -1.0), -1.0)
                bucket = get_bucket_from_dec(dec or {})

                out = {
                    "ts_ms": str(ts_ms),
                    "sid": sid,
                    "symbol": symbol,
                    "bucket": bucket,
                    "p_edge": f"{p_edge:.6f}",
                    "y": str(int(y)),
                    "r_mult": f"{float(r_mult):.6f}",
                    "label_src": str(label_src),
                }

                # Optional close-side debug fields
                for k in ("pnl", "pnl_net", "risk_usd", "reason", "reason_raw"):
                    if k in fields and fields.get(k) is not None:
                        out[f"close_{k}"] = _as_str(fields.get(k))

                # Optional decision-side fields
                if dec:
                    out.update(
                        {
                            "enforce": str(_i(dec.get("enforce", 0), 0)),
                            "ok_rule": str(_i(dec.get("ok_rule", 1), 1)),
                            "missing": str(_i(dec.get("missing", 0), 0)),
                        },
                    )
                    model_ver = dec.get("model_ver", "")
                    if model_ver:
                        out["model_ver"] = _as_str(model_ver)

                with contextlib.suppress(Exception):
                    retry_redis_operation(
                        operation=lambda: r.xadd(out_stream, out, maxlen=STREAM_RETENTION.get(out_stream, STREAM_RETENTION[RS.ML_OUTCOME_METRICS]), approximate=True),
                        operation_name="xadd_outcome",
                        max_retries=5,
                    )

                with contextlib.suppress(Exception):
                    retry_redis_operation(
                        operation=lambda: r.xack(src_stream, group, msg_id),
                        operation_name="xack_success",
                        max_retries=5,
                    )

                processed_total += 1

        loop_latency_ms = (time.time() - loop_iter_start) * 1000.0
        if processed_total % 200 == 0 or (time.time() - loop_start) > 60:
            _emit_aux_metric(
                r,
                "stats",
                {
                    "processed_total": processed_total,
                    "missing_decision_total": missing_decision_total,
                    "skipped_total": skipped_total,
                    "loop_latency_ms": f"{loop_latency_ms:.2f}",
                },
            )
            loop_start = time.time()


if __name__ == "__main__":
    main()
