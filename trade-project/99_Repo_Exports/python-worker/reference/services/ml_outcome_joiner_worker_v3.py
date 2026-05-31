from __future__ import annotations

import json
import os
import time
from typing import Any

import redis

from common.redis_errors import retry_redis_operation
from core.bucket_utils import bucket_from_scenario
from core.redis_client import get_redis
from services.ml_pred_cache import get_pred

# We don't import write_decision_record / etc to avoid circular deps if they import something heavy
# But we need basic struct knowledge or just raw JSON parsing.
# We will just raw parse.


def _i(x: Any, d: int = 0) -> int:
    """Safe int conversion."""
    try:
        return int(float(x))
    except Exception:
        return d


def _event_ts_ms(fields: dict[str, Any]) -> int:
    """Extract event timestamp from fields (supports multiple formats)."""
    return _i(fields.get("ts_ms", fields.get("ts", fields.get("timestamp", 0))), 0)


def _is_closed(fields: dict[str, Any]) -> bool:
    """Check if event is a position closed event."""
    et = (fields.get("event_type", fields.get("type", "")) or "").upper()
    if et in ("POSITION_CLOSED", "CLOSE"):
        return True
    p = fields.get("payload")
    if isinstance(p, str) and p and p[0] == "{":
        try:
            j = json.loads(p)
            et2 = (j.get("event_type", j.get("type", "")) or "").upper()
            return et2 in ("POSITION_CLOSED", "CLOSE")
        except Exception:
            return False
    return False


def _get_sid(fields: dict[str, Any]) -> str:
    """Extract signal ID from fields (supports nested payload)."""
    sid = (fields.get("sid", "") or "")
    if sid:
        return sid
    p = fields.get("payload")
    if isinstance(p, str) and p and p[0] == "{":
        try:
            j = json.loads(p)
            return (j.get("sid", "") or "")
        except Exception:
            return ""
    return ""


def _get_symbol(fields: dict[str, Any]) -> str:
    """Extract symbol from fields (supports nested payload)."""
    s = (fields.get("symbol", "") or "").upper()
    if s:
        return s
    p = fields.get("payload")
    if isinstance(p, str) and p and p[0] == "{":
        try:
            j = json.loads(p)
            return (j.get("symbol", "") or "").upper()
        except Exception:
            return ""
    return ""


def _get_r_mult(fields: dict[str, Any]) -> float | None:
    """Extract r_mult (risk multiplier) from fields."""
    if "r_mult" in fields:
        try:
            return float(fields["r_mult"])
        except Exception:
            return None
    p = fields.get("payload")
    if isinstance(p, str) and p and p[0] == "{":
        try:
            j = json.loads(p)
            if "r_mult" in j:
                return float(j["r_mult"])
        except Exception:
            return None
    return None


def _brier(p: float, y: int) -> float:
    """Compute Brier score: (p - y)^2."""
    return (p - float(y)) ** 2


def main() -> None:
    """Main worker loop: consume events:trades, join with pred cache, emit metrics:ml_outcome (includes exec_risk_norm, bucket)."""
    # Use get_redis() which handles LOADING state with retries
    r = get_redis(retry_attempts=20, retry_delay=2.0)

    trade_stream = os.getenv("TRADE_EVENTS_STREAM", "events:trades")
    out_stream = os.getenv("ML_OUTCOME_METRICS_STREAM", "metrics:ml_outcome")

    group = os.getenv("ML_OUTCOME_GROUP", "ml-outcome-joiner-v3")
    consumer = os.getenv("ML_OUTCOME_CONSUMER", "c1")

    r_min = float(os.getenv("ML_LABEL_R_MIN", "0.5") or 0.5)

    def _ensure_group():
        """Ensure consumer group exists, handling race conditions and Redis LOADING state."""
        def _create_group():
            """Inner function to create group, used with retry_redis_operation."""
            try:
                r.xgroup_create(trade_stream, group, id="0", mkstream=True)
            except redis.exceptions.ResponseError as e:
                error_msg = str(e)
                if "BUSYGROUP" in error_msg:
                    # Group already exists (created by another worker) - this is fine
                    return
                # Re-raise other ResponseErrors to be handled by retry logic
                raise

        # Use retry_redis_operation to handle LOADING state and connection errors
        try:
            retry_redis_operation(
                _create_group,
                operation_name="xgroup_create ml_outcome",
                max_retries=20,
                base_delay=2.0,
                max_delay=30.0,
            )
        except Exception as e:
            # If it's a BUSYGROUP error, that's fine (group already exists)
            if isinstance(e, redis.exceptions.ResponseError) and "BUSYGROUP" in str(e):
                return
            # For other errors, log and re-raise
            print(f"❌ Failed to create consumer group after retries: {e}")
            raise

    # Initialize consumer group
    print("⏳ Ensuring consumer group exists...")
    _ensure_group()
    print("✅ Consumer group ready")

    while True:
        try:
            resp = retry_redis_operation(
                lambda: r.xreadgroup(group, consumer, {trade_stream: ">"}, count=100, block=5000),
                operation_name="xreadgroup ml_outcome",
            )
        except redis.exceptions.ResponseError as e:
            error_msg = str(e)
            if "NOGROUP" in error_msg:
                # Consumer group missing - recreate and retry
                print(f"⚠️ NOGROUP error detected, recreating consumer group: {e}")
                try:
                    _ensure_group()
                    time.sleep(0.2)  # Brief delay before retry
                    continue
                except Exception as create_err:
                    print(f"❌ Failed to recreate consumer group: {create_err}")
                    time.sleep(2.0)
                    continue
            else:
                # Other Redis errors - log and retry
                print(f"⚠️ Redis error in xreadgroup: {e}")
                time.sleep(1.0)
                continue
        except Exception as e:
            # Connection errors, etc.
            print(f"⚠️ Error reading from stream: {e}")
            time.sleep(2.0)
            continue

        if not resp:
            continue

        for _st, msgs in resp:
            for msg_id, fields in msgs:
                try:
                    if not isinstance(fields, dict) or not _is_closed(fields):
                        retry_redis_operation(
                            lambda: r.xack(trade_stream, group, msg_id),
                            operation_name="xack skip",
                        )
                        continue

                    sid = _get_sid(fields)
                    if not sid:
                        retry_redis_operation(
                            lambda: r.xack(trade_stream, group, msg_id),
                            operation_name="xack no_sid",
                        )
                        continue

                    pred = get_pred(r, sid)
                    if not pred:
                        retry_redis_operation(
                            lambda: r.xack(trade_stream, group, msg_id),
                            operation_name="xack no_pred",
                        )
                        continue

                    sym = _get_symbol(fields) or (pred.get("symbol", "")).upper()
                    ts = _event_ts_ms(fields) or int(pred.get("ts_ms", 0) or 0)

                    rmult = _get_r_mult(fields)
                    if rmult is None:
                        retry_redis_operation(
                            lambda: r.xack(trade_stream, group, msg_id),
                            operation_name="xack no_rmult",
                        )
                        continue

                    y = 1 if float(rmult) >= r_min else 0

                    scenario = (pred.get("scenario_v4", "")) or ""
                    bucket = str(pred.get("bucket", "") or bucket_from_scenario(scenario))

                    p = float(pred.get("p_edge", 0.0) or 0.0)
                    pch = float(pred.get("p_edge_chal", 0.0) or 0.0)
                    exec_risk_norm = float(pred.get("exec_risk_norm", 0.0) or 0.0)

                    row = {
                        "ts_ms": str(int(ts)),
                        "sid": sid,
                        "symbol": sym,
                        "scenario_v4": scenario,
                        "bucket": bucket,
                        "y": str(int(y)),
                        "r_mult": str(float(rmult)),
                        "p_edge": str(float(p)),
                        "brier": str(float(_brier(p, y))),
                        "model_ver": (pred.get("model_ver", "na")),
                        "enforce": str(int(pred.get("enforce", 0) or 0)),
                        "share_used": str(float(pred.get("share_used", 0.0) or 0.0)),
                        "p_min": str(float(pred.get("p_min", 0.0) or 0.0)),
                        "exec_risk_norm": str(float(exec_risk_norm)),
                    }
                    if pch > 0.0 and (pred.get("chal_ver", "")).strip():
                        row.update({
                            "p_edge_chal": str(float(pch)),
                            "brier_chal": str(float(_brier(pch, y))),
                            "chal_ver": (pred.get("chal_ver", "")),
                        })

                    # P69: Enrich with policy mode from decision record
                    try:
                        dec_raw = r.get(f"decision:{sid}")
                        if dec_raw:
                            dec = json.loads(dec_raw)
                            row["policy_mode"] = str(dec.get("policy_effective_mode", "") or dec.get("policy_regime", "") or "na")
                            row["policy_raw"] = (dec.get("policy_raw_mode", "") or "na")
                        else:
                             row["policy_mode"] = "na"
                             row["policy_raw"] = "na"
                    except Exception:
                         row["policy_mode"] = "error"
                         row["policy_raw"] = "error"

                    retry_redis_operation(
                        lambda: r.xadd(out_stream, row, maxlen=700000, approximate=True),
                        operation_name="xadd ml_outcome",
                    )
                    retry_redis_operation(
                        lambda: r.xack(trade_stream, group, msg_id),
                        operation_name="xack success",
                    )

                except Exception:
                    try:
                        retry_redis_operation(
                            lambda: r.xack(trade_stream, group, msg_id),
                            operation_name="xack exception",
                        )
                    except Exception:
                        pass  # Fail-open: if ack fails, continue processing


if __name__ == "__main__":
    main()

