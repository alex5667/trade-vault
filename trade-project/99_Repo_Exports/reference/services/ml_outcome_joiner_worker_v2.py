from __future__ import annotations

import json
import os
import time
from typing import Any, Dict, Optional

import redis

from services.ml_pred_cache import get_pred
from core.bucket_utils import bucket_from_scenario


def _i(x: Any, d: int = 0) -> int:
    """Safe int conversion."""
    try:
        return int(float(x))
    except Exception:
        return d


def _event_ts_ms(fields: Dict[str, Any]) -> int:
    """Extract event timestamp from fields (supports multiple formats)."""
    return _i(fields.get("ts_ms", fields.get("ts", fields.get("timestamp", 0))), 0)


def _is_closed(fields: Dict[str, Any]) -> bool:
    """Check if event is a position closed event."""
    et = str(fields.get("event_type", fields.get("type", "")) or "").upper()
    if et in ("POSITION_CLOSED", "CLOSE"):
        return True
    p = fields.get("payload")
    if isinstance(p, str) and p and p[0] == "{":
        try:
            j = json.loads(p)
            et2 = str(j.get("event_type", j.get("type", "")) or "").upper()
            return et2 in ("POSITION_CLOSED", "CLOSE")
        except Exception:
            return False
    return False


def _get_sid(fields: Dict[str, Any]) -> str:
    """Extract signal ID from fields (supports nested payload)."""
    sid = str(fields.get("sid", "") or "")
    if sid:
        return sid
    p = fields.get("payload")
    if isinstance(p, str) and p and p[0] == "{":
        try:
            j = json.loads(p)
            return str(j.get("sid", "") or "")
        except Exception:
            return ""
    return ""


def _get_symbol(fields: Dict[str, Any]) -> str:
    """Extract symbol from fields (supports nested payload)."""
    s = str(fields.get("symbol", "") or "").upper()
    if s:
        return s
    p = fields.get("payload")
    if isinstance(p, str) and p and p[0] == "{":
        try:
            j = json.loads(p)
            return str(j.get("symbol", "") or "").upper()
        except Exception:
            return ""
    return ""


def _get_r_mult(fields: Dict[str, Any]) -> Optional[float]:
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
    """Main worker loop: consume events:trades, join with pred cache, emit metrics:ml_outcome (bucket-aware)."""
    redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
    r = redis.Redis.from_url(redis_url, decode_responses=True)

    trade_stream = os.getenv("TRADE_EVENTS_STREAM", "events:trades")
    out_stream = os.getenv("ML_OUTCOME_METRICS_STREAM", "metrics:ml_outcome")

    group = os.getenv("ML_OUTCOME_GROUP", "ml-outcome-joiner-v2")
    consumer = os.getenv("ML_OUTCOME_CONSUMER", "c1")

    r_min = float(os.getenv("ML_LABEL_R_MIN", "0.5") or 0.5)

    def _ensure_group():
        """Ensure consumer group exists, handling race conditions."""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                r.xgroup_create(trade_stream, group, id="0", mkstream=True)
                return
            except redis.exceptions.ResponseError as e:
                error_msg = str(e)
                if "BUSYGROUP" in error_msg:
                    # Group already exists (created by another worker) - this is fine
                    return
                if attempt < max_retries - 1:
                    time.sleep(0.5 * (attempt + 1))
                    continue
                raise
            except Exception:
                if attempt < max_retries - 1:
                    time.sleep(0.5 * (attempt + 1))
                    continue
                raise

    # Initialize consumer group
    _ensure_group()

    while True:
        try:
            resp = r.xreadgroup(group, consumer, {trade_stream: ">"}, count=50, block=5000)
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
                        r.xack(trade_stream, group, msg_id)
                        continue

                    sid = _get_sid(fields)
                    if not sid:
                        r.xack(trade_stream, group, msg_id)
                        continue

                    pred = get_pred(r, sid)
                    if not pred:
                        r.xack(trade_stream, group, msg_id)
                        continue

                    sym = _get_symbol(fields) or str(pred.get("symbol", "")).upper()
                    ts = _event_ts_ms(fields) or int(pred.get("ts_ms", 0) or 0)

                    rmult = _get_r_mult(fields)
                    if rmult is None:
                        r.xack(trade_stream, group, msg_id)
                        continue

                    y = 1 if float(rmult) >= r_min else 0

                    scenario = str(pred.get("scenario_v4", "")) or ""
                    bucket = str(pred.get("bucket", "")) or bucket_from_scenario(scenario)

                    p = float(pred.get("p_edge", 0.0) or 0.0)
                    pch = float(pred.get("p_edge_chal", 0.0) or 0.0)

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
                        "model_ver": str(pred.get("model_ver", "na")),
                        "enforce": str(int(pred.get("enforce", 0) or 0)),
                    }
                    if pch > 0.0 and str(pred.get("chal_ver", "")).strip():
                        row.update({
                            "p_edge_chal": str(float(pch)),
                            "brier_chal": str(float(_brier(pch, y))),
                            "chal_ver": str(pred.get("chal_ver", "")),
                        })

                    r.xadd(out_stream, row, maxlen=500000, approximate=True)
                    r.xack(trade_stream, group, msg_id)

                except Exception:
                    r.xack(trade_stream, group, msg_id)


if __name__ == "__main__":
    main()

