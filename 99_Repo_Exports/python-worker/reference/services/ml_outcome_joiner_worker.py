from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import json
import os
import time
from typing import Any, Dict, Optional

import redis

from core.redis_client import get_redis, wait_for_redis
from services.ml_pred_cache import get_pred


def now_ms() -> int:
    """Returns current timestamp in milliseconds (epoch)."""
    return get_ny_time_millis()


def _i(x: Any, d: int = 0) -> int:
    """Safe int conversion."""
    try:
        return int(float(x))
    except Exception:
        return d


def _f(x: Any, d: float = 0.0) -> float:
    """Safe float conversion."""
    try:
        return float(x)
    except Exception:
        return d


def _event_ts_ms(fields: Dict[str, Any]) -> int:
    """Extract timestamp from event fields."""
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
    """Extract signal ID from event fields."""
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
    """Extract symbol from event fields."""
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
    """Extract r_mult from event fields."""
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
    """Main worker loop: consume events:trades, join with pred cache, emit metrics:ml_outcome."""
    try:
        r = get_redis(retry_attempts=10, retry_delay=2)
        # Wait for Redis to be fully ready (handles BusyLoading)
        print("⏳ Waiting for Redis to be ready...")
        if not wait_for_redis(r, max_retries=30, delay=10.0):
            print("❌ Redis is still loading after maximum wait time")
            raise RuntimeError("Redis is not ready after waiting")
        print("✅ Redis connected and ready")
    except Exception as e:
        print(f"ERROR: Failed to connect to Redis: {e}")
        raise

    trade_stream = os.getenv("TRADE_EVENTS_STREAM", "events:trades")
    out_stream = os.getenv("ML_OUTCOME_METRICS_STREAM", "metrics:ml_outcome")

    group = os.getenv("ML_OUTCOME_GROUP", "ml-outcome-joiner")
    consumer = os.getenv("ML_OUTCOME_CONSUMER", "c1")

    # label rule: y=1 if r_mult >= R_MIN
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
                    if not isinstance(fields, dict):
                        r.xack(trade_stream, group, msg_id)
                        continue
                    if not _is_closed(fields):
                        r.xack(trade_stream, group, msg_id)
                        continue

                    sid = _get_sid(fields)
                    if not sid:
                        r.xack(trade_stream, group, msg_id)
                        continue

                    pred = get_pred(r, sid)
                    if not pred:
                        # no pred cache - skip (can monitor separately)
                        r.xack(trade_stream, group, msg_id)
                        continue

                    sym = _get_symbol(fields) or str(pred.get("symbol", "")).upper()
                    ts = _event_ts_ms(fields) or int(pred.get("ts_ms", 0) or 0)
                    rmult = _get_r_mult(fields)
                    if rmult is None:
                        r.xack(trade_stream, group, msg_id)
                        continue

                    y = 1 if float(rmult) >= r_min else 0

                    p = float(pred.get("p_edge", 0.0) or 0.0)
                    pch = float(pred.get("p_edge_chal", 0.0) or 0.0)

                    b = _brier(p, y)
                    bch = _brier(pch, y) if pch > 0.0 else 0.0

                    row = {
                        "ts_ms": str(int(ts))
                        "sid": sid
                        "symbol": sym
                        "y": str(int(y))
                        "r_mult": str(float(rmult))
                        "p_edge": str(float(p))
                        "brier": str(float(b))
                        "model_ver": str(pred.get("model_ver", "na"))
                        "enforce": str(int(pred.get("enforce", 0) or 0))
                        "scenario_v4": str(pred.get("scenario_v4", ""))
                    }
                    if float(pred.get("p_edge_chal", 0.0) or 0.0) > 0.0 and str(pred.get("chal_ver", "")):
                        row.update({
                            "p_edge_chal": str(float(pch))
                            "brier_chal": str(float(bch))
                            "chal_ver": str(pred.get("chal_ver", ""))
                        })

                    r.xadd(out_stream, row, maxlen=500000, approximate=True)
                    r.xack(trade_stream, group, msg_id)
                except Exception:
                    # ack to avoid poison loop
                    r.xack(trade_stream, group, msg_id)


if __name__ == "__main__":
    main()

