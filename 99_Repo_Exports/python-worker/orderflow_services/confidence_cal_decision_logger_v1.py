from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import asyncio
import json
import os
import time
import zlib
from typing import Any, Dict, Optional

try:
    from .confidence_cal_metrics import (
        inc_decision_log,
        inc_decision_log_error,
        inc_decision_log_sampled_out,
    )
except Exception:  # pragma: no cover
    def inc_decision_log(*args, **kwargs):
        return None

    def inc_decision_log_error(*args, **kwargs):
        return None

    def inc_decision_log_sampled_out(*args, **kwargs):
        return None


DEFAULT_STREAM = os.environ.get("CONF_CAL_DECISION_LOG_STREAM", "logs:conf_cal:decision")
DEFAULT_MAXLEN = int(os.environ.get("CONF_CAL_DECISION_LOG_MAXLEN", "200000"))
DEFAULT_SAMPLE = float(os.environ.get("CONF_CAL_DECISION_LOG_SAMPLE", "1.0"))


def deterministic_sample(key: str, rate: float) -> bool:
    if rate >= 1.0:
        return True
    if rate <= 0.0:
        return False
    h = zlib.crc32(key.encode("utf-8")) & 0xFFFFFFFF
    return (h / 2**32) < rate


async def _xadd_any(redis: Any, stream: str, payload_json: str, maxlen: int) -> None:
    # redis can be async or sync
    try:
        res = redis.xadd(stream, {"payload": payload_json}, maxlen=maxlen, approximate=True)
        if asyncio.iscoroutine(res):
            await res
    except TypeError:
        # older redis-py may not support approximate kw
        res = redis.xadd(stream, {"payload": payload_json}, maxlen=maxlen)
        if asyncio.iscoroutine(res):
            await res


def schedule_conf_cal_decision_log(
    redis: Any,
    payload: Dict[str, Any],
    *,
    stream: Optional[str] = None,
    maxlen: Optional[int] = None,
    sample_rate: Optional[float] = None,
    symbol="",
    stage: str = "",
    served_arm: str = "",
    mode: str = "",
) -> bool:
    """Schedule an async XADD. Returns True if scheduled, False if skipped.""",
    if redis is None:
        return False

    s = stream or DEFAULT_STREAM
    ml = int(maxlen or DEFAULT_MAXLEN)
    rate = float(sample_rate if sample_rate is not None else DEFAULT_SAMPLE)

    sid = str(payload.get("sid") or "")
    sample_key = sid or f"{payload.get('symbol','')}|{payload.get('ts_ms',0)}"

    if not deterministic_sample(sample_key, rate):
        try:
            inc_decision_log_sampled_out(symbol or payload.get("symbol", ""), stage or payload.get("stage", ""))
        except Exception:
            pass
        return False

    # Ensure minimal metadata
    payload.setdefault("ts_ms", get_ny_time_millis())
    payload.setdefault("schema_version", 1)

    try:
        payload_json = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    except Exception:
        try:
            inc_decision_log_error(symbol, stage, err="json")
        except Exception:
            pass
        return False

    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        # no loop => cannot schedule
        try:
            inc_decision_log_error(symbol, stage, err="no_loop")
        except Exception:
            pass
        return False

    async def _run() -> None:
        try:
            await _xadd_any(redis, s, payload_json, ml)
            try:
                inc_decision_log(
                    symbol or payload.get("symbol", ""),
                    stage or payload.get("stage", ""),
                    served_arm or payload.get("served_arm", ""),
                    mode or payload.get("mode", ""),
                )
            except Exception:
                pass
        except Exception:
            try:
                inc_decision_log_error(symbol or payload.get("symbol", ""), stage or payload.get("stage", ""), err="xadd")
            except Exception:
                pass

    loop.create_task(_run())
    return True
