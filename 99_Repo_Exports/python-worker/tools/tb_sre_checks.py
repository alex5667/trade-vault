from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import os
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple


try:
    import redis  # type: ignore
except Exception:  # pragma: no cover
    redis = None

try:
    from redis.exceptions import BusyLoadingError, ConnectionError
except ImportError:
    BusyLoadingError = Exception
    ConnectionError = Exception


def _now_ms() -> int:
    return get_ny_time_millis()


def _safe_int(x: Any, default: int = 0) -> int:
    try:
        if x is None:
            return default
        if isinstance(x, (bytes, bytearray)):
            x = x.decode("utf-8", "ignore")
        return int(float(x))
    except Exception:
        return default


def _parse_stream_id_ms(stream_id: str) -> int:
    # Redis stream IDs: "<ms>-<seq>"
    try:
        return int(stream_id.split("-", 1)[0])
    except Exception:
        return 0


@dataclass
class TBHealth:
    ok: bool
    reason: str
    now_ms: int
    last_input_ts_ms: int
    last_label_ts_ms: int
    last_err_ts_ms: int
    input_lag_ms: int
    label_stale_ms: int
    pending: int
    group_lag_ms: int


def check_tb_health(
    *,
    redis_url: Optional[str] = None,
    input_stream: str = "signals:of:inputs",
    labels_stream: str = "labels:tb",
    group: Optional[str] = None,
    max_input_lag_ms: int = 120_000,
    max_label_stale_ms: int = 300_000,
    max_pending: int = 5_000,
) -> TBHealth:
    """
    Lightweight TB labeler health check.
    Uses keys set by P3/P4: tb:last_ts_ms, tb:last_label_ts_ms, tb:last_err_ts_ms
    Optionally checks consumer group lag/pending if group is provided.
    """
    if redis is None:
        return TBHealth(
            ok=False,
            reason="redis_import_error",
            now_ms=_now_ms(),
            last_input_ts_ms=0,
            last_label_ts_ms=0,
            last_err_ts_ms=0,
            input_lag_ms=0,
            label_stale_ms=0,
            pending=0,
            group_lag_ms=0,
        )

    redis_url = redis_url or os.getenv("REDIS_URL") or os.getenv("TB_REDIS_URL") or "redis://localhost:6379/0"
    group = group or os.getenv("OF_INPUTS_GROUP")  # in P4

    try:
        r = redis.Redis.from_url(redis_url, decode_responses=False)
        now_ms = _now_ms()

        last_input_ts_ms = _safe_int(r.get("tb:last_ts_ms"), 0)
        last_label_ts_ms = _safe_int(r.get("tb:last_label_ts_ms"), 0)
        last_err_ts_ms = _safe_int(r.get("tb:last_err_ts_ms"), 0)

        input_lag_ms = now_ms - last_input_ts_ms if last_input_ts_ms > 0 else 0
        label_stale_ms = now_ms - last_label_ts_ms if last_label_ts_ms > 0 else 0

        pending = 0
        group_lag_ms = 0
        if group:
            try:
                # XPENDING summary
                xp = r.xpending(input_stream, group)
                if isinstance(xp, dict):
                    pending = _safe_int(xp.get("pending"), 0)
                elif isinstance(xp, (list, tuple)) and len(xp) >= 1:
                    pending = _safe_int(xp[0], 0)

                # XINFO GROUPS for last-delivered-id
                groups = r.execute_command("XINFO", "GROUPS", input_stream)
                last_delivered = None
                for g in groups or []:
                    # g is list alternating key/value, or dict-like depending on redis-py
                    if isinstance(g, dict):
                        if (g.get("name") or b"").decode() == group:
                            last_delivered = g.get("last-delivered-id")
                    elif isinstance(g, (list, tuple)):
                        # decode alternating fields
                        gd = {}
                        for i in range(0, len(g) - 1, 2):
                            k = g[i]
                            v = g[i + 1]
                            if isinstance(k, (bytes, bytearray)):
                                k = k.decode("utf-8", "ignore")
                            gd[str(k)] = v
                        if (gd.get("name") or b"").decode("utf-8", "ignore") == group:
                            last_delivered = gd.get("last-delivered-id")
                    if last_delivered:
                        break
                if last_delivered:
                    if isinstance(last_delivered, (bytes, bytearray)):
                        last_delivered = last_delivered.decode("utf-8", "ignore")
                    group_lag_ms = now_ms - _parse_stream_id_ms(str(last_delivered))
                else:
                    group_lag_ms = 0
            except Exception:
                # group check best-effort
                pending = pending or 0
                group_lag_ms = group_lag_ms or 0
    except (BusyLoadingError, ConnectionError) as e:
        return TBHealth(
            ok=False,
            reason="redis_loading" if isinstance(e, BusyLoadingError) else "redis_connection_error",
            now_ms=_now_ms(),
            last_input_ts_ms=0,
            last_label_ts_ms=0,
            last_err_ts_ms=0,
            input_lag_ms=0,
            label_stale_ms=0,
            pending=0,
            group_lag_ms=0,
        )

    # Also ensure labels stream exists (best effort)
    try:
        _ = r.xinfo_stream(labels_stream)
    except Exception:
        # if missing, treat as unhealthy
        return TBHealth(
            ok=False,
            reason="labels_stream_missing",
            now_ms=now_ms,
            last_input_ts_ms=last_input_ts_ms,
            last_label_ts_ms=last_label_ts_ms,
            last_err_ts_ms=last_err_ts_ms,
            input_lag_ms=input_lag_ms,
            label_stale_ms=label_stale_ms,
            pending=pending,
            group_lag_ms=group_lag_ms,
        )

    if last_label_ts_ms <= 0:
        return TBHealth(
            ok=False,
            reason="labels_never_written",
            now_ms=now_ms,
            last_input_ts_ms=last_input_ts_ms,
            last_label_ts_ms=0,
            last_err_ts_ms=last_err_ts_ms,
            input_lag_ms=input_lag_ms,
            label_stale_ms=0,
            pending=pending,
            group_lag_ms=group_lag_ms,
        )

    if input_lag_ms > max_input_lag_ms and (pending > 0 or group_lag_ms > 5000):
        return TBHealth(
            ok=False,
            reason=f"input_lag_ms>{max_input_lag_ms}",
            now_ms=now_ms,
            last_input_ts_ms=last_input_ts_ms,
            last_label_ts_ms=last_label_ts_ms,
            last_err_ts_ms=last_err_ts_ms,
            input_lag_ms=input_lag_ms,
            label_stale_ms=label_stale_ms,
            pending=pending,
            group_lag_ms=group_lag_ms,
        )
    if label_stale_ms > max_label_stale_ms and (pending > 0 or group_lag_ms > 5000 or label_stale_ms > (input_lag_ms + 300000)):
        return TBHealth(
            ok=False,
            reason=f"label_stale_ms>{max_label_stale_ms}",
            now_ms=now_ms,
            last_input_ts_ms=last_input_ts_ms,
            last_label_ts_ms=last_label_ts_ms,
            last_err_ts_ms=last_err_ts_ms,
            input_lag_ms=input_lag_ms,
            label_stale_ms=label_stale_ms,
            pending=pending,
            group_lag_ms=group_lag_ms,
        )
    if group and pending > max_pending:
        return TBHealth(
            ok=False,
            reason=f"pending>{max_pending}",
            now_ms=now_ms,
            last_input_ts_ms=last_input_ts_ms,
            last_label_ts_ms=last_label_ts_ms,
            last_err_ts_ms=last_err_ts_ms,
            input_lag_ms=input_lag_ms,
            label_stale_ms=label_stale_ms,
            pending=pending,
            group_lag_ms=group_lag_ms,
        )

    return TBHealth(
        ok=True,
        reason="ok",
        now_ms=now_ms,
        last_input_ts_ms=last_input_ts_ms,
        last_label_ts_ms=last_label_ts_ms,
        last_err_ts_ms=last_err_ts_ms,
        input_lag_ms=input_lag_ms,
        label_stale_ms=label_stale_ms,
        pending=pending,
        group_lag_ms=group_lag_ms,
    )
