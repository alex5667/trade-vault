"""OFInputs DLQ fixed replay (P97).

Goal:
- Provide a safe, deterministic replay path for OFInputs publish-failure DLQ.
- This is *separate* from of_gate DLQ logic.

DLQ format (produced by tick_processor P96):
- DLQ stream field: {"payload": "{...json...}"}
- Inside JSON: {ts_ms, symbol, stream, attempt_version, published_version, dq_code, err_prefix, err, payload}
  where `payload` is the original OFInputs JSON string.

Safety rules:
- Replay only if the nested `payload` is valid JSON and contains minimal keys.
- Replay writes to target stream (ctx.stream by default, override via env).
- Optional dry-run mode.

State:
- Writes hash `state:of_inputs_dlq_replay:last` with last status for exporter/alerts.

Env:
- REDIS_URL (required)
- OF_INPUTS_DLQ_STREAM (default: stream:dlq:of_inputs)
- OF_INPUTS_DLQ_GROUP (default: of_inputs_dlq_replay)
- OF_INPUTS_DLQ_CONSUMER (default: host:pid)
- OF_INPUTS_DLQ_COUNT (default: 200)
- OF_INPUTS_DLQ_MAX_REPLAY (default: 5000)
- OF_INPUTS_DLQ_COMMIT (default: 0)  # 1 to actually XADD
- OF_INPUTS_DLQ_DELETE_AFTER (default: 0)  # 1 to XDEL after successful replay
- OF_INPUTS_DLQ_TARGET_STREAM (optional)  # overrides ctx.stream
- OF_INPUTS_DLQ_TARGET_MAXLEN (default: 50000)
- OF_INPUTS_DLQ_EXIT2_ON_FAIL (default: 1)

Usage:
  python -m orderflow_services.of_inputs_dlq_fixed_replay_p97
"""

from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import json
import os
import socket
import sys
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import redis


STATE_KEY = "state:of_inputs_dlq_replay:last"


def _now_ms() -> int:
    return get_ny_time_millis()


def _consumer_name() -> str:
    host = socket.gethostname() or "host"
    return f"{host}:{os.getpid()}"


def _safe_json_load(s: str) -> Optional[Dict[str, Any]]:
    if not s:
        return None
    try:
        d = json.loads(s)
        return d if isinstance(d, dict) else None
    except Exception:
        return None


def _payload_min_ok(payload_obj: Dict[str, Any]) -> bool:
    # Minimal keys to ensure it looks like OFInputs
    # We avoid strict schema validation here to keep replay resilient.
    for k in ("v", "symbol", "ts_ms"):
        if k not in payload_obj:
            return False
    return True


@dataclass
class ReplayResult:
    replayed: int = 0
    skipped: int = 0
    failed: int = 0
    last_err: str = ""


def _write_state(r: redis.Redis, ok: bool, dur_ms: int, res: ReplayResult) -> None:
    try:
        now_ms = _now_ms()
        mapping = {
            "last_run_ok": "1" if ok else "0"
            "last_run_ts_ms": str(now_ms)
            "last_dur_ms": str(int(dur_ms))
            "replayed": str(int(res.replayed))
            "skipped": str(int(res.skipped))
            "failed": str(int(res.failed))
            "last_err": (res.last_err or "")[:512]
        }
        # Preserve last successful timestamp across failing runs.
        if ok:
            mapping["last_ok"] = "1"
            mapping["last_ok_ts_ms"] = str(now_ms)
        else:
            mapping["last_ok"] = "0"
        r.hset(STATE_KEY, mapping=mapping)
        r.expire(STATE_KEY, 7 * 24 * 3600)
    except Exception:
        pass


def _ensure_group(r: redis.Redis, stream: str, group: str) -> None:
    try:
        r.xgroup_create(name=stream, groupname=group, id="0-0", mkstream=True)
    except Exception as e:
        # BUSYGROUP is ok
        if "BUSYGROUP" in str(e):
            return
        raise


def _claim_pending(
    r: redis.Redis
    stream: str
    group: str
    consumer: str
    min_idle_ms: int
    count: int
) -> List[Tuple[str, Dict[str, str]]]:
    # Use XAUTOCLAIM when available.
    try:
        resp = r.execute_command("XAUTOCLAIM", stream, group, consumer, str(min_idle_ms), "0-0", "COUNT", str(count))
        # resp: [next_start, [ [id, {fields}], ... ], deleted]
        msgs = resp[1] if isinstance(resp, (list, tuple)) and len(resp) > 1 else []
        out: List[Tuple[str, Dict[str, str]]] = []
        for m in msgs or []:
            try:
                mid = m[0]
                fields = m[1]
                if isinstance(mid, bytes):
                    mid = mid.decode()
                if isinstance(fields, dict):
                    dec = {}
                    for k, v in fields.items():
                        if isinstance(k, bytes):
                            k = k.decode()
                        if isinstance(v, bytes):
                            v = v.decode()
                        dec[str(k)] = str(v)
                    out.append((str(mid), dec))
            except Exception:
                continue
        return out
    except Exception:
        return []


def _read_new(
    r: redis.Redis
    stream: str
    group: str
    consumer: str
    count: int
    block_ms: int
) -> List[Tuple[str, Dict[str, str]]]:
    try:
        resp = r.xreadgroup(groupname=group, consumername=consumer, streams={stream: ">"}, count=count, block=block_ms)
        out: List[Tuple[str, Dict[str, str]]] = []
        for _s, msgs in resp or []:
            for mid, fields in msgs or []:
                dec: Dict[str, str] = {}
                for k, v in (fields or {}).items():
                    if isinstance(k, bytes):
                        k = k.decode()
                    if isinstance(v, bytes):
                        v = v.decode()
                    dec[str(k)] = str(v)
                if isinstance(mid, bytes):
                    mid = mid.decode()
                out.append((str(mid), dec))
        return out
    except Exception:
        return []


def main() -> int:
    t0 = _now_ms()

    redis_url = os.environ.get("REDIS_URL")
    if not redis_url:
        print("REDIS_URL is required", file=sys.stderr)
        return 2

    dlq_stream = os.environ.get("OF_INPUTS_DLQ_STREAM", "stream:dlq:of_inputs")
    group = os.environ.get("OF_INPUTS_DLQ_GROUP", "of_inputs_dlq_replay")
    consumer = os.environ.get("OF_INPUTS_DLQ_CONSUMER", "") or _consumer_name()

    count = int(os.environ.get("OF_INPUTS_DLQ_COUNT", "200"))
    max_replay = int(os.environ.get("OF_INPUTS_DLQ_MAX_REPLAY", "5000"))

    commit = os.environ.get("OF_INPUTS_DLQ_COMMIT", "0") in ("1", "true", "yes", "on")
    delete_after = os.environ.get("OF_INPUTS_DLQ_DELETE_AFTER", "0") in ("1", "true", "yes", "on")

    target_stream_override = os.environ.get("OF_INPUTS_DLQ_TARGET_STREAM")
    target_maxlen = int(os.environ.get("OF_INPUTS_DLQ_TARGET_MAXLEN", "50000"))

    exit2_on_fail = os.environ.get("OF_INPUTS_DLQ_EXIT2_ON_FAIL", "1") in ("1", "true", "yes", "on")

    # Optional: also claim pending messages (stuck)
    claim_min_idle_ms = int(os.environ.get("OF_INPUTS_DLQ_CLAIM_MIN_IDLE_MS", str(10 * 60 * 1000)))

    r = redis.Redis.from_url(redis_url, decode_responses=False)

    res = ReplayResult()
    ok = True

    try:
        _ensure_group(r, dlq_stream, group)

        processed = 0
        while processed < max_replay:
            # First, try to reclaim pending (stuck) messages
            msgs = _claim_pending(r, dlq_stream, group, consumer, min_idle_ms=claim_min_idle_ms, count=min(count, max_replay - processed))
            if not msgs:
                msgs = _read_new(r, dlq_stream, group, consumer, count=min(count, max_replay - processed), block_ms=200)

            if not msgs:
                break

            for mid, fields in msgs:
                processed += 1

                raw = fields.get("payload")
                if raw is None:
                    res.skipped += 1
                    try:
                        r.xack(dlq_stream, group, mid)
                    except Exception:
                        pass
                    continue

                if isinstance(raw, bytes):
                    raw_s = raw.decode("utf-8", errors="replace")
                else:
                    raw_s = str(raw)

                ctx = _safe_json_load(raw_s)
                if not ctx:
                    res.skipped += 1
                    try:
                        r.xack(dlq_stream, group, mid)
                    except Exception:
                        pass
                    continue

                payload_str = ctx.get("payload")
                if not isinstance(payload_str, str) or not payload_str:
                    res.skipped += 1
                    try:
                        r.xack(dlq_stream, group, mid)
                    except Exception:
                        pass
                    continue

                pobj = _safe_json_load(payload_str)
                if not pobj or not _payload_min_ok(pobj):
                    # Keep it in DLQ for manual triage
                    res.skipped += 1
                    try:
                        r.xack(dlq_stream, group, mid)
                    except Exception:
                        pass
                    continue

                target_stream = str(target_stream_override or ctx.get("stream") or "signals:of:inputs")

                if not commit:
                    res.replayed += 1
                    try:
                        r.xack(dlq_stream, group, mid)
                    except Exception:
                        pass
                    continue

                try:
                    r.xadd(
                        target_stream
                        fields={"payload": payload_str, "replay": "1", "dlq_id": mid}
                        maxlen=target_maxlen
                        approximate=True
                    )
                    res.replayed += 1
                    try:
                        r.xack(dlq_stream, group, mid)
                    except Exception:
                        pass
                    if delete_after:
                        try:
                            r.xdel(dlq_stream, mid)
                        except Exception:
                            pass
                except Exception as e:
                    ok = False
                    res.failed += 1
                    res.last_err = f"{type(e).__name__}: {e}"
                    # Do not ack on failure, so it stays pending for future auto-claim.

            if processed >= max_replay:
                break

    except Exception as e:
        ok = False
        res.last_err = f"{type(e).__name__}: {e}"

    dur_ms = _now_ms() - t0
    try:
        _write_state(r, ok=ok, dur_ms=dur_ms, res=res)
    except Exception:
        pass

    summary = {
        "ok": ok
        "commit": commit
        "dlq_stream": dlq_stream
        "target_stream": target_stream_override or "<ctx.stream>"
        "replayed": res.replayed
        "skipped": res.skipped
        "failed": res.failed
        "dur_ms": dur_ms
        "last_err": res.last_err
    }
    print(json.dumps(summary, ensure_ascii=False, sort_keys=True))

    if not ok and exit2_on_fail:
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
