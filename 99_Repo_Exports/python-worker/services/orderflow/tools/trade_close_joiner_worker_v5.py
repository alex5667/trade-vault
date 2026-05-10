from __future__ import annotations

from domain.evidence_keys import MetaKeys
from core.redis_keys import RedisStreams as RS

"""
Trade Close Joiner Worker (v5)

Consumes: TRADE_EVENTS_STREAM (default: events:trades) where each entry has:
  - payload: JSON string (preferred)
Writes:
  - TRADES_CLOSED_STREAM (default: trades:closed) with a single field:
      payload: JSON string (enriched)
Also optionally writes:
  - ML_REPLAY_INPUTS_STREAM (default: ml_replay_inputs_v1)

Enrichment fields added to trades:closed payload:
  - dq_state, drift_state, drift_mode
  - rule_reason_code_top1
  - meta_enforce_cov_bucket, meta_enforce_applied

Reliability:
  - Dedup by position_id (or sid+close_ts_ms) via SETNX with TTL
  - If decision:{sid} missing at close time -> push to CLOSE_WAIT_STREAM for retry
"""

import asyncio
import json
import os
from typing import Any

import redis.asyncio as aioredis
from prometheus_client import Counter, Gauge, start_http_server

from core.redis_stream_consumer import AsyncRedisStreamHelper
from utils.time_utils import get_ny_time_millis

_join_runs_total = Counter("trade_close_joiner_runs_total", "Joiner loop runs", ["result"])
_join_events_total = Counter("trade_close_joiner_events_total", "Events processed", ["type", "result"])
_close_wait_total = Counter("trade_close_joiner_close_wait_total", "Close events sent to wait stream", ["reason"])
_trades_closed_written_total = Counter("trade_close_joiner_trades_closed_written_total", "Closed trades written", ["result"])
_trades_closed_dedup_total = Counter("trade_close_joiner_trades_closed_dedup_total", "Dedup drops", ["reason"])
_last_ok_ts_ms = Gauge("trade_close_joiner_last_ok_ts_ms", "Last successful join timestamp (ms)")

def _now_ms() -> int:
    return get_ny_time_millis()

def _env(name: str, default: str) -> str:
    v = os.getenv(name)
    return v if (v is not None and str(v).strip() != "") else default

def _env_int(name: str, default: str) -> int:
    try:
        return int(_env(name, default))
    except Exception:
        return default

def _env_float(name: str, default: str) -> float:
    try:
        return float(_env(name, default))
    except Exception:
        return default

def _loads_json(s: Any) -> dict[str, Any]:
    if s is None:
        return {}
    if isinstance(s, dict):
        return s
    try:
        return json.loads(s)
    except Exception:
        return {}

def _get_payload(fields: dict[str, Any]) -> dict[str, Any]:
    # Preferred: single "payload" field with JSON
    p = fields.get("payload")
    if p:
        d = _loads_json(p)
        if isinstance(d, dict):
            return d
    # Fallback: treat fields as flat
    return dict(fields)

def _norm_state(v: Any) -> str:
    if v is None:
        return "unknown"
    if isinstance(v, str):
        s = v.strip().lower()
        if s in ("ok", "warn", "warning", "block", "blocked", "unknown"):
            return "warn" if s == "warning" else ("block" if s == "blocked" else s)
        if s.isdigit():
            v = int(s)
        else:
            return "unknown"
    if isinstance(v, (int, float)):
        i = int(v)
        return {0: "ok", 1: "warn", 2: "block", 3: "unknown"}.get(i, "unknown")
    return "unknown"

def _drift_mode(drift_state: Any, actual_action: str, actual_reason: str) -> str:
    ds = _norm_state(drift_state)
    if ds == "block":
        if actual_action == "emit" and "RULE_STRONG_ONLY_PASS" in (actual_reason or ""):
            return "block_strong_pass"
        return "block"
    if ds == "warn":
        return "warn"
    if ds == "ok":
        return "ok"
    return "unknown"

def _pick(d: dict[str, Any], *keys: str) -> Any | None:
    for k in keys:
        if k in d and d[k] is not None:
            return d[k]
    return None

async def _setnx_dedup(r: aioredis.Redis, key: str, ttl_sec: int) -> bool:
    try:
        ok = await r.set(key, "1", nx=True, ex=ttl_sec)
        return bool(ok)
    except Exception:
        # fail-open: if dedup fails, do not drop
        return True

async def _write_trades_closed(r: aioredis.Redis, stream: str, payload: dict[str, Any], maxlen: int) -> None:
    await r.xadd(stream, {"payload": json.dumps(payload, ensure_ascii=False)}, maxlen=maxlen, approximate=True)

async def _write_ml_replay(r: aioredis.Redis, stream: str, payload: dict[str, Any], maxlen: int) -> None:
    await r.xadd(stream, {"payload": json.dumps(payload, ensure_ascii=False)}, maxlen=maxlen, approximate=True)

async def _push_close_wait(r: aioredis.Redis, stream: str, close_payload: dict[str, Any], reason: str, maxlen: int) -> None:
    doc = {
        "ts_ms": _now_ms(),
        "reason": reason,
        "close": close_payload,
    }
    await r.xadd(stream, {"payload": json.dumps(doc, ensure_ascii=False)}, maxlen=maxlen, approximate=True)

async def _load_of_input(
    r: aioredis.Redis,
    sid: str,
    *,
    stream: str,
    field: str,
    sid_index_prefix: str,
    scan_count: int,
) -> dict[str, Any] | None:
    """Fetch the originating OF input by SID from an OF inputs stream.

    Index path: GET {sid_index_prefix}{sid} -> stream_id, then XRANGE stream_id..stream_id.
    Fallback: bounded tail scan (XREVRANGE count=scan_count).

    Returns decoded JSON payload (dict) or None.
    """
    stream_id: str | None = None
    try:
        stream_id = await r.get(f"{sid_index_prefix}{sid}")
    except Exception:
        stream_id = None

    if stream_id:
        try:
            msgs = await r.xrange(stream, min=stream_id, max=stream_id, count=1)
        except Exception:
            msgs = []
        if msgs:
            _id, fields = msgs[0]
            raw = fields.get(field) if isinstance(fields, dict) else None
            o = _loads_json(raw)
            if isinstance(o, dict) and (o.get("sid") or "") == sid:
                return o

    try:
        msgs = await r.xrevrange(stream, max="+", min="-", count=int(scan_count))
    except Exception:
        return None

    for _id, fields in msgs:
        if not isinstance(fields, dict):
            continue
        raw = fields.get(field)
        o = _loads_json(raw)
        if not isinstance(o, dict):
            continue
        if (o.get("sid") or "") == sid:
            try:
                # Cache the stream id for future look-ups (TTL = 3 days)
                await r.set(f"{sid_index_prefix}{sid}", _id, ex=3 * 24 * 3600)
            except Exception:
                pass
            return o

    return None

async def _handle_close(
    r: aioredis.Redis,
    close_payload: dict[str, Any],
    *,
    decision_prefix: str,
    trades_closed_stream: str,
    trades_closed_maxlen: int,
    close_wait_stream: str,
    close_wait_maxlen: int,
    dedup_ttl_sec: int,
    ml_replay_stream: str,
    ml_replay_maxlen: int,
    write_ml_replay: bool,
    of_inputs_stream: str,
    of_inputs_field: str,
    of_inputs_sid_index_prefix: str,
    of_inputs_scan_count: int,
) -> tuple[bool, str]:
    sid = _pick(close_payload, "sid", "SID", "signal_id")
    if not sid:
        return False, "no_sid"

    decision_raw = await r.get(f"{decision_prefix}{sid}")
    if not decision_raw:
        await _push_close_wait(r, close_wait_stream, close_payload, "missing_decision", close_wait_maxlen)
        _close_wait_total.labels(reason="missing_decision").inc()
        return False, "missing_decision"

    decision = _loads_json(decision_raw)
    # Dedup key
    position_id = _pick(close_payload, "position_id", "positionId", "ticket", "id")
    close_ts_ms = _pick(close_payload, "close_ts_ms", "ts_ms", "closed_ts_ms")
    dedup_id = position_id or f"{sid}:{close_ts_ms or ''}"
    if not dedup_id:
        dedup_id = sid

    if not await _setnx_dedup(r, f"dedup:trades_closed:{dedup_id}", dedup_ttl_sec):
        _trades_closed_dedup_total.labels(reason="setnx_failed").inc()

    # Merge + enrichment
    out: dict[str, Any] = dict(close_payload)

    # Meta enforcement (from close payload preferred, else from decision)
    out[MetaKeys.ENFORCE_COV_BUCKET] = _pick(close_payload, "meta_enforce_cov_bucket", "meta_cov_bucket", "meta_enforce_bucket") or _pick(
        decision, "meta_enforce_cov_bucket", "meta_cov_bucket"
    )
    out[MetaKeys.ENFORCE_APPLIED] = _pick(close_payload, "meta_enforce_applied", "meta_applied", "meta_enforce_apply") or _pick(
        decision, "meta_enforce_applied", "meta_applied"
    )

    # DQ/Drift + binding/actual
    dq_state = _pick(decision, "dq_state", "dq_state_24h")
    drift_state = _pick(decision, "drift_state", "drift_state_24h")
    out["dq_state"] = _norm_state(dq_state)
    out["drift_state"] = _norm_state(drift_state)
    actual_action = str(_pick(decision, "actual_action") or "unknown")
    actual_reason = str(_pick(decision, "actual_reason_code") or "")
    out["drift_mode"] = _drift_mode(drift_state, actual_action, actual_reason)

    # Rule reason
    out["rule_reason_code_top1"] = _pick(decision, "rule_reason_code_top1", "rule_reason_top1", "reason_code_top1") or "na"

    # Useful join fields
    out["decision_ts_ms"] = _pick(decision, "decision_ts_ms", "ts_ms")
    out["actual_action"] = actual_action
    out["actual_reason_code"] = actual_reason

    # -----------------------------------------------------------------------
    # Calibration / shadow trade field propagation (outcome loop)
    #
    # Priority cascade:  close_payload → signal_payload(close) → signal_payload(decision) → decision
    # These fields let calibrators (cont_ctx_window, adverse_gate, etc.)
    # match shadow signals with real trade outcomes in trades:closed.
    # -----------------------------------------------------------------------
    try:
        from services.shadow_calib_meta import merge_calib_fields
        # Extract nested signal_payload from close or decision (may be JSON string)
        sp_from_close = _loads_json(close_payload.get("signal_payload")) if isinstance(close_payload.get("signal_payload"), str) else (close_payload.get("signal_payload") or {})
        sp_from_decision = _loads_json(decision.get("signal_payload")) if isinstance(decision.get("signal_payload"), str) else (decision.get("signal_payload") or {})
        merge_calib_fields(out, close_payload, sp_from_close, sp_from_decision, decision)
    except Exception:
        pass  # fail-open: never break joiner on calib import

    await _write_trades_closed(r, trades_closed_stream, out, trades_closed_maxlen)
    _trades_closed_written_total.labels(result="ok").inc()

    if write_ml_replay:
        # Try to load the original OF input payload (flat feature snapshot produced at signal time).
        # This gives dataset builders the full indicator vector for train≠serve parity.
        of_input = await _load_of_input(
            r,
            sid,
            stream=of_inputs_stream,
            field=of_inputs_field,
            sid_index_prefix=of_inputs_sid_index_prefix,
            scan_count=of_inputs_scan_count,
        )

        replay_payload: dict[str, Any]
        if isinstance(of_input, dict):
            # Use the original OF input as the base — has all features at signal time.
            replay_payload = dict(of_input)
        else:
            # Fallback: reconstruct minimal payload from decision + close (Commit 8 path).
            replay_payload = {
                "sid": sid,
                "ts_ms": _pick(decision, "ts_ms", "decision_ts_ms") or _pick(close_payload, "ts_ms", "close_ts_ms"),
                "symbol": _pick(decision, "symbol"),
                "direction": _pick(decision, "direction"),
                "scenario_v4": _pick(decision, "scenario_v4", "scenario") or "other",
                "indicators": {},
            }

        # Always stamp label, close summary, and source regardless of which path was taken.
        replay_payload["label"] = {
            "r_mult": _pick(close_payload, "r_mult", "r"),
            "close_ts_ms": close_ts_ms,
        }
        replay_payload["close"] = {
            "pnl_usd": _pick(close_payload, "pnl_usd", "pnl"),
            "risk_usd": _pick(close_payload, "risk_usd"),
            "close_ts_ms": close_ts_ms,
        }
        replay_payload["_source"] = "trade_close_joiner_worker_v5"

        await _write_ml_replay(r, ml_replay_stream, replay_payload, ml_replay_maxlen)

    _last_ok_ts_ms.set(_now_ms())
    return True, "ok"

async def main() -> None:
    redis_url = _env("REDIS_URL", "redis://localhost:6379/0")
    http_port = _env_int("TRADE_CLOSE_JOINER_EXPORTER_PORT", "0")
    if http_port > 0:
        start_http_server(http_port)

    trade_events_stream = _env("TRADE_EVENTS_STREAM", RS.EVENTS_TRADES)
    trades_closed_stream = _env("TRADES_CLOSED_STREAM", RS.TRADES_CLOSED)
    trades_closed_maxlen = _env_int("TRADES_CLOSED_MAXLEN", "200000")
    decision_prefix = _env("DECISION_KEY_PREFIX", "decision:")
    close_wait_stream = _env("CLOSE_WAIT_STREAM", RS.TRADES_CLOSE_WAIT)
    close_wait_maxlen = _env_int("CLOSE_WAIT_MAXLEN", "200000")
    dedup_ttl_sec = _env_int("TRADES_CLOSED_DEDUP_TTL_SEC", str(3 * 24 * 3600))

    group = _env("TRADE_CLOSE_JOINER_GROUP", "trade_close_joiner")
    consumer = _env("TRADE_CLOSE_JOINER_CONSUMER", f"c-{os.getpid()}")
    batch = _env_int("TRADE_CLOSE_JOINER_BATCH", "100")
    block_ms = _env_int("TRADE_CLOSE_JOINER_BLOCK_MS", "2000")

    write_ml_replay = _env("WRITE_ML_REPLAY_INPUTS", "1").strip() in ("1", "true", "yes", "on")
    ml_replay_stream = _env("ML_REPLAY_INPUTS_STREAM", RS.ML_REPLAY_INPUTS)
    ml_replay_maxlen = _env_int("ML_REPLAY_INPUTS_MAXLEN", "200000")

    # OF inputs stream for enriching ML replay with original feature snapshots (Commit 10).
    of_inputs_stream = _env("OF_INPUTS_STREAM", RS.OF_INPUTS)
    of_inputs_field = _env("OF_INPUTS_STREAM_FIELD", "payload")
    of_inputs_sid_index_prefix = _env("OF_INPUTS_SID_INDEX_PREFIX", "idx:of_inputs:sid:")
    of_inputs_scan_count = _env_int("OF_INPUTS_SCAN_COUNT", "5000")

    r = aioredis.Redis.from_url(redis_url, decode_responses=True)

    helper = AsyncRedisStreamHelper(
        client=r,
        group=group,
        consumer=consumer,
    )

    # Ensure group exists
    await helper.ensure_group(trade_events_stream)

    while True:
        try:
            # AsyncRedisStreamHelper.read returns raw structure: [[stream, messages], ...]
            # messages is [(msg_id, fields), ...]
            res = await helper.read(
                streams={trade_events_stream: ">"},
                count=batch,
                block=block_ms,
            )

            if not res:
                _join_runs_total.labels(result="idle").inc()
                continue

            # Since we only read one stream, we can extract messages directly
            msgs = []
            for stream_name, stream_msgs in res:
                msgs.extend(stream_msgs)

            if not msgs:
                _join_runs_total.labels(result="idle").inc()
                continue

            for msg_id, fields in msgs:
                payload = _get_payload(fields)
                ev = str(_pick(payload, "event", "event_type", "type") or "").upper()
                if ev != "POSITION_CLOSED":
                    await helper.ack(trade_events_stream, msg_id)
                    _join_events_total.labels(type=ev or "na", result="skip").inc()
                    continue

                ok, reason = await _handle_close(
                    r,
                    payload,
                    decision_prefix=decision_prefix,
                    trades_closed_stream=trades_closed_stream,
                    trades_closed_maxlen=trades_closed_maxlen,
                    close_wait_stream=close_wait_stream,
                    close_wait_maxlen=close_wait_maxlen,
                    dedup_ttl_sec=dedup_ttl_sec,
                    ml_replay_stream=ml_replay_stream,
                    ml_replay_maxlen=ml_replay_maxlen,
                    write_ml_replay=write_ml_replay,
                    of_inputs_stream=of_inputs_stream,
                    of_inputs_field=of_inputs_field,
                    of_inputs_sid_index_prefix=of_inputs_sid_index_prefix,
                    of_inputs_scan_count=of_inputs_scan_count,
                )
                await helper.ack(trade_events_stream, msg_id)
                _join_events_total.labels(type="POSITION_CLOSED", result=reason).inc()
            _join_runs_total.labels(result="ok").inc()
        except asyncio.CancelledError:
            raise
        except Exception:
            _join_runs_total.labels(result="err").inc()
            await asyncio.sleep(1.0)

if __name__ == "__main__":
    asyncio.run(main())
