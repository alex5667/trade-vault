#!/usr/bin/env python3
# P54: Drain/retry for trades:close_wait -> trades:closed (enriched) once decision:{sid} appears.
#
# Contract assumptions:
# - trades:close_wait entries contain field "payload" which is JSON with at least:
#     { "sid": "...", "close_event": { ... POSITION_CLOSED payload ... } }
#   (We tolerate alternative keys: "close", "event", "position_closed".)
# - decision is stored at key "decision:{sid}" as JSON string (or a HASH field "payload").
# - output uses payload-only format: XADD <stream> payload "<json>"
#
# Safety:
# - Fail-open: never raises on per-message errors; moves bad messages to dead-letter.
# - Dedup: join:closed:{sid} with TTL, only set on successful write to trades:closed.
#
# Run modes:
#   --loop-s 2          (default) continuous drain
#   --batch 500         process up to N messages and exit (timer-friendly)
import argparse
import json
import os
import time
from dataclasses import dataclass
from typing import Any

import redis

from domain.evidence_keys import MetaKeys
from utils.time_utils import get_ny_time_millis
import contextlib


def now_ms() -> int:
    return get_ny_time_millis()


def env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)).strip())
    except Exception:
        return default


def env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)).strip())
    except Exception:
        return default


def env_str(name: str, default: str) -> str:
    v = os.getenv(name)
    return default if v is None else v


def env_bool(name: str, default: str = "0") -> bool:
    return env_str(name, default).strip().lower() in ("1", "true", "yes", "on")


def json_loads_safe(s: Any) -> dict[str, Any] | None:
    if s is None:
        return None
    if isinstance(s, (bytes, bytearray)):
        s = s.decode("utf-8", errors="replace")
    if isinstance(s, dict):
        return s
    if not isinstance(s, str):
        return None
    s = s.strip()
    if not s:
        return None
    try:
        return json.loads(s)
    except Exception:
        return None


def pick(d: dict[str, Any], *keys: str) -> Any:
    for k in keys:
        if k in d and d[k] is not None:
            return d[k]
    return None


def norm_state(v: Any) -> str:
    if v is None:
        return "unknown"
    if isinstance(v, str):
        s = v.strip().lower()
        if s in ("ok", "warn", "block", "unknown"):
            return s
        if s.isdigit():
            v = int(s)
        else:
            return "unknown"
    if isinstance(v, (int, float)):
        i = int(v)
        return {0: "ok", 1: "warn", 2: "block", 3: "unknown"}.get(i, "unknown")
    return "unknown"


@dataclass
class Cfg:
    redis_url: str

    close_wait_stream: str
    close_wait_group: str
    close_wait_consumer: str

    trades_closed_stream: str
    ml_replay_inputs_stream: str
    write_ml_replay_inputs: bool

    decision_key_prefix: str

    dedup_key_prefix: str
    dedup_ttl_sec: int
    lock_ttl_sec: int

    max_attempts: int
    max_wait_age_ms: int

    label_win_r_min: float

    metrics_hash: str

    delete_after_ack: bool


def load_cfg() -> Cfg:
    consumer = env_str("CLOSE_WAIT_CONSUMER", f"drainer-{os.getpid()}")
    return Cfg(
        redis_url=env_str("REDIS_URL", "redis://localhost:6379/0"),
        close_wait_stream=env_str("CLOSE_WAIT_STREAM", "trades:close_wait"),
        close_wait_group=env_str("CLOSE_WAIT_GROUP", "close_wait_drainer_v1"),
        close_wait_consumer=consumer,
        trades_closed_stream=env_str("TRADES_CLOSED_STREAM", "trades:closed"),
        ml_replay_inputs_stream=env_str("ML_REPLAY_INPUTS_STREAM", "ml_replay_inputs_v1"),
        write_ml_replay_inputs=env_bool("WRITE_ML_REPLAY_INPUTS", "1"),
        decision_key_prefix=env_str("DECISION_KEY_PREFIX", "decision:"),
        dedup_key_prefix=env_str("DEDUP_KEY_PREFIX", "join:closed:"),
        dedup_ttl_sec=env_int("DEDUP_TTL_SEC", 7 * 24 * 3600),
        lock_ttl_sec=env_int("LOCK_TTL_SEC", 30),
        max_attempts=env_int("CLOSE_WAIT_MAX_ATTEMPTS", 200),
        max_wait_age_ms=env_int("CLOSE_WAIT_MAX_AGE_MS", 48 * 3600 * 1000),
        label_win_r_min=env_float("LABEL_WIN_R_MIN", 0.0),
        metrics_hash=env_str("CLOSE_WAIT_METRICS_HASH", "metrics:close_wait_drainer"),
        delete_after_ack=env_bool("CLOSE_WAIT_DELETE_AFTER_ACK", "0"),
    )


def rconn(cfg: Cfg) -> redis.Redis:
    return redis.Redis.from_url(cfg.redis_url, decode_responses=False)


def ensure_group(r: redis.Redis, stream: str, group: str) -> None:
    try:
        r.xgroup_create(stream, group, id="0-0", mkstream=True)
    except redis.ResponseError as e:
        if "BUSYGROUP" in str(e):
            return
        raise


def xinfo_pending_count(r: redis.Redis, stream: str, group: str) -> int:
    try:
        info = r.xpending(stream, group)
        if isinstance(info, dict):
            return int(info.get("pending", 0))
        if isinstance(info, (list, tuple)) and info:
            return int(info[0])
        return 0
    except Exception:
        return 0


def read_decision_json(r: redis.Redis, cfg: Cfg, sid: str) -> dict[str, Any] | None:
    key = f"{cfg.decision_key_prefix}{sid}"
    raw = r.get(key)
    if raw is not None:
        return json_loads_safe(raw)
    try:
        raw2 = r.hget(key, b"payload")
        if raw2 is not None:
            return json_loads_safe(raw2)
    except Exception:
        pass
    return None


def parse_close_wait_payload(fields: dict[bytes, bytes]) -> tuple[str | None, dict[str, Any] | None]:
    payload = None
    if b"payload" in fields:
        payload = json_loads_safe(fields.get(b"payload"))
    else:
        payload = {k.decode("utf-8", "replace"): v.decode("utf-8", "replace") for k, v in fields.items()}
    if not payload:
        return None, None
    sid = pick(payload, "sid", "SID", "signal_id", "signalId")
    close_ev = pick(payload, "close_event", "close", "event", "position_closed")
    close_ev = json_loads_safe(close_ev) if not isinstance(close_ev, dict) else close_ev
    if close_ev is None and isinstance(payload, dict):
        if pick(payload, "event_type", "type") in ("POSITION_CLOSED", "position_closed"):
            close_ev = payload
    if sid is None and close_ev:
        sid = pick(close_ev, "sid", "SID", "signal_id", "signalId")
    if sid is None:
        return None, None
    return str(sid), close_ev or {}


def extract_close_fields(close_ev: dict[str, Any]) -> dict[str, Any]:
    event_type = pick(close_ev, "event_type", "type") or "POSITION_CLOSED"
    ts_ms = pick(close_ev, "close_ts_ms", "ts_ms", "ts", "timestamp_ms", "timestamp")
    if isinstance(ts_ms, (int, float)) and ts_ms < 10_000_000_000:
        ts_ms = int(ts_ms * 1000)
    if isinstance(ts_ms, str) and ts_ms.isdigit():
        ts_i = int(ts_ms)
        ts_ms = ts_i * 1000 if ts_i < 10_000_000_000 else ts_i
    r_mult = pick(close_ev, "r_mult", "RMult", "rMult", "r", "R")
    try:
        r_mult = float(r_mult) if r_mult is not None else None
    except Exception:
        r_mult = None
    return {
        "event_type": event_type,
        "close_ts_ms": int(ts_ms) if ts_ms is not None else None,
        "symbol": pick(close_ev, "symbol", "sym"),
        "tf": pick(close_ev, "tf", "timeframe"),
        "strategy": pick(close_ev, "strategy"),
        "position_id": pick(close_ev, "position_id", "positionId", "pos_id", "posId"),
        "r_mult": r_mult,
        "meta_enforce_cov_bucket": pick(close_ev, "meta_enforce_cov_bucket", "meta_cov_bucket", "cov_bucket"),
        "meta_enforce_applied": pick(close_ev, "meta_enforce_applied", "meta_applied", "enforce_applied"),
    }


def compute_drift_mode(decision: dict[str, Any]) -> str:
    drift_state = norm_state(pick(decision, "drift_state"))
    actual_action = str(pick(decision, "actual_action", "action") or "")
    actual_reason = str(pick(decision, "actual_reason_code", "reason_code") or "")
    if drift_state == "block" and actual_action == "emit" and "RULE_STRONG_ONLY_PASS" in actual_reason:
        return "block_strong_pass"
    return drift_state


def build_trades_closed_payload(cfg: Cfg, sid: str, close_ev: dict[str, Any], decision: dict[str, Any]) -> dict[str, Any]:
    c = extract_close_fields(close_ev)
    decision_ts_ms = pick(decision, "decision_ts_ms", "ts_ms", "timestamp_ms")
    if isinstance(decision_ts_ms, str) and decision_ts_ms.isdigit():
        decision_ts_ms = int(decision_ts_ms)
    if isinstance(decision_ts_ms, (int, float)) and decision_ts_ms < 10_000_000_000:
        decision_ts_ms = int(decision_ts_ms * 1000)

    close_ts_ms = c.get("close_ts_ms")
    r_mult = c.get("r_mult")
    y = None
    if r_mult is not None:
        y = 1 if r_mult >= cfg.label_win_r_min else 0

    dq_state = norm_state(pick(decision, "dq_state"))
    drift_state = norm_state(pick(decision, "drift_state"))
    drift_mode = compute_drift_mode(decision)

    p_cal = pick(decision, "ml_p_cal", "p_cal", "pCal")
    p_raw = pick(decision, "ml_p", "p_raw", "p", "pRaw")
    try:
        p_cal = float(p_cal) if p_cal is not None else None
    except Exception:
        p_cal = None
    try:
        p_raw = float(p_raw) if p_raw is not None else None
    except Exception:
        p_raw = None

    brier = None
    if y is not None and p_cal is not None:
        brier = (p_cal - float(y)) ** 2

    out: dict[str, Any] = {
        "ver": "p54",
        "sid": sid,
        "symbol": c.get("symbol") or pick(decision, "symbol"),
        "tf": c.get("tf") or pick(decision, "tf"),
        "strategy": c.get("strategy") or pick(decision, "strategy"),
        "position_id": c.get("position_id"),
        "close_ts_ms": close_ts_ms,
        "decision_ts_ms": decision_ts_ms,
        "decision_age_ms": (int(close_ts_ms) - int(decision_ts_ms)) if (close_ts_ms and decision_ts_ms) else None,
        "r_mult": r_mult,
        "y": y,
        "ml_state": pick(decision, "ml_state"),
        "ml_model_ver": pick(decision, "ml_model_ver", "model_ver"),
        "ml_p": p_raw,
        "ml_p_cal": p_cal,
        "brier": brier,
        "rule_score": pick(decision, "rule_score", "score"),
        "rule_ok": pick(decision, "rule_ok", "ok"),
        "rule_soft": pick(decision, "rule_soft", "soft"),
        "dq_state": dq_state,
        "drift_state": drift_state,
        "drift_mode": drift_mode,
        "meta_enforce_cov_bucket": c.get(MetaKeys.ENFORCE_COV_BUCKET) or pick(decision, "meta_enforce_cov_bucket"),
        "meta_enforce_applied": bool(int(c.get(MetaKeys.ENFORCE_APPLIED))) if (c.get(MetaKeys.ENFORCE_APPLIED)).isdigit() else bool(c.get(MetaKeys.ENFORCE_APPLIED, False)),
        "actual_action": pick(decision, "actual_action"),
        "actual_reason_code": pick(decision, "actual_reason_code"),
        "source": "close_wait_drainer",
    }
    for k in ("drift_psi_max_24h", "drift_z_max_24h", "drift_top_feature_psi", "drift_top_feature_z", "drift_last_ts_ms"):
        if k in decision and decision[k] is not None:
            out[k] = decision[k]
    return out


def metrics_hincrby(r: redis.Redis, key: str, field: str, inc: int = 1) -> None:
    with contextlib.suppress(Exception):
        r.hincrby(key, field, inc)


def metrics_hset(r: redis.Redis, key: str, field: str, value: Any) -> None:
    try:
        if isinstance(value, str):
            r.hset(key, field, value.encode("utf-8"))
        elif value is None:
            r.hset(key, field, b"")
        else:
            r.hset(key, field, str(value).encode("utf-8"))
    except Exception:
        pass


def dead_letter(r: redis.Redis, cfg: Cfg, sid: str, close_ev: dict[str, Any], reason: str) -> None:
    payload = {"ver": "p54", "sid": sid, "reason": reason, "ts_ms": now_ms(), "close_event": close_ev}
    r.xadd("trades:close_dead", {"payload": json.dumps(payload, ensure_ascii=False, separators=(",", ":"))}, maxlen=50000)
    metrics_hincrby(r, cfg.metrics_hash, "dead_letter_total", 1)


def should_dead_letter(r: redis.Redis, cfg: Cfg, sid: str, close_ts_ms: int | None) -> tuple[bool, str]:
    attempt_key = f"join:close_wait_attempt:{sid}"
    try:
        n = r.incr(attempt_key)
        r.expire(attempt_key, cfg.dedup_ttl_sec)
    except Exception:
        n = 0
    if n >= cfg.max_attempts:
        return True, f"max_attempts n={n}"
    if close_ts_ms is not None:
        age = now_ms() - int(close_ts_ms)
        if age >= cfg.max_wait_age_ms:
            return True, f"max_age_ms age={age}"
    return False, ""


def ack_and_optionally_delete(r: redis.Redis, cfg: Cfg, stream: str, group: str, msg_id: bytes) -> None:
    r.xack(stream, group, msg_id)
    if cfg.delete_after_ack:
        with contextlib.suppress(Exception):
            r.xdel(stream, msg_id)


def process_one(r: redis.Redis, cfg: Cfg, msg_id: bytes, fields: dict[bytes, bytes]) -> None:
    sid, close_ev = parse_close_wait_payload(fields)
    if not sid:
        dead_letter(r, cfg, "na", {}, "bad_payload_no_sid")
        ack_and_optionally_delete(r, cfg, cfg.close_wait_stream, cfg.close_wait_group, msg_id)
        return

    metrics_hincrby(r, cfg.metrics_hash, "seen_total", 1)

    lock_key = f"join:close_wait_lock:{sid}"
    got_lock = r.set(lock_key, b"1", nx=True, ex=cfg.lock_ttl_sec)
    if not got_lock:
        metrics_hincrby(r, cfg.metrics_hash, "lock_contended_total", 1)
        return

    try:
        dedup_key = f"{cfg.dedup_key_prefix}{sid}"
        if r.exists(dedup_key):
            metrics_hincrby(r, cfg.metrics_hash, "dedup_skipped_total", 1)
            ack_and_optionally_delete(r, cfg, cfg.close_wait_stream, cfg.close_wait_group, msg_id)
            return

        decision = read_decision_json(r, cfg, sid)
        close_fields = extract_close_fields(close_ev or {})
        close_ts_ms = close_fields.get("close_ts_ms")
        if decision is None:
            dl, why = should_dead_letter(r, cfg, sid, close_ts_ms)
            metrics_hincrby(r, cfg.metrics_hash, "missing_decision_total", 1)
            if dl:
                dead_letter(r, cfg, sid, close_ev or {}, f"decision_missing {why}")
                ack_and_optionally_delete(r, cfg, cfg.close_wait_stream, cfg.close_wait_group, msg_id)
            return

        payload = build_trades_closed_payload(cfg, sid, close_ev or {}, decision)
        p = r.pipeline(transaction=True)
        p.set(dedup_key, b"1", nx=True, ex=cfg.dedup_ttl_sec)
        p.xadd(cfg.trades_closed_stream, {"payload": json.dumps(payload, ensure_ascii=False, separators=(",", ":"))}, maxlen=50000)
        if cfg.write_ml_replay_inputs:
            replay_payload = {"ver": "p54", "sid": sid, "close": extract_close_fields(close_ev or {}), "decision": decision, "ts_ms": now_ms()}
            p.xadd(cfg.ml_replay_inputs_stream, {"payload": json.dumps(replay_payload, ensure_ascii=False, separators=(",", ":"))}, maxlen=50000)
        res = p.execute()
        if not res or res[0] is None:
            metrics_hincrby(r, cfg.metrics_hash, "dedup_race_skipped_total", 1)
        else:
            metrics_hincrby(r, cfg.metrics_hash, "joined_total", 1)
            metrics_hincrby(r, cfg.metrics_hash, "written_trades_closed_total", 1)
            if cfg.write_ml_replay_inputs:
                metrics_hincrby(r, cfg.metrics_hash, "written_ml_replay_total", 1)

        ack_and_optionally_delete(r, cfg, cfg.close_wait_stream, cfg.close_wait_group, msg_id)
    except Exception as e:
        metrics_hincrby(r, cfg.metrics_hash, "error_total", 1)
        dead_letter(r, cfg, sid, close_ev or {}, f"exception {type(e).__name__}")
        with contextlib.suppress(Exception):
            ack_and_optionally_delete(r, cfg, cfg.close_wait_stream, cfg.close_wait_group, msg_id)
    finally:
        with contextlib.suppress(Exception):
            r.delete(lock_key)


def drain(r: redis.Redis, cfg: Cfg, batch: int, block_ms: int, read_new: bool, read_pending: bool) -> int:
    processed = 0
    metrics_hset(r, cfg.metrics_hash, "last_run_ts_ms", now_ms())
    metrics_hset(r, cfg.metrics_hash, "pending_count", xinfo_pending_count(r, cfg.close_wait_stream, cfg.close_wait_group))

    if read_pending:
        msgs = r.xreadgroup(cfg.close_wait_group, cfg.close_wait_consumer, {cfg.close_wait_stream: b"0-0"}, count=batch)
        for _stream, entries in msgs or []:
            for msg_id, fields in entries:
                process_one(r, cfg, msg_id, fields)
                processed += 1
                if processed >= batch:
                    return processed

    if read_new:
        msgs = r.xreadgroup(cfg.close_wait_group, cfg.close_wait_consumer, {cfg.close_wait_stream: b">"}, count=max(1, batch - processed), block=block_ms)
        for _stream, entries in msgs or []:
            for msg_id, fields in entries:
                process_one(r, cfg, msg_id, fields)
                processed += 1
                if processed >= batch:
                    return processed
    return processed


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--loop-s", type=float, default=float(env_str("CLOSE_WAIT_LOOP_S", "2")))
    ap.add_argument("--batch", type=int, default=int(env_str("CLOSE_WAIT_BATCH", "500")))
    ap.add_argument("--block-ms", type=int, default=int(env_str("CLOSE_WAIT_BLOCK_MS", "1000")))
    ap.add_argument("--once", action="store_true")
    args = ap.parse_args()

    cfg = load_cfg()
    r = rconn(cfg)

    # Outer loop to retry on Redis loading/connection errors during startup or runtime
    while True:
        try:
            ensure_group(r, cfg.close_wait_stream, cfg.close_wait_group)
            metrics_hset(r, cfg.metrics_hash, "start_ts_ms", now_ms())

            if args.once:
                n = drain(r, cfg, batch=args.batch, block_ms=0, read_new=True, read_pending=True)
                metrics_hset(r, cfg.metrics_hash, "last_once_processed", n)
                return

            while True:
                n = drain(r, cfg, batch=args.batch, block_ms=args.block_ms, read_new=True, read_pending=True)
                if n == 0:
                    time.sleep(max(0.05, float(args.loop_s)))

        except (redis.exceptions.BusyLoadingError, redis.exceptions.ConnectionError):
            time.sleep(2.0)
        except Exception as e:
            # For other exceptions, log and maybe crash?
            # The original code just crashed. Failsafe: print and sleep?
            # Better to crash for visibility unless it's a transient network issue.
            # But let's stick to fixing the Loading error.
            print(f"Error in drain polling: {e}")
            time.sleep(5.0)



if __name__ == "__main__":
    main()
