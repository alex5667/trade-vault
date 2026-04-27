from utils.time_utils import get_ny_time_millis
#!/usr/bin/env python3
# P55: Backfill/replay POSITION_CLOSED from events:trades into trades:close_wait (or directly to trades:closed).
#
# Problem:
# - Older period may have missing enrichment in trades:closed because close came before decision record existed
#   or joiner/drainer was not running.
#
# Strategy:
# - Scan events:trades (payload JSON) over a window (default 48h) or by count.
# - For each POSITION_CLOSED:
#     sid = payload.sid
#     if join:closed:{sid} exists -> already joined, skip
#     else:
#       if decision:{sid} exists and DIRECT_JOIN_ON_BACKFILL=1:
#           write enriched payload into trades:closed and set join:closed:{sid}
#       else:
#           push into trades:close_wait stream (payload {sid, close_event, backfill_ts_ms})
#
# Safety:
# - Dedup key join:closed:{sid} prevents duplicates across real-time and backfill.
# - backfill:seen:{event_id} prevents reprocessing the same stream entry across runs.
#
# Usage examples:
#   python -m tools.close_backfill_replay_v1 --hours 72 --count 200000
#   python -m tools.close_backfill_replay_v1 --since-id 1739990000000-0 --count 50000

import argparse
import json
import os
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import redis


def now_ms() -> int:
    return get_ny_time_millis()


def env_str(name: str, default: str) -> str:
    v = os.getenv(name)
    return default if v is None else v


def env_int(name: str, default: int) -> int:
    try:
        return int(env_str(name, str(default)).strip())
    except Exception:
        return default


def env_bool(name: str, default: str = "0") -> bool:
    return env_str(name, default).strip().lower() in ("1", "true", "yes", "on")


def json_loads_safe(s: Any) -> Optional[Dict[str, Any]]:
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


def pick(d: Dict[str, Any], *keys: str) -> Any:
    for k in keys:
        if k in d and d[k] is not None:
            return d[k]
    return None


def extract_close_fields(close_ev: Dict[str, Any]) -> Dict[str, Any]:
    def _ts_to_ms(v: Any) -> Optional[int]:
        if v is None:
            return None
        if isinstance(v, str) and v.isdigit():
            v = int(v)
        if isinstance(v, (int, float)):
            iv = int(v)
            return iv * 1000 if iv < 10_000_000_000 else iv
        return None

    ts_ms = _ts_to_ms(pick(close_ev, "close_ts_ms", "ts_ms", "ts", "timestamp_ms", "timestamp"))
    r_mult = pick(close_ev, "r_mult", "RMult", "rMult", "r", "R")
    try:
        r_mult = float(r_mult) if r_mult is not None else None
    except Exception:
        r_mult = None
    return {
        "event_type": pick(close_ev, "event_type", "type") or "POSITION_CLOSED",
        "close_ts_ms": ts_ms,
        "symbol": pick(close_ev, "symbol", "sym"),
        "tf": pick(close_ev, "tf", "timeframe"),
        "strategy": pick(close_ev, "strategy"),
        "position_id": pick(close_ev, "position_id", "positionId", "pos_id", "posId"),
        "r_mult": r_mult,
        "meta_enforce_cov_bucket": pick(close_ev, "meta_enforce_cov_bucket", "meta_cov_bucket", "cov_bucket"),
        "meta_enforce_applied": pick(close_ev, "meta_enforce_applied", "meta_applied", "enforce_applied"),
    }


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


def compute_drift_mode(decision: Dict[str, Any]) -> str:
    drift_state = norm_state(pick(decision, "drift_state"))
    actual_action = str(pick(decision, "actual_action", "action") or "")
    actual_reason = str(pick(decision, "actual_reason_code", "reason_code") or "")
    if drift_state == "block" and actual_action == "emit" and "RULE_STRONG_ONLY_PASS" in actual_reason:
        return "block_strong_pass"
    return drift_state


def build_trades_closed_payload(
    sid: str, close_ev: Dict[str, Any], decision: Dict[str, Any], label_win_r_min: float
) -> Dict[str, Any]:
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
        y = 1 if r_mult >= label_win_r_min else 0

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

    out: Dict[str, Any] = {
        "ver": "p55",
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
        "dq_state": norm_state(pick(decision, "dq_state")),
        "drift_state": norm_state(pick(decision, "drift_state")),
        "drift_mode": compute_drift_mode(decision),
        "meta_enforce_cov_bucket": c.get("meta_enforce_cov_bucket") or pick(decision, "meta_enforce_cov_bucket"),
        "meta_enforce_applied": bool(int(c.get("meta_enforce_applied"))) if str(c.get("meta_enforce_applied")).isdigit() else bool(c.get("meta_enforce_applied", False)),
        "actual_action": pick(decision, "actual_action"),
        "actual_reason_code": pick(decision, "actual_reason_code"),
        "source": "close_backfill_replay",
    }
    for k in ("drift_psi_max_24h", "drift_z_max_24h", "drift_top_feature_psi", "drift_top_feature_z", "drift_last_ts_ms"):
        if k in decision and decision[k] is not None:
            out[k] = decision[k]
    return out


@dataclass
class Cfg:
    redis_url: str
    trade_events_stream: str
    trades_closed_stream: str
    close_wait_stream: str
    decision_key_prefix: str
    join_dedup_prefix: str
    seen_event_prefix: str
    seen_ttl_sec: int
    dedup_ttl_sec: int
    label_win_r_min: float
    direct_join: bool
    scan_batch: int
    max_count: int
    write_ml_replay_inputs: bool
    ml_replay_inputs_stream: str
    metrics_hash: str


def load_cfg() -> Cfg:
    return Cfg(
        redis_url=env_str("REDIS_URL", "redis://localhost:6379/0"),
        trade_events_stream=env_str("TRADE_EVENTS_STREAM", "events:trades"),
        trades_closed_stream=env_str("TRADES_CLOSED_STREAM", "trades:closed"),
        close_wait_stream=env_str("CLOSE_WAIT_STREAM", "trades:close_wait"),
        decision_key_prefix=env_str("DECISION_KEY_PREFIX", "decision:"),
        join_dedup_prefix=env_str("DEDUP_KEY_PREFIX", "join:closed:"),
        seen_event_prefix=env_str("BACKFILL_SEEN_PREFIX", "backfill:seen:"),
        seen_ttl_sec=env_int("BACKFILL_SEEN_TTL_SEC", 14 * 24 * 3600),
        dedup_ttl_sec=env_int("DEDUP_TTL_SEC", 7 * 24 * 3600),
        label_win_r_min=float(env_str("LABEL_WIN_R_MIN", "0.0")),
        direct_join=env_bool("DIRECT_JOIN_ON_BACKFILL", "1"),
        write_ml_replay_inputs=env_bool("WRITE_ML_REPLAY_INPUTS", "1"),
        ml_replay_inputs_stream=env_str("ML_REPLAY_INPUTS_STREAM", "ml_replay_inputs_v1"),
        scan_batch=env_int("BACKFILL_SCAN_BATCH", 1000),
        max_count=env_int("BACKFILL_MAX_COUNT", 200000),
        metrics_hash=env_str("BACKFILL_METRICS_HASH", "metrics:close_backfill_replay"),
    )


def rconn(cfg: Cfg) -> redis.Redis:
    return redis.Redis.from_url(cfg.redis_url, decode_responses=False)


def decision_get(r: redis.Redis, cfg: Cfg, sid: str) -> Optional[Dict[str, Any]]:
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


def metrics_incr(r: redis.Redis, key: str, field: str, inc: int = 1) -> None:
    try:
        r.hincrby(key, field, inc)
    except Exception:
        pass


def metrics_set(r: redis.Redis, key: str, field: str, val: Any) -> None:
    try:
        if isinstance(val, str):
            r.hset(key, field, val.encode("utf-8"))
        else:
            r.hset(key, field, str(val).encode("utf-8"))
    except Exception:
        pass


def parse_trade_event_payload(fields: Dict[bytes, bytes]) -> Optional[Dict[str, Any]]:
    if b"payload" in fields:
        return json_loads_safe(fields.get(b"payload"))
    if b"data" in fields:
        return json_loads_safe(fields.get(b"data"))
    return None


def is_position_closed(p: Dict[str, Any]) -> bool:
    t = str(pick(p, "event_type", "type") or "").upper()
    return t == "POSITION_CLOSED"


def main() -> None:
    cfg = load_cfg()
    r = rconn(cfg)

    ap = argparse.ArgumentParser()
    ap.add_argument("--hours", type=int, default=int(env_str("BACKFILL_HOURS", "48")))
    ap.add_argument("--count", type=int, default=int(env_str("BACKFILL_MAX_COUNT", str(cfg.max_count))))
    ap.add_argument("--since-id", type=str, default="")
    args = ap.parse_args()

    metrics_set(r, cfg.metrics_hash, "start_ts_ms", now_ms())
    metrics_set(r, cfg.metrics_hash, "last_run_ts_ms", now_ms())

    # Determine start id
    if args.since_id:
        start_id = args.since_id.encode("utf-8")
    else:
        start_ms = now_ms() - int(args.hours) * 3600 * 1000
        start_id = f"{start_ms}-0".encode("utf-8")

    # We scan forward using XRANGE in batches.
    # Note: XRANGE is O(N) but acceptable for bounded windows + capped count.
    current_id = start_id
    processed = 0

    while processed < args.count:
        remain = min(cfg.scan_batch, args.count - processed)
        entries = r.xrange(cfg.trade_events_stream, min=current_id, max=b"+", count=remain)
        if not entries:
            break

        for msg_id, fields in entries:
            processed += 1
            # next start id should be strictly greater than current msg_id to avoid re-reading
            current_id = msg_id

            # seen dedup by event id
            seen_key = f"{cfg.seen_event_prefix}{msg_id.decode('utf-8','replace')}".encode("utf-8")
            if r.set(seen_key, b"1", nx=True, ex=cfg.seen_ttl_sec) is None:
                metrics_incr(r, cfg.metrics_hash, "seen_dedup_skipped_total", 1)
                continue

            payload = parse_trade_event_payload(fields)
            if not payload:
                metrics_incr(r, cfg.metrics_hash, "bad_payload_total", 1)
                continue

            if not is_position_closed(payload):
                metrics_incr(r, cfg.metrics_hash, "non_close_skipped_total", 1)
                continue

            sid = pick(payload, "sid", "SID", "signal_id", "signalId")
            if sid is None:
                metrics_incr(r, cfg.metrics_hash, "no_sid_total", 1)
                continue
            sid = str(sid)

            dedup_key = f"{cfg.join_dedup_prefix}{sid}".encode("utf-8")
            if r.exists(dedup_key):
                metrics_incr(r, cfg.metrics_hash, "already_joined_total", 1)
                continue

            decision = decision_get(r, cfg, sid)
            if cfg.direct_join and decision is not None:
                # write trades:closed directly
                close_payload = build_trades_closed_payload(sid, payload, decision, cfg.label_win_r_min)
                p = r.pipeline(transaction=True)
                p.set(dedup_key, b"1", nx=True, ex=cfg.dedup_ttl_sec)
                p.xadd(cfg.trades_closed_stream, {"payload": json.dumps(close_payload, ensure_ascii=False, separators=(",", ":"))}, maxlen=50000)
                if cfg.write_ml_replay_inputs:
                    replay_payload = {
                        "ver": "p55",
                        "sid": sid,
                        "close": extract_close_fields(payload),
                        "decision": decision,
                        "ts_ms": now_ms(),
                        "source": "close_backfill_replay",
                    }
                    p.xadd(cfg.ml_replay_inputs_stream, {"payload": json.dumps(replay_payload, ensure_ascii=False, separators=(",", ":"))}, maxlen=50000)
                res = p.execute()
                if res and res[0] is not None:
                    metrics_incr(r, cfg.metrics_hash, "direct_joined_total", 1)
                    if cfg.write_ml_replay_inputs:
                        metrics_incr(r, cfg.metrics_hash, "written_ml_replay_total", 1)
                else:
                    metrics_incr(r, cfg.metrics_hash, "direct_join_dedup_race_total", 1)
                continue

            # otherwise push to close_wait
            wait_payload = {
                "ver": "p55",
                "sid": sid,
                "close_event": payload,
                "backfill_ts_ms": now_ms(),
                "src_event_id": msg_id.decode("utf-8", "replace"),
            }
            r.xadd(cfg.close_wait_stream, {"payload": json.dumps(wait_payload, ensure_ascii=False, separators=(",", ":"))}, maxlen=50000)
            metrics_incr(r, cfg.metrics_hash, "pushed_to_close_wait_total", 1)

        # advance min id to strictly after last processed id:
        # If last id is like b"<ms>-<seq>", next min is b"<ms>-<seq+1>"
        try:
            ms_s, seq_s = current_id.decode("utf-8").split("-")
            current_id = f"{ms_s}-{int(seq_s)+1}".encode("utf-8")
        except Exception:
            # fallback: add a high seq
            current_id = (current_id.decode("utf-8", "replace") + "-1").encode("utf-8")

        metrics_set(r, cfg.metrics_hash, "last_id", current_id.decode("utf-8", "replace"))
        metrics_set(r, cfg.metrics_hash, "processed", processed)

    metrics_set(r, cfg.metrics_hash, "end_ts_ms", now_ms())


if __name__ == "__main__":
    main()
