from __future__ import annotations

import json
import os
import time

import psycopg2
import psycopg2.extras
import redis

from services.atr_control_plane_graph_service import ControlPlaneGraphService

_STAGE_FLOW = ["shadow", "canary_5", "canary_25", "live_100"]

def _dsn():
    return (
        os.getenv("ANALYTICS_DB_DSN")
        or os.getenv("TRADES_DB_DSN")
        or "postgresql://postgres:12345@postgres:5432/scanner_analytics"
    )

def _redis():
    return redis.Redis.from_url(os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0"), decode_responses=True)

def _next_stage(stage: str) -> str:
    if stage not in _STAGE_FLOW:
        return "shadow"
    i = _STAGE_FLOW.index(stage)
    return _STAGE_FLOW[min(i + 1, len(_STAGE_FLOW) - 1)]

def _share_for_stage(stage: str) -> float:
    return {
        "shadow": 0.0,
        "canary_5": 0.05,
        "canary_25": 0.25,
        "live_100": 1.0,
        "frozen": 0.0,
        "rolled_back": 0.0,
    }.get(stage, 0.0)

def run_once() -> int:
    r = _redis()
    conn = psycopg2.connect(_dsn(), connect_timeout=5, application_name="atr_policy_rollout_controller")
    changed = 0
    try:
        cur = 0
        proposals = []
        while True:
            cur, keys = r.scan(cur, match="cfg:suggestions:atr_policy_v2:*", count=10000)
            for key in keys:
                raw = r.get(key)
                if raw:
                    proposals.append(json.loads(raw))
            if cur == 0:
                break

        with conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as db:
            for p in proposals:
                db.execute("""
                    SELECT rollout_stage, rollout_share
                    FROM atr_policy_rollouts
                    WHERE source=%s AND symbol=%s AND scenario=%s AND regime=%s
                      AND risk_horizon_bucket=%s AND layer=%s
                      AND is_current=true
                """, (
                    p["source"], p["symbol"], p["scenario"], p["regime"],
                    p["risk_horizon_bucket"], p["layer"],
                ))
                row = db.fetchone()
                cur_stage = row["rollout_stage"] if row else "shadow"

                action = (p.get("action") or "HOLD").upper()
                if action == "PROMOTE":
                    new_stage = _next_stage(cur_stage)
                    reason_code = "ATR_POLICY_ROLLOUT_PROMOTE"
                elif action == "ROLLBACK":
                    new_stage = "rolled_back"
                    reason_code = "ATR_POLICY_ROLLOUT_ROLLBACK"
                else:
                    new_stage = cur_stage
                    reason_code = "ATR_POLICY_ROLLOUT_HOLD"

                # ALWAYS PUBLISH CURRENT STATE TO REDIS FOR HOT-PATH READS (since it might be new or updated)
                redis_key = f"cfg:atr_policy_rollout:state:{p['source']}:{p['symbol']}:{p['scenario']}:{p['regime']}:{p['risk_horizon_bucket']}:{p['layer']}"
                r.set(redis_key, new_stage)

                if new_stage == cur_stage:
                    continue

                now_ms = int(time.time() * 1000)
                db.execute("""
                    UPDATE atr_policy_rollouts
                    SET is_current=false, updated_at_ms=%s
                    WHERE source=%s AND symbol=%s AND scenario=%s AND regime=%s
                      AND risk_horizon_bucket=%s AND layer=%s
                      AND is_current=true
                """, (
                    now_ms,
                    p["source"], p["symbol"], p["scenario"], p["regime"],
                    p["risk_horizon_bucket"], p["layer"],
                ))

                db.execute("""
                    INSERT INTO atr_policy_rollouts (
                      source, symbol, scenario, regime, risk_horizon_bucket,
                      layer, policy_ver, rollout_stage, rollout_share,
                      is_current, reason_code, created_at_ms, updated_at_ms
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,true,%s,%s,%s)
                """, (
                    p["source"], p["symbol"], p["scenario"], p["regime"], p["risk_horizon_bucket"],
                    p["layer"], int(p["policy_ver"]), new_stage, _share_for_stage(new_stage),
                    reason_code, now_ms, now_ms
                ))

                db.execute("""
                    INSERT INTO atr_policy_rollout_events (
                      source, symbol, scenario, regime, risk_horizon_bucket,
                      layer, policy_ver, old_stage, new_stage, action, reason_code, event_json
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb)
                """, (
                    p["source"], p["symbol"], p["scenario"], p["regime"], p["risk_horizon_bucket"],
                    p["layer"], int(p["policy_ver"]),
                    cur_stage, new_stage, action.lower(), reason_code,
                    json.dumps(p, ensure_ascii=False, sort_keys=True)
                ))

                ControlPlaneGraphService.emit_graph_event(
                    scope_kind="symbol",
                    scope_value=p["symbol"],
                    event_type="rollout_stage_changed",
                    payload={"old_stage": cur_stage, "new_stage": new_stage}
                )

                changed += 1
        return changed
    finally:
        conn.close()

if __name__ == "__main__":
    run_once()
