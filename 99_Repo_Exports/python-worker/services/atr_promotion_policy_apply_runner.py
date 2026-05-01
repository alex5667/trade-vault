from __future__ import annotations

import json
import os
import time

import redis


def _redis():
    return redis.Redis.from_url(os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0"), decode_responses=True)


def _dsn():
    return (
        os.getenv("ANALYTICS_DB_DSN")
        or os.getenv("TRADES_DB_DSN")
        or "postgresql://postgres:12345@postgres:5432/scanner_analytics"
    )

def _audit(conn, proposal: dict, decision: dict, applied: bool, previous_active_json: str = "") -> None:
    with conn.cursor() as cur:
        cur.execute(
            """,
            INSERT INTO atr_promotion_policy_audit (
              source, symbol, scenario, regime, risk_horizon_bucket,
              stop_ttl_mode, trailing_mode, reason_code,
              approved, applied, suggestion_json, decision_json, previous_active_json
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s::jsonb,%s::jsonb)
            """,
            (
                proposal["source"],
                proposal["symbol"],
                proposal["scenario"],
                proposal["regime"],
                proposal["risk_horizon_bucket"],
                proposal["stop_ttl_mode"],
                proposal["trailing_mode"],
                proposal.get("reason_code", ""),
                bool(proposal.get("approved", False)),
                bool(applied),
                json.dumps(proposal, ensure_ascii=False, sort_keys=True),
                json.dumps(decision, ensure_ascii=False, sort_keys=True),
                previous_active_json or "{}",
            ),
        )



def apply_one(key: str) -> bool:
    r = _redis()
    raw = r.get(key)
    if not raw:
        return False
    proposal = json.loads(raw)
    proposal_id = str(proposal.get("proposal_id") or "")
    if not proposal_id:
        return False
    
    from services.atr_policy_workflow import (
        proposal_key,
        decision_key,
        active_key,
        active_prev_key,
    )
    from services.atr_policy_state_store import transition_snapshot
    
    raw_decision = r.get(decision_key(proposal_id))
    if not raw_decision:
        return False
    decision = json.loads(raw_decision)
    action = str(decision.get("action") or "").upper()

    import psycopg2
    conn = psycopg2.connect(_dsn(), connect_timeout=5, application_name="atr_promotion_policy_apply_runner")
    try:
        if action == "REJECT":
            _audit(conn, proposal, decision, applied=False, previous_active_json="{}")
            conn.commit()
            return True

        akey = active_key(proposal)
        prev = r.get(akey) or "{}"
        r.set(active_prev_key(proposal), prev)

        if action == "APPROVE" and bool(proposal.get("approved", False)):
            proposal["applied_at_ms"] = int(time.time() * 1000)
            proposal["status"] = "APPLIED"
            r.set(akey, json.dumps(proposal, ensure_ascii=False, sort_keys=True))
            r.set(proposal_key(proposal_id), json.dumps(proposal, ensure_ascii=False, sort_keys=True))
            transition_snapshot(
                conn,
                snapshot_kind="active",
                policy=proposal,
                applied_from_proposal_id=proposal_id,
                effective_from_ms=int(proposal["applied_at_ms"]),
            )
            _audit(conn, proposal, decision, applied=True, previous_active_json=prev)

            try:
                from services.atr_promotion_policy_metrics import (
                    atr_promotion_policy_apply_total,
                    atr_promotion_policy_active_total
                )
                atr_promotion_policy_apply_total.labels(
                    stop_ttl_mode=proposal.get('stop_ttl_mode', 'canary'),
                    trailing_mode=proposal.get('trailing_mode', 'canary')
                ).inc()
                atr_promotion_policy_active_total.labels(
                    symbol=proposal.get('symbol', ''),
                    scenario=proposal.get('scenario', ''),
                    regime=proposal.get('regime', ''),
                    bucket=proposal.get('risk_horizon_bucket', '')
                ).inc()
            except Exception:
                pass

            # ── Phase 3.8: post-apply verify + mirror ──────────────────────
            try:
                from services.atr_policy_post_apply_verifier import verify_active_policy
                verify_result = verify_active_policy(akey, r)
            except Exception:
                verify_result = {"verified_ok": False, "reason_code": "VERIFIER_IMPORT_ERROR"}

            try:
                from services.atr_policy_active_mirror_service import mirror_after_verified_apply
                mirror_after_verified_apply(proposal, verify_result, r)
            except Exception:
                pass  # mirror errors are non-blocking

            # Trigger rollback watcher on verify failure (non-blocking)
            if not verify_result.get("verified_ok"):
                try:
                    from services.atr_policy_rollback_watcher import rollback_to_last_good
                    rollback_to_last_good(
                        proposal, r,
                        trigger_reason=str(verify_result.get("reason_code", "VERIFY_FAIL_POST_APPLY")),
                    )
                except Exception:
                    pass

            # Update reconcile timestamp
            try:
                r.set("atr_policy:reconcile:last_success_ts_ms", int(time.time() * 1000))
            except Exception:
                pass

            conn.commit()
            return True

        if action == "REVOKE":
            r.delete(akey)
            proposal["status"] = "REVOKED_APPLIED"
            proposal["applied_at_ms"] = int(time.time() * 1000)
            r.set(proposal_key(proposal_id), json.dumps(proposal, ensure_ascii=False, sort_keys=True))
            # Close current active snapshot by inserting a canary fallback snapshot
            revoke_snapshot = dict(proposal)
            revoke_snapshot["stop_ttl_mode"] = "canary"
            revoke_snapshot["trailing_mode"] = "canary"
            revoke_snapshot["reason_code"] = "ATR_POLICY_REVOKED_TO_CANARY"
            transition_snapshot(
                conn,
                snapshot_kind="active",
                policy=revoke_snapshot,
                applied_from_proposal_id=proposal_id,
                effective_from_ms=int(proposal["applied_at_ms"]),
            )
            _audit(conn, proposal, decision, applied=False, previous_active_json=prev)
            conn.commit()
            return True
        return False
    finally:
        conn.close()

if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        print(apply_one(sys.argv[1]))
