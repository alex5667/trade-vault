from __future__ import annotations

import json
import os
import time
from contextlib import contextmanager
from typing import Any

import psycopg2
import psycopg2.extras


def _dsn() -> str:
    return (
        os.getenv("ANALYTICS_DB_DSN")
        or os.getenv("TRADES_DB_DSN")
        or "postgresql://postgres:12345@postgres:5432/scanner_analytics"
    )


@contextmanager
def get_conn():
    conn = psycopg2.connect(_dsn(), connect_timeout=5, application_name="atr_policy_state_store")
    try:
        yield conn
    finally:
        conn.close()


def _proposal_upsert_sql() -> str:
    return """
    INSERT INTO atr_policy_proposals (
      proposal_id, policy_ver, source, symbol, scenario, regime, risk_horizon_bucket,
      stop_ttl_mode, trailing_mode, reason_code, status, approved, proposal_json,
      created_at_ms, updated_at_ms
    ) VALUES (
      %(proposal_id)s, %(policy_ver)s, %(source)s, %(symbol)s, %(scenario)s, %(regime)s, %(risk_horizon_bucket)s,
      %(stop_ttl_mode)s, %(trailing_mode)s, %(reason_code)s, %(status)s, %(approved)s, %(proposal_json)s,
      %(created_at_ms)s, %(updated_at_ms)s
    )
    ON CONFLICT (proposal_id)
    DO UPDATE SET
      policy_ver = EXCLUDED.policy_ver,
      stop_ttl_mode = EXCLUDED.stop_ttl_mode,
      trailing_mode = EXCLUDED.trailing_mode,
      reason_code = EXCLUDED.reason_code,
      status = EXCLUDED.status,
      approved = EXCLUDED.approved,
      proposal_json = EXCLUDED.proposal_json,
      updated_at_ms = EXCLUDED.updated_at_ms
    """


def upsert_proposal(conn, proposal: dict[str, Any]) -> None:
    row = {
      "proposal_id": str(proposal["proposal_id"]),
      "policy_ver": int(proposal.get("policy_ver", 1)),
      "source": str(proposal["source"]),
      "symbol": str(proposal["symbol"]).upper(),
      "scenario": str(proposal["scenario"]).lower(),
      "regime": str(proposal["regime"]).lower(),
      "risk_horizon_bucket": str(proposal["risk_horizon_bucket"]).lower(),
      "stop_ttl_mode": (proposal.get("stop_ttl_mode", "canary")),
      "trailing_mode": (proposal.get("trailing_mode", "canary")),
      "reason_code": (proposal.get("reason_code", "")),
      "status": (proposal.get("status", "SUBMITTED")),
      "approved": bool(proposal.get("approved", False)),
      "proposal_json": json.dumps(proposal, ensure_ascii=False, sort_keys=True),
      "created_at_ms": int(proposal.get("created_at_ms", int(time.time() * 1000))),
      "updated_at_ms": int(proposal.get("updated_at_ms", int(time.time() * 1000))),
    }
    with conn.cursor() as cur:
        cur.execute(_proposal_upsert_sql(), row)


def insert_decision(conn, proposal_id: str, decision: dict[str, Any]) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO atr_policy_decisions (
              proposal_id, action, actor, note, decision_json, ts_ms
            ) VALUES (%s,%s,%s,%s,%s::jsonb,%s)
            """,
            (
                proposal_id,
                (decision.get("action", "")),
                (decision.get("actor", "")),
                (decision.get("note", "")),
                json.dumps(decision, ensure_ascii=False, sort_keys=True),
                int(decision.get("ts_ms", int(time.time() * 1000))),
            ),
        )


def update_proposal_status(conn, proposal_id: str, *, status: str, approved: bool, updated_at_ms: int) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE atr_policy_proposals
            SET status = %s,
                approved = %s,
                updated_at_ms = %s
            WHERE proposal_id = %s
            """,
            (status, approved, updated_at_ms, proposal_id),
        )


def transition_snapshot(
    conn,
    *,
    snapshot_kind: str,
    policy: dict[str, Any],
    applied_from_proposal_id: str | None,
    effective_from_ms: int | None = None,
) -> None:
    effective_from_ms = int(effective_from_ms or int(time.time() * 1000))
    source = str(policy["source"])
    symbol = str(policy["symbol"]).upper()
    scenario = str(policy["scenario"]).lower()
    regime = str(policy["regime"]).lower()
    bucket = str(policy["risk_horizon_bucket"]).lower()

    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE atr_policy_snapshots
            SET is_current = false,
                effective_to_ms = %s
            WHERE snapshot_kind = %s
              AND source = %s
              AND symbol = %s
              AND scenario = %s
              AND regime = %s
              AND risk_horizon_bucket = %s
              AND is_current = true
            """,
            (effective_from_ms, snapshot_kind, source, symbol, scenario, regime, bucket),
        )

        cur.execute(
            """
            INSERT INTO atr_policy_snapshots (
              snapshot_kind, source, symbol, scenario, regime, risk_horizon_bucket,
              policy_ver, stop_ttl_mode, trailing_mode, snapshot_json,
              is_current, effective_from_ms, applied_from_proposal_id
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb,true,%s,%s)
            """,
            (
                snapshot_kind,
                source,
                symbol,
                scenario,
                regime,
                bucket,
                int(policy.get("policy_ver", 1)),
                (policy.get("stop_ttl_mode", "canary")),
                (policy.get("trailing_mode", "canary")),
                json.dumps(policy, ensure_ascii=False, sort_keys=True),
                effective_from_ms,
                applied_from_proposal_id,
            ),
        )


def insert_recovery_event(
    conn,
    *,
    event_type: str,
    policy_ref: dict[str, Any],
    status: str,
    reason_code: str,
    payload: dict[str, Any],
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO atr_policy_recovery_events (
              event_type, source, symbol, scenario, regime, risk_horizon_bucket,
              status, reason_code, payload
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb)
            """,
            (
                event_type,
                (policy_ref.get("source", "")),
                (policy_ref.get("symbol", "")).upper(),
                (policy_ref.get("scenario", "")).lower(),
                (policy_ref.get("regime", "")).lower(),
                (policy_ref.get("risk_horizon_bucket", "")).lower(),
                status,
                reason_code,
                json.dumps(payload, ensure_ascii=False, sort_keys=True),
            ),
        )
