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
    conn = psycopg2.connect(_dsn(), connect_timeout=5, application_name="atr_policy_operator_state_store")
    try:
        yield conn
    finally:
        conn.close()


def insert_confirm_request(
    conn,
    *,
    token: str,
    actor: str,
    action: str,
    target_kind: str,
    target_id: str,
    proposal_id: str,
    payload: dict[str, Any],
    expires_at_ms: int,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO atr_policy_confirm_requests (
              token, actor, action, target_kind, target_id, proposal_id,
              payload_json, status, created_at_ms, expires_at_ms
            ) VALUES (%s,%s,%s,%s,%s,%s,%s::jsonb,'PENDING',%s,%s)
            ON CONFLICT (token) DO NOTHING
            """,
            (
                token,
                actor,
                action,
                target_kind,
                target_id,
                proposal_id or None,
                json.dumps(payload, ensure_ascii=False, sort_keys=True),
                int(time.time() * 1000),
                int(expires_at_ms),
            ),
        )


def mark_confirm_consumed(conn, token: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE atr_policy_confirm_requests
            SET status = 'CONSUMED',
                consumed_at_ms = %s
            WHERE token = %s
              AND status = 'PENDING'
            """,
            (int(time.time() * 1000), token),
        )


def expire_pending_confirms_on_boot(conn) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE atr_policy_confirm_requests
            SET status = 'EXPIRED_ON_BOOT'
            WHERE status = 'PENDING'
            """
        )
        return int(cur.rowcount or 0)


def load_current_active_snapshots(conn) -> list[dict[str, Any]]:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT snapshot_json
            FROM atr_policy_snapshots
            WHERE snapshot_kind = 'active'
              AND is_current = true
            """
        )
        return [dict(r["snapshot_json"]) for r in cur.fetchall()]
