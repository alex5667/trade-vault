"""
P2 — ON CONFLICT semantic change test.
Checks the behavior of two consecutive mirror_decision (record_decision)
calls with the same decision_id but different created_ts_ms.

Contract: Currently, due to the hypertable UNIQUE(decision_id, ts) constraint,
two differing timestamps WILL produce 2 separate rows in both risk_decisions
and risk_snapshot. We lock down this behavior here (or up to an update if the
database schema changes).
"""
import os
import time
from typing import Any

import pytest

try:
    import psycopg
except ImportError:
    psycopg = None

# [AUTOGRAVITY CLEANUP] sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from services.risk.risk_audit_sql import RiskAuditSqlSink

PG_DSN = os.getenv("ANALYTICS_DB_DSN") or os.getenv("PG_DSN") or "postgresql://trading:trading_password@localhost:5432/scanner_analytics"

class MockRiskInput:
    symbol = "BTCUSDT"
    requested_notional_usd = 1000.0
    cluster = "BTC"

class MockRiskDecision:
    level = "HIGH"
    allow_trade_publish = True
    adjusted_notional_usd = 800.0
    effective_execution_policy = "SAFETY_FIRST"
    leverage_cap = 10.0
    risk_multiplier = 0.8
    snapshot: dict[str, Any] = {"decision_latency_ms": 100, "clamp_ratio": 0.5}
    reasons = ["Notional limits applied"]

@pytest.fixture
def pg_connection():
    if psycopg is None:
        pytest.skip("psycopg3 not installed")
    try:
        conn = psycopg.connect(PG_DSN)
        yield conn
        conn.close()
    except Exception as e:
        pytest.skip(f"Could not connect to DB: {e}")

@pytest.fixture
def cleanup_postgres(pg_connection):
    conn = pg_connection
    test_decision_id = "test_on_conflict_001"
    with conn.cursor() as cur:
        cur.execute("DELETE FROM risk_decisions WHERE decision_id = %s", (test_decision_id,))
        cur.execute("DELETE FROM risk_snapshot WHERE decision_id = %s", (test_decision_id,))
        conn.commit()
    yield
    with conn.cursor() as cur:
        cur.execute("DELETE FROM risk_decisions WHERE decision_id = %s", (test_decision_id,))
        cur.execute("DELETE FROM risk_snapshot WHERE decision_id = %s", (test_decision_id,))
        conn.commit()

def test_record_decision_on_conflict_semantics(pg_connection, cleanup_postgres):
    """
    Test two sequential record_decisions with the SAME decision_id but DIFFERENT created_ts_ms.
    Contract: This MUST create 2 separate rows because the ON CONFLICT constraint
    includes both (decision_id, ts).
    """
    sink = RiskAuditSqlSink(dsn=PG_DSN, enabled=True)

    decision_id = "test_on_conflict_001"
    ts1 = int(time.time() * 1000)
    ts2 = ts1 + 1000  # +1 second

    signal1 = {"ts_event_ms": ts1, "symbol": "BTCUSDT", "risk_cluster": "cls"}
    signal2 = {"ts_event_ms": ts2, "symbol": "BTCUSDT", "risk_cluster": "cls"}
    risk_input = MockRiskInput()
    risk_decision = MockRiskDecision()

    # First write
    success1 = sink.record_decision(
        decision_id=decision_id,
        signal=signal1,
        risk_input=risk_input,
        risk_decision=risk_decision
    )
    assert success1 is True, "First write failed"

    # Second write with same decision_id but different ts
    success2 = sink.record_decision(
        decision_id=decision_id,
        signal=signal2,
        risk_input=risk_input,
        risk_decision=risk_decision
    )
    assert success2 is True, "Second write failed"

    # Verify the database state
    with pg_connection.cursor() as cur:
        # Check risk_decisions
        cur.execute("SELECT ts FROM risk_decisions WHERE decision_id = %s ORDER BY ts", (decision_id,))
        rows_decisions = cur.fetchall()

        # Checking contract: Currently, it results in 2 rows.
        assert len(rows_decisions) == 2, "Contract violation: Expected exactly 2 rows in risk_decisions for different timestamps"

        # Check risk_snapshot
        cur.execute("SELECT ts FROM risk_snapshot WHERE decision_id = %s ORDER BY ts", (decision_id,))
        rows_snapshot = cur.fetchall()

        assert len(rows_snapshot) == 2, "Contract violation: Expected exactly 2 rows in risk_snapshot for different timestamps"

def test_record_decision_idempotent(pg_connection, cleanup_postgres):
    """
    Test two sequential record_decisions with the SAME decision_id AND SAME created_ts_ms.
    Contract: This MUST update an existing row (1 row total).
    """
    sink = RiskAuditSqlSink(dsn=PG_DSN, enabled=True)

    decision_id = "test_on_conflict_001"
    ts1 = int(time.time() * 1000)

    signal1 = {"ts_event_ms": ts1, "symbol": "BTCUSDT", "risk_cluster": "cls"}
    risk_input = MockRiskInput()
    risk_decision = MockRiskDecision()

    # First write
    success1 = sink.record_decision(
        decision_id=decision_id,
        signal=signal1,
        risk_input=risk_input,
        risk_decision=risk_decision
    )
    assert success1 is True, "First write failed"

    # Second write with SAME decision_id and SAME ts
    success2 = sink.record_decision(
        decision_id=decision_id,
        signal=signal1,
        risk_input=risk_input,
        risk_decision=risk_decision
    )
    assert success2 is True, "Second write failed"

    # Verify the database state
    with pg_connection.cursor() as cur:
        cur.execute("SELECT ts FROM risk_decisions WHERE decision_id = %s ORDER BY ts", (decision_id,))
        rows_decisions = cur.fetchall()
        assert len(rows_decisions) == 1, "Idempotent write should result in 1 row in risk_decisions"

        cur.execute("SELECT ts FROM risk_snapshot WHERE decision_id = %s ORDER BY ts", (decision_id,))
        rows_snapshot = cur.fetchall()
        assert len(rows_snapshot) == 1, "Idempotent write should result in 1 row in risk_snapshot"

