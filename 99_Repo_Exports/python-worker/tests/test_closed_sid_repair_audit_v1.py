from __future__ import annotations

from services.orderflow.tools.closed_sid_repair_audit_v1 import inspect_closed_record


def test_inspect_closed_record_marks_weak_progress_sid_as_repairable() -> None:
    row = inspect_closed_record(
        "1779015063934-0",
        {
            "order_id": "oid-1",
            "sid": "weak_progress:BTCUSDT:1779011458422:L",
            "symbol": "BTCUSDT",
            "exit_ts_ms": "1779015063934",
        },
    )
    assert row["status"] == "repairable"
    assert row["weak_progress_sid"] is True
    assert row["new_sid"] == "crypto-of:BTCUSDT:1779011458422"


def test_inspect_closed_record_keeps_canonical_sid() -> None:
    row = inspect_closed_record(
        "1779015063934-0",
        {
            "order_id": "oid-2",
            "sid": "crypto-of:ETHUSDT:1779011458422",
            "symbol": "ETHUSDT",
            "exit_ts_ms": "1779015063934",
        },
    )
    assert row["status"] == "canonical"
    assert row["repairable"] is False
    assert row["new_sid"] == "crypto-of:ETHUSDT:1779011458422"


def test_inspect_closed_record_uses_stream_id_ts_when_exit_ts_missing() -> None:
    row = inspect_closed_record(
        "1779015063934-0",
        {
            "order_id": "oid-3",
            "sid": "",
            "symbol": "SOLUSDT",
        },
    )
    assert row["status"] == "missing_sid"
    assert int(row["exit_ts_ms"]) == 1779015063934
