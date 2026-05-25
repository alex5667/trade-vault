"""Unit tests for ``runners.ml_predictions_writer`` normalization."""

from runners.ml_predictions_writer import _normalize_row


def test_normalize_row_reads_nested_ml_payload_and_computes_margin():
    row, reason = _normalize_row(
        {
            "sid": "sig-1",
            "symbol": "BTCUSDT",
            "ts_ms": "1770000000000",
            "ml": {
                "model_run_id": "edge-stack-v1",
                "mode": "SHADOW",
                "p_edge": 0.61,
                "p_min": 0.55,
                "allow": True,
                "bucket": "trend",
                "latency_us": 42,
            },
        }
    )

    assert reason == ""
    assert row is not None
    assert row["model_ver"] == "edge-stack-v1"
    assert row["mode"] == "SHADOW"
    assert row["p_edge"] == 0.61
    assert row["p_min"] == 0.55
    assert round(row["p_margin"], 6) == 0.06
    assert row["allow"] is True
    assert row["bucket"] == "trend"
    assert row["latency_us"] == 42


def test_normalize_row_uses_unknown_model_version_when_absent():
    row, reason = _normalize_row(
        {
            "sid": "sig-2",
            "symbol": "ETHUSDT",
            "ts_ms": "1770000000001",
            "p_edge": "0.50",
            "p_min": "0.55",
        }
    )

    assert reason == ""
    assert row is not None
    assert row["model_ver"] == "unknown"
    assert round(row["p_margin"], 6) == -0.05
