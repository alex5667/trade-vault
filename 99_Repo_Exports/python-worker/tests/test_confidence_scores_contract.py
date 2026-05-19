"""Contract tests for signals:confidence:scores stream.

Verifies that:
1. _maybe_publish_confidence_scores emits a payload with all required fields
   at schema_version=1 (producer contract).
2. StreamArchiver.conf_score_row() correctly parses the producer payload
   into the DB insert tuple (consumer contract).
3. evidence_map → evidence_json round-trip works.
4. Missing optional fields (confidence_final, context_json) are nullable.
"""
from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch


# ---------------------------------------------------------------------------
# Producer contract — _maybe_publish_confidence_scores payload shape
# ---------------------------------------------------------------------------

def _make_minimal_pipeline():
    """Stub SignalPipeline with only the fields needed for _maybe_publish_confidence_scores."""
    import importlib
    sp_mod = importlib.import_module("services.orderflow.signal_pipeline")

    class _Stub:
        pass

    stub = _Stub()
    stub.conf_scores_publish_enabled = True
    stub.conf_scores_schema_version = 1
    stub.conf_scores_stream = "signals:confidence:scores"
    stub.conf_scores_stream_maxlen = 10_000
    stub.conf_scores_include_evidence_json = False
    stub.conf_scores_quarantine_stream = "signals:confidence:scores:quarantine"
    stub.conf_scores_quarantine_maxlen = 1_000
    stub._cached_service_name = "test-worker"

    # Bind methods
    stub._safe_num = sp_mod.SignalPipeline._safe_num.__get__(stub)
    stub._build_conf_evidence_map = sp_mod.SignalPipeline._build_conf_evidence_map.__get__(stub)
    stub._extract_conf_scores = sp_mod.SignalPipeline._extract_conf_scores.__get__(stub)
    stub._conf_scores_enabled = sp_mod.SignalPipeline._conf_scores_enabled.__get__(stub)
    stub._maybe_publish_confidence_scores = sp_mod.SignalPipeline._maybe_publish_confidence_scores.__get__(stub)

    # Mock publisher
    captured: list[dict] = []

    async def _xadd_json(*, sink, payload, symbol, **kw):
        captured.append(dict(payload))

    pub = MagicMock()
    pub.xadd_json = _xadd_json
    stub.publisher = pub
    stub._captured = captured
    return stub


def _run(coro):
    return asyncio.run(coro)


REQUIRED_FIELDS = {
    "schema_version",
    "producer",
    "sid",
    "symbol",
    "ts_event_ms",
    "confidence_raw",
    "evidence_map",
}


def test_all_required_fields_present():
    stub = _make_minimal_pipeline()
    signal = {
        "signal_id": "s-001",
        "confidence": 0.72,
        "indicators": {"spread_bps": 3.5},
    }
    _run(stub._maybe_publish_confidence_scores(
        symbol="BTCUSDT",
        sid="s-001",
        ts_event_ms=1_716_000_000_000,
        signal=signal,
        confirmations=["rsi_agree=1", "obi_stable=0.8"],
        indicators={"spread_bps": 3.5},
        evidence_dict={},
    ))

    assert len(stub._captured) == 1, "Expected exactly one published payload"
    evt = stub._captured[0]

    missing = REQUIRED_FIELDS - set(evt.keys())
    assert not missing, f"Missing required fields: {missing}"


def test_schema_version_is_1():
    stub = _make_minimal_pipeline()
    _run(stub._maybe_publish_confidence_scores(
        symbol="ETHUSDT",
        sid="s-002",
        ts_event_ms=1_716_000_001_000,
        signal={"confidence": 0.55},
        confirmations=[],
        indicators={},
        evidence_dict={},
    ))
    assert stub._captured[0]["schema_version"] == 1


def test_producer_matches_service_name():
    stub = _make_minimal_pipeline()
    stub._cached_service_name = "scanner-python-worker"
    _run(stub._maybe_publish_confidence_scores(
        symbol="SOLUSDT",
        sid="s-003",
        ts_event_ms=1_716_000_002_000,
        signal={"confidence": 0.60},
        confirmations=[],
        indicators={},
        evidence_dict={},
    ))
    assert stub._captured[0]["producer"] == "scanner-python-worker"


def test_evidence_map_is_dict():
    stub = _make_minimal_pipeline()
    _run(stub._maybe_publish_confidence_scores(
        symbol="BTCUSDT",
        sid="s-004",
        ts_event_ms=1_716_000_003_000,
        signal={"confidence": 0.80},
        confirmations=["sweep=1", "div_strength=0.9"],
        indicators={"spread_bps": 2.0},
        evidence_dict={},
    ))
    evt = stub._captured[0]
    assert isinstance(evt["evidence_map"], dict), "evidence_map must be a dict"


def test_confidence_final_nullable():
    """confidence_final may be None when signal has no separate final confidence."""
    stub = _make_minimal_pipeline()
    _run(stub._maybe_publish_confidence_scores(
        symbol="BTCUSDT",
        sid="s-005",
        ts_event_ms=1_716_000_004_000,
        signal={},
        confirmations=[],
        indicators={},
        evidence_dict={},
    ))
    evt = stub._captured[0]
    # confidence_raw must be present; confidence_final is allowed to be None
    assert "confidence_raw" in evt
    # If confidence_final key exists, it may be None — that's OK
    if "confidence_final" in evt:
        pass  # nullable field — any value is valid


def test_ts_event_ms_passthrough():
    stub = _make_minimal_pipeline()
    ts = 1_716_042_000_123
    _run(stub._maybe_publish_confidence_scores(
        symbol="BTCUSDT",
        sid="s-006",
        ts_event_ms=ts,
        signal={"confidence": 0.65},
        confirmations=[],
        indicators={},
        evidence_dict={},
    ))
    assert stub._captured[0]["ts_event_ms"] == ts


def test_disabled_when_flag_off():
    stub = _make_minimal_pipeline()
    stub.conf_scores_publish_enabled = False
    _run(stub._maybe_publish_confidence_scores(
        symbol="BTCUSDT",
        sid="s-007",
        ts_event_ms=1_716_000_005_000,
        signal={"confidence": 0.70},
        confirmations=[],
        indicators={},
        evidence_dict={},
    ))
    assert stub._captured == [], "No publish when disabled"


# ---------------------------------------------------------------------------
# Consumer contract — StreamArchiver.conf_score_row() parses producer payload
# ---------------------------------------------------------------------------

def _make_archiver():
    import importlib
    sa_mod = importlib.import_module("services.archivers.stream_archiver")
    arch = object.__new__(sa_mod.StreamArchiver)
    arch.conf_schema_accepted = {1}
    arch.conf_scores_store_evidence = True
    arch.conf_scores_store_context = False
    return arch


def test_archiver_parses_minimal_producer_payload():
    arch = _make_archiver()
    payload = {
        "schema_version": 1,
        "producer": "test-worker",
        "sid": "s-001",
        "symbol": "BTCUSDT",
        "ts_event_ms": 1_716_000_000_000,
        "confidence_raw": 0.72,
        "confidence_final": 0.75,
        "evidence_map": {"spread_bps": 3.5, "sweep": 1.0},
    }
    stream_id = "1716000000000-0"
    row = arch.conf_score_row(stream_id, payload)

    # (stream_id, ts_ms, ts, sid, symbol, schema_version, producer,
    #  confidence_raw, confidence_final, evidence_json, context_json)
    assert row[0] == stream_id
    assert row[3] == "s-001"        # sid
    assert row[4] == "BTCUSDT"      # symbol
    assert row[5] == 1              # schema_version
    assert row[6] == "test-worker"  # producer
    assert abs(row[7] - 0.72) < 1e-9  # confidence_raw
    assert abs(row[8] - 0.75) < 1e-9  # confidence_final
    ev = json.loads(row[9])
    assert ev["spread_bps"] == 3.5
    assert ev["sweep"] == 1.0


def test_archiver_confidence_final_null_ok():
    arch = _make_archiver()
    payload = {
        "schema_version": 1,
        "producer": "test-worker",
        "sid": "s-002",
        "symbol": "ETHUSDT",
        "ts_event_ms": 1_716_000_001_000,
        "confidence_raw": 0.60,
        # confidence_final deliberately absent
        "evidence_map": {},
    }
    row = arch.conf_score_row("1716000001000-0", payload)
    assert row[8] is None  # confidence_final nullable


def test_archiver_evidence_map_fallback_to_evidence_key():
    arch = _make_archiver()
    payload = {
        "schema_version": 1,
        "producer": "test-worker",
        "sid": "s-003",
        "symbol": "BTCUSDT",
        "ts_event_ms": 1_716_000_002_000,
        "confidence_raw": 0.65,
        "evidence": {"rsi_agree": 1.0},  # legacy key
    }
    row = arch.conf_score_row("1716000002000-0", payload)
    ev = json.loads(row[9])
    assert ev["rsi_agree"] == 1.0


def test_archiver_rejects_unknown_schema_version():
    import pytest
    arch = _make_archiver()
    payload = {
        "schema_version": 99,
        "sid": "s-004",
        "symbol": "BTCUSDT",
        "ts_event_ms": 1_716_000_003_000,
        "confidence_raw": 0.70,
    }
    with pytest.raises(ValueError, match="schema_version_not_accepted"):
        arch.conf_score_row("1716000003000-0", payload)
