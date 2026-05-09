from __future__ import annotations

"""
tests/news_pipeline/test_p6_repair_evidence_unit.py

Unit tests for P6 JSON repair + evidence (citations) path.

Tests:
  T1. Pydantic validation — invalid JSON → repair JSON  (LLM replaced by stub)
  T2. citations_are_substrings check on repaired event
  T3. NewsEventContractDTO strict schema (extra fields rejected)

Design: all async‑compatible, no real Redis/LLM needed.
Dependencies: pydantic (already installed in python-worker venv).
"""

import json

import pytest

# ── DTOs under test ────────────────────────────────────────────────────────────
from news_pipeline.p6_dto import (
    NewsEventContractDTO,
)

# ── Test data helpers ──────────────────────────────────────────────────────────

def _valid_contract_payload(**overrides) -> dict:
    """Minimal valid ContractDTO payload."""
    payload = {
        "schema_ver": "v1",
        "prompt_ver": "p2026-03-04b",
        "provider": "gemini",
        "model_id": "gemini-1.5-flash",
        "doc_id": "doc-abc-123",
        "event_type": "macro",
        "symbols": ["BTCUSDT"],
        "impact": 0.7,
        "bias": {"up": 0.6, "down": 0.3},
        "confidence": 0.8,
        "credibility_hint": 0.5,
        "citations": [],
        "event_ts_ms": 1709900000000,
    }
    payload.update(overrides)
    return payload


# ── T1: Pydantic validation + repair path (stub) ───────────────────────────────

class _StubLLMRepair:
    """Stub that returns valid JSON on 'repair' call, used instead of real LLM."""

    def __init__(self, valid_payload: str) -> None:
        self._payload = valid_payload

    def repair(self, bad_json: str) -> str:
        # Ignore bad_json, return the preset valid payload
        return self._payload


def _simulate_repair_path(bad_json: str, stub: _StubLLMRepair) -> dict:
    from dataclasses import asdict

    try:
        data = json.loads(bad_json)
        # All fields present and within range?
        try:
            NewsEventContractDTO(**data)
        except TypeError:
            raise json.JSONDecodeError("Missing or extra kwargs", bad_json, 0)
        return data
    except (json.JSONDecodeError, TypeError):
        repaired_json = stub.repair(bad_json)
        data2 = json.loads(repaired_json)
        dto = NewsEventContractDTO(**data2)
        return asdict(dto)


def test_repair_invalid_json_produces_valid_contract():
    """Initial JSON is invalid → repair stub returns valid → DTO validates OK."""
    bad_json = '{"impact": "not_a_number", "symbols": "BTCUSDT"'  # malformed
    good_payload = _valid_contract_payload()
    stub = _StubLLMRepair(json.dumps(good_payload))

    result = _simulate_repair_path(bad_json, stub)
    assert result["impact"] == 0.7
    assert result["symbols"] == ["BTCUSDT"]


def test_repair_already_valid_skips_repair():
    """If initial JSON is already valid, no repair call needed."""
    good_payload = _valid_contract_payload()
    stub = _StubLLMRepair("{}")  # stub would return invalid JSON if called

    result = _simulate_repair_path(json.dumps(good_payload), stub)
    assert result["event_type"] == "macro"


# ── T2: citations_are_substrings ───────────────────────────────────────────────

def citations_are_substrings(text: str, citations: list) -> bool:
    """Check that every citation's text is a substring of `text`."""
    for c in citations:
        snippet = c.get("text", "") if isinstance(c, dict) else str(c)
        if snippet and snippet not in text:
            return False
    return True


def test_citations_are_substrings_pass():
    text = "FOMC raised rates by 25 bps. Bitcoin fell 3%."
    citations = [
        {"text": "FOMC raised rates by 25 bps"},
        {"text": "Bitcoin fell 3%"},
    ]
    assert citations_are_substrings(text, citations)


def test_citations_are_substrings_fail():
    text = "FOMC raised rates by 25 bps."
    citations = [{"text": "Bitcoin surged 10%"}]  # not in text
    assert not citations_are_substrings(text, citations)


def test_empty_citations_always_pass():
    assert citations_are_substrings("anything", [])


# ── T3: NewsEventContractDTO strict schema ────────────────────────────────────

class TestNewsEventContractDTO:
    def test_valid_payload_accepted(self):
        dto = NewsEventContractDTO(**_valid_contract_payload())
        assert dto.impact == pytest.approx(0.7)
        assert dto.confidence == pytest.approx(0.8)

    def test_extra_fields_rejected(self):
        # extra=forbid logic is now naturally handled by dataclass throwing TypeError
        bad = _valid_contract_payload(rationale="some free text")  # not in contract
        with pytest.raises(TypeError):
            NewsEventContractDTO(**bad)

    def test_serialization_roundtrip(self):
        from dataclasses import asdict
        payload = _valid_contract_payload()
        dto = NewsEventContractDTO(**payload)
        dumped = asdict(dto)
        assert dumped["doc_id"] == "doc-abc-123"
        assert isinstance(dumped["symbols"], list)
