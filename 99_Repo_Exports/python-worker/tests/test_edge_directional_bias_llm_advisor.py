"""Tests for orderflow_services.edge_directional_bias_llm_advisor."""
from __future__ import annotations

import json
from unittest.mock import patch

import pytest

from orderflow_services import edge_directional_bias_llm_advisor as adv


def _kwargs(**over):
    base = dict(
        bucket_key="SHORT|trending_bull",
        current_phase="OBSERVE",
        proposed_phase="CANARY_LOW",
        n_baseline=200, baseline_avg_r=-0.4,
        n_applied=80, applied_avg_r=-0.1,
        dwell_h=49.5,
    )
    base.update(over)
    return base


# ─────────────────────────────────────────────────────────────────────
# build_prompt
# ─────────────────────────────────────────────────────────────────────


def test_build_prompt_contains_required_brief_fields():
    p = adv.build_prompt(**_kwargs())
    assert "SHORT|trending_bull" in p
    assert "CANARY_LOW" in p
    assert "OBSERVE" in p
    assert "propose_threshold_canary" in p
    assert "freeze_candidate" in p
    # Embedded JSON brief must parse
    i = p.find("{")
    brief = json.loads(p[i:])
    assert brief["bucket"]["direction"] == "SHORT"
    assert brief["bucket"]["regime"] == "trending_bull"
    assert brief["phase"]["proposed"] == "CANARY_LOW"
    assert brief["stats"]["n_applied"] == 80


def test_build_prompt_advertises_blocked_actions():
    p = adv.build_prompt(**_kwargs())
    for blocked in ("enable_enforce", "raise_risk_limit", "change_execution_caps"):
        assert blocked in p


# ─────────────────────────────────────────────────────────────────────
# _extract_json_obj / _build_envelope
# ─────────────────────────────────────────────────────────────────────


def test_extract_json_handles_markdown_wrap():
    text = "```json\n{\"status\":\"ok\"}\n```"
    out = adv._extract_json_obj(text)
    assert out == {"status": "ok"}


def test_extract_json_strips_think_block():
    text = "<think>thinking</think>{\"status\":\"warn\"}"
    out = adv._extract_json_obj(text)
    assert out == {"status": "warn"}


def test_build_envelope_neutral_when_no_payload():
    env = adv._build_envelope(
        None, bucket_key="SHORT|trending_bull",
        current_phase="OBSERVE", proposed_phase="CANARY_LOW",
        fallback_reason="llm_unreachable",
    )
    assert env["recommendations"] == []
    assert env["status"] == "ok"
    assert "llm_unreachable" in env["summary"]


def test_build_envelope_coerces_recommendations():
    payload = {
        "status": "warn",
        "summary": "x" * 1000,
        "findings": ["a", 42, "b"],
        "recommendations": [
            {"action": "propose_threshold_canary", "risk": "LOW"},
            "not_a_dict",
            {"action": "enable_enforce", "risk": "high"},  # guard will block downstream
        ],
    }
    env = adv._build_envelope(
        payload, bucket_key="SHORT|trending_bull",
        current_phase="OBSERVE", proposed_phase="CANARY_LOW",
    )
    assert len(env["summary"]) <= 240
    assert "a" in env["findings"]
    assert "42" in env["findings"]
    assert any(r["action"] == "propose_threshold_canary" for r in env["recommendations"])
    assert any(r["action"] == "enable_enforce" for r in env["recommendations"])
    assert all("current_phase" in r for r in env["recommendations"])


# ─────────────────────────────────────────────────────────────────────
# advise_bucket_transition — end-to-end with mocked LLM
# ─────────────────────────────────────────────────────────────────────


def test_advise_returns_guarded_payload_on_llm_unreachable():
    with patch.object(adv, "_call_llm", return_value=None):
        out = adv.advise_bucket_transition(**_kwargs())
    assert out["valid"] is True
    assert out["guarded_recommendations"] == []
    assert out["blocked_recommendations"] == []


def test_advise_passes_through_guard_for_safe_action():
    llm_response = json.dumps({
        "status": "ok",
        "summary": "stats look healthy",
        "findings": ["sample size adequate"],
        "recommendations": [
            {"action": "propose_threshold_canary", "risk": "low",
             "reason": "applied beats baseline by 0.3R"}
        ],
    })
    with patch.object(adv, "_call_llm", return_value=llm_response):
        out = adv.advise_bucket_transition(**_kwargs())
    assert out["valid"] is True
    assert len(out["guarded_recommendations"]) == 1
    assert out["guarded_recommendations"][0]["action"] == "propose_threshold_canary"
    assert out["guarded_recommendations"][0]["apply_mode"] == "REVIEW_ONLY"
    assert out["blocked_recommendations"] == []


def test_advise_guard_blocks_dangerous_action():
    """LLM hallucinates `enable_enforce` → guard rejects it → blocked list non-empty."""
    llm_response = json.dumps({
        "status": "ok",
        "summary": "ship it",
        "findings": [],
        "recommendations": [
            {"action": "enable_enforce", "risk": "low",
             "reason": "looks great"}
        ],
    })
    with patch.object(adv, "_call_llm", return_value=llm_response):
        out = adv.advise_bucket_transition(**_kwargs())
    assert out["valid"] is True
    assert out["guarded_recommendations"] == []
    assert len(out["blocked_recommendations"]) == 1
    assert out["blocked_recommendations"][0]["reason"] == "blocked_action"


def test_advise_handles_unparseable_llm_output():
    with patch.object(adv, "_call_llm", return_value="utter nonsense without json"):
        out = adv.advise_bucket_transition(**_kwargs())
    assert out["valid"] is True
    assert out["guarded_recommendations"] == []


def test_normalize_ollama_base_strips_api_suffix():
    assert adv._normalize_ollama_base("http://ollama:11434") == "http://ollama:11434"
    assert adv._normalize_ollama_base("http://ollama:11434/") == "http://ollama:11434"
    # aiops-agent convention: full path to endpoint
    assert adv._normalize_ollama_base("http://ollama:11434/api/generate") == "http://ollama:11434"
    assert adv._normalize_ollama_base("http://minik:11434/api/chat") == "http://minik:11434"
    assert adv._normalize_ollama_base("") == ""


def test_ollama_backend_called_with_normalized_url(monkeypatch):
    """Even when OLLAMA_URL has /api/generate suffix (aiops-agent style),
    _call_ollama posts to <base>/api/chat — never a doubled path."""
    monkeypatch.setenv("OLLAMA_URL", "http://minik:11434/api/generate")
    monkeypatch.delenv("OLLAMA_BASE_URL", raising=False)
    captured: dict = {}

    def fake_urlopen(req, timeout):
        captured["url"] = req.full_url
        class _R:
            def __enter__(self_): return self_
            def __exit__(self_, *a): return False
            def read(self_): return b'{"message":{"content":"{\\"status\\":\\"ok\\"}"}}'
        return _R()

    monkeypatch.setattr(adv.urllib.request, "urlopen", fake_urlopen)
    out = adv._call_ollama("hello", timeout_sec=1.0)
    assert out is not None
    assert captured["url"] == "http://minik:11434/api/chat"


def test_ollama_base_url_takes_priority_over_url(monkeypatch):
    monkeypatch.setenv("OLLAMA_BASE_URL", "http://override:11434")
    monkeypatch.setenv("OLLAMA_URL", "http://minik:11434")
    captured: dict = {}

    def fake_urlopen(req, timeout):
        captured["url"] = req.full_url
        class _R:
            def __enter__(self_): return self_
            def __exit__(self_, *a): return False
            def read(self_): return b'{"message":{"content":"{}"}}'
        return _R()

    monkeypatch.setattr(adv.urllib.request, "urlopen", fake_urlopen)
    adv._call_ollama("x", timeout_sec=1.0)
    assert captured["url"] == "http://override:11434/api/chat"


def test_call_llm_default_auto_tries_ollama_first(monkeypatch):
    """Auto backend order: Ollama, Gemini, DeepSeek — local-first."""
    monkeypatch.delenv("EDB_AC_LLM_BACKEND", raising=False)
    order: list[str] = []

    def fake_ollama(p, t):
        order.append("ollama"); return "ollama-resp"

    def fake_gemini(p, t):
        order.append("gemini"); return None

    def fake_ds(p, t):
        order.append("ds"); return None

    monkeypatch.setattr(adv, "_call_ollama", fake_ollama)
    monkeypatch.setattr(adv, "_call_gemini", fake_gemini)
    monkeypatch.setattr(adv, "_call_nvidia_deepseek", fake_ds)

    out = adv._call_llm("x", timeout_sec=1.0)
    assert out == "ollama-resp"
    assert order == ["ollama"]  # short-circuited on success


def test_call_llm_backend_pinned_to_ollama_skips_others(monkeypatch):
    monkeypatch.setenv("EDB_AC_LLM_BACKEND", "ollama")
    order: list[str] = []
    monkeypatch.setattr(adv, "_call_ollama", lambda p, t: (order.append("ollama") or "ok"))
    monkeypatch.setattr(adv, "_call_gemini", lambda p, t: (order.append("gemini") or "ok"))
    monkeypatch.setattr(adv, "_call_nvidia_deepseek", lambda p, t: (order.append("ds") or "ok"))
    adv._call_llm("x", timeout_sec=1.0)
    assert order == ["ollama"]


def test_advise_freeze_candidate_passes_guard():
    llm_response = json.dumps({
        "status": "warn",
        "summary": "sample size too small",
        "recommendations": [
            {"action": "freeze_candidate", "risk": "medium",
             "reason": "n_applied below confidence threshold"}
        ],
    })
    with patch.object(adv, "_call_llm", return_value=llm_response):
        out = adv.advise_bucket_transition(**_kwargs())
    assert any(r["action"] == "freeze_candidate" for r in out["guarded_recommendations"])
