from __future__ import annotations

"""edge_directional_bias_llm_advisor.py — LLM advisory for bias phase promotion.

Called by `edge_directional_bias_autocal_v1.run_once` when numerical criteria
for promoting a (direction × regime) bucket are met. The LLM reads a compact
JSON brief and returns a recommendation object that we route through
`llm_recommendation_guard_v1.guard_recommendations`. The guard:

  * BLOCKS unsafe actions (`enable_enforce`, `raise_risk_limit`, ...).
  * ALLOWS only `propose_threshold_canary` / `freeze_candidate`
    / `request_calibration_refresh` / etc.

The autocalibrator's `_advisory_blocks_promotion` then:
  * If `blocked_recommendations` non-empty → block promotion (LLM proposed
    something dangerous; guard refused; we don't trust the run).
  * If any guarded recommendation has action=`freeze_candidate` → block.
  * Else → allow promotion.

Result: LLM can never FORCE a promotion or rollback — numerical gates are
authoritative. LLM only VETOES borderline promotions. This matches the
project's `BLOCKED_ACTIONS` policy in llm_recommendation_guard_v1.

Backends (tried in order, first available wins):
  1) Ollama   — `OLLAMA_BASE_URL` (or `OLLAMA_URL`) + `OLLAMA_MODEL`
                Project default: in-stack `ollama:11434` container,
                model `deepseek-r1:8b` (already pulled for aiops-agent).
                Preferred — no external API keys needed.
  2) Gemini   — `GEMINI_API_KEY` + `GEMINI_MODEL` (default gemini-1.5-pro)
  3) Nvidia DeepSeek — `NVIDIA_DEEPSEEK_API_KEY` + `NVIDIA_DEEPSEEK_MODEL`

ENV (advisor-specific):
  EDB_AC_LLM_BACKEND     auto|gemini|nvidia_deepseek|ollama  (default auto;
                         set =ollama to force local-only and skip cloud
                         backends entirely)
  EDB_AC_LLM_PROMPT_VER  v1
"""

import json
import logging
import os
import time
import urllib.error
import urllib.request
from typing import Any

logger = logging.getLogger(__name__)

PROMPT_VERSION = os.getenv("EDB_AC_LLM_PROMPT_VER", "v1")


def _extract_json_obj(text: str) -> dict | None:
    """Best-effort JSON extraction (mirrors news_pipeline.llm_client)."""
    text = (text or "").strip()
    if not text:
        return None
    if "<think>" in text:
        parts = text.split("</think>")
        text = parts[-1].strip()
    try:
        v = json.loads(text)
        return v if isinstance(v, dict) else None
    except Exception:
        pass
    i = text.find("{")
    j = text.rfind("}")
    if i >= 0 and j > i:
        try:
            v = json.loads(text[i : j + 1])
            return v if isinstance(v, dict) else None
        except Exception:
            return None
    return None


def build_prompt(
    *,
    bucket_key: str,
    current_phase: str,
    proposed_phase: str,
    n_baseline: int,
    baseline_avg_r: float,
    n_applied: int,
    applied_avg_r: float,
    dwell_h: float,
) -> str:
    """Build a tight, structured prompt. Returns a string the LLM ingests.

    Pure function — no I/O. Used both in production and tests.
    """
    try:
        direction, regime = bucket_key.split("|", 1)
    except ValueError:
        direction, regime = bucket_key, ""

    allowed_actions = [
        "propose_threshold_canary",
        "freeze_candidate",
        "request_calibration_refresh",
    ]
    blocked_actions = [
        "enable_enforce",
        "raise_risk_limit",
        "lower_risk_limit",
        "change_execution_caps",
        "change_exit_policy",
        "change_position_size",
    ]

    brief = {
        "schema_version": 1,
        "analysis_run_id": f"edb_ac_{int(time.time() * 1000)}",
        "policy_version": PROMPT_VERSION,
        "task": "advise_directional_bias_phase_transition",
        "bucket": {
            "key": bucket_key,
            "direction": direction,
            "regime": regime,
        },
        "phase": {
            "current": current_phase,
            "proposed": proposed_phase,
        },
        "stats": {
            "n_baseline": n_baseline,
            "baseline_avg_r": baseline_avg_r,
            "n_applied": n_applied,
            "applied_avg_r": applied_avg_r,
            "dwell_h_in_current_phase": dwell_h,
        },
        "context": (
            "We tighten EdgeCostGate p_min for counter-trend signals "
            "(side ≠ SMT leader). Plan 2 rollout: OBSERVE (bias=0.00) -> "
            "CANARY_LOW (0.03) -> CANARY_MID (0.05) -> CANARY_HIGH (0.06). "
            "Numerical gates already confirmed: dwell >= threshold, samples "
            "sufficient, applied-window R within no-harm band vs baseline. "
            "Your job: catch failure modes the numerics miss "
            "(distribution shift, regime breakdown, sample-bias, n too low "
            "for confidence). You CANNOT force promotion — your output is "
            "advisory only and will be guarded."
        ),
        "allowed_actions": allowed_actions,
        "blocked_actions": blocked_actions,
        "response_format": {
            "schema_version": 1,
            "status": "ok|warn|error",
            "summary": "<= 240 chars, why you allow or block",
            "findings": ["<short string>", "..."],
            "recommendations": [
                {
                    "action": "propose_threshold_canary | freeze_candidate "
                              "| request_calibration_refresh",
                    "target": "bucket_key",
                    "risk": "low|medium|high",
                    "reason": "<= 200 chars",
                }
            ],
        },
    }

    return (
        "You are a quantitative risk reviewer. Read the brief below and "
        "return ONLY a compact JSON object matching `response_format`. "
        "If statistics are stable and consistent with the proposed phase, "
        "recommend `propose_threshold_canary` (action). If anything looks "
        "off — sample bias, low confidence, regime instability, distribution "
        "shift — recommend `freeze_candidate`. NEVER recommend any action "
        "from `blocked_actions`.\n\n"
        + json.dumps(brief, ensure_ascii=False)
    )


def _call_gemini(prompt: str, timeout_sec: float) -> str | None:
    api_key = os.getenv("GEMINI_API_KEY", "").strip()
    if not api_key:
        return None
    model = os.getenv("GEMINI_MODEL", "gemini-1.5-pro").strip()
    endpoint = (
        f"https://generativelanguage.googleapis.com/v1beta/models/"
        f"{model}:generateContent"
    )
    body = json.dumps(
        {
            "contents": [{"parts": [{"text": prompt}]}],
            "generationConfig": {
                "temperature": 0.1,
                "maxOutputTokens": 512,
            },
        }
    ).encode("utf-8")
    try:
        req = urllib.request.Request(
            url=endpoint,
            data=body,
            headers={"Content-Type": "application/json", "x-goog-api-key": api_key},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=timeout_sec) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
        data = json.loads(raw)
        try:
            return data["candidates"][0]["content"]["parts"][0]["text"]
        except Exception:
            return raw[:1024]
    except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError) as e:
        logger.debug("gemini call failed: %s", e)
        return None
    except Exception as e:
        logger.debug("gemini unexpected error: %s", e)
        return None


def _call_nvidia_deepseek(prompt: str, timeout_sec: float) -> str | None:
    api_key = os.getenv("NVIDIA_DEEPSEEK_API_KEY", "").strip()
    if not api_key:
        return None
    model = os.getenv("NVIDIA_DEEPSEEK_MODEL", "deepseek-ai/deepseek-v3.2").strip()
    endpoint = "https://integrate.api.nvidia.com/v1/chat/completions"
    body = json.dumps(
        {
            "model": model,
            "messages": [{"role": "user", "content": prompt}],
            "max_tokens": 512,
            "temperature": 0.1,
            "top_p": 0.95,
            "stream": False,
        }
    ).encode("utf-8")
    try:
        req = urllib.request.Request(
            url=endpoint,
            data=body,
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {api_key}",
                "Accept": "application/json",
            },
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=timeout_sec) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
        data = json.loads(raw)
        try:
            return data["choices"][0]["message"]["content"]
        except Exception:
            return raw[:1024]
    except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError) as e:
        logger.debug("nvidia_deepseek call failed: %s", e)
        return None
    except Exception as e:
        logger.debug("nvidia_deepseek unexpected error: %s", e)
        return None


def _normalize_ollama_base(url: str) -> str:
    """Strip any `/api/...` suffix so we always have a clean base URL.

    Project conventions disagree: news_pipeline + crypto-orderflow set
    `OLLAMA_BASE_URL=http://ollama:11434` (bare), but aiops-agent uses
    `OLLAMA_URL=http://ollama:11434/api/generate` (with path). The advisor
    must accept either and always append `/api/chat` at the end without
    producing `/api/generate/api/chat`.
    """
    from urllib.parse import urlparse

    u = url.strip().rstrip("/")
    if not u:
        return ""
    try:
        parsed = urlparse(u)
        if parsed.scheme and parsed.netloc:
            return f"{parsed.scheme}://{parsed.netloc}"
    except Exception:
        pass
    return u


def _call_ollama(prompt: str, timeout_sec: float) -> str | None:
    # Project convention is OLLAMA_BASE_URL (news_pipeline, crypto-orderflow,
    # notify-telegram-v2). Accept OLLAMA_URL as fallback (aiops-agent style,
    # may include /api/generate suffix — _normalize_ollama_base strips it).
    # Default points at the in-stack ollama container — autocal shares
    # scanner-infra with it so DNS resolves without extra config.
    url = _normalize_ollama_base(
        os.getenv("OLLAMA_BASE_URL", "").strip()
        or os.getenv("OLLAMA_URL", "").strip()
        or "http://ollama:11434"
    )
    if not url:
        return None
    model = os.getenv("OLLAMA_MODEL", "deepseek-r1:8b").strip()
    body = json.dumps(
        {
            "model": model,
            "messages": [{"role": "user", "content": prompt}],
            "stream": False,
            "options": {"temperature": 0.1, "num_predict": 512},
        }
    ).encode("utf-8")
    endpoint = url.rstrip("/") + "/api/chat"
    try:
        req = urllib.request.Request(
            url=endpoint,
            data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=timeout_sec) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
        data = json.loads(raw)
        try:
            return data["message"]["content"]
        except Exception:
            return raw[:1024]
    except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError) as e:
        logger.debug("ollama call failed: %s", e)
        return None
    except Exception as e:
        logger.debug("ollama unexpected error: %s", e)
        return None


def _call_llm(prompt: str, timeout_sec: float) -> str | None:
    """Try backends in order from EDB_AC_LLM_BACKEND or auto.

    Auto order prefers local Ollama first — no API key needed, in-stack,
    no external egress on the hot path. Falls back to cloud backends only
    when the local instance is unreachable.
    """
    backend = (os.getenv("EDB_AC_LLM_BACKEND", "auto") or "auto").lower()
    if backend == "gemini":
        return _call_gemini(prompt, timeout_sec)
    if backend == "nvidia_deepseek":
        return _call_nvidia_deepseek(prompt, timeout_sec)
    if backend == "ollama":
        return _call_ollama(prompt, timeout_sec)
    # auto: local first, cloud fallback
    for fn in (_call_ollama, _call_gemini, _call_nvidia_deepseek):
        out = fn(prompt, timeout_sec)
        if out is not None:
            return out
    return None


def _build_envelope(
    raw_payload: dict[str, Any] | None,
    *,
    bucket_key: str,
    current_phase: str,
    proposed_phase: str,
    fallback_reason: str = "",
) -> dict[str, Any]:
    """Coerce raw LLM JSON into the envelope expected by the guard.

    The guard requires: schema_version, analysis_run_id, status, summary,
    findings, recommendations. If LLM output is missing any of these,
    or unparseable, return a NEUTRAL envelope (no recommendations) — the
    guard will pass it through with empty `guarded_recommendations`, and
    `_advisory_blocks_promotion` will see no blocks → numerical gate stands.
    """
    base: dict[str, Any] = {
        "schema_version": 1,
        "analysis_run_id": f"edb_ac_{int(time.time() * 1000)}",
        "policy_version": PROMPT_VERSION,
        "status": "ok",
        "summary": "",
        "findings": [],
        "recommendations": [],
    }
    if not isinstance(raw_payload, dict):
        base["status"] = "ok"
        base["summary"] = fallback_reason[:240] or "no_llm_payload"
        return base

    # Copy through known fields (with safe defaults)
    base["status"] = str(raw_payload.get("status") or "ok").strip().lower()[:16]
    base["summary"] = str(raw_payload.get("summary") or "")[:240]
    findings = raw_payload.get("findings") or []
    if isinstance(findings, list):
        base["findings"] = [str(x)[:160] for x in findings[:10]]

    recs = raw_payload.get("recommendations") or []
    if not isinstance(recs, list):
        recs = []
    cleaned: list[dict[str, Any]] = []
    for r in recs[:5]:
        if not isinstance(r, dict):
            continue
        action = str(r.get("action") or "").strip()
        cleaned.append(
            {
                "action": action,
                "target": str(r.get("target") or bucket_key)[:64],
                "risk": str(r.get("risk") or "medium").strip().lower()[:16] or "medium",
                "reason": str(r.get("reason") or "")[:200],
                "current_phase": current_phase,
                "proposed_phase": proposed_phase,
            }
        )
    base["recommendations"] = cleaned
    return base


def advise_bucket_transition(
    *,
    bucket_key: str,
    current_phase: str,
    proposed_phase: str,
    n_baseline: int,
    baseline_avg_r: float,
    n_applied: int,
    applied_avg_r: float,
    dwell_h: float,
    timeout_sec: float = 8.0,
) -> dict[str, Any]:
    """High-level entry: build prompt, call LLM, guard the output.

    Returns the result of `guard_recommendations(envelope)`. On any LLM
    error returns a NEUTRAL envelope through the guard (no blocks, no
    recommendations) so the autocalibrator's numerical promotion stands.
    """
    # Late import keeps the module testable without runtime deps.
    from orderflow_services.llm_recommendation_guard_v1 import guard_recommendations

    prompt = build_prompt(
        bucket_key=bucket_key,
        current_phase=current_phase,
        proposed_phase=proposed_phase,
        n_baseline=n_baseline,
        baseline_avg_r=baseline_avg_r,
        n_applied=n_applied,
        applied_avg_r=applied_avg_r,
        dwell_h=dwell_h,
    )

    raw_text = _call_llm(prompt, timeout_sec=timeout_sec)
    if raw_text is None:
        envelope = _build_envelope(
            None,
            bucket_key=bucket_key,
            current_phase=current_phase,
            proposed_phase=proposed_phase,
            fallback_reason="llm_unreachable",
        )
        return guard_recommendations(envelope)

    parsed = _extract_json_obj(raw_text)
    envelope = _build_envelope(
        parsed,
        bucket_key=bucket_key,
        current_phase=current_phase,
        proposed_phase=proposed_phase,
        fallback_reason="llm_unparseable",
    )
    return guard_recommendations(envelope)
