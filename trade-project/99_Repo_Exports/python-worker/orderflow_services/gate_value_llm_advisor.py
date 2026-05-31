"""gate_value_llm_advisor.py — LLM advisory for gate_value autocal.

Called by `gate_value_autocalibrator_v1.run_once` after numerical gates pass.
LLM reads a compact JSON brief (cohort stats, lift, CI, proposed phase) and
returns a recommendation; we route it through `llm_recommendation_guard_v1`.

Same VETO-only contract as edge_directional_bias_llm_advisor:
  * LLM can NEVER force promotion / disable a gate.
  * LLM can only veto borderline promotions (`freeze_candidate`).
  * `propose_threshold_canary` is allowed but advisory.

Backends (auto order): in-stack Ollama → Gemini → Nvidia DeepSeek.
Default model: `deepseek-r1:8b` (shared with aiops-agent / EDB autocal).

ENV:
  GVA_LLM_BACKEND   auto|gemini|nvidia_deepseek|ollama  (default auto)
  GVA_LLM_PROMPT_VER  v1
  OLLAMA_BASE_URL / OLLAMA_URL / OLLAMA_MODEL (shared with EDB autocal)
"""

from __future__ import annotations

import json
import logging
import os
import time
import urllib.error
import urllib.request
from typing import Any

logger = logging.getLogger(__name__)

PROMPT_VERSION = os.getenv("GVA_LLM_PROMPT_VER", "v1")


def _extract_json_obj(text: str) -> dict | None:
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
    group_key: str,
    current_phase: str,
    proposed_phase: str,
    decision_action: str,
    passed_n: int,
    passed_avg_r: float,
    passed_win_rate: float,
    passed_profit_factor: float,
    gated_out_n: int,
    gated_out_avg_r: float,
    gated_out_win_rate: float,
    gated_out_profit_factor: float,
    avg_r_lift: float,
    false_negative_rate: float,
    ci_low: float,
    ci_high: float,
    dwell_h: float,
) -> str:
    """Build LLM brief. Pure function — used in tests and prod."""
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
        "analysis_run_id": f"gva_{int(time.time() * 1000)}",
        "policy_version": PROMPT_VERSION,
        "task": "advise_gate_value_phase_transition",
        "group": {"key": group_key},
        "phase": {"current": current_phase, "proposed": proposed_phase},
        "reporter_decision": decision_action,
        "stats": {
            "passed": {
                "n": passed_n,
                "avg_r": passed_avg_r,
                "win_rate": passed_win_rate,
                "profit_factor": passed_profit_factor,
            },
            "gated_out": {
                "n": gated_out_n,
                "avg_r": gated_out_avg_r,
                "win_rate": gated_out_win_rate,
                "profit_factor": gated_out_profit_factor,
            },
            "lift": {
                "avg_r": avg_r_lift,
                "false_negative_rate": false_negative_rate,
                "bootstrap_ci_low": ci_low,
                "bootstrap_ci_high": ci_high,
            },
            "dwell_h": dwell_h,
        },
        "context": (
            "Confidence gate filters signals where confidence < min_conf. "
            "We compare PASSED cohort (gate let through, real outcomes via "
            "labels:tb) vs GATED_OUT cohort (gate rejected, virtual outcomes "
            "via gated_out_outcome_tracker). avg_r_lift > 0 means the gate "
            "filters losers (good). false_negative_rate is win rate of the "
            "rejected cohort: if high, gate also rejects winners. "
            "Numerical gates already confirmed: sufficient samples, dwell, "
            "stability. Your job: catch failure modes the numerics miss — "
            "selection-policy mismatch (gated cohort uses virtual fills, "
            "passed uses real), regime shifts, TP/SL bucket mismatch. "
            "You CANNOT force phase change — advisory only, guarded."
        ),
        "allowed_actions": allowed_actions,
        "blocked_actions": blocked_actions,
        "response_format": {
            "schema_version": 1,
            "status": "ok|warn|error",
            "summary": "<= 240 chars — why allow or block",
            "findings": ["<short string>", "..."],
            "recommendations": [
                {
                    "action": "propose_threshold_canary | freeze_candidate "
                              "| request_calibration_refresh",
                    "target": "group_key",
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
        "recommend `propose_threshold_canary`. If anything looks off — "
        "selection-policy mismatch, low n, regime shift, virtual-vs-real "
        "cohort drift — recommend `freeze_candidate`. NEVER recommend any "
        "action from `blocked_actions`.\n\n"
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
            "generationConfig": {"temperature": 0.1, "maxOutputTokens": 512},
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
    backend = (os.getenv("GVA_LLM_BACKEND", "auto") or "auto").lower()
    if backend == "gemini":
        return _call_gemini(prompt, timeout_sec)
    if backend == "nvidia_deepseek":
        return _call_nvidia_deepseek(prompt, timeout_sec)
    if backend == "ollama":
        return _call_ollama(prompt, timeout_sec)
    for fn in (_call_ollama, _call_gemini, _call_nvidia_deepseek):
        out = fn(prompt, timeout_sec)
        if out is not None:
            return out
    return None


def _build_envelope(
    raw_payload: dict[str, Any] | None,
    *,
    group_key: str,
    current_phase: str,
    proposed_phase: str,
    fallback_reason: str = "",
) -> dict[str, Any]:
    base: dict[str, Any] = {
        "schema_version": 1,
        "analysis_run_id": f"gva_{int(time.time() * 1000)}",
        "policy_version": PROMPT_VERSION,
        "status": "ok",
        "summary": "",
        "findings": [],
        "recommendations": [],
    }
    if not isinstance(raw_payload, dict):
        base["summary"] = fallback_reason[:240] or "no_llm_payload"
        return base

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
                "target": str(r.get("target") or group_key)[:64],
                "risk": str(r.get("risk") or "medium").strip().lower()[:16] or "medium",
                "reason": str(r.get("reason") or "")[:200],
                "current_phase": current_phase,
                "proposed_phase": proposed_phase,
            }
        )
    base["recommendations"] = cleaned
    return base


def advise_gate_transition(
    *,
    group_key: str,
    current_phase: str,
    proposed_phase: str,
    decision_action: str,
    passed_n: int,
    passed_avg_r: float,
    passed_win_rate: float,
    passed_profit_factor: float,
    gated_out_n: int,
    gated_out_avg_r: float,
    gated_out_win_rate: float,
    gated_out_profit_factor: float,
    avg_r_lift: float,
    false_negative_rate: float,
    ci_low: float,
    ci_high: float,
    dwell_h: float,
    timeout_sec: float = 8.0,
) -> dict[str, Any]:
    """High-level entry: build prompt, call LLM, guard the output.

    LLM errors → neutral envelope through the guard (no blocks, no recs).
    """
    from orderflow_services.llm_recommendation_guard_v1 import guard_recommendations

    prompt = build_prompt(
        group_key=group_key,
        current_phase=current_phase,
        proposed_phase=proposed_phase,
        decision_action=decision_action,
        passed_n=passed_n,
        passed_avg_r=passed_avg_r,
        passed_win_rate=passed_win_rate,
        passed_profit_factor=passed_profit_factor,
        gated_out_n=gated_out_n,
        gated_out_avg_r=gated_out_avg_r,
        gated_out_win_rate=gated_out_win_rate,
        gated_out_profit_factor=gated_out_profit_factor,
        avg_r_lift=avg_r_lift,
        false_negative_rate=false_negative_rate,
        ci_low=ci_low,
        ci_high=ci_high,
        dwell_h=dwell_h,
    )

    raw_text = _call_llm(prompt, timeout_sec=timeout_sec)
    if raw_text is None:
        envelope = _build_envelope(
            None,
            group_key=group_key,
            current_phase=current_phase,
            proposed_phase=proposed_phase,
            fallback_reason="llm_unreachable",
        )
        return guard_recommendations(envelope)

    parsed = _extract_json_obj(raw_text)
    envelope = _build_envelope(
        parsed,
        group_key=group_key,
        current_phase=current_phase,
        proposed_phase=proposed_phase,
        fallback_reason="llm_unparseable",
    )
    return guard_recommendations(envelope)
