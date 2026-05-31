"""gate_value_rollout_llm_advisor.py — LLM advisory for the rollout governor.

Same VETO-only contract as gate_value_llm_advisor: LLM cannot force a stage
advance, only block borderline ones (`freeze_candidate` from the guard's
ALLOWED_ACTIONS). Numerical gates remain authoritative.

The advisor is consulted at every stage transition candidate:
  STAGE_3_ACCUMULATING → STAGE_5_ADVISORY    (accumulation sufficient?)
  STAGE_5_ADVISORY     → STAGE_6_ENFORCE_CANDIDATE  (autocal stable?)
  STAGE_6_ENFORCE_CANDIDATE → STAGE_6_ENFORCED      (final flip safe?)

The final flip is highly sensitive — that's the moment when the autocal's
shadow writes start materially affecting downstream confidence_gate (once
the reader is wired). LLM gets a richer brief at this step.

ENV (advisor-specific, env-namespaced under GVR_*):
  GVR_LLM_BACKEND     auto|ollama|gemini|nvidia_deepseek  (default auto)
  GVR_LLM_PROMPT_VER  v1
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

PROMPT_VERSION = os.getenv("GVR_LLM_PROMPT_VER", "v1")


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
    current_stage: str,
    proposed_stage: str,
    stage_dwell_h: float,
    xlen_gated_out_outcomes: int,
    xlen_labels_tb: int,
    xlen_ml_confirm: int,
    growth_gated_out_window: int,
    growth_labels_tb_window: int,
    window_hours: float,
    autocal_groups: int,
    autocal_rollback_total: int,
    autocal_phase_distribution: dict[str, int],
    llm_veto_rate: float,
    days_since_start: float,
) -> str:
    """Build rollout LLM brief. Pure function."""
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
        "analysis_run_id": f"gvr_{int(time.time() * 1000)}",
        "policy_version": PROMPT_VERSION,
        "task": "advise_gate_value_rollout_stage_transition",
        "stage": {"current": current_stage, "proposed": proposed_stage},
        "stage_dwell_h": stage_dwell_h,
        "data_streams": {
            "stream:signals:gated_out_outcomes": {
                "xlen_now": xlen_gated_out_outcomes,
                "growth_in_window": growth_gated_out_window,
            },
            "labels:tb": {
                "xlen_now": xlen_labels_tb,
                "growth_in_window": growth_labels_tb_window,
            },
            "metrics:ml_confirm": {"xlen_now": xlen_ml_confirm},
            "window_hours": window_hours,
        },
        "autocal_state": {
            "groups": autocal_groups,
            "rollback_total": autocal_rollback_total,
            "phase_distribution": autocal_phase_distribution,
            "llm_veto_rate": llm_veto_rate,
            "days_since_start": days_since_start,
        },
        "context": (
            "Three-stage rollout: 3=ACCUMULATING (waiting for cohorts to grow), "
            "5=ADVISORY (autocal running ENFORCE=0, decisions accumulating), "
            "6_CANDIDATE (final dwell before flip), 6_ENFORCED (autocal writes "
            "shadow cfg). Flip 5→6 candidate ONLY if autocal decisions look "
            "stable (low rollback_total, phase distribution dominated by "
            "KEEP_CONFIRMED or RELAX_APPLIED, low llm_veto_rate). Flip 6c→6 "
            "is the sensitive one — it auto-enables ENFORCE via Redis "
            "override. You CANNOT force advancement; output is advisory + "
            "guarded. Recommend `freeze_candidate` to block, "
            "`propose_threshold_canary` to allow. NEVER recommend any action "
            "from `blocked_actions`."
        ),
        "allowed_actions": allowed_actions,
        "blocked_actions": blocked_actions,
        "response_format": {
            "schema_version": 1,
            "status": "ok|warn|error",
            "summary": "<= 240 chars — why allow or block this stage",
            "findings": ["<short string>", "..."],
            "recommendations": [
                {
                    "action": "propose_threshold_canary | freeze_candidate "
                              "| request_calibration_refresh",
                    "target": "stage_transition",
                    "risk": "low|medium|high",
                    "reason": "<= 200 chars",
                }
            ],
        },
    }

    return (
        "You are a quantitative risk reviewer. Read the brief below and "
        "return ONLY a compact JSON object matching `response_format`. "
        "If accumulation is steady and autocal is stable, recommend "
        "`propose_threshold_canary`. If anything looks off — slow growth, "
        "high rollback counts, churning phase distribution, LLM veto loop, "
        "insufficient dwell — recommend `freeze_candidate`. NEVER "
        "recommend any action from `blocked_actions`.\n\n"
        + json.dumps(brief, ensure_ascii=False)
    )


def _call_ollama(prompt: str, timeout_sec: float) -> str | None:
    from urllib.parse import urlparse

    raw_url = (
        os.getenv("OLLAMA_BASE_URL", "").strip()
        or os.getenv("OLLAMA_URL", "").strip()
        or "http://ollama:11434"
    )
    url = raw_url.rstrip("/")
    try:
        parsed = urlparse(url)
        if parsed.scheme and parsed.netloc:
            url = f"{parsed.scheme}://{parsed.netloc}"
    except Exception:
        pass
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
    backend = (os.getenv("GVR_LLM_BACKEND", "auto") or "auto").lower()
    if backend == "ollama":
        return _call_ollama(prompt, timeout_sec)
    if backend == "auto":
        return _call_ollama(prompt, timeout_sec)
    return _call_ollama(prompt, timeout_sec)


def _build_envelope(
    raw_payload: dict[str, Any] | None,
    *,
    current_stage: str,
    proposed_stage: str,
    fallback_reason: str = "",
) -> dict[str, Any]:
    base: dict[str, Any] = {
        "schema_version": 1,
        "analysis_run_id": f"gvr_{int(time.time() * 1000)}",
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
                "target": str(r.get("target") or "stage_transition")[:64],
                "risk": str(r.get("risk") or "medium").strip().lower()[:16] or "medium",
                "reason": str(r.get("reason") or "")[:200],
                "current_stage": current_stage,
                "proposed_stage": proposed_stage,
            }
        )
    base["recommendations"] = cleaned
    return base


def advise_stage_transition(
    *,
    current_stage: str,
    proposed_stage: str,
    stage_dwell_h: float,
    xlen_gated_out_outcomes: int,
    xlen_labels_tb: int,
    xlen_ml_confirm: int,
    growth_gated_out_window: int,
    growth_labels_tb_window: int,
    window_hours: float,
    autocal_groups: int,
    autocal_rollback_total: int,
    autocal_phase_distribution: dict[str, int],
    llm_veto_rate: float,
    days_since_start: float,
    timeout_sec: float = 30.0,
) -> dict[str, Any]:
    """High-level entry: build prompt, call LLM, guard the output."""
    from orderflow_services.llm_recommendation_guard_v1 import guard_recommendations

    prompt = build_prompt(
        current_stage=current_stage,
        proposed_stage=proposed_stage,
        stage_dwell_h=stage_dwell_h,
        xlen_gated_out_outcomes=xlen_gated_out_outcomes,
        xlen_labels_tb=xlen_labels_tb,
        xlen_ml_confirm=xlen_ml_confirm,
        growth_gated_out_window=growth_gated_out_window,
        growth_labels_tb_window=growth_labels_tb_window,
        window_hours=window_hours,
        autocal_groups=autocal_groups,
        autocal_rollback_total=autocal_rollback_total,
        autocal_phase_distribution=autocal_phase_distribution,
        llm_veto_rate=llm_veto_rate,
        days_since_start=days_since_start,
    )
    raw_text = _call_llm(prompt, timeout_sec=timeout_sec)
    if raw_text is None:
        envelope = _build_envelope(
            None,
            current_stage=current_stage,
            proposed_stage=proposed_stage,
            fallback_reason="llm_unreachable",
        )
        return guard_recommendations(envelope)
    parsed = _extract_json_obj(raw_text)
    envelope = _build_envelope(
        parsed,
        current_stage=current_stage,
        proposed_stage=proposed_stage,
        fallback_reason="llm_unparseable",
    )
    return guard_recommendations(envelope)
