from __future__ import annotations

import json
import os
import random
import time
from dataclasses import dataclass
from typing import Any

from orderflow_services.vertex_budget_guard_v1 import VertexBudgetGuardV1, estimate_vertex_triage_cost_usd

try:
    from google import genai  # type: ignore
    from google.genai import types as genai_types  # type: ignore
except Exception:  # pragma: no cover
    genai = None  # type: ignore
    genai_types = None  # type: ignore


PROMPT_TEMPLATE = """You are a production ML triage analyst for a low-latency trading system.
Return JSON only.
Policy version: {policy_version}
Prompt version: {prompt_version}

Allowed actions: require_shadow_retrain, freeze_candidate, unfreeze_candidate,
request_calibration_refresh, propose_threshold_canary, open_incident, draft_postmortem.

Input pack:
{payload}
"""


@dataclass
class ProviderResult:
    raw_text: str
    parsed: dict[str, Any] | None
    latency_ms: int
    estimated_cost_usd: float
    prompt_version: str
    policy_version: str
    model_name: str
    provider: str = "vertex"


class VertexGenAIProviderV1_1:
    def __init__(self, redis_url: str) -> None:
        self._project = os.getenv("VERTEX_PROJECT_ID", "")
        self._location = os.getenv("VERTEX_LOCATION", "global")
        self._model = os.getenv("VERTEX_TRIAGE_MODEL", "gemini-2.5-flash-lite")
        self._timeout_ms = int(os.getenv("VERTEX_TIMEOUT_MS", "45000") or 45000)
        self._max_retries = int(os.getenv("VERTEX_RETRY_MAX", "5") or 5)
        self._base_backoff_ms = int(os.getenv("VERTEX_RETRY_BASE_MS", "500") or 500)
        self._budget = VertexBudgetGuardV1(redis_url)
        if genai is None:
            raise RuntimeError("google-genai is required for Vertex provider")
        self._client = genai.Client(vertexai=True, project=self._project, location=self._location)

    def _sleep_backoff(self, attempt: int) -> None:
        base = self._base_backoff_ms * (2 ** max(0, attempt - 1))
        jitter = random.randint(0, max(50, base // 4))
        time.sleep((base + jitter) / 1000.0)

    def analyze(self, compact_pack: dict[str, Any]) -> ProviderResult:
        prompt_version = str(compact_pack.get("prompt_version") or os.getenv("ML_TRIAGE_PROMPT_VERSION", "ml_triage_v1"))
        policy_version = str(compact_pack.get("policy_version") or os.getenv("ML_TRIAGE_POLICY_VERSION", "policy_v1"))
        payload_json = json.dumps(compact_pack, ensure_ascii=False, sort_keys=True)
        prompt = PROMPT_TEMPLATE.format(prompt_version=prompt_version, policy_version=policy_version, payload=payload_json)
        est_cost = estimate_vertex_triage_cost_usd(len(prompt), int(os.getenv("VERTEX_TRIAGE_EST_OUTPUT_CHARS", "2500") or 2500))
        budget = self._budget.check_and_reserve(provider="vertex", model=self._model, estimated_cost_usd=est_cost, ts=int(time.time()))
        if not budget.allowed:
            raise RuntimeError(f"vertex_budget_blocked:{budget.reason}")
        started = time.time()
        last_exc: Exception | None = None
        for attempt in range(1, self._max_retries + 1):
            try:
                cfg = genai_types.GenerateContentConfig(
                    temperature=0.1,
                    max_output_tokens=int(os.getenv("VERTEX_TRIAGE_MAX_OUTPUT_TOKENS", "1500") or 1500),
                    response_mime_type="application/json",
                )
                resp = self._client.models.generate_content(
                    model=self._model,
                    contents=prompt,
                    config=cfg,
                )
                text = getattr(resp, "text", None) or "{}"
                parsed = json.loads(text)
                return ProviderResult(
                    raw_text=text,
                    parsed=parsed,
                    latency_ms=int((time.time() - started) * 1000),
                    estimated_cost_usd=est_cost,
                    prompt_version=prompt_version,
                    policy_version=policy_version,
                    model_name=self._model,
                )
            except Exception as exc:
                last_exc = exc
                msg = str(exc).lower()
                retriable = ("429" in msg) or ("rate" in msg) or ("timeout" in msg) or ("503" in msg)
                if attempt >= self._max_retries or not retriable:
                    break
                self._sleep_backoff(attempt)
        raise RuntimeError(f"vertex_request_failed:{last_exc}")

