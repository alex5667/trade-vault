from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional


class VertexProviderError(RuntimeError):
    pass


@dataclass
class VertexResult:
    provider: str
    model_name: str
    output_json: Dict[str, Any]
    latency_ms: int
    raw_text: str
    request_tokens: Optional[int] = None
    response_tokens: Optional[int] = None


SYSTEM_INSTRUCTION = """You analyze ML health for a trading system.
Output STRICT JSON only.
Allowed actions:
- require_shadow_retrain
- freeze_candidate
- unfreeze_candidate
- request_calibration_refresh
- propose_threshold_canary
- open_incident
- draft_postmortem
Never recommend direct risk-limit changes, execution cap changes, or auto-enforce.
"""


class VertexGenAIProviderV1:
    def __init__(self) -> None:
        self.project = os.getenv("VERTEX_PROJECT_ID", "")
        self.location = os.getenv("VERTEX_LOCATION", "global")
        self.model = os.getenv("VERTEX_TRIAGE_MODEL", "gemini-2.5-flash-lite")
        self.temperature = float(os.getenv("VERTEX_TEMPERATURE", "0.1"))
        self.max_output_tokens = int(os.getenv("VERTEX_MAX_OUTPUT_TOKENS", "4096"))
        self.timeout_ms = int(os.getenv("VERTEX_TIMEOUT_MS", "45000"))

    def _build_prompt(self, req: Dict[str, Any]) -> str:
        return json.dumps(
            {
                "task_type": req.get("task_type"),
                "scope": json.loads(req.get("scope_json", "{}")),
                "input_pack": json.loads(req.get("input_pack_json", "{}")),
                "reason_codes": json.loads(req.get("reason_codes_json", "[]")),
                "output_contract": {
                    "schema_version": 1,
                    "analysis_run_id": "string",
                    "status": "ok|needs_human|error",
                    "summary": "string",
                    "findings": [{"kind": "string", "target": "string", "confidence": 0.0, "evidence": ["string"]}],
                    "recommendations": [{"action": "string", "target": "string", "risk": "low|medium|high", "reason_code": "string"}],
                },
            },
            ensure_ascii=False,
        )

    def analyze(self, req: Dict[str, Any]) -> VertexResult:
        try:
            from google import genai
            from google.genai import types
        except Exception as exc:
            raise VertexProviderError("google-genai SDK is not installed") from exc

        if not self.project:
            raise VertexProviderError("VERTEX_PROJECT_ID is empty")

        client = genai.Client(
            vertexai=True,
            project=self.project,
            location=self.location,
        )

        prompt = self._build_prompt(req)
        t0 = time.perf_counter()
        response = client.models.generate_content(
            model=self.model,
            contents=prompt,
            config=types.GenerateContentConfig(
                system_instruction=SYSTEM_INSTRUCTION,
                temperature=self.temperature,
                max_output_tokens=self.max_output_tokens,
                response_mime_type="application/json",
            ),
        )
        latency_ms = int((time.perf_counter() - t0) * 1000)
        text = getattr(response, "text", "") or ""
        if not text:
            raise VertexProviderError("empty response text")

        try:
            output_json = json.loads(text)
        except Exception as exc:
            raise VertexProviderError("invalid JSON from model") from exc

        usage = getattr(response, "usage_metadata", None)
        req_tokens = getattr(usage, "prompt_token_count", None) if usage else None
        resp_tokens = getattr(usage, "candidates_token_count", None) if usage else None

        return VertexResult(
            provider="vertex",
            model_name=self.model,
            output_json=output_json,
            latency_ms=latency_ms,
            raw_text=text,
            request_tokens=req_tokens,
            response_tokens=resp_tokens,
        )
