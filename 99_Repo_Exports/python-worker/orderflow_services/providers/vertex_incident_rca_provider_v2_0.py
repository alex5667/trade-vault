from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Any


@dataclass
class IncidentRCAResponse:
    status: str
    provider: str
    model_name: str
    latency_ms: int
    output_json: dict[str, Any]
    estimated_cost_usd: float


class VertexIncidentRCAProviderV20:
    def __init__(self) -> None:
        self.project_id = os.getenv("VERTEX_PROJECT_ID", "")
        self.location = os.getenv("VERTEX_LOCATION", "global")
        self.model_name = os.getenv("VERTEX_RCA_MODEL", "gemini-2.5-flash")
        self.timeout_ms = int(os.getenv("VERTEX_RCA_TIMEOUT_MS", "45000"))
        self.dry_run = int(os.getenv("VERTEX_RCA_DRY_RUN", "0")) == 1

    def _build_prompt(self, payload: dict[str, Any]) -> str:
        instruction = {
            "role": "system",
            "content": (
                "You are an incident RCA assistant for ML control plane. "
                "Return JSON only. "
                "Do not recommend actions outside the allowed_actions list. "
                "Never suggest auto-apply. "
                "Be concise and evidence-based."
            ),
        }
        user = {
            "role": "user",
            "content": json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True),
        }
        return json.dumps({"messages": [instruction, user]}, ensure_ascii=False, separators=(",", ":"))

    def _dry_run_response(self, payload: dict[str, Any]) -> IncidentRCAResponse:
        rid = payload.get("recommendation_id", "unknown")
        model_id = payload.get("model_id", "unknown")
        reasons = payload.get("primary_reason_codes", []) or []
        severity = payload.get("severity", "warning")
        output = {
            "schema_version": 1,
            "analysis_run_id": f"dryrun:{rid}",
            "status": "ok",
            "summary": f"Dry-run RCA for {model_id}",
            "findings": [
                {"kind": "incident_bundle_rca", "target": model_id, "confidence": 0.5, "evidence": reasons[:5]}
            ],
            "recommendations": [
                {
                    "action": "open_incident" if severity == "critical" else "draft_postmortem",
                    "target": model_id,
                    "risk": "low",
                    "reason_code": reasons[0] if reasons else "UNKNOWN",
                }
            ],
        }
        return IncidentRCAResponse(status="ok", provider="vertex", model_name=self.model_name, latency_ms=0, output_json=output, estimated_cost_usd=0.0)

    def analyze(self, payload: dict[str, Any]) -> IncidentRCAResponse:
        if self.dry_run:
            return self._dry_run_response(payload)

        import time

        from google import genai  # type: ignore
        from google.genai import types  # type: ignore

        client = genai.Client(vertexai=True, project=self.project_id, location=self.location)
        prompt = self._build_prompt(payload)
        t0 = time.perf_counter()
        resp = client.models.generate_content(
            model=self.model_name,
            contents=prompt,
            config=types.GenerateContentConfig(response_mime_type="application/json", temperature=0.1, max_output_tokens=2048),
        )
        latency_ms = int((time.perf_counter() - t0) * 1000)
        text = getattr(resp, "text", "") or "{}"
        output = json.loads(text)
        return IncidentRCAResponse(status="ok", provider="vertex", model_name=self.model_name, latency_ms=latency_ms, output_json=output, estimated_cost_usd=0.0)
