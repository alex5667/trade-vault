from __future__ import annotations

import json
import os
from typing import Any

try:  # pragma: no cover
    from google import genai  # type: ignore
    from google.genai import types  # type: ignore
except Exception:  # pragma: no cover
    genai = None
    types = None


def _schema() -> dict[str, Any]:
    return {
        "type": "object",
        "properties": {
            "schema_version": {"type": "integer"},
            "route_change_id": {"type": "string"},
            "status": {"type": "string"},
            "summary": {"type": "string"},
            "findings": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "kind": {"type": "string"},
                        "target": {"type": "string"},
                        "confidence": {"type": "number"},
                        "evidence": {"type": "array", "items": {"type": "string"}},
                    },
                    "required": ["kind", "target", "confidence", "evidence"],
                },
            },
            "recommendations": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "action": {"type": "string"},
                        "target": {"type": "string"},
                        "risk": {"type": "string"},
                        "reason_code": {"type": "string"},
                    },
                    "required": ["action", "target", "risk", "reason_code"],
                },
            },
        },
        "required": ["schema_version", "route_change_id", "status", "summary", "findings", "recommendations"],
    }


def _system_instruction() -> str:
    return (
        "You are analyzing a routing policy incident inside a quantitative trading control-plane. "
        "You must return strict JSON only. "
        "Do not discuss UI, business strategy, or unbounded changes. "
        "Recommendations must remain advisory-only and bounded to routing/provider/prompt/policy governance."
    )


class VertexRoutingIncidentRCAProviderV29:
    def __init__(self) -> None:
        self._project = os.getenv("VERTEX_PROJECT_ID", "")
        self._location = os.getenv("VERTEX_LOCATION", "global")
        self._model = os.getenv("VERTEX_ROUTING_INCIDENT_RCA_MODEL", "gemini-2.5-flash-lite")

    def is_available(self) -> bool:
        return bool(self._project and genai is not None and types is not None)

    def analyze(self, payload: dict[str, Any]) -> dict[str, Any]:
        if not self.is_available():  # pragma: no cover
            raise RuntimeError("vertex_genai_unavailable")
        client = genai.Client(vertexai=True, project=self._project, location=self._location)
        content = json.dumps(payload, ensure_ascii=False)
        response = client.models.generate_content(
            model=self._model,
            contents=content,
            config=types.GenerateContentConfig(
                temperature=0.2,
                response_mime_type="application/json",
                response_schema=_schema(),
                system_instruction=_system_instruction(),
            ),
        )
        text = getattr(response, "text", "") or ""
        if not text:
            raise RuntimeError("vertex_empty_response")
        parsed = json.loads(text)
        parsed["provider"] = "vertex"
        parsed["model_name"] = self._model
        return parsed
