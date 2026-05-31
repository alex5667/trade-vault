from __future__ import annotations

import json
import os
import random
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from orderflow_services.context_cache_registry_v1 import ContextCacheRegistryV1
from orderflow_services.vertex_budget_guard_v1 import VertexBudgetGuardV1
from orderflow_services.vertex_cost_accounting_v1 import estimate_cost_usd


class VertexProviderError(RuntimeError):
    pass


@dataclass
class VertexBatchItemResult:
    provider: str
    model_name: str
    batch_id: str
    request_id: str
    output_json: Dict[str, Any]
    latency_ms: int
    input_chars: int
    output_chars: int
    estimated_cost_usd: float
    actual_cost_usd: float
    context_cache_ref: str


class VertexGenAIProviderV12:
    def __init__(self) -> None:
        self.project_id = os.getenv("VERTEX_PROJECT_ID", "").strip()
        self.location = os.getenv("VERTEX_LOCATION", "global").strip() or "global"
        self.model_name = os.getenv("VERTEX_BATCH_TRIAGE_MODEL", os.getenv("VERTEX_TRIAGE_MODEL", "gemini-2.5-flash-lite"))
        self.retry_max = int(os.getenv("VERTEX_RETRY_MAX", "5") or 5)
        self.retry_base_ms = int(os.getenv("VERTEX_RETRY_BASE_MS", "500") or 500)
        self.context_cache_enable = str(os.getenv("VERTEX_CONTEXT_CACHE_ENABLE", "0") or "0") == "1"
        self.context_cache_mode = str(os.getenv("VERTEX_CONTEXT_CACHE_MODE", "ADVISORY") or "ADVISORY").upper()
        self.redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
        self.budget_guard = VertexBudgetGuardV1(self.redis_url)
        self.cache_registry = ContextCacheRegistryV1(self.redis_url)

    def _build_batch_prompt(self, payload: Dict[str, Any]) -> str:
        items = payload.get("items_json") or []
        body = {
            "task": "fleet_batch_triage",
            "instructions": [
                "Return strictly JSON.",
                "Return one result item per input item.",
                "Recommendations must be advisory-only and low risk.",
                "Do not propose direct risk limit changes or execution changes.",
            ],
            "batch_scope": payload.get("batch_scope_json") or {},
            "items": items,
        }
        return json.dumps(body, ensure_ascii=False, sort_keys=True)

    def _parse_batch_response(self, text: str, fallback_payload: Dict[str, Any]) -> List[Dict[str, Any]]:
        try:
            obj = json.loads(text)
            if isinstance(obj, dict) and isinstance(obj.get("items"), list):
                return obj["items"]
            if isinstance(obj, list):
                return obj
        except Exception:
            pass
        # deterministic fallback: emit one blocked/inspection result per item
        items = fallback_payload.get("items_json") or []
        out: List[Dict[str, Any]] = []
        for item in items:
            out.append({
                "analysis_run_id": f"fallback_{item.get('model_id', 'unknown')}",
                "status": "fallback",
                "summary": "vertex_batch_parse_failed",
                "findings": [],
                "recommendations": [
                    {
                        "action": "open_incident",
                        "target": str(item.get("model_id") or "unknown"),
                        "risk": "low",
                        "reason_code": "VERTEX_BATCH_PARSE_FAILED",
                    }
                ],
            })
        return out

    def _call_vertex(self, prompt: str, *, context_cache_ref: str = "") -> str:
        try:
            from google import genai  # type: ignore
        except Exception as exc:  # pragma: no cover
            raise VertexProviderError(f"google-genai unavailable: {exc}")

        if not self.project_id:
            raise VertexProviderError("VERTEX_PROJECT_ID is not set")

        client = genai.Client(vertexai=True, project=self.project_id, location=self.location)
        config = {
            "response_mime_type": "application/json",
            "temperature": 0.0,
            "top_p": 0.1,
        }
        # Keep context-cache handling advisory by default; avoid relying on volatile SDK fields.
        if self.context_cache_enable and context_cache_ref and self.context_cache_mode == "ATTACH":
            config["cached_content"] = context_cache_ref
        resp = client.models.generate_content(model=self.model_name, contents=prompt, config=config)
        text = getattr(resp, "text", None)
        if not text:
            raise VertexProviderError("empty_vertex_response")
        return str(text)

    def analyze_batch(self, payload: Dict[str, Any]) -> List[VertexBatchItemResult]:
        batch_id = str(payload.get("batch_id") or "")
        prompt = self._build_batch_prompt(payload)
        prompt_version = str(payload.get("prompt_version") or "unknown")
        policy_version = str(payload.get("policy_version") or "unknown")
        compact_hash = str(payload.get("batch_compact_hash") or batch_id)
        context_cache_ref = ""
        if self.context_cache_enable:
            entry = self.cache_registry.lookup(compact_hash)
            if entry and entry.eligible:
                context_cache_ref = entry.cache_ref
        input_chars = len(prompt)
        estimated_output_chars = int(os.getenv("VERTEX_BATCH_TRIAGE_EST_OUTPUT_CHARS", "6000") or 6000)
        est_cost = estimate_cost_usd(model_name=self.model_name, input_chars=input_chars, output_chars=estimated_output_chars)
        budget_dec = self.budget_guard.check_and_reserve(provider="vertex", model=self.model_name, estimated_cost_usd=est_cost, ts=int((payload.get("ts_ms") or 0) / 1000 or time.time()))
        if not budget_dec.allowed:
            raise VertexProviderError(f"budget_guard:{budget_dec.reason}")

        started = time.perf_counter()
        last_exc: Optional[Exception] = None
        text = ""
        for attempt in range(self.retry_max):
            try:
                text = self._call_vertex(prompt, context_cache_ref=context_cache_ref)
                break
            except Exception as exc:  # pragma: no cover
                last_exc = exc
                if attempt + 1 >= self.retry_max:
                    raise VertexProviderError(str(exc))
                sleep_ms = (self.retry_base_ms * (2 ** attempt)) + random.randint(0, 200)
                time.sleep(max(0.05, sleep_ms / 1000.0))
        latency_ms = int((time.perf_counter() - started) * 1000)
        parsed = self._parse_batch_response(text, payload)
        output_chars = len(text)
        actual_cost = estimate_cost_usd(model_name=self.model_name, input_chars=input_chars, output_chars=output_chars)
        items = payload.get("items_json") or []
        out: List[VertexBatchItemResult] = []
        for idx, item in enumerate(items):
            result_json = parsed[idx] if idx < len(parsed) else {
                "analysis_run_id": f"missing_{item.get('model_id', idx)}",
                "status": "fallback",
                "summary": "missing_batch_item_result",
                "findings": [],
                "recommendations": [],
            }
            out.append(VertexBatchItemResult(
                provider="vertex",
                model_name=self.model_name,
                batch_id=batch_id,
                request_id=str(item.get("model_id") or f"item_{idx}"),
                output_json=result_json,
                latency_ms=latency_ms,
                input_chars=input_chars,
                output_chars=max(0, int(output_chars / max(1, len(items)))),
                estimated_cost_usd=max(0.0, est_cost / max(1, len(items))),
                actual_cost_usd=max(0.0, actual_cost / max(1, len(items))),
                context_cache_ref=context_cache_ref,
            ))
        return out
