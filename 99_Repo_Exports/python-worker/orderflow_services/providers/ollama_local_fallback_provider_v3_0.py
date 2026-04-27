from __future__ import annotations

import json
import os
import urllib.error
import urllib.request
from typing import Any, Dict, List, Tuple


OLLAMA_BASE_URL = os.getenv("LOCAL_FALLBACK_OLLAMA_BASE_URL", "http://host.docker.internal:11434")
OLLAMA_CHAT_URL = f"{OLLAMA_BASE_URL.rstrip('/')}/api/chat"
REQUEST_TIMEOUT_SEC = int(os.getenv("LOCAL_FALLBACK_OLLAMA_TIMEOUT_SEC", "120"))


def _headers() -> Dict[str, str]:
    return {"Content-Type": "application/json"}


def _post_json(url: str, payload: Dict[str, Any], timeout_sec: int) -> Dict[str, Any]:
    body = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=body, headers=_headers(), method="POST")
    try:
        with urllib.request.urlopen(req, timeout=timeout_sec) as resp:
            raw = resp.read().decode("utf-8")
    except urllib.error.HTTPError as exc:  # pragma: no cover
        raise RuntimeError(f"ollama_http_error:{exc.code}") from exc
    except urllib.error.URLError as exc:  # pragma: no cover
        raise RuntimeError("ollama_unreachable") from exc
    return json.loads(raw)


def _system_prompt(task_type: str) -> str:
    base = (
        "You are a local fallback LLM in a production trading analytics stack. "
        "You are not the primary reasoning plane. "
        "You must stay compact, deterministic, and output strict JSON only."
    )
    if task_type == "offline_debug":
        return base + " Focus on bounded debugging hypotheses, replayability, contracts, and next checks."
    if task_type == "local_report":
        return base + " Focus on concise local operational reports for engineers."
    if task_type == "vertex_unavailable_fallback":
        return base + " Provide emergency fallback summarization while Vertex is unavailable."
    return base + " Provide emergency summarization only."


def _default_schema(task_type: str) -> Dict[str, Any]:
    if task_type == "offline_debug":
        return {
            "type": "object",
            "properties": {
                "schema_version": {"type": "integer"},
                "task_type": {"type": "string"},
                "summary": {"type": "string"},
                "hypotheses": {"type": "array", "items": {"type": "string"}},
                "checks": {"type": "array", "items": {"type": "string"}},
                "reason_codes": {"type": "array", "items": {"type": "string"}},
            },
            "required": ["schema_version", "task_type", "summary", "hypotheses", "checks", "reason_codes"],
        }
    if task_type == "local_report":
        return {
            "type": "object",
            "properties": {
                "schema_version": {"type": "integer"},
                "task_type": {"type": "string"},
                "title": {"type": "string"},
                "summary": {"type": "string"},
                "sections": {"type": "array", "items": {"type": "string"}},
                "reason_codes": {"type": "array", "items": {"type": "string"}},
            },
            "required": ["schema_version", "task_type", "title", "summary", "sections", "reason_codes"],
        }
    return {
        "type": "object",
        "properties": {
            "schema_version": {"type": "integer"},
            "task_type": {"type": "string"},
            "summary": {"type": "string"},
            "top_findings": {"type": "array", "items": {"type": "string"}},
            "actions": {"type": "array", "items": {"type": "string"}},
            "reason_codes": {"type": "array", "items": {"type": "string"}},
        },
        "required": ["schema_version", "task_type", "summary", "top_findings", "actions", "reason_codes"],
    }


def build_ollama_payload(
    *,
    model: str,
    task_type: str,
    user_prompt: str,
    schema: Dict[str, Any],
    keep_alive: str,
) -> Dict[str, Any]:
    return {
        "model": model,
        "messages": [
            {"role": "system", "content": _system_prompt(task_type)},
            {"role": "user", "content": user_prompt},
        ],
        "stream": False,
        "format": schema or _default_schema(task_type),
        "options": {"temperature": 0},
        "keep_alive": keep_alive,
    }


class OllamaLocalFallbackProviderV30:
    def __init__(self) -> None:
        self._base_model = os.getenv("LOCAL_FALLBACK_OLLAMA_MODEL", "")
        self._code_model = os.getenv("LOCAL_FALLBACK_OLLAMA_CODE_MODEL", "")
        self._keep_alive = os.getenv("LOCAL_FALLBACK_OLLAMA_KEEP_ALIVE", "15m")

    def is_available(self) -> bool:
        return bool(self._base_model)

    def choose_model(self, task_type: str) -> str:
        if task_type == "offline_debug" and self._code_model:
            return self._code_model
        return self._base_model

    def analyze(
        self,
        *,
        task_type: str,
        prompt: str,
        schema: Dict[str, Any] | None = None,
    ) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        model = self.choose_model(task_type)
        if not model:
            raise RuntimeError("local_fallback_model_not_configured")
        payload = build_ollama_payload(
            model=model,
            task_type=task_type,
            user_prompt=prompt,
            schema=schema or _default_schema(task_type),
            keep_alive=self._keep_alive,
        )
        response = _post_json(OLLAMA_CHAT_URL, payload, REQUEST_TIMEOUT_SEC)
        content = (((response or {}).get("message") or {}).get("content") or "").strip()
        if not content:
            raise RuntimeError("ollama_empty_response")
        parsed = json.loads(content)
        meta = {
            "provider": "ollama_local",
            "model_name": model,
            "total_duration_ns": response.get("total_duration", 0),
            "load_duration_ns": response.get("load_duration", 0),
            "prompt_eval_count": response.get("prompt_eval_count", 0),
            "eval_count": response.get("eval_count", 0),
        }
        return parsed, meta
