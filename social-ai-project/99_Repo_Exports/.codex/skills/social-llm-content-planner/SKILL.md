---
name: social-llm-content-planner
description: Use this skill for the LLM content-planning layer in the social-ai project: trend_analyst/content_brief/script/youtube_title/thumbnail/caption/policy_critic/commerce_fit agents, structured JSON output contracts, prompt/version pinning, Ollama/vLLM/llama.cpp serving, schema validation, retries, golden tests, cost control. Relevant for prompts about LLM agents, content brief, hook generation, JSON schema output, reason codes, hallucination control.
---

# Social LLM Content Planner

## Goal
Turn trends and features into **structured, validated content proposals** — never direct publish actions.

## Hard rule
The LLM does NOT decide "publish or not". It emits structured proposals. Decisions flow: proposal → schema validation → policy critic → scoring → publish gate → human review → governor.

## Agent roles
`trend_analyst_agent, content_brief_agent, script_writer_agent, hook_generator_agent, youtube_title_agent, thumbnail_agent, caption_agent, policy_critic_agent, commerce_fit_agent, platform_optimizer_agent, experiment_explainer_agent`.

## Canonical output envelope (all agents return JSON)
```json
{
  "hook_variants": [],
  "script": "",
  "shot_list": [],
  "caption": "",
  "title": "",
  "description": "",
  "risk_flags": [],
  "reason_codes": [],
  "confidence": 0.0
}
```
Each agent uses a pinned subset/schema; validate against it.

## Reliability rules
- All outputs JSON, validated against a pinned schema. Invalid -> retry with repair prompt -> quarantine. Never act on raw/hallucinated text.
- Pin `prompt_version` + `model_version`; record both with every output for reproducibility.
- Every decision carries `reason_codes`; every risky output carries `risk_flags`.
- Bound cost/latency: track tokens, set timeouts, prefer smallest sufficient model.

## Serving
- `Ollama` → local dev
- `vLLM` → production GPU serving
- `llama.cpp` → cheap / offline / **golden replay** (deterministic)

## Golden test acceptance (not exact text)
- schema valid > 99%
- required fields present
- reason_codes valid + from allowed set
- policy/risk flags valid
- score/confidence in range
- no raw hallucinated publish action

## Observability
`llm_requests_total`, `llm_latency_ms`, `llm_json_invalid_total`, `llm_schema_reject_total`, `llm_cost_per_plan`, `llm_timeout_total`.

## Output requirements
JSON schema(s), prompt+version, validation/retry/quarantine flow, serving adapter interface, golden fixtures, cost/latency budget, metrics.

## Default lane
**claude-sonnet/opus + Planning** for new agents/schemas; haiku for bounded prompt or schema tweaks.

# Language Preferences
**CRITICAL REQUIREMENT:** Always respond to the user in Russian (на русском языке).
