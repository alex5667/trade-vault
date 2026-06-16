# Architecture Overview

social-ai-infra is the social-domain port of `scanner_infra`'s production decision framework. The reusable value is the **control loop**, not trading code:

```
collect → normalize → score → decide → shadow → measure → promote → rollback
```
becomes
```
collect trends → enrich media → generate hypothesis → review/publish → measure outcome → promote strategy
```

## Layers (plan §3.2)
| Layer | Tech | Purpose |
|---|---|---|
| Ingestion | Go | quota-aware collectors, webhooks |
| Event bus | Redis Streams | reliable events, consumer groups, replay |
| Analytics workers | Python | scoring, ML, ASR/OCR, LLM orchestration |
| API/control | NestJS | workflow, approvals, adapters, auth |
| UI | Next.js | dashboard, review, experiments |
| DB | Postgres + Timescale | events, outcomes, aggregates |
| Object storage | S3/MinIO | video, frames, thumbnails, transcripts |
| Vector DB | Qdrant | creative memory, RAG, reference search |
| LLM serving | vLLM / Ollama / llama.cpp | local LLM 7B–14B |
| Monitoring | Prometheus + Grafana + Alertmanager | SRE |
| ChatOps | Telegram | commands, approve/reject, alerts |

## Domain mapping (trade → social)
market data → social signals · signal → trend candidate · forward gate → publish gate · alpha governor → content/strategy governor · PnL/R-multiple → watch time / CTR / CVR / revenue / LTV · tick/book archive → raw event/media archive · feature registry → trend/content feature registry.

See also: [`event-streams.md`](./event-streams.md), [`state-machine.md`](./state-machine.md).
