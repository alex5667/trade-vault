# services/python-workers — Enrichment / Scoring / LLM / Governor workers

One package, many entrypoint modules selected by compose `command`. Each reads a Redis
Streams consumer group and writes downstream streams/DB.

Modules (add per phase): `enrich`, `trend_rank` (Phase 4), `content_plan` (Phase 5, LLM),
`policy_critic` (Phase 5), `outcome_attribution` (Phase 9), `replay`, `governors` (Phase 10).

```bash
pip install -e ".[dev]"
python -m trend_rank
pytest
```

Skills: `social-project-core`, `social-trend-scoring`, `social-llm-content-planner`,
`social-governor`, `social-data-quality-time`, `python-patterns`, `python-testing`.
