---
description: Review replay and backtest validity for the trade project, focusing on leakage, event-time correctness, fill realism, train/test boundaries, and trustworthiness of offline results.
---

1. Act as **@trade-lead**. Restate the evaluation goal, strategy/signal scope, datasets involved, and what decision depends on the result.
2. Load **trade-project-core**.
3. Load **trade-backtest-validity**.
4. Load **trade-ml-replay-gating**.
5. If detector or signal logic is touched, load **trade-python-signal-engine**.
6. If execution assumptions matter, load **trade-execution-risk**.
7. Act as **@backtest-validity-reviewer** and produce a validity checklist, leakage review, and trust assessment for the offline result.
8. Act as **@ml-replay-engineer** for replay determinism, baseline diffing, and dataset-contract review.
9. Act as **@execution-risk-analyst** if fill assumptions or execution costs may affect the conclusion.
10. Act as **@quality-gatekeeper** and define acceptance criteria before using the offline result for rollout or strategy decisions.
11. Return one merged answer:
   - Goal
   - Facts
   - Assumptions
   - Risks
   - Validity findings
   - Required tests / evidence
   - Metrics / thresholds
   - Decision gates / rollout notes
