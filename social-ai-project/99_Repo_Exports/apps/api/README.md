# apps/api — NestJS Control Plane

Operator/control plane (plan §12). Skeleton with a health endpoint; modules land in Phase 6.

## Dev
```bash
npm install
npm run start:dev   # http://localhost:3000/health
```

## Modules to add (plan §12.1)
Auth · Accounts · Platforms · Trend · ContentBrief · Script · Asset · Review · Publish · Outcome · Experiment · Governor · Policy · Commerce · ChatOps · Replay · Observability.

## State machine (plan §12.3)
`DISCOVERED → ENRICHED → SCORED → BRIEF_GENERATED → SCRIPT_GENERATED → ASSET_RENDERED → POLICY_CHECKED → READY_FOR_REVIEW → APPROVED → SCHEDULED → PUBLISHING → PUBLISHED → OUTCOME_PENDING → OUTCOME_READY → LEARNED` (+ QUARANTINED/REJECTED/FAILED/RETRY_WAIT/DLQ).

Skills: `social-project-core`, `social-contract-check`, `social-publish-policy`, `nestjs-patterns`.
