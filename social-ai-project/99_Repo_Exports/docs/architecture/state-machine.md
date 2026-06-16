# Content State Machine (plan §12.3)

State lives on `publish_jobs.state` (and content lineage entities).

## Happy path
```
DISCOVERED → ENRICHED → SCORED → BRIEF_GENERATED → SCRIPT_GENERATED
→ ASSET_RENDERED → POLICY_CHECKED → READY_FOR_REVIEW → APPROVED
→ SCHEDULED → PUBLISHING → PUBLISHED → OUTCOME_PENDING → OUTCOME_READY → LEARNED
```

## Error / terminal states
```
QUARANTINED · REJECTED · FAILED · RETRY_WAIT · DLQ
```

## Rules
- Transitions are explicit, audited, and emit reason codes.
- `APPROVED` requires a `review_decisions` row; `PUBLISHING` requires an allowing `policy_decisions` row.
- Default publish visibility is draft/private/unlisted until `AUTOPUBLISH_ENABLED=true` and SLOs are stable.
- Every state change is replay-reproducible (skill: `social-replay`).
- Outcome windows (1h/6h/24h/7d) feed `OUTCOME_READY → LEARNED` and the governors.
