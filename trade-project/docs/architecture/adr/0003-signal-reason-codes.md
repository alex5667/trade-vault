# ADR-0003: Signal Reason Codes

## Status
Accepted

## Context
When the system generates a signal or blocks a trade (via a gate or policy), the end user, the SRE, and the ML pipeline need to know exactly *why*. Boolean flags (`passed=True/False`) are insufficient for debugging production issues or training ML models. If a trade is blocked, we need to distinguish whether it was due to spread risk, an invalid timeframe, missing ML features, or an explicit ML rejection. We need a standardized classification system for all pipeline decisions.

## Decision
We will enforce **Signal Reason Codes** (e.g., `PASS_OK`, `ERR_SPREAD_TOO_HIGH`, `REJ_CONFIDENCE_LOW`, `ERR_MISSING_DATA`) for every gate, detector, and execution block in the pipeline. All signals and decision snapshots must include a `reason_code` string in their DTOs.

## Alternatives considered
1. **Free-text error messages**: Rejected. Hard to aggregate in TimescaleDB or Grafana, prone to typos, and impossible to use categorically in ML models.
2. **Numeric status codes (like HTTP 400, 500)**: Rejected. Less readable in logs and requires maintaining a separate lookup table.
3. **Boolean flags**: Rejected. Fails to capture the rich context needed for root cause analysis (RCA).

## Consequences
Positive:
- **Observability**: We can build Grafana dashboards grouping rejected signals by `reason_code` to instantly see if a new gate is too aggressive.
- **ML Feedback Loop**: Reason codes serve as structured labels/metadata for the ML pipeline to understand false positives.
- **Clear RCA**: Faster incident resolution when debugging why a specific order was quarantined.

Negative:
- **Contract Strictness**: Requires maintaining a registry of valid codes (e.g., via Enums in Python/Go) and coordinating updates across services.

## Risks
- **Code Proliferation**: Developers might create too many specific codes, making aggregation difficult. Mitigation: Use standard prefixes (`ERR_`, `REJ_`, `PASS_`, `WARN_`).
- **Drift**: NestJS API might reject unknown codes from the Python worker. Mitigation: Use shared contract schemas.

## Observability
Metrics:
- `signal_generated_total{type="volatility", reason_code="..."}`: Counter grouped by reason.
- `signal_quarantine_total{reason_code="..."}`: Monitor why trades are entering the quarantine ledger.

Logs:
- Include `reason_code` as a structured field in all signal log outputs.

## Rollout
- Define canonical `SignalReason` Enum in the shared Python `contracts/` package.
- Update `SmtCoherenceGate`, `ConfidenceGate`, and `ExecutionGate` to return specific reason codes instead of simple booleans.
- Update `decision_snapshot` Postgres schema to include `reason_code`.

## Rollback
- Revert logic changes if a new code breaks the downstream NestJS serialization.
