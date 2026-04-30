"""P84: DLQ auto-triage fix-hints registry for OF-Gate.

This module provides stable, low-cardinality hint codes and recommended
actions for common DLQ causes.

Inputs:
  - dq_code: produced by contract validator (e.g. 'dq_missing_required')
  - err: raw error string captured into DLQ (archiver: stream_archiver.dlq)

Outputs:
  - hint_code: stable code
  - severity: 'info'|'warn'|'page'
  - title: short
  - details: short
  - actions: list[str]
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional


@dataclass(frozen=True)
class FixHint:
    hint_code: str
    severity: str
    title: str
    details: str
    actions: List[str]


_HINTS: Dict[str, FixHint] = {
    # Contract / schema
    "dq_schema_missing": FixHint(
        hint_code="schema_missing"
        severity="warn"
        title="Missing schema markers"
        details="Records lack schema_name/schema_version; consumers may mis-parse or drop rows."
        actions=[
            "Ensure producers call enrich_schema_fields() before XADD"
            "Rollout additive fields first; then enforce REQUIRED_FIELDS"
        ]
    )
    "dq_schema_version_bad": FixHint(
        hint_code="schema_version_bad"
        severity="warn"
        title="Bad schema_version"
        details="schema_version is missing/invalid; indicates mixed producers or broken serializer."
        actions=[
            "Confirm schema_version emitted as int"
            "Check for old containers still running"
        ]
    )
    "dq_missing_required": FixHint(
        hint_code="missing_required"
        severity="page"
        title="Missing required fields"
        details="Validator rejected row due to missing required fields (ts_ms/symbol/ok/ok_soft/missing_legs/scenario_v4/reason_code)."
        actions=[
            "Verify producer contract: required fields present before XADD"
            "If reading DecisionRecord, duplicate ok/ok_soft/scenario_v4 to top-level"
            "If old data: run DLQ fixed-replay with safe fixes (schema/ts_ms/reason_code)"
        ]
    )

    # Time
    "dq_ts_bad_range": FixHint(
        hint_code="ts_bad_range"
        severity="page"
        title="ts_ms out of range"
        details="ts_ms looks like seconds/us/ns or garbage; breaks time bucketing and no-data detection."
        actions=[
            "Ensure normalize_epoch_ms() applied at emit time"
            "Check upstream tick_ts units (sec vs ms)"
            "Use DLQ fixed-replay to normalize ts_ms from stream_id"
        ]
    )

    # Invariants
    "dq_ok_invariant": FixHint(
        hint_code="ok_invariant"
        severity="warn"
        title="ok/ok_soft invariant violated"
        details="Expected ok,ok_soft ∈ {0,1} and ok_soft => ok==0. Broken scoring serializer or mapping."
        actions=[
            "Verify rule.ok and rule.ok_soft mapping"
            "Ensure ok_soft implies not ok_hard"
            "Optionally sanitize in replay: if ok_soft=1 and ok=1 -> set ok=0"
        ]
    )

    # JSON
    "dq_missing_legs_bad": FixHint(
        hint_code="missing_legs_bad"
        severity="warn"
        title="missing_legs invalid JSON"
        details="missing_legs should be a JSON object/list (stringified is OK if valid JSON)."
        actions=[
            "Emit missing_legs as JSON (dict/list) or valid JSON string"
            "Fix old rows by setting missing_legs='[]' only if truly unknown"
        ]
    )

    # Labels
    "dq_scenario_bad": FixHint(
        hint_code="scenario_bad"
        severity="info"
        title="scenario_v4 not normalized"
        details="High-cardinality or inconsistent scenario_v4 breaks grouping."
        actions=[
            "Use allowlist/normalization to low-card values"
        ]
    )
}


def _err_prefix(err: Optional[str]) -> str:
    if not err:
        return ""
    s = str(err).strip()
    if not s:
        return ""
    return s.split(" ", 1)[0][:64]


def hint_for(dq_code: Optional[str], err: Optional[str] = None) -> FixHint:
    """Return best-effort hint based on dq_code and err prefix."""
    if dq_code and dq_code in _HINTS:
        return _HINTS[dq_code]

    p = _err_prefix(err)
    # Fallback mapping by common prefixes
    if p in ("JSONDecodeError", "ValueError"):
        return FixHint(
            hint_code="payload_parse"
            severity="warn"
            title="Payload parse error"
            details="DLQ entry payload could not be parsed as JSON."
            actions=["Check producer JSON serialization", "Inspect sample DLQ payload"]
        )
    if "timeout" in p.lower():
        return FixHint(
            hint_code="timeout"
            severity="warn"
            title="Timeout"
            details="Archiver timed out or dependency timed out while processing row."
            actions=["Check Redis/DB latency", "Reduce batch or increase timeouts"]
        )
    return FixHint(
        hint_code="unknown"
        severity="info"
        title="Unknown DLQ cause"
        details="No known hint for dq_code/err; inspect samples."
        actions=["Run dlq drilldown sample", "Check top err strings"]
    )


def known_dq_codes() -> List[str]:
    return sorted(_HINTS.keys())

