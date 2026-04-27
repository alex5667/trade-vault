from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Any, Dict, Iterable, List

from orderflow_services.rollback_slo_analytics_v1 import summarize_rollback_slo, build_slo_reason_codes


def _i(x: Any, d: int = 0) -> int:
    try:
        return int(float(x))
    except Exception:
        return d


def _s(x: Any, d: str = "") -> str:
    try:
        return str(x)
    except Exception:
        return d


@dataclass
class EscalationSummary:
    severity: str
    summary: str
    reason_codes: List[str]
    failed_ids: List[str]

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


def build_auto_escalation_summary(
    rows: Iterable[Dict[str, Any]],
    *,
    mttr_slo_sec: float = 900.0,
    success_rate_floor: float = 0.90,
) -> EscalationSummary:
    materialized = list(rows)
    slo = summarize_rollback_slo(materialized, mttr_slo_sec=mttr_slo_sec)
    reason_codes = build_slo_reason_codes(slo, success_rate_floor=success_rate_floor, mttr_p95_ceiling_sec=mttr_slo_sec)
    failed_ids = [
        _s(r.get("recommendation_id"))
        for r in materialized
        if _s(r.get("final_state")) in {"ROLLBACK_FAILED", "MANUAL_REVIEW"}
    ]
    severity = "info"
    if any(rc in {"ROLLBACK_SUCCESS_RATE_LOW", "ROLLBACK_MTTR_P95_HIGH", "ROLLBACK_MTTR_SLO_BREACH"} for rc in reason_codes):
        severity = "warning"
    if len(failed_ids) >= 3:
        severity = "critical"
    summary = (
        f"rollback_slo total={slo.total} success={slo.success} failed={slo.failed} "
        f"success_rate={slo.success_rate:.3f} mttr_p95_sec={slo.mttr_sec_p95:.1f}"
    )
    if failed_ids:
        summary += f" failed_ids={','.join(failed_ids[:5])}"
    return EscalationSummary(severity=severity, summary=summary, reason_codes=reason_codes, failed_ids=failed_ids)
