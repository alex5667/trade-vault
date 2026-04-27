from __future__ import annotations

from dataclasses import dataclass, asdict
from statistics import median
from typing import Any, Dict, Iterable, List, Optional


def _f(x: Any, d: float = 0.0) -> float:
    try:
        v = float(x)
        return v
    except Exception:
        return d


def _i(x: Any, d: int = 0) -> int:
    try:
        return int(float(x))
    except Exception:
        return d


TERMINAL_SUCCESS = {"ROLLBACK_SUCCESS"}
TERMINAL_FAIL = {"ROLLBACK_FAILED", "MANUAL_REVIEW"}


@dataclass
class RollbackSLOSummary:
    total: int
    success: int
    failed: int
    success_rate: float
    mttr_sec_p50: float
    mttr_sec_p95: float
    breach_n: int

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


def _percentile_sorted(values: List[float], p: float) -> float:
    if not values:
        return 0.0
    if len(values) == 1:
        return float(values[0])
    idx = max(0, min(len(values) - 1, round((len(values) - 1) * p)))
    return float(values[idx])


def summarize_rollback_slo(
    rows: Iterable[Dict[str, Any]],
    *,
    mttr_slo_sec: float = 900.0,
) -> RollbackSLOSummary:
    total = 0
    success = 0
    failed = 0
    mttr_values: List[float] = []
    breach_n = 0

    for row in rows:
        total += 1
        state = str(row.get("final_state", "") or "")
        req_ts = _i(row.get("requested_ts_ms"), 0)
        done_ts = _i(row.get("terminal_ts_ms"), 0)
        if state in TERMINAL_SUCCESS:
            success += 1
        elif state in TERMINAL_FAIL:
            failed += 1
        if req_ts > 0 and done_ts >= req_ts and state in TERMINAL_SUCCESS | TERMINAL_FAIL:
            mttr_sec = (done_ts - req_ts) / 1000.0
            mttr_values.append(mttr_sec)
            if mttr_sec > float(mttr_slo_sec):
                breach_n += 1

    mttr_values.sort()
    return RollbackSLOSummary(
        total=total,
        success=success,
        failed=failed,
        success_rate=(success / total) if total else 0.0,
        mttr_sec_p50=_percentile_sorted(mttr_values, 0.50),
        mttr_sec_p95=_percentile_sorted(mttr_values, 0.95),
        breach_n=breach_n,
    )


def build_slo_reason_codes(summary: RollbackSLOSummary, *, success_rate_floor: float = 0.90, mttr_p95_ceiling_sec: float = 900.0) -> List[str]:
    out: List[str] = []
    if summary.total == 0:
        out.append("NO_ROLLBACK_DATA")
        return out
    if summary.success_rate < float(success_rate_floor):
        out.append("ROLLBACK_SUCCESS_RATE_LOW")
    if summary.mttr_sec_p95 > float(mttr_p95_ceiling_sec):
        out.append("ROLLBACK_MTTR_P95_HIGH")
    if summary.breach_n > 0:
        out.append("ROLLBACK_MTTR_SLO_BREACH")
    return out
