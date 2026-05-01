#!/usr/bin/env python3
from __future__ import annotations
'''
DQ-aware rules used by nightly quality + ramp + guardrails.

Goal:
  - Provide a single source of truth for extracting DQ metrics from a quality report JSON.
  - Provide conservative "freeze latch" conditions when DQ coverage is missing or degraded.

Design:
  - Robust to report format changes (top-level keys vs nested metrics).
  - Thresholds are overridable via:
      1) cfg2/dynamic cfg dict (preferred)
      2) env vars (fallback)
      3) safe defaults (conservative)
'''


from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple
import math
import os


def _safe_float(x: Any) -> Optional[float]:
    try:
        v = float(x)
        if math.isnan(v) or math.isinf(v):
            return None
        return v
    except Exception:
        return None


def _safe_int(x: Any) -> Optional[int]:
    try:
        v = int(x)
        return v
    except Exception:
        fv = _safe_float(x)
        if fv is None:
            return None
        return int(fv)


def _get_nested(d: Dict[str, Any], path: str) -> Any:
    '''
    path: 'metrics.dq_present_n' etc. Returns None if missing.
    '''
    cur: Any = d
    for part in path.split('.'):
        if not isinstance(cur, dict):
            return None
        cur = cur.get(part)
        if cur is None:
            return None
    return cur


def _get_first(report: Dict[str, Any], keys: Tuple[str, ...]) -> Any:
    for k in keys:
        if '.' in k:
            v = _get_nested(report, k)
        else:
            v = report.get(k)
        if v is not None:
            return v
    return None


@dataclass(frozen=True)
class DQMetrics:
    dq_present_n: int
    dq_health_mean: Optional[float]
    corr_meta_p_dq_health: Optional[float]
    worst_dq_bucket_pr_auc: Optional[float]
    worst_dq_bucket_ece: Optional[float]
    worst_dq_bucket: Optional[str]


@dataclass(frozen=True)
class DQThresholds:
    dq_present_min: int
    dq_health_mean_min: float
    corr_min: float
    worst_pr_auc_min: float
    worst_ece_max: float


def extract_dq_metrics(report: Dict[str, Any]) -> DQMetrics:
    '''
    Extracts DQ-related signals from report JSON.
    Supports multiple report formats (top-level vs nested metrics).
    '''
    dq_present_n = _safe_int(
        _get_first(report, ('dq_present_n', 'metrics.dq_present_n', 'counts.dq_present_n', 'dq.present_n'))
    ) or 0

    dq_health_mean = _safe_float(
        _get_first(report, ('dq_health_mean', 'metrics.dq_health_mean', 'dq.health_mean'))
    )

    corr = _safe_float(
        _get_first(report, ('corr_meta_p_dq_health', 'metrics.corr_meta_p_dq_health', 'dq.corr_meta_p_health'))
    )

    worst_pr_auc = _safe_float(
        _get_first(report, ('worst_dq_bucket_pr_auc', 'metrics.worst_dq_bucket_pr_auc', 'worst.dq_bucket_pr_auc'))
    )
    worst_ece = _safe_float(
        _get_first(report, ('worst_dq_bucket_ece', 'metrics.worst_dq_bucket_ece', 'worst.dq_bucket_ece'))
    )
    worst_bucket = _get_first(report, ('worst_dq_bucket', 'metrics.worst_dq_bucket', 'worst.dq_bucket'))
    if isinstance(worst_bucket, (int, float)):
        worst_bucket = str(worst_bucket)
    if not isinstance(worst_bucket, str):
        worst_bucket = None

    return DQMetrics(
        dq_present_n=dq_present_n,
        dq_health_mean=dq_health_mean,
        corr_meta_p_dq_health=corr,
        worst_dq_bucket_pr_auc=worst_pr_auc,
        worst_dq_bucket_ece=worst_ece,
        worst_dq_bucket=worst_bucket,
    )


def thresholds_from_cfg(cfg2: Optional[Dict[str, Any]], schema_name: Optional[str] = None) -> DQThresholds:
    '''
    cfg2: merged config (static + dynamic). If missing, env vars are used.
    Per-schema overrides support keys like ramp_dq_present_min__meta_feat_v5.
    '''
    cfg2 = cfg2 or {}
    schema = schema_name or ''

    def pick_int(key: str, default: int) -> int:
        v = cfg2.get(key)
        if schema:
            v = cfg2.get(f'{key}__{schema}', v)
        env_key = key.upper()
        if v is None:
            v = os.getenv(env_key)
        if schema and v is None:
            v = os.getenv(f'{env_key}__{schema}')
        return _safe_int(v) or default

    def pick_f(key: str, default: float) -> float:
        v = cfg2.get(key)
        if schema:
            v = cfg2.get(f'{key}__{schema}', v)
        env_key = key.upper()
        if v is None:
            v = os.getenv(env_key)
        if schema and v is None:
            v = os.getenv(f'{env_key}__{schema}')
        fv = _safe_float(v)
        return fv if fv is not None else default

    return DQThresholds(
        dq_present_min=pick_int('ramp_dq_present_min', 500),
        dq_health_mean_min=pick_f('ramp_dq_health_mean_min', 0.75),
        corr_min=pick_f('ramp_dq_corr_min', -0.10),
        worst_pr_auc_min=pick_f('ramp_worst_dq_pr_auc_min', 0.52),
        worst_ece_max=pick_f('ramp_worst_dq_ece_max', 0.12),
    )


def dq_freeze_decision(
    report: Dict[str, Any],
    cfg2: Optional[Dict[str, Any]] = None,
    schema_name: Optional[str] = None,
) -> Tuple[bool, str, Dict[str, Any]]:
    '''
    Returns:
      freeze(bool), reason(str), details(dict)
    '''
    m = extract_dq_metrics(report)
    t = thresholds_from_cfg(cfg2, schema_name=schema_name)

    details: Dict[str, Any] = {
        'dq_present_n': m.dq_present_n,
        'dq_health_mean': m.dq_health_mean,
        'corr_meta_p_dq_health': m.corr_meta_p_dq_health,
        'worst_dq_bucket_pr_auc': m.worst_dq_bucket_pr_auc,
        'worst_dq_bucket_ece': m.worst_dq_bucket_ece,
        'worst_dq_bucket': m.worst_dq_bucket,
        'thr_dq_present_min': t.dq_present_min,
        'thr_dq_health_mean_min': t.dq_health_mean_min,
        'thr_corr_min': t.corr_min,
        'thr_worst_pr_auc_min': t.worst_pr_auc_min,
        'thr_worst_ece_max': t.worst_ece_max,
    }

    if m.dq_present_n < t.dq_present_min:
        return True, 'dq_coverage_too_low', details

    if m.dq_health_mean is not None and m.dq_health_mean < t.dq_health_mean_min:
        return True, 'dq_health_mean_too_low', details

    if m.corr_meta_p_dq_health is not None and m.corr_meta_p_dq_health < t.corr_min:
        return True, 'dq_corr_negative', details

    if m.worst_dq_bucket_pr_auc is not None and m.worst_dq_bucket_pr_auc < t.worst_pr_auc_min:
        return True, 'dq_worst_bucket_pr_auc_low', details

    if m.worst_dq_bucket_ece is not None and m.worst_dq_bucket_ece > t.worst_ece_max:
        return True, 'dq_worst_bucket_ece_high', details

    return False, 'ok', details
