"""
calibration/promotion_manifest.py — Plan 3 / Step 4 manifest builder/serializer.

A promotion manifest is a single-file record of "what we measured" for a
candidate config/model. It rides with the trial through the
SHADOW → CANARY → ENFORCE pipeline so any reviewer sees the OOS evidence
without re-running the pipeline.

Pure-Python; no external IO. The caller is responsible for writing the
JSON to disk / Redis / S3 / wherever audit retention lives.
"""
from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from typing import Any

from .promotion_gate import PromotionMetrics, PromotionThresholds, can_promote


@dataclass(frozen=True)
class PromotionManifest:
    """A single candidate's complete evaluation record."""

    candidate_id: str            # opaque id (timestamp-based or hash)
    code_sha: str                 # git sha of the code that produced these metrics
    schema_hash: str              # feature schema fingerprint
    feature_cols_hash: str        # explicit feature_cols fingerprint (model input set)
    data_start_ms: int
    data_end_ms: int
    n_trials: int                 # how many Optuna trials were run

    metrics: PromotionMetrics
    thresholds: PromotionThresholds
    decision: str                 # "PROMOTE_TO_SHADOW" / "REJECTED" / "REPORT_ONLY"
    reasons: list[str] = field(default_factory=list)
    extras: dict[str, Any] = field(default_factory=dict)


def build_manifest(
    *,
    candidate_id: str,
    code_sha: str,
    schema_hash: str,
    feature_cols_hash: str,
    data_start_ms: int,
    data_end_ms: int,
    n_trials: int,
    metrics: PromotionMetrics,
    thresholds: PromotionThresholds | None = None,
    enforce_decision: bool = False,
    extras: dict[str, Any] | None = None,
) -> PromotionManifest:
    """Construct a manifest and stamp the decision per the promotion gate.

    Args:
        enforce_decision: when False (default) the manifest carries decision
            "REPORT_ONLY" regardless of can_promote outcome — used during the
            Plan 3 step-4 ramp where we measure but do not auto-promote.
            When True, decision is "PROMOTE_TO_SHADOW" iff passed else
            "REJECTED".
    """
    thr = thresholds or PromotionThresholds()
    passed, reasons = can_promote(metrics, thr)

    if not enforce_decision:
        decision = "REPORT_ONLY"
    elif passed:
        decision = "PROMOTE_TO_SHADOW"
    else:
        decision = "REJECTED"

    return PromotionManifest(
        candidate_id=candidate_id,
        code_sha=code_sha,
        schema_hash=schema_hash,
        feature_cols_hash=feature_cols_hash,
        data_start_ms=data_start_ms,
        data_end_ms=data_end_ms,
        n_trials=n_trials,
        metrics=metrics,
        thresholds=thr,
        decision=decision,
        reasons=reasons,
        extras=extras or {},
    )


def to_json(manifest: PromotionManifest, indent: int | None = 2) -> str:
    """JSON serialization — round-trippable via from_json."""
    data = asdict(manifest)
    return json.dumps(data, indent=indent, sort_keys=True, default=str)


def from_json(s: str) -> PromotionManifest:
    """Reconstruct a manifest from JSON (typed parse)."""
    raw = json.loads(s)
    metrics = PromotionMetrics(**raw["metrics"])
    thr = PromotionThresholds(**raw["thresholds"])
    return PromotionManifest(
        candidate_id=raw["candidate_id"],
        code_sha=raw["code_sha"],
        schema_hash=raw["schema_hash"],
        feature_cols_hash=raw["feature_cols_hash"],
        data_start_ms=int(raw["data_start_ms"]),
        data_end_ms=int(raw["data_end_ms"]),
        n_trials=int(raw["n_trials"]),
        metrics=metrics,
        thresholds=thr,
        decision=raw["decision"],
        reasons=list(raw.get("reasons") or []),
        extras=dict(raw.get("extras") or {}),
    )
