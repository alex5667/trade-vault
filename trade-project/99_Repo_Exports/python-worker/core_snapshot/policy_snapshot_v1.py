from __future__ import annotations

"""Policy snapshot for Train==Serve determinism.

Why this file exists:
  - Runtime DQ-gate / trackers use *effective* thresholds and alpha values.
  - Offline builders must record the same effective policy (SAFE/STRICT, alpha, thresholds)
    as *metadata*, not as features.

Design:
  - Snapshot is JSON-serializable, stable-ordered, and produces a stable hash.
  - The snapshot intentionally includes only policy knobs that affect decisions/features.
  - Runtime/uptime values are NOT included here (they go to decision record separately).
"""


import hashlib
import json
import os
from dataclasses import asdict, dataclass
from typing import Any


def _as_bool(x: Any, default: bool = False) -> bool:
    try:
        if isinstance(x, bool):
            return x
        if x is None:
            return default
        s = str(x).strip().lower()
        if s in ("1", "true", "yes", "y", "on"):
            return True
        if s in ("0", "false", "no", "n", "off"):
            return False
    except Exception:
        pass
    return default


def _as_int(x: Any, default: int) -> int:
    try:
        return int(x)
    except Exception:
        return default


def _as_float(x: Any, default: float) -> float:
    try:
        return float(x)
    except Exception:
        return default


def _stable_json(obj: Any) -> str:
    """Stable JSON string for hashing/persistence."""
    return json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)


def stable_hash(obj: Any) -> str:
    """sha256 over stable-json."""
    b = _stable_json(obj).encode("utf-8")
    return hashlib.sha256(b).hexdigest()


def _derive_book_alpha(book_stream_interval_ms: int) -> float:
    """Map stream interval to EMA alpha.

    Matches rollout guidance (deterministic default picks):
      ~100ms (~10Hz): 0.10
      250ms: 0.20
      500ms: 0.30
      1Hz+:  0.40 (mid of 0.30..0.50)
    """
    ms = int(book_stream_interval_ms)
    if ms <= 120:
        return 0.10
    if ms <= 300:
        return 0.20
    if ms <= 700:
        return 0.30
    return 0.40


@dataclass(frozen=True)
class DQThresholdsV1:
    """Effective thresholds (resolved after SAFE/STRICT + interval mapping + overrides)."""

    dq_mode: str
    book_stream_interval_ms: int

    # gap
    dq_gap_soft_ms: int
    dq_gap_hard_ms: int
    dq_gap_extreme_ms: int
    dq_gap_min_samples: int

    # tick missing seq
    dq_tick_missing_seq_soft: float
    dq_tick_missing_seq_hard: float

    # book missing seq
    dq_book_missing_seq_soft: float
    dq_book_missing_seq_hard: float

    # alpha
    dq_book_seq_ema_alpha: float


def resolve_dq_thresholds(cfg2: dict[str, Any]) -> DQThresholdsV1:
    """Resolve effective thresholds.

    If project already has core_snapshot.dq_thresholds, prefer it.
    Otherwise use local deterministic defaults.
    """
    try:
        from core_snapshot.dq_thresholds import resolve_effective_thresholds  # type: ignore

        eff = resolve_effective_thresholds(cfg2)
        return DQThresholdsV1(
            dq_mode=(eff.get("dq_mode", "safe")),
            book_stream_interval_ms=int(eff.get("book_stream_interval_ms", 100)),
            dq_gap_soft_ms=int(eff.get("dq_gap_soft_ms", 3000)),
            dq_gap_hard_ms=int(eff.get("dq_gap_hard_ms", 10000)),
            dq_gap_extreme_ms=int(eff.get("dq_gap_extreme_ms", 30000)),
            dq_gap_min_samples=int(eff.get("dq_gap_min_samples", 20)),
            dq_tick_missing_seq_soft=float(eff.get("dq_tick_missing_seq_soft", 2.0)),
            dq_tick_missing_seq_hard=float(eff.get("dq_tick_missing_seq_hard", 10.0)),
            dq_book_missing_seq_soft=float(eff.get("dq_book_missing_seq_soft", 10.0)),
            dq_book_missing_seq_hard=float(eff.get("dq_book_missing_seq_hard", 30.0)),
            dq_book_seq_ema_alpha=float(eff.get("dq_book_seq_ema_alpha", _derive_book_alpha(int(eff.get("book_stream_interval_ms", 100))))),
        )
    except Exception:
        pass

    dq_mode = (cfg2.get("dq_mode", cfg2.get("DQ_MODE", "safe")) or "safe").lower()
    if dq_mode not in ("safe", "strict"):
        dq_mode = "safe"

    book_ms = _as_int(cfg2.get("book_stream_interval_ms", cfg2.get("BOOK_STREAM_INTERVAL_MS", 100)), 100)

    if dq_mode == "strict":
        gap_soft, gap_hard, gap_ext = 1500, 3000, 10000
        tick_soft, tick_hard = 1.0, 3.0
    else:
        gap_soft, gap_hard, gap_ext = 3000, 10000, 30000
        tick_soft, tick_hard = 2.0, 10.0

    gap_soft = _as_int(cfg2.get("dq_gap_soft_ms", cfg2.get("DQ_GAP_SOFT_MS", gap_soft)), gap_soft)
    gap_hard = _as_int(cfg2.get("dq_gap_hard_ms", cfg2.get("DQ_GAP_HARD_MS", gap_hard)), gap_hard)
    gap_ext = _as_int(cfg2.get("dq_gap_extreme_ms", cfg2.get("DQ_GAP_EXTREME_MS", gap_ext)), gap_ext)
    gap_min_samples = _as_int(cfg2.get("dq_gap_min_samples", cfg2.get("DQ_GAP_MIN_SAMPLES", 20)), 20)

    tick_soft = _as_float(cfg2.get("dq_tick_missing_seq_soft", cfg2.get("DQ_TICK_MISSING_SEQ_SOFT", tick_soft)), tick_soft)
    tick_hard = _as_float(cfg2.get("dq_tick_missing_seq_hard", cfg2.get("DQ_TICK_MISSING_SEQ_HARD", tick_hard)), tick_hard)

    base_book_soft = 10.0
    base_book_hard = 30.0
    if book_ms >= 900:
        base_book_hard = 60.0
    elif book_ms >= 450:
        base_book_hard = 45.0
    elif book_ms >= 200:
        base_book_hard = 35.0

    book_soft = _as_float(cfg2.get("dq_book_missing_seq_soft", cfg2.get("DQ_BOOK_MISSING_SEQ_SOFT", base_book_soft)), base_book_soft)
    book_hard = _as_float(cfg2.get("dq_book_missing_seq_hard", cfg2.get("DQ_BOOK_MISSING_SEQ_HARD", cfg2.get("book_hard", base_book_hard))), base_book_hard)

    alpha = cfg2.get("dq_book_seq_ema_alpha")
    if alpha is None:
        alpha = cfg2.get("DQ_BOOK_SEQ_EMA_ALPHA")
    if alpha is None:
        alpha = cfg2.get("BOOK_MISSING_SEQ_EMA_ALPHA")
    a = _as_float(alpha, _derive_book_alpha(book_ms))
    if a < 0.01:
        a = 0.01
    if a > 0.95:
        a = 0.95

    return DQThresholdsV1(
        dq_mode=dq_mode,
        book_stream_interval_ms=int(book_ms),
        dq_gap_soft_ms=int(gap_soft),
        dq_gap_hard_ms=int(gap_hard),
        dq_gap_extreme_ms=int(gap_ext),
        dq_gap_min_samples=int(gap_min_samples),
        dq_tick_missing_seq_soft=float(tick_soft),
        dq_tick_missing_seq_hard=float(tick_hard),
        dq_book_missing_seq_soft=float(book_soft),
        dq_book_missing_seq_hard=float(book_hard),
        dq_book_seq_ema_alpha=float(a),
    )


@dataclass(frozen=True)
class DQPolicySnapshotV1:
    snapshot_version: int
    thresholds: DQThresholdsV1
    dq_book_veto_enabled: bool
    dq_observe_only_sec: int
    dq_gate_mode: str


def build_dq_policy_snapshot(cfg2: dict[str, Any]) -> tuple[DQPolicySnapshotV1, str]:
    thr = resolve_dq_thresholds(cfg2)
    dq_book_veto_enabled = _as_bool(cfg2.get("dq_book_veto_enabled", cfg2.get("DQ_BOOK_VETO_ENABLED", False)), False)
    dq_observe_only_sec = _as_int(cfg2.get("dq_observe_only_sec", cfg2.get("DQ_OBSERVE_ONLY_SEC", 86400)), 86400)
    dq_gate_mode = (cfg2.get("dq_gate_mode", cfg2.get("DQ_GATE_MODE", "off")) or "off").lower()

    snap = DQPolicySnapshotV1(
        snapshot_version=1,
        thresholds=thr,
        dq_book_veto_enabled=bool(dq_book_veto_enabled),
        dq_observe_only_sec=int(dq_observe_only_sec),
        dq_gate_mode=str(dq_gate_mode),
    )
    return snap, stable_hash(asdict(snap))


def env_git_sha() -> str:
    for k in ("GIT_SHA", "GIT_COMMIT", "SOURCE_VERSION"):
        v = os.environ.get(k)
        if v:
            return str(v)
    return ""


@dataclass(frozen=True)
class FeatureManifestV1:
    manifest_version: int
    meta_schema_name: str
    meta_schema_version: int
    meta_schema_hash: str
    meta_cols_hash: str
    dq_policy_hash: str
    dq_mode: str
    book_stream_interval_ms: int
    dq_book_seq_ema_alpha: float
    git_sha: str


def build_feature_manifest_v1(
    *,
    meta_schema_name: str,
    meta_schema_version: int,
    meta_schema_hash: str,
    meta_cols: tuple[str, ...],
    dq_policy_hash: str,
    thr: DQThresholdsV1,
) -> tuple[FeatureManifestV1, str]:
    cols_hash = stable_hash(list(meta_cols))
    man = FeatureManifestV1(
        manifest_version=1,
        meta_schema_name=str(meta_schema_name),
        meta_schema_version=int(meta_schema_version),
        meta_schema_hash=str(meta_schema_hash),
        meta_cols_hash=str(cols_hash),
        dq_policy_hash=str(dq_policy_hash),
        dq_mode=str(thr.dq_mode),
        book_stream_interval_ms=int(thr.book_stream_interval_ms),
        dq_book_seq_ema_alpha=float(thr.dq_book_seq_ema_alpha),
        git_sha=env_git_sha(),
    )
    return man, stable_hash(asdict(man))


def to_public_dict(obj: Any) -> dict[str, Any]:
    if hasattr(obj, "__dataclass_fields__"):
        return asdict(obj)
    if isinstance(obj, dict):
        return dict(obj)
    return {"value": str(obj)}
