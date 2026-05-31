from __future__ import annotations

import hashlib
from typing import Any

_STAGE_TO_SHARE = {
    "shadow": 0.0,
    "canary_5": 0.05,
    "canary_25": 0.25,
    "live_100": 1.0,
    "frozen": 0.0,
    "rolled_back": 0.0,
}

def _u01(key: str) -> float:
    h = hashlib.sha1(key.encode("utf-8")).hexdigest()
    return (int(h[:8], 16) % 10_000) / 10_000.0

def should_apply_rollout(*, sticky_key: str, rollout_stage: str, explicit_share: float | None = None) -> bool:
    share = explicit_share if explicit_share is not None else _STAGE_TO_SHARE.get(rollout_stage, 0.0)
    share = max(0.0, min(1.0, float(share)))
    return _u01(sticky_key) < share

def build_rollout_sticky_key(signal: dict[str, Any]) -> str:
    meta = signal.get("meta", {}) if isinstance(signal.get("meta"), dict) else {}
    return "|".join([
        (signal.get("symbol") or "").upper(),
        str(signal.get("kind") or signal.get("scenario") or "").lower(),
        str(meta.get("regime") or signal.get("regime") or "na").lower(),
        str(meta.get("horizon", {}).get("risk_horizon_bucket") if isinstance(meta.get("horizon"), dict) else signal.get("risk_horizon_bucket") or "unknown").lower(),
        str(signal.get("session") or meta.get("session") or "default"),
    ])
