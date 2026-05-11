from __future__ import annotations

"""
Entry Policy Freeze Schema V1

Purpose:
  Circuit-breaker freeze mechanism to halt entry execution when microstructure degrades.
  Prevents trading in bad market conditions (wide spreads, stale book, high pressure).

Design:
  - Fail-open: if freeze key missing/invalid, execution continues normally
  - TTL-based: freeze auto-expires (2-3h depending on regime)
  - Deterministic: same inputs → same freeze decision
  - Anti-flap: min-gap between freeze activations

Expert review:
  - Financial Analysts: Protects against adverse selection in degraded microstructure
  - Senior Python: Fail-open design, strict validation, TTL cleanup
  - DevOps/SRE: Redis-based state, no DB dependency, observable via keys
  - Professor Statistics: Freeze triggers on 2-of-4 metrics (reduces false positives)
"""
import json
from dataclasses import asdict, dataclass, field
from typing import Any

from utils.time_utils import get_ny_time_millis


def _now_ms() -> int:
    return get_ny_time_millis()


def _i(x: Any, d: int) -> int:
    try:
        return int(x)
    except Exception:
        return d


def _f(x: Any, d: float) -> float:
    try:
        v = float(x)
        if v != v:  # NaN check
            return d
        return v
    except Exception:
        return d


def _s(x: Any, d: str = "") -> str:
    try:
        return str(x)
    except Exception:
        return d


@dataclass
class EntryPolicyFreezeV1:
    """
    Redis JSON schema for circuit-breaker freeze.
    
    Key pattern: cfg:entry_policy:freeze:v1:{SYMBOL}:{GROUP}:{SCENARIO}
    
    Fields:
        ver: Schema version (must be 1)
        symbol: Trading symbol (e.g., "BTCUSDT")
        group: Regime group (e.g., "default", "thin")
        scenario: "reversal" or "continuation"
        until_ts_ms: Freeze active while now < until_ts_ms
        mode: "hard" (stop execution) or "shadow" (audit only, no execution)
        reason_code: "DATA_BAD" (extensible for future reasons)
        notes: Human-readable explanation of trigger
        src: Source service ("cb_v1")
        created_ts_ms: When freeze was created
        metrics: Snapshot of metrics that triggered freeze
    """
    ver: int = 1
    symbol=""
    group: str = "default"
    scenario: str = ""
    until_ts_ms: int = 0
    mode: str = "hard"  # "hard" | "shadow" (default hard for backward compat)
    reason_code: str = "DATA_BAD"
    notes: str = ""
    src: str = "cb_v1"
    created_ts_ms: int = 0
    metrics: dict[str, Any] = field(default_factory=dict)
    # Promotion tracking (shadow→hard auto-promotion)
    promoted_ts_ms: int = 0   # epoch ms when promoted; 0 = not promoted
    promoted_reason: str = "" # e.g. "blocked_8_seen_15_bad_cnt_2"

    def is_active(self, now_ms: int | None = None) -> bool:
        """Check if freeze is currently active"""
        now = int(now_ms or _now_ms())
        return int(self.until_ts_ms or 0) > now

    def to_json(self) -> str:
        """Serialize to JSON for Redis storage"""
        return json.dumps(asdict(self), ensure_ascii=False, separators=(",", ":"))

    @staticmethod
    def from_json(raw: str) -> tuple[EntryPolicyFreezeV1 | None, str]:
        """
        Deserialize from Redis JSON (fail-safe).
        
        Returns:
            (freeze_obj, error_code): freeze_obj=None if invalid
        """
        try:
            d = json.loads(raw)
            if not isinstance(d, dict):
                return None, "not_dict"
        except Exception:
            return None, "bad_json"

        fz = EntryPolicyFreezeV1(
            ver=_i(d.get("ver", 1), 1),
            symbol=_s(d.get("symbol", ""), "").upper(),  # type: ignore
            group=_s(d.get("group", "default"), "default").lower(),
            scenario=_s(d.get("scenario", ""), "").lower(),
            until_ts_ms=_i(d.get("until_ts_ms", 0), 0),
            mode=_s(d.get("mode", "hard"), "hard").lower(),
            reason_code=_s(d.get("reason_code", "DATA_BAD"), "DATA_BAD"),
            notes=_s(d.get("notes", ""), ""),
            src=_s(d.get("src", "cb_v1"), "cb_v1"),
            created_ts_ms=_i(d.get("created_ts_ms", 0), 0),
            metrics=d.get("metrics") if isinstance(d.get("metrics"), dict) else {},
            promoted_ts_ms=_i(d.get("promoted_ts_ms", 0), 0),
            promoted_reason=_s(d.get("promoted_reason", ""), ""),
        )

        ok, err = fz.validate()
        return (fz if ok else None), err

    def validate(self) -> tuple[bool, str]:
        """
        Validate freeze object.
        
        Returns:
            (ok, error_code)
        """
        if self.ver != 1:
            return False, "bad_ver"
        if not self.symbol:
            return False, "no_symbol"
        if self.scenario not in ("reversal", "continuation"):
            return False, "bad_scenario"
        if self.until_ts_ms <= 0:
            return False, "bad_until"
        if self.mode not in ("hard", "shadow"):
            return False, "bad_mode"
        return True, ""
