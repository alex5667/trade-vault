from __future__ import annotations

"""
Entry Policy Overrides V1 - Strict Schema with Validation

Purpose:
  Type-safe, forward-compatible override schema for entry policy thresholds.
  Prevents oscillation via hold-down and hysteresis mechanisms.

Design principles (Expert review):
  - Fail-open: invalid overrides are ignored, system falls back to ENV defaults
  - Deterministic: applied as snapshot per decision (no mid-flight changes)
  - Stable: hold-down prevents rapid switching, hysteresis requires significant improvement
  - Traceable: metadata tracks source (manual/lcb), suggestion ID, application timestamp

Expert validation:
  - Financial Analysts: Hold-down (6-12h) prevents overreaction to noise
  - Senior Python: Dataclass with strict validation, backward-compatible parsing
  - DevOps/SRE: TTL-based Redis storage, no schema migrations required
  - Professor Statistics: Hysteresis prevents statistical dithering at decision boundaries
"""
import json
from dataclasses import asdict, dataclass, field
from typing import Any

from utils.time_utils import get_ny_time_millis


def _now_ms() -> int:
    return get_ny_time_millis()


def _f(x: Any, d: float) -> float:
    """Safe float conversion with NaN check"""
    try:
        v = float(x)
        if v != v:  # NaN check
            return d
        return v
    except Exception:
        return d


def _i(x: Any, d: int) -> int:
    """Safe int conversion"""
    try:
        return int(x)
    except Exception:
        return d


def _s(x: Any, d: str) -> str:
    """Safe string conversion"""
    try:
        return str(x)
    except Exception:
        return d


@dataclass
class EntryPolicyOverridesV1:
    """
    Strict, forward-compatible override schema.
    
    Storage: Redis key cfg:entry_policy:overrides:{SYMBOL}:{GROUP}
    
    Rules:
      - fail-open: invalid => ignored (system uses ENV defaults)
      - deterministic: applied as snapshot per decision
      - stable: supports hold-down/hysteresis to prevent oscillation
    
    Hold-down mechanism:
      After application, no new suggestions accepted for hold_down_ms duration.
      Prevents rapid switching due to statistical noise.
    
    Hysteresis mechanism:
      New candidate must exceed current by (min_impr + hysteresis_impr).
      Prevents dithering at decision boundaries.
    """
    ver: int = 1

    # Policy thresholds (all optional - only override what's needed)
    entry_min_of_score: float | None = None      # Range: 0..2 (typical 0.67..1.0)
    entry_max_spread_z: float | None = None      # Range: 0..10 (typical 1.5..3.0)
    entry_near_zone_bp: float | None = None      # Range: 1..200 (typical 8..20)
    entry_obi_min_sec: float | None = None       # Range: 0..10 (typical 1.0..2.0)
    entry_min_leader_conf: float | None = None   # Range: 0..1

    # Stabilization metadata
    applied_ts_ms: int = 0                          # Timestamp when override was applied
    hold_down_ms: int = 0                           # Duration to block new suggestions (ms)
    hysteresis_impr: float = 0.0                    # Additional improvement required vs current

    # Traceability
    sid: str = ""                                   # Suggestion ID that was applied
    src: str = "manual"                             # Source: "manual"|"ab_lcb"|"thresh_lcb"
    notes: str = ""                                 # Human-readable notes
    extra: dict[str, Any] = field(default_factory=dict)  # Extensibility

    def to_json(self) -> str:
        """Serialize to JSON for Redis storage"""
        return json.dumps(asdict(self), ensure_ascii=False, separators=(",", ":"))

    @staticmethod
    def from_json(raw: str) -> tuple[EntryPolicyOverridesV1 | None, str]:
        """
        Deserialize from JSON with validation.
        
        Returns:
            (obj, err_code): obj is None if invalid, err_code="" means success
        
        Backward compatibility:
            Accepts both legacy ENV-like keys (ENTRY_MIN_OF_SCORE) and
            new snake_case keys (entry_min_of_score) for zero-downtime migration.
        """
        try:
            d = json.loads(raw)
            if not isinstance(d, dict):
                return None, "not_dict"
        except Exception:
            return None, "bad_json"

        # Backward-compatible key picker (ENV-style or snake_case)
        def pick(*ks: str) -> Any:
            for k in ks:
                if k in d:
                    return d.get(k)
            return None

        o = EntryPolicyOverridesV1(
            ver=_i(pick("ver", "version"), 1),
            entry_min_of_score=pick("entry_min_of_score", "ENTRY_MIN_OF_SCORE"),
            entry_max_spread_z=pick("entry_max_spread_z", "ENTRY_MAX_SPREAD_Z"),
            entry_near_zone_bp=pick("entry_near_zone_bp", "ENTRY_NEAR_ZONE_BP"),
            entry_obi_min_sec=pick("entry_obi_min_sec", "ENTRY_OBI_MIN_SEC"),
            entry_min_leader_conf=pick("entry_min_leader_conf", "ENTRY_MIN_LEADER_CONF"),
            applied_ts_ms=_i(pick("applied_ts_ms", "appliedAt", "applied_at"), 0),
            hold_down_ms=_i(pick("hold_down_ms", "holdDownMs"), 0),
            hysteresis_impr=_f(pick("hysteresis_impr", "hysteresisImpr"), 0.0),
            sid=_s(pick("sid", "suggestion_id"), ""),
            src=_s(pick("src", "source"), "manual"),
            notes=_s(pick("notes"), ""),
            extra=d.get("extra") if isinstance(d.get("extra"), dict) else {},  # type: ignore
        )

        ok, err = o.validate()
        return (o if ok else None), err

    def validate(self) -> tuple[bool, str]:
        """
        Validate and enforce range constraints.
        
        Philosophy: Fail-closed for obviously broken values to prevent surprise.
        We do NOT auto-clamp silently - invalid => reject entire override.
        
        Returns:
            (ok, err_code): ok=True means valid, err_code="" on success
        """
        if self.ver != 1:
            return False, "bad_ver"

        # Validate ranges (conservative bounds to catch typos/bugs)
        if self.entry_min_of_score is not None:
            v = _f(self.entry_min_of_score, -1.0)
            if not (0.0 <= v <= 2.0):
                return False, "bad_entry_min_of_score"
            self.entry_min_of_score = v

        if self.entry_max_spread_z is not None:
            v = _f(self.entry_max_spread_z, -1.0)
            if not (0.0 <= v <= 10.0):
                return False, "bad_entry_max_spread_z"
            self.entry_max_spread_z = v

        if self.entry_near_zone_bp is not None:
            v = _f(self.entry_near_zone_bp, -1.0)
            if not (1.0 <= v <= 200.0):
                return False, "bad_entry_near_zone_bp"
            self.entry_near_zone_bp = v

        if self.entry_obi_min_sec is not None:
            v = _f(self.entry_obi_min_sec, -1.0)
            if not (0.0 <= v <= 10.0):
                return False, "bad_entry_obi_min_sec"
            self.entry_obi_min_sec = v

        if self.entry_min_leader_conf is not None:
            v = _f(self.entry_min_leader_conf, -1.0)
            if not (0.0 <= v <= 1.0):
                return False, "bad_entry_min_leader_conf"
            self.entry_min_leader_conf = v

        # Validate stabilization params
        if self.hold_down_ms < 0:
            return False, "bad_hold_down_ms"
        if self.hysteresis_impr < 0:
            return False, "bad_hysteresis_impr"

        # Applied timestamp is optional; 0 means "effective immediately"
        if self.applied_ts_ms < 0:
            self.applied_ts_ms = 0

        return True, ""

    def is_in_hold_down(self, now_ms: int) -> bool:
        """
        Check if override is in hold-down period.
        
        Hold-down prevents new suggestions from being applied too quickly.
        Typical values: 6-12 hours depending on regime volatility.
        
        Args:
            now_ms: Current timestamp (milliseconds)
        
        Returns:
            True if still in hold-down period (block new suggestions)
        """
        if self.hold_down_ms <= 0:
            return False

        ts0 = int(self.applied_ts_ms or 0)
        if ts0 <= 0:
            return False  # No application timestamp => not in hold-down

        elapsed = now_ms - ts0
        return elapsed < int(self.hold_down_ms)
