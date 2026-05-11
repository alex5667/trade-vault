import json
from dataclasses import asdict, dataclass
from typing import Any, Optional


def _s(x: Any) -> str:
    return (x or "").strip()


def _arm(x: Any) -> str:
    v = _s(x).upper()
    return v if v in ("A", "B", "C") else "A"


def _rg(x: Any) -> str:
    return _s(x).lower() or "na"


def _scn(x: Any) -> str:
    v = _s(x).lower()
    return v if v in ("continuation", "reversal") else "na"


@dataclass
class EntryPolicySuggestionMetaV1:
    """
    Stored at: cfg:suggestions:entry_policy:meta:{sid}
    Pointer at: cfg:suggestions:entry_policy:latest:ab_winner:{symbol}:{regime}:{group}:{scenario} -> sid
    """
    v: int = 1
    sid: str = ""
    created_ts_ms: int = 0
    updated_ts_ms: int = 0
    expires_ts_ms: int = 0

    symbol=""
    regime: str = "na"
    group: str = "default"
    scenario: str = "na"

    winner_arm: str = "A"
    baseline_arm: str = "A"

    # Key stats for audit / reproducibility
    min_n: int = 0
    alpha: float = 0.10
    min_edge_r: float = 0.0
    reason: str = ""

    # Arm metrics snapshot (LCB etc) - compact dict for UI/reporting
    arm_metrics: dict[str, Any] = None  # type: ignore

    # Approval policy
    approvals_required: int = 2

    def validate(self) -> tuple[bool, str]:
        if self.v != 1:
            return False, "bad_version"
        if not _s(self.sid):
            return False, "sid_empty"
        if not _s(self.symbol):
            return False, "symbol_empty"
        if _scn(self.scenario) == "na":
            return False, "scenario_invalid"
        if _arm(self.winner_arm) not in ("A", "B", "C"):
            return False, "winner_arm_invalid"
        if int(self.approvals_required) < 0:
            return False, "approvals_required_invalid"
        return True, "ok"

    def to_json(self) -> str:
        d = asdict(self)
        if d.get("arm_metrics") is None:
            d["arm_metrics"] = {}
        return json.dumps(d, ensure_ascii=False, separators=(",", ":"))

    @staticmethod
    def from_json(raw: str) -> tuple[Optional["EntryPolicySuggestionMetaV1"], str]:
        try:
            d = json.loads(raw or "")
            o = EntryPolicySuggestionMetaV1(
                v=int(d.get("v", 1) or 1),
                sid=_s(d.get("sid", "")),
                created_ts_ms=int(d.get("created_ts_ms", 0) or 0),
                updated_ts_ms=int(d.get("updated_ts_ms", 0) or 0),
                expires_ts_ms=int(d.get("expires_ts_ms", 0) or 0),
                symbol=_s(d.get("symbol", "")),  # type: ignore
                regime=_rg(d.get("regime", "na")),
                group=_s(d.get("group", "default")).lower() or "default",
                scenario=_scn(d.get("scenario", "na")),
                winner_arm=_arm(d.get("winner_arm", d.get("winner", "A"))),
                baseline_arm=_arm(d.get("baseline_arm", "A")),
                min_n=int(d.get("min_n", 0) or 0),
                alpha=float(d.get("alpha", 0.10) or 0.10),
                min_edge_r=float(d.get("min_edge_r", 0.0) or 0.0),
                reason=_s(d.get("reason", "")),
                arm_metrics=(d.get("arm_metrics") if isinstance(d.get("arm_metrics"), dict) else {}),
                approvals_required=int(d.get("approvals_required", 2) or 2),
            )
            ok, why = o.validate()
            return (o if ok else None), why
        except Exception:
            return None, "parse_error"
