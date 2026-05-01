from dataclasses import dataclass, asdict
from typing import Dict, List

@dataclass(frozen=True)
class InvariantChaosDrill:
    code: str
    invariant_id: str
    drill_kind: str          # payload_mutation | state_stale | gate_conflict | rollout_conflict | portfolio_conflict | protective_path
    expected_action: str     # deny | clip | scope_freeze | rollout_pause | rollback_request | incident_open_and_hard_freeze_new_entries
    execute_mode: str        # runtime | replay | release_gate

DRILLS: Dict[str, InvariantChaosDrill] = {
    "BUY_ORDERING_BROKEN": InvariantChaosDrill(
        code="BUY_ORDERING_BROKEN",
        invariant_id="INV_PAYLOAD_BUY_ORDERING",
        drill_kind="payload_mutation",
        expected_action="deny",
        execute_mode="runtime",
    ),
    "TRADEABLE_WITH_BOOK_STALE": InvariantChaosDrill(
        code="TRADEABLE_WITH_BOOK_STALE",
        invariant_id="INV_TRADEABLE_REQUIRES_NO_HARD_VETO",
        drill_kind="gate_conflict",
        expected_action="deny",
        execute_mode="runtime",
    ),
    "LIVE_WITH_STALE_ALLOCATOR": InvariantChaosDrill(
        code="LIVE_WITH_STALE_ALLOCATOR",
        invariant_id="INV_NO_ALLOCATOR_ON_STALE_STATE_FOR_LIVE_SCOPE",
        drill_kind="state_stale",
        expected_action="scope_freeze",
        execute_mode="runtime",
    ),
    "PORTFOLIO_CAP_BYPASS": InvariantChaosDrill(
        code="PORTFOLIO_CAP_BYPASS",
        invariant_id="INV_NO_PORTFOLIO_CAP_BYPASS",
        drill_kind="portfolio_conflict",
        expected_action="scope_freeze",
        execute_mode="runtime",
    ),
    "LIVE_STAGE_WITHOUT_ROLLOUT_CERT": InvariantChaosDrill(
        code="LIVE_STAGE_WITHOUT_ROLLOUT_CERT",
        invariant_id="INV_NO_STAGE_ADVANCE_WITHOUT_ROLLOUT_CERT",
        drill_kind="rollout_conflict",
        expected_action="rollout_pause",
        execute_mode="release_gate",
    ),
    "PROTECTIVE_EXIT_BLOCKED": InvariantChaosDrill(
        code="PROTECTIVE_EXIT_BLOCKED",
        invariant_id="INV_PROTECTIVE_EXITS_ALWAYS_ALLOWED_UNDER_DEGRADE",
        drill_kind="protective_path",
        expected_action="incident_open_and_hard_freeze_new_entries",
        execute_mode="runtime",
    ),
}

def list_drills() -> List[dict]:
    return [asdict(x) for x in DRILLS.values()]
