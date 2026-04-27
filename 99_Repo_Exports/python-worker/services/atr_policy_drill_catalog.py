from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Dict, List


@dataclass(frozen=True)
class RecoveryDrill:
    code: str
    description: str
    mutate_kind: str       # redis_delete | redis_corrupt | workflow_rebuild | operator_state_rebuild
    requires_execute: bool


DRILLS: Dict[str, RecoveryDrill] = {
    "ACTIVE_KEY_DELETE": RecoveryDrill(
        code="ACTIVE_KEY_DELETE",
        description="Delete active policy key for one bounded cohort; recovery must rebuild from SQL snapshot.",
        mutate_kind="redis_delete",
        requires_execute=True,
    ),
    "LAST_GOOD_DELETE": RecoveryDrill(
        code="LAST_GOOD_DELETE",
        description="Delete last_good key for one bounded cohort; recovery must rebuild from SQL snapshot.",
        mutate_kind="redis_delete",
        requires_execute=True,
    ),
    "ACTIVE_REF_DELETE": RecoveryDrill(
        code="ACTIVE_REF_DELETE",
        description="Delete Telegram active_ref mapping; operator bootstrap must rebuild it.",
        mutate_kind="redis_delete",
        requires_execute=True,
    ),
    "PENDING_QUEUE_DROP": RecoveryDrill(
        code="PENDING_QUEUE_DROP",
        description="Remove pending queue members; workflow rebuild must restore them from SQL proposals.",
        mutate_kind="workflow_rebuild",
        requires_execute=True,
    ),
    "DECIDED_QUEUE_DROP": RecoveryDrill(
        code="DECIDED_QUEUE_DROP",
        description="Remove decided queue members and/or decision payloads; rebuild must restore them from SQL.",
        mutate_kind="workflow_rebuild",
        requires_execute=True,
    ),
    "CONFIRM_TOKEN_WIPE": RecoveryDrill(
        code="CONFIRM_TOKEN_WIPE",
        description="Clear Redis confirm tokens; operator bootstrap must expire pending confirms safely.",
        mutate_kind="operator_state_rebuild",
        requires_execute=True,
    ),
}


def list_drills() -> List[dict]:
    return [asdict(x) for x in DRILLS.values()]
