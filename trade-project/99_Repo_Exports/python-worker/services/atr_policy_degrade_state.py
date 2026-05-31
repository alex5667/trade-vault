from enum import StrEnum


class AtrPolicyDegradeState(StrEnum):
    NORMAL = "normal"
    CLIP = "clip"
    REDUCE_ONLY = "reduce_only"
    NO_NEW_RISK = "no_new_risk"
    VENUE_REROUTE = "venue_reroute"
    HARD_FREEZE = "hard_freeze"
