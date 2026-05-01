from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol

from handlers.pipeline.candidate import Candidate
from handlers.detector.rules.breakout import BreakoutRule
from handlers.detector.rules.absorption import AbsorptionRule
from handlers.detector.rules.extreme import ExtremeRule
from handlers.detector.rules.obi_spike import ObiSpikeRule


class Rule(Protocol):
    def detect(self, ctx: Any) -> list[Candidate]: ...


@dataclass
class RuleSet:
    rules: list[Rule]

    @staticmethod
    def default() -> "RuleSet":
        return RuleSet(
            rules=[
                BreakoutRule(),
                AbsorptionRule(),
                ExtremeRule(),
                ObiSpikeRule(),
            ]
        )

    def detect(self, ctx: Any) -> list[Candidate]:
        out: list[Candidate] = []
        for r in self.rules:
            out.extend(r.detect(ctx))
        return out
