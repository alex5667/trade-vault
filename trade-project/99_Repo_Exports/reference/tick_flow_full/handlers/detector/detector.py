from __future__ import annotations

from handlers.detector.ruleset import RuleSet


class Detector:
    """
    FINАЛЬНАЯ ФОРМА:
      - детектор = набор rules/* (механический перенос веток из старого _generate_signals)
      - наружу отдаёт только Candidate’ы (event-only)
    """
    def __init__(self) -> None:
        from handlers.detector.ruleset import RuleSet
        self._rules = RuleSet.default()

    def detect(self, ctx):
        return self._rules.detect(ctx)
