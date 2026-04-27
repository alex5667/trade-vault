from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict


@dataclass(frozen=True)
class ConfirmVeto:
    """
    Унифицированный veto для валидаторов.
    """
    veto: bool
    reason: str


@dataclass(frozen=True)
class ConfirmResultBase:
    """
    База для результатов подтверждений:
      - ok: прошёл ли валидатор
      - veto: жёсткий запрет (fail-closed)
      - score01: мягкий скор [0..1] (для conf_factor)
      - reason: короткий reason_code
      - details: доп. фичи (для аудита/обучения)
    """
    ok: bool
    veto: bool
    score01: float
    reason: str
    details: Dict[str, Any]

    def as_tuple_legacy(self) -> tuple[bool, dict[str, Any]]:
        """
        Помогает сохранить обратную совместимость, если старый код ожидает (ok, details).
        """
        d = dict(self.details)
        d.setdefault("reason", self.reason)
        d.setdefault("veto", self.veto)
        d.setdefault("score01", float(self.score01))
        return bool(self.ok), d
