from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

"""
ConfirmResult — единый контракт для handler-free валидаторов (L2/L3/etc).

Почему так:
  - ConfirmationsEngine должен агрегировать результаты из разных валидаторов
    без знания деталей конкретного валидатора.
  - reason_code/reason_u16 нужны для 5.2 (структурные причины veto).

Семантика:
  - veto=True  => сигнал запрещён (fail-closed на уровне ConfirmationsEngine по policy).
  - passed=False,veto=False => "soft fail": ухудшаем качество/score, но не блокируем.
  - score01 ∈ [0..1] — детерминированный локальный скор (если валидатор считает).
"""


@dataclass(frozen=True)
class ConfirmResult:
    passed: bool
    veto: bool
    # "parts" — численные/категориальные фичи для скоринга/логов (допускаем Any для wire/debug)
    parts: dict[str, Any] = field(default_factory=dict)
    # "flags" — структурные булевы/числовые флаги (для kind_rules и др.)
    flags: dict[str, Any] = field(default_factory=dict)
    # "reasons" — debug-строки (НЕ стабильный интерфейс, можно менять)
    reasons: list[str] = field(default_factory=list)
    # локальный quality score (0..1); default=1.0, если валидатор не оценивает
    score01: float = 1.0
    # 5.2 wire ABI
    reason_code: str = "OK"
    reason_u16: int = 0

    @property
    def reason(self) -> str:
        if self.reason_code and self.reason_code != "OK":
            return self.reason_code
        if self.reasons:
            return self.reasons[0]
        return self.reason_code

    @property
    def details(self) -> dict[str, Any]:
        return self.parts
