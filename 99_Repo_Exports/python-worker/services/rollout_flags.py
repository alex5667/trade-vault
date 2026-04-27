from __future__ import annotations

"""Central rollout/feature-flag contract for execution hardening phases.

The project now has multiple progressively enabled layers:
  * P0/P1 execution safety + maker ladder
  * P2 reconcile + user stream
  * P3/P4 DQ hard veto + portfolio risk + SQL journal

Operationally these must be rolled out independently and quickly rolled back
without editing Python code or re-building images. This module centralises the
flag names and default semantics so executor / signal publisher / compose
files all speak the same language.
"""

from dataclasses import dataclass
import os
from typing import Dict, Any


def _b(name: str, default: bool) -> bool:
    v = os.getenv(name)
    if v is None or str(v).strip() == "":
        return bool(default)
    return str(v).strip().lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class RolloutFlags:
    exec_algo_canonical_v2: bool = True
    exec_reconcile_enable: bool = True
    exec_user_stream_enable: bool = True
    exec_maker_tp_enable: bool = True
    exec_journal_sql_enable: bool = True
    trade_dq_hard_veto_enable: bool = True
    trade_risk_engine_v2_enable: bool = True
    strategy_shadow_only: bool = False
    exec_force_safety_first: bool = False
    exec_degraded_mode_force_safety_first: bool = True
    exec_degraded_mode_disable_maker: bool = True

    @classmethod
    def from_env(cls) -> "RolloutFlags":
        """Build rollout flags from environment.

        Names are intentionally explicit so operators can grep configs and
        overrides quickly during incidents.
        """
        return cls(
            exec_algo_canonical_v2=_b("EXEC_ALGO_CANONICAL_V2", True),
            exec_reconcile_enable=_b("EXEC_RECONCILE_ENABLE", True),
            exec_user_stream_enable=_b("EXEC_USER_STREAM_ENABLE", True),
            exec_maker_tp_enable=_b("EXEC_MAKER_TP_ENABLE", True),
            exec_journal_sql_enable=_b("EXEC_JOURNAL_SQL_ENABLE", True),
            trade_dq_hard_veto_enable=_b("TRADE_DQ_HARD_VETO_ENABLE", True),
            trade_risk_engine_v2_enable=_b("TRADE_RISK_ENGINE_V2_ENABLE", _b("RISK_ENGINE_V2_ENABLE", True)),
            strategy_shadow_only=_b("STRATEGY_SHADOW_ONLY", False),
            exec_force_safety_first=_b("EXEC_FORCE_SAFETY_FIRST", False),
            exec_degraded_mode_force_safety_first=_b("EXEC_DEGRADED_MODE_FORCE_SAFETY_FIRST", True),
            exec_degraded_mode_disable_maker=_b("EXEC_DEGRADED_MODE_DISABLE_MAKER", True),
        )

    def as_dict(self) -> Dict[str, Any]:
        return {
            "exec_algo_canonical_v2": self.exec_algo_canonical_v2,
            "exec_reconcile_enable": self.exec_reconcile_enable,
            "exec_user_stream_enable": self.exec_user_stream_enable,
            "exec_maker_tp_enable": self.exec_maker_tp_enable,
            "exec_journal_sql_enable": self.exec_journal_sql_enable,
            "trade_dq_hard_veto_enable": self.trade_dq_hard_veto_enable,
            "trade_risk_engine_v2_enable": self.trade_risk_engine_v2_enable,
            "strategy_shadow_only": self.strategy_shadow_only,
            "exec_force_safety_first": self.exec_force_safety_first,
            "exec_degraded_mode_force_safety_first": self.exec_degraded_mode_force_safety_first,
            "exec_degraded_mode_disable_maker": self.exec_degraded_mode_disable_maker,
        }

    def maker_allowed(self, *, infra_degraded: bool = False) -> bool:
        if self.exec_force_safety_first:
            return False
        if not self.exec_maker_tp_enable:
            return False
        if infra_degraded and self.exec_degraded_mode_disable_maker:
            return False
        return True

    def safety_forced(self, *, infra_degraded: bool = False) -> bool:
        if self.exec_force_safety_first:
            return True
        if infra_degraded and self.exec_degraded_mode_force_safety_first:
            return True
        return False
