from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import json
import time
from dataclasses import dataclass
from typing import Any, Dict

from services.abc_router import ABCConfig


def _now_ms() -> int:
    return get_ny_time_millis()


def _b(x: Any) -> bool:
    try:
        return bool(int(x))
    except Exception:
        return bool(x)


def _f(x: Any, d: float) -> float:
    try:
        return float(x)
    except Exception:
        return float(d)


def _i(x: Any, d: int) -> int:
    try:
        return int(x)
    except Exception:
        return int(d)


@dataclass
class ArmPolicy:
    """
    Per-arm policy knobs for your FSM step.
    touch_bp/away_bp/retest_bp are already passed to _fsm_step; we make them per-arm.
    """
    version: int = 0
    shadow: bool = False
    # FSM distances
    touch_bp: float = 10.0
    away_bp: float = 25.0
    retest_bp: float = 10.0
    # Strong-of requirements
    min_of_score: float = 1.0
    # Thin/news extra requirements
    obi_min_sec: float = 1.5


class ABCPolicyLoader:
    """
    Loads:
      - ABC routing config
      - per-arm policy overrides docs
    Poll-based, fail-open.
    """

    def __init__(self, *, cfg_key: str = "cfg:smt_entry:abc:config") -> None:
        self.cfg_key = cfg_key
        self._cfg: ABCConfig = ABCConfig(enabled=False, version=0, salt="smt-entry-v1", poll_ms=2000, splits={"default":{"b":10,"c":10},"thin":{"b":15,"c":15}}, overrides={"A":"cfg:smt_entry:policy:A","B":"cfg:smt_entry:policy:B","C":"cfg:smt_entry:policy:C"})
        self._last_poll_ms: int = 0
        self._arm_cache: Dict[str, ArmPolicy] = {}
        self._arm_ver: Dict[str, int] = {}

    @property
    def cfg(self) -> ABCConfig:
        return self._cfg

    async def poll(self, r) -> None:
        now = _now_ms()
        if (now - self._last_poll_ms) < int(getattr(self._cfg, "poll_ms", 2000) or 2000):
            return
        self._last_poll_ms = now
        # 1) load abc config
        try:
            raw = await r.get(self.cfg_key)
            if raw:
                d = json.loads(raw)
                if isinstance(d, dict):
                    ver = int(d.get("version", 0) or 0)
                    if ver > int(self._cfg.version):
                        splits = d.get("splits", None)
                        overrides = d.get("overrides", None)
                        self._cfg = ABCConfig(
                            enabled=_b(d.get("enabled", 0)),
                            version=ver,
                            salt=str(d.get("salt") or "smt-entry-v1"),
                            poll_ms=_i(d.get("poll_ms", 2000), 2000),
                            splits=splits if isinstance(splits, dict) else (self._cfg.splits or {"default":{"b":10,"c":10},"thin":{"b":15,"c":15}}),
                            overrides=overrides if isinstance(overrides, dict) else (self._cfg.overrides or {"A":"cfg:smt_entry:policy:A","B":"cfg:smt_entry:policy:B","C":"cfg:smt_entry:policy:C"}),
                        )
        except Exception:
            pass

        # 2) load per-arm policy docs (only if enabled; else keep defaults)
        if not bool(self._cfg.enabled):
            return
        for arm in ("A", "B", "C"):
            key = (self._cfg.overrides or {}).get(arm, f"cfg:smt_entry:policy:{arm}")
            try:
                raw = await r.get(key)
                if not raw:
                    continue
                d = json.loads(raw)
                if not isinstance(d, dict):
                    continue
                ver = int(d.get("version", 0) or 0)
                if ver <= int(self._arm_ver.get(arm, 0)):
                    continue
                ov = d.get("overrides", {})
                if not isinstance(ov, dict):
                    ov = {}
                # shadow flag is mandatory for B/C
                shadow = _b(ov.get("ENTRY_POLICY_SHADOW", 1 if arm in ("B","C") else 0))
                pol = ArmPolicy(
                    version=ver,
                    shadow=shadow,
                    touch_bp=_f(ov.get("SMT_TOUCH_BP", 10.0), 10.0),
                    away_bp=_f(ov.get("SMT_AWAY_BP", 25.0), 25.0),
                    retest_bp=_f(ov.get("SMT_RETEST_BP", 10.0), 10.0),
                    min_of_score=_f(ov.get("SMT_ENTRY_MIN_OF_SCORE", 1.0), 1.0),
                    obi_min_sec=_f(ov.get("SMT_ENTRY_OBI_MIN_SEC", 1.5), 1.5),
                )
                self._arm_cache[arm] = pol
                self._arm_ver[arm] = ver
            except Exception:
                continue

    def policy_for(self, arm: str) -> ArmPolicy:
        a = (arm or "A").upper()
        # defaults: A enforce-ish, B/C shadow-ish
        if a not in self._arm_cache:
            return ArmPolicy(
                version=0,
                shadow=(a in ("B","C")),
                touch_bp=10.0,
                away_bp=25.0,
                retest_bp=10.0,
                min_of_score=1.0,
                obi_min_sec=1.5,
            )
        return self._arm_cache[a]
