"""NewsPriorGate — безопасная интеграция news priors в Stage-5 gates (trade).

Цель:
- Не тянуть Redis/IO в критический путь.
- Работать синхронно (validate()) и только с тем, что уже есть в ctx.

Ожидается, что где-то ДО pre_publish_gates:
- либо отдельный consumer подписан на `stream:signals_news` и обновляет in-memory cache,
- либо периодический poller делает GET `news:prior:<SYMBOL>` и обновляет cache,
- а затем pipeline делает: ctx.news_prior = cache.get(symbol).

Профили (ENV: NEWS_PRIOR_GATE_PROFILE):
- soft:     только аннотация q.flags["news_prior"]. Никаких ужесточений.
- tighten:  повышает ctx.min_confidence_override + (best-effort) ужесточает risk caps.
- hard:     как tighten + veto в узких условиях:
            low credibility + pump-pattern + conflict-with-leaders.

ВАЖНО (безопасность):
- Gate НИКОГДА не блокирует event-loop.
- Gate НЕ делает предположений о структуре ctx/q кроме минимально необходимых.
- Любые изменения risk-caps выполняются только если поля явно существуют.

ENV (минимум):
- NEWS_PRIOR_GATE_PROFILE=soft|tighten|hard
- NEWS_PRIOR_MIN_IMPACT=0.40
- NEWS_PRIOR_TIGHTEN_MIN_CONF=75.0
- NEWS_PRIOR_TIGHTEN_RISK_MULT=0.70
- NEWS_PRIOR_HARD_MAX_CRED=0.35
- NEWS_PRIOR_HARD_REQUIRE_CONFLICT=1
- NEWS_PRIOR_HARD_PUMP_FLAG=pump_suspect
"""

from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import os
import time
from typing import Any, Dict, Optional


def _now_ms() -> int:
    return get_ny_time_millis()


class NewsPriorGate:
    """Stage-5 gate: consumes ctx.news_prior (dict) and produces flags / overrides."""

    def __init__(self) -> None:
        self.profile = os.getenv("NEWS_PRIOR_GATE_PROFILE", "tighten").lower()  # soft|tighten|hard
        self.min_impact = float(os.getenv("NEWS_PRIOR_MIN_IMPACT", "0.40"))

        # tighten behavior
        self.tighten_min_conf = float(os.getenv("NEWS_PRIOR_TIGHTEN_MIN_CONF", "75.0"))
        self.tighten_risk_mult = float(os.getenv("NEWS_PRIOR_TIGHTEN_RISK_MULT", "0.70"))

        # hard behavior
        self.hard_cred_max = float(os.getenv("NEWS_PRIOR_HARD_MAX_CRED", "0.35"))
        self.hard_require_conflict = int(os.getenv("NEWS_PRIOR_HARD_REQUIRE_CONFLICT", "1")) == 1
        self.hard_pump_flag = os.getenv("NEWS_PRIOR_HARD_PUMP_FLAG", "pump_suspect")

    def validate(self, ctx: Any, cand: Any, q: Any) -> None:
        prior = getattr(ctx, "news_prior", None)
        if not prior:
            return

        now_ms = _now_ms()
        expires_ms = _safe_int(prior.get("expires_ms"))
        if expires_ms and expires_ms < now_ms:
            # Stale prior → annotate only.
            _set_flag(q, {
                "stale": True,
                "expires_ms": expires_ms,
            })
            return

        impact = _safe_float(prior.get("impact"), 0.0)
        credibility = _safe_float(prior.get("credibility"), 0.5)
        event_type = str(prior.get("event_type", "unknown"))
        bias_up = _safe_float(prior.get("bias_up"), None)
        bias_down = _safe_float(prior.get("bias_down"), None)

        actions: Dict[str, Any] = {"profile": self.profile}

        # Always annotate.
        _set_flag(q, {
            "impact": impact,
            "credibility": credibility,
            "event_type": event_type,
            "expires_ms": expires_ms,
            "confidence": _safe_float(prior.get("confidence"), 0.0),
            "bias": {"up": bias_up, "down": bias_down},
            "actions": actions,
        })

        # Below impact threshold → only annotation.
        if impact < self.min_impact:
            actions["impact_below_min"] = True
            return

        # soft profile: annotate only.
        if self.profile == "soft":
            actions["soft_no_overrides"] = True
            return

        # tighten/hard: raise min_confidence_override.
        if self.profile in ("tighten", "hard"):
            _raise_min_confidence(ctx, self.tighten_min_conf, actions)
            _tighten_risk_caps(ctx, self.tighten_risk_mult, actions)

        # hard: veto in narrow conditions.
        if self.profile == "hard":
            flags = prior.get("flags") or {}
            pump = _is_true(flags.get(self.hard_pump_flag)) or _is_true(_flag_get(q, self.hard_pump_flag)) or _is_true(getattr(ctx, self.hard_pump_flag, False))

            # "conflict with leaders" is expected to be produced by your SMT/Leader gates.
            # We support several common flag names, fail-safe to False.
            conflict = (
                _is_true(flags.get("leaders_conflict"))
                or _is_true(_flag_get(q, "leaders_conflict"))
                or _is_true(_flag_get(q, "leader_conflict"))
                or _is_true(_flag_get(q, "smt_leader_conflict"))
                or _is_true(getattr(ctx, "leaders_conflict", False))
            )

            actions["hard_pump"] = pump
            actions["hard_conflict"] = conflict

            if credibility <= self.hard_cred_max and pump and (conflict or not self.hard_require_conflict):
                # Veto is deliberately narrow to avoid false positives.
                _veto(q, f"news_prior_hard_veto:{self.hard_pump_flag}:cred={credibility:.2f}")


# ─────────────────────────────────────────────────────────────────────────────
# Helpers (robust / fail-safe)


def _safe_float(v: Any, default: Optional[float]) -> Optional[float]:
    if v is None:
        return default
    try:
        return float(v)
    except Exception:
        return default


def _safe_int(v: Any) -> int:
    if v is None:
        return 0
    try:
        return int(v)
    except Exception:
        return 0


def _is_true(v: Any) -> bool:
    return bool(v is True or (isinstance(v, (int, float)) and v != 0) or (isinstance(v, str) and v.lower() in ("1", "true", "yes", "y")))


def _set_flag(q: Any, payload: Dict[str, Any]) -> None:
    flags = getattr(q, "flags", None)
    if isinstance(flags, dict):
        flags["news_prior"] = payload


def _flag_get(q: Any, key: str) -> Any:
    flags = getattr(q, "flags", None)
    if isinstance(flags, dict):
        return flags.get(key)
    return None


def _veto(q: Any, reason: str) -> None:
    # Prefer q.veto_with(reason) (your pattern). Fall back to setting a field.
    if hasattr(q, "veto_with"):
        q.veto_with(reason)
    else:
        setattr(q, "veto_reason", reason)


def _raise_min_confidence(ctx: Any, target: float, actions: Dict[str, Any]) -> None:
    current = _safe_float(getattr(ctx, "min_confidence_override", 0.0), 0.0) or 0.0
    new = max(current, target)
    setattr(ctx, "min_confidence_override", new)
    actions["min_confidence_override"] = {"prev": current, "new": new}


def _tighten_risk_caps(ctx: Any, mult: float, actions: Dict[str, Any]) -> None:
    """Best-effort risk caps tightening.

    Безопасный подход:
    - если ctx.risk_caps — dict: модифицируем только известные numeric keys;
    - иначе выставляем ctx.risk_caps_mult (для downstream policy), но не ломаем структуру.

    Это позволяет включить функционал постепенно, не меняя сразу все компоненты.
    """

    if mult <= 0 or mult >= 1.0:
        actions["risk_caps_mult_skipped"] = True
        return

    caps = getattr(ctx, "risk_caps", None)

    keys = (
        "max_notional_usd",
        "max_position_usd",
        "max_order_usd",
        "max_risk_usd",
        "max_leverage",
    )

    if isinstance(caps, dict):
        changed = {}
        for k in keys:
            v = caps.get(k)
            if isinstance(v, (int, float)):
                prev = float(v)
                caps[k] = prev * mult
                changed[k] = {"prev": prev, "new": caps[k]}
        if changed:
            actions["risk_caps_tightened"] = {"mult": mult, "changed": changed}
        else:
            actions["risk_caps_tightened"] = {"mult": mult, "changed": {}}
        return

    # Fallback for unknown ctx structure
    cur_mult = _safe_float(getattr(ctx, "risk_caps_mult", 1.0), 1.0) or 1.0
    new_mult = min(cur_mult, mult)
    setattr(ctx, "risk_caps_mult", new_mult)
    actions["risk_caps_mult"] = {"prev": cur_mult, "new": new_mult}
