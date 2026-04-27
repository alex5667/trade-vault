# python-worker/handlers/regime_gate.py
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class RegimeGateCfg:
    # breakout family allowed when score >= min_score
    breakout_min_score: float = 0.0
    extreme_min_score: float = 0.0
    obi_spike_min_score: float = 0.0

    # absorption allowed when score <= max_score
    absorption_max_score: float = 0.0

    # sweep allowed regardless of score
    allow_sweep_any: bool = True


def regime_allows(signal_type: str, regime_score: float, cfg: RegimeGateCfg) -> bool:
    st = (signal_type or "").lower()

    # дефолт: если режима нет, считаем mixed => не режем
    if regime_score is None:
        return True

    if st == "breakout":
        return regime_score >= cfg.breakout_min_score
    if st == "extreme":
        return regime_score >= cfg.extreme_min_score
    if st == "obi_spike":
        return regime_score >= cfg.obi_spike_min_score
    if st == "absorption":
        return regime_score <= cfg.absorption_max_score
    if st == "sweep":
        return bool(cfg.allow_sweep_any)

    # неизвестное — лучше не резать автоматически
    return True


def regime_reject_reason(signal_type: str, regime_score: float, cfg: RegimeGateCfg) -> str:
    st = (signal_type or "").lower()
    if st in ("breakout", "extreme", "obi_spike"):
        return f"{st} требует regime_score>={getattr(cfg, st + '_min_score', 0.0):.3f}, но score={regime_score:.3f}"
    if st == "absorption":
        return f"absorption требует regime_score<={cfg.absorption_max_score:.3f}, но score={regime_score:.3f}"
    if st == "sweep":
        return "sweep запрещён allow_sweep_any=False"
    return "unknown"