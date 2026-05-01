# -*- coding: utf-8 -*-
from __future__ import annotations
"""bucket2_v1.py

Bucket2 — дополнительная (не ломающая текущий bucket:) категоризация режима/сценария,
предназначенная для *новых* моделей. Старый bucket:trend/range/other остаётся
источником истины для существующих моделей и правил.

Зачем:
  - bucket: (legacy) слишком грубый, часто смешивает разные классы поведения.
  - bucket2 вводит 3 orthogonal “world-practice” категории:
      * breakout   — импульс/выход из диапазона (range expansion / momentum)
      * reversal   — разворот/mean-reversion (sweep/reclaim/reversal markers)
      * high_var   — высоковолатильные режимы/шоки (vol_shock/news proxy, HIGH_VAR)

Важно:
  - bucket2 *не заменяет* bucket:. Он добавляется как отдельный префикс `bucket2:`
    и может использоваться только новыми моделями через feature_registry.
  - Если bucket2 невозможно вывести надёжно, возвращается пустая строка (=> one-hot = 0).

Determinism:
  - Функция использует только входные строки/скаляры, без времени/случайности.
  - Логика intentionally conservative: лучше "не классифицировать", чем ошибиться.
"""


from typing import Any, Dict, Optional


def _norm_scenario(s: str) -> str:
    """Normalize scenario-like strings.

    We follow the same rules as ml_confirm_gate._scenario_norm():
      - take left part before '|', ' ', ':', '@'
      - lowercase
    """
    s0 = (s or "").strip().lower()
    if "|" in s0:
        s0 = s0.split("|", 1)[0].strip()
    if " " in s0:
        s0 = s0.split(" ", 1)[0].strip()
    if ":" in s0:
        s0 = s0.split(":", 1)[0].strip()
    if "@" in s0:
        s0 = s0.split("@", 1)[0].strip()
    return s0


def derive_bucket2_label(
    scenario_v4: str,
    *,
    indicators: Optional[Dict[str, Any]] = None,
    evidence: Optional[Dict[str, Any]] = None,
) -> str:
    """Derive bucket2 label: breakout|reversal|high_var|"".

    Args:
        scenario_v4: scenario string (usually scenario_v4 / scenario id).
        indicators:  dict of computed indicators (may contain exec_regime_bucket,
                     momentum flags, realized_vol, etc.). Optional.
        evidence:    evidence dict from OFConfirm (may contain sweep/reclaim/etc.). Optional.

    Returns:
        "high_var" / "reversal" / "breakout" / "" (unknown).
    """
    sv = _norm_scenario(str(scenario_v4 or ""))
    ind = indicators or {}
    ev = evidence or {}

    # ---- 1) HIGH_VAR (highest precedence)
    # Strong explicit markers in scenario id.
    if "vol_shock" in sv or "news" in sv or "high_var" in sv or "highvar" in sv:
        return "high_var"

    # Runtime bucket (exec regime) already exists and is robust.
    try:
        b = str(ind.get("exec_regime_bucket") or "").strip().upper()
        if b in ("HIGH_VAR", "EXTREME", "HIGH_VOL", "HIGH_VOL_LOW_LIQ", "LOW_LIQ"):
            return "high_var"
    except Exception:
        pass

    # Explicit realized-vol / flags (if present).
    try:
        if int(ind.get("flag_high_realized_vol", 0) or 0) == 1:
            return "high_var"
    except Exception:
        pass

    # ---- 2) REVERSAL
    # Scenario markers.
    if "reversal" in sv or "meanrev" in sv or "mean_revert" in sv or "reclaim" in sv:
        return "reversal"
    # Evidence flags (producer-side, more reliable than fuzzy string matching).
    try:
        if int(ev.get("sweep", 0) or 0) == 1:
            return "reversal"
        if int(ev.get("reclaim", 0) or 0) == 1:
            return "reversal"
    except Exception:
        pass

    # ---- 3) BREAKOUT
    if "breakout" in sv:
        return "breakout"
    # Range-expansion / momentum proxies (conservative).
    try:
        if int(ind.get("fp_edge_range_expansion", 0) or 0) == 1:
            return "breakout"
    except Exception:
        pass
    try:
        if int(ind.get("flag_high_mom", 0) or 0) == 1:
            return "breakout"
    except Exception:
        pass

    # No reliable classification.
    return ""
