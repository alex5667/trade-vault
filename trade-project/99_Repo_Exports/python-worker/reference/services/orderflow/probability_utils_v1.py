from __future__ import annotations

from typing import Any

ProbSource = str


def extract_prob_with_source(decision: dict[str, Any]) -> tuple[float | None, ProbSource]:
    """Extract a probability-like value from decision['ml'].

    Priority:
      1) p_edge (gate probability)
      2) p (generic probability)
      3) score (legacy, only if already in [0,1])

    IMPORTANT: p_min is a THRESHOLD and MUST NOT be treated as a probability.

    Returns:
      (p, source) where source in {'p_edge','p','score','none'}.
    """
    ml = decision.get("ml") if isinstance(decision.get("ml"), dict) else {}
    if not isinstance(ml, dict) or not ml:
        return None, "none"

    for k in ("p_edge", "p", "score"):
        if k not in ml:
            continue
        try:
            p = float(ml.get(k))
        except Exception:
            continue
        if 0.0 <= p <= 1.0:
            return p, k

    return None, "none"


def extract_prob(decision: dict[str, Any]) -> float | None:
    """Backward-compatible wrapper returning only probability."""
    p, _ = extract_prob_with_source(decision)
    return p
