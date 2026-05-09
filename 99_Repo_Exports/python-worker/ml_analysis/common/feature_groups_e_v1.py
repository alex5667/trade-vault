from __future__ import annotations

"""Feature-group definitions for ablation/denylist (E block).

Goal
  - Provide deterministic groups of *E-block* features (Hawkes/VPIN/limit-add)
    for offline ablation and auto-denylist suggestions.

Scope
  - Offline ML tooling only.
  - No runtime dependencies.

Notes
  - Feature keys are expected as raw registry keys (no prefixes):
      hawkes_taker_buy_lam, vpin_tox_z, limit_add_total_rate_ema
  - If you pass full registry names ("n:xxx" / "b:xxx"), use normalize_feature_key().
"""


from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass


def normalize_feature_key(name: str) -> str:
    """Normalize feature name into raw key.

    Accepts:
      - registry style: "n:xxx", "b:xxx"
      - column style:   "n_xxx", "b_xxx"
      - plain raw key:  "xxx"
      - f-style:        "f_xxx" (treated as num)
    """
    s = (name or "").strip()
    if s.startswith("n:") or s.startswith("b:"):
        return s[2:]
    if s.startswith("n_") or s.startswith("b_"):
        return s[2:]
    if s.startswith("f_"):
        return s[2:]
    return s


@dataclass(frozen=True)
class FeatureGroup:
    name: str
    description: str
    matcher: Callable[[str], bool]


def build_e_groups() -> list[FeatureGroup]:
    """Return ordered E-block groups (stable names for reports)."""

    def _starts_any(s: str, prefixes: Sequence[str]) -> bool:
        return any(s.startswith(p) for p in prefixes)

    def _contains_any(s: str, needles: Sequence[str]) -> bool:
        return any(n in s for n in needles)

    return [
        FeatureGroup(
            name="E_vpin",
            description="VPIN-like toxicity (ema/z + related)",
            matcher=lambda k: _starts_any(k, ("vpin_",)) or _contains_any(k, ("vpin_tox", "toxicity")),
        ),
        FeatureGroup(
            name="E_limit_add",
            description="Limit-add / replenishment rates (added/limit_add_*)",
            matcher=lambda k: _starts_any(k, ("limit_add_", "added_")) or _contains_any(k, ("replenish", "replenishment")),
        ),
        FeatureGroup(
            name="E_hawkes_split",
            description="Hawkes-like split intensities (buy/sell/cancel bid/ask/limit-add)",
            matcher=lambda k: k
            in (
                "hawkes_taker_buy_lam",
                "hawkes_taker_sell_lam",
                "hawkes_cancel_bid_lam",
                "hawkes_cancel_ask_lam",
                "hawkes_limit_add_lam",
                "hawkes_limit_add_bid_lam",
                "hawkes_limit_add_ask_lam",
            ),
        ),
        FeatureGroup(
            name="E_hawkes_legacy",
            description="Legacy Hawkes aggregates + internal states (S_*)",
            matcher=lambda k: _starts_any(k, ("hawkes_S_",))
            or k in ("hawkes_taker_lam", "hawkes_cancel_lam", "hawkes_churn_lam"),
        ),
        FeatureGroup(
            name="E_lambda_alias",
            description="Aliases from EMAs (lambda_trade_buy/sell/limit_add)",
            matcher=lambda k: _starts_any(k, ("lambda_trade_", "lambda_limit_", "lambda_limit_add")),
        ),
    ]


def group_features(feature_names: Iterable[str], groups: Sequence[FeatureGroup]) -> dict[str, set[str]]:
    """Return mapping group_name -> set(raw_feature_keys) for provided names."""
    keys = [normalize_feature_key(x) for x in feature_names]
    out: dict[str, set[str]] = {g.name: set() for g in groups}
    for k in keys:
        for g in groups:
            try:
                if g.matcher(k):
                    out[g.name].add(k)
            except Exception:
                continue
    return out


def flatten_groups(m: dict[str, set[str]]) -> set[str]:
    out: set[str] = set()
    for s in m.values():
        out |= set(s)
    return out
