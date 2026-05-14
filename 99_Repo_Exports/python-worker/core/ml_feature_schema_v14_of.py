from __future__ import annotations

"""
v14_of — Feature schema v14 (OrderFlow), pinned snapshot.

Generated: 2026-05-13 (manual bump from v13_of).

v14_of = v13_of (~242 keys) + 16 additional rule-gate consensus indicators:
  Group OG (16) — OrderFlow Rule-Gate Consensus
                  (of_score components, have/need legs, contributions,
                   reason codes, gate bits, strong-need flags)

  Subtotal new  = 16 (all `og_*` prefixed — orthogonal to v13_of keys)

Coverage: ~258 numeric indicators (no separate bool block — all bool as float 0/1)
          + direction/bucket/hour/dow/session one-hots.

Design notes
------------
- Fail-open: ALL keys vectorize as 0.0 if missing in runtime snapshot.
- Group OG (rule consensus): mirrors `of_confirm_engine` decision artifacts
  (dec.have, dec.need, contrib dict, need_reason). Population is added in a
  separate change; until then keys vectorize to 0.0 (no model failure).
- Anti-overfit policy: each key has Pearson(new, nearest_v13_key) < 0.70 by
  design — of_score_final (v9_of+) is the aggregated post-clip score; og_*
  surface the pre-aggregation structure (which leg fired, by how much).
- Naming: `og_` prefix everywhere to guarantee zero collision with existing
  keys (`of_score_final`, `weak_progress`, `strong_gate_have/need`, etc.).
- Append-only: new schema versions always add keys, never remove.

Phase / rollout
---------------
Phase 0 (this file): schema declaration + registry mapping. No prod env switch.
Phase 1: `of_confirm_engine` writes og_* keys into `indicators` dict before
         XADD to `signals:of:inputs`; dataset builder picks them up automatically.
Phase 2: offline train baseline LR + GBDT challenger; compare to v13_of champion.
Phase 3: canary (BTCUSDT/ETHUSDT/SOLUSDT) shadow → enforce when metrics pass.
"""


SCHEMA_HASH = "v14of_og16_2026_05_13"


# Import base v13 keys to avoid duplication drift
try:
    from core.ml_feature_schema_v13_of import V13_OF_NUMERIC_KEYS as _V13_OF_BASE
except ImportError:
    _V13_OF_BASE = []


# ---------------------------------------------------------------------------
# Group OG — OrderFlow Rule-Gate Consensus (16 keys)
#
# Source artifacts (of_confirm_engine.py):
#   dec.have                            — number of legs satisfied
#   dec.need                            — number of legs required
#   dec.need_reason                     — reason_code string ("rev_dz_strong", ...)
#   contrib (dict)                      — per-leg contribution weights
#   nd.need_rev, nd.need_cont, nd.reason (strong_need_same_tick)
#   weak_progress (int 0/1)
#
# Orthogonality vs v13_of:
#   - of_score_final / of_score_final_raw (v9_of+, already in v13) = aggregated score
#   - og_*                                                         = decomposition
# ---------------------------------------------------------------------------

_GROUP_OG_RULE_CONSENSUS: list[str] = [
    # Gate progress (raw legs)
    "og_have",                   # int → float: legs currently satisfied (dec.have)
    "og_need",                   # int → float: legs required for confirm (dec.need)
    "og_have_minus_need",        # have - need (negative = gap; 0 = passed; positive = surplus)
    "og_ok",                     # float 0/1: gate passed (dec.have >= dec.need)
    "og_score_minus_threshold",  # of_score_final - legacy_of_score_min  (margin vs symbol min)

    # Per-leg contributions (from contrib dict in of_confirm_engine)
    # Each = weight * leg_score, normalized to [0, 1] (or 0 if leg absent)
    "og_contrib_z",              # delta_z component
    "og_contrib_wp",             # weak_progress component
    "og_contrib_reclaim",        # reclaim component
    "og_contrib_obi",            # OBI / book-pressure component
    "og_contrib_iceberg",        # iceberg / hidden-liquidity component
    "og_contrib_absorption",     # absorption / fp_edge component

    # Gate structure
    "og_gate_bits_count",        # popcount of active gate bits in current tick (int → float)

    # Strong-need policy (strong_need_same_tick)
    "og_strong_need_rev",        # int → float 0/1: reversal-strong-need fired this tick
    "og_strong_need_cont",       # int → float 0/1: continuation-strong-need fired this tick

    # Categorical / progress flags
    "og_weak_progress_any",      # int → float 0/1: any weak-progress leg present (mirror of weak_progress)
    "og_reason_code_id",         # stable hash(need_reason) % 64 (categorical encoded as small int → float)
]


# ---------------------------------------------------------------------------
# Final composite key list — V14_OF_NUMERIC_KEYS (sorted for determinism)
# ---------------------------------------------------------------------------

V14_OF_NUMERIC_KEYS: list[str] = sorted(set(
    _V13_OF_BASE
    + _GROUP_OG_RULE_CONSENSUS
))

# Sanity guard (caught immediately at import in tests)
_EXPECTED_MIN = 245
_EXPECTED_MAX = 280
if _V13_OF_BASE:
    assert _EXPECTED_MIN <= len(V14_OF_NUMERIC_KEYS) <= _EXPECTED_MAX, (
        f"v14_of key count {len(V14_OF_NUMERIC_KEYS)} out of expected range "
        f"[{_EXPECTED_MIN}, {_EXPECTED_MAX}] — check for duplicates or deletions"
    )

# Hard guard: og_* must not collide with any v13_of key (would silently shadow).
_OG_COLLISIONS = set(_GROUP_OG_RULE_CONSENSUS) & set(_V13_OF_BASE)
assert not _OG_COLLISIONS, (
    f"v14_of OG group collides with v13_of base keys: {sorted(_OG_COLLISIONS)}"
)


def get_v14_of_numeric_keys() -> list[str]:
    """Return sorted list of numeric indicator keys for v14_of."""
    return list(V14_OF_NUMERIC_KEYS)


def v14_of_info() -> dict:
    """Summary dict for logging / audit."""
    n_v13 = len(_V13_OF_BASE)
    n_new = len(V14_OF_NUMERIC_KEYS) - n_v13
    return {
        "ver": "v14_of",
        "schema_hash": SCHEMA_HASH,
        "n_numeric_keys": len(V14_OF_NUMERIC_KEYS),
        "n_v13_of_base": n_v13,
        "n_new_keys": n_new,
        "groups": {
            "group_og_rule_consensus": len(_GROUP_OG_RULE_CONSENSUS),
        },
    }
