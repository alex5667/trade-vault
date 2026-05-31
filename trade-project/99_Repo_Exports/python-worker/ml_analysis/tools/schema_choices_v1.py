from __future__ import annotations

"""Shared schema-version choices for ML analysis tools.

Why this exists
  Several CLI tools accept --schema-ver/--feature_schema_ver to pin a deterministic
  Feature Registry version (train==serve). Historically, each tool kept its own
  hard-coded list which drifts easily.

This module centralizes the allowlist and provides a small normalizer.

Notes
  - The Feature Registry itself supports aliases like v7 -> v7_of, v7_stable -> v7_of_stable.
  - We keep the allowlist explicit (argparse choices) to fail fast on typos.
"""



# Keep this list append-only.
# If you add a new schema in tick_flow_full/core/feature_registry.py,
# extend this list in the same commit.
_SCHEMA_VERSIONS: list[str] = [
    # legacy
    "v2",
    "v3",
    "v4",
    "v4_of",
    "v5",
    "v5_of",
    "v5_of_stable",
    "v5_stable",
    "v6",
    "v6_of",
    "v6_of_stable",
    "v6_stable",
    # v7
    "v7",
    "v7_of",
    "v7_of_stable",
    "v7_stable",
    # v9_of — pinned snapshot from infer_feature_cols() 2026-03-03 (128 numeric keys)
    "v9",
    "v9_of",
    # v10_of — v9_of + Group1 stream-proven + Group2A-E new indicators (165 numeric keys)
    "v10",
    "v10_of",
    # v11_of — v10_of + 28 new regression keys (Groups A-F)
    "v11",
    "v11_of",
    # v12_of (214 keys)
    "v12",
    "v12_of",
    # v13_of — v12_of (214) + GroupNA-NX (28) = 242 numeric keys
    "v13",
    "v13_of",
    # v14_of — v13_of (242) + Group OG (16) rule-gate + Group OE (81) external/execution = 359 (incl Phase 8.1 composites)
    "v14",
    "v14_of",
    # v15_of — v14_of (349) + 182 keys (Phase 8.2/8.3/8.4/8.5/P1/P2/P3/4.x: Hawkes/VPIN,
    # cross-venue, CoinGecko/CoinPaprika/CMC, Deribit term structure, DefiLlama,
    # macro calendar, derivatives base, PIT priors, breadth segments) = 531 numeric keys
    "v15",
    "v15_of",
]


def schema_choices(*, include_empty: bool) -> list[str]:
    """Return argparse choices list.

    Args:
        include_empty: prepend "" (empty string) to allow "unset".
    """
    out = list(_SCHEMA_VERSIONS)
    if include_empty:
        out.insert(0, "")
    return out


def normalize_schema_ver(ver: str) -> str:
    """Normalize common aliases so tools behave consistently.

    This does NOT validate; validation is done by argparse choices.
    """
    v = (ver or "").strip()
    if not v:
        return ""
    vv = v.lower().replace("-", "_")
    # stable aliases
    if vv in ("v7stable", "v7_stable"):
        return "v7_of_stable"
    if vv in ("v6stable", "v6_stable"):
        return "v6_of_stable"
    if vv in ("v5stable", "v5_stable"):
        return "v5_of_stable"
    # base aliases
    if vv == "v7":
        return "v7_of"
    if vv == "v6":
        return "v6_of"
    if vv == "v5":
        return "v5_of"
    if vv == "v4":
        return "v4_of"
    if vv == "v9":
        return "v9_of"
    if vv == "v10":
        return "v10_of"
    if vv == "v11":
        return "v11_of"
    if vv == "v12":
        return "v12_of"
    if vv == "v13":
        return "v13_of"
    if vv == "v14":
        return "v14_of"
    if vv == "v15":
        return "v15_of"
    return v
