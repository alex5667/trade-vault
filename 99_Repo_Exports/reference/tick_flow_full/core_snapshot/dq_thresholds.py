"""Mirror of core_snapshot.dq_thresholds (keep in sync).

Why a mirror:
  The repo contains two runtime trees (SoT + tick_flow_full mirror). Some
  deployments run the mirror package directly. Keeping the same resolver and
  defaults in both trees preserves train==serve reproducibility.
"""

# Re-export exact implementation from the SoT module when available.
try:
    from core_snapshot.dq_thresholds import *  # noqa: F401,F403
except Exception:  # pragma: no cover
    # Fallback for isolated mirror runs: import local copy.
    from tick_flow_full.core_snapshot._dq_thresholds_impl import *  # type: ignore  # noqa: F401,F403

