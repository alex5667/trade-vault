"""Pytest configuration for services/* tests.

We run unit tests in a minimal environment where the repo is not installed as a
package. This file makes local imports deterministic.

Important ordering:
- Repo root must come before tick_flow_full in sys.path, because tick_flow_full
  also contains a `services/` directory and would otherwise shadow the real SoT
  `services/` package.
- tick_flow_full is added to support top-level imports like `common.*`.
"""

import sys
from pathlib import Path

_HERE = Path(__file__).resolve()
_REPO_ROOT = None
for p in _HERE.parents:
    if (p / "services").is_dir() and (p / "tick_flow_full").is_dir():
        _REPO_ROOT = p
        break

if _REPO_ROOT is None:
    _REPO_ROOT = _HERE.parents[3]

# 1) Repo root (for `services.*` imports)
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

# 2) tick_flow_full (for top-level `common.*` imports) — append to avoid shadowing
_tick_flow_full = _REPO_ROOT / "tick_flow_full"
if _tick_flow_full.is_dir() and str(_tick_flow_full) not in sys.path:
    sys.path.append(str(_tick_flow_full))
