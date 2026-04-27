from __future__ import annotations

import os


def strict_enabled() -> bool:
    """
    Global strict profile toggle (docker-compose).

    Supported toggles:
      1) GATE_PROFILE=strict|aggressive|hard  (highest priority)
      2) GATES_STRICT=1                       (fallback)

    Rationale:
      - One switch to harden multiple gates coherently.
      - Profile is explicit (human-readable) and still supports boolean legacy flag.
    """
    try:
        p = str(os.getenv("GATE_PROFILE", "") or "").strip().lower()
        if p in {"strict", "aggressive", "hard"}:
            return True
        if p in {"default", "normal", "soft"}:
            return False
    except Exception:
        pass
    try:
        v = str(os.getenv("GATES_STRICT", "") or "").strip()
        return v in {"1", "true", "True", "yes", "on"}
    except Exception:
        return False
