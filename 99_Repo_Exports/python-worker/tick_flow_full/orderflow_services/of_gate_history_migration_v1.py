"""Wrapper for canonical implementation.

Keep single source of truth in top-level `orderflow_services`.
"""

from orderflow_services.of_gate_history_migration_v1 import main

if __name__ == "__main__":
    raise SystemExit(main())
