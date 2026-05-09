"""Wrapper for canonical implementation.

Keep single source of truth in top-level `orderflow_services`.
"""

from orderflow_services.of_gate_archiver_exporter_v1 import run

if __name__ == "__main__":
    raise SystemExit(run())
