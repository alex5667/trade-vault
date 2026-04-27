from __future__ import annotations

import os
import subprocess
import sys

def main() -> None:
    env = dict(os.environ)
    cmd = [
        sys.executable,
        "-m",
        "orderflow_services.route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_replay_gate_runner_v3_57_4",
    ]
    raise SystemExit(subprocess.call(cmd, env=env))

if __name__ == "__main__":
    main()
