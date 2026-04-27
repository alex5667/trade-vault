from __future__ import annotations

import os
import subprocess
import sys
import time

APP_NAME = "ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_replay_validator_runner_v3_57_3"

WINDOW_MIN = int(os.getenv("ML_ROUTE_INCIDENT_RCA_APPLY_REPLAY_RUNNER_WINDOW_MIN", "60"))
WINDOW_END_TS_MS = int(os.getenv("ML_ROUTE_INCIDENT_RCA_APPLY_REPLAY_RUNNER_WINDOW_END_TS_MS", "0"))

def main() -> None:
    end_ts_ms = WINDOW_END_TS_MS if WINDOW_END_TS_MS > 0 else int(time.time() * 1000)
    start_ts_ms = end_ts_ms - WINDOW_MIN * 60 * 1000

    env = dict(os.environ)
    env["ML_ROUTE_INCIDENT_RCA_APPLY_REPLAY_WINDOW_START_TS_MS"] = str(start_ts_ms)
    env["ML_ROUTE_INCIDENT_RCA_APPLY_REPLAY_WINDOW_END_TS_MS"] = str(end_ts_ms)

    cmd = [
        sys.executable,
        "-m",
        "orderflow_services.route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_replay_validator_v3_57_3",
    ]
    raise SystemExit(subprocess.call(cmd, env=env))

if __name__ == "__main__":
    main()
