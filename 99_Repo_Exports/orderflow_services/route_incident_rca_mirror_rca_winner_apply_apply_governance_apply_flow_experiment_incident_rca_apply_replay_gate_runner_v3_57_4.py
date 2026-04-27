from __future__ import annotations

import os
import subprocess
import sys
import time

APP_NAME = "ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_replay_gate_runner_v3_57_4"

WINDOW_MIN = int(os.getenv("ML_ROUTE_INCIDENT_RCA_APPLY_REPLAY_GATE_RUNNER_WINDOW_MIN", "60"))
LAG_MIN = int(os.getenv("ML_ROUTE_INCIDENT_RCA_APPLY_REPLAY_GATE_RUNNER_LAG_MIN", "15"))
END_TS_MS = int(os.getenv("ML_ROUTE_INCIDENT_RCA_APPLY_REPLAY_GATE_RUNNER_END_TS_MS", "0"))

def floor_minute_ms(ts_ms: int, minute_step: int) -> int:
    step_ms = minute_step * 60 * 1000
    return (ts_ms // step_ms) * step_ms

def run(cmd: list[str], env: dict[str, str]) -> int:
    return subprocess.call(cmd, env=env)

def main() -> None:
    now_ts_ms = int(time.time() * 1000)
    end_ts_ms = END_TS_MS if END_TS_MS > 0 else floor_minute_ms(now_ts_ms - LAG_MIN * 60 * 1000, WINDOW_MIN)
    start_ts_ms = end_ts_ms - WINDOW_MIN * 60 * 1000

    env = dict(os.environ)
    env["ML_ROUTE_INCIDENT_RCA_APPLY_REPLAY_WINDOW_START_TS_MS"] = str(start_ts_ms)
    env["ML_ROUTE_INCIDENT_RCA_APPLY_REPLAY_WINDOW_END_TS_MS"] = str(end_ts_ms)
    env["ML_ROUTE_INCIDENT_RCA_APPLY_REPLAY_GATE_WINDOW_START_TS_MS"] = str(start_ts_ms)
    env["ML_ROUTE_INCIDENT_RCA_APPLY_REPLAY_GATE_WINDOW_END_TS_MS"] = str(end_ts_ms)

    validator_cmd = [
        sys.executable,
        "-m",
        "orderflow_services.route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_replay_validator_v3_57_3",
    ]
    gate_cmd = [
        sys.executable,
        "-m",
        "orderflow_services.route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_replay_gate_v3_57_4",
    ]

    rc = run(validator_cmd, env)
    if rc != 0:
        raise SystemExit(rc)
    raise SystemExit(run(gate_cmd, env))

if __name__ == "__main__":
    main()
