import os

file_path = "/home/alex/front/trade/scanner_infra/python-worker/services/of_timers_worker.py"
with open(file_path, "r") as f:
    content = f.read()

# Insert the function before main
func_code = """
def run_of_gate_dlq_triage() -> bool:
    \"\"\"Run OF-Gate DLQ Triage (P84) (Hourly).\"\"\"
    if os.getenv("ENABLE_OF_GATE_DLQ_TRIAGE_TIMER", "0") != "1":
        return True
    
    args = [
        "triage",
        "--limit", os.getenv("OF_GATE_DLQ_TRIAGE_LIMIT", "5000"),
        "--notify"
    ]
    return run_tool("orderflow_services.of_gate_dlq_fixed_replay_p84", args, timeout=600)

"""

if "run_of_gate_dlq_triage" not in content:
    content = content.replace("def main() -> None:", func_code + "def main() -> None:")

# Insert the schedule inside main loop
schedule_code = """
            # P84: Hourly:42 OF Gate DLQ Triage
            if minute >= 42 and minute < 43:
                last = last_run.get("of_gate_dlq_triage", 0)
                if now.timestamp() - last > 3500:
                    run_of_gate_dlq_triage()
                    last_run["of_gate_dlq_triage"] = now.timestamp()
"""

if "Hourly:42 OF Gate DLQ Triage" not in content:
    content = content.replace("            time.sleep(30)", schedule_code + "            time.sleep(30)")

with open(file_path, "w") as f:
    f.write(content)

# Also update the symlink if it exists
file_path_2 = "/home/alex/front/trade/scanner_infra/python-worker/tick_flow_full/services/of_timers_worker.py"
if os.path.exists(file_path_2) and not os.path.islink(file_path_2):
    with open(file_path_2, "w") as f:
        f.write(content)

print("Timer updated.")
