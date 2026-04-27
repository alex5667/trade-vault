import re

def fix_file(path, pattern_str, insert_lines):
    with open(path, 'r') as f:
        lines = f.readlines()
        
    out = []
    for line in lines:
        if pattern_str in line and 'dynamic_cfg' not in line:
            indent = line[:len(line) - len(line.lstrip())]
            for ins in insert_lines:
                out.append(indent + ins + '\n')
        out.append(line)
        
    with open(path, 'w') as f:
        f.writelines(out)

fix_file('tests/test_of_gate_metrics.py', "runtime.config = ", ["runtime.dynamic_cfg = {}", "runtime.heartbeat_counter = 0", "runtime.tick_count = 1"])
fix_file('tests/test_dn_gate_metrics.py', 'runtime.config["delta_tier_min"] = 1', ["runtime.dynamic_cfg = {}", "runtime.heartbeat_counter = 0", "runtime.tick_count = 1"])
