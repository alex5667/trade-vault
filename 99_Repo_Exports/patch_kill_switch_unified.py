import os
import glob
import re

target_dir = "/home/alex/front/trade/scanner_infra/python-worker/orderflow_services"
files = glob.glob(os.path.join(target_dir, "*.py"))

count = 0
for file in files:
    with open(file, "r") as f:
        content = f.read()

    modified = False
    
    # Pattern 1: Files with `policy_from_hash` where `controller_policy` is read
    if "controller_policy = policy_from_hash" in content:
        # We find the line calling policy_from_hash
        target_line = r"(controller_policy\s*=\s*policy_from_hash\([^)]*\)\s*\n)"
        replacement = r"\1                    try:\n                        exec_kill = await r.get('trade:exec_kill_switch')\n                        if exec_kill and exec_kill.decode().strip() == '1':\n                            controller_policy['kill_switch'] = 1\n                    except: pass\n"
        if not "trade:exec_kill_switch" in content:
            content = re.sub(target_line, replacement, content)
            modified = True

    # Pattern 2: operator_routing..._v2_14 style files (direct check in loop)
    if "is_killed = kill_val and kill_val.decode(" in content:
        # It already reads a kill switch, let's override the result with trade:exec_kill_switch
        if not "trade:exec_kill_switch" in content:
            target_line2 = r"(is_killed\s*=\s*(.*?)\n)"
            replacement2 = r"\1                    try:\n                        unified_ks = await r.get('trade:exec_kill_switch')\n                        if unified_ks and unified_ks.decode().strip() == '1':\n                            is_killed = True\n                    except: pass\n"
            content = re.sub(target_line2, replacement2, content)
            modified = True

    if modified:
        with open(file, "w") as f:
            f.write(content)
        count += 1
        print(f"Patched {file}")

print(f"Total patched: {count}")
