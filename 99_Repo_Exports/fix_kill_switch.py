import os
import glob
import re

target_dir = "/home/alex/front/trade/scanner_infra/python-worker/orderflow_services"
pattern = r"KILL_SWITCH_KEY(?:_NAME)?\s*=\s*os\.getenv\([\s\S]*?\n\)"

files = glob.glob(os.path.join(target_dir, "*.py"))

replacement = """KILL_SWITCH_KEY = os.getenv(
    "GLOBAL_EXEC_KILL_SWITCH",
    "trade:exec_kill_switch",
)"""

count = 0
for file in files:
    with open(file, "r") as f:
        content = f.read()
    
    # We look for KILL_SWITCH_KEY = os.getenv(..., ...)
    if "KILL_SWITCH_KEY" in content and "os.getenv" in content:
        new_content = re.sub(pattern, replacement, content, flags=re.MULTILINE|re.DOTALL)
        if new_content != content:
            with open(file, "w") as f:
                f.write(new_content)
            count += 1
            print(f"Updated {file}")

print(f"Total files updated for kill switch: {count}")
