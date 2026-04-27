import glob
import re

files = glob.glob("/home/alex/front/trade/scanner_infra/python-worker/orderflow_services/*.py")
count = 0
for file in files:
    with open(file, "r") as f:
        content = f.read()

    # Pattern to match: any identifier = policy_from_hash(...) followed by newline
    # Since it's Python, the line could be slightly complex, so let's match the start of the line up to newline.
    target_pattern = r"(^[\s]*(controller_policy|base_policy|policy)\s*=\s*(?:[a-zA-Z_0-9]+_)?policy_from_hash.*?\n)"
    
    modified = False
    
    if content.count("trade:exec_kill_switch") == 0:
        if re.search(target_pattern, content, flags=re.MULTILINE):
            # For each match, we extract the variable name and append the kill switch check
            def replacer(match):
                full_line = match.group(1)
                var_name = match.group(2)
                # match the leading spaces
                leading_spaces = full_line[:len(full_line) - len(full_line.lstrip())]
                ret = full_line + leading_spaces + "try:\n"
                ret += leading_spaces + "    exec_kill = await r.get('trade:exec_kill_switch')\n"
                ret += leading_spaces + "    if exec_kill and exec_kill.decode().strip() == '1':\n"
                ret += leading_spaces + f"        {var_name}['kill_switch'] = 1\n"
                ret += leading_spaces + "except: pass\n"
                return ret
                
            content = re.sub(target_pattern, replacer, content, flags=re.MULTILINE)
            modified = True
                
    if modified:
        with open(file, "w") as f:
            f.write(content)
        count += 1
        print("Patched", file)

print("Total extra patched:", count)
