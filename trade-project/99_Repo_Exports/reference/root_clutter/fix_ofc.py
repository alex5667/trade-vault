import sys

with open("/home/alex/front/trade/scanner_infra/python-worker/services/orderflow_strategy.py", "r") as f:
    lines = f.readlines()

sre_start = -1
sre_end = -1
dyn_start = -1
dyn_end = -1

for i, line in enumerate(lines):
    if 'indicators["of_confirm"] = ofc.to_dict()' in line:
        if sre_start == -1: sre_start = i
    if 'except Exception:' in line and 'pass' in lines[i+1] and sre_start != -1 and dyn_start == -1:
        # Check if the next line is # Use dec directly
        if '# Use dec directly from build() instead of overwriting with None' in lines[i+3]:
            sre_end = i + 2
    if '# Use dec directly from build() instead of overwriting with None' in line:
        dyn_start = i
    if 'indicators["of_confirm_soft_reason"] = str(ev.get("soft_reason", ""))' in line:
        dyn_end = i + 1

print(f"SRE Block: {sre_start} to {sre_end}")
print(f"Dyn Block: {dyn_start} to {dyn_end}")

if sre_start != -1 and dyn_end != -1:
    sre_block = lines[sre_start:sre_end]
    dyn_block = lines[dyn_start:dyn_end]
    
    # We want to insert updates to ofc.reason inside dyn_block
    new_dyn_block = []
    for line in dyn_block:
        if 'dec.reason = f"{dec.reason}|liq_relax"' in line:
            new_dyn_block.append(line)
            new_dyn_block.append(line.replace('dec.reason =', 'ofc.reason =').replace('f"{dec.reason}', 'f"{ofc.reason}'))
        elif 'eff_need = int(dec.need) + need_bump' in line:
            new_dyn_block.append(line)
            new_dyn_block.append(line.replace('eff_need = int(dec.need) + need_bump', 'ofc.need = eff_need\n                    dec.need = eff_need'))
        elif 'ofc.ok = False # Sync object' in line:
            new_dyn_block.append(line)
            indent = line.split('ofc.ok')[0]
            new_dyn_block.append(indent + "dec.ok = False\n")
            new_dyn_block.append(indent + "if \"need_bump_veto\" not in str(ofc.reason):\n")
            new_dyn_block.append(indent + "    ofc.reason = f\"need_bump_veto({dec.have}/{eff_need})|{getattr(ofc, 'reason', '')}\"\n")
            new_dyn_block.append(indent + "    dec.reason = ofc.reason\n")
        else:
            new_dyn_block.append(line)

    new_lines = lines[:sre_start] + new_dyn_block + ["\n"] + sre_block + lines[dyn_end:]
    
    with open("/home/alex/front/trade/scanner_infra/python-worker/services/orderflow_strategy.py", "w") as f:
        f.writelines(new_lines)
    print("Done writing.")
else:
    print("Failed to find boundaries.")
