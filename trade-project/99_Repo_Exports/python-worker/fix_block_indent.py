with open("services/crypto_orderflow_service.py", "r") as f:
    lines = f.readlines()

in_block = False
for i in range(len(lines)):
    # Start block right after `if not tick: continue`
    if "if not tick:" in lines[i] and "processed_ok = True" in lines[i+1] and "continue" in lines[i+2]:
        in_block = True
        continue # skip the 'continue'
        
    if in_block:
        # Stop block when we reach the `except Exception as exc:` which is at 24 spaces now
        if "except Exception as exc:" in lines[i] and "# noqa: BLE001" in lines[i]:
            in_block = False
            break
            
        # If it's the `continue` line, skip
        if "continue" in lines[max(0, i-2):i] and "if not tick:" in lines[max(0, i-3):i]:
            pass
        elif lines[i].strip():
            # Add 4 spaces
            lines[i] = "    " + lines[i]

with open("services/crypto_orderflow_service.py", "w") as f:
    f.writelines(lines)
print("Block indented.")
