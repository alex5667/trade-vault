import re

with open("python-worker/core/instrument_config.py", "r") as f:
    content = f.read()

# We need to find `if sym in TONUSDT...` and replace it with `if sym in INSTRUMENT_CONFIGS:`
idx = content.find("def get_config(symbol")
if idx != -1:
    before = content[:idx]
    after = content[idx:]
    
    # In after, replace the broken if sym in ... with `if sym in INSTRUMENT_CONFIGS:`
    # The broken part starts with `if sym in TONUSDT_CONFIG = OrderFlowConfig(` 
    # and ends right before `        preset = INSTRUMENT_CONFIGS[sym]`
    
    start_bad = after.find("if sym in TONUSDT_CONFIG = OrderFlowConfig(")
    end_bad = after.find("        preset = INSTRUMENT_CONFIGS[sym]")
    
    if start_bad != -1 and end_bad != -1:
        after_fixed = after[:start_bad] + "if sym in INSTRUMENT_CONFIGS:\n" + after[end_bad:]
        
        with open("python-worker/core/instrument_config.py", "w") as f:
            f.write(before + after_fixed)
        print("Fixed get_config.")
    else:
        print("Could not find bounds of the broken code.", start_bad, end_bad)
else:
    print("Could not find get_config.")

