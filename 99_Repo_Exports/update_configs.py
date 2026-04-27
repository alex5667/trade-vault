import re

files_to_update = [
    "/home/alex/front/trade/scanner_infra/python-worker/core/instrument_config.py",
    "/home/alex/front/trade/scanner_infra/python-worker/confidence_calculation/instrument_config.py",
    "/home/alex/front/trade/scanner_infra/python-worker/reference/confidence_calculation/instrument_config.py",
    "/home/alex/front/trade/scanner_infra/reference/confidence_calculation/instrument_config.py"
]

core_path = "/home/alex/front/trade/scanner_infra/core/instrument_config.py"

with open(core_path, "r") as f:
    core_lines = f.readlines()

def get_block(start_marker, end_marker=None):
    inside = False
    block = []
    for line in core_lines:
        if start_marker in line:
            inside = True
        if inside:
            block.append(line)
        if end_marker and end_marker in line and inside:
            break
        if inside and not end_marker and line.strip() == ")":
            break
    return "".join(block)

symbols_to_add = ["TONUSDT", "ONDOUSDT", "OPUSDT", "HBARUSDT", "SEIUSDT", "RENDERUSDT", "AAVEUSDT", "TRBUSDT"]

for symbol in symbols_to_add:
    config_block = get_block(f"{symbol}_CONFIG =")
    specs_block = get_block(f"{symbol}_SPECS =")

for fpath in set(files_to_update):
    try:
        with open(fpath, "r") as f:
            content = f.read()
    except:
        continue
    
    # 1. Add blocks before INSTRUMENT_CONFIGS
    if "TONUSDT_CONFIG" not in content:
        blocks_to_insert = ""
        for symbol in symbols_to_add:
            blocks_to_insert += get_block(f"{symbol}_CONFIG =") + "\n\n"
            blocks_to_insert += get_block(f"{symbol}_SPECS =") + "\n\n"
        
        content = content.replace("INSTRUMENT_CONFIGS:", blocks_to_insert + "INSTRUMENT_CONFIGS:")
        
        # 2. Add to INSTRUMENT_CONFIGS
        for symbol in symbols_to_add:
            content = content.replace(
                '"FETUSDT": FETUSDT_CONFIG,',
                f'"FETUSDT": FETUSDT_CONFIG,\n    "{symbol}": {symbol}_CONFIG,'
            )
        
        # 3. Add to INSTRUMENT_SPECS
        for symbol in symbols_to_add:
            content = content.replace(
                '"FETUSDT": FETUSDT_SPECS,',
                f'"FETUSDT": FETUSDT_SPECS,\n    "{symbol}": {symbol}_SPECS,'
            )
            
        with open(fpath, "w") as f:
            f.write(content)

print("Updates applied")
