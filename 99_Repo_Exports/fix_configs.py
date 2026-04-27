import re

files_to_update = [
    "/home/alex/front/trade/scanner_infra/python-worker/core/instrument_config.py",
    "/home/alex/front/trade/scanner_infra/python-worker/confidence_calculation/instrument_config.py",
    "/home/alex/front/trade/scanner_infra/python-worker/reference/confidence_calculation/instrument_config.py"
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

symbols_to_add = ["TONUSDT", "ONDOUSDT", "OPUSDT", "HBARUSDT", "SEIUSDT", "RENDERUSDT", "AAVEUSDT", "TRBUSDT", "NEARUSDT", "FETUSDT"]

for fpath in set(files_to_update):
    try:
        with open(fpath, "r") as f:
            content = f.read()
    except Exception as e:
        print("Skipping", fpath, e)
        continue
    
    # Extract blocks that aren't already present
    blocks_to_insert = ""
    for symbol in symbols_to_add:
        if f"{symbol}_CONFIG =" not in content:
            blocks_to_insert += get_block(f"{symbol}_CONFIG =") + "\n\n"
        if f"{symbol}_SPECS =" not in content:
            blocks_to_insert += get_block(f"{symbol}_SPECS =") + "\n\n"
    
    if blocks_to_insert:
        content = content.replace("INSTRUMENT_CONFIGS: Dict", blocks_to_insert + "INSTRUMENT_CONFIGS: Dict")

    # update dicts
    for symbol in symbols_to_add:
        if f'"{symbol}": {symbol}_CONFIG,' not in content:
            # find last entry in INSTRUMENT_CONFIGS
            content = re.sub(
                r'("NATGASUSDT": NATGASUSDT_CONFIG,?\n|    "XAGUSDT": XAGUSDT_CONFIG,?\n)',
                lambda m: m.group(1) + f'    "{symbol}": {symbol}_CONFIG,\n',
                content
            )
        if f'"{symbol}": {symbol}_SPECS,' not in content:
            # find last entry in INSTRUMENT_SPECS
            content = re.sub(
                r'("NATGASUSDT": NATGASUSDT_SPECS,?\n|    "XAGUSDT": XAGUSDT_SPECS,?\n)',
                lambda m: m.group(1) + f'    "{symbol}": {symbol}_SPECS,\n',
                content
            )

    with open(fpath, "w") as f:
        f.write(content)
        print("Updated", fpath)

