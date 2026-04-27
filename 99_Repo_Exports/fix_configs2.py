import sys

files_to_update = [
    "/home/alex/front/trade/scanner_infra/python-worker/core/instrument_config.py",
    "/home/alex/front/trade/scanner_infra/python-worker/confidence_calculation/instrument_config.py",
    "/home/alex/front/trade/scanner_infra/python-worker/reference/confidence_calculation/instrument_config.py"
]

symbols_to_add = ["TONUSDT", "ONDOUSDT", "OPUSDT", "HBARUSDT", "SEIUSDT", "RENDERUSDT", "AAVEUSDT", "TRBUSDT", "NEARUSDT", "FETUSDT"]

for fpath in files_to_update:
    try:
        with open(fpath, "r") as f:
            content = f.read()
            
        # Add to configs
        config_inserts = "".join([f'    "{sym}": {sym}_CONFIG,\n' for sym in symbols_to_add])
        if '"TONUSDT": TONUSDT_CONFIG,' not in content:
            content = content.replace('"XAGUSDT": XAGUSDT_CONFIG,', f'"XAGUSDT": XAGUSDT_CONFIG,\n{config_inserts}')
            
        # Add to specs
        spec_inserts = "".join([f'    "{sym}": {sym}_SPECS,\n' for sym in symbols_to_add])
        if '"TONUSDT": TONUSDT_SPECS,' not in content:
            content = content.replace('"XAGUSDT": XAGUSDT_SPECS,', f'"XAGUSDT": XAGUSDT_SPECS,\n{spec_inserts}')
            
        with open(fpath, "w") as f:
            f.write(content)
        print("Fixed dicts in", fpath)
    except Exception as e:
        print("Skipping", fpath, e)

