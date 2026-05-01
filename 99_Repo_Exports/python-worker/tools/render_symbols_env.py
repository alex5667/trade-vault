import os
import yaml

CONFIG_PATH = os.path.join(os.path.dirname(__file__), "../../config/symbols.yml")
OUTPUT_PATH = os.path.join(os.path.dirname(__file__), "../../config/generated/symbols.env")

def render():
    if not os.path.exists(CONFIG_PATH):
        print(f"Error: {CONFIG_PATH} not found.")
        return

    with open(CONFIG_PATH, "r") as f:
        config = yaml.safe_load(f) or {}

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)

    with open(OUTPUT_PATH, "w") as f:
        f.write("# GENERATED FILE - DO NOT EDIT DIRECTLY\n")
        f.write(f"# Source: config/symbols.yml\n\n")

        universe = ",".join(config.get("universe", []))
        f.write(f"TRADE_SYMBOLS_UNIVERSE={universe}\n")
        
        # Legacy compat
        f.write(f"CRYPTO_SYMBOLS={universe}\n\n")

        shards = config.get("shards", {})
        for shard_name, symbols in shards.items():
            env_key = f"TRADE_SYMBOLS_SHARD_{shard_name.upper().replace('ORDERFLOW_', '')}"
            legacy_key = f"CRYPTO_SYMBOLS_SHARD_{shard_name.upper().replace('ORDERFLOW_', '')}"
            val = ",".join(symbols)
            f.write(f"{env_key}={val}\n")
            f.write(f"{legacy_key}={val}\n")
            
        f.write("\n")
        allowlist = ",".join(config.get("execution", {}).get("binance_allowlist", []))
        f.write(f"TRADE_BINANCE_ALLOWLIST={allowlist}\n")
        f.write(f"BINANCE_SYMBOL_ALLOWLIST={allowlist}\n")
        
        f.write("\n")
        canary = ",".join(config.get("metrics", {}).get("canary_symbols", []))
        f.write(f"TRADE_CANARY_SYMBOLS={canary}\n")
        f.write(f"CANARY_SYMBOLS={canary}\n")

    print(f"Successfully generated {OUTPUT_PATH}")

if __name__ == "__main__":
    render()
