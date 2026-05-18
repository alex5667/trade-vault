import os
import re

import yaml

CONFIG_PATH = os.path.join(os.path.dirname(__file__), "../../config/symbols.yml")
OUTPUT_PATH = os.path.join(os.path.dirname(__file__), "../../config/generated/symbols.env")
ROOT_ENV_PATH = os.path.join(os.path.dirname(__file__), "../../.env")

_SECTION_START = "# --- GENERATED SYMBOLS (auto-updated by make symbols-env) ---"
_SECTION_END   = "# --- END GENERATED SYMBOLS ---"


def _inject_root_env(symbol_vars: dict[str, str]) -> None:
    """Write/replace the GENERATED SYMBOLS section in the root .env file.

    Docker Compose reads .env for ${VAR} substitution at parse time.
    env_file only injects into the container, not for compose YAML substitution.
    """
    if not os.path.exists(ROOT_ENV_PATH):
        return

    with open(ROOT_ENV_PATH) as f:
        content = f.read()

    block_lines = [_SECTION_START]
    for k, v in symbol_vars.items():
        block_lines.append(f"{k}={v}")
    block_lines.append(_SECTION_END)
    new_block = "\n".join(block_lines)

    pattern = re.compile(
        rf"{re.escape(_SECTION_START)}.*?{re.escape(_SECTION_END)}",
        re.DOTALL,
    )
    if pattern.search(content):
        updated = pattern.sub(new_block, content)
    else:
        sep = "\n" if content.endswith("\n") else "\n\n"
        updated = content + sep + new_block + "\n"

    with open(ROOT_ENV_PATH, "w") as f:
        f.write(updated)

    print(f"Updated {ROOT_ENV_PATH} (GENERATED SYMBOLS section)")


def render():
    if not os.path.exists(CONFIG_PATH):
        print(f"Error: {CONFIG_PATH} not found.")
        return

    with open(CONFIG_PATH) as f:
        config = yaml.safe_load(f) or {}

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)

    active = config.get("active") or config.get("universe", [])
    active_str = ",".join(active)

    monitoring = config.get("monitoring", {})
    liq_str = ",".join(monitoring.get("liquidation_symbols", active))
    latency_str = ",".join(monitoring.get("latency_symbols", active))
    canary_str = ",".join(config.get("metrics", {}).get("canary_symbols", []))
    allowlist_str = ",".join(config.get("execution", {}).get("binance_allowlist", []))

    with open(OUTPUT_PATH, "w") as f:
        f.write("# GENERATED FILE - DO NOT EDIT DIRECTLY\n")
        f.write("# Source: config/symbols.yml\n\n")

        f.write(f"TRADE_SYMBOLS_UNIVERSE={active_str}\n")
        f.write(f"CRYPTO_SYMBOLS={active_str}\n")
        f.write(f"FUTURES_SYMBOLS={active_str}\n")
        f.write(f"REQUIRED_SYMBOLS={active_str}\n\n")

        shards = config.get("shards", {})
        for shard_name, symbols in shards.items():
            env_key = f"TRADE_SYMBOLS_SHARD_{shard_name.upper().replace('ORDERFLOW_', '')}"
            legacy_key = f"CRYPTO_SYMBOLS_SHARD_{shard_name.upper().replace('ORDERFLOW_', '')}"
            val = ",".join(symbols)
            f.write(f"{env_key}={val}\n")
            f.write(f"{legacy_key}={val}\n")

        f.write("\n")
        f.write(f"TRADE_BINANCE_ALLOWLIST={allowlist_str}\n")
        f.write(f"BINANCE_SYMBOL_ALLOWLIST={allowlist_str}\n")

        f.write("\n")
        f.write(f"LIQ_SYMBOLS={liq_str}\n")
        f.write(f"LATENCY_SYMBOLS={latency_str}\n")

        f.write("\n")
        f.write(f"TRADE_CANARY_SYMBOLS={canary_str}\n")
        f.write(f"CANARY_SYMBOLS={canary_str}\n")

    print(f"Successfully generated {OUTPUT_PATH}")

    # Mirror the compose-substitution vars into root .env so that
    # ${CRYPTO_SYMBOLS} etc. resolve at docker compose parse time.
    _inject_root_env({
        "CRYPTO_SYMBOLS":          active_str,
        "LIQ_SYMBOLS":             liq_str,
        "LATENCY_SYMBOLS":         latency_str,
        "CANARY_SYMBOLS":          canary_str,
        "TRADE_BINANCE_ALLOWLIST": allowlist_str,
    })


if __name__ == "__main__":
    render()
