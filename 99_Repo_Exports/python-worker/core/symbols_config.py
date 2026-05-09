import logging
import os
from typing import Any

import yaml

logger = logging.getLogger(__name__)

class SymbolsConfig:
    def __init__(self, config_path: str = "/app/config/symbols.yml"):
        self.config_path = config_path
        self._config: dict[str, Any] = {}
        self.universe: list[str] = []
        self.shards: dict[str, list[str]] = {}
        self.binance_allowlist: list[str] = []
        self.canary_symbols: list[str] = []

    def load(self):
        if not os.path.exists(self.config_path):
            logger.warning(f"Symbols config file not found at {self.config_path}")
            return

        with open(self.config_path) as f:
            self._config = yaml.safe_load(f) or {}

        self.universe = self._config.get("universe", [])
        self.shards = self._config.get("shards", {})
        self.binance_allowlist = self._config.get("execution", {}).get("binance_allowlist", [])
        self.canary_symbols = self._config.get("metrics", {}).get("canary_symbols", [])

        self.validate()

    def validate(self):
        # 1. Shards must be disjoint
        seen_symbols: set[str] = set()
        for shard_name, symbols in self.shards.items():
            for sym in symbols:
                if sym in seen_symbols:
                    raise ValueError(f"Symbol {sym} is duplicated in shards! Found in {shard_name}")
                seen_symbols.add(sym)

        # 2. Shards subset of universe
        universe_set = set(self.universe)
        if not seen_symbols.issubset(universe_set):
            invalid = seen_symbols - universe_set
            raise ValueError(f"Symbols in shards not in universe: {invalid}")

        # 3. Binace allowlist coverage (for this specific system, we assume all trading symbols must be in allowlist unless explicitly analysis only)
        # Assuming for now everything must be in allowlist or universe
        allowlist_set = set(self.binance_allowlist)
        if not seen_symbols.issubset(allowlist_set):
            invalid = seen_symbols - allowlist_set
            logger.warning(f"Symbols in shards not in binance_allowlist (may be analysis only): {invalid}")

    @classmethod
    def get_symbols_for_shard(cls, shard_env_var: str) -> list[str]:
        """
        Backward compatibility layer.
        Priority:
        1. SYMBOLS env variable (if set for the specific container)
        2. CRYPTO_SYMBOLS_OVERRIDE
        3. Fallback to parsing symbols.yml based on shard_env_var mapping
        """
        # Highest priority: local explicit SYMBOLS override
        local_symbols = os.environ.get("SYMBOLS")
        if local_symbols:
            return [s.strip() for s in local_symbols.split(",") if s.strip()]

        # Priority 2: CRYPTO_SYMBOLS_OVERRIDE
        override_symbols = os.environ.get("CRYPTO_SYMBOLS_OVERRIDE")
        if override_symbols:
            return [s.strip() for s in override_symbols.split(",") if s.strip()]

        # Priority 3: yaml config
        config = cls()
        try:
            config.load()
            # Map CRYPTO_SYMBOLS_SHARD_1 -> orderflow_1 etc
            shard_key = shard_env_var.lower().replace("crypto_symbols_shard_", "orderflow_")
            if shard_key in config.shards:
                return config.shards[shard_key]

            # If not found in shards, return universe
            if config.universe:
                return config.universe
        except Exception as e:
            logger.error(f"Failed to load symbols.yml: {e}")

        # Final fallback
        fallback = os.environ.get(shard_env_var)
        if fallback:
             return [s.strip() for s in fallback.split(",") if s.strip()]

        return []

