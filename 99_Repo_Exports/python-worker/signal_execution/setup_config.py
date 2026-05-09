"""
Setup configurations for signal execution plans.
Centralized configuration for different symbols and setup types.
"""

from dataclasses import dataclass
from typing import Any

from .models import SymbolSetupConfig


@dataclass
class SetupConfig:
    """
    Extended setup configuration with risk management parameters.,
    """
    name: str
    max_risk_r: float
    tp_r: float
    sl_r: float
    # Additional raw config for custom parameters
    raw: dict[str, Any]


# Centralized setup configurations
# Key: (symbol, setup_type)
SETUP_CONFIGS: dict[tuple[str, str], SymbolSetupConfig] = {
    "breakout_R1": SymbolSetupConfig(
        symbol="",
        setup_type="breakout_R1",
        expiry_bars=5,
        score_buckets=(0.4, 0.7, 0.85),
        risk_multipliers=(0.5, 1.0, 1.5, 2.0),
    ),
    "fade_PDH": SymbolSetupConfig(
        symbol="",
        setup_type="fade_PDH",
        expiry_bars=3,
        score_buckets=(0.4, 0.7, 0.85),
        risk_multipliers=(0.5, 1.0, 1.5, 2.0),
    ),
    ("BTCUSDT", "breakout_R1"): SymbolSetupConfig(
        symbol="BTCUSDT",
        setup_type="breakout_R1",
        expiry_bars=4,
        score_buckets=(0.4, 0.7, 0.85),
        risk_multipliers=(0.5, 1.0, 1.5, 2.0),
    )
    # Add more configurations as needed...
}


class ExecutionSetupRepository:
    """
    Repository for execution setup configurations.
    In production, this could be backed by a database.
    """

    def get_by_name(self, symbol: str, setup_name: str) -> SymbolSetupConfig:
        """
        Get setup config by symbol and setup name.
        Returns default config if not found.
        """
        key = (symbol, setup_name)
        if key in SETUP_CONFIGS:
            return SETUP_CONFIGS[key]

        # Return default config
        return SymbolSetupConfig(
            symbol=symbol,
            setup_type=setup_name,
            expiry_bars=3,
            score_buckets=(0.4, 0.7, 0.85),
            risk_multipliers=(0.5, 1.0, 1.5, 2.0),
        )
