"""
Unified Stream Codec for payload extraction and alias normalization.
Provides a central mechanism for parsing stream payloads and bridging deprecated field names.
"""
from typing import Any

_ALIAS_MAP = {
    "vol_ratio_z": "vol_ratio",
    "vol_ratio_bps": "vol_ratio",
    "obi": "obi_avg",
    "spread": "spread_bps",
    "l3_spread": "spread_bps",
    "l3_spread_bps": "spread_bps",
    "queue_pressure_bid": "l3_queue_pressure_bid",
    "queue_pressure_ask": "l3_queue_pressure_ask",
}

class UnifiedStreamCodec:
    """Centralized codec for extracting fields from stream payloads with alias normalization."""

    def __init__(self, alias_map: dict[str, str] | None = None):
        self._aliases = alias_map or _ALIAS_MAP

    def normalize_payload(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Returns a new payload dict where alias keys are mapped to their canonical forms."""
        normalized = {}
        for k, v in payload.items():
            canonical = self._aliases.get(k, k)
            # If canonical already exists, don't overwrite it with an alias value
            if canonical not in normalized:
                normalized[canonical] = v
            elif k == canonical:
                # If we encounter the true canonical key later, overwrite the alias value
                normalized[canonical] = v
        return normalized

    def extract_field(self, payload: dict[str, Any], field_name: str, default: Any = None) -> Any:
        """Extract a field, trying the canonical name first, then known aliases."""
        if field_name in payload:
            return payload[field_name]

        # Reverse lookup for aliases
        for alias, canonical in self._aliases.items():
            if canonical == field_name and alias in payload:
                return payload[alias]

        return default

    @classmethod
    def get_default_codec(cls) -> "UnifiedStreamCodec":
        return cls()
