"""
Feature bucketing for signal quality assessment.

This module provides functions to create feature buckets for clustering signals
based on their characteristics (delta_spike_z, obi, weak_progress, atr_quantile).
"""


def _bin(x: float | None, edges: tuple[float, ...]) -> str:
    """
    Bin a value into discrete categories based on edges.

    Args:
        x: Value to bin (can be None)
        edges: Tuple of bin edges (sorted, immutable)

    Returns:
        String representation of the bin
    """
    if x is None:
        return "na"

    for e in edges:
        if x < e:
            return f"<{e}"

    return f">={edges[-1]}"


def make_feature_bucket(
    *,
    delta_spike_z: float | None,
    obi: float | None,
    weak_progress: float | None,
    atr_quantile: float | None,
) -> str:
    """
    Create a feature bucket string for signal clustering.

    Args:
        delta_spike_z: Delta spike Z-score
        obi: Order Book Imbalance metric
        weak_progress: Weak progress indicator (range vs ATR)
        atr_quantile: ATR quantile for volatility assessment

    Returns:
        Feature bucket string in format: "dz:bin|obi:bin|wp:bin|atr:bin"
    """
    # Bin edges can be configured via ENV or made adaptive.
    # These are reasonable defaults for trading signals.
    # Tuples are immutable and slightly faster to iterate than lists.
    dz_bin = _bin(delta_spike_z, (0.5, 1.0, 1.5, 2.0, 3.0))
    obi_bin = _bin(obi, (0.5, 1.0, 1.5, 2.0))
    # weak_progress: lower values are better (less weak progress)
    wp_bin = _bin(weak_progress, (0.15, 0.3, 0.5))
    # atr_quantile: higher values indicate stronger volatility
    atr_bin = _bin(atr_quantile, (0.3, 0.7, 0.9))

    return f"dz:{dz_bin}|obi:{obi_bin}|wp:{wp_bin}|atr:{atr_bin}"


def get_bucket_quality_description(bucket: str) -> str:
    """
    Get human-readable description of a feature bucket.

    Args:
        bucket: Feature bucket string

    Returns:
        Human-readable description
    """
    parts = bucket.split("|")
    descriptions = []

    for part in parts:
        key, value = part.split(":", 1)
        if key == "dz":
            if value == "na":
                desc = "Delta Z: unknown"
            elif value.startswith("<"):
                desc = f"Delta Z: weak ({value})"
            else:
                desc = f"Delta Z: strong ({value})"
        elif key == "obi":
            if value == "na":
                desc = "OBI: unknown"
            elif value.startswith("<"):
                desc = f"OBI: weak ({value})"
            else:
                desc = f"OBI: strong ({value})"
        elif key == "wp":
            if value == "na":
                desc = "Progress: unknown"
            elif value.startswith("<0.15"):
                desc = "Progress: very weak"
            elif value.startswith("<0.3"):
                desc = "Progress: weak"
            else:
                desc = "Progress: strong"
        elif key == "atr":
            if value == "na":
                desc = "Volatility: unknown"
            elif value.startswith("<0.3"):
                desc = "Volatility: low"
            elif value.startswith("<0.7"):
                desc = "Volatility: medium"
            elif value.startswith("<0.9"):
                desc = "Volatility: high"
            else:
                desc = "Volatility: extreme"
        else:
            desc = f"{key}: {value}"

        descriptions.append(desc)

    return " | ".join(descriptions)
