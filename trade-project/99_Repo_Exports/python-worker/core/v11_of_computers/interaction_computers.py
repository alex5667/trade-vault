def compute_kyle_x_vpin(kyle_lambda: float, vpin_rolling: float) -> float:
    """kyle_lambda × VPIN — toxic flow with price impact."""
    return float(kyle_lambda * vpin_rolling)


def compute_momentum_x_vol_ratio(momentum_10s: float, vol_ratio: float) -> float:
    """momentum_10s × vol_ratio — momentum quality in regime.
    High momentum + high vol ratio = true breakout.
    High momentum + low vol ratio = noise.
    """
    return float(momentum_10s * vol_ratio)


def compute_pressure_x_obi(pressure: float, obi: float) -> float:
    """pressure × OBI — aggressive flow + book structure alignment.
    Both positive -> strong buy pressure into weak ask book.
    """
    return float(pressure * obi)


def compute_liq_score_x_spread(liq_score: float, spread_bps: float) -> float:
    """liq_score × spread_bps — risk-adjusted liquidity gate.
    spread_bps behaves as a penalty/cost on liquidity.
    """
    return float(liq_score * spread_bps)


def compute_confidence_x_of_score(confidence: float, of_score_final: float) -> float:
    """confidence × of_score_final — double-gate signal strength.
    Combines primary ML edge with rule-based order flow consensus.
    """
    return float(confidence * of_score_final)

