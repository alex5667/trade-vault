from typing import Any

try:
    import numpy as np
except ImportError:
    np = None

def compute_market_breadth_score(signal_direction: int, recent_signals: list[dict[str, Any]]) -> float:
    """Ratio of assets trending in same direction as signal (0-1).
    signal_direction: 1 (BUY) or -1 (SELL).
    recent_signals: last N cross-asset signals (dict with 'direction' and 'symbol').
    """
    if not recent_signals or signal_direction == 0:
        return 0.5

    same_dir_count = 0
    unique_symbols = set()

    for sig in recent_signals[-50:]:  # Look at recent market breadth
        direct = sig.get("direction", 0)
        sym = sig.get("symbol", "")
        if sym:
            unique_symbols.add(sym)
            if direct == signal_direction:
                same_dir_count += 1

    if not unique_symbols:
        return 0.5

    return float(same_dir_count / len(recent_signals[-50:]))


def compute_crypto_fear_greed(liquidation_usd: float, open_interest_delta: float) -> float:
    """Fear/Greed index proxy (liquidation_usd vs open_interest_delta).
    Normalized to 0.0 (extreme fear) to 1.0 (extreme greed).
    """
    if liquidation_usd == 0 and open_interest_delta == 0:
        return 0.5

    greed_score = 0.5

    # High liquidations usually mean fear/dump, but rising OI means greed/leverage
    if liquidation_usd > 1_000_000 and open_interest_delta < 0:
        greed_score -= 0.3
    elif open_interest_delta > 1_000_000 and liquidation_usd < 100_000:
        greed_score += 0.3

    return min(max(greed_score, 0.0), 1.0)


def compute_alt_season_index(alt_btc_beta_1h: float, prev_beta_1h: float) -> float:
    """Alts vs BTC dominance momentum (alt_btc_beta_1h derivative).
    >0 means alts outperforming BTC (alt season).
    """
    if alt_btc_beta_1h == 0.0 or prev_beta_1h == 0.0:
        return 0.0

    return float(alt_btc_beta_1h - prev_beta_1h)


def compute_cross_asset_vol_ratio(btc_perp_implied_vol: float, symbol_atr_bps: float) -> float:
    """Implied vol of BTC perp / symbol ATR — relative vol regime.
    btc_perp_implied_vol: e.g. DVOL index.
    """
    if symbol_atr_bps <= 0 or btc_perp_implied_vol <= 0:
        return 0.0

    return float(btc_perp_implied_vol / symbol_atr_bps)
