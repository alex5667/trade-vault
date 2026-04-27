import logging

logger = logging.getLogger(__name__)

class CVDTracker:
    """
    Cumulative Volume Delta Tracker.
    Aggregates buy vs sell volumes to detect absorption and divergence against price action.
    """
    def __init__(self):
        self.cvd = 0.0
        self.last_price = 0.0
        self.peaks = []
        self.troughs = []

    def update(self, price: float, taker_buy_vol: float, taker_sell_vol: float) -> dict:
        """
        Updates the tracking with a new bucket or tick value.
        delta = buy_volume - sell_volume
        """
        delta = taker_buy_vol - taker_sell_vol
        self.cvd += delta
        self.last_price = price
        
        # Basic state returned for further processing
        return {
            "cvd": self.cvd,
            "delta": delta,
            "buy_vol": taker_buy_vol,
            "sell_vol": taker_sell_vol
        }

    def reset(self):
        self.cvd = 0.0
