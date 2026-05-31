
import numba as nb
import numpy as np

# ==========================================
# Core Numba-Optimized Math Functions
# ==========================================

@nb.njit(cache=True)
def calc_obi(bids: np.ndarray, asks: np.ndarray, depth: int = 5) -> float:
    """Calculates Order Book Imbalance (OBI)."""
    bid_vol = 0.0
    ask_vol = 0.0
    for i in range(min(depth, bids.shape[0])):
        if bids[i, 1] > 0:
            bid_vol += bids[i, 1]
    for i in range(min(depth, asks.shape[0])):
        if asks[i, 1] > 0:
            ask_vol += asks[i, 1]

    total = bid_vol + ask_vol
    if total == 0:
        return 0.0
    return (bid_vol - ask_vol) / total

@nb.njit(cache=True)
def calc_microprice(bids: np.ndarray, asks: np.ndarray) -> float:
    """Calculates volume-weighted microprice."""
    if bids.shape[0] == 0 or asks.shape[0] == 0:
        return 0.0

    best_bid = bids[0, 0]
    best_ask = asks[0, 0]
    best_bid_vol = bids[0, 1]
    best_ask_vol = asks[0, 1]

    total_vol = best_bid_vol + best_ask_vol
    if total_vol == 0:
        return (best_bid + best_ask) / 2.0

    return (best_bid * best_ask_vol + best_ask * best_bid_vol) / total_vol

@nb.njit(cache=True)
def calc_execution_penalty(microprice: float, execution_price: float, side: int) -> float:
    """
    Calculates execution penalty in bps.
    side: 1 for Buy, -1 for Sell
    """
    if microprice == 0:
        return 0.0

    if side == 1:
        diff = execution_price - microprice
    else:
        diff = microprice - execution_price

    return (diff / microprice) * 10000.0

@nb.njit(cache=True)
def calc_queue_eta(queue_pos: float, trade_intensity_per_sec: float) -> float:
    """Estimates ETA to fill based on queue position and trade intensity."""
    if trade_intensity_per_sec <= 0:
        return np.inf
    return queue_pos / trade_intensity_per_sec

@nb.njit(cache=True)
def calc_trade_intensity(trade_volumes: np.ndarray, trade_times: np.ndarray, current_time: float, window_sec: float) -> float:
    """Calculates trade intensity (volume per second) over a rolling window."""
    n = trade_volumes.shape[0]
    if n == 0:
        return 0.0

    vol_sum = 0.0
    for i in range(n):
        if (current_time - trade_times[i]) <= window_sec:
            vol_sum += trade_volumes[i]

    return vol_sum / window_sec if window_sec > 0 else 0.0

# ==========================================
# State Management and Aggregation
# ==========================================

class OrderBookState:
    def __init__(self, max_depth: int = 50):
        self.max_depth = max_depth
        self.bids = np.zeros((0, 2), dtype=np.float64)
        self.asks = np.zeros((0, 2), dtype=np.float64)
        self.timestamp = 0.0
        self.price_volumes: dict[float, float] = {}
        self.price_last_change_time: dict[float, float] = {}

    def update(self, bids: np.ndarray, asks: np.ndarray, timestamp_sec: float):
        self.bids = bids
        self.asks = asks
        self.timestamp = timestamp_sec

        # Check volume changes for bids
        for i in range(bids.shape[0]):
            p = float(bids[i, 0])
            v = float(bids[i, 1])
            if self.price_volumes.get(p, -1.0) != v:
                self.price_volumes[p] = v
                self.price_last_change_time[p] = timestamp_sec

        # Check volume changes for asks
        for i in range(asks.shape[0]):
            p = float(asks[i, 0])
            v = float(asks[i, 1])
            if self.price_volumes.get(p, -1.0) != v:
                self.price_volumes[p] = v
                self.price_last_change_time[p] = timestamp_sec

class TradeMetricsState:
    def __init__(self, max_trades: int = 1000):
        self.max_trades = max_trades
        self.volumes = np.zeros(max_trades, dtype=np.float64)
        self.times = np.zeros(max_trades, dtype=np.float64)
        self.idx = 0
        self.count = 0

    def add_trade(self, volume: float, timestamp_sec: float):
        self.volumes[self.idx] = volume
        self.times[self.idx] = timestamp_sec
        self.idx = (self.idx + 1) % self.max_trades
        if self.count < self.max_trades:
            self.count += 1

    def get_arrays(self) -> tuple[np.ndarray, np.ndarray]:
        if self.count == 0:
            return np.zeros(0, dtype=np.float64), np.zeros(0, dtype=np.float64)

        if self.count < self.max_trades:
            return self.volumes[:self.idx], self.times[:self.idx]
        else:
            v_arr = np.concatenate((self.volumes[self.idx:], self.volumes[:self.idx]))
            t_arr = np.concatenate((self.times[self.idx:], self.times[:self.idx]))
            return v_arr, t_arr

class OrderflowMetricsTracker:
    def __init__(self):
        self.ob_state = OrderBookState()
        self.trade_state = TradeMetricsState()
        self.last_microprice = 0.0

    def process_book_update(self, bids: np.ndarray, asks: np.ndarray, timestamp: int):
        self.ob_state.update(bids, asks, timestamp)

    def process_trade(self, volume: float, timestamp_sec: float):
        self.trade_state.add_trade(volume, timestamp_sec)

    def compute_metrics(self, current_time_sec: float) -> dict[str, float]:
        metrics = {}

        bids = self.ob_state.bids
        asks = self.ob_state.asks

        # 1. OBI & Microprice
        metrics['obi_5'] = float(calc_obi(bids, asks, depth=5))
        metrics['obi_10'] = float(calc_obi(bids, asks, depth=10))

        current_microprice = float(calc_microprice(bids, asks))
        metrics['microprice'] = current_microprice
        metrics['microprice_shift'] = current_microprice - self.last_microprice if self.last_microprice != 0 else 0.0
        self.last_microprice = current_microprice

        # 2. Trade Intensity
        vols, times = self.trade_state.get_arrays()
        intensity_1s = float(calc_trade_intensity(vols, times, current_time_sec, 1.0))
        intensity_5s = float(calc_trade_intensity(vols, times, current_time_sec, 5.0))

        metrics['trade_intensity_1s'] = intensity_1s
        metrics['trade_intensity_5s'] = intensity_5s

        # 3. Queue ETA
        if bids.shape[0] > 0:
            metrics['eta_best_bid'] = float(calc_queue_eta(bids[0, 1], intensity_1s))
        else:
            metrics['eta_best_bid'] = np.inf

        # 4. Average Staleness of Top 5 Levels
        staleness_sum = 0.0
        levels = 0
        for i in range(min(5, bids.shape[0])):
            p = float(bids[i, 0])
            t_change = self.ob_state.price_last_change_time.get(p, current_time_sec)
            staleness_sum += max(0.0, current_time_sec - t_change)
            levels += 1
        for i in range(min(5, asks.shape[0])):
            p = float(asks[i, 0])
            t_change = self.ob_state.price_last_change_time.get(p, current_time_sec)
            staleness_sum += max(0.0, current_time_sec - t_change)
            levels += 1

        metrics['avg_staleness_top5_sec'] = staleness_sum / levels if levels > 0 else 0.0

        return metrics
