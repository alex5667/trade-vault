import logging
import time

import numpy as np
from orderflow_metrics import OrderflowMetricsTracker

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("MetricsSimulator")

def simulate():
    tracker = OrderflowMetricsTracker()
    base_price = 100.0

    logger.info("Starting high-frequency metrics simulation...")
    start_time = time.time()

    # Compile numba functions initially with dummy data to avoid profiling compiling time
    b_dummy = np.array([[100.0, 10.0]], dtype=np.float64)
    a_dummy = np.array([[100.1, 10.0]], dtype=np.float64)
    tracker.process_book_update(b_dummy, a_dummy, start_time)
    tracker.process_trade(10.0, start_time)
    tracker.compute_metrics(start_time)

    for i in range(1, 101):
        current_time = time.time()

        # Random walk for price
        base_price += np.random.normal(0, 0.05)

        # Generator order book
        bids = np.array([
            [base_price - 0.1 * j, np.random.randint(1, 100)]
            for j in range(1, 21)
        ], dtype=np.float64)

        asks = np.array([
            [base_price + 0.1 * j, np.random.randint(1, 100)]
            for j in range(1, 21)
        ], dtype=np.float64)

        tracker.process_book_update(bids, asks, current_time)

        # Random trade
        if np.random.random() > 0.3:
            trade_vol = float(np.random.randint(1, 50))
            tracker.process_trade(trade_vol, current_time)

        metrics = tracker.compute_metrics(current_time)

        if i % 10 == 0:
            logger.info(
                f"Tick {i:03d} | dTime: {current_time - start_time:.3f}s | "
                f"Microprice: {metrics['microprice']:.3f} | OBI_5: {metrics['obi_5']:.3f} | "
                f"Intensity_1s: {metrics['trade_intensity_1s']:.1f} | "
                f"ETA_Best: {metrics['eta_best_bid']:.2f}s | "
                f"Staleness: {metrics['avg_staleness_top5_sec']:.3f}s"
            )

        # sleep slightly to simulate real-time interval
        time.sleep(0.02)

if __name__ == "__main__":
    simulate()
