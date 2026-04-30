"""
Example: Grafana-like Log Sampling

This example shows how to use LogSampler for Grafana-style repetitive messages.
Instead of logging every update check, log only every 1000th message.

Before:
    logger.info("Update check succeeded")  # Logs every time

After:
    sampled_info(logger, "update_check", "Update check succeeded")  # Logs every 1000th time
"""

import logging
import os
from log_sampler import sampled_info, LogSamplerFactory

# Example logger setup
logger = logging.getLogger(__name__)

def simulate_grafana_logs():
    """Simulate Grafana repetitive logging."""

    print("=== Simulating Grafana logs WITHOUT sampling ===")
    for i in range(1, 1001):
        logger.info(f"Update check succeeded: attempt {i}")

    print("\n=== Simulating Grafana logs WITH sampling (every 1000th) ===")
    for i in range(1, 10001):
        sampled_info(logger, "update_check", f"Update check succeeded: attempt {i}")

    # Show stats
    stats = LogSamplerFactory.get_stats()
    print(f"\nSampling stats: {stats}")

def simulate_trade_veto_logs():
    """Simulate trade system veto logging."""

    veto_reasons = ["DATA_QUALITY", "CONFIDENCE", "COST_EDGE", "CONSISTENCY"]
    symbols = ["BTCUSDT", "ETHUSDT", "ADAUSDT"]

    print("\n=== Simulating trade veto logs WITH sampling ===")
    for i in range(1, 5001):
        reason = veto_reasons[i % len(veto_reasons)]
        symbol = symbols[i % len(symbols)]

        sampled_info(
            logger
            f"trade_veto_{reason.lower()}"
            f"Trade veto: {reason} for {symbol} (attempt {i})"
        )

def simulate_periodic_reporter_logs():
    """Simulate PeriodicReporter summary logging."""

    sources = ["TechnicalAnalysis", "OrderFlow", "MarketData"]
    symbols = ["BTCUSDT", "ETHUSDT", "ADAUSDT", "SOLUSDT"]

    print("\n=== Simulating PeriodicReporter summary logs WITH sampling (every 10000th) ===")
    for i in range(1, 50001):  # Simulate 50k iterations
        source = sources[i % len(sources)]
        symbol = symbols[i % len(symbols)]
        total_trades = 0 if i % 100 == 0 else i % 50  # Mostly 0 trades, some with trades

        sampled_info(
            logger
            "PERIODIC_REPORTER_SUMMARY"
            f"📊 Итого собрано {total_trades} сделок для {source}/{symbol} "
            f"(окно 100 trades, matched={total_trades} из 192, processed_order_ids={total_trades})"
        )

    # Show stats
    stats = LogSamplerFactory.get_stats()
    print(f"\nFinal sampling stats: {stats}")

if __name__ == "__main__":
    # Configure logging to show output
    logging.basicConfig(
        level=logging.INFO
        format='%(asctime)s %(name)s %(levelname)s %(message)s'
    )

    simulate_grafana_logs()
    simulate_trade_veto_logs()
    simulate_periodic_reporter_logs()

    print("""
Environment variables for configuration:
    LOG_SAMPLE_UPDATE_CHECK_RATE=5000                    # Every 5000th update check
    LOG_SAMPLE_TRADE_VETO_RATE=100                       # Every 100th veto
    LOG_SAMPLE_METRICS_RATE=50                           # Every 50th metrics message
    LOG_SAMPLE_PERIODIC_REPORTER_SUMMARY_RATE=10000     # Every 10000th summary (default)
    LOG_SAMPLE_PERIODIC_REPORTER_TRIGGER_RATE=5000      # Every 5000th trigger
    LOG_SAMPLE_PERIODIC_REPORTER_SEND_REPORT_RATE=500   # Every 500th send report
""")
