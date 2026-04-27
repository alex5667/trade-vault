import os
import sys
# Add python-worker to sys.path so we can import services
# [AUTOGRAVITY CLEANUP] sys.path.insert(0, '/home/alex/front/trade/scanner_infra/python-worker')

from services.periodic_reporter import PeriodicReporter
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('test')

def test_all_report():
    reporter = PeriodicReporter()
    logger.info("Triggering ALL report for CryptoOrderFlow")
    reporter.send_report_for_pair("CryptoOrderFlow", "ALL", window_seconds=3600 * 24)
    logger.info("Done")

if __name__ == "__main__":
    test_all_report()
