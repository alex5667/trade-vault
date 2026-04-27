import sys, os
sys.path.append('/app')
import redis
# Just in case REDIS_URL isn't preset in standard env
if not os.getenv("REDIS_URL"):
    os.environ["REDIS_URL"] = "redis://redis-worker-1:6379/0"
from services.periodic_reporter import get_reporter_instance
import logging
logging.basicConfig(level=logging.DEBUG)
try:
    rep = get_reporter_instance()
    rep._generate_and_send_report_internal("CryptoOrderFlow", "ALL", window_seconds=3600)
    print("Report generated successfully")
except Exception as e:
    import traceback
    traceback.print_exc()
