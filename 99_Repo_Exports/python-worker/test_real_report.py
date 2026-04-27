import sys
import os

from pathlib import Path

# Add python-worker to PYTHONPATH
# [AUTOGRAVITY CLEANUP] sys.path.insert(0, str(Path("/home/alex/front/trade/scanner_infra/python-worker").resolve()))

# Force environment for the test
os.environ["PERIODIC_REPORT_SEND_VIRTUAL_ONLY"] = "false"
os.environ["PERIODIC_REPORT_SEND_EMPTY"] = "true"
os.environ["REDIS_URL"] = "redis://redis-worker-1:6379/0"

from services.periodic_reporter import get_reporter_instance

def run():
    rep = get_reporter_instance()
    
    window_sec = 3600 * 24 # За последние сутки
    
    print("Calling _generate_and_send_report_internal with demo_only=False...")
    
    # 2. Вызываем _generate_and_send_report_internal напрямую с demo_only = False
    rep._generate_and_send_report_internal("CryptoOrderFlow", "ALL", window_sec, demo_only=False)
    
    print("Report dispatched to notify:telegram stream!")

if __name__ == "__main__":
    run()
