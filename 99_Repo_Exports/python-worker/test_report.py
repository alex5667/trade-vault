import sys
import logging
import traceback
logging.basicConfig(level=logging.DEBUG)
print('Phase 1: started', flush=True)

try:
    print('Importing periodic_reporter...', flush=True)
    from services.periodic_reporter import PeriodicReporter
    print('Import successful', flush=True)
    
    r = PeriodicReporter()
    print('Instance created', flush=True)
    
    print('Calling send_report...', flush=True)
    r.send_report_for_pair("CryptoOrderFlow", "BTCUSDT", window_seconds=86400)
    print('Call complete!', flush=True)
except Exception as e:
    print('Exception occurred:', e, flush=True)
    traceback.print_exc()
