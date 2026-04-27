import sys
from pathlib import Path
root = Path('/home/alex/front/trade/scanner_infra/python-worker')
sys.path.insert(0, str(root))
import services.execution_metrics as metrics_mod
import services.binance_active_symbol_guard_repair_worker as worker_mod

print("Metrics mod:", id(metrics_mod.EXECUTION_ACTIVE_SYMBOL_GUARD_RELEASED_TOMBSTONE_AGE_MS))
print("Worker mod :", id(worker_mod.EXECUTION_ACTIVE_SYMBOL_GUARD_RELEASED_TOMBSTONE_AGE_MS))
