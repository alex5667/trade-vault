import sys
import os
sys.path.append(os.path.join(os.getcwd(), 'python-worker'))
try:
    from core.crypto_orderflow_detectors import DeltaSpikeDetector
    print("Import success")
except Exception as e:
    print(f"Import failed: {e}")
    print(f"sys.path: {sys.path}")
