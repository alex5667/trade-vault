import sys
import logging

logging.basicConfig(level=logging.INFO)

from services.atr_runtime_gate_equivalence_cert_service import ATRRuntimeGateEquivalenceCertService

def run_bootstrap():
    print("Evaluating Phase 8.5 Runtime Gate Cutover Readiness...")
    status, summary = ATRRuntimeGateEquivalenceCertService.evaluate_cutover_readiness()
    
    print("\n========================")
    print(f"Status: {status}")
    print("========================")
    print("Summary Details:")
    for k, v in summary.items():
        print(f"  {k}: {v}")
    
if __name__ == "__main__":
    run_bootstrap()
