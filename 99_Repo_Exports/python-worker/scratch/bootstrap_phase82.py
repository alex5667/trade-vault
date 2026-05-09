import logging

# Add parent dir to sys.path to allow imports from services.*
# [AUTOGRAVITY CLEANUP] sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from services.atr_graph_backed_release_gate import mark_cutover_readiness
from services.atr_release_equivalence_cert_service import ReleaseEquivalenceCertService

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("bootstrap_phase82")

def bootstrap():
    logger.info("Starting Phase 8.2 Bootstrap...")

    # 1. Initialize Cutover Readiness Ladder
    logger.info("Initializing cutover readiness ladder...")
    readiness = mark_cutover_readiness(component="release_gate")
    logger.info(f"Readiness status: {readiness.get('status')} | Summary: {readiness.get('summary')}")

    # 2. Run initial Equivalence Certification
    logger.info("Running initial Equivalence Certification (7d window)...")
    cert = ReleaseEquivalenceCertService.run_cert(window_days=7)
    logger.info(f"Cert Status: {cert.get('status')} | Cert ID: {cert.get('cert_id')}")
    logger.info(f"Summary: {cert.get('summary')}")

    logger.info("Phase 8.2 Bootstrap COMPLETED.")

if __name__ == "__main__":
    bootstrap()
