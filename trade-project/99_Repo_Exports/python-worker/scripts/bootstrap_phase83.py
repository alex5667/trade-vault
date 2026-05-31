import logging

# Add parent dir to sys.path to find services
# [AUTOGRAVITY CLEANUP] sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from services.analytics_db import get_conn
from services.atr_freeze_override_equivalence_cert_service import ATRFreezeOverrideEquivalenceCertService

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("bootstrap_phase83")

def get_active_symbols() -> list[str]:
    """Fetch symbols currently in rollout or with active freezes."""
    symbols = set()
    with get_conn() as conn, conn.cursor() as cur:
        # From rollout
        cur.execute("SELECT DISTINCT symbol FROM atr_policy_rollouts WHERE is_current = true")
        symbols.update([r[0] for r in cur.fetchall() if r[0]])

        # From freezes
        cur.execute("SELECT DISTINCT scope_value FROM atr_active_freezes WHERE status != 'released' AND scope_kind = 'symbol'")
        symbols.update([r[0] for r in cur.fetchall() if r[0]])

    # Always include global
    symbols.add("all")
    return sorted(list(symbols))

def initialize_cutover_ladder(symbols: list[str]):
    """Initialize the cutover readiness ladder for symbols."""
    with get_conn() as conn, conn.cursor() as cur:
        for sym in symbols:
            scope_kind = "symbol" if sym != "all" else "global"
            cur.execute("""
                INSERT INTO atr_freeze_override_cutover_readiness (
                    scope_kind, scope_value, readiness_stage, status
                ) VALUES (%s, %s, %s, %s)
                ON CONFLICT (scope_kind, scope_value) DO NOTHING
            """, (scope_kind, sym, "F1_DUAL_WRITE_ESTABLISHED", "passed"))
        conn.commit()
    logger.info(f"Initialized cutover ladder for {len(symbols)} scopes.")

def main():
    logger.info("🚀 Starting Phase 8.3 Bootstrap: Freeze/Override Graph Transition")

    # 1. Gather scopes
    symbols = get_active_symbols()
    if not symbols:
        logger.warning("No active symbols found. Defaulting to 'BTCUSDT', 'ETHUSDT', 'all'")
        symbols = ["BTCUSDT", "ETHUSDT", "all"]

    logger.info(f"Target scopes for initial certification: {symbols}")

    # 2. Initialize ladder
    initialize_cutover_ladder(symbols)

    # 3. Run initial certification
    logger.info("Running initial equivalence certification (F1-F9)...")
    results = ATRFreezeOverrideEquivalenceCertService.run_batch_certification("symbol", [s for s in symbols if s != "all"])
    # Global cert
    results.append(ATRFreezeOverrideEquivalenceCertService.certify_equivalence("global", "all"))

    passed_count = sum(1 for r in results if r.get("passed"))
    logger.info(f"Certification complete: {passed_count}/{len(results)} passed.")

    if passed_count < len(results):
        logger.warning("Drift detected during bootstrap. Check atr_freeze_override_drifts table.")
    else:
        logger.info("✅ All systems nominal. Graph truth matches Legacy truth.")

if __name__ == "__main__":
    main()
