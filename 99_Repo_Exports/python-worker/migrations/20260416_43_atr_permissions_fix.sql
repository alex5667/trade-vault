-- 20260416_37_atr_permissions_fix.sql
-- Grants permissions to 'trading' user for ATR Phase 8.2 governance tables and views.

GRANT ALL PRIVILEGES ON TABLE atr_release_equivalence_checks TO trading;
GRANT ALL PRIVILEGES ON TABLE atr_release_drifts TO trading;
GRANT ALL PRIVILEGES ON TABLE atr_release_cutover_readiness TO trading;

GRANT SELECT ON v_control_plane_release_readiness TO trading;
GRANT SELECT ON v_control_plane_effective_release_state TO trading;
GRANT SELECT ON v_governance_release_graph_board TO trading;
GRANT SELECT ON v_governance_release_drift_board TO trading;
