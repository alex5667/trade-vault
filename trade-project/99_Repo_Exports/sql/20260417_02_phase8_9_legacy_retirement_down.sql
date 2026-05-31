BEGIN;

DROP VIEW IF EXISTS v_governance_legacy_decommission_readiness_board;
DROP VIEW IF EXISTS v_governance_hidden_dependency_board;
DROP VIEW IF EXISTS v_governance_legacy_inventory_board;

DROP TABLE IF EXISTS atr_legacy_decommission_readiness;
DROP TABLE IF EXISTS atr_hidden_dependency_findings;
DROP TABLE IF EXISTS atr_legacy_decommission_events;
DROP TABLE IF EXISTS atr_legacy_path_inventory;

COMMIT;
