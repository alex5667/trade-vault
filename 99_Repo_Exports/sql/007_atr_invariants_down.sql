-- ==============================================================================
-- Migration: 007_atr_invariants_down
-- Description: Rollback for Phase 7 Formal Invariants
-- ==============================================================================

BEGIN;

DROP VIEW IF EXISTS v_governance_invariant_board;
DROP TABLE IF EXISTS atr_invariant_snapshots;
DROP TABLE IF EXISTS atr_invariant_violations;
DROP TABLE IF EXISTS atr_invariants;

COMMIT;
