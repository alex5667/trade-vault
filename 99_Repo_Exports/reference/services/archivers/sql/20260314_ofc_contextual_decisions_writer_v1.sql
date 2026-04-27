-- Patch C addendum: add missing columns to ofc_contextual_decisions
-- Idempotent — safe to run multiple times.
-- Prerequisites: 20260314_ofc_contextual_decisions_v1.sql (Patch A/B) already applied.

alter table ofc_contextual_decisions
    add column if not exists ctx_decision text,
    add column if not exists spread_bps_missing boolean,
    add column if not exists slippage_missing boolean;

-- Patch-C canonical index names (matching writer INSERT column set)
create index if not exists idx_ofc_ctx_decisions_symbol_ts
    on ofc_contextual_decisions(symbol, decision_ts_ms desc);

create index if not exists idx_ofc_ctx_decisions_bundle_ts
    on ofc_contextual_decisions(ctx_bundle_ver, decision_ts_ms desc);

create index if not exists idx_ofc_ctx_decisions_disagree_ts
    on ofc_contextual_decisions(ctx_shadow_disagree, decision_ts_ms desc);
