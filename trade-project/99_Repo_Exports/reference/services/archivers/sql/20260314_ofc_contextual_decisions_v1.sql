create table if not exists ofc_contextual_decisions (
    decision_ts_ms bigint not null,
    sid text not null,
    symbol text not null,
    direction text not null,
    session text,
    dow smallint,
    hour_utc smallint,
    scenario_v4 text,
    legacy_rule_score double precision,
    legacy_rule_ok boolean,
    legacy_reason text,
    ctx_enabled boolean,
    ctx_mode text,
    ctx_key text,
    ctx_bundle_ver text,
    ctx_p_rule_raw double precision,
    ctx_p_rule_cal double precision,
    ctx_cost_p50_bps double precision,
    ctx_cost_p90_bps double precision,
    ctx_exec_risk_ref_bps double precision,
    ctx_edge_net_p50_bps double precision,
    ctx_edge_net_p90_bps double precision,
    ctx_reason text,
    ctx_fallback_level text,
    ctx_shadow_disagree boolean,
    ctx_infer_latency_us integer,
    created_at timestamptz default now(),
    primary key (decision_ts_ms, sid)
);
create index if not exists idx_ofc_ctx_symbol_ts on ofc_contextual_decisions(symbol, decision_ts_ms desc);
create index if not exists idx_ofc_ctx_bundle_ts on ofc_contextual_decisions(ctx_bundle_ver, decision_ts_ms desc);
create index if not exists idx_ofc_ctx_disagree_ts on ofc_contextual_decisions(ctx_shadow_disagree, decision_ts_ms desc);
