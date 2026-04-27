-- P4.4/P4.5: Indexes for risk_decisions and risk_snapshot.

create index if not exists risk_decisions_symbol_ts_idx
  on risk_decisions (symbol, ts desc);

create index if not exists risk_decisions_tier_level_ts_idx
  on risk_decisions (tier, level, ts desc);

create index if not exists risk_snapshot_symbol_ts_idx
  on risk_snapshot (symbol, ts desc);

create index if not exists risk_snapshot_tier_level_ts_idx
  on risk_snapshot (tier, level, ts desc);
