-- P4.9: retention + partitioning for risk_mismatch_quarantine_ledger.

create table if not exists risk_mismatch_quarantine_ledger_archive (
  id               bigint not null,
  decision_id      text null,
  sid              text null,
  signal_id        text null,
  symbol           text not null default '',
  tier             text not null default '',
  repeated_count   integer not null default 0,
  mismatch_rate    double precision not null default 0,
  reasons_jsonb    jsonb not null default '[]'::jsonb,
  source           text not null default 'risk_consistency_checker',
  quarantine_action text not null default 'REPEATED_MISMATCH_QUARANTINED',
  created_ts_ms    bigint not null
) partition by range (created_ts_ms);

create or replace function ensure_monthly_range_partition_risk_mismatch(parent_table text, partition_prefix text, ts_ms bigint)
returns void language plpgsql as $$
declare
  start_ts timestamptz;
  end_ts timestamptz;
  part_name text;
  from_ms bigint;
  to_ms bigint;
begin
  start_ts := date_trunc('month', to_timestamp(ts_ms / 1000.0) at time zone 'utc');
  end_ts := start_ts + interval '1 month';
  part_name := partition_prefix || to_char(start_ts, 'YYYYMM');
  from_ms := (extract(epoch from start_ts) * 1000)::bigint;
  to_ms := (extract(epoch from end_ts) * 1000)::bigint;
  execute format('create table if not exists %I partition of %I for values from (%s) to (%s)', part_name, parent_table, from_ms, to_ms);
end$$;

create or replace function purge_risk_mismatch_hot_tables(cutoff_ts_ms bigint)
returns table(purged_quarantine bigint) language plpgsql as $$
declare
  q_count bigint := 0;
begin
  perform ensure_monthly_range_partition_risk_mismatch('risk_mismatch_quarantine_ledger_archive', 'risk_mismatch_quarantine_archive_', cutoff_ts_ms);
  insert into risk_mismatch_quarantine_ledger_archive
  select * from risk_mismatch_quarantine_ledger where created_ts_ms < cutoff_ts_ms;
  get diagnostics q_count = row_count;
  delete from risk_mismatch_quarantine_ledger where created_ts_ms < cutoff_ts_ms;
  return query select q_count;
end$$;
