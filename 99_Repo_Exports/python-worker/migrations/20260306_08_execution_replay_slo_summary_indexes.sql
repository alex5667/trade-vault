-- P3.3 autonomy: unique index on window_name for concurrent refresh support
CREATE UNIQUE INDEX IF NOT EXISTS execution_replay_slo_summary_mv_window_name_idx
  ON execution_replay_slo_summary_mv (window_name);
