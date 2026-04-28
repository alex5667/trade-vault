-- P84: indexes for of_gate_dlq_events to support drilldown dashboards and correlation

-- Safe to run multiple times.

CREATE INDEX IF NOT EXISTS of_gate_dlq_events_stream_ts_idx
  ON of_gate_dlq_events (stream, ts DESC);

CREATE INDEX IF NOT EXISTS of_gate_dlq_events_schema_ts_idx
  ON of_gate_dlq_events (schema_version, ts DESC);

-- err prefix (best-effort); helps grouping by common errors
-- Note: expression index is optional; if it fails, you can skip it.
DO $$
BEGIN
  EXECUTE 'CREATE INDEX IF NOT EXISTS of_gate_dlq_events_err_prefix_ts_idx ON of_gate_dlq_events ((split_part(coalesce(err, ''''), '' '', 1)), ts DESC)';
EXCEPTION
  WHEN others THEN
    NULL;
END $$;

-- payload_json fields (optional): src_stream / symbol
DO $$
BEGIN
  EXECUTE 'CREATE INDEX IF NOT EXISTS of_gate_dlq_events_payload_src_stream_idx ON of_gate_dlq_events ((payload_json->>''stream''))';
EXCEPTION
  WHEN others THEN
    NULL;
END $$;

DO $$
BEGIN
  EXECUTE 'CREATE INDEX IF NOT EXISTS of_gate_dlq_events_payload_symbol_idx ON of_gate_dlq_events ((payload_json->>''symbol''))';
EXCEPTION
  WHEN others THEN
    NULL;
END $$;

