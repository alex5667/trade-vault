-- P99: DLQ DB rollups/views for OFInputs (of_inputs_dlq_events)
-- Purpose:
--  - Provide deterministic, low-cardinality rollups for Grafana/Prometheus exporters.
--  - Standardize reason extraction: dq_code if present else err_prefix else 'unknown'.

-- Parsed view: adds kind/reason/symbol/version extracted from payload_json.
CREATE OR REPLACE VIEW v_of_inputs_dlq_events_parsed AS
SELECT
  ts,
  ts_ms,
  stream,
  dlq_id,
  src_stream,
  src_stream_id,
  err,
  dq_code,
  attempt_version,
  published_version,
  missing_fields,
  payload_json,
  COALESCE(NULLIF(payload_json->>'symbol',''), NULLIF(payload_json->>'sym',''), NULLIF(payload_json->>'s','')) AS symbol,
  CASE
    WHEN payload_json ? 'v' THEN NULLIF(payload_json->>'v','')
    WHEN payload_json ? 'version' THEN NULLIF(payload_json->>'version','')
    ELSE NULL
  END AS payload_v_str,
  CASE
    WHEN stream LIKE 'stream:dlq:%' THEN 'dlq'
    WHEN stream LIKE 'quarantine:%' THEN 'quarantine'
    ELSE 'other'
  END AS kind,
  COALESCE(
    NULLIF(dq_code,''),
    NULLIF(substring(COALESCE(err,'') from '^([^\s:]+)'),'') ,
    'unknown'
  ) AS reason
FROM of_inputs_dlq_events;

-- Hourly rollup (lightweight view). For Timescale you can convert this into a
-- continuous aggregate if desired.
CREATE OR REPLACE VIEW v_of_inputs_dlq_events_1h AS
SELECT
  time_bucket('1 hour', ts) AS bucket,
  kind,
  reason,
  COALESCE(NULLIF(symbol,''),'na') AS symbol,
  COUNT(*)::bigint AS n_events,
  MAX(ts) AS last_ts
FROM v_of_inputs_dlq_events_parsed
GROUP BY 1,2,3,4;

-- Reason rollup over last 24h (for exporters/alerts)
CREATE OR REPLACE VIEW v_of_inputs_dlq_events_reason_24h AS
SELECT
  kind,
  reason,
  COUNT(*)::bigint AS n_events,
  MAX(ts) AS last_ts
FROM v_of_inputs_dlq_events_parsed
WHERE ts >= now() - interval '24 hours'
GROUP BY 1,2;
