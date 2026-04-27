# P98 — OFInputs DLQ/quarantine DB archiver dashboard

Dashboard file:
- `orderflow_services/grafana/of_inputs_archiver_p98.json`

Panels:
- Archiver staleness (seconds): `of_inputs_archiver_staleness_sec{kind=...}`
- Backlog len: `of_inputs_dlq_len{stream=...}` (from OFInputs DLQ exporter)
- Inserted total: `of_inputs_archiver_inserted_total{kind=...}`
- Error total: `of_inputs_archiver_error_total{kind=...}`

Notes:
- If backlog grows but staleness stays low, DB archiver is running.
- If backlog grows and staleness grows, the archiver is stuck (DB down / job not scheduled).
