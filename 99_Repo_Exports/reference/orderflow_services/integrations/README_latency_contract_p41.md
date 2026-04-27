# P4.1 external latency-contract adapters

Reference snippets for services that are **outside this archive**:
- Go ingest writer for `go_ingest / ingest_to_redis`
- NestJS WS gateway writer for `nest_gateway / emit_to_ws` and `nest_gateway / end_to_end_event`

Both examples must write Redis hashes under:
`metrics:latency_contract:last:<service>:<stage>:<symbol>`

Required stage-owner matrix consumed by SLO gate/exporter:
- `go_ingest / ingest_to_redis`
- `python_worker / redis_to_feature`
- `python_worker / feature_to_emit`
- `nest_gateway / emit_to_ws`
- `nest_gateway / end_to_end_event`

See `go_ingest_latency_writer_v1.go` and `nest_ws_latency_writer_v1.ts` for copy-paste-ready adapters.
