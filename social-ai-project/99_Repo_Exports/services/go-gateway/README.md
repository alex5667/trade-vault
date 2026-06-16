# services/go-gateway — Social Gateway

Command intake / webhooks / Redis Streams publish (plan §5.1). Adapted from scanner_infra `go-gateway`.
Skeleton exposes `/health`; `/commands` and webhook intake land in Phase 2/6.

```bash
go build ./... && go run ./cmd/gateway   # :8080/health
```

Commands (plan §5.1): `collect_trends`, `generate_brief`, `render_asset`, `schedule_publish`, `cancel_publish`, `replay_window`, `approve`, `reject`. `PublishCommand` → `schemas/publish_job.v1.json`.

Skills: `social-go-redis-ingest`, `golang-patterns`.
