# Trade Scanner Failure Drills

## ⚠️ Safety Rules
1. **NEVER** run drills D1/D3 with open exchange positions
2. Run during **low-volume hours** (03:00-06:00 UTC weekdays) or on staging
3. Each script checks preconditions before executing
4. All evidence is logged to `drills/evidence/`

## Drill Index

| Script | Scenario | Risk |
|--------|----------|------|
| `d5_ml_config_missing.sh` | ML champion config deleted | 🟢 LOW |
| `d6_postgres_down.sh` | Postgres stops for 60s | 🟡 MEDIUM |
| `d2_go_worker_crash.sh` | 1m Go worker stops for 60s | 🟡 MEDIUM |
| `d4_stale_order_book.sh` | redis-ticks paused 30s | 🟡 MEDIUM |
| `d1_redis_worker_restart.sh` | redis-worker-1 restarts | 🔴 HIGH |
| `d3_user_stream_disconnect.sh` | User stream worker restart | 🔴 HIGH |

## Running a Drill
```bash
cd /home/alex/front/trade/scanner_infra
bash drills/d5_ml_config_missing.sh   # lowest risk first
```

## Evidence
After each drill, evidence is saved to `drills/evidence/D<N>_<timestamp>.md`
