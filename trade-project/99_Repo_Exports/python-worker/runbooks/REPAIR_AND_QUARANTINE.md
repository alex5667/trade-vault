# Repair and quarantine

## When to repair SQL mirror
Use `scripts/repair_execution_inconsistencies.py` when:
- Redis `orders:state:*` and `orders:exec` agree,
- SQL mirror is missing or stale,
- exchange state is already stable,
- you need BI/Grafana/audit continuity.

Example dry-run:
```bash
python scripts/repair_execution_inconsistencies.py --dry-run
```

Example apply:
```bash
python scripts/repair_execution_inconsistencies.py
```

## When to quarantine a sid
Use `scripts/quarantine_inconsistent_sid.py` when:
- a `sid` keeps showing critical consistency mismatches,
- operator review decides that downstream automation must stop acting on that `sid`,
- there is no evidence that the live position itself is unsafe.

Example dry-run:
```bash
python scripts/quarantine_inconsistent_sid.py --dry-run
```

Example apply:
```bash
python scripts/quarantine_inconsistent_sid.py --severity critical
```

## Via Docker Compose (ops profile)

```bash
# Dry-run repair
docker compose -f docker-compose-timers.yml -f config/docker-compose.execution-p7.override.yml \
  --profile ops run --rm trade-execution-repair

# Apply repair (remove --dry-run from the compose command override or override command)
docker compose -f docker-compose-timers.yml -f config/docker-compose.execution-p7.override.yml \
  --profile ops run --rm trade-execution-repair \
  python /app/scripts/repair_execution_inconsistencies.py
```

## Important limits
- Repair tooling updates SQL mirror only.
- Quarantine tooling marks Redis metadata only.
- Neither tool replaces the emergency flatten runbook when exposure is unsafe.
