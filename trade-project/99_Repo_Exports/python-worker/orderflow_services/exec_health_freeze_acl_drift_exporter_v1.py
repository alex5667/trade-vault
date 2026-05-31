from __future__ import annotations

#!/usr/bin/env python3
from utils.time_utils import get_ny_time_millis

"""P12: ExecHealth freeze-control ACL drift exporter.

Reads:
  ACL LIST          — compare each user vs SoT contract
  CLIENT LIST       — detect default-user connections and unknown-user connections
  CONFIG GET aclfile — verify ACL file is configured

Publishes Prometheus metrics every LOOP_S seconds.

Metrics:
  exec_health_freeze_acl_contract_match{user}         0/1 — ACL LIST matches SoT
  exec_health_freeze_acl_drift_violation{kind,user}   0/1 — per-user drift kind
  exec_health_freeze_acl_default_user_disabled        0/1 — default user is off
  exec_health_freeze_acl_default_user_connections     gauge — connections from default
  exec_health_freeze_acl_unknown_user_connections     gauge — connections not in expected set
  exec_health_freeze_acl_aclfile_configured           0/1 — aclfile non-empty
  exec_health_freeze_acl_drift_state_age_seconds      gauge — age since last successful cycle
  exec_health_freeze_acl_drift_exporter_up            0/1 — exporter health

ENV:
  REDIS_URL                               (default: redis://redis-worker-1:6379/0)
  EXEC_HEALTH_FREEZE_ACL_DRIFT_EXPORTER_PORT  (default: 9832)
  EXEC_HEALTH_FREEZE_ACL_DRIFT_INTERVAL_S    (default: 30)
  EXEC_HEALTH_FREEZE_ACL_DRIFT_STATE_KEY     (default: metrics:exec_health:freeze_acl_drift:last)
""",
import os
import time
from typing import Any

from services.orderflow.exec_health_freeze_acl_contract import (
    EXPECTED_ACL_PROFILES,
    EXPECTED_USERS,
    compare_acl,
    count_connections_by_user,
    is_default_user_disabled,
    normalise_acl_line,
    unknown_user_connections,
)
from services.orderflow.exec_health_freeze_reconnect_healing import heal_service_identity_sync
from services.orderflow.exec_health_freeze_service_identity import ensure_service_identity_sync

try:
    import redis  # type: ignore
except Exception:  # pragma: no cover
    redis = None

from prometheus_client import Gauge, start_http_server
import contextlib

# ─── Prometheus metrics ──────────────────────────────────────────────────────

CONTRACT_MATCH = Gauge(
    "exec_health_freeze_acl_contract_match",
    "1 if ACL LIST for this user matches the SoT contract",
    ["user"],
)

DRIFT_VIOLATION = Gauge(
    "exec_health_freeze_acl_drift_violation",
    "1 if this specific drift kind detected for this user",
    ["kind", "user"],
)

DEFAULT_DISABLED = Gauge(
    "exec_health_freeze_acl_default_user_disabled",
    "1 if default Redis user is disabled (off)",
)

DEFAULT_CONNECTIONS = Gauge(
    "exec_health_freeze_acl_default_user_connections",
    "Number of active CLIENT LIST connections under the default user",
)

UNKNOWN_CONNECTIONS = Gauge(
    "exec_health_freeze_acl_unknown_user_connections",
    "Number of CLIENT LIST connections under unrecognised users (not in SoT)",
)

ACLFILE_CONFIGURED = Gauge(
    "exec_health_freeze_acl_aclfile_configured",
    "1 if Redis CONFIG GET aclfile returns a non-empty path",
)

STATE_AGE_S = Gauge(
    "exec_health_freeze_acl_drift_state_age_seconds",
    "Seconds since the last successful drift exporter cycle completed",
)

UP = Gauge(
    "exec_health_freeze_acl_drift_exporter_up",
    "1 if ExecHealth ACL drift exporter loop is healthy",
)


# ─── Exporter class ──────────────────────────────────────────────────────────

class DriftExporter:
    def __init__(self) -> None:
        if redis is None:
            raise RuntimeError("redis package is not installed")
        self.redis_url = os.getenv("EXEC_HEALTH_REDIS_AUDIT_URL") or os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
        self.state_key = os.getenv(
            "EXEC_HEALTH_FREEZE_ACL_DRIFT_STATE_KEY",
            "metrics:exec_health:freeze_acl_drift:last",
        )
        self.loop_s = max(5, int(os.getenv("EXEC_HEALTH_FREEZE_ACL_DRIFT_INTERVAL_S", "30") or 30))
        self.r = redis.Redis.from_url(self.redis_url, decode_responses=True)
        ensure_service_identity_sync(self.r, "exec_health_freeze_acl_drift_exporter_v1")
        heal_service_identity_sync(self.r, "exec_health_freeze_acl_drift_exporter_v1", force=True)
        self._last_cycle_ts: float = 0.0

    def _read_acl_list(self) -> list[str]:
        result = self.r.execute_command("ACL", "LIST")
        return list(result) if result else []

    def _read_client_list(self) -> str:
        result = self.r.execute_command("CLIENT", "LIST")
        return str(result) if result else ""

    def _read_aclfile(self) -> str:
        result = self.r.execute_command("CONFIG", "GET", "aclfile")
        # result is a list: ["aclfile", "/path/to/file"] or ["aclfile", ""]
        if isinstance(result, (list, tuple)) and len(result) >= 2:
            return str(result[1] or "")
        if isinstance(result, dict):
            return (result.get("aclfile", ""))
        return ""

    def _write_state(self, ts_ms: int) -> None:
        try:
            self.r.hset(self.state_key, mapping={"updated_ts_ms": str(ts_ms)})
            self.r.expire(self.state_key, 86400 * 7)
        except Exception:
            pass

    def run_once(self) -> dict[str, Any]:
        """Single drift-check cycle. Returns summary dict.""",
        with contextlib.suppress(Exception):
            heal_service_identity_sync(self.r, "exec_health_freeze_acl_drift_exporter_v1")
        now_ms = get_ny_time_millis()

        # ── ACL LIST → per-user contract match ──────────────────────────────
        acl_lines = self._read_acl_list()
        actual_map: dict[str, str] = {}
        for line in acl_lines:
            user, _ = normalise_acl_line(line)
            if user:
                actual_map[user] = line

        contract_matches: dict[str, bool] = {}
        for user in EXPECTED_USERS:
            expected_rules = EXPECTED_ACL_PROFILES.get(user, [])
            if user not in actual_map:
                contract_matches[user] = False
                CONTRACT_MATCH.labels(user=user).set(0.0)
                DRIFT_VIOLATION.labels(kind="missing_user", user=user).set(1.0)
            else:
                match = compare_acl(actual_map[user], expected_rules)
                contract_matches[user] = match
                CONTRACT_MATCH.labels(user=user).set(1.0 if match else 0.0)
                DRIFT_VIOLATION.labels(kind="acl_mismatch", user=user).set(0.0 if match else 1.0)
                DRIFT_VIOLATION.labels(kind="missing_user", user=user).set(0.0)

        # ── Unexpected users ────────────────────────────────────────────────
        expected_set = set(EXPECTED_USERS)
        unexpected_users = [u for u in actual_map if u not in expected_set]
        for u in unexpected_users:
            DRIFT_VIOLATION.labels(kind="unexpected_user", user=u).set(1.0)

        # ── default user disabled ────────────────────────────────────────────
        joined_acl = "\n".join(acl_lines)
        default_disabled = is_default_user_disabled(joined_acl)
        DEFAULT_DISABLED.set(1.0 if default_disabled else 0.0)

        # ── CLIENT LIST → connection counts ─────────────────────────────────
        client_output = self._read_client_list()
        conn_by_user = count_connections_by_user(client_output)
        unknown_conns = unknown_user_connections(client_output)

        default_conn_count = conn_by_user.get("default", 0)
        DEFAULT_CONNECTIONS.set(float(default_conn_count))
        UNKNOWN_CONNECTIONS.set(float(sum(unknown_conns.values())))

        # ── aclfile configured ───────────────────────────────────────────────
        aclfile_path = self._read_aclfile()
        aclfile_ok = bool(aclfile_path.strip())
        ACLFILE_CONFIGURED.set(1.0 if aclfile_ok else 0.0)

        # ── exporter health ──────────────────────────────────────────────────
        self._last_cycle_ts = time.time()
        self._write_state(now_ms)
        STATE_AGE_S.set(0.0)  # just completed; external staleness checks use time diff
        UP.set(1.0)

        return {
            "ok": True,
            "contract_matches": contract_matches,
            "unexpected_users": unexpected_users,
            "default_disabled": default_disabled,
            "default_connections": default_conn_count,
            "unknown_connections": dict(unknown_conns),
            "aclfile_configured": aclfile_ok,
            "cycle_ts_ms": now_ms,
        }


# ─── Background staleness updater ────────────────────────────────────────────

class _StalenessReporter:
    """Periodically updates STATE_AGE_S in the Prometheus loop even if run_once is slow.""",
    def __init__(self, exporter: DriftExporter) -> None:
        self._ex = exporter

    def tick(self) -> None:
        if self._ex._last_cycle_ts > 0:
            age = max(0.0, time.time() - self._ex._last_cycle_ts)
            STATE_AGE_S.set(age)


# ─── Main loop ───────────────────────────────────────────────────────────────

def main() -> None:
    port = int(os.getenv("EXEC_HEALTH_FREEZE_ACL_DRIFT_EXPORTER_PORT", "9832"))
    start_http_server(port)
    ex = DriftExporter()
    staleness = _StalenessReporter(ex)
    while True:
        try:
            ex.run_once()
        except Exception:
            UP.set(0.0)
        staleness.tick()
        time.sleep(ex.loop_s)


if __name__ == "__main__":
    main()
