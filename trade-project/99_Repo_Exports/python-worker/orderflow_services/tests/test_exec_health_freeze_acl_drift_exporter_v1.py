from __future__ import annotations

from orderflow_services.exec_health_freeze_acl_drift_exporter_v1 import DriftExporter
from services.orderflow.exec_health_freeze_acl_contract import EXPECTED_USERS


class FakeRedisForExporter:
    def __init__(self):
        self.hashes = {}
        self.cmd_log = []
        self.acl_list = [
            "user default off nopass nocommands reset",
        ]
        self.client_list = "id=1 user=default\nid=2 user=unknown_guy\n"

    def execute_command(self, *args):
        self.cmd_log.append(args)
        if args[0] == "ACL" and args[1] == "LIST":
            return self.acl_list
        if args[0] == "CLIENT" and args[1] == "LIST":
            return self.client_list
        if args[0] == "CONFIG" and args[1] == "GET":
            # list-style return
            return ["aclfile", "/data/users.acl"]
        return None

    def hset(self, key, mapping):
        self.hashes.setdefault(key, {}).update(mapping)

    def expire(self, key, ttl):
        return True


def test_drift_exporter_run_once() -> None:
    ex = DriftExporter.__new__(DriftExporter)
    ex.state_key = "test_key"
    ex.loop_s = 5
    ex.r = FakeRedisForExporter()
    ex._last_cycle_ts = 0.0

    # Initial state: only default user is defined in fake redis.
    res = ex.run_once()
    assert res["ok"] is True
    # Default disabled check
    assert res["default_disabled"] is True
    # Contract matches should be False for expected auth users (missing)
    missing = [u for u, ok in res["contract_matches"].items() if not ok]
    assert len(missing) == len(EXPECTED_USERS) - 1  # default is there and matches

    # Connection counts
    assert res["default_connections"] == 1
    assert "unknown_guy" in res["unknown_connections"]
    assert res["unknown_connections"]["unknown_guy"] == 1

    # ACL file
    assert res["aclfile_configured"] is True

    # State
    assert ex.r.hashes["test_key"]["updated_ts_ms"] == str(res["cycle_ts_ms"])
