from __future__ import annotations

from orderflow_services.exec_health_freeze_acl_audit_exporter_v1 import Exporter


class FakeRedis:
    def __init__(self):
        self.hashes = {'metrics:exec_health:freeze_acl_audit:last': {}}
        self.events = [
            {'entry-id': '1', 'username': 'unknown', 'reason': 'command', 'command': 'HSET', 'object': 'cfg:orderflow:exec_health:freeze_control:v1'},
            {'entry-id': '2', 'username': 'other', 'reason': 'command', 'command': 'GET', 'object': 'foo'},
        ]

    def execute_command(self, *args):
        if args[:2] == ('ACL', 'LOG'):
            return list(self.events)
        raise AssertionError(args)

    def hgetall(self, key: str):
        return dict(self.hashes.get(key, {}))

    def hset(self, key: str, mapping):
        self.hashes.setdefault(key, {}).update({str(k): str(v) for k, v in dict(mapping).items()})

    def expire(self, key: str, ttl: int):
        return True


def test_acl_audit_exporter_counts_matching_acl_log_entries() -> None:
    ex = Exporter.__new__(Exporter)
    ex.state_key = 'metrics:exec_health:freeze_acl_audit:last'
    ex.loop_s = 30
    ex.r = FakeRedis()
    ex._seen_ids = set()
    out = ex.run_once()
    assert out['match_count'] == 1
    st = ex.r.hashes[ex.state_key]
    assert int(st['match_count']) == 1
