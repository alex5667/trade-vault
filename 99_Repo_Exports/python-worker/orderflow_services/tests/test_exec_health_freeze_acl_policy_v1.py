from __future__ import annotations

from contextlib import contextmanager

from orderflow_services.exec_health_freeze_acl_policy_v1 import (
    apply,
    check,
    render,
)
from services.orderflow.exec_health_freeze_acl_contract import EXPECTED_USERS


class FakeRedisForPolicy:
    def __init__(self):
        self.cmd_log: list[tuple] = []
        self.acl_rules: dict[str, str] = {
            "default": "user default on nopass ~* &* +@all"
        }
        self.kv = {}
        self.hashes = {}
        self.client_id = 91
        self.client_name = ''
        self.lib_name = ''

    def get(self, key):
        return self.kv.get(key)

    def hgetall(self, key):
        return dict(self.hashes.get(key, {}))

    def hset(self, key, mapping=None):
        self.hashes.setdefault(key, {}).update({str(k): str(v) for k, v in (mapping or {}).items()})
        return 1

    def expire(self, key, ttl):
        return 1

    def execute_command(self, *args):
        self.cmd_log.append(args)
        if args[:2] == ('CLIENT', 'ID'):
            return self.client_id
        if args[:2] == ('CLIENT', 'SETNAME'):
            self.client_name = args[2]
            return 'OK'
        if args[:3] == ('CLIENT', 'SETINFO', 'LIB-NAME'):
            self.lib_name = args[3]
            return 'OK'
        if args[:2] == ('CLIENT', 'LIST') and len(args) >= 4 and args[2] == 'ID':
            return f'id={self.client_id} user=exec_health_freeze_bootstrap name={self.client_name} lib-name={self.lib_name}'
        if args[0] == "ACL" and args[1] == "SETUSER":
            user = args[2]
            rules = args[3:]
            # Replace >pwd with #hashed to simulate Redis behaviour better
            clean_rules = []
            for r in rules:
                if r.startswith(">"):
                    clean_rules.append("#" + r[1:])
                else:
                    clean_rules.append(r)
            self.acl_rules[user] = f"user {user} " + " ".join(clean_rules)
            return True
        if args[0] == "ACL" and args[1] == "SAVE":
            return True
        if args[0] == "ACL" and args[1] == "LOAD":
            return True
        if args[0] == "ACL" and args[1] == "LIST":
            return list(self.acl_rules.values())
        return None


@contextmanager
def patch_redis_client():
    import orderflow_services.exec_health_freeze_acl_policy_v1 as mod
    orig = mod._redis_client
    fake = FakeRedisForPolicy()
    mod._redis_client = lambda: fake
    try:
        yield fake
    finally:
        mod._redis_client = orig


def test_policy_render(capsys) -> None:
    render()
    out = capsys.readouterr().out
    assert "ACL SETUSER default" in out
    for u in EXPECTED_USERS:
        assert f"ACL SETUSER {u}" in out


def test_policy_apply() -> None:
    with patch_redis_client() as fake:
        res = apply(reload_check=True)
        assert res["ok"] is True
        assert len(res["applied"]) == len(EXPECTED_USERS)
        assert res["save_ok"] is True
        assert res["reload_ok"] is True

        # Check command log
        cmds = [c for c in fake.cmd_log if c[0] == "ACL" and c[1] == "SETUSER"]
        assert len(cmds) == len(EXPECTED_USERS)
        users_set = {c[2] for c in cmds}
        assert users_set == set(EXPECTED_USERS)


def test_policy_check_drift_when_empty() -> None:
    # Redis has nothing but default user initially
    with patch_redis_client() as fake:
        res = check()
        assert res["ok"] is False
        assert res["drift"] is True
        assert len(res["missing_users"]) > 0


def test_policy_check_match_after_apply() -> None:
    # After apply, check should be OK
    with patch_redis_client() as fake:
        import os
        from unittest.mock import patch
        env = {
            "EXEC_HEALTH_FREEZE_READER_PASS": "pwd1",
            "EXEC_HEALTH_FREEZE_WRITER_PASS": "pwd2",
            "EXEC_HEALTH_FREEZE_AUDIT_PASS": "pwd3",
            "EXEC_HEALTH_FREEZE_BOOTSTRAP_PASS": "pwd4",
        }
        with patch.dict(os.environ, env):
            apply()
            res = check()
            print(res)

            # Since check() doesn't substitute passwords for the "expected" side
            # (it just compares against EXPECTED_ACL_PROFILES directly),
            # we need to make sure the tokens match.
            assert res["ok"] is True, f"Violations: {res['violations']}, Raw ACL: {res['raw_acl_list']}"
            assert res["drift"] is False
            assert res["default_off"] is True
            for u in EXPECTED_USERS:
                assert res["per_user"].get(u) is True


def test_acl_policy_apply_blocked_by_reconnect_rollout_gate() -> None:
    with patch_redis_client() as fake:
        fake.kv['cfg:orderflow:exec_health:reconnect_smoke:rollout_gate:v1'] = '{"reason":"nightly_reconnect_smoke_failed","ts_ms":1}'
        fake.hashes['metrics:exec_health:freeze_reconnect_smoke:gate:last'] = {'gate_active': '1', 'last_fail_ts_ms': '1'}
        try:
            apply(reload_check=False)
        except SystemExit as exc:
            assert int(exc.code) == 24
        else:
            raise AssertionError('expected rollout gate block')


def test_acl_policy_render_includes_sensitive_deploy_templates(capsys) -> None:
    """P20: render() output includes the sensitive_deploy_env_templates section."""
    render()
    out = capsys.readouterr().out
    assert 'sensitive_deploy_env_templates' in out, (
        "render() output is missing the P20 sensitive_deploy_env_templates section"
    )
    assert 'exec_health_freeze_acl_policy_apply' in out
    assert 'EXEC_HEALTH_ROLLOUT_PREFLIGHT_SCHEMA_VERSION' in out

