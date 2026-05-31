from __future__ import annotations

import json

from orderflow_services.exec_health_freeze_acl_bootstrap_v1 import main


def test_acl_bootstrap_contains_writer_reader_and_denies_direct_hash_writes(capsys) -> None:
    assert main([]) == 0
    out = json.loads(capsys.readouterr().out)
    cmds = '\n'.join(out['acl_commands'])
    assert 'ACL SETUSER exec_health_freeze_writer' in cmds
    assert 'ACL SETUSER exec_health_freeze_reader' in cmds
    assert '-hset' in cmds.lower()
    assert '+fcall' in cmds.lower()
