from __future__ import annotations

import pytest

from services.orderflow.exec_health_freeze_acl_contract import (
    EXPECTED_ACL_PROFILES,
    EXPECTED_USERS,
    compare_acl,
    count_connections_by_user,
    is_default_user_disabled,
    normalise_acl_line,
    normalise_setuser_rules,
    parse_client_list,
    render_all_setuser_commands,
    render_setuser,
    unknown_user_connections,
)


def test_normalise_acl_line() -> None:
    line = 'user exec_health_freeze_reader on #deadbeef ~* &* %R~cfg:* +@all -hset'
    user, tokens = normalise_acl_line(line)
    assert user == 'exec_health_freeze_reader'
    assert tokens == sorted(['on', '#deadbeef', '~*', '&*', '%R~cfg:*', '+@all', '-hset'])

    assert normalise_acl_line('invalid format') == ('', [])


def test_normalise_setuser_rules() -> None:
    rules = ['reset', 'on', '>%REPLACE_ME', '+get', '-hset']
    clean = normalise_setuser_rules(rules)
    assert clean == sorted(['reset', 'on', '+get', '-hset'])
    assert '>%REPLACE_ME' not in clean


def test_compare_acl() -> None:
    # Match: expected rules vs actual ACL LIST output
    expected = ['reset', 'on', '>%MY_PASS', '+get', '-hset']
    actual_line = 'user bob on #hashedpass +get -hset reset'
    assert compare_acl(actual_line, expected)

    # Mismatch: missing rule
    actual_line_bad = 'user bob on #hashedpass +get'
    assert not compare_acl(actual_line_bad, expected)


def test_is_default_user_disabled() -> None:
    assert not is_default_user_disabled("user bob on\nuser default on nopass")
    assert is_default_user_disabled("user bob on\nuser default off nopass")


def test_parse_client_list() -> None:
    out = """
id=12 addr=127.0.0.1:4000 fd=8 name= age=0 idle=0 flags=N db=0 sub=0 psub=0 multi=-1 qbuf=26 qbuf-free=32742 argv-mem=10 obl=0 oll=0 omem=0 tot-mem=61466 events=r cmd=client|list user=default
id=14 addr=127.0.0.1:4002 ... user=exec_health_freeze_writer
"""
    clients = parse_client_list(out)
    assert len(clients) == 2
    assert clients[0]['id'] == '12'
    assert clients[0]['user'] == 'default'
    assert clients[1]['id'] == '14'
    assert clients[1]['user'] == 'exec_health_freeze_writer'


def test_count_connections() -> None:
    out = "id=1 user=foo\nid=2 user=foo\nid=3 user=bar\nid=4 user=default"
    counts = count_connections_by_user(out)
    assert counts == {'foo': 2, 'bar': 1, 'default': 1}

    unks = unknown_user_connections(out)
    assert 'foo' in unks
    assert 'bar' in unks
    assert 'default' not in unks  # default is in EXPECTED_USERS


def test_render_commands() -> None:
    cmds = render_all_setuser_commands()
    assert len(cmds) == len(EXPECTED_USERS)
    assert cmds[0].startswith('ACL SETUSER default reset off nopass nocommands')


def test_expected_profiles_contains_all_roles() -> None:
    for u in EXPECTED_USERS:
        assert u in EXPECTED_ACL_PROFILES
