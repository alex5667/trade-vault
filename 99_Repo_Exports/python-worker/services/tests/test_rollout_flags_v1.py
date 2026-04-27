from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from rollout_flags import RolloutFlags


def test_rollout_flags_defaults(monkeypatch):
    monkeypatch.delenv("EXEC_MAKER_TP_ENABLE", raising=False)
    flags = RolloutFlags.from_env()
    assert flags.exec_reconcile_enable is True
    assert flags.exec_user_stream_enable is True
    assert flags.maker_allowed(infra_degraded=False) is True


def test_rollout_flags_degraded_mode_disables_maker(monkeypatch):
    monkeypatch.setenv("EXEC_DEGRADED_MODE_DISABLE_MAKER", "1")
    flags = RolloutFlags.from_env()
    assert flags.maker_allowed(infra_degraded=True) is False
    assert flags.maker_allowed(infra_degraded=False) is True


def test_rollout_flags_force_safety(monkeypatch):
    monkeypatch.setenv("EXEC_FORCE_SAFETY_FIRST", "true")
    flags = RolloutFlags.from_env()
    assert flags.safety_forced(infra_degraded=False) is True
    assert flags.maker_allowed(infra_degraded=False) is False


def test_rollout_flags_disable_journal(monkeypatch):
    monkeypatch.setenv("EXEC_JOURNAL_SQL_ENABLE", "0")
    flags = RolloutFlags.from_env()
    assert flags.exec_journal_sql_enable is False


def test_rollout_flags_disable_maker_tp(monkeypatch):
    monkeypatch.setenv("EXEC_MAKER_TP_ENABLE", "0")
    flags = RolloutFlags.from_env()
    assert flags.maker_allowed(infra_degraded=False) is False


def test_rollout_flags_as_dict(monkeypatch):
    monkeypatch.delenv("EXEC_FORCE_SAFETY_FIRST", raising=False)
    flags = RolloutFlags.from_env()
    d = flags.as_dict()
    assert "exec_algo_canonical_v2" in d
    assert "exec_force_safety_first" in d
    assert isinstance(d["exec_force_safety_first"], bool)


def test_rollout_flags_degraded_safety_forced(monkeypatch):
    monkeypatch.setenv("EXEC_DEGRADED_MODE_FORCE_SAFETY_FIRST", "1")
    flags = RolloutFlags.from_env()
    assert flags.safety_forced(infra_degraded=True) is True
    assert flags.safety_forced(infra_degraded=False) is False
