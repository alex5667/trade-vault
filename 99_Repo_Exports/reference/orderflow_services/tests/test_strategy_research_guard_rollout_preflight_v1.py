from __future__ import annotations

import importlib
from unittest.mock import patch


def test_allowed_purposes_include_sensitive_jobs() -> None:
    mod = importlib.import_module('orderflow_services.strategy_research_guard_rollout_preflight_v1')
    for item in [
        'conf_score_guardrails_apply',
        'conf_score_guardrails_promote',
        'meta_cov_rollout_controller',
        'conf_score_guardrails_autopromo_controller',
    ]:
        assert item in mod.ALLOWED_PURPOSES


def test_preflight_stage_mode_allows_apply_when_configured(monkeypatch) -> None:
    mod = importlib.import_module('orderflow_services.strategy_research_guard_rollout_preflight_v1')
    monkeypatch.setenv('ENABLE_STRATEGY_RESEARCH_GUARD_HARD_GATE', '1')
    monkeypatch.setenv('CONF_SCORE_GUARD_STAGE', '1')
    monkeypatch.setenv('STRATEGY_RESEARCH_GUARD_PREFLIGHT_ALLOW_STAGE', '1')
    with patch('sys.argv', ['prog', '--purpose', 'conf_score_guardrails_apply']):
        assert mod.main() == 0


def test_preflight_returns_block_exit_code(monkeypatch) -> None:
    mod = importlib.import_module('orderflow_services.strategy_research_guard_rollout_preflight_v1')
    monkeypatch.setenv('ENABLE_STRATEGY_RESEARCH_GUARD_HARD_GATE', '1')
    monkeypatch.delenv('CONF_SCORE_GUARD_STAGE', raising=False)
    with patch.object(mod, 'evaluate_research_guard_gate', return_value={'status': 'block', 'reason': 'pbo_high'}):
        with patch('sys.argv', ['prog', '--purpose', 'conf_score_guardrails_promote']):
            assert mod.main() == 24


def test_preflight_returns_invalid_exit_code(monkeypatch) -> None:
    mod = importlib.import_module('orderflow_services.strategy_research_guard_rollout_preflight_v1')
    monkeypatch.setenv('ENABLE_STRATEGY_RESEARCH_GUARD_HARD_GATE', '1')
    with patch.object(mod, 'evaluate_research_guard_gate', return_value={'status': 'invalid', 'reason': 'state_missing'}):
        with patch('sys.argv', ['prog', '--purpose', 'meta_cov_rollout_controller']):
            assert mod.main() == 25
