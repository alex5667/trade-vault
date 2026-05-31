from __future__ import annotations

import importlib


def test_allowed_purposes_include_sensitive_jobs() -> None:
    mod = importlib.import_module('orderflow_services.latency_contract_rollout_preflight_v1')
    for item in [
        'conf_score_guardrails_apply',
        'conf_score_guardrails_promote',
        'meta_cov_rollout_controller',
        'conf_score_guardrails_autopromo_controller',
    ]:
        assert item in mod.ALLOWED_PURPOSES
