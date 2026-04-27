from __future__ import annotations
"""v13: test that DEFAULT_REASONS includes all required block sources.

Fail-closed contract: if any source is missing from DEFAULT_REASONS, auto-apply
guard will NOT check it by default, breaking fail-closed guarantees.
"""

import sys
import os

# Ensure both module trees are importable from python-worker root
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)


def test_default_reasons_include_multi_sources() -> None:
    from orderflow_services import auto_apply_guard as a
    from services.orderflow import auto_apply_guard as b
    from tick_flow_full.orderflow_services import auto_apply_guard as c
    from tick_flow_full.services.orderflow import auto_apply_guard as d


    required = {"tick_gate", "enforce_bucket_promoter", "meta_cov", "prom_rules_bundle_smoke", "prom_rules_loaded_probe", "of_inputs_v3", "of_inputs_exporters_smoke", "of_gate_exporters_smoke"}

    for mod in (a, b, c, d):
        reasons = {x.strip() for x in mod.DEFAULT_REASONS.split(",") if x.strip()}
        missing = required - reasons
        assert not missing, (
            f"DEFAULT_REASONS in {mod.__file__} missing sources: {missing}. "
            "All sources required for fail-closed auto-apply guard."
        )
