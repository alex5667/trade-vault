from __future__ import annotations

import json


def test_suggestion_id_stable():
    from tools.approve_entry_policy_suggestions import compute_suggestion_id
    s1 = {"proposed": {"A": "1", "B": "2"}}
    s2 = {"proposed": {"B": "2", "A": "1"}}
    assert compute_suggestion_id(s1) == compute_suggestion_id(s2)


def test_overrides_parse():
    from services.config_overrides import parse_overrides_json
    raw = json.dumps({"version": 7, "updated_ts_ms": 1, "overrides": {"X": "1", "ENTRY_POLICY_SHADOW": "1"}})
    ver, ov = parse_overrides_json(raw)
    assert ver == 7
    assert ov["X"] == "1"
    assert ov["ENTRY_POLICY_SHADOW"] == "1"
