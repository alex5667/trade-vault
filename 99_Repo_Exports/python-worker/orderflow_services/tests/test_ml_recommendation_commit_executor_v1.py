import orderflow_services.ml_recommendation_commit_executor_v1 as m


def test_parse_msg_decodes_bytes():
    msg = {b"recommendation_id": b"r1", b"action_type": b"freeze_candidate"}
    out = m._parse_msg(msg)
    assert out["recommendation_id"] == "r1"
    assert out["action_type"] == "freeze_candidate"


def test_whitelist_contains_expected_actions():
    assert "freeze_candidate" in m.ACTION_WHITELIST
    assert "propose_threshold_canary" in m.ACTION_WHITELIST


def test_env_stream_defaults():
    assert m.INPUT_STREAM == "stream:ml:recommendation_commit_requests"
    assert m.RESULT_STREAM == "stream:ml:recommendation_apply_results"
