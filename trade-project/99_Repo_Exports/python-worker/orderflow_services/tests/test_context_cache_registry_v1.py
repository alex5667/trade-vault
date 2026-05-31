from orderflow_services.context_cache_registry_v1 import build_cache_observation


def test_build_cache_observation_extracts_versions_and_size():
    payload = {
        "compact_hash": "abc",
        "prompt_version": "p1",
        "policy_version": "q1",
        "x": "y",
    }
    obs = build_cache_observation(payload)
    assert obs["compact_hash"] == "abc"
    assert obs["prompt_version"] == "p1"
    assert obs["policy_version"] == "q1"
    assert obs["payload_bytes"] > 0
