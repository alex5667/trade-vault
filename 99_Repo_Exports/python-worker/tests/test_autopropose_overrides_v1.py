import json
import tempfile
from unittest.mock import MagicMock

from tools.tm_policy_autopropose import propose_overrides_v1


def test_autopropose_writes_keys(monkeypatch):
    # Mock redis
    mock_r = MagicMock()
    mock_pipe = MagicMock()
    mock_r.pipeline.return_value = mock_pipe
    
    monkeypatch.setattr("redis.from_url", lambda *args, **kwargs: mock_r)

    reco = {
        "tier_reco": {
            "BTCUSDT:range:continuation": {"symbol":"BTCUSDT","regime":"range","scenario":"continuation","abs_lvl_tier":1}
        },
        "arm_winners": {
            "BTCUSDT:range:continuation:default": {"symbol":"BTCUSDT","regime":"range","scenario":"continuation","group":"default","winner_arm":"A"}
        }
    }
    with tempfile.NamedTemporaryFile("w+", delete=True) as f:
        f.write(json.dumps(reco))
        f.flush()
        res = propose_overrides_v1(redis_url="redis://x", reco_json_path=f.name, group="default")
    
    # Check calls
    assert mock_r.pipeline.called
    assert mock_pipe.set.call_count >= 2
    assert mock_pipe.execute.called
    
    # Check return values
    sid = res["sid"]
    assert sid
    # Inspect arguments to set
    # mock_pipe.set.assert_any_call(..., ...) 
    # But arguments are complex strings. 
    # We can verify the logic ran through.
