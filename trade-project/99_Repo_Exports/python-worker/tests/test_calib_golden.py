from __future__ import annotations

import json
from pathlib import Path


def load_norm(path: Path):
    out = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.strip():
            out.append(json.loads(line))
    return out


def test_calib_audit_golden_matches():
    data_dir = Path(__file__).parent / "data"
    expected_path = data_dir / "calib_effq_norm.ndjson"
    replay_path = data_dir / "calib_effq_replay_norm.ndjson"
    if not expected_path.exists() or not replay_path.exists():
        return

    exp = load_norm(expected_path)
    got = load_norm(replay_path)

    # Compare stable fields (hash may be absent in replay; compare thresholds evolution)
    assert len(exp) == len(got)
    for e, g in zip(exp, got):
        for k in ("v", "symbol", "regime", "ts_ms", "src", "n"):
            assert e.get(k) == g.get(k)
        # thresholds with rounding already applied by normalizer
        assert e.get("eff_quote_th") == g.get("eff_quote_th")
        assert e.get("min_quote_delta") == g.get("min_quote_delta")
