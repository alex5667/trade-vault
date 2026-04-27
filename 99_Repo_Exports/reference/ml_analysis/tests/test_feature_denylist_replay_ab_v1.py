import json
import subprocess
import sys
from pathlib import Path

import pytest


def _has_sklearn() -> bool:
    try:
        import sklearn  # noqa: F401

        return True
    except Exception:
        return False


@pytest.mark.skipif(not _has_sklearn(), reason="sklearn недоступен")
def test_feature_denylist_replay_ab_pass_on_noise(tmp_path: Path):
    import numpy as np
    import pandas as pd

    rng = np.random.default_rng(123)
    n = 4000
    ts = 1700000000000 + np.arange(n, dtype=np.int64) * 1000

    good = rng.normal(size=n)
    noise = rng.normal(size=n)
    logit = 2.0 * good + 0.1 * noise
    p = 1.0 / (1.0 + np.exp(-logit))
    y = (rng.random(n) < p).astype(int)

    b_flag = (good > 0.0).astype(int)

    regimes = np.array(["trend", "range", "other"], dtype=object)
    scenario = regimes[np.arange(n) % 3]

    df = pd.DataFrame(
        {
            "ts_ms": ts,
            "label": y,
            "scenario_v4": scenario,
            "n_good": good,
            "n_noise": noise,
            "b_flag": b_flag,
        }
    )

    run_dir = tmp_path / "fs_run"
    run_dir.mkdir(parents=True, exist_ok=True)
    data_path = run_dir / "data.csv"
    df.to_csv(data_path, index=False)

    meta = {
        "ver": "unit",
        "feature_names": ["n:good", "n:noise", "b:flag"],
        "column_names": ["n_good", "n_noise", "b_flag"],
    }
    meta_path = run_dir / "meta.json"
    meta_path.write_text(json.dumps(meta, indent=2) + "\n", encoding="utf-8")

    summary = {
        "data_path": str(data_path),
        "meta_json": str(meta_path),
        "schema_ver": "unit",
        "model": "lr",
    }
    (run_dir / "summary.json").write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")

    # stability table path (not used directly in test, but kept for realism)
    (run_dir / "stability_table.csv").write_text("feature,score\n", encoding="utf-8")

    proposals = run_dir / "proposals"
    proposals.mkdir(parents=True, exist_ok=True)

    manifest = {
        "kind": "feature_denylist_proposal",
        "proposal_hash": "deadbeef" * 8,
        "status": "pending_ab",
        "inputs": {"fs_run_dir": str(run_dir), "stability_table": str(run_dir / "stability_table.csv")},
        "denylist_after": {"deny_num": ["noise"], "deny_bool": []},
    }

    mp = proposals / "denylist_proposal_unit.manifest.json"
    mp.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")

    cmd = [
        sys.executable,
        "-m",
        "ml_analysis.tools.feature_denylist_replay_ab_v1",
        "--manifest",
        str(mp),
        "--out_dir",
        str(proposals / "ab_runs"),
        "--model",
        "lr",
        "--auc_drop_max",
        "0.01",
    ]

    r = subprocess.run(cmd, cwd=str(Path(__file__).resolve().parents[2]), capture_output=True, text=True)
    assert r.returncode == 0, (r.stdout, r.stderr)

    m2 = json.loads(mp.read_text(encoding="utf-8"))
    assert m2["status"] == "ab_done"
    assert "ab" in m2
    assert int(m2["ab"]["gate_pass"]) == 1
    assert Path(m2["ab"]["report_json"]).exists()


@pytest.mark.skipif(not _has_sklearn(), reason="sklearn недоступен")
def test_feature_denylist_replay_ab_fail_on_good(tmp_path: Path):
    import numpy as np
    import pandas as pd

    rng = np.random.default_rng(456)
    n = 3000
    ts = 1700000000000 + np.arange(n, dtype=np.int64) * 1000

    good = rng.normal(size=n)
    noise = rng.normal(size=n)
    logit = 2.4 * good
    p = 1.0 / (1.0 + np.exp(-logit))
    y = (rng.random(n) < p).astype(int)

    b_flag = (good > 0.0).astype(int)
    scenario = np.array(["trend", "range", "other"], dtype=object)[np.arange(n) % 3]

    df = pd.DataFrame(
        {
            "ts_ms": ts,
            "label": y,
            "scenario_v4": scenario,
            "n_good": good,
            "n_noise": noise,
            "b_flag": b_flag,
        }
    )

    run_dir = tmp_path / "fs_run"
    run_dir.mkdir(parents=True, exist_ok=True)
    data_path = run_dir / "data.csv"
    df.to_csv(data_path, index=False)

    meta = {
        "ver": "unit",
        "feature_names": ["n:good", "n:noise", "b:flag"],
        "column_names": ["n_good", "n_noise", "b_flag"],
    }
    meta_path = run_dir / "meta.json"
    meta_path.write_text(json.dumps(meta, indent=2) + "\n", encoding="utf-8")

    summary = {
        "data_path": str(data_path),
        "meta_json": str(meta_path),
        "schema_ver": "unit",
        "model": "lr",
    }
    (run_dir / "summary.json").write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    (run_dir / "stability_table.csv").write_text("feature,score\n", encoding="utf-8")

    proposals = run_dir / "proposals"
    proposals.mkdir(parents=True, exist_ok=True)

    manifest = {
        "kind": "feature_denylist_proposal",
        "proposal_hash": "cafebabe" * 8,
        "status": "pending_ab",
        "inputs": {"fs_run_dir": str(run_dir), "stability_table": str(run_dir / "stability_table.csv")},
        "denylist_after": {"deny_num": ["good"], "deny_bool": []},
    }

    mp = proposals / "denylist_proposal_unit2.manifest.json"
    mp.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")

    cmd = [
        sys.executable,
        "-m",
        "ml_analysis.tools.feature_denylist_replay_ab_v1",
        "--manifest",
        str(mp),
        "--out_dir",
        str(proposals / "ab_runs"),
        "--model",
        "lr",
        "--auc_drop_max",
        "0.001",
    ]

    r = subprocess.run(cmd, cwd=str(Path(__file__).resolve().parents[2]), capture_output=True, text=True)
    assert r.returncode == 2, (r.stdout, r.stderr)

    m2 = json.loads(mp.read_text(encoding="utf-8"))
    assert m2["status"] == "ab_failed"
    assert "ab" in m2
    assert int(m2["ab"]["gate_pass"]) == 0
    assert Path(m2["ab"]["report_json"]).exists()
