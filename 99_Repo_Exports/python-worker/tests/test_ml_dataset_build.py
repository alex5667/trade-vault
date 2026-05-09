import json
import subprocess
import sys
from pathlib import Path


def test_dataset_builder_join_and_labels(tmp_path: Path):
    inputs = tmp_path / "inputs.ndjson"
    closed = tmp_path / "closed.ndjson"
    out = tmp_path / "ds.ndjson"

    # minimal OFInputsV1-like snapshots
    inputs.write_text(
        "\n".join([
            json.dumps({"sid":"A","ts_ms":1000,"symbol":"BTCUSDT","direction":"LONG","delta_z":2.0,"spread_bps":3.0,"expected_slippage_bps":2.0}),
            json.dumps({"sid":"B","ts_ms":2000,"symbol":"BTCUSDT","direction":"LONG","delta_z":1.0}),
        ]) + "\n",
        encoding="utf-8",
    )

    closed.write_text(
        "\n".join([
            json.dumps({"event_type":"POSITION_CLOSED","sid":"A","ts":5000,"symbol":"BTCUSDT","r_mult":0.7,"mae_r":0.5}),
            json.dumps({"event_type":"POSITION_CLOSED","sid":"B","ts":6000,"symbol":"BTCUSDT","r_mult":0.2}),
        ]) + "\n",
        encoding="utf-8",
    )

    cmd = [sys.executable, str(Path(__file__).resolve().parents[1] / "tools" / "ml_build_dataset_from_ndjson.py"),
           "--inputs", str(inputs), "--closed", str(closed), "--out", str(out), "--r-min", "0.5", "--adv-max", "1.0"]
    subprocess.check_call(cmd)

    rows = [json.loads(l) for l in out.read_text(encoding="utf-8").splitlines() if l.strip()]
    assert len(rows) == 2
    rA = [r for r in rows if r["sid"]=="A"][0]
    rB = [r for r in rows if r["sid"]=="B"][0]
    assert rA["y_edge"] == 1
    assert rB["y_edge"] == 0
    assert "exec_risk_norm" in rA["X"]

