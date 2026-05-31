import json
from pathlib import Path


def _ensure_repo_root_on_path() -> None:
    # Tests in this repo commonly assume "repo root" is on sys.path.
    # Make the test resilient when executed from other working directories.
    import sys

    root = Path(__file__).resolve().parents[2]
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))


def test_quantiles_and_thresholds_smoke(tmp_path: Path):
    _ensure_repo_root_on_path()
    # Create synthetic NDJSON that mimics replay inputs (signals:of:inputs):
    # indicators carry the DQ metrics.
    rows = []
    base_ts = 1_700_000_000_000
    for i in range(500):
        rows.append(
            {
                "sid": f"s{i}",
                "ts_ms": base_ts + i * 60_000,
                "symbol": "BTCUSDT" if i < 300 else "ETHUSDT",
                "indicators": {
                    "tick_gap_p95_ms": 100.0 + (i % 10),
                    "tick_missing_seq_ema": 0.05 + (i % 5) * 0.01,
                    "book_missing_seq_ema": 0.12 + (i % 3) * 0.02,
                }
            }
        )

    p = tmp_path / "in.ndjson"
    with open(p, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

    out = tmp_path / "out.json"

    from ml_analysis.tools.eval_dq_thresholds_from_decision_records_v1 import main

    # Low min_n so both symbols are emitted.
    main(["--in", str(p), "--out", str(out), "--min-n", "50"])

    obj = json.loads(out.read_text(encoding="utf-8"))
    assert obj["version"] == "eval_dq_thresholds_from_decision_records_v1"
    assert "BTCUSDT" in obj["by_symbol"]
    assert "ETHUSDT" in obj["by_symbol"]

    btc = obj["by_symbol"]["BTCUSDT"]["tick_gap_p95_ms"]
    assert btc["safe"]["n"] >= 50
    assert btc["strict"]["n"] >= 50

    # strict should be <= safe for soft/hard/extreme.
    assert btc["strict"]["soft"] <= btc["safe"]["soft"]
    assert btc["strict"]["hard"] <= btc["safe"]["hard"]
    assert btc["strict"]["extreme"] <= btc["safe"]["extreme"]

    # EMA caps in [0,1]
    ema = obj["by_symbol"]["BTCUSDT"]["tick_missing_seq_ema"]
    assert 0.0 <= ema["safe"]["soft"] <= 1.0
    assert 0.0 <= ema["safe"]["hard"] <= 1.0


def test_payload_wrapped_variant(tmp_path: Path):
    _ensure_repo_root_on_path()
    # Some stream export variants wrap records under a `payload` field.
    rows = []
    for i in range(120):
        payload = {
            "sid": f"x{i}",
            "ts_ms": 1_700_000_000_000 + i * 60_000,
            "symbol": "BTCUSDT",
            "indicators": {"tick_gap_p95_ms": 200 + i, "tick_missing_seq_ema": 0.1, "book_missing_seq_ema": 0.2},
        }
        rows.append({"payload": json.dumps(payload)})

    p = tmp_path / "in.ndjson"
    with open(p, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

    out = tmp_path / "out.yml"
    from ml_analysis.tools.eval_dq_thresholds_from_decision_records_v1 import main

    main(["--in", str(p), "--out", str(out), "--min-n", "50"])
    # YAML was written.
    assert out.exists()
    txt = out.read_text(encoding="utf-8")
    assert "by_symbol" in txt
