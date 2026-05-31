from pathlib import Path

from tools.ndjson_canary import _pass_share, filter_inputs, pick_baseline_for_symbol


def test_pass_share_deterministic():
    # sha1 based share should be deterministic
    a = _pass_share("BTCUSDT", 0.3)
    b = _pass_share("BTCUSDT", 0.3)
    assert a == b


def test_filter_inputs_allowlist(tmp_path: Path):
    rows = [
        {"symbol": "BTCUSDT", "id": 1},
        {"symbol": "ETHUSDT", "id": 2},
        {"symbol": "SOLUSDT", "id": 3},
    ]
    # Allowlist has priority
    filtered = list(filter_inputs(rows, canary_symbols=["BTCUSDT", "SOLUSDT"]))
    assert len(filtered) == 2
    assert {r["symbol"] for r in filtered} == {"BTCUSDT", "SOLUSDT"}


def test_filter_inputs_share():
    rows = [{"symbol": f"S{i}", "v": i} for i in range(100)]
    # 10% share
    filtered = list(filter_inputs(rows, canary_share=0.1))
    # Statistical but deterministic; for 100 random symbols it should be around 10
    assert 0 < len(filtered) < 30


def test_pick_baseline_for_symbol(tmp_path: Path):
    (tmp_path / "baseline.ndjson").write_text("{}\n", encoding="utf-8")
    assert pick_baseline_for_symbol(str(tmp_path), "BTCUSDT").endswith("baseline.ndjson")

    (tmp_path / "baseline_BTCUSDT.ndjson").write_text("{}\n", encoding="utf-8")
    assert pick_baseline_for_symbol(str(tmp_path), "BTCUSDT").endswith("baseline_BTCUSDT.ndjson")
