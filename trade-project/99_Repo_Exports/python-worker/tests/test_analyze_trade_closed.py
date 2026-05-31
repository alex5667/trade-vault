import json

from tools.analyze_trade_closed_ndjson import analyze_trades


def test_analyze_trades_aggregation(tmp_path):
    # Create a dummy input file
    input_file = tmp_path / "trades.ndjson"
    output_file = tmp_path / "stats.json"

    trades = [
        # Scenario A, OK=1 (2 trades: 2.0, -0.5)
        {"r_mult": 2.0, "indicators": {"strong_gate_scn": "A", "of_confirm_ok": 1}},
        {"r_mult": -0.5, "indicators": {"of_confirm_v3": {"scenario": "A", "ok": 1}}},

        # Scenario A, OK=0 (1 trade: 1.0)
        {"r_mult": 1.0, "indicators": {"strong_gate_scn": "A", "of_confirm_ok": 0}},

        # Scenario B, OK=1 (1 trade: 0.0)
        {"r_mult": 0.0, "indicators": {"strong_gate_scn": "B", "of_confirm_ok": 1}}
    ]

    with open(input_file, "w") as f:
        # write mixed format (concatenated no newlines for first two, newline for others)
        f.write(json.dumps(trades[0]) + json.dumps(trades[1]) + "\n")
        f.write(json.dumps(trades[2]) + "\n")
        f.write(json.dumps(trades[3]) + "\n")

    analyze_trades(str(input_file), str(output_file))

    assert output_file.exists()
    with open(output_file) as f:
        data = json.load(f)

    stats = data["stats"]
    assert len(stats) == 3

    # Check Scenario A, OK 1
    s_a_1 = next(s for s in stats if s["scenario"] == "A" and s["of_confirm_ok"] == 1)
    assert s_a_1["n"] == 2
    assert s_a_1["mean_r"] == 0.75 # (2.0 - 0.5) / 2
    assert s_a_1["winrate"] == 0.5 # 1 win out of 2

    # Check Scenario A, OK 0
    s_a_0 = next(s for s in stats if s["scenario"] == "A" and s["of_confirm_ok"] == 0)
    assert s_a_0["n"] == 1
    assert s_a_0["mean_r"] == 1.0





















