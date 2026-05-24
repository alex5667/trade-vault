"""Layout regression for dq_threshold_eval_harness_p112.

The CLI used to read DQ inputs only from `payload["indicators"]`. When fed the
v7 NDJSON capture (flat `decision_*`-prefixed keys, no nested `indicators`) it
silently aggregated nothing. The `_dq()` closure now falls back to
`decision_<key>` and bare `<key>` at top-level.
"""
from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path


def test_harness_aggregates_from_flat_decision_layout(tmp_path: Path):
    nd = tmp_path / "flat.ndjson"
    out_json = tmp_path / "report.json"
    with nd.open("w", encoding="utf-8") as f:
        for i in range(120):
            rec = {
                "symbol": "BTCUSDT",
                "ts_ms": 1700000000000 + i * 1000,
                # NO `indicators` dict — only flat `decision_*` keys (v7 capture shape)
                "decision_tick_gap_p95_ms": 200.0 + i,
                "decision_tick_gap_n": 100,  # >= min-gap-samples=50
                "decision_tick_missing_seq_ema": 0.03 + (i % 5) * 0.001,
                "decision_book_missing_seq_ema": 0.08 + (i % 7) * 0.001,
            }
            f.write(json.dumps(rec) + "\n")

    repo_root = Path(__file__).resolve().parents[1]
    proc = subprocess.run(
        [
            sys.executable,
            "-m", "orderflow_services.dq_threshold_eval_harness_p112",
            "--inputs", str(nd),
            "--max-records", "1000",
            "--min-gap-samples", "50",
            "--out-json", str(out_json),
        ],
        capture_output=True,
        text=True,
        cwd=str(repo_root),
    )
    assert out_json.exists(), (
        f"no report produced: rc={proc.returncode} "
        f"stdout={proc.stdout[-500:]} stderr={proc.stderr[-500:]}"
    )
    blob = json.loads(out_json.read_text())
    blob_str = json.dumps(blob)
    # If the decision_*-fallback works, the harness counted all 120 rows for BTCUSDT
    # AND aggregated gap samples (n_with_gap_samples should be 120 — every row had
    # decision_tick_gap_n=100 >= min-gap-samples).
    assert "BTCUSDT" in blob_str
    by_sym = blob.get("by_symbol") or {}
    btc = by_sym.get("BTCUSDT") or {}
    assert btc.get("n_rows") == 120, btc
    assert btc.get("n_with_gap_samples") == 120, btc
