"""Smoke test for soak_dq_level_share_v1 CLI and SymStats ring-buffer."""

import json
import sys
from io import StringIO
from pathlib import Path

import pytest


def _build_ndjson(tmp_path: Path, n: int = 300) -> Path:
    p = tmp_path / "soak.ndjson"
    with p.open("w", encoding="utf-8") as f:
        for i in range(n):
            sym = "BTCUSDT" if i < 200 else "ETHUSDT"
            rec = {
                "symbol": sym
                "dq_level": 2 if i % 5 == 0 else 0
                "indicators": {
                    "book_missing_seq_ema": 0.05 + (i % 7) * 0.01
                    "tick_missing_seq_ema": 0.03 + (i % 3) * 0.01
                    "tick_gap_p95_ms": 100.0 + (i % 11)
                }
            }
            f.write(json.dumps(rec) + "\n")
    return p


def test_soak_main_smoke(tmp_path: Path, capsys):
    from ml_analysis.tools.soak_dq_level_share_v1 import main as soak_main

    p = _build_ndjson(tmp_path)
    # Patch sys.argv
    old_argv = sys.argv
    try:
        sys.argv = ["soak_dq_level_share_v1", str(p), "--max-points", "100"]
        rc = soak_main()
    finally:
        sys.argv = old_argv

    assert rc == 0
    captured = capsys.readouterr()
    assert "BTCUSDT" in captured.out
    assert "ETHUSDT" in captured.out
    # Header must be present
    assert "symbol" in captured.out
    assert "dq2" in captured.out


def test_soak_missing_file(tmp_path: Path, capsys):
    from ml_analysis.tools.soak_dq_level_share_v1 import main as soak_main

    old_argv = sys.argv
    try:
        sys.argv = ["soak_dq_level_share_v1", str(tmp_path / "nonexistent.ndjson")]
        rc = soak_main()
    finally:
        sys.argv = old_argv

    assert rc == 2


def test_ring_buffer_correctness():
    """Ring-buffer must evenly distribute writes across all slots (not overwrite slot 0 only)."""
    from ml_analysis.tools.soak_dq_level_share_v1 import SymStats, _ring_append

    s = SymStats()
    max_pts = 5
    # Fill once
    for v in range(max_pts):
        _ring_append(s.book_ema, "_book_idx", s, float(v), max_pts)
    assert s.book_ema == [0.0, 1.0, 2.0, 3.0, 4.0]

    # Overwrite: write 10 more values — each slot should be touched at least once
    for v in range(10, 20):
        _ring_append(s.book_ema, "_book_idx", s, float(v), max_pts)

    # All 5 slots should have been overwritten with values >= 10
    assert all(x >= 10.0 for x in s.book_ema), f"Ring buffer bug: {s.book_ema}"
