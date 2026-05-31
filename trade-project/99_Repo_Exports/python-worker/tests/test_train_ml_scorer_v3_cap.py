"""Tests for per-symbol cap logic in train_ml_scorer_v3.

The cap is implemented inline in `main()` (not a separate function) because
it operates on the raw fetched `rows` and `cols` from `fetch_training_data()`.
We verify the logic via a synthetic harness that mirrors the inline block.
"""
from __future__ import annotations


def _apply_cap(rows: list[tuple], cols: list[str], cap: int) -> tuple[list[tuple], dict[str, int]]:
    """Subset of the main()-inline cap block — keep in sync with the trainer."""
    try:
        sym_idx = cols.index("symbol")
    except ValueError:
        return rows, {}
    if cap <= 0:
        return rows, {}
    sym_counts: dict[str, int] = {}
    kept: list[tuple] = []
    # Rows are ASC by ts → walk reversed (newest first) for tail-bias retention.
    for row in reversed(rows):
        sym = str(row[sym_idx] or "")
        if sym_counts.get(sym, 0) < cap:
            sym_counts[sym] = sym_counts.get(sym, 0) + 1
            kept.append(row)
    kept.reverse()  # restore ASC
    return kept, sym_counts


class TestPerSymbolCap:
    def test_no_cap_returns_all(self):
        cols = ["ts", "symbol", "r"]
        rows = [(i, "BTCUSDT", 0.1) for i in range(10)]
        out, counts = _apply_cap(rows, cols, cap=0)
        assert out == rows
        assert counts == {}

    def test_cap_limits_dominant_symbol(self):
        cols = ["ts", "symbol", "r"]
        rows  = [(i, "1000PEPEUSDT", -0.5) for i in range(100)]
        rows += [(i + 100, "BTCUSDT", 0.5) for i in range(5)]
        out, counts = _apply_cap(rows, cols, cap=10)
        # PEPE capped to 10, BTC all 5 → total 15
        assert len(out) == 15
        assert counts["1000PEPEUSDT"] == 10
        assert counts["BTCUSDT"] == 5

    def test_cap_keeps_most_recent_chronologically(self):
        cols = ["ts", "symbol"]
        rows = [(i, "ETHUSDT") for i in range(20)]
        out, _ = _apply_cap(rows, cols, cap=5)
        assert len(out) == 5
        ts_kept = [r[0] for r in out]
        # Tail bias: must keep ts=15,16,17,18,19
        assert ts_kept == [15, 16, 17, 18, 19]
        # Order is ASC (restored after reverse-cap)
        assert ts_kept == sorted(ts_kept)

    def test_cap_independent_per_symbol(self):
        cols = ["ts", "symbol"]
        rows  = [(i, "BTCUSDT") for i in range(8)]
        rows += [(i + 100, "ETHUSDT") for i in range(6)]
        rows += [(i + 200, "SOLUSDT") for i in range(3)]
        out, counts = _apply_cap(rows, cols, cap=5)
        # BTC 5, ETH 5, SOL 3 (below cap) → 13
        assert len(out) == 13
        assert counts == {"BTCUSDT": 5, "ETHUSDT": 5, "SOLUSDT": 3}

    def test_missing_symbol_column_no_op(self):
        cols = ["ts", "r"]
        rows = [(i, 0.1) for i in range(10)]
        out, counts = _apply_cap(rows, cols, cap=3)
        assert out == rows  # untouched
        assert counts == {}

    def test_null_symbol_treated_as_empty_string(self):
        cols = ["ts", "symbol"]
        rows = [(0, None), (1, "BTCUSDT"), (2, None), (3, "BTCUSDT")]
        out, counts = _apply_cap(rows, cols, cap=1)
        # "" capped to 1, BTCUSDT capped to 1 → 2 kept
        assert len(out) == 2
        assert counts.get("", 0) == 1
        assert counts.get("BTCUSDT", 0) == 1
