"""Tests for train_scorer_model_v1: per-symbol cap and load_dataset."""
from __future__ import annotations

import json

from core.scorer_categorical_features import (
    SCORER_CATEGORICAL_FEATURES,
    encode_regime,
    encode_session,
    encode_symbol,
)
from tools.train_scorer_model_v1 import load_dataset


def _write_jsonl(path: str, rows: list[dict]) -> None:
    with open(path, "w") as f:
        for r in rows:
            f.write(json.dumps(r) + "\n")


def _make_row(sym: str, ts_ms: int, r_mult: float, feats: dict | None = None) -> dict:
    inds = feats or {"delta_z": 0.5, "obi_z": 0.1, "spread_bps": 0.3}
    return {"symbol": sym, "ts_ms": ts_ms, "r_mult": r_mult, "indicators": inds}


class TestLoadDatasetSymbolCap:
    def test_no_cap_returns_all(self, tmp_path):
        ts_base = 1_700_000_000_000
        rows = [_make_row("BTCUSDT", ts_base + i * 1000, 0.2) for i in range(10)]
        rows += [_make_row("ETHUSDT", ts_base + i * 1000, 0.3) for i in range(5)]
        p = str(tmp_path / f"edge_live_{ts_base}.jsonl")
        _write_jsonl(p, rows)

        X, y, r, ts, feats = load_dataset(
            str(tmp_path), since_ms=0,
            features=["delta_z", "obi_z", "spread_bps"],
            max_samples_per_symbol=0,
        )
        assert len(X) == 15

    def test_cap_limits_dominant_symbol(self, tmp_path):
        ts_base = 1_700_000_000_000
        # 100 PEPE rows (pos_rate ~0%), 5 BTC rows (pos_rate 100%)
        rows = [_make_row("1000PEPEUSDT", ts_base + i * 1000, -0.5) for i in range(100)]
        rows += [_make_row("BTCUSDT", ts_base + i * 1000, 0.5) for i in range(5)]
        p = str(tmp_path / f"edge_live_{ts_base}.jsonl")
        _write_jsonl(p, rows)

        X, y, r, ts, feats = load_dataset(
            str(tmp_path), since_ms=0,
            features=["delta_z", "obi_z", "spread_bps"],
            max_samples_per_symbol=10,
        )
        # PEPE capped to 10, BTC all 5 → total 15
        assert len(X) == 15

    def test_cap_keeps_most_recent(self, tmp_path):
        ts_base = 1_700_000_000_000
        # 20 rows with ascending r_mult so we can verify which were kept
        rows = [_make_row("ETHUSDT", ts_base + i * 1000, float(i)) for i in range(20)]
        p = str(tmp_path / f"edge_live_{ts_base}.jsonl")
        _write_jsonl(p, rows)

        X, y, r, ts, feats = load_dataset(
            str(tmp_path), since_ms=0,
            features=["delta_z", "obi_z", "spread_bps"],
            max_samples_per_symbol=5,
            y_min_r_override=0.5,
        )
        assert len(r) == 5
        # Most recent 5: r_mult=15,16,17,18,19 — all >= 0.5 threshold → y=1
        assert sorted(r) == sorted([15.0, 16.0, 17.0, 18.0, 19.0])

    def test_cap_zero_means_unlimited(self, tmp_path):
        ts_base = 1_700_000_000_000
        rows = [_make_row("SOLUSDT", ts_base + i * 1000, 0.2) for i in range(50)]
        p = str(tmp_path / f"edge_live_{ts_base}.jsonl")
        _write_jsonl(p, rows)

        X, y, r, ts, feats = load_dataset(
            str(tmp_path), since_ms=0,
            features=["delta_z", "obi_z", "spread_bps"],
            max_samples_per_symbol=0,
        )
        assert len(X) == 50

    def test_cap_preserves_chronological_order(self, tmp_path):
        ts_base = 1_700_000_000_000
        rows = [_make_row("BTCUSDT", ts_base + i * 1000, 0.1 * i) for i in range(20)]
        p = str(tmp_path / f"edge_live_{ts_base}.jsonl")
        _write_jsonl(p, rows)

        X, y, r, ts_out, feats = load_dataset(
            str(tmp_path), since_ms=0,
            features=["delta_z", "obi_z", "spread_bps"],
            max_samples_per_symbol=5,
        )
        # timestamps must be non-decreasing
        assert ts_out == sorted(ts_out)

    def test_cap_multiple_symbols_independent(self, tmp_path):
        ts_base = 1_700_000_000_000
        rows  = [_make_row("BTCUSDT", ts_base + i * 1000, 0.5) for i in range(8)]
        rows += [_make_row("ETHUSDT", ts_base + i * 1000, 0.5) for i in range(6)]
        rows += [_make_row("SOLUSDT", ts_base + i * 1000, 0.5) for i in range(3)]
        p = str(tmp_path / f"edge_live_{ts_base}.jsonl")
        _write_jsonl(p, rows)

        X, y, r, ts_out, feats = load_dataset(
            str(tmp_path), since_ms=0,
            features=["delta_z", "obi_z", "spread_bps"],
            max_samples_per_symbol=5,
        )
        # BTC capped to 5, ETH capped to 5, SOL 3 (under cap) → 13
        assert len(X) == 13


class TestCategoricalFeatureIntegration:
    """Train/serve contract: load_dataset must append categorical features to
    both the row vectors and the returned feature-name list, in the same order
    and with values matching the encoders."""

    def test_returned_features_include_categorical_at_end(self, tmp_path):
        ts_base = 1_700_000_000_000
        rows = [_make_row("BTCUSDT", ts_base + i * 1000, 0.2) for i in range(5)]
        p = str(tmp_path / f"edge_live_{ts_base}.jsonl")
        _write_jsonl(p, rows)
        X, y, r, ts_out, feats = load_dataset(
            str(tmp_path), since_ms=0, features=["delta_z", "obi_z", "spread_bps"],
        )
        # Last N feature names must be exactly the categorical block.
        assert feats[-len(SCORER_CATEGORICAL_FEATURES):] == list(SCORER_CATEGORICAL_FEATURES)
        # Row width matches name count.
        assert len(X[0]) == len(feats)

    def test_categorical_values_match_encoders(self, tmp_path):
        ts_base = 1_700_000_000_000
        # Single row with known categorical inputs so we can hand-verify the encoding.
        d = {
            "symbol": "ETHUSDT", "ts_ms": ts_base, "r_mult": 0.4, "direction": "BUY",
            "indicators": {"delta_z": 0.5, "obi_z": 0.1, "spread_bps": 0.3,
                           "regime": "trending_bull", "session": "NY"},
        }
        p = str(tmp_path / f"edge_live_{ts_base}.jsonl")
        with open(p, "w") as f:
            f.write(json.dumps(d) + "\n")
        X, y, r, ts_out, feats = load_dataset(
            str(tmp_path), since_ms=0, features=["delta_z", "obi_z", "spread_bps"],
        )
        assert len(X) == 1
        row = X[0]
        # Find each cat name's index and verify the encoded value
        sym_i = feats.index("_cat_symbol_idx")
        reg_i = feats.index("_cat_regime_idx")
        sess_i = feats.index("_cat_session_idx")
        assert int(row[sym_i]) == encode_symbol("ETHUSDT")
        assert int(row[reg_i]) == encode_regime("trending_bull")
        assert int(row[sess_i]) == encode_session("NY")

    def test_unknown_categorical_values_encoded_as_minus_one(self, tmp_path):
        ts_base = 1_700_000_000_000
        d = {
            "symbol": "?", "ts_ms": ts_base, "r_mult": 0.4, "direction": "?",
            "indicators": {"delta_z": 0.5, "obi_z": 0.1, "spread_bps": 0.3,
                           "regime": "?", "session": "?"},
        }
        p = str(tmp_path / f"edge_live_{ts_base}.jsonl")
        with open(p, "w") as f:
            f.write(json.dumps(d) + "\n")
        X, y, r, ts_out, feats = load_dataset(
            str(tmp_path), since_ms=0, features=["delta_z", "obi_z", "spread_bps"],
        )
        row = X[0]
        for cat in ("_cat_symbol_idx", "_cat_regime_idx", "_cat_session_idx"):
            assert int(row[feats.index(cat)]) == -1, f"{cat} should be UNKNOWN(-1)"
