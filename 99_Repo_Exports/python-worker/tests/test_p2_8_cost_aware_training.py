"""P2.8 — cost-aware label integration in v15_lgbm training pipeline.

Covers:
  1. _compute_cost_aware_hit: winner (pnl > fees + slip)
  2. _compute_cost_aware_hit: loser (pnl < fees + slip)
  3. _compute_cost_aware_hit: borderline — exactly at cost → loser (strict >)
  4. _compute_cost_aware_hit: slippage chain — realized → expected → fallback
  5. _compute_cost_aware_hit: missing pnl_net → None
  6. _compute_cost_aware_hit: non-finite pnl_net → None
  7. _compute_cost_aware_hit: notional_usd absent → only fees counted
  8. _compute_cost_aware_hit: fee_mul scaling
  9. cost-aware label is more conservative than r-threshold label
 10. load_dataset cost_aware=True skips trades with missing pnl_net
 11. load_dataset cost_aware=False uses r_multiple threshold (unchanged)
 12. run_trainer cmd includes --cost-aware-label when COST_AWARE_LABEL=True
 13. run_trainer cmd omits --cost-aware-label when COST_AWARE_LABEL=False
 14. verdict JSON contains cost_aware_label field
 15. _compute_cost_aware_hit: negative fees handled via abs()
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch


# ── Import helper ─────────────────────────────────────────────────────────────

def _import_train():
    import importlib
    return importlib.import_module("tools.train_v15_lgbm")


def _import_bundle():
    import importlib
    return importlib.import_module("tools.nightly_v15_lgbm_train_bundle")


# ─── 1-8. _compute_cost_aware_hit unit tests ─────────────────────────────────

def test_cost_aware_hit_winner():
    from tools.train_v15_lgbm import _compute_cost_aware_hit
    # pnl_net=10, fees=2 (×2 fee_mul=4), slip=0 → 10-4=6 > 0 → 1
    fields = {"pnl_net": "10.0", "fees": "2.0", "notional_usd": "0"}
    assert _compute_cost_aware_hit(fields, fee_mul=2.0, slip_bps_fallback=0.0) == 1


def test_cost_aware_hit_loser():
    from tools.train_v15_lgbm import _compute_cost_aware_hit
    # pnl_net=3, fees=2 (×2=4), no slip → 3-4 = -1 < 0 → 0
    fields = {"pnl_net": "3.0", "fees": "2.0", "notional_usd": "0"}
    assert _compute_cost_aware_hit(fields, fee_mul=2.0, slip_bps_fallback=0.0) == 0


def test_cost_aware_hit_borderline_is_loser():
    from tools.train_v15_lgbm import _compute_cost_aware_hit
    # pnl_net == cost_total → NOT > 0 → loser
    fields = {"pnl_net": "4.0", "fees": "2.0", "notional_usd": "0"}
    assert _compute_cost_aware_hit(fields, fee_mul=2.0, slip_bps_fallback=0.0) == 0


def test_cost_aware_hit_slippage_chain_realized():
    from tools.train_v15_lgbm import _compute_cost_aware_hit
    # realized_bps=2, notional=10000 → slip_usd = 2/1e4 * 10000 = 2
    # pnl=100, fees=0, slip=2 → 100-2=98 > 0 → 1
    fields = {
        "pnl_net": "100.0", "fees": "0", "notional_usd": "10000",
        "slippage_realized_bps": "2.0",
    }
    assert _compute_cost_aware_hit(fields, fee_mul=2.0, slip_bps_fallback=999.0) == 1


def test_cost_aware_hit_slippage_chain_expected_when_no_realized():
    from tools.train_v15_lgbm import _compute_cost_aware_hit
    # no realized → falls back to expected_slippage_bps=2
    fields = {
        "pnl_net": "5.0", "fees": "0", "notional_usd": "10000",
        "expected_slippage_bps": "2.0",
    }
    # slip_usd=2, pnl=5 > 2 → 1
    assert _compute_cost_aware_hit(fields, fee_mul=0.0, slip_bps_fallback=999.0) == 1


def test_cost_aware_hit_slippage_chain_fallback():
    from tools.train_v15_lgbm import _compute_cost_aware_hit
    # neither realized nor expected present → fallback=4.0 bps
    fields = {"pnl_net": "0.5", "fees": "0", "notional_usd": "10000"}
    # slip_usd = 4/1e4 * 10000 = 4, pnl=0.5 < 4 → loser
    assert _compute_cost_aware_hit(fields, fee_mul=0.0, slip_bps_fallback=4.0) == 0


def test_cost_aware_hit_missing_pnl_returns_none():
    from tools.train_v15_lgbm import _compute_cost_aware_hit
    fields = {"fees": "1.0"}
    assert _compute_cost_aware_hit(fields) is None


def test_cost_aware_hit_nonfinite_pnl_returns_none():
    from tools.train_v15_lgbm import _compute_cost_aware_hit
    for bad in ("nan", "inf", "-inf"):
        fields = {"pnl_net": bad, "fees": "1.0"}
        assert _compute_cost_aware_hit(fields) is None, f"expected None for pnl_net={bad}"


def test_cost_aware_hit_no_notional_only_fees():
    from tools.train_v15_lgbm import _compute_cost_aware_hit
    # notional_usd absent → slip_usd=0; only fee_mul×fees counts
    fields = {"pnl_net": "5.0", "fees": "3.0"}
    # cost = 2*3 = 6, pnl=5 < 6 → 0
    assert _compute_cost_aware_hit(fields, fee_mul=2.0, slip_bps_fallback=0.0) == 0


def test_cost_aware_hit_negative_fees_uses_abs():
    from tools.train_v15_lgbm import _compute_cost_aware_hit
    # Some feeds store fees as negative (rebate-style sign convention)
    fields = {"pnl_net": "10.0", "fees": "-2.0", "notional_usd": "0"}
    # abs(-2)=2, fee_mul=2 → cost=4, pnl=10>4 → 1
    assert _compute_cost_aware_hit(fields, fee_mul=2.0, slip_bps_fallback=0.0) == 1


# ─── 9. cost-aware label is more conservative ────────────────────────────────

def test_cost_aware_more_conservative_than_r_threshold():
    """With non-zero fees and slippage, cost-aware positives ≤ r-threshold positives."""
    from tools.train_v15_lgbm import _compute_cost_aware_hit

    trades = [
        # r_mult >= 0.3 (r-label=1), but pnl barely beats only some cost thresholds
        {"pnl_net": "0.05", "fees": "0.10", "r_multiple": "0.35"},  # cost>pnl → cost=0
        {"pnl_net": "2.00", "fees": "0.10", "r_multiple": "0.50"},  # winner in both
        {"pnl_net": "-0.10", "fees": "0.05", "r_multiple": "-0.10"},  # loser in both
    ]
    r_thr = 0.3
    r_labels = [1 if float(t["r_multiple"]) >= r_thr else 0 for t in trades]
    ca_labels = [
        h if (h := _compute_cost_aware_hit(t, fee_mul=2.0, slip_bps_fallback=0.0)) is not None
        else 0
        for t in trades
    ]
    assert sum(ca_labels) <= sum(r_labels), (
        f"cost-aware positives ({sum(ca_labels)}) should be ≤ r-threshold positives ({sum(r_labels)})"
    )


# ─── 10-11. load_dataset cost_aware path ─────────────────────────────────────

class _FakeRedis:
    """Minimal fake redis for load_dataset() unit tests."""

    def __init__(self, signals_data, trades_data):
        self._streams = {
            "signals:of:inputs": signals_data,
            "trades:closed": trades_data,
        }

    def xrange(self, stream, **_):
        return self._streams.get(stream, [])

    @classmethod
    def from_url(cls, *a, **kw):
        raise NotImplementedError("use patch")


# norm_sid() requires ≥ 3 colon-separated parts: "prefix:SYMBOL:ts"
_SID_RAW = "of:BTCUSDT:1700000000000"     # 3 parts → norm_sid returns "BTCUSDT:1700000000000"
_SID_NORM = "BTCUSDT:1700000000000"


def _make_signal_entry(sid_raw=_SID_RAW, ts_ms=1_700_000_000_000):
    import json
    payload = json.dumps({
        "sid": sid_raw, "symbol": "BTCUSDT", "ts_ms": ts_ms,
        "indicators": {"delta_z": 1.5, "regime": "trending_bull"},
    })
    return (f"{ts_ms}-0", {"payload": json.dumps({"data": json.loads(payload)})})


def _make_trade_entry(sid_raw=_SID_RAW, r_multiple=0.5, pnl_net=None, fees=None,
                      ts_ms=1_700_001_000_000):
    fields = {"sid": sid_raw, "r_multiple": str(r_multiple)}
    if pnl_net is not None:
        fields["pnl_net"] = str(pnl_net)
    if fees is not None:
        fields["fees"] = str(fees)
    return (f"{ts_ms}-0", fields)


def test_load_dataset_cost_aware_skips_missing_pnl():
    """With cost_aware=True, trades without pnl_net are skipped."""
    import json
    from unittest.mock import patch
    import redis as redis_mod

    sig = _make_signal_entry()
    # trade without pnl_net — should be skipped
    trade_no_pnl = _make_trade_entry(r_multiple=0.5)

    fake_r = _FakeRedis(
        signals_data=[sig],
        trades_data=[trade_no_pnl],
    )
    with patch.object(redis_mod, "from_url", return_value=fake_r):
        from tools.train_v15_lgbm import load_dataset
        samples = load_dataset(
            "redis://fake",
            lookback_days=1,
            label_threshold_r=0.3,
            cost_aware=True,
        )
    assert len(samples) == 0, "trade without pnl_net must be skipped in cost_aware mode"


def test_load_dataset_r_threshold_unchanged_without_cost_aware():
    """cost_aware=False keeps the original r_multiple >= threshold logic."""
    import redis as redis_mod

    # r_multiple = 0.5 >= 0.3 → hit=1
    sig = _make_signal_entry()
    trade = _make_trade_entry(r_multiple=0.5)

    fake_r = _FakeRedis(signals_data=[sig], trades_data=[trade])
    with patch.object(redis_mod, "from_url", return_value=fake_r):
        from tools.train_v15_lgbm import load_dataset
        samples = load_dataset(
            "redis://fake",
            lookback_days=1,
            label_threshold_r=0.3,
            cost_aware=False,
        )
    assert len(samples) == 1
    assert samples[0].hit == 1


# ─── 12-13. nightly bundle run_trainer cmd ───────────────────────────────────

def test_run_trainer_cmd_includes_cost_aware_flag():
    """When COST_AWARE_LABEL=True, trainer subprocess gets --cost-aware-label."""
    import tools.nightly_v15_lgbm_train_bundle as bundle
    orig = bundle.COST_AWARE_LABEL
    orig_fee = bundle.COST_AWARE_FEE_MUL
    orig_slip = bundle.COST_AWARE_SLIP_BPS_FALLBACK
    try:
        bundle.COST_AWARE_LABEL = True
        bundle.COST_AWARE_FEE_MUL = 2.0
        bundle.COST_AWARE_SLIP_BPS_FALLBACK = 4.0

        captured_cmd = []

        def _fake_run(cmd, **kw):
            captured_cmd.extend(cmd)
            m = MagicMock()
            m.returncode = 0
            m.stdout = ""
            m.stderr = ""
            return m

        import subprocess
        with patch.object(subprocess, "run", side_effect=_fake_run):
            with patch("builtins.open", MagicMock(return_value=MagicMock(
                __enter__=lambda s, *a: s,
                __exit__=lambda s, *a: False,
                read=lambda s: "{}",
            ))):
                import json as _json
                with patch.object(_json, "load", return_value={"status": "ACCEPT"}):
                    bundle.run_trainer("/tmp/v.json", "/tmp/cand.joblib")

        assert "--cost-aware-label" in captured_cmd
        assert "--cost-aware-fee-mul" in captured_cmd
        assert "--cost-aware-slip-bps-fallback" in captured_cmd
    finally:
        bundle.COST_AWARE_LABEL = orig
        bundle.COST_AWARE_FEE_MUL = orig_fee
        bundle.COST_AWARE_SLIP_BPS_FALLBACK = orig_slip


def test_run_trainer_cmd_omits_cost_aware_flag_when_disabled():
    """When COST_AWARE_LABEL=False, --cost-aware-label must not appear."""
    import tools.nightly_v15_lgbm_train_bundle as bundle
    orig = bundle.COST_AWARE_LABEL
    try:
        bundle.COST_AWARE_LABEL = False

        captured_cmd = []

        def _fake_run(cmd, **kw):
            captured_cmd.extend(cmd)
            m = MagicMock()
            m.returncode = 0
            m.stdout = ""
            m.stderr = ""
            return m

        import subprocess
        with patch.object(subprocess, "run", side_effect=_fake_run):
            with patch("builtins.open", MagicMock(return_value=MagicMock(
                __enter__=lambda s, *a: s,
                __exit__=lambda s, *a: False,
            ))):
                import json as _json
                with patch.object(_json, "load", return_value={"status": "REJECT"}):
                    bundle.run_trainer("/tmp/v.json", "/tmp/cand.joblib")

        assert "--cost-aware-label" not in captured_cmd
    finally:
        bundle.COST_AWARE_LABEL = orig


# ─── 14. verdict JSON contains cost_aware_label ──────────────────────────────

def test_verdict_json_contains_cost_aware_label_field():
    """main() writes cost_aware_label into the verdict JSON."""
    import json
    import tempfile
    import os
    from unittest.mock import patch

    with tempfile.TemporaryDirectory() as tmpdir:
        verdict_path = os.path.join(tmpdir, "verdict.json")

        # Patch sys.argv and the loaders to return empty samples list (→ REJECTED)
        with patch("sys.argv", [
            "train_v15_lgbm",
            "--redis-url", "redis://fake",
            "--source", "redis",
            "--lookback-days", "1",
            "--label-threshold-r", "0.3",
            "--cost-aware-label",
            "--verdict-out", verdict_path,
            "--out", os.path.join(tmpdir, "model.joblib"),
        ]):
            with patch("tools.train_v15_lgbm.load_dataset", return_value=[]):
                from tools import train_v15_lgbm
                train_v15_lgbm.main()

        assert os.path.exists(verdict_path), "verdict file should be written even on reject"
        with open(verdict_path) as f:
            verdict = json.load(f)
        # cost_aware_label must appear in verdict (may be rejected due to no samples)
        assert "cost_aware_label" in verdict, (
            f"verdict.json must contain 'cost_aware_label'; got keys: {list(verdict.keys())}"
        )
        assert verdict["cost_aware_label"] is True
