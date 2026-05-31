"""Tests for path-based TP autocal (Plan 3.3).

Coverage:
  1. core.path_based_tp_cdf — parse_trade_for_cdf, build_cdf_buckets,
     BucketCDF.percentile, recommend_tp1_r, lookup_recommendation.
  2. orderflow_services.path_based_tp_autocal_v1 — evaluate_window,
     publish_state (in-memory Redis stub), dwell + enforce promotion.
  3. services.path_based_tp_runtime_overrides — reader with HMAC,
     enforce gating, fallback hierarchy, fail-open when disabled.
"""
from __future__ import annotations

import hashlib
import hmac
import json
import time
from typing import Any

import pytest


# ──────────────────────────────────────────────────────────────────────────
# 1. CORE: parse_trade_for_cdf + BucketCDF.percentile
# ──────────────────────────────────────────────────────────────────────────

def test_parse_trade_winner_via_r_multiple_positive() -> None:
    from core.path_based_tp_cdf import parse_trade_for_cdf

    t = {
        "symbol": "btcusdt", "direction": "long", "entry_regime": "range",
        "r_multiple": "0.42", "mfe_pnl": "21.0", "one_r_money": "50.0",
        "tp_hits": "1",
    }
    out = parse_trade_for_cdf(t)
    assert out is not None
    assert out["symbol"] == "BTCUSDT"
    assert out["direction"] == "LONG"
    assert out["regime"] == "range"
    assert out["mfe_r"] == pytest.approx(0.42, rel=1e-6)
    assert out["pnl_r"] == pytest.approx(0.42, rel=1e-6)
    assert out["is_winner"] is True


def test_parse_trade_loser_negative_pnl_zero_tp_hits() -> None:
    from core.path_based_tp_cdf import parse_trade_for_cdf

    t = {
        "symbol": "ETHUSDT", "direction": "SHORT", "regime": "trending_bear",
        "r_multiple": "-1.0", "mfe_pnl": "5.0", "one_r_money": "50.0",
        "tp_hits": "0",
    }
    out = parse_trade_for_cdf(t)
    assert out is not None
    assert out["is_winner"] is False
    assert out["mfe_r"] == pytest.approx(0.1, rel=1e-6)


def test_parse_trade_falls_back_to_mfe_r_field_when_present() -> None:
    from core.path_based_tp_cdf import parse_trade_for_cdf

    t = {"symbol": "SOL", "direction": "long", "regime": "range",
         "mfe_r": "0.85", "r_multiple": "0.3"}
    out = parse_trade_for_cdf(t)
    assert out is not None
    assert out["mfe_r"] == pytest.approx(0.85, rel=1e-6)


def test_bucketcdf_percentile_linear_interp() -> None:
    from core.path_based_tp_cdf import BucketCDF, BucketKey

    b = BucketCDF(key=BucketKey("BTCUSDT", "range", "LONG"))
    b.mfe_r_sorted = [0.1, 0.2, 0.4, 0.8, 1.6]
    b.n_total = 5
    b.n_winners = 5
    # Median of 5 sorted samples = middle element 0.4.
    assert b.percentile(0.5) == pytest.approx(0.4, rel=1e-6)
    # 25th pct = pos=1.0 → element[1] = 0.2.
    assert b.percentile(0.25) == pytest.approx(0.2, rel=1e-6)
    # 0th = first, 100th = last.
    assert b.percentile(0.0) == pytest.approx(0.1, rel=1e-6)
    assert b.percentile(1.0) == pytest.approx(1.6, rel=1e-6)
    # Empty bucket returns 0.0
    e = BucketCDF(key=BucketKey("X", "y", "z"))
    assert e.percentile(0.5) == 0.0


# ──────────────────────────────────────────────────────────────────────────
# 2. CORE: build_cdf_buckets — hierarchy population
# ──────────────────────────────────────────────────────────────────────────

def _make_trade(sym: str, regime: str, direction: str, *, mfe_r: float,
                pnl_r: float, tp_hits: int = 0,
                is_virtual: bool = False) -> dict[str, Any]:
    return {
        "symbol": sym, "regime": regime, "direction": direction,
        "mfe_r": mfe_r, "pnl_r": pnl_r, "tp_hits": tp_hits,
        "is_virtual": is_virtual,
        "is_winner": (pnl_r > 0.0) or (tp_hits >= 1 and mfe_r > 0.0),
    }


def test_build_cdf_buckets_populates_full_hierarchy() -> None:
    from core.path_based_tp_cdf import ALL, build_cdf_buckets

    trades = [
        _make_trade("BTCUSDT", "range", "LONG", mfe_r=0.5, pnl_r=0.3),
        _make_trade("ETHUSDT", "range", "LONG", mfe_r=0.7, pnl_r=0.5),
        _make_trade("BTCUSDT", "trend", "SHORT", mfe_r=1.2, pnl_r=-1.0),
    ]
    buckets = build_cdf_buckets(trades)
    # specific bucket
    assert "BTCUSDT|range|LONG" in buckets
    assert buckets["BTCUSDT|range|LONG"].n_winners == 1
    assert buckets["BTCUSDT|range|LONG"].mfe_r_sorted == [0.5]
    # symbol-agnostic
    assert f"{ALL}|range|LONG" in buckets
    assert buckets[f"{ALL}|range|LONG"].n_winners == 2
    assert buckets[f"{ALL}|range|LONG"].mfe_r_sorted == [0.5, 0.7]
    # regime-agnostic
    assert f"BTCUSDT|{ALL}|LONG" in buckets
    # global
    assert f"{ALL}|{ALL}|{ALL}" in buckets
    g = buckets[f"{ALL}|{ALL}|{ALL}"]
    assert g.n_total == 3 and g.n_winners == 2


def test_build_cdf_buckets_excludes_virtual_when_flag_off() -> None:
    from core.path_based_tp_cdf import build_cdf_buckets

    trades = [
        _make_trade("BTC", "range", "LONG", mfe_r=0.5, pnl_r=0.3),
        _make_trade("BTC", "range", "LONG", mfe_r=0.9, pnl_r=0.8, is_virtual=True),
    ]
    buckets = build_cdf_buckets(trades, include_virtual=False)
    assert buckets["BTC|range|LONG"].n_total == 1
    assert buckets["BTC|range|LONG"].mfe_r_sorted == [0.5]


# ──────────────────────────────────────────────────────────────────────────
# 3. CORE: recommend_tp1_r + lookup_recommendation
# ──────────────────────────────────────────────────────────────────────────

def test_recommend_tp1_r_clips_and_marks_pass_only_when_in_bounds() -> None:
    from core.path_based_tp_cdf import (
        BucketCDF, BucketKey, recommend_tp1_r,
    )

    # Healthy bucket: 60 winners, p50 = 0.5 → passes
    b = BucketCDF(key=BucketKey("BTC", "range", "LONG"))
    b.mfe_r_sorted = sorted([0.3 + 0.01 * i for i in range(60)])
    b.n_total = 80
    b.n_winners = 60
    rec = recommend_tp1_r(b, quantile=0.5, min_winners=30,
                          tp1_r_min=0.20, tp1_r_max=1.50)
    assert rec.passes == 1
    assert 0.5 < rec.tp1_r < 0.7

    # Degenerate bucket: tiny tp1 below floor → clipped & does NOT pass.
    b2 = BucketCDF(key=BucketKey("X", "y", "z"))
    b2.mfe_r_sorted = [0.01] * 60
    b2.n_total = 100
    b2.n_winners = 60
    rec2 = recommend_tp1_r(b2, quantile=0.5, min_winners=30,
                           tp1_r_min=0.20, tp1_r_max=1.50)
    assert rec2.tp1_r == pytest.approx(0.20)  # clipped to floor
    assert rec2.passes == 0

    # Too few winners — no pass even with great MFE.
    b3 = BucketCDF(key=BucketKey("X", "y", "z"))
    b3.mfe_r_sorted = [0.5, 0.6]
    b3.n_total = 5
    b3.n_winners = 2
    rec3 = recommend_tp1_r(b3, quantile=0.5, min_winners=30,
                           tp1_r_min=0.20, tp1_r_max=1.50)
    assert rec3.passes == 0


def test_lookup_recommendation_fallback_hierarchy() -> None:
    from core.path_based_tp_cdf import lookup_recommendation

    recs = {
        # Specific does NOT pass (n too low).
        "BTC|range|LONG": {"tp1_r": 0.3, "passes": 0, "enforce": 0},
        # Symbol-agnostic passes.
        "*|range|LONG":   {"tp1_r": 0.45, "passes": 1, "enforce": 1},
        "*|*|*":          {"tp1_r": 0.50, "passes": 1, "enforce": 0},
    }
    hit = lookup_recommendation(recs, symbol="BTC", regime="range", direction="LONG")
    assert hit is not None
    assert hit["tp1_r"] == 0.45  # picked level 2 (symbol-agnostic)

    # When even symbol-agnostic does NOT pass → falls to global.
    recs2 = {"*|*|*": {"tp1_r": 0.55, "passes": 1, "enforce": 1}}
    hit2 = lookup_recommendation(recs2, symbol="ZZZ", regime="rare", direction="LONG")
    assert hit2 is not None and hit2["tp1_r"] == 0.55

    # All failing → None.
    recs3 = {"BTC|range|LONG": {"tp1_r": 0.3, "passes": 0}}
    assert lookup_recommendation(recs3, symbol="BTC", regime="range",
                                 direction="LONG") is None


# ──────────────────────────────────────────────────────────────────────────
# 4. AUTOCAL: evaluate_window + dwell + enforce
# ──────────────────────────────────────────────────────────────────────────

def _make_cfg(**overrides: Any) -> Any:
    from orderflow_services.path_based_tp_autocal_v1 import Cfg

    base: dict[str, Any] = dict(
        enable=True, enforce=False, interval_sec=900, window_h=72.0,
        min_winners=30, quantile=0.5, tp1_r_min=0.20, tp1_r_max=1.50,
        dwell_h=24.0, include_virtual=True,
        hmac_secret="", prom_port=9999, stream="trades:closed",
        redis_url="redis://localhost:6379/0",
    )
    base.update(overrides)
    return Cfg(**base)


def test_evaluate_window_no_enforce_when_dwell_short() -> None:
    from orderflow_services.path_based_tp_autocal_v1 import evaluate_window

    trades = [
        _make_trade("BTC", "range", "LONG", mfe_r=0.4 + 0.005 * i, pnl_r=0.2)
        for i in range(60)
    ]
    cfg = _make_cfg(enforce=True, dwell_h=24.0, min_winners=30)
    now = int(time.time() * 1000)
    out = evaluate_window(trades, cfg, prev_buckets={}, now_ms=now)
    # Bucket exists and passes …
    spec = out["BTC|range|LONG"]
    assert spec["passes"] == 1
    # … but dwell_h still 0 → enforce=0.
    assert spec["dwell_h"] == 0.0
    assert spec["enforce"] == 0


def test_evaluate_window_promotes_to_enforce_after_dwell_satisfied() -> None:
    from orderflow_services.path_based_tp_autocal_v1 import evaluate_window

    trades = [
        _make_trade("BTC", "range", "LONG", mfe_r=0.4 + 0.005 * i, pnl_r=0.2)
        for i in range(60)
    ]
    cfg = _make_cfg(enforce=True, dwell_h=1.0, min_winners=30,
                    interval_sec=900)
    now = int(time.time() * 1000)
    # Simulate prev state: bucket has been passing 0.9h ago.
    prev = {
        "BTC|range|LONG": {
            "dwell_h": 0.9,
            "last_pass_ms": now - 30 * 60 * 1000,  # 30 min back
        }
    }
    out = evaluate_window(trades, cfg, prev_buckets=prev, now_ms=now)
    spec = out["BTC|range|LONG"]
    # Δh = 0.5h; capped at interval*2/3600 = 900*2/3600 = 0.5h → new dwell ≥ 1.0
    assert spec["dwell_h"] == pytest.approx(1.4, rel=1e-2)
    assert spec["enforce"] == 1


def test_evaluate_window_resets_dwell_when_bucket_stops_passing() -> None:
    from orderflow_services.path_based_tp_autocal_v1 import evaluate_window

    # Only 10 winners — below default min_winners=30 → fails.
    trades = [
        _make_trade("BTC", "range", "LONG", mfe_r=0.4, pnl_r=0.2)
        for _ in range(10)
    ]
    cfg = _make_cfg(enforce=True, dwell_h=1.0, min_winners=30)
    prev = {"BTC|range|LONG": {"dwell_h": 12.0, "last_pass_ms": int(time.time() * 1000)}}
    out = evaluate_window(trades, cfg, prev_buckets=prev,
                          now_ms=int(time.time() * 1000))
    spec = out["BTC|range|LONG"]
    assert spec["passes"] == 0
    assert spec["dwell_h"] == 0.0
    assert spec["enforce"] == 0


# ──────────────────────────────────────────────────────────────────────────
# 5. AUTOCAL: publish_state + reader (in-memory Redis stub)
# ──────────────────────────────────────────────────────────────────────────

class _FakeRedis:
    """Trivial in-memory string store (get/set), no TTL semantics needed."""
    def __init__(self) -> None:
        self.store: dict[str, str] = {}

    def get(self, k: str) -> str | None:
        return self.store.get(k)

    def set(self, k: str, v: str, ex: int | None = None) -> bool:
        self.store[k] = v
        return True


def test_publish_state_writes_json_with_hmac_sig() -> None:
    from orderflow_services.path_based_tp_autocal_v1 import (
        STATE_KEY, publish_state,
    )

    cfg = _make_cfg(hmac_secret="sekret-1")
    r = _FakeRedis()
    buckets = {"BTC|range|LONG": {"tp1_r": 0.45, "passes": 1, "enforce": 1,
                                  "n_winners": 60, "n_total": 100, "dwell_h": 2.0}}
    ok = publish_state(r, buckets, cfg, n_trades=200)
    assert ok
    data = json.loads(r.store[STATE_KEY])
    assert "sig" in data
    assert data["n_trades"] == 200
    # HMAC matches when computed exactly the same way.
    sig = data.pop("sig")
    canon = json.dumps(data, sort_keys=True, separators=(",", ":")).encode()
    expected = hmac.new(b"sekret-1", canon, hashlib.sha256).hexdigest()
    assert sig == expected


def test_reader_returns_default_when_no_snapshot() -> None:
    from services.path_based_tp_runtime_overrides import PathBasedTpReader

    r = _FakeRedis()
    rdr = PathBasedTpReader(r, hmac_secret="")
    assert rdr.get_tp1_r(symbol="BTC", regime="range", direction="LONG",
                         default=0.5) == 0.5


def test_reader_respects_enforce_flag_and_fallback() -> None:
    from orderflow_services.path_based_tp_autocal_v1 import (
        STATE_KEY, publish_state,
    )
    from services.path_based_tp_runtime_overrides import PathBasedTpReader

    cfg = _make_cfg(hmac_secret="")
    r = _FakeRedis()
    buckets = {
        # Specific passes BUT not enforced.
        "BTC|range|LONG": {"tp1_r": 0.40, "passes": 1, "enforce": 0,
                           "n_winners": 60},
        # Symbol-agnostic — enforced.
        "*|range|LONG":   {"tp1_r": 0.55, "passes": 1, "enforce": 1,
                           "n_winners": 200},
    }
    publish_state(r, buckets, cfg, n_trades=200)
    rdr = PathBasedTpReader(r, redis_key=STATE_KEY, refresh_ms=1,
                            stale_ms=24 * 3600 * 1000, hmac_secret="")
    # With require_enforce=True (default), specific bucket is skipped because
    # enforce=0, fallback level 2 (*|range|LONG) returns 0.55.
    v = rdr.get_tp1_r(symbol="BTC", regime="range", direction="LONG",
                      default=0.99)
    assert v == 0.55
    # With require_enforce=False, the first PASSING bucket wins (specific 0.40).
    v_shadow = rdr.get_tp1_r(symbol="BTC", regime="range", direction="LONG",
                             default=0.99, require_enforce=False)
    assert v_shadow == 0.40


def test_reader_hmac_mismatch_drops_snapshot() -> None:
    from orderflow_services.path_based_tp_autocal_v1 import (
        STATE_KEY, publish_state,
    )
    from services.path_based_tp_runtime_overrides import PathBasedTpReader

    cfg = _make_cfg(hmac_secret="writer-secret")
    r = _FakeRedis()
    publish_state(
        r,
        {"BTC|range|LONG": {"tp1_r": 0.45, "passes": 1, "enforce": 1,
                            "n_winners": 60}},
        cfg, n_trades=10,
    )
    # Reader uses a DIFFERENT secret → must not apply override.
    rdr = PathBasedTpReader(r, redis_key=STATE_KEY, refresh_ms=1,
                            stale_ms=24 * 3600 * 1000,
                            hmac_secret="reader-other-secret")
    v = rdr.get_tp1_r(symbol="BTC", regime="range", direction="LONG",
                      default=0.99)
    assert v == 0.99  # fail-open to default — bad HMAC ignored.


def test_reader_returns_default_when_snapshot_stale() -> None:
    from services.path_based_tp_runtime_overrides import PathBasedTpReader

    r = _FakeRedis()
    # Snapshot ts is the unix EPOCH — definitely stale.
    payload = {
        "ts_ms": 0,
        "buckets": {"BTC|range|LONG": {"tp1_r": 0.45, "passes": 1,
                                       "enforce": 1, "n_winners": 60}},
    }
    r.set("autocal:path_tp:state", json.dumps(payload))
    rdr = PathBasedTpReader(r, refresh_ms=1, stale_ms=60 * 1000,
                            hmac_secret="")
    assert rdr.get_tp1_r(symbol="BTC", regime="range", direction="LONG",
                         default=0.7) == 0.7


# ──────────────────────────────────────────────────────────────────────────
# 6. Reader singleton — disabled by default (fail-open)
# ──────────────────────────────────────────────────────────────────────────

def test_get_reader_returns_none_when_env_disabled(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    from services.path_based_tp_runtime_overrides import (
        get_reader, reset_reader_for_tests,
    )

    monkeypatch.delenv("AUTOCAL_PATH_TP_READ_ENABLED", raising=False)
    reset_reader_for_tests()
    assert get_reader() is None


def test_get_path_based_tp1_r_returns_default_when_disabled(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    from services.path_based_tp_runtime_overrides import (
        get_path_based_tp1_r, reset_reader_for_tests,
    )

    monkeypatch.delenv("AUTOCAL_PATH_TP_READ_ENABLED", raising=False)
    reset_reader_for_tests()
    assert get_path_based_tp1_r("BTC", "range", "LONG", default=0.33) == 0.33
