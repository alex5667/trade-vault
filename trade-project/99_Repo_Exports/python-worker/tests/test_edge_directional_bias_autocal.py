"""Tests for orderflow_services.edge_directional_bias_autocal_v1."""
from __future__ import annotations

import json
from unittest.mock import MagicMock

import pytest

from orderflow_services.edge_directional_bias_autocal_v1 import (
    PHASE_BIAS,
    PHASE_LADDER,
    SCHEMA_VERSION,
    STATE_KEY,
    TRACKED_BUCKETS,
    BucketDecision,
    Cfg,
    aggregate_per_bucket,
    commit_transition,
    evaluate_bucket,
    publish_state,
    run_once,
)


def _cfg(**overrides) -> Cfg:
    base = dict(
        enable=True,
        enforce=False,
        interval_sec=900,
        window_h=168.0,
        min_applied=10,
        min_baseline=10,
        step_dwell_h=24.0,
        r_leak_max=-0.3,
        r_no_harm_tol=0.10,
        r_rollback_margin=0.30,
        pass_rate_drop=0.10,
        llm_enabled=False,
        llm_timeout_sec=8.0,
        include_virtual=True,
        hmac_secret="",
        prom_port=9904,
        stream="trades:closed",
        redis_url="redis://test/0",
        notify_telegram=False,
        notify_stream="notify:telegram",
    )
    base.update(overrides)
    return Cfg(**base)


# ─────────────────────────────────────────────────────────────────────
# aggregate_per_bucket
# ─────────────────────────────────────────────────────────────────────


def test_aggregate_skips_untracked_buckets():
    trades = [
        # tracked
        {"direction": "SHORT", "regime": "trending_bull", "r": -0.5, "bias_applied": 0.0, "ts_ms": 0, "is_virtual": False},
        # untracked (LONG×trending_bull is trend-aligned, not counter)
        {"direction": "LONG", "regime": "trending_bull", "r": 0.5, "bias_applied": 0.0, "ts_ms": 0, "is_virtual": False},
        {"direction": "SHORT", "regime": "range", "r": -0.2, "bias_applied": 0.0, "ts_ms": 0, "is_virtual": False},
    ]
    out = aggregate_per_bucket(trades, include_virtual=True)
    assert set(out.keys()) <= TRACKED_BUCKETS
    assert "SHORT|trending_bull" in out
    assert out["SHORT|trending_bull"]["n_baseline"] == 1


def test_aggregate_splits_baseline_vs_applied():
    trades = [
        # baseline rows
        {"direction": "SHORT", "regime": "trending_bull", "r": -0.5, "bias_applied": 0.0, "ts_ms": 0, "is_virtual": False},
        {"direction": "SHORT", "regime": "trending_bull", "r": -0.3, "bias_applied": 0.0, "ts_ms": 0, "is_virtual": False},
        # applied rows
        {"direction": "SHORT", "regime": "trending_bull", "r": 0.1, "bias_applied": 0.03, "ts_ms": 0, "is_virtual": False},
        {"direction": "SHORT", "regime": "trending_bull", "r": -0.1, "bias_applied": 0.03, "ts_ms": 0, "is_virtual": False},
    ]
    out = aggregate_per_bucket(trades, include_virtual=True)
    b = out["SHORT|trending_bull"]
    assert b["n_baseline"] == 2
    assert b["baseline_avg_r"] == pytest.approx(-0.4)
    assert b["n_applied"] == 2
    assert b["applied_avg_r"] == pytest.approx(0.0)
    assert b["applied_bias_observed"] == pytest.approx(0.03)


def test_aggregate_skips_virtuals_when_disabled():
    trades = [
        {"direction": "SHORT", "regime": "trending_bull", "r": -0.5, "bias_applied": 0.0, "ts_ms": 0, "is_virtual": False},
        {"direction": "SHORT", "regime": "trending_bull", "r": -2.0, "bias_applied": 0.0, "ts_ms": 0, "is_virtual": True},
    ]
    out_keep = aggregate_per_bucket(trades, include_virtual=True)
    out_drop = aggregate_per_bucket(trades, include_virtual=False)
    assert out_keep["SHORT|trending_bull"]["n_baseline"] == 2
    assert out_drop["SHORT|trending_bull"]["n_baseline"] == 1


# ─────────────────────────────────────────────────────────────────────
# evaluate_bucket — phase machine
# ─────────────────────────────────────────────────────────────────────


def test_evaluate_observe_no_leak_does_not_promote():
    cfg = _cfg()
    raw = {"n_baseline": 100, "baseline_avg_r": 0.1, "n_applied": 0, "applied_avg_r": 0.0}
    prev = {"phase": "OBSERVE", "last_phase_change_ms": 0}
    now_ms = int(cfg.step_dwell_h * 3_600_000) + 1_000
    d = evaluate_bucket("SHORT|trending_bull", raw, prev, cfg, now_ms)
    assert d.phase == "OBSERVE"
    assert d.proposed_transition is None
    assert "no_leak" in d.transition_reason


def test_evaluate_observe_with_leak_proposes_canary_low():
    cfg = _cfg()
    raw = {"n_baseline": 100, "baseline_avg_r": -0.5, "n_applied": 0, "applied_avg_r": 0.0}
    prev = {"phase": "OBSERVE", "last_phase_change_ms": 0}
    now_ms = int(cfg.step_dwell_h * 3_600_000) + 1_000
    d = evaluate_bucket("SHORT|trending_bull", raw, prev, cfg, now_ms)
    assert d.proposed_transition == "CANARY_LOW"
    assert d.phase == "OBSERVE"  # not yet committed


def test_evaluate_dwell_blocks_promotion():
    cfg = _cfg()
    raw = {"n_baseline": 100, "baseline_avg_r": -0.5, "n_applied": 0, "applied_avg_r": 0.0}
    prev = {"phase": "OBSERVE", "last_phase_change_ms": 0}
    now_ms = int((cfg.step_dwell_h - 1.0) * 3_600_000)
    d = evaluate_bucket("SHORT|trending_bull", raw, prev, cfg, now_ms)
    assert d.proposed_transition is None
    assert "dwell_h" in d.transition_reason


def test_evaluate_low_to_mid_when_no_harm():
    cfg = _cfg()
    # baseline_avg_r=-0.5, applied_avg_r=-0.1 → applied better → no harm
    raw = {"n_baseline": 100, "baseline_avg_r": -0.5, "n_applied": 80, "applied_avg_r": -0.1}
    prev = {"phase": "CANARY_LOW", "last_phase_change_ms": 0}
    now_ms = int(cfg.step_dwell_h * 3_600_000) + 1_000
    d = evaluate_bucket("SHORT|trending_bull", raw, prev, cfg, now_ms)
    assert d.proposed_transition == "CANARY_MID"


def test_evaluate_promotion_blocked_when_applied_harms():
    cfg = _cfg()
    # applied worse than (baseline - no_harm_tol=0.10) but not by rollback_margin=0.30 →
    # promotion blocked (harm) without triggering rollback.
    # baseline=-0.5; no-harm floor = -0.6; rollback floor = -0.8
    # applied=-0.7 violates no-harm but is above rollback floor.
    raw = {"n_baseline": 100, "baseline_avg_r": -0.5, "n_applied": 80, "applied_avg_r": -0.7}
    prev = {"phase": "CANARY_LOW", "last_phase_change_ms": 0}
    now_ms = int(cfg.step_dwell_h * 3_600_000) + 1_000
    d = evaluate_bucket("SHORT|trending_bull", raw, prev, cfg, now_ms)
    assert d.proposed_transition is None
    assert d.phase == "CANARY_LOW"  # not rolled back, just held
    assert "harm" in d.transition_reason


def test_evaluate_rollback_when_applied_severely_underperforms():
    cfg = _cfg()
    # applied much worse than baseline by more than rollback_margin
    raw = {"n_baseline": 100, "baseline_avg_r": -0.3, "n_applied": 80, "applied_avg_r": -1.5}
    prev = {"phase": "CANARY_LOW", "last_phase_change_ms": 0}
    now_ms = 60_000
    d = evaluate_bucket("SHORT|trending_bull", raw, prev, cfg, now_ms)
    assert d.phase == "ROLLED_BACK"
    assert d.bias_value == 0.0
    assert d.rollback_count == 1
    assert "rollback" in d.transition_reason


def test_evaluate_rollback_sticky():
    cfg = _cfg()
    raw = {"n_baseline": 100, "baseline_avg_r": -0.3, "n_applied": 80, "applied_avg_r": 0.5}
    prev = {"phase": "ROLLED_BACK", "last_phase_change_ms": 0, "rollback_count": 1}
    now_ms = int(cfg.step_dwell_h * 3_600_000) + 1_000
    d = evaluate_bucket("SHORT|trending_bull", raw, prev, cfg, now_ms)
    assert d.phase == "ROLLED_BACK"
    assert d.proposed_transition is None
    assert "sticky" in d.transition_reason


def test_evaluate_terminal_high_does_not_promote():
    cfg = _cfg()
    raw = {"n_baseline": 100, "baseline_avg_r": -0.5, "n_applied": 80, "applied_avg_r": -0.1}
    prev = {"phase": "CANARY_HIGH", "last_phase_change_ms": 0}
    now_ms = int(cfg.step_dwell_h * 3_600_000) + 1_000
    d = evaluate_bucket("SHORT|trending_bull", raw, prev, cfg, now_ms)
    assert d.proposed_transition is None


# ─────────────────────────────────────────────────────────────────────
# commit_transition — interplay with enforce + advisory
# ─────────────────────────────────────────────────────────────────────


def _decision(phase="OBSERVE", proposed="CANARY_LOW") -> BucketDecision:
    return BucketDecision(
        key="SHORT|trending_bull",
        phase=phase,
        bias_value=PHASE_BIAS[phase],
        n_baseline=100, baseline_avg_r=-0.5,
        n_applied=80, applied_avg_r=-0.1,
        dwell_h=48.0, last_phase_change_ms=0, rollback_count=0,
        proposed_transition=proposed,
    )


def test_commit_shadow_does_not_advance_phase():
    d = _decision()
    commit_transition(d, advisory_blocks=False, enforce=False, now_ms=12345)
    assert d.phase == "OBSERVE"
    assert d.bias_value == 0.0
    assert d.proposed_transition is None
    assert "shadow_no_enforce" in d.transition_reason


def test_commit_enforce_advances_phase():
    d = _decision()
    commit_transition(d, advisory_blocks=False, enforce=True, now_ms=12345)
    assert d.phase == "CANARY_LOW"
    assert d.bias_value == PHASE_BIAS["CANARY_LOW"]
    assert d.last_phase_change_ms == 12345
    assert d.dwell_h == 0.0


def test_commit_llm_veto_blocks_promotion_even_with_enforce():
    d = _decision()
    commit_transition(d, advisory_blocks=True, enforce=True, now_ms=12345)
    assert d.phase == "OBSERVE"
    assert d.proposed_transition is None
    assert "llm_veto" in d.transition_reason


def test_commit_noop_when_no_proposal():
    d = _decision(proposed=None)  # type: ignore[arg-type]
    commit_transition(d, advisory_blocks=False, enforce=True, now_ms=12345)
    assert d.phase == "OBSERVE"


# ─────────────────────────────────────────────────────────────────────
# publish_state — schema + HMAC
# ─────────────────────────────────────────────────────────────────────


def test_publish_state_schema_and_hmac():
    cfg = _cfg(hmac_secret="topsecret")
    fake = MagicMock()
    d = _decision()
    publish_state(fake, {"SHORT|trending_bull": d}, cfg, n_trades=42)
    fake.set.assert_called_once()
    args, kwargs = fake.set.call_args
    key, payload_json = args[0], args[1]
    assert key == STATE_KEY
    payload = json.loads(payload_json)
    assert payload["schema_version"] == SCHEMA_VERSION
    assert payload["n_trades"] == 42
    assert "sig" in payload
    assert payload["buckets"]["SHORT|trending_bull"]["phase"] == "OBSERVE"


def test_publish_state_no_hmac_when_no_secret():
    cfg = _cfg(hmac_secret="")
    fake = MagicMock()
    publish_state(fake, {"SHORT|trending_bull": _decision()}, cfg, n_trades=0)
    payload = json.loads(fake.set.call_args[0][1])
    assert "sig" not in payload


# ─────────────────────────────────────────────────────────────────────
# run_once — end-to-end with mock redis
# ─────────────────────────────────────────────────────────────────────


def test_run_once_always_lists_tracked_buckets():
    cfg = _cfg()
    fake = MagicMock()
    fake.xrevrange.return_value = []
    fake.get.return_value = None
    out = run_once(fake, cfg)
    # All tracked buckets present even when there are no trades
    assert set(out.keys()) == TRACKED_BUCKETS
    for d in out.values():
        assert d.phase == "OBSERVE"
        assert d.bias_value == 0.0


def test_run_once_promotion_blocked_in_shadow():
    cfg = _cfg(enforce=False)
    fake = MagicMock()
    # Build entries that show a leak in SHORT|trending_bull
    entries = [
        ("id1", {"direction": "SHORT", "entry_regime": "trending_bull",
                 "r_multiple": "-0.5", "edge_directional_bias_value": "0.0",
                 "close_ts_ms": "1000", "is_virtual": "0"})
        for _ in range(60)
    ]
    fake.xrevrange.return_value = entries
    # Previous snapshot: bucket in OBSERVE for long enough
    prev_snapshot = {
        "buckets": {
            "SHORT|trending_bull": {
                "phase": "OBSERVE", "last_phase_change_ms": 0,
            }
        }
    }
    fake.get.return_value = json.dumps(prev_snapshot)
    out = run_once(fake, cfg)
    bucket = out["SHORT|trending_bull"]
    # Numerically eligible but shadow_no_enforce → still OBSERVE
    assert bucket.phase == "OBSERVE"
    assert "shadow_no_enforce" in bucket.transition_reason


def test_run_once_promotion_in_enforce_mode():
    cfg = _cfg(enforce=True)
    fake = MagicMock()
    entries = [
        ("id1", {"direction": "SHORT", "entry_regime": "trending_bull",
                 "r_multiple": "-0.5", "edge_directional_bias_value": "0.0",
                 "close_ts_ms": "1000", "is_virtual": "0"})
        for _ in range(60)
    ]
    fake.xrevrange.return_value = entries
    prev_snapshot = {
        "buckets": {
            "SHORT|trending_bull": {
                "phase": "OBSERVE", "last_phase_change_ms": 0,
            }
        }
    }
    fake.get.return_value = json.dumps(prev_snapshot)
    out = run_once(fake, cfg)
    bucket = out["SHORT|trending_bull"]
    assert bucket.phase == "CANARY_LOW"
    assert bucket.bias_value == PHASE_BIAS["CANARY_LOW"]


# ─────────────────────────────────────────────────────────────────────
# Phase ladder integrity
# ─────────────────────────────────────────────────────────────────────


def test_send_telegram_xadds_envelope():
    from orderflow_services.edge_directional_bias_autocal_v1 import _send_telegram
    fake = MagicMock()
    cfg = _cfg(notify_telegram=True, notify_stream="notify:telegram")
    _send_telegram(fake, cfg=cfg, event="phase_transition", text="<b>x</b>")
    fake.xadd.assert_called_once()
    args, kwargs = fake.xadd.call_args
    assert args[0] == "notify:telegram"
    envelope = args[1]
    assert envelope["event"] == "phase_transition"
    assert envelope["text"] == "<b>x</b>"
    assert envelope["parse_mode"] == "HTML"
    assert envelope["subtype"] == "edb_autocal"
    assert kwargs["maxlen"] == 5_000


def test_send_telegram_noop_when_disabled():
    from orderflow_services.edge_directional_bias_autocal_v1 import _send_telegram
    fake = MagicMock()
    cfg = _cfg(notify_telegram=False)
    _send_telegram(fake, cfg=cfg, event="startup", text="hi")
    fake.xadd.assert_not_called()


def test_send_telegram_fail_open_on_redis_error():
    from orderflow_services.edge_directional_bias_autocal_v1 import _send_telegram
    fake = MagicMock()
    fake.xadd.side_effect = RuntimeError("boom")
    cfg = _cfg(notify_telegram=True)
    # Must not raise
    _send_telegram(fake, cfg=cfg, event="llm_advisory", text="oops")


def test_format_phase_transition_msg_contains_arrows_and_stats():
    from orderflow_services.edge_directional_bias_autocal_v1 import (
        _format_phase_transition_msg,
    )
    d = _decision(phase="CANARY_LOW", proposed=None)  # type: ignore[arg-type]
    d.bias_value = 0.03
    d.transition_reason = "eligible:OBSERVE->CANARY_LOW"
    msg = _format_phase_transition_msg("SHORT|trending_bull", "OBSERVE", "CANARY_LOW", d)
    assert "SHORT|trending_bull" in msg
    assert "OBSERVE" in msg and "CANARY_LOW" in msg
    assert "0.03" in msg
    assert "🔺" in msg  # bias went up


def test_format_phase_transition_msg_rollback_emoji():
    from orderflow_services.edge_directional_bias_autocal_v1 import (
        _format_phase_transition_msg,
    )
    d = _decision(phase="ROLLED_BACK", proposed=None)  # type: ignore[arg-type]
    d.bias_value = 0.0
    d.transition_reason = "rollback:applied=-0.9"
    msg = _format_phase_transition_msg("SHORT|trending_bull", "CANARY_LOW", "ROLLED_BACK", d)
    assert "🔻" in msg
    assert "ROLLED_BACK" in msg


def test_format_llm_msg_classifies_verdict():
    from orderflow_services.edge_directional_bias_autocal_v1 import _format_llm_msg
    d = _decision()
    d.llm_advisory = {"guarded_recommendations": [{"action": "propose_threshold_canary"}]}
    assert "allowed" in _format_llm_msg(d)
    d.llm_advisory = {"blocked_recommendations": [{"reason": "blocked_action"}]}
    assert "vetoed" in _format_llm_msg(d)
    d.llm_advisory = {"skipped": "llm_disabled"}
    assert "skipped" in _format_llm_msg(d)


def test_run_once_emits_phase_transition_telegram_in_enforce():
    cfg = _cfg(enforce=True, notify_telegram=True)
    fake = MagicMock()
    entries = [
        ("id1", {"direction": "SHORT", "entry_regime": "trending_bull",
                 "r_multiple": "-0.5", "edge_directional_bias_value": "0.0",
                 "close_ts_ms": "1000", "is_virtual": "0"})
        for _ in range(60)
    ]
    fake.xrevrange.return_value = entries
    prev_snapshot = {
        "buckets": {
            "SHORT|trending_bull": {"phase": "OBSERVE", "last_phase_change_ms": 0}
        }
    }
    fake.get.return_value = json.dumps(prev_snapshot)
    run_once(fake, cfg)
    # Look for any xadd call to notify:telegram with event=phase_transition
    notify_calls = [
        c for c in fake.xadd.call_args_list
        if c.args and c.args[0] == "notify:telegram"
        and c.args[1].get("event") == "phase_transition"
    ]
    assert len(notify_calls) >= 1


def test_phase_ladder_is_monotone_in_bias():
    biases = [PHASE_BIAS[p] for p in PHASE_LADDER]
    assert biases == sorted(biases)
    assert biases[0] == 0.0  # OBSERVE
    assert biases[-1] == 0.06  # CANARY_HIGH
    assert PHASE_BIAS["ROLLED_BACK"] == 0.0


# ─────────────────────────────────────────────────────────────────────
# _parse_trade — realistic trades:closed payload smoke tests
# ─────────────────────────────────────────────────────────────────────


def test_parse_trade_uses_market_regime_fallback():
    """Audit P0 fix: redis_repo.save_closed writes `market_regime` as the
    canonical field; autocal MUST accept it alongside entry_regime/regime.
    Without this fallback the autocal silently skips all trades whose
    upstream rename dropped entry_regime but kept market_regime.
    """
    from orderflow_services.edge_directional_bias_autocal_v1 import _parse_trade

    row = {
        "side": "SHORT",
        "market_regime": "trending_bull",
        "r_multiple": "-0.7",
        "edge_directional_bias_value": "0.03",
        "edge_directional_bias_source": "autocal",
        "edge_directional_bias_countertrend": "1",
        "close_ts_ms": "1700000000000",
        "is_virtual": "0",
    }
    parsed = _parse_trade(row)
    assert parsed is not None
    assert parsed["direction"] == "SHORT"
    assert parsed["regime"] == "trending_bull"
    assert parsed["r"] == -0.7
    assert parsed["bias_applied"] == 0.03


def test_parse_trade_accepts_realistic_save_closed_payload():
    """End-to-end-shape check: the dict matches what redis_repo.save_closed
    writes to trades:closed (direction, market_regime, r_multiple,
    edge_directional_bias_value, ts_close, is_virtual). Catches both the
    `direction` vs `side` rename and the regime-key rename in one go.
    """
    from orderflow_services.edge_directional_bias_autocal_v1 import (
        TRACKED_BUCKETS,
        _parse_trade,
        aggregate_per_bucket,
    )

    rows = [
        {
            "direction": "SHORT",
            "side": "SHORT",
            "entry_regime": "trending_bull",
            "market_regime": "trending_bull",
            "regime": "trending_bull",
            "r_multiple": "-0.40",
            "edge_directional_bias_value": "0.030000",
            "edge_directional_bias_source": "autocal",
            "edge_directional_bias_countertrend": "1",
            "close_ts_ms": "1700000060000",
            "ts_close": "1700000060000",
            "is_virtual": "0",
        }
        for _ in range(10)
    ]
    parsed_opt = [_parse_trade(r) for r in rows]
    assert all(p is not None for p in parsed_opt)
    parsed = [p for p in parsed_opt if p is not None]
    assert all(p["bias_applied"] == 0.03 for p in parsed)
    buckets = aggregate_per_bucket(parsed, include_virtual=True)
    assert "SHORT|trending_bull" in TRACKED_BUCKETS
    b = buckets["SHORT|trending_bull"]
    # All ten rows have bias>0 → applied bucket, NOT baseline.
    assert b["n_applied"] == 10
    assert b["n_baseline"] == 0


def test_parse_trade_baseline_when_bias_value_zero_or_missing():
    """bias=0 OR bias_field absent → counts as baseline."""
    from orderflow_services.edge_directional_bias_autocal_v1 import _parse_trade

    zero_row = {
        "side": "SHORT", "market_regime": "trending_bull",
        "r_multiple": "-0.2", "edge_directional_bias_value": "0.0",
        "close_ts_ms": "1700000000000", "is_virtual": "0",
    }
    missing_row = {
        "side": "SHORT", "market_regime": "trending_bull",
        "r_multiple": "-0.2",
        "close_ts_ms": "1700000000000", "is_virtual": "0",
    }
    p_zero = _parse_trade(zero_row)
    p_missing = _parse_trade(missing_row)
    assert p_zero is not None and p_zero["bias_applied"] == 0.0
    assert p_missing is not None and p_missing["bias_applied"] == 0.0
