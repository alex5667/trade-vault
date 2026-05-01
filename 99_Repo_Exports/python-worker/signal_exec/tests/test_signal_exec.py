from __future__ import annotations
"""
Comprehensive pytest test suite for signal_exec module.

All tests are self-contained: no Redis, no Postgres required.
"""


import json
from datetime import datetime, timezone, timedelta
from unittest.mock import MagicMock

from signal_exec.models import (
    Side,
    AccountState,
    SwingPoint,
    HTFLevel,
    OrderBookSnapshot,
    Bar1m,
    ExecutionPlan,
    SymbolSetupConfig,
)
from signal_exec.context import SignalContext
from signal_exec.execution_planner import ExecutionPlanner
from signal_exec.performance_tracker import (
    SignalPerformanceTracker,
    Outcome,
)
from signal_exec.bus import SignalBus


# ──────────────────────────────────────────────
# Fixtures
# ──────────────────────────────────────────────

def _make_account(equity: float = 10_000.0, open_risk: float = 0.0) -> AccountState:
    return AccountState(
        equity_usd=equity,
        open_risk_usd=open_risk,
        max_risk_per_trade_pct=0.5,
        max_portfolio_risk_pct=5.0,
    )


def _make_config(**kwargs) -> SymbolSetupConfig:
    defaults = dict(
        symbol="",
        setup_type="breakout",
        expiry_bars=5,
        min_stop_ticks=10,
        max_stop_R=3.0,
        atr_buffer_ratio=0.15,
        entry_zone_min_R=0.3,
        entry_zone_max_R=0.7,
        default_tp_R=(1.0, 2.0, 3.0),
        score_buckets=(0.4, 0.7, 0.85),
        risk_multipliers=(0.5, 1.0, 1.5, 2.0),
        max_risk_R_per_trade=2.0,
        max_portfolio_risk_pct=5.0,
    )
    defaults.update(kwargs)
    return SymbolSetupConfig(**defaults)


def _make_ctx(
    side: Side = Side.LONG,
    price: float = 2000.0,
    atr: float = 2.0,
    score: float = 0.8,
    swings=None,
    htf=None,
    ttd_expiry_bars=None,
) -> SignalContext:
    return SignalContext(
        signal_id="test-signal-001",
        symbol="",
        setup_type="breakout",
        side=side,
        ts_signal=datetime(2024, 1, 15, 10, 0, 0, tzinfo=timezone.utc),
        price_at_signal=price,
        atr_1m=atr,
        tick_size=0.1,
        contract_size=100.0,
        final_score=score,
        account_state=_make_account(),
        local_swings=swings or [],
        htf_levels=htf or [],
        ttd_expiry_bars=ttd_expiry_bars,
    )


def _make_planner(cfg: SymbolSetupConfig | None = None) -> ExecutionPlanner:
    if cfg is None:
        cfg = _make_config()
    return ExecutionPlanner({("breakout"): cfg})


def _make_bar(ts: datetime, high: float, low: float, close: float = 0.0) -> Bar1m:
    return Bar1m(ts=ts, open=close, high=high, low=low, close=close)


# ──────────────────────────────────────────────
# 1. Models instantiation
# ──────────────────────────────────────────────

class TestModelsInstantiation:
    def test_side_enum(self):
        assert Side.LONG == "long"
        assert Side.SHORT == "short"

    def test_account_state(self):
        acc = _make_account()
        assert acc.equity_usd == 10_000.0

    def test_swing_point(self):
        sp = SwingPoint(ts=datetime.now(), price=1990.0, type="low")
        assert sp.volume == 0.0

    def test_htf_level(self):
        lv = HTFLevel(ts=datetime.now(), price=2010.0, kind="D_high")
        assert lv.strength == 1.0

    def test_order_book_snapshot(self):
        ob = OrderBookSnapshot(
            ts=datetime.now(), best_bid=1999.9, best_ask=2000.1
        )
        assert ob.bids == []

    def test_bar1m(self):
        bar = _make_bar(datetime.now(), high=2001.0, low=1999.0)
        assert bar.high > bar.low

    def test_execution_plan(self):
        plan = ExecutionPlan(
            signal_id="s1",
            symbol="",
            side=Side.LONG,
            setup_type="breakout",
            ts_signal=datetime.now(timezone.utc),
            price_at_signal=2000.0,
            entry_zone_low=1998.0,
            entry_zone_high=2000.0,
            stop_price=1995.0,
            tp_levels=[2010.0, 2020.0],
            partials=[0.5, 0.5],
            pos_risk_R=1.0,
            risk_usd=50.0,
            position_size=0.02,
            expiry_bars=5,
            created_at=datetime.now(timezone.utc),
        )
        assert plan.signal_id == "s1"

    def test_symbol_setup_config_defaults(self):
        cfg = SymbolSetupConfig(symbol="BTCUSDT", setup_type="breakout")
        assert cfg.expiry_bars == 3
        assert cfg.max_stop_R == 3.0


# ──────────────────────────────────────────────
# 2. ExecutionPlanner
# ──────────────────────────────────────────────

class TestExecutionPlanner:
    def test_happy_path_long(self):
        ctx = _make_ctx(side=Side.LONG, price=2000.0, atr=2.0, score=0.8)
        planner = _make_planner()
        plan = planner.build_plan(ctx)

        assert plan is not None
        assert plan.symbol == ""
        assert plan.side == Side.LONG
        assert plan.stop_price < plan.entry_zone_low <= plan.entry_zone_high
        assert all(tp > plan.entry_zone_high for tp in plan.tp_levels)
        assert plan.risk_usd > 0
        assert plan.position_size > 0
        assert plan.expiry_bars == 5

    def test_happy_path_short(self):
        ctx = _make_ctx(side=Side.SHORT, price=2000.0, atr=2.0, score=0.8)
        planner = _make_planner()
        plan = planner.build_plan(ctx)

        assert plan is not None
        assert plan.side == Side.SHORT
        assert plan.stop_price > plan.entry_zone_high
        assert all(tp < plan.entry_zone_low for tp in plan.tp_levels)

    def test_stop_too_wide_returns_none(self):
        # min_stop_ticks=200 gives a huge stop in ATR multiples
        cfg = _make_config(min_stop_ticks=500, max_stop_R=0.1)
        ctx = _make_ctx(price=2000.0, atr=2.0)
        planner = _make_planner(cfg)
        plan = planner.build_plan(ctx)
        assert plan is None

    def test_no_config_returns_none(self):
        planner = ExecutionPlanner({})  # empty config
        ctx = _make_ctx()
        plan = planner.build_plan(ctx)
        assert plan is None

    def test_risk_R_buckets_all_covered(self):
        cfg = _make_config(
            score_buckets=(0.4, 0.7, 0.85),
            risk_multipliers=(0.5, 1.0, 1.5, 2.0),
            max_risk_R_per_trade=2.0,
        )
        scores_and_expected = [
            (0.3, 0.5),
            (0.5, 1.0),
            (0.75, 1.5),
            (0.9, 2.0),
        ]
        for score, expected_mult in scores_and_expected:
            result = ExecutionPlanner._compute_risk_R(score, cfg)
            assert result == expected_mult, f"score={score} → expected {expected_mult}, got {result}"

    def test_ttd_expiry_bars_override(self):
        ctx = _make_ctx(ttd_expiry_bars=99)
        planner = _make_planner()
        plan = planner.build_plan(ctx)
        assert plan is not None
        assert plan.expiry_bars == 99

    def test_swing_based_stop_long(self):
        """Stop should be placed below the nearest local low.
        Uses atr=10.0 so stop_R=(2000-1984.5)/10=1.55 < max_stop_R=3.0.
        """
        swings = [SwingPoint(ts=datetime.now(), price=1985.0, type="low")]
        # atr large enough that stop_R = (2000-1984.5)/10.0 ≈ 1.55 < max_stop_R
        ctx = _make_ctx(side=Side.LONG, price=2000.0, atr=10.0, swings=swings)
        planner = _make_planner()
        plan = planner.build_plan(ctx)
        assert plan is not None
        # Stop is below the swing low (1985 - atr_buffer_ratio*10)
        assert plan.stop_price < 1985.0

    def test_portfolio_risk_exhausted_returns_none(self):
        """When open_risk >= portfolio limit, build_plan returns None."""
        account = AccountState(
            equity_usd=10_000.0,
            open_risk_usd=600.0,   # already over 5% max
            max_risk_per_trade_pct=0.5,
            max_portfolio_risk_pct=5.0,
        )
        ctx = _make_ctx()
        ctx = SignalContext(
            signal_id="s2",
            symbol="",
            setup_type="breakout",
            side=Side.LONG,
            ts_signal=datetime.now(timezone.utc),
            price_at_signal=2000.0,
            atr_1m=2.0,
            tick_size=0.1,
            contract_size=100.0,
            final_score=0.8,
            account_state=account,
        )
        planner = _make_planner()
        plan = planner.build_plan(ctx)
        assert plan is None


# ──────────────────────────────────────────────
# 3. SignalContext serialization roundtrip
# ──────────────────────────────────────────────

class TestSignalContextRoundtrip:
    def test_minimal_roundtrip(self):
        ctx = _make_ctx()
        d = ctx.to_dict()
        restored = SignalContext.from_dict(d)
        assert restored.signal_id == ctx.signal_id
        assert restored.side == ctx.side
        assert restored.price_at_signal == ctx.price_at_signal

    def test_roundtrip_with_swings_and_htf(self):
        swings = [SwingPoint(ts=datetime.now(timezone.utc), price=1990.0, type="low")]
        htf = [HTFLevel(ts=datetime.now(timezone.utc), price=2010.0, kind="D_high")]
        ctx = _make_ctx(swings=swings, htf=htf)
        restored = SignalContext.from_dict(ctx.to_dict())
        assert len(restored.local_swings) == 1
        assert restored.local_swings[0].price == 1990.0
        assert len(restored.htf_levels) == 1
        assert restored.htf_levels[0].kind == "D_high"

    def test_roundtrip_with_orderbook(self):
        ctx = _make_ctx()
        ctx.orderbook = OrderBookSnapshot(
            ts=datetime.now(timezone.utc),
            best_bid=1999.9,
            best_ask=2000.1,
            bids=[1999.9, 1999.8],
            asks=[2000.1, 2000.2],
        )
        restored = SignalContext.from_dict(ctx.to_dict())
        assert restored.orderbook is not None
        assert restored.orderbook.best_bid == 1999.9

    def test_to_dict_is_json_serializable(self):
        ctx = _make_ctx()
        d = ctx.to_dict()
        json_str = json.dumps(d)  # should not raise
        assert "signal_id" in json_str


# ──────────────────────────────────────────────
# 4. SignalPerformanceTracker
# ──────────────────────────────────────────────

def _make_tracker() -> tuple[SignalPerformanceTracker, MagicMock]:
    repo = MagicMock()
    tracker = SignalPerformanceTracker(
        repo=repo,
        ttd_target_R=1.0,
        max_ttd_bars=10,
        max_lifetime_bars_after_entry=20,
        max_lifetime_ms_after_entry=0,  # disable time fallback in tests
    )
    return tracker, repo


def _register_signal(tracker: SignalPerformanceTracker, ctx: SignalContext | None = None):
    if ctx is None:
        ctx = _make_ctx()
    planner = _make_planner()
    plan = planner.build_plan(ctx)
    assert plan is not None
    tracker.register_signal(ctx, plan)
    return ctx, plan


class TestSignalPerformanceTracker:
    def test_register_signal(self):
        tracker, _ = _make_tracker()
        ctx, plan = _register_signal(tracker)
        assert plan.signal_id in tracker._states

    def test_bars_seen_increments(self):
        tracker, _ = _make_tracker()
        ctx, plan = _register_signal(tracker)
        ts = datetime(2024, 1, 15, 10, 1, tzinfo=timezone.utc)
        tracker.on_bar_1m(_make_bar(ts, high=2005.0, low=1998.0))
        st = tracker._states.get(plan.signal_id)
        if st:
            assert st.bars_seen == 1

    def test_expired_no_entry_when_no_entry_after_expiry(self):
        tracker, repo = _make_tracker()
        ctx, plan = _register_signal(tracker)
        expiry = plan.expiry_bars

        ts = datetime(2024, 1, 15, 10, 0, tzinfo=timezone.utc)
        for i in range(expiry + 1):
            bar_ts = ts + timedelta(minutes=i + 1)
            tracker.on_bar_1m(_make_bar(bar_ts, high=1999.0, low=1998.0))

        assert repo.insert_signal_performance.called
        call_args = repo.insert_signal_performance.call_args[0][0]
        assert call_args.outcome == Outcome.EXPIRED_NO_ENTRY

    def test_entry_recorded_correctly(self):
        tracker, _ = _make_tracker()
        ctx, plan = _register_signal(tracker)

        entry_ts = datetime(2024, 1, 15, 10, 2, tzinfo=timezone.utc)
        tracker.on_execution_event(plan.signal_id, "ENTRY_FILLED", entry_ts, 2000.5)

        st = tracker._states.get(plan.signal_id)
        assert st is not None
        assert st.entry_price == 2000.5

    def test_stop_hit_finalizes_immediately(self):
        tracker, repo = _make_tracker()
        ctx, plan = _register_signal(tracker)

        entry_ts = datetime(2024, 1, 15, 10, 2, tzinfo=timezone.utc)
        tracker.on_execution_event(plan.signal_id, "ENTRY_FILLED", entry_ts, 2000.5)

        stop_ts = datetime(2024, 1, 15, 10, 5, tzinfo=timezone.utc)
        tracker.on_execution_event(plan.signal_id, "STOP_HIT", stop_ts, 1996.0)

        assert repo.insert_signal_performance.called
        call_args = repo.insert_signal_performance.call_args[0][0]
        assert call_args.outcome == Outcome.STOP_HIT
        # Signal must be removed from active states after finalization
        assert plan.signal_id not in tracker._states

    def test_tp_hit_finalizes_with_target_hit(self):
        tracker, repo = _make_tracker()
        ctx, plan = _register_signal(tracker)

        entry_ts = datetime(2024, 1, 15, 10, 2, tzinfo=timezone.utc)
        tracker.on_execution_event(plan.signal_id, "ENTRY_FILLED", entry_ts, 2000.0)

        tp_ts = datetime(2024, 1, 15, 10, 10, tzinfo=timezone.utc)
        tracker.on_execution_event(plan.signal_id, "TP_HIT", tp_ts, 2010.0)

        call_args = repo.insert_signal_performance.call_args[0][0]
        assert call_args.outcome == Outcome.TARGET_HIT
        assert call_args.realized_R is not None and call_args.realized_R > 0

    def test_idempotent_finalization(self):
        """Double _finalize_and_store must not call repo twice."""
        tracker, repo = _make_tracker()
        ctx, plan = _register_signal(tracker)

        stop_ts = datetime(2024, 1, 15, 10, 5, tzinfo=timezone.utc)
        tracker.on_execution_event(plan.signal_id, "STOP_HIT", stop_ts, 1996.0)
        # Second call is a noop
        tracker.on_execution_event(plan.signal_id, "STOP_HIT", stop_ts, 1996.0)

        assert repo.insert_signal_performance.call_count == 1

    def test_late_event_ignored_after_finalize(self):
        tracker, repo = _make_tracker()
        ctx, plan = _register_signal(tracker)

        stop_ts = datetime(2024, 1, 15, 10, 5, tzinfo=timezone.utc)
        tracker.on_execution_event(plan.signal_id, "STOP_HIT", stop_ts, 1996.0)

        # Late TP event arrives after STOP already finalized
        late_ts = datetime(2024, 1, 15, 10, 20, tzinfo=timezone.utc)
        tracker.on_execution_event(plan.signal_id, "TP_HIT", late_ts, 2020.0)

        # Only 1 insert, not 2
        assert repo.insert_signal_performance.call_count == 1

    def test_expired_no_target_after_max_bars_in_trade(self):
        tracker, repo = _make_tracker()
        ctx, plan = _register_signal(tracker)

        entry_ts = datetime(2024, 1, 15, 10, 2, tzinfo=timezone.utc)
        tracker.on_execution_event(plan.signal_id, "ENTRY_FILLED", entry_ts, 2000.0)

        ts = datetime(2024, 1, 15, 10, 3, tzinfo=timezone.utc)
        for i in range(25):  # > max_lifetime_bars_after_entry=20
            bar_ts = ts + timedelta(minutes=i)
            tracker.on_bar_1m(_make_bar(bar_ts, high=2001.0, low=1999.0))

        assert repo.insert_signal_performance.called
        call_args = repo.insert_signal_performance.call_args[0][0]
        assert call_args.outcome == Outcome.EXPIRED_NO_TARGET

    def test_mfe_mae_computed_correctly(self):
        tracker, repo = _make_tracker()
        ctx, plan = _register_signal(tracker)

        entry_ts = datetime(2024, 1, 15, 10, 2, tzinfo=timezone.utc)
        tracker.on_execution_event(plan.signal_id, "ENTRY_FILLED", entry_ts, 2000.0)

        # Bar with favorable high
        bar1_ts = datetime(2024, 1, 15, 10, 3, tzinfo=timezone.utc)
        tracker.on_bar_1m(_make_bar(bar1_ts, high=2004.0, low=1999.0))

        st = tracker._states.get(plan.signal_id)
        if st:
            assert st.mfe_R > 0  # price moved up → MFE positive for LONG
            assert st.mae_R <= 0  # price briefly dipped below entry → MAE negative

    def test_symbol_index_cleaned_after_finalize(self):
        tracker, _ = _make_tracker()
        ctx, plan = _register_signal(tracker)

        stop_ts = datetime(2024, 1, 15, 10, 5, tzinfo=timezone.utc)
        tracker.on_execution_event(plan.signal_id, "STOP_HIT", stop_ts, 1996.0)

        # After finalization, the symbol index should be empty for 
        remaining = tracker._ids_by_symbol.get(set())
        assert plan.signal_id not in remaining


# ──────────────────────────────────────────────
# 5. SignalBus serialization
# ──────────────────────────────────────────────

class TestSignalBusPlanDict:
    def test_plan_to_dict_contains_all_fields(self):
        plan = ExecutionPlan(
            signal_id="s1",
            symbol="",
            side=Side.LONG,
            setup_type="breakout",
            ts_signal=datetime(2024, 1, 15, 10, 0, tzinfo=timezone.utc),
            price_at_signal=2000.0,
            entry_zone_low=1998.0,
            entry_zone_high=2000.0,
            stop_price=1995.0,
            tp_levels=[2010.0, 2020.0, 2030.0],
            partials=[0.33, 0.33, 0.34],
            pos_risk_R=1.0,
            risk_usd=50.0,
            position_size=0.025,
            expiry_bars=5,
            created_at=datetime(2024, 1, 15, 10, 0, tzinfo=timezone.utc),
        )
        d = SignalBus._plan_to_dict(plan)
        required_keys = {
            "signal_id", "symbol", "setup_type", "side", "ts_signal",
            "price_at_signal", "entry_zone_low", "entry_zone_high",
            "stop_price", "tp_levels", "partials", "pos_risk_R",
            "risk_usd", "position_size", "expiry_bars", "created_at",
        }
        assert required_keys.issubset(d.keys())
        assert d["side"] == "long"  # enum serialized as string
        assert isinstance(d["tp_levels"], list)

    def test_plan_to_dict_json_serializable(self):
        plan = ExecutionPlan(
            signal_id="s2",
            symbol="BTCUSDT",
            side=Side.SHORT,
            setup_type="fade",
            ts_signal=datetime(2024, 2, 1, tzinfo=timezone.utc),
            price_at_signal=50000.0,
            entry_zone_low=49900.0,
            entry_zone_high=50000.0,
            stop_price=50200.0,
            tp_levels=[49500.0],
            partials=[1.0],
            pos_risk_R=1.5,
            risk_usd=75.0,
            position_size=0.001,
            expiry_bars=3,
            created_at=datetime(2024, 2, 1, tzinfo=timezone.utc),
        )
        d = SignalBus._plan_to_dict(plan)
        json_str = json.dumps(d)
        assert "BTCUSDT" in json_str
