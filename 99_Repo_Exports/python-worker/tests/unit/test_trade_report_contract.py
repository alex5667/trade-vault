import pytest

from common.trade_report_contract import (
    compute_hold_ms_with_quarantine,
    normalize_close_bucket,
    extract_tp_flags_from_pos,
    compute_baseline_pnl_net_usd,
    clamp_one_r_money,
)


class MetricsStub:
    def __init__(self) -> None:
        self.counters = {}

    def inc(self, name: str, n: int = 1) -> None:
        self.counters[name] = int(self.counters.get(name, 0)) + int(n)

    def get(self, name: str) -> int:
        return int(self.counters.get(name, 0))


class QuarantineStub:
    def __init__(self) -> None:
        self.items = []

    def push(self, reason: str, data: dict) -> None:
        self.items.append({"reason": reason, "data": dict(data)})

    def __len__(self) -> int:
        return len(self.items)


class PosStub:
    def __init__(
        self,
        tp1_hit=False,
        tp2_hit=False,
        tp3_hit=False,
        tp_hits=0,
        trailing_started=False,
        trailing_active=False,
        trailing_moves_count=0,
    ) -> None:
        self.tp1_hit = tp1_hit
        self.tp2_hit = tp2_hit
        self.tp3_hit = tp3_hit
        self.tp_hits = tp_hits
        self.trailing_started = trailing_started
        self.trailing_active = trailing_active
        self.trailing_moves_count = trailing_moves_count


def test_duration_negative_goes_quarantine():
    metrics = MetricsStub()
    q = QuarantineStub()

    entry_ts_ms = 1700000000000  # ms
    exit_ts_ms = 1700000000      # sec (unit mismatch)

    hold_ms, quarantined = compute_hold_ms_with_quarantine(
        entry_ts_ms=entry_ts_ms,
        exit_ts_ms=exit_ts_ms,
        quarantine=q,
        metrics=metrics,
        max_back_ms=0,
        unit_mismatch_guard=True,
    )

    assert hold_ms == 0
    assert quarantined is True
    assert len(q) == 1
    assert metrics.get("trade.bad_time.quarantined") == 1
    assert q.items[0]["reason"] in ("ts_unit_mismatch", "exit_before_entry")


def test_close_reason_mapping_trail_sl_is_not_initial_sl():
    b1 = normalize_close_bucket(
        close_reason_raw_bucket="SL",
        pnl_net=-1.0,
        tp_hits=0,
        trailing_started=False,
        trailing_active=False,
        sl_moved_to_be=False,
        time_quarantined=False,
    )
    assert b1 in ("SL",)

    b2 = normalize_close_bucket(
        close_reason_raw_bucket="SL",
        pnl_net=+0.5,
        tp_hits=1,
        trailing_started=False,
        trailing_active=False,
        sl_moved_to_be=True,
        time_quarantined=False,
    )
    assert b2 in ("TRAIL_SL", "MOVED_SL")

    b3 = normalize_close_bucket(
        close_reason_raw_bucket="SL",
        pnl_net=+0.2,
        tp_hits=0,
        trailing_started=True,
        trailing_active=True,
        sl_moved_to_be=False,
        time_quarantined=False,
    )
    assert b3 in ("TRAIL_SL", "MOVED_SL")

    b4 = normalize_close_bucket(
        close_reason_raw_bucket="TP",
        pnl_net=+0.3,
        tp_hits=1,
        trailing_started=False,
        trailing_active=False,
        sl_moved_to_be=False,
        time_quarantined=False,
    )
    assert b4 == "TP"


def test_tp_touch_flags_survive_pipeline():
    pos = PosStub(tp1_hit=True, tp_hits=1, trailing_started=True, trailing_moves_count=2)
    flags = extract_tp_flags_from_pos(pos)

    assert flags["tp1_hit"] is True
    assert flags["tp2_hit"] is False
    assert flags["tp3_hit"] is False
    assert flags["tp_hits"] == 1
    assert flags["trailing_started"] is True
    assert flags["trailing_active"] is False
    assert flags["trailing_moves"] == 2


def test_baseline_pnl_nonzero_when_tp_sl_present():
    pnl_net_baseline = compute_baseline_pnl_net_usd(
        entry_price=100.0,
        baseline_exit_price=102.0,
        is_long=True,
        lot=1.0,
        contract_size=1.0,
        fees_usd=0.10,
    )
    assert pnl_net_baseline == pytest.approx(1.90, abs=1e-12)


def test_risk_usd_clamp_applied_and_counted():
    metrics = MetricsStub()
    one_r_eff, clamped = clamp_one_r_money(
        one_r_money=1e-9,
        fees_usd=0.50,
        min_risk_usd=1.0,
        fees_risk_mult=3.0,  # 0.5*3=1.5 -> floor=1.5
        metrics=metrics,
    )
    assert one_r_eff == pytest.approx(1.5, abs=1e-12)
    assert clamped is True
    assert metrics.get("trade.risk.one_r_clamped") == 1
