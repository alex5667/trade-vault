


class _SpecStub:
    """Minimal SymbolSpec stub required by finalize_trade()."""

    contract_size = 1.0

    def calculate_fees(self, *, entry_price, exit_price, lot, side, duration_ms):
        # keep deterministic in tests
        return 0.0


def _mk_pos(*, entry_ts_ms: int = 1_000_000, tp1_ts_ms: int = 1_001_000):
    # Import inside helper so tests can run in repo context.
    from domain.models import PositionState

    pos = PositionState(
        id="p1",
        sid="s1",
        strategy="k1",
        source="CryptoOrderFlow",
        symbol="BTCUSDT",
        tf="1m",
        direction="LONG",
        entry_price=100.0,
        entry_ts_ms=int(entry_ts_ms),
        lot=1.0,
        remaining_qty=1.0,
        sl=99.0,
        tp_levels=[101.0, 102.0, 103.0],
        signal_payload={},
    )
    pos.tp1_hit = True
    pos.tp_hits = 1
    pos.tp_fill_times[1] = int(tp1_ts_ms)
    return pos


def test_nosl_flags_sl_within_bucket(monkeypatch):
    """
    Integration-ish test (domain layer):
      TP1 hit at tp1_ts
      Stop-like close (SL) within 500ms
      -> sl_within_tp1_t500 = 1
      -> nosl_after_tp1_t500 = 0
    """
    monkeypatch.setenv("NOSL_AFTER_TP1_BUCKETS_MS", "500,2000")
    monkeypatch.setenv("NOSL_AFTER_TP1_STOP_REASONS", "SL,TRAILING_STOP")

    from domain.handlers import finalize_trade

    pos = _mk_pos(tp1_ts_ms=1_001_000)
    spec = _SpecStub()

    closed = finalize_trade(
        pos,
        spec,
        exit_price=99.0,
        exit_ts_ms=1_001_400,  # 400ms after TP1
        close_reason_raw="SL",
        tp_ratios=[0.33, 0.33, 0.34],
    )

    assert closed.nosl_after_tp1_applicable == 1
    assert closed.tp1_hit_ts_ms == 1_001_000
    assert closed.sl_after_tp1_elapsed_ms == 400
    assert closed.sl_within_tp1_t500 == 1
    assert closed.nosl_after_tp1_t500 == 0
    assert closed.sl_within_tp1_t2000 == 1
    assert closed.nosl_after_tp1_t2000 == 0


def test_nosl_flags_sl_outside_bucket(monkeypatch):
    """
    TP1 hit, SL happens after 3000ms:
      - within 500/2000 -> false
      - nosl -> true for both buckets
    """
    monkeypatch.setenv("NOSL_AFTER_TP1_BUCKETS_MS", "500,2000")
    monkeypatch.setenv("NOSL_AFTER_TP1_STOP_REASONS", "SL,TRAILING_STOP")

    from domain.handlers import finalize_trade

    pos = _mk_pos(tp1_ts_ms=1_001_000)
    spec = _SpecStub()

    closed = finalize_trade(
        pos,
        spec,
        exit_price=99.0,
        exit_ts_ms=1_004_000,  # 3000ms after TP1
        close_reason_raw="SL",
        tp_ratios=[0.33, 0.33, 0.34],
    )

    assert closed.nosl_after_tp1_applicable == 1
    assert closed.sl_after_tp1_elapsed_ms == 3000
    assert closed.sl_within_tp1_t500 == 0
    assert closed.nosl_after_tp1_t500 == 1
    assert closed.sl_within_tp1_t2000 == 0
    assert closed.nosl_after_tp1_t2000 == 1


def test_nosl_flags_not_applicable_without_tp1(monkeypatch):
    monkeypatch.setenv("NOSL_AFTER_TP1_BUCKETS_MS", "500,2000")

    from domain.handlers import finalize_trade
    from domain.models import PositionState

    pos = PositionState(
        id="p2",
        sid="s2",
        strategy="k1",
        source="CryptoOrderFlow",
        symbol="BTCUSDT",
        tf="1m",
        direction="LONG",
        entry_price=100.0,
        entry_ts_ms=1_000_000,
        lot=1.0,
        remaining_qty=1.0,
        sl=99.0,
        tp_levels=[101.0, 102.0, 103.0],
        signal_payload={},
    )
    pos.tp1_hit = False
    spec = _SpecStub()

    closed = finalize_trade(
        pos,
        spec,
        exit_price=102.0,
        exit_ts_ms=1_010_000,
        close_reason_raw="TP2",
        tp_ratios=[0.33, 0.33, 0.34],
    )

    assert closed.nosl_after_tp1_applicable == 0
    assert closed.sl_after_tp1_elapsed_ms == 0
    assert closed.sl_within_tp1_t500 == 0
    assert closed.nosl_after_tp1_t500 == 0
