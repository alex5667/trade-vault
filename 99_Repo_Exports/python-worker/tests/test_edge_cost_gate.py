from utils.time_utils import get_ny_time_millis
import time
from types import SimpleNamespace

import pytest

from fake_redis import FakeRedis
from handlers.crypto_orderflow.utils.edge_cost_gate import EdgeCostGate

# NOTE: _ctx(...) helper and existing tests are already in this file.

def _put_slipema_v2(
    r: FakeRedis,
    *,
    symbol: str,
    venue: str,
    session: str,
    tf: str,
    kind: str,
    ema_bps: float,
    samples: int = 100,
) -> str:
    """
    Helper: writes EMA hash exactly in the format that edge_cost_gate._load_slippage_ema_bps() expects.
    """
    key = f"slipema:{symbol}:{venue}:{session}:{tf}:{kind}"
    r.hset(key, mapping={"samples": str(samples), "ema_slippage_bps": str(float(ema_bps))})
    return key


def _ctx(entry: float | None = None, tp1: float | None = None, spread_bps: float | None = None):
    ctx = SimpleNamespace()
    if entry is not None:
        ctx.entry_price = float(entry)
        ctx.entry = float(entry)
        ctx.price = float(entry)
    if tp1 is not None:
        ctx.tp1_price = float(tp1)
        ctx.tp1 = float(tp1)
    if spread_bps is not None:
        ctx.spread_bps = float(spread_bps)
    return ctx


def test_edge_cost_gate_tp1_veto_when_expected_move_below_threshold(monkeypatch: pytest.MonkeyPatch):
    """
    Verify gate vetoes when expected_move < threshold.
    
    Setup:
      - K = 4.0
      - fees = 8 bps (round-trip)
      - slippage = 4 bps (no spread, so just default)
      - threshold = 4 * (8 + 4) = 48 bps
      - expected = |100.03 - 100.00| / 100.00 * 10000 = 3 bps
    
    Expected: veto (3 < 48)
    """
    # Gate enabled only for breakout
    monkeypatch.setenv("EDGE_COST_GATE_ENABLED", "1")
    monkeypatch.setenv("EDGE_COST_APPLY_KINDS", "breakout")
    monkeypatch.setenv("EDGE_EXPECTED_MOVE_MODE", "tp1")
    monkeypatch.setenv("EDGE_COST_K", "4.0")
    monkeypatch.setenv("EDGE_FEES_BPS_DEFAULT", "8.0")
    monkeypatch.setenv("EDGE_SLIPPAGE_BPS_DEFAULT", "4.0")
    monkeypatch.setenv("EDGE_SLIPPAGE_USE_SPREAD_HALF", "0")
    monkeypatch.setenv("EDGE_COST_STRICT_MISSING_LEVELS", "0")
    # Deterministic TS handling — prevents ts=None from triggering veto slippage
    monkeypatch.setenv("EDGE_DISABLE_EMA", "1")
    monkeypatch.setenv("EDGE_TS_BAD_POLICY", "correct_skip_ema")
    monkeypatch.setenv("EDGE_DRIFT_TIGHTEN", "0")
    monkeypatch.setenv("EDGE_BUFFER_BASE_BPS", "0.0")
    monkeypatch.setenv("EDGE_BUFFER_ATR_MULT", "0.0")
    monkeypatch.setenv("EDGE_BUFFER_SPREAD_MULT", "0.0")

    gate = EdgeCostGate.from_env()
    # entry=100, tp1=100.30 => 30 bps expected move
    ctx = _ctx(entry=100.0, tp1=100.30)

    d = gate.evaluate(ctx=ctx, kind="breakout", symbol="BTCUSDT")
    assert d.apply is True
    assert d.veto is True
    assert d.expected_move_bps == pytest.approx(30.0, abs=0.2)
    # threshold = K*(fees+slip)=4*(8+4)=48
    assert d.threshold_bps == pytest.approx(48.0, abs=1e-9)
    assert d.reason_code == EdgeCostGate.REASON_BELOW_K


def test_edge_cost_gate_not_applied_for_other_kind(monkeypatch: pytest.MonkeyPatch):
    """
    Verify gate does not apply when kind is not in apply_kinds.
    """
    monkeypatch.setenv("EDGE_COST_GATE_ENABLED", "1")
    monkeypatch.setenv("EDGE_COST_APPLY_KINDS", "breakout")
    monkeypatch.setenv("EDGE_EXPECTED_MOVE_MODE", "tp1")
    monkeypatch.setenv("EDGE_DISABLE_EMA", "1")
    monkeypatch.setenv("EDGE_TS_BAD_POLICY", "correct_skip_ema")
    monkeypatch.setenv("EDGE_DRIFT_TIGHTEN", "0")
    monkeypatch.setenv("EDGE_BUFFER_BASE_BPS", "0.0")
    monkeypatch.setenv("EDGE_BUFFER_ATR_MULT", "0.0")
    monkeypatch.setenv("EDGE_BUFFER_SPREAD_MULT", "0.0")

    gate = EdgeCostGate.from_env()
    ctx = _ctx(entry=100.0, tp1=100.01)

    d = gate.evaluate(ctx=ctx, kind="obi_spike", symbol="BTCUSDT")
    assert d.apply is False
    assert d.veto is False


def test_edge_cost_gate_missing_levels_fail_open_by_default(monkeypatch: pytest.MonkeyPatch):
    """
    Verify gate is fail-open when levels are missing and strict=0 (default).
    """
    monkeypatch.setenv("EDGE_COST_GATE_ENABLED", "1")
    monkeypatch.setenv("EDGE_COST_APPLY_KINDS", "breakout")
    monkeypatch.setenv("EDGE_EXPECTED_MOVE_MODE", "tp1")
    monkeypatch.setenv("EDGE_COST_STRICT_MISSING_LEVELS", "0")
    monkeypatch.setenv("EDGE_DISABLE_EMA", "1")
    monkeypatch.setenv("EDGE_TS_BAD_POLICY", "correct_skip_ema")
    monkeypatch.setenv("EDGE_DRIFT_TIGHTEN", "0")
    monkeypatch.setenv("EDGE_BUFFER_BASE_BPS", "0.0")
    monkeypatch.setenv("EDGE_BUFFER_ATR_MULT", "0.0")
    monkeypatch.setenv("EDGE_BUFFER_SPREAD_MULT", "0.0")

    gate = EdgeCostGate.from_env()
    # No tp1 => expected_move is NaN => fail-open
    ctx = _ctx(entry=100.0, tp1=None)
    d = gate.evaluate(ctx=ctx, kind="breakout", symbol="BTCUSDT")
    assert d.apply is True
    assert d.veto is False
    assert d.reason_code == EdgeCostGate.REASON_OK


def test_edge_cost_gate_missing_levels_strict_veto(monkeypatch: pytest.MonkeyPatch):
    """
    Verify gate vetoes when levels are missing and strict=1.
    """
    monkeypatch.setenv("EDGE_COST_GATE_ENABLED", "1")
    monkeypatch.setenv("EDGE_COST_APPLY_KINDS", "breakout")
    monkeypatch.setenv("EDGE_EXPECTED_MOVE_MODE", "tp1")
    monkeypatch.setenv("EDGE_COST_STRICT_MISSING_LEVELS", "1")
    monkeypatch.setenv("EDGE_DISABLE_EMA", "1")
    monkeypatch.setenv("EDGE_TS_BAD_POLICY", "correct_skip_ema")
    monkeypatch.setenv("EDGE_DRIFT_TIGHTEN", "0")
    monkeypatch.setenv("EDGE_BUFFER_BASE_BPS", "0.0")
    monkeypatch.setenv("EDGE_BUFFER_ATR_MULT", "0.0")
    monkeypatch.setenv("EDGE_BUFFER_SPREAD_MULT", "0.0")

    gate = EdgeCostGate.from_env()
    ctx = _ctx(entry=100.0, tp1=None)
    d = gate.evaluate(ctx=ctx, kind="breakout", symbol="BTCUSDT")
    assert d.apply is True
    assert d.veto is True
    assert d.reason_code == EdgeCostGate.REASON_MISSING_LEVELS


def test_estimate_slippage_bps_ts_bad_policy_veto_returns_huge(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Regression guard:
      EDGE_TS_BAD_POLICY=veto + bad ts -> slippage returns huge bps,
      which forces cost-edge gate to veto (when expected_move is finite).
    """
    monkeypatch.setenv("EDGE_TS_BAD_POLICY", "veto")
    monkeypatch.setenv("EDGE_TS_BAD_VETO_BPS", "1000000")
    monkeypatch.setenv("EDGE_TS_MAX_SKEW_MS", "21600000")
    monkeypatch.setenv("EDGE_DISABLE_EMA", "0")

    from handlers.crypto_orderflow.utils.edge_cost_gate import estimate_slippage_bps

    r = FakeRedis()
    ctx = SimpleNamespace(bid=100.0, ask=100.02, tf="1m", kind="absorption")

    # bad ts => returns >= veto floor (independent from EMA availability)
    slip = estimate_slippage_bps(
        ctx,
        redis_client=r,
        symbol="BTCUSDT",
        venue="binance_futures",
        ts_ms=0,
        kind="absorption",
        default_bps=5.0,
        use_spread_half=True,
    )
    assert slip >= 1_000_000.0


def test_estimate_slippage_bps_correct_skip_ema_skips_ema_only_when_ts_bad(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    The intended semantics for 'correct_skip_ema':
      - if ts BAD: correct for audit, but SKIP EMA (base only)
      - if ts OK : USE EMA (normal behavior)
    """
    monkeypatch.setenv("EDGE_TS_BAD_POLICY", "correct_skip_ema")
    monkeypatch.setenv("EDGE_TS_MAX_SKEW_MS", "21600000")
    monkeypatch.setenv("EDGE_DISABLE_EMA", "0")
    monkeypatch.setenv("EDGE_SLIP_EMA_MIN_SAMPLES", "20")

    from domain.time_utils import session_from_ts_ms
    from handlers.crypto_orderflow.utils.edge_cost_gate import estimate_slippage_bps

    r = FakeRedis()
    now_ms = get_ny_time_millis()
    sess = str(session_from_ts_ms(now_ms)).lower()

    # Put EMA=50 bps into Redis for this exact (symbol×venue×session×tf×kind)
    _put_slipema_v2(
        r,
        symbol="BTCUSDT",
        venue="binance_futures",
        session=sess,
        tf="1m",
        kind="absorption",
        ema_bps=50.0,
        samples=100,
    )

    ctx = SimpleNamespace(bid=100.0, ask=100.02, tf="1m", kind="absorption")

    # 1) BAD ts => EMA must be skipped => base only (default dominates spread/2)
    slip_bad = estimate_slippage_bps(
        ctx,
        redis_client=r,
        symbol="BTCUSDT",
        venue="binance_futures",
        ts_ms=0,
        kind="absorption",
        default_bps=5.0,
        use_spread_half=True,
    )
    assert slip_bad == 5.0

    # 2) OK ts => EMA should be used => max(base, ema) = 50
    slip_ok = estimate_slippage_bps(
        ctx,
        redis_client=r,
        symbol="BTCUSDT",
        venue="binance_futures",
        ts_ms=now_ms,
        kind="absorption",
        default_bps=5.0,
        use_spread_half=True,
    )
    assert slip_ok == 50.0


def test_estimate_slippage_bps_seconds_timestamp_is_normalized_and_uses_ema(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Insurance test: if ts passed in seconds (epoch seconds),
    normalize_ts_ms() must convert to ms and EMA lookup must still work.
    """
    monkeypatch.setenv("EDGE_TS_BAD_POLICY", "correct_skip_ema")
    monkeypatch.setenv("EDGE_TS_MAX_SKEW_MS", "21600000")
    monkeypatch.setenv("EDGE_DISABLE_EMA", "0")
    monkeypatch.setenv("EDGE_SLIP_EMA_MIN_SAMPLES", "20")

    from domain.time_utils import session_from_ts_ms
    from handlers.crypto_orderflow.utils.edge_cost_gate import estimate_slippage_bps

    r = FakeRedis()
    now_s = int(time.time())
    now_ms = now_s * 1000
    sess = str(session_from_ts_ms(now_ms)).lower()

    _put_slipema_v2(
        r,
        symbol="BTCUSDT",
        venue="binance_futures",
        session=sess,
        tf="1m",
        kind="absorption",
        ema_bps=80.0,
        samples=100,
    )

    ctx = SimpleNamespace(bid=100.0, ask=100.02, tf="1m", kind="absorption")
    slip = estimate_slippage_bps(
        ctx,
        redis_client=r,
        symbol="BTCUSDT",
        venue="binance_futures",
        ts_ms=now_s,  # seconds on purpose
        kind="absorption",
        default_bps=5.0,
        use_spread_half=True,
    )
    assert slip == 80.0


def _force_gate_tp1_defaults(gate) -> None:
    """
    Делает gate детерминированным для теста, независимо от текущих ENV-дефолтов from_env().
    Это важно, чтобы тест не ломался при изменениях конфигов.
    """
    gate.enabled = True
    gate.mode = "tp1"  # ExpectedMoveMode = Literal["tp1", "rr", "atr", "ev"]
    gate.strict_missing_levels = False
    gate.apply_kinds = set()  # применять ко всем kind

    # Делаем пороги предсказуемыми:
    gate.k_default = 2.0
    gate.k_by_symbol = {}
    gate.fees_bps_default = 1.0
    gate.slippage_bps_default = 5.0
    gate.slippage_use_spread_half = True

    gate.min_expected_move_bps_default = 0.0
    gate.min_expected_move_bps_by_symbol = {}

    # EV knobs не важны для TP1, но фиксируем для стабильности:
    gate.ev_p_min = 0.0
    gate.ev_p_min_by_kind = {}
    gate.ev_min_trades = 0
    gate.ev_strict_missing_stats = False
    gate.ev_dynamic_k_enabled = False
    gate.ev_dynamic_k_atr_mult = 0.0


def test_edge_cost_gate_bad_ts_soft_vs_veto_policy(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Интеграционный (на уровне gate.evaluate):
      - при EDGE_TS_BAD_POLICY=correct_skip_ema и ts=0:
          EMA пропускаем, но gate НЕ обязан резать сигнал (если expected_move достаточно)
      - при EDGE_TS_BAD_POLICY=veto и ts=0:
          slippage становится огромным => gate обязан veto (при конечном expected_move)
    """
    from handlers.crypto_orderflow.utils.edge_cost_gate import EdgeCostGate

    # Общие env для теста (важно: EMA включена, чтобы отличать "skip-ema" от "veto")
    monkeypatch.setenv("EDGE_DISABLE_EMA", "0")
    monkeypatch.setenv("EDGE_TS_MAX_SKEW_MS", "21600000")  # 6h
    monkeypatch.setenv("EDGE_TS_BAD_VETO_BPS", "1000000")

    # --- CASE A: soft (bad ts => skip EMA, but do not force veto) ---
    monkeypatch.setenv("EDGE_TS_BAD_POLICY", "correct_skip_ema")
    gate_a = EdgeCostGate.from_env()
    _force_gate_tp1_defaults(gate_a)

    ctx_a = _ctx(entry=100.0, tp1=101.0, spread_bps=2.0)  # expected_move ~= 100 bps
    ctx_a.symbol = "BTCUSDT"
    ctx_a.venue = "binance_futures"
    ctx_a.tf = "1m"
    ctx_a.ts_ms = 0  # BAD ts

    dec_a = gate_a.evaluate(ctx=ctx_a, kind="absorption", symbol="BTCUSDT")
    assert dec_a.apply is True
    assert dec_a.veto is False
    # slippage остаётся "базовой" (default vs spread/2)
    assert 0.0 < float(dec_a.slippage_bps) < 1000.0

    # --- CASE B: hard (bad ts => force veto via huge slippage) ---
    monkeypatch.setenv("EDGE_TS_BAD_POLICY", "veto")
    gate_b = EdgeCostGate.from_env()
    _force_gate_tp1_defaults(gate_b)

    ctx_b = _ctx(entry=100.0, tp1=101.0, spread_bps=2.0)
    ctx_b.symbol = "BTCUSDT"
    ctx_b.venue = "binance_futures"
    ctx_b.tf = "1m"
    ctx_b.ts_ms = 0  # BAD ts

    dec_b = gate_b.evaluate(ctx=ctx_b, kind="absorption", symbol="BTCUSDT")
    assert dec_b.apply is True
    assert dec_b.veto is True
    assert float(dec_b.slippage_bps) >= 1_000_000.0

