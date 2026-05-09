"""
Unit-тесты для compute_sweep_recent() и compute_reclaim_recent() из core.of_evidence.

Запуск:
    python -m pytest -q \
        tick_flow_full/core/test_of_evidence_recent_flags_v1.py \
        tick_flow_full/core/test_ml_feature_schema_confirmations_v2.py
"""


# Ensure project root is in sys.path (tick_flow_full is not a proper package — нет __init__.py)
# [AUTOGRAVITY CLEANUP] sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from core.of_evidence import compute_reclaim_recent, compute_sweep_recent
from core.reclaim_detector import ReclaimEvent
from core.sweep_detector import SweepEvent

# ---------------------------------------------------------------------------
# Вспомогательные строители событий
# ---------------------------------------------------------------------------

def _mk_sweep(ts_ms: int, kind: str = "EQH_SWEEP", bias: str = "SHORT") -> SweepEvent:
    """Минимально валидный SweepEvent для тестов."""
    return SweepEvent(
        kind=kind,
        direction_bias=bias,
        ts_ms=int(ts_ms),
        pool_id="p1",
        pool_kind="EQH" if "EQH" in kind else "EQL",
        level=100.0,
        touches=2,
        tol_px=0.5,
        breach_ts_ms=int(ts_ms) - 1000,
        breach_px=101.0,
        confirm_px=99.0,
    )


def _mk_reclaim(ts_ms: int, kind: str = "EQH", bias: str = "SHORT", hold_bars: int = 2) -> ReclaimEvent:
    """Минимально валидный ReclaimEvent для тестов."""
    return ReclaimEvent(
        ts_ms=int(ts_ms),
        pool_id="p1",
        kind=kind,
        level=100.0,
        tol_px=0.5,
        hold_bars=int(hold_bars),
        direction_bias=bias,
    )


# ---------------------------------------------------------------------------
# compute_sweep_recent тесты
# ---------------------------------------------------------------------------

def test_compute_sweep_recent_true_within_valid_window() -> None:
    """Sweep в пределах окна → True, sweep_kind и sweep_age_ms заполнены."""
    cfg = {"sweep_valid_ms": 120_000}
    now = 1_000_000
    ev = _mk_sweep(ts_ms=now - 1_500, kind="EQH_SWEEP", bias="SHORT")

    indicators: dict = {}
    ok = compute_sweep_recent(now_ts_ms=now, last_sweep=ev, cfg=cfg, indicators=indicators)

    assert ok is True
    assert indicators["sweep_kind"] == "EQH_SWEEP"
    assert indicators["sweep_age_ms"] == 1_500


def test_compute_sweep_recent_false_when_expired() -> None:
    """Sweep вышел за пределы окна → False, sweep_kind не заполняется, sweep_age_ms всегда есть."""
    cfg = {"sweep_valid_ms": 5_000}
    now = 1_000_000
    ev = _mk_sweep(ts_ms=now - 10_000, kind="EQH_SWEEP", bias="SHORT")

    indicators: dict = {}
    ok = compute_sweep_recent(now_ts_ms=now, last_sweep=ev, cfg=cfg, indicators=indicators)

    assert ok is False
    assert "sweep_kind" not in indicators
    # sweep_age_ms всегда записывается если last_sweep существует
    assert indicators["sweep_age_ms"] == 10_000


# ---------------------------------------------------------------------------
# compute_reclaim_recent тесты
# ---------------------------------------------------------------------------

def test_compute_reclaim_recent_true_when_direction_matches() -> None:
    """Свежий reclaim + направление совпадает → True, hold_bars и диагностика заполнены."""
    cfg = {"reclaim_signal_valid_ms": 120_000}
    now = 2_000_000
    ev = _mk_reclaim(ts_ms=now - 2_000, kind="EQH", bias="LONG", hold_bars=3)

    indicators: dict = {}
    ok, hold_bars = compute_reclaim_recent(
        direction="LONG",
        now_ts_ms=now,
        last_reclaim=ev,
        cfg=cfg,
        indicators=indicators,
    )

    assert ok is True
    assert hold_bars == 3
    assert indicators["reclaim_age_ms"] == 2_000
    assert indicators["reclaim_level"] == 100.0
    assert indicators["reclaim_pool_id"] == "p1"


def test_compute_reclaim_recent_false_when_direction_mismatch() -> None:
    """Reclaim есть, но направление не совпадает → False, диагностика опциональная не заполняется."""
    cfg = {"reclaim_signal_valid_ms": 120_000}
    now = 2_000_000
    ev = _mk_reclaim(ts_ms=now - 1_000, kind="EQH", bias="SHORT", hold_bars=2)

    indicators: dict = {}
    ok, hold_bars = compute_reclaim_recent(
        direction="LONG",
        now_ts_ms=now,
        last_reclaim=ev,
        cfg=cfg,
        indicators=indicators,
    )

    assert ok is False
    assert hold_bars == 0
    # reclaim_age_ms всегда записывается при наличии last_reclaim
    assert indicators["reclaim_age_ms"] == 1_000
    # Опциональные диагностики заполняются только при принятом reclaim
    assert "reclaim_level" not in indicators
    assert "reclaim_pool_id" not in indicators
