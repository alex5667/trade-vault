from __future__ import annotations

from dataclasses import dataclass, field

from core.seq_gap_tracker_v1 import GapEmaTracker
from core.tick_gap_tracker import TickGapTracker
from services.orderflow.components.tick_processor import TickProcessor


@dataclass
class MiniRuntime:
    """Minimal runtime stub for unit-testing strict DQ helpers without Redis."""

    symbol: str
    config: dict

    tick_count: int = 0

    # Trackers used by _update_strict_dq_trackers
    tick_gaps: TickGapTracker = field(default_factory=lambda: TickGapTracker(window=512))
    tick_seq_gap: GapEmaTracker = field(default_factory=lambda: GapEmaTracker(tau_ms=10_000))

    # Cached DQ outputs
    tick_gap_p50_ms: float = 0.0
    tick_gap_p95_ms: float = 0.0
    tick_gap_n: int = 0

    tick_missing_seq_ema: float = 0.0
    tick_seq_last_reason: str = ""

    # Last seen monotone trade id
    last_trade_id: int = 0

    # Per-microbar counters (normally reset on bar close)
    tick_id_gap_count: int = 0
    tick_id_dup_count: int = 0
    tick_id_reorder_count: int = 0


def test_tick_trade_id_gap_dup_reorder_separation():
    runtime = MiniRuntime("BTCUSDT", config={"tick_gap_snapshot_every_n": 1})

    ind = {}
    TickProcessor._update_strict_dq_trackers(
        runtime=runtime,
        tick={"trade_id": 100},
        tick_ts_ms=1_000,
        cfg_eff=runtime.config,
        indicators=ind,
    )

    assert runtime.last_trade_id == 100
    assert ind.get("tick_gap_count", 0) == 0
    assert ind.get("tick_dup_count", 0) == 0
    assert ind.get("tick_reorder_count", 0) == 0

    # DUP: tid == last_tid
    ind = {}
    TickProcessor._update_strict_dq_trackers(
        runtime=runtime,
        tick={"trade_id": 100},
        tick_ts_ms=1_100,
        cfg_eff=runtime.config,
        indicators=ind,
    )
    assert runtime.last_trade_id == 100
    assert ind.get("tick_dup_count") == 1
    assert ind.get("tick_seq_last_reason") == "dup"
    assert ind.get("tick_id_dup") == 1
    assert ind.get("tick_id_gap") == 0

    # REORDER: tid < last_tid (must NOT regress last_trade_id)
    ind = {}
    TickProcessor._update_strict_dq_trackers(
        runtime=runtime,
        tick={"trade_id": 99},
        tick_ts_ms=1_200,
        cfg_eff=runtime.config,
        indicators=ind,
    )
    assert runtime.last_trade_id == 100
    assert ind.get("tick_reorder_count") == 1
    assert ind.get("tick_seq_last_reason") == "reorder"
    assert ind.get("tick_id_reorder") == 1

    # GAP: tid > last_tid + 1 (missing) -> tick_missing_seq_ema increases
    ind = {}
    TickProcessor._update_strict_dq_trackers(
        runtime=runtime,
        tick={"trade_id": 103},
        tick_ts_ms=1_300,
        cfg_eff=runtime.config,
        indicators=ind,
    )

    assert runtime.last_trade_id == 103
    assert ind.get("tick_gap_count") == 1
    assert ind.get("tick_id_gap") == 1
    assert runtime.tick_missing_seq_ema > 0.0
