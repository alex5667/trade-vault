from __future__ import annotations

import os
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Dict, Optional, Tuple

import pytest

import importlib.util


def _load_test_harness() -> Tuple[Any, Any]:
    """Load local test harness modules by path (works even if tests/ is not a package)."""
    p = Path(__file__).resolve().parent / "harness" / "fake_redis_v1.py"
    spec = importlib.util.spec_from_file_location("fake_redis_v1", str(p))
    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot load test harness from {p}")
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)  # type: ignore[attr-defined]
    return mod.FakeRedis, mod.FakePublisher



def _find_repo_root(start: Path) -> Path:
    cur = start
    for _ in range(16):
        if (cur / ".git").exists():
            return cur
        if (cur / "services").exists() or (cur / "tick_flow_full").exists():
            return cur
        if cur.parent == cur:
            break
        cur = cur.parent
    return start.parents[2]


def _ensure_repo_on_syspath(repo: Path) -> None:
    s = str(repo)
    if s not in sys.path:
        sys.path.insert(0, s)


def _import_sot_then_mirror(mod_path_sot: str, mod_path_mirror: str):
    """Import helper that prefers SoT (tick_flow_full/...) but supports mirror (services/...)."""
    try:
        return __import__(mod_path_sot, fromlist=["*"])
    except Exception:
        return __import__(mod_path_mirror, fromlist=["*"])


@dataclass
class _DNTiers:
    tier0_usd: float
    tier1_usd: float
    tier2_usd: float
    scale: float = 1.0
    src: str = "test"


class _DummyDNCalib:
    """Minimal tier service to satisfy TickProcessor DN gating section."""

    def tiers(self, regime: str, ts_ms: int, default_t0: float, default_t1: float, default_t2: float):  # noqa: ARG002
        # Ensure tier selection always passes for any positive delta_usd.
        return _DNTiers(tier0_usd=0.0, tier1_usd=0.0, tier2_usd=0.0, src="test")

    def update(self, regime: str, dn_usd: float, ts_ms: int) -> None:  # noqa: ARG002
        return None


class _DummyDeltaDetector:
    def __init__(self, delta: float = 50.0, z: float = 6.0) -> None:
        self._delta = float(delta)
        self._z = float(z)

    def push(self, tick: Dict[str, Any]) -> Dict[str, float]:  # noqa: ARG002
        # TickProcessor requires a truthy dict. It then computes delta_usd = abs(delta) * price.
        return {"delta": self._delta, "z": self._z}


class _DummyTickGaps:
    """Minimal gap tracker expected by TickProcessor._update_strict_dq_trackers."""

    def __init__(self) -> None:
        self._gaps: list[int] = []

    def record_gap(self, dt_ms: int) -> None:
        try:
            self._gaps.append(int(dt_ms))
        except Exception:
            return None

    def snapshot(self) -> Tuple[float, float, int]:
        if not self._gaps:
            return 0.0, 0.0, 0
        xs = sorted(self._gaps)
        n = len(xs)
        p50 = float(xs[int(0.50 * (n - 1))])
        p95 = float(xs[int(0.95 * (n - 1))])
        return p50, p95, n


class _DummySeqGapEMA:
    """EMA over a boolean gap event; deterministic, time-agnostic for tests."""

    def __init__(self, alpha: float = 1.0) -> None:
        self.alpha = float(alpha)
        self.ema: float = 0.0
        self.last_ts_ms: int = 0

    def update(self, is_gap: bool, ts_ms: int) -> float:
        x = 1.0 if bool(is_gap) else 0.0
        a = float(self.alpha)
        if not (0.0 < a <= 1.0):
            a = 1.0
        self.last_ts_ms = int(ts_ms)
        self.ema = (a * x) + ((1.0 - a) * float(self.ema))
        return float(self.ema)


class _DummyOFC:
    """OFConfirm-like object sufficient for tick_processor to proceed and early-return on veto."""

    def __init__(self, *, ok: int, scenario: str, reason: str, evidence: Dict[str, Any]):
        self.ok = int(ok)
        self.scenario = str(scenario)
        self.reason = str(reason)
        self.score = 0.0
        self.have = 0
        self.need = 0
        self.gate_bits = 0
        self.evidence = dict(evidence)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "ok": int(self.ok),
            "scenario": str(self.scenario),
            "reason": str(self.reason),
            "score": float(self.score),
            "evidence": dict(self.evidence),
        }


class _DummyOFEngine:
    """Minimal engine: evaluate DQ gate then force a strong-gate veto to stop the pipeline."""

    def __init__(self, dq_mod: Any) -> None:
        self.dq = dq_mod
        self.last_indicators_pre_dq: Optional[Dict[str, Any]] = None
        self.last_dq_out: Optional[Dict[str, Any]] = None

    def build(self, *, runtime: Any, cfg: Dict[str, Any], indicators: Dict[str, Any], **kwargs: Any):  # noqa: ARG002
        # Capture pre-DQ surface contract (A2).
        self.last_indicators_pre_dq = dict(indicators)

        out = self.dq.eval_dq_gate(indicators, cfg)
        indicators.update(out)
        self.last_dq_out = dict(out)

        # Force the path: require_strong_confirmation + ok_soft=0 => early veto and return None.
        # This keeps the integration test cheap and avoids unrelated downstream logic.
        return _DummyOFC(ok=0, scenario="dq_veto", reason="DQ", evidence={"ok_soft": 0, "scenario_v4": "dq"}), out


def _mk_min_runtime(cfg: Dict[str, Any]) -> Any:
    rt = SimpleNamespace()
    rt.symbol = "BTCUSDT"
    rt.config = dict(cfg)
    rt.dynamic_cfg = {}
    rt.tick_count = 0
    rt.last_tick_ts = 0
    rt.last_regime = "na"
    rt.last_trade_id = 0
    rt.last_obi_event = None
    rt.last_iceberg_event = None

    # DQ trackers
    rt.tick_gaps = _DummyTickGaps()
    rt.tick_seq_gap = _DummySeqGapEMA(alpha=float(cfg.get("dq_tick_seq_ema_alpha", 1.0) or 1.0))

    # DN + delta
    rt.delta_detector = _DummyDeltaDetector(delta=50.0, z=6.0)
    rt.tick_dn_calib = _DummyDNCalib()

    # Book DQ fields (written by BookProcessor)
    rt.book_seq_last_u = 0
    rt.book_seq_last_reason = "init"
    rt.book_missing_seq_ema = 0.0

    class _DummyPassRate:
        def update(self, *args, **kwargs): pass
        def get_rate(self) -> float: return 1.0

    class _DummyPressure:
        def update(self, *args, **kwargs): pass
        def get_ema(self) -> float: return 0.0
        def snapshot(self, **kwargs) -> Any: return SimpleNamespace(per_min_ema=0.0, acc_pressure=0.0, cd_rate_ema=0.0)

    rt.dn_passrate = _DummyPassRate()
    rt.dn_eval_pressure = _DummyPressure()
    rt.pressure = _DummyPressure()

    return rt


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "uptime_sec, expect_veto",
    [
        (3600, 0),       # observe-only: veto blocked
        (90000, 1),      # after window: veto allowed
    ],
)
async def test_tick_processor_runtime_contour_book_gap_observe_only(uptime_sec: int, expect_veto: int):
    """Realistic integration (no monkeypatching TickProcessor internals):

    BookProcessor._update_book_missing_seq -> runtime book DQ state
    TickProcessor.process_tick -> surfaces book_* into indicators (A2)
    OFEngine.build -> calls eval_dq_gate (B1) -> observe-only logic applied
    """
    repo = _find_repo_root(Path(__file__).resolve())
    _ensure_repo_on_syspath(repo)
    _ensure_repo_on_syspath(repo / "python-worker")

    # Prefer SoT paths; support mirror.
    tp_mod = _import_sot_then_mirror(
        "tick_flow_full.services.orderflow.components.tick_processor",
        "services.orderflow.components.tick_processor",
    )
    bp_mod = _import_sot_then_mirror(
        "tick_flow_full.services.orderflow.components.book_processor",
        "services.orderflow.components.book_processor",
    )
    dq_mod = _import_sot_then_mirror(
        "tick_flow_full.core.dq_gate_v1",
        "core.dq_gate_v1",
    )

    # Deterministic uptime snapshot (B1 observe-only window gating).
    dq_mod.runtime_snapshot = lambda event_ts_ms=None: SimpleNamespace(uptime_sec=int(uptime_sec), runtime_start_ts_ms=0)  # type: ignore[attr-defined]

    # Keep this test quiet/cheap:
    os.environ["OF_GATE_METRICS_ENABLE"] = "0"
    os.environ["OF_GATE_DQ_QUARANTINE_ENABLE"] = "0"
    os.environ["DECISION_RECORD_SAMPLE"] = "1.0"
    os.environ["DECISION_RECORD_EARLY_VETO_ENABLE"] = "0"  # avoid background tasks in unit tests

    cfg = {
        # Ensure we reach engine.build regardless of DN tiers.
        "delta_tier_min": -1,

        # Strong gate: force early return (cheap path).
        "require_strong_confirmation": 1,
        "strong_gate_shadow": 0,

        # Enable DQ gate.
        "dq_gate_enable": 1,
        "dq_gate_mode": "veto",

        # Observe-only (B1)
        "dq_book_veto_enabled": True,
        "dq_observe_only_sec": 86400,

        # Thresholds: make book gap sufficient to reach dq_level=2
        "book_hard": 0.05,
        "dq_book_missing_seq_soft": 0.01,

        # Disable other hard triggers to isolate this test.
        "dq_tick_gap_p95_soft_ms": 1e9,
        "dq_tick_gap_p95_hard_ms": 1e9,
        "dq_tick_gap_p95_extreme_ms": 1e9,
        "dq_tick_missing_seq_soft": 1e9,
        "dq_tick_missing_seq_hard": 1e9,
        "dq_data_health_min": 0.0,
        "dq_data_health_hard_min": 0.0,

        # Train==serve determinism for book EMA in this test: jump to 1.0 on first gap.
        "dq_book_seq_ema_alpha": 1.0,
    }

    runtime = _mk_min_runtime(cfg)

    # Book stream simulation: init -> overlap -> GAP -> recovery.
    bp = bp_mod.BookProcessor()
    bp._update_book_missing_seq(runtime, {"U": 101, "u": 105})  # type: ignore[attr-defined]
    bp._update_book_missing_seq(runtime, {"U": 103, "u": 110})  # overlap => ok
    bp._update_book_missing_seq(runtime, {"U": 120, "u": 125})  # GAP => ema -> 1.0

    # Sanity: book DQ state is present before tick path.
    assert float(getattr(runtime, "book_missing_seq_ema", 0.0) or 0.0) >= 0.0
    assert str(getattr(runtime, "book_seq_last_reason", "")) != ""

    _FakeRedis, _FakePublisher = _load_test_harness()

    redis = _FakeRedis()
    ticks = _FakeRedis()
    pub = _FakePublisher()
    engine = _DummyOFEngine(dq_mod)

    tp = tp_mod.TickProcessor(
        redis=redis,
        ticks=ticks,
        publisher=pub,
        of_engine=engine,
        calib_svc=None,
        atr_cache=None,
        atr_sanity=None,
        conf_scorer=None,
    )

    # Use current time to satisfy tick_time_guard without monkeypatching.
    now_ms = int(time.time() * 1000)
    tick = {"ts_ms": now_ms, "price": 100.0, "qty": 1.0, "m": False, "trade_id": 12345}

    out = await tp.process_tick(runtime, tick)

    # Veto path returns None (expected): the assertions are based on captured indicators.
    assert out is None

    # A2: book keys are surfaced into indicators before DQ gate.
    assert engine.last_indicators_pre_dq is not None
    pre = engine.last_indicators_pre_dq
    assert "book_missing_seq_ema" in pre
    assert "book_seq_last_reason" in pre

    # B1: eval_dq_gate produces dq_level always; dq_veto depends on observe-only.
    assert engine.last_dq_out is not None
    dq = engine.last_dq_out
    assert int(dq.get("dq_level", -1)) == 2
    assert int(dq.get("dq_veto", -1)) == int(expect_veto)
    assert str(dq.get("dq_reason_bucket", "")) in ("book_seq", "book", "data_health", "other", "")

