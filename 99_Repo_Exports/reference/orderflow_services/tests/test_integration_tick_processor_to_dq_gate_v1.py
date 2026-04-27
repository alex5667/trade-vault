from __future__ import annotations

import importlib.util
import sys
import types
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Dict, Optional, Tuple

import pytest


def find_repo_root(start: Path) -> Path:
    """Best-effort repo root discovery (works for both repo-root and nested checkouts)."""
    cur = start
    for _ in range(15):
        if (cur / ".git").exists():
            return cur
        # Common monorepo markers in this project
        if (cur / "services").exists() or (cur / "tick_flow_full").exists():
            return cur
        if cur.parent == cur:
            break
        cur = cur.parent
    # Fallback: the tests folder is usually under repo_root/orderflow_services/tests/
    return start.parents[2]


def load_module_from_candidates(repo: Path, candidates: list[str], module_name: str):
    # Ensure repo root is importable for absolute imports used by the modules.
    repo_str = str(repo)
    if repo_str not in sys.path:
        sys.path.insert(0, repo_str)
    
    # Also add worker paths so that imports like `from common.time_utils` resolve
    # to the worker's common package rather than the root common package.
    worker_paths = [
        str(repo / "python-worker" / "tick_flow_full"),
        str(repo / "python-worker"),
        str(repo / "reference" / "tick_flow_full"),
        str(repo / "reference" / "137"),
        str(repo / "reference"),
    ]
    for wp in reversed(worker_paths):
        if wp not in sys.path:
            sys.path.insert(0, wp)
    last_err: Optional[Exception] = None
    for rel in candidates:
        p = repo / rel
        if not p.exists():
            continue
        try:
            spec = importlib.util.spec_from_file_location(module_name, str(p))
            if spec is None or spec.loader is None:
                raise ImportError(f"spec/loader missing for {p}")
            mod = importlib.util.module_from_spec(spec)
            sys.modules[module_name] = mod
            spec.loader.exec_module(mod)  # type: ignore[attr-defined]
            return mod, p
        except Exception as exc:
            last_err = exc
            continue
    raise FileNotFoundError(f"No candidate found for {module_name}: {candidates}. Last error: {last_err}")


def _load_tick_processor():
    repo = find_repo_root(Path(__file__).resolve())
    candidates = [
        "python-worker/tick_flow_full/services/orderflow/components/tick_processor.py",
        "python-worker/services/orderflow/components/tick_processor.py",
        "reference/tick_flow_full/services/orderflow/components/tick_processor.py",
        "reference/services/orderflow/components/tick_processor.py",
        "reference/137/services/orderflow/components/tick_processor.py",
    ]
    return load_module_from_candidates(repo, candidates, module_name="tick_processor")


def _load_book_processor():
    repo = find_repo_root(Path(__file__).resolve())
    candidates = [
        "python-worker/tick_flow_full/services/orderflow/components/book_processor.py",
        "python-worker/services/orderflow/components/book_processor.py",
        "reference/tick_flow_full/services/orderflow/components/book_processor.py",
        "reference/services/orderflow/components/book_processor.py",
        "reference/137/services/orderflow/components/book_processor.py",
    ]
    return load_module_from_candidates(repo, candidates, module_name="book_processor")


def _load_dq_gate():
    repo = find_repo_root(Path(__file__).resolve())
    candidates = [
        "python-worker/tick_flow_full/core/dq_gate_v1.py",
        "python-worker/core/dq_gate_v1.py",
        "python-worker/services/core/dq_gate_v1.py",
        "reference/tick_flow_full/core/dq_gate_v1.py",
        "reference/core/dq_gate_v1.py",
        "reference/137/core/dq_gate_v1.py",
    ]
    return load_module_from_candidates(repo, candidates, module_name="dq_gate_v1")


class DummyRedis:
    async def xadd(self, *args: Any, **kwargs: Any) -> str:
        return "0-0"
    async def hgetall(self, *args: Any, **kwargs: Any) -> dict:
        return {}
    async def hgetall_async(self, *args: Any, **kwargs: Any) -> dict:
        return {}
    async def hset(self, *args: Any, **kwargs: Any) -> None:
        pass
    def hset_async(self, *args: Any, **kwargs: Any) -> None:
        pass


@dataclass
class DummyDNTiers:
    tier0_usd: float = 0.0
    tier1_usd: float = 0.0
    tier2_usd: float = 0.0
    src: str = "test"
    scale: float = 1.0


class DummyDNCalib:
    def tiers(self, *args: Any, **kwargs: Any) -> DummyDNTiers:
        # Make DN gate always pass: any positive delta_usd hits tier2.
        return DummyDNTiers()

    def update(self, *args: Any, **kwargs: Any) -> None:
        return None


class DummyPassRate:
    def update(self, *args: Any, **kwargs: Any) -> None:
        return None


class DummyLogSampler:
    def should_log(self, key: str) -> bool:
        return False


class DummyDeltaDetector:
    def __init__(self, delta_usd: float = 2000.0, z: float = 6.0) -> None:
        self._delta = float(delta_usd)
        self._z = float(z)

    def push(self, tick: Dict[str, Any]) -> Dict[str, float]:
        # TickProcessor expects a truthy dict with at least delta and z.
        return {"delta": self._delta, "z": self._z}

class DummyPressure:
    def snapshot(self, *args: Any, **kwargs: Any) -> Any:
        sn = SimpleNamespace()
        sn.per_min_ema = 0.0
        sn.per_5min_ema = 0.0
        sn.per_15min_ema = 0.0
        sn.cd_rate_ema = 0.0
        return sn

class DummySeqGap:
    last_ts_ms: int = 0
    def update(self, *args: Any, **kwargs: Any) -> float:
        return 0.0

class DummyOFC:
    ok: int = 1

    scenario: str = "OK"
    evidence: Dict[str, Any] = {}

    def to_dict(self) -> Dict[str, Any]:
        return {"ok": int(self.ok), "scenario": str(self.scenario), "evidence": dict(self.evidence)}


class DummyEngine:
    def __init__(self, dq_mod: Any):
        self.dq = dq_mod
        self.last_indicators_pre_dq: Optional[Dict[str, Any]] = None

    def build(self, *args: Any, **kwargs: Any) -> Tuple[DummyOFC, Dict[str, Any]]:
        # Signature in TickProcessor:
        # build(symbol, tick, direction, tick_ts, price, delta_z, runtime, cfg, indicators, absorption=...)
        indicators = kwargs.get("indicators")
        if indicators is None and len(args) >= 9:
            indicators = args[8]
        cfg = kwargs.get("cfg")
        if cfg is None and len(args) >= 8:
            cfg = args[7]

        assert isinstance(indicators, dict)
        assert isinstance(cfg, dict)

        # Capture pre-DQ snapshot for assertions (tick_processor must "surface" book keys).
        self.last_indicators_pre_dq = dict(indicators)

        # Evaluate DQ gate and write results into indicators (as of_confirm_engine would).
        out = self.dq.eval_dq_gate(indicators, cfg)
        indicators.update(out)

        return DummyOFC(), out


def _mk_runtime(cfg: Dict[str, Any]) -> Any:
    # Minimal runtime object to make TickProcessor.process_tick reach OFConfirm stage.
    rt = SimpleNamespace()
    rt.symbol = "BTCUSDT"
    rt.config = dict(cfg)
    rt.dynamic_cfg = {}
    rt.tick_count = 0
    rt.last_tick_ts = 0
    rt.last_regime = "na"
    rt.last_obi_event = {}
    rt.last_iceberg_event = {}
    rt.pressure = DummyPressure()
    rt.tick_seq_gap = DummySeqGap()

    # Critical non-optional runtime deps for the DN gate section (not wrapped in try/except).
    rt.delta_detector = DummyDeltaDetector()
    rt.tick_dn_calib = DummyDNCalib()
    rt.dn_passrate = DummyPassRate()
    rt.delta_log_sampler = DummyLogSampler()

    # Book continuity state (to be mutated by BookProcessor).
    rt.book_seq_last_u = 0
    rt.book_seq_last_reason = "init"
    rt.book_seq_last_gap = 0
    rt.book_missing_seq_ema = 0.0

    return rt


async def _patch_tick_time_guard(tp: Any) -> None:
    async def _fake_apply(self: Any, runtime: Any, tick: Dict[str, Any]) -> Dict[str, Any]:
        ts = int(tick.get("ts_ms") or tick.get("T") or tick.get("E") or 0)
        # Provide minimal TT fields that downstream logic may read.
        return {
            "tick_ts_ms": ts,
            "tick_time_age_ms": 0,
            "tick_ts_source": "now",
            "tick_ts_source_now_ema": 0.0,
            "tick_ts_source_stream_id_ema": 0.0,
        }

    tp._apply_tick_time_guard = types.MethodType(_fake_apply, tp)  # type: ignore[attr-defined]


async def _patch_emit_payload(tp: Any) -> None:
    async def _fake_emit(self: Any, runtime: Any, payload: Dict[str, Any], tick_ts_ms: int) -> Dict[str, Any]:
        # Return payload verbatim to allow inspection of payload["indicators"].
        return payload

    tp._emit_payload = types.MethodType(_fake_emit, tp)  # type: ignore[attr-defined]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "uptime_sec, expect_veto",
    [
        (3600, 0),      # observe-only: block veto
        (90000, 1),     # after window: allow veto
    ],
)
async def test_tick_processor_to_indicators_to_eval_dq_gate_observe_only(uptime_sec: int, expect_veto: int):
    """Integration: book_processor -> tick_processor -> eval_dq_gate (+observe-only)."""
    try:
        tp_mod, _ = _load_tick_processor()
    except Exception as exc:
        pytest.skip(f"tick_processor not importable: {exc}")

    try:
        bp_mod, _ = _load_book_processor()
    except Exception as exc:
        pytest.skip(f"book_processor not importable: {exc}")

    try:
        dq_mod, _ = _load_dq_gate()
    except Exception as exc:
        pytest.skip(f"dq_gate_v1 not importable: {exc}")

    # Deterministic uptime (bypass real monotonic clock).
    dq_mod.runtime_snapshot = lambda event_ts_ms=None: SimpleNamespace(uptime_sec=int(uptime_sec), runtime_start_ts_ms=0)

    # Config: deterministic + avoid unrelated vetos.
    cfg = {
        # DN gate: always pass
        "dn_tier_min": 0,
        "dn_tier0_usd": 1.0,
        # DQ gate enabled
        "dq_gate_enable": 1,
        "dq_gate_mode": "veto",
        "dq_book_veto_enabled": True,
        "dq_observe_only_sec": 86400,
        # Force book hard to trigger easily (EMA is binary-ish in our tracker)
        "dq_book_missing_seq_hard": 0.05,
        "dq_book_missing_seq_soft": 0.01,
        # Disable other hard triggers
        "dq_tick_gap_p95_soft_ms": 1e9,
        "dq_tick_gap_p95_hard_ms": 1e9,
        "dq_tick_gap_p95_extreme_ms": 1e9,
        "dq_tick_missing_seq_soft": 1e9,
        "dq_tick_missing_seq_hard": 1e9,
        "dq_data_health_min": 0.0,
        "dq_data_health_hard_min": 0.0,
        # Retain memory of the gap
        "dq_book_seq_ema_alpha": 0.1,
        # Avoid publishing inputs / decision records during unit test
        "publish_of_inputs": 0,
        "min_confirmations": 0,
    }

    runtime = _mk_runtime(cfg)

    # Book stream simulation: init -> overlap -> GAP -> recovery.
    bp = bp_mod.BookProcessor()
    bp._update_book_missing_seq(runtime, {"U": 101, "u": 105})  # type: ignore[attr-defined]
    bp._update_book_missing_seq(runtime, {"U": 103, "u": 110})  # type: ignore[attr-defined]
    bp._update_book_missing_seq(runtime, {"U": 120, "u": 125})  # type: ignore[attr-defined]
    bp._update_book_missing_seq(runtime, {"U": 126, "u": 130})  # type: ignore[attr-defined]

    engine = DummyEngine(dq_mod)

    tp = tp_mod.TickProcessor(  # type: ignore[attr-defined]
        redis=DummyRedis(),
        ticks=DummyRedis(),
        publisher=None,
        of_engine=engine,
        calib_svc=None,
        atr_cache=None,
        atr_sanity=None,
        conf_scorer=None,
    )
    await _patch_tick_time_guard(tp)
    await _patch_emit_payload(tp)

    tick = {
        "ts_ms": 1700000000000,
        "trade_id": 123456,
        "price": 100.0,
        "qty": 1.0,
        "m": False,
    }

    out = await tp.process_tick(runtime, tick)  # type: ignore[attr-defined]
    assert isinstance(out, dict)
    assert "indicators" in out
    ind = out["indicators"]
    assert isinstance(ind, dict)

    # 1) "Surface" contract: book keys must exist in indicators before DQ eval.
    assert engine.last_indicators_pre_dq is not None
    assert "book_missing_seq_ema" in engine.last_indicators_pre_dq
    assert "book_seq_last_reason" in engine.last_indicators_pre_dq

    # 2) DQ output must exist in final indicators.
    for k in ("dq_level", "dq_veto", "dq_reason", "dq_reason_bucket", "dq_reasons"):
        assert k in ind

    assert int(ind["dq_level"]) == 2
    assert int(ind["dq_veto"]) == int(expect_veto)
