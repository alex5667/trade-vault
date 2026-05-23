"""Micro-benchmark for SignalPipeline._enrich_atr_floor_indicators.

Used to validate the ATR_FLOOR_ENRICHMENT_EARLY rollout: measures per-call cost
of the helper to confirm moving it before gate cascade doesn't bloat hot path.

Usage:
    python -m tools.bench_atr_floor_enrichment
"""
from __future__ import annotations

import statistics
import time
from types import SimpleNamespace
from typing import Any

from core.dyn_cfg_keys import DynCfgKeys as DK
from services.orderflow.signal_pipeline import SignalPipeline


class _StubPipeline:
    FEES_BPS_RT = 6.0
    TP_BPS_BUFFER = 2.0

    def _get_rocket_multiplier(self, symbol: str) -> float:  # noqa: ARG002
        return 1.0

    _enrich_atr_floor_indicators = SignalPipeline._enrich_atr_floor_indicators


def _build_runtime() -> Any:
    dyn = {
        DK.ATR_FLOOR_T0_BPS: 3.0,
        DK.ATR_FLOOR_T1_BPS: 5.0,
        DK.ATR_FLOOR_T2_BPS: 8.0,
        DK.ATR_CALIB_READY: 1,
        DK.ATR_BPS_SRC: "calibrated",
        DK.ATR_BPS_N: 42,
    }
    return SimpleNamespace(symbol="BTCUSDT", last_regime="trend", dynamic_cfg=dyn, config={})


def main(iterations: int = 100_000) -> None:
    pipe = _StubPipeline()
    runtime = _build_runtime()
    cfg = {"tp_ratio": "0.5,0.5"}

    for _ in range(1000):
        pipe._enrich_atr_floor_indicators(
            indicators={}, runtime=runtime, cfg=cfg, entry=65000.0, atr=50.0,
        )

    samples_ns: list[int] = []
    for _ in range(iterations):
        indicators: dict[str, Any] = {}
        t0 = time.perf_counter_ns()
        pipe._enrich_atr_floor_indicators(
            indicators=indicators, runtime=runtime, cfg=cfg, entry=65000.0, atr=50.0,
        )
        samples_ns.append(time.perf_counter_ns() - t0)

    samples_us = [n / 1000.0 for n in samples_ns]
    samples_us.sort()

    def p(q: float) -> float:
        idx = int(len(samples_us) * q)
        return samples_us[min(idx, len(samples_us) - 1)]

    print(f"iterations:     {iterations}")
    print(f"mean:           {statistics.mean(samples_us):.2f} us")
    print(f"median (p50):   {p(0.50):.2f} us")
    print(f"p95:            {p(0.95):.2f} us")
    print(f"p99:            {p(0.99):.2f} us")
    print(f"max:            {samples_us[-1]:.2f} us")


if __name__ == "__main__":
    main()
