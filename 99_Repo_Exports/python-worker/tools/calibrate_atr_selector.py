"""
ATR Selector Calibration Tool (Phase 1).
Simulates ATRSourceSelector over historical ATR data to find optimal hold_down_ms and jump_max_rel.

Usage:
    python -m tools.calibrate_atr_selector --dataset data/atr_history_v1.ndjson --symbol BTCUSDT --hold-grid 60000,300000,600000,1800000 --jump-grid 0.05,0.1,0.2,0.3
"""

import argparse
import json
import logging
from dataclasses import dataclass
from typing import Any

# Mocking core logic or importing if safe
try:
    from core.atr_tf_calibrator import ATRTfCalibrator, ATRTfChoice
except ImportError:
    # Minimal fallback or let it fail if not in PYTHONPATH
    pass

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("CalibrateATR")

@dataclass
class ATRPoint:
    symbol: str
    tf: str
    src: str
    key: str
    ts_ms: int
    atr: float
    atr_bps: float

def load_dataset(path: str) -> list[ATRPoint]:
    points = []
    with open(path) as f:
        for line in f:
            if not line.strip(): continue
            d = json.loads(line)
            # Support both raw stream dump and structured meta
            points.append(ATRPoint(
                symbol=d.get("symbol", ""),
                tf=d.get("tf", d.get("picked_tf", "")),
                src=d.get("src", d.get("picked_src", "")),
                key=d.get("key", d.get("picked_key", "")),
                ts_ms=int(d["ts_ms"]),
                atr=float(d["atr"]),
                atr_bps=float(d["atr_bps"])
            ))
    return sorted(points, key=lambda x: x.ts_ms)

class Simulator:
    def __init__(self, hold_down_ms: int, jump_max_rel: float):
        self.calib = ATRTfCalibrator(
            hold_down_ms=hold_down_ms,
            jump_max_rel=jump_max_rel
        )
        self.last_choice: Any | None = None
        self.switches = 0
        self.total_jumps = 0.0
        self.points_count = 0
        self.stale_count = 0
        self._max_age = 5 * 60 * 1000 # 5m sanity

    def step(self, symbol: str, candidates: list[Any], now_ms: int):
        # In real worker, we call calib.choose()
        # We need to simulate the stateful nature of calib
        choice = self.calib.choose(
            symbol=symbol,
            candidates=candidates,
            prev_choice=self.last_choice,
            now_ms=now_ms
        )

        if choice:
            if self.last_choice and (choice.tf != self.last_choice.tf or choice.src != self.last_choice.src):
                self.switches += 1

            # Metric: average jump in bps
            if self.last_choice:
                jump = abs(choice.atr_bps - self.last_choice.atr_bps) / max(1.0, self.last_choice.atr_bps)
                self.total_jumps += jump

            self.last_choice = choice
            self.points_count += 1
        return choice

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", required=True)
    parser.add_argument("--symbol", default=None)
    parser.add_argument("--hold-grid", default="60000,300000,600000,1800000")
    parser.add_argument("--jump-grid", default="0.05,0.1,0.2,0.3,0.5")
    args = parser.parse_args()

    data = load_dataset(args.dataset)
    if args.symbol:
        data = [p for p in data if p.symbol == args.symbol]

    if not data:
        logger.error("No data found for simulation.")
        return

    hold_grid = [int(x) for x in args.hold_grid.split(",")]
    jump_grid = [float(x) for x in args.jump_grid.split(",")]

    results = []

    # Simple time-window simulation
    # In reality, candidates arrive at different times. We'll group by "run" interval (e.g. 5s)
    run_interval = 5000
    min_ts = data[0].ts_ms
    max_ts = data[-1].ts_ms

    for h in hold_grid:
        for j in jump_grid:
            sim = Simulator(h, j)

            # Simulation loop
            current_ts = min_ts
            data_ptr = 0

            while current_ts <= max_ts:
                # Gather available candidates for this window
                # (In reality, they might be older, but we assume we see latest known)
                # For simulation, we'll just take the latest point for each (tf, src) seen so far
                candidates_map = {}
                while data_ptr < len(data) and data[data_ptr].ts_ms <= current_ts:
                    p = data[data_ptr]
                    candidates_map[(p.tf, p.src)] = p
                    data_ptr += 1

                # Convert to calibrator expected format (List of objects with tf, src, key, ts_ms, atr, atr_bps)
                cands = []
                for p in candidates_map.values():
                    # We only include "fresh" enough candidates to be realistic
                    if current_ts - p.ts_ms < 10 * 60 * 1000: # 10m freshness
                        # Mock an object with attributes
                        class Obj: pass
                        o = Obj()
                        o.tf = p.tf
                        o.src = p.src
                        o.key = p.key
                        o.ts_ms = p.ts_ms
                        o.atr = p.atr
                        o.atr_bps = p.atr_bps
                        o.age_ms = current_ts - p.ts_ms
                        cands.append(o)

                if cands:
                    sim.step(args.symbol or "GLOBAL", cands, current_ts)

                current_ts += run_interval

            avg_jump = sim.total_jumps / max(1, sim.points_count)
            results.append({
                "hold_down_ms": h,
                "jump_max_rel": j,
                "switches": sim.switches,
                "avg_jump_rel": avg_jump,
                "stability_score": 1.0 / (sim.switches + 1) * (1.0 - min(1.0, avg_jump))
            })

    # Sort by stability score or switches
    results.sort(key=lambda x: x["stability_score"], reverse=True)

    print(f"\n📊 Calibration Results for {args.symbol or 'All Symbols'}:")
    print(f"{'Hold (ms)':>10} | {'Jump (%)':>8} | {'Switches':>8} | {'Avg Jump':>8} | {'Stability':>8}")
    print("-" * 55)
    for r in results[:10]:
        print(f"{r['hold_down_ms']:>10} | {r['jump_max_rel']:>8.2f} | {r['switches']:>8} | {r['avg_jump_rel']:>8.4f} | {r['stability_score']:>8.4f}")

    if results:
        best = results[0]
        print(f"\n✅ Recommended Parameters: HOLD_DOWN_MS={best['hold_down_ms']}, JUMP_MAX_REL={best['jump_max_rel']}")

if __name__ == "__main__":
    main()
