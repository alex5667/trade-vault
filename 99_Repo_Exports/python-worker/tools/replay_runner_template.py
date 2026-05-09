from __future__ import annotations

from typing import Any

# Template runner for tools/golden_replay.py and tools/bench_latency.py
#
# Usage example:
#   python -m tools.golden_replay --inputs capture.ndjson --runner tools.replay_runner_template:run_one --write-baseline baseline.ndjson
#
# You MUST adapt the imports/constructor to your project wiring (engine/cfg/runtime stubs).

def run_one(inp: dict[str, Any]) -> dict[str, Any]:
    """Map one captured input dict -> output dict.

    Expected keys in inp (recommended):
      symbol, tf, direction, tick_ts_ms, price, delta_z, indicators, absorption, cfg
    """
    symbol = (inp.get("symbol", ""))
    # TODO: create engine instance once and reuse (module-level singleton) for performance
    # from of_confirm.of_confirm_engine_ml_integrated import OFConfirmEngineMLIntegrated
    # engine = get_engine_singleton()
    #
    # runtime = build_runtime_stub(inp.get("runtime", {}))
    # cfg = inp.get("cfg", {})
    # indicators = inp.get("indicators", {})
    # absorption = inp.get("absorption")
    #
    # ofc, dec = engine.build(...)

    return {
        "decision": int(inp.get("_expected_dec", 0) or 0),
        "score": float(inp.get("_expected_score", 0.0) or 0.0),
        "ml_prob": float(inp.get("_expected_ml_prob", 0.0) or 0.0),
        "symbol": symbol,
    }

