from __future__ import annotations

from types import SimpleNamespace
from typing import Any
import contextlib

# Production runner for tools/golden_replay.py and tools/bench_latency.py
#
# Usage example:
#   python -m tools.golden_replay --inputs capture.ndjson --runner tools.replay_runner_prod:run_one --write-baseline baseline.ndjson
#
# Adapted from tools/of_confirm_replay_from_inputs.py to match strategy.py structure

_engine_singleton: Any = None


def _mk_runtime(inp: dict[str, Any]) -> Any:
    """
    Minimal runtime stub: must satisfy whatever OFConfirmEngine.build reads.
    Strategy passes `runtime=runtime` where runtime has .symbol and .config at minimum.
    """
    sym = (inp.get("symbol", "") or "")
    cfg = inp.get("runtime_config") if isinstance(inp.get("runtime_config"), dict) else None
    if cfg is None and isinstance(inp.get("runtime", {}), dict):
        cfg = inp.get("runtime", {}).get("config") if isinstance(inp.get("runtime", {}).get("config"), dict) else None
    if cfg is None:
        cfg = {}
    # include micro_tf if present (common in your strategy)
    if "micro_tf" in inp and "micro_tf" not in cfg:
        cfg["micro_tf"] = inp.get("micro_tf")
    if "tf" in inp and "micro_tf" not in cfg:
        cfg["micro_tf"] = inp.get("tf")
    return SimpleNamespace(symbol=sym, config=cfg)


def _mk_engine() -> Any:
    """
    Try to construct engine in a robust way:
    - OFConfirmEngine.from_env() if exists
    - else OFConfirmEngine() default ctor
    """
    from core.of_confirm_engine import OFConfirmEngine  # type: ignore
    if hasattr(OFConfirmEngine, "from_env") and callable(OFConfirmEngine.from_env):
        return OFConfirmEngine.from_env()  # type: ignore
    return OFConfirmEngine()  # type: ignore


def _get_engine_singleton() -> Any:
    """Module-level singleton for performance."""
    global _engine_singleton
    if _engine_singleton is None:
        _engine_singleton = _mk_engine()
    return _engine_singleton


def _ofc_to_dict(ofc: Any) -> dict[str, Any]:
    if ofc is None:
        return {}
    if isinstance(ofc, dict):
        return ofc
    if hasattr(ofc, "to_dict") and callable(ofc.to_dict):
        try:
            d = ofc.to_dict()
            return d if isinstance(d, dict) else {}
        except Exception:
            return {}
    # best effort
    out: dict[str, Any] = {}
    for k in ("ok", "scenario", "have", "need", "score", "reason", "gate_bits"):
        if hasattr(ofc, k):
            with contextlib.suppress(Exception):
                out[k] = getattr(ofc, k)
    return out


def _evidence(ofc: Any) -> dict[str, Any]:
    try:
        ev = getattr(ofc, "evidence", {}) if ofc is not None else {}
        return ev if isinstance(ev, dict) else {}
    except Exception:
        return {}


def run_one(inp: dict[str, Any]) -> dict[str, Any]:
    """Map one captured input dict -> output dict.

    Expected keys in inp (from strategy.py capture):
      symbol, tf, direction, tick_ts_ms, price, delta_z, indicators, absorption, cfg
    """
    engine = _get_engine_singleton()
    runtime = _mk_runtime(inp)
    indicators = inp.get("indicators", {}) if isinstance(inp.get("indicators", {}), dict) else {}
    absorption = inp.get("absorption")
    if not isinstance(absorption, dict):
        absorption = None

    # cfg: either provided as cfg/cfg2 or empty
    cfg = inp.get("cfg") if isinstance(inp.get("cfg"), dict) else None
    if cfg is None and isinstance(inp.get("cfg2"), dict):
        cfg = inp.get("cfg2")
    if cfg is None:
        cfg = {}

    # core fields
    symbol = str(inp.get("symbol", "") or getattr(runtime, "symbol", "") or "")
    tf = str(inp.get("tf", inp.get("micro_tf", getattr(runtime, "config", {}).get("micro_tf", "1s"))) or "1s")
    direction = (inp.get("direction", "") or "")
    tick_ts_ms = int(float(inp.get("tick_ts_ms", inp.get("ts_ms", 0)) or 0))
    price = float(inp.get("price", 0.0) or 0.0)
    delta_z = float(inp.get("delta_z", inp.get("delta_z_used", 0.0)) or 0.0)

    # build() call
    ofc = None
    dec = None
    try:
        res = engine.build(
            symbol=symbol,
            tf=tf,
            direction=direction,
            tick_ts_ms=tick_ts_ms,
            price=price,
            delta_z=delta_z,
            runtime=runtime,
            cfg=cfg,
            indicators=indicators,
            absorption=absorption,
        )
        # expected (ofc, dec) as in strategy
        if isinstance(res, tuple) and len(res) >= 1:
            ofc = res[0]
            dec = res[1] if len(res) > 1 else None
        else:
            ofc = res
    except Exception:
        pass

    ofc_d = _ofc_to_dict(ofc)
    ev = _evidence(ofc)
    scenario_v4 = (ev.get("scenario_v4", "") or "") or (ofc_d.get("scenario", "") or "")
    ok = 1 if bool(ofc_d.get("ok", False)) else 0

    # Extract ML prob if available
    ml_prob = 0.0
    if ev:
        ml_prob = float(ev.get("ml_prob", ev.get("ml_probability", 0.0)) or 0.0)

    return {
        "decision": int(ok),
        "score": float(ofc_d.get("score", 0.0) or 0.0),
        "ml_prob": float(ml_prob),
        "symbol": symbol,
        "scenario_v4": scenario_v4,
        "have": int(ofc_d.get("have", 0) or 0),
        "need": int(ofc_d.get("need", 0) or 0),
        "gate_bits": int(ofc_d.get("gate_bits", 0) or 0),
    }

