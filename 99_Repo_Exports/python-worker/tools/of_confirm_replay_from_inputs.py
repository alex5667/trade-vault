from __future__ import annotations

import argparse
import inspect
import json
import os
import time
from copy import deepcopy
from types import SimpleNamespace
from typing import Any, Dict, Optional, Tuple


def _safe_loads(line: str) -> Optional[Dict[str, Any]]:
    try:
        d = json.loads(line)
        return d if isinstance(d, dict) else None
    except Exception:
        return None


def _safe_loads_maybe(s: Any) -> Optional[Dict[str, Any]]:
    if s is None:
        return None
    if isinstance(s, dict):
        return s
    if isinstance(s, (bytes, bytearray)):
        try:
            s = s.decode("utf-8", "ignore")
        except Exception:
            return None
    if not isinstance(s, str):
        return None
    ss = s.strip()
    if not ss:
        return None
    # tolerate payload wrapped as JSON string
    try:
        d = json.loads(ss)
        return d if isinstance(d, dict) else None
    except Exception:
        return None


def _filter_kwargs(fn: Any, kw: Dict[str, Any]) -> Dict[str, Any]:
    try:
        sig = inspect.signature(fn)
        allowed = set(sig.parameters.keys())
        return {k: v for k, v in kw.items() if k in allowed}
    except Exception:
        return kw


def _key(inp: Dict[str, Any]) -> str:
    """
    Stable replay key. Prefer explicit ids if present to avoid collisions when multiple events share ts.
    """
    sym = str(inp.get("symbol", "") or "").upper()
    ts = str(int(float(inp.get("tick_ts_ms", inp.get("ts_ms", 0)) or 0)))
    direction = str(inp.get("direction", "") or "")
    tf = str(inp.get("tf", inp.get("micro_tf", "1s")) or "1s")
    sid = str(inp.get("sid", inp.get("signal_id", inp.get("id", ""))) or "")
    if sid:
        return f"{sym}|{ts}|{direction}|{tf}|{sid}"
    return f"{sym}|{ts}|{direction}|{tf}"


def _extract_inputs(raw: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normalize various Redis/XADD shapes into one canonical dict.

    Supported shapes:
      1) NDJSON line already is payload dict (flat).
      2) Wrapper dict: {"payload": "<json str>" , ... meta fields ...}
      3) Wrapper dict: {"payload": {...}, ...}
      4) Wrapper dict: {"data": {...}}
      5) Redis fields dumped: {"ts_ms":"..","symbol":"..","payload":"{...}"}

    Rule: payload wins, then meta fills missing.
    """
    meta = dict(raw or {})

    # unwrap known wrappers
    payload = None
    if "payload" in meta:
        payload = _safe_loads_maybe(meta.get("payload"))
    if payload is None and "data" in meta:
        payload = _safe_loads_maybe(meta.get("data"))
    if payload is None and "msg" in meta:
        payload = _safe_loads_maybe(meta.get("msg"))

    out: Dict[str, Any] = {}
    if isinstance(payload, dict):
        out.update(payload)

    # Fill from meta if missing (do NOT override payload)
    for k in ("symbol", "direction", "tf", "micro_tf", "tick_ts_ms", "ts_ms", "price", "delta_z", "delta_z_used", "sid", "signal_id", "id"):
        if k not in out and k in meta:
            out[k] = meta.get(k)

    # If meta has indicators/cfg/runtime and payload didn't, pull them in
    for k in ("indicators", "absorption", "cfg", "cfg2", "runtime", "runtime_config"):
        if k not in out and k in meta:
            out[k] = meta.get(k)

    return out


def _mk_runtime(inp: Dict[str, Any]) -> Any:
    """
    Minimal runtime stub: must satisfy whatever OFConfirmEngine.build reads.
    Strategy passes `runtime=runtime` where runtime has .symbol and .config at minimum.
    """
    sym = str(inp.get("symbol", "") or "")
    rtd = inp.get("runtime", None)
    rt_cfg = inp.get("runtime_config") if isinstance(inp.get("runtime_config"), dict) else None

    # If runtime dict exists, preserve as much as possible (determinism)
    base: Dict[str, Any] = {}
    if isinstance(rtd, dict):
        base.update(rtd)
        if rt_cfg is None and isinstance(rtd.get("config"), dict):
            rt_cfg = dict(rtd.get("config"))

    if rt_cfg is None:
        rt_cfg = {}

    # ensure micro_tf stable
    if "micro_tf" in inp and "micro_tf" not in rt_cfg:
        rt_cfg["micro_tf"] = inp.get("micro_tf")

    # build namespace with known stable fields (avoid missing attribute branches inside engine)
    ns = SimpleNamespace(**{k: v for k, v in base.items() if k != "config"})
    setattr(ns, "symbol", sym)
    setattr(ns, "config", rt_cfg)
    return ns


def _mk_engine() -> Any:
    """
    Try to construct engine in a robust way:
    - OFConfirmEngine.from_env() if exists
    - else OFConfirmEngine() default ctor
    """
    import logging
    log = logging.getLogger("replay")

    from core.of_confirm_engine import OFConfirmEngine  # type: ignore
    if hasattr(OFConfirmEngine, "from_env") and callable(getattr(OFConfirmEngine, "from_env")):
        engine = OFConfirmEngine.from_env()  # type: ignore
    else:
        engine = OFConfirmEngine()  # type: ignore

    ml_gate = getattr(engine, "_ml_gate", None)
    if ml_gate is None:
        log.info("MLConfirmGate not attached to engine — replay runs without ML pre-warm")
    elif not hasattr(ml_gate, "_refresh_cache_if_needed"):
        log.warning(
            "MLConfirmGate has no _refresh_cache_if_needed() — skipping pre-warm "
            "(replay will use cold cache; results may differ from live)"
        )
    else:
        try:
            ml_gate._refresh_cache_if_needed()
        except Exception as e:
            log.warning(f"Failed to pre-warm MLConfirmGate: {e}")

    return engine


def _ofc_to_dict(ofc: Any) -> Dict[str, Any]:
    if ofc is None:
        return {}
    if isinstance(ofc, dict):
        return ofc
    if hasattr(ofc, "to_dict") and callable(getattr(ofc, "to_dict")):
        try:
            d = ofc.to_dict()
            return d if isinstance(d, dict) else {}
        except Exception:
            return {}
    # best effort
    out: Dict[str, Any] = {}
    for k in ("ok", "scenario", "have", "need", "score", "reason", "gate_bits"):
        if hasattr(ofc, k):
            try:
                out[k] = getattr(ofc, k)
            except Exception:
                pass
    return out


def _evidence(ofc: Any) -> Dict[str, Any]:
    try:
        ev = getattr(ofc, "evidence", {}) if ofc is not None else {}
        return ev if isinstance(ev, dict) else {}
    except Exception:
        return {}


def _to_int(x: Any, d: int = 0) -> int:
    try:
        if x is None:
            return d
        if isinstance(x, bool):
            return int(x)
        return int(float(x))
    except Exception:
        return d


def _to_float(x: Any, d: float = 0.0) -> float:
    try:
        if x is None:
            return d
        if isinstance(x, bool):
            return float(int(x))
        return float(x)
    except Exception:
        return d


def _norm_direction(inp: Dict[str, Any]) -> str:
    # tolerate various naming
    v = inp.get("direction", inp.get("dir", inp.get("side", "")))
    s = str(v or "")
    return s


def _norm_tick_ts_ms(inp: Dict[str, Any]) -> int:
    # tolerate tick_ts_ms / tick_ts / ts_ms / timestamp
    for k in ("tick_ts_ms", "tick_ts", "ts_ms", "timestamp_ms", "timestamp"):
        if k in inp and inp.get(k) is not None:
            return _to_int(inp.get(k), 0)
    return 0


def _norm_tf(inp: Dict[str, Any], runtime: Any) -> str:
    # precedence: explicit tf -> micro_tf -> runtime.config.micro_tf -> "1s"
    for k in ("tf", "timeframe", "micro_tf"):
        v = inp.get(k, None)
        if v is not None and str(v).strip():
            return str(v)
    try:
        cfg = getattr(runtime, "config", {}) or {}
        v = cfg.get("micro_tf", None)
        if v is not None and str(v).strip():
            return str(v)
    except Exception:
        pass
    return "1s"


def _norm_price(inp: Dict[str, Any]) -> float:
    # strict order (avoid accidental switch if multiple present)
    for k in ("price", "last_price", "mid_price", "mid", "px"):
        if k in inp and inp.get(k) is not None:
            return _to_float(inp.get(k), 0.0)
    return 0.0


def _norm_delta_z(inp: Dict[str, Any]) -> float:
    for k in ("delta_z", "delta_z_used", "deltaZ", "delta_zscore", "delta_z_spike"):
        if k in inp and inp.get(k) is not None:
            return _to_float(inp.get(k), 0.0)
    return 0.0


def replay_one(engine: Any, inp: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """
    Runs engine.build() with best-effort kwargs filtering.
    Returns (out_row, debug_raw).
    """
    runtime = _mk_runtime(inp)
    # deep-copy to prevent accidental mutation causing nondeterminism across replays
    indicators = deepcopy(inp.get("indicators", {})) if isinstance(inp.get("indicators", {}), dict) else {}
    absorption = deepcopy(inp.get("absorption", None))
    if not isinstance(absorption, dict):
        absorption = None

    # cfg: either provided as cfg/cfg2 or empty
    # IMPORTANT: prefer cfg2 (strategy passes cfg2=cfg for build())
    cfg = inp.get("cfg2") if isinstance(inp.get("cfg2"), dict) else None
    if cfg is None and isinstance(inp.get("cfg"), dict):
        cfg = inp.get("cfg")
    if cfg is None:
        cfg = {}

    # core fields
    symbol = str(inp.get("symbol", "") or getattr(runtime, "symbol", "") or "")
    direction = _norm_direction(inp)
    tick_ts_ms = _norm_tick_ts_ms(inp)
    tf = _norm_tf(inp, runtime)
    price = _norm_price(inp)
    delta_z = _norm_delta_z(inp)

    # build() call
    build = getattr(engine, "build")
    kw = {
        "symbol": symbol,
        "tf": tf,
        "direction": direction,
        "tick_ts_ms": tick_ts_ms,
        "price": price,
        "delta_z": delta_z,
        "runtime": runtime,
        "cfg": cfg,
        "indicators": indicators,
        "absorption": absorption,
    }
    kw2 = _filter_kwargs(build, kw)

    # best-effort deterministic clock override if engine supports it
    try:
        if hasattr(engine, "set_replay_time_ms") and callable(getattr(engine, "set_replay_time_ms")):
            engine.set_replay_time_ms(int(tick_ts_ms))
        elif hasattr(engine, "now_ms_override"):
            setattr(engine, "now_ms_override", int(tick_ts_ms))
        elif hasattr(engine, "replay_now_ms"):
            setattr(engine, "replay_now_ms", int(tick_ts_ms))
    except Exception:
        pass

    t0 = time.perf_counter_ns()
    ofc = None
    dec = None
    err = ""
    try:
        res = build(**kw2)
        # expected (ofc, dec) as in strategy
        if isinstance(res, tuple) and len(res) >= 1:
            ofc = res[0]
            dec = res[1] if len(res) > 1 else None
        else:
            ofc = res
    except Exception as e:
        err = str(e)[:240]
    t_us = int((time.perf_counter_ns() - t0) / 1000)

    ofc_d = _ofc_to_dict(ofc)
    ev = _evidence(ofc)
    scenario_v4 = str(ev.get("scenario_v4", "") or "") or str(ofc_d.get("scenario", "") or "")
    ok = 1 if bool(ofc_d.get("ok", False)) else 0

    out = {
        "k": _key(inp),
        "symbol": str(symbol).upper(),
        "tick_ts_ms": int(tick_ts_ms),
        "tf": tf,
        "direction": direction,
        "scenario_v4": scenario_v4,
        "ok": int(ok),
        "have": int(ofc_d.get("have", 0) or 0),
        "need": int(ofc_d.get("need", 0) or 0),
        "score": float(ofc_d.get("score", 0.0) or 0.0),
        "gate_bits": int(ofc_d.get("gate_bits", 0) or 0),
        "reason": str(ofc_d.get("reason", "") or "")[:160],
        "exec_risk_norm": float(ev.get("exec_risk_norm", 0.0) or 0.0),
        "ok_soft": int(ev.get("ok_soft", 0) or 0),
        "meta_veto": int(ev.get("meta_veto", 0) or 0),
        "latency_us": int(t_us),
        "err": err,
        "evidence": ev,
        "missing_legs": ev.get("missing_legs", []),
    }
    # include normalized fields to debug mapping determinism
    dbg_inputs = dict(kw2)
    if "runtime" in dbg_inputs and hasattr(dbg_inputs["runtime"], "__dict__"):
        # Convert SimpleNamespace to dict for JSON serialization
        dbg_inputs["runtime"] = vars(dbg_inputs["runtime"])

    dbg = {
        "inputs_used": dbg_inputs,
        "normalized": {
            "symbol": symbol,
            "tf": tf,
            "direction": direction,
            "tick_ts_ms": tick_ts_ms,
            "price": price,
            "delta_z": delta_z,
            "cfg_keys": sorted(list(cfg.keys()))[:40],
            "indicators_keys": sorted(list(indicators.keys()))[:60] if isinstance(indicators, dict) else [],
            "absorption_keys": sorted(list(absorption.keys()))[:40] if isinstance(absorption, dict) else [],
        },
        "ofc": ofc_d,
        "evidence": ev,
    }
    return out, dbg


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--inputs", required=True, help="NDJSON inputs exported from signals:of:inputs")
    ap.add_argument("--out", required=True, help="NDJSON replay outputs")
    ap.add_argument("--debug-out", default="", help="Optional NDJSON debug file (same order as out)")
    ap.add_argument("--max-rows", type=int, default=int(os.getenv("OF_REPLAY_MAX_ROWS", "500000")))
    args = ap.parse_args()

    engine = _mk_engine()

    n = 0
    with open(args.inputs, "r", encoding="utf-8") as f_in, open(args.out, "w", encoding="utf-8") as f_out:
        f_dbg = open(args.debug_out, "w", encoding="utf-8") if args.debug_out else None
        try:
            for line in f_in:
                if n >= int(args.max_rows):
                    break
                line = line.strip()
                if not line:
                    continue
                raw = _safe_loads(line)
                if not raw:
                    continue
                inp = _extract_inputs(raw)
                out, dbg = replay_one(engine, inp)
                f_out.write(json.dumps(out, ensure_ascii=False) + "\n")
                if f_dbg is not None:
                    f_dbg.write(json.dumps({"k": out.get("k", ""), "dbg": dbg}, ensure_ascii=False) + "\n")
                n += 1
        finally:
            if f_dbg is not None:
                f_dbg.close()

    print(json.dumps({"ok": True, "wrote": n, "out": args.out, "debug_out": args.debug_out or ""}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()

