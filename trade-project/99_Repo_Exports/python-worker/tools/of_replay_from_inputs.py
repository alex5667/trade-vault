from __future__ import annotations

import argparse
import inspect
import json
import os
from typing import Any

from core.of_confirm_contract import OFConfirmV3
from core.strong_of_gate import eval_continuation, eval_reversal


def _filter_kwargs_for_callable(fn: Any, **kwargs: Any) -> dict[str, Any]:
    try:
        sig = inspect.signature(fn)
        allowed = set(sig.parameters.keys())
        return {k: v for k, v in kwargs.items() if k in allowed}
    except Exception:
        return dict(kwargs)


def _safe_loads(line: str) -> dict[str, Any] | None:
    try:
        d = json.loads(line)
        return d if isinstance(d, dict) else None
    except Exception:
        return None


def _key(inp: dict[str, Any]) -> str:
    # stable key for diff: symbol|ts_ms|direction|scenario
    sym = (inp.get("symbol", "") or "")
    ts = str(int(float(inp.get("ts_ms", 0) or 0)))
    direction = (inp.get("direction", "") or "")
    scenario = (inp.get("scenario", "") or "")
    return f"{sym}|{ts}|{direction}|{scenario}"


def _dec_to_dict(dec: Any) -> dict[str, Any]:
    if dec is None:
        return {"ok": 0, "scenario": "na", "need": 0, "have": 0, "reason": "none", "gate_bits": 0}
    if isinstance(dec, dict):
        return dec
    # dataclass-like / object-like
    out: dict[str, Any] = {}
    for k in ("ok", "scenario", "need", "have", "a", "b", "c", "reason", "gate_bits", "legs"):
        if hasattr(dec, k):
            out[k] = getattr(dec, k)
    return out


def replay_one(inp: dict[str, Any]) -> dict[str, Any]:
    """
    Deterministic replay based on OFInputsV1:
    - Uses core.strong_of_gate.eval_reversal / eval_continuation (production logic)
    - Drops unknown kwargs via signature filter (forward compatible)
    """
    try:
        from core.strong_of_gate import eval_continuation, eval_reversal  # type: ignore
    except Exception as e:
        raise RuntimeError("core.strong_of_gate is required for replay (run inside python-worker repo)") from e

    cfg = inp.get("cfg", {}) if isinstance(inp.get("cfg", {}), dict) else {}
    direction = (inp.get("direction", "") or "")
    scenario = (inp.get("scenario", "") or "")

    # common evidence
    kw_common = dict(
        direction=direction,
        delta_z=float(inp.get("delta_z", 0.0) or 0.0),
        weak_progress=int(inp.get("weak_progress", 0) or 0),
        sweep_recent=int(inp.get("sweep_recent", 0) or 0),
        reclaim_recent=int(inp.get("reclaim_recent", 0) or 0),
        obi_stable=int(inp.get("obi_stable", 0) or 0),
        iceberg_strict=int(inp.get("iceberg_strict", 0) or 0),
        abs_lvl_ok=int(inp.get("abs_lvl_ok", 0) or 0),
        fp_eff_quote=float(inp.get("fp_eff_quote", 0.0) or 0.0),
        fp_quote_delta=float(inp.get("fp_quote_delta", 0.0) or 0.0),
        fp_move_bp=float(inp.get("fp_move_bp", 0.0) or 0.0),
        cfg=cfg,
    )

    if scenario.lower() == "reversal":
        dec = eval_reversal(**_filter_kwargs_for_callable(eval_reversal, **kw_common))
    elif scenario.lower() == "continuation":
        kw = dict(
            **kw_common,
            trend_dir=(inp.get("trend_dir", "NONE") or "NONE"),
            hidden_ctx_recent=int(inp.get("hidden_ctx_recent", 0) or 0),
            cont_ctx_recent=int(inp.get("cont_ctx_recent", 0) or 0),
        )
        dec = eval_continuation(**_filter_kwargs_for_callable(eval_continuation, **kw))
    else:
        # unknown -> fail-closed
        dec = {"ok": 0, "scenario": scenario or "na", "need": 0, "have": 0, "reason": "scenario_na", "gate_bits": 0}

    d = _dec_to_dict(dec)
    ok = 1 if bool(d.get("ok", 0)) else 0
    return {
        "k": _key(inp),
        "symbol": (inp.get("symbol", "") or ""),
        "ts_ms": int(float(inp.get("ts_ms", 0) or 0)),
        "direction": direction,
        "scenario": scenario,
        "ok": int(ok),
        "need": int(d.get("need", 0) or 0),
        "have": int(d.get("have", 0) or 0),
        "reason": (d.get("reason", "") or "")[:160],
        "gate_bits": int(d.get("gate_bits", 0) or 0),
        "a": int(d.get("a", 0) or 0),
        "b": int(d.get("b", 0) or 0),
        "c": int(d.get("c", 0) or 0),
    }


def load_payload_ndjson(path: str) -> List[dict[str, Any]]:
    """Load OFInputsV1 records from NDJSON file.
    
    Supports two formats:
    1. Wrapped: {"payload": "{...json...}"} or {"payload": {...object...}}
    2. Direct: {...OFInputsV1 object...} (from export_of_inputs_ndjson.py)
    """
    out = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            row = json.loads(line)
            # Check if wrapped format (has "payload" field)
            payload = row.get("payload")
            if payload is not None:
                # Wrapped format: extract payload
                if isinstance(payload, str):
                    payload = json.loads(payload)
                out.append(payload)
            else:
                # Direct format: entire row is OFInputsV1 (from export tool)
                # Validate it looks like OFInputsV1 (has required fields)
                if isinstance(row, dict) and "v" in row and "symbol" in row and "ts_ms" in row:
                    out.append(row)
    return out


def build_of_confirm_from_inputs(inp: dict[str, Any]) -> dict[str, Any]:
    cfg = inp.get("cfg") or {}
    scenario = (inp.get("scenario", "none"))
    direction = (inp.get("direction", ""))
    delta_z = float(inp.get("delta_z", 0.0))
    weak_progress = bool(int(inp.get("weak_progress", 0)))
    sweep_recent = bool(int(inp.get("sweep_recent", 0)))
    reclaim_recent = bool(int(inp.get("reclaim_recent", 0)))
    obi_stable = bool(int(inp.get("obi_stable", 0)))
    iceberg_strict = bool(int(inp.get("iceberg_strict", 0)))
    abs_lvl_ok = bool(int(inp.get("abs_lvl_ok", 0)))

    if scenario == "reversal":
        dec = eval_reversal(
            direction=direction,
            delta_z=delta_z,
            weak_progress=weak_progress,
            sweep_recent=sweep_recent,
            reclaim_recent=reclaim_recent,
            obi_stable=obi_stable,
            iceberg_strict=iceberg_strict,
            abs_lvl_ok=abs_lvl_ok,
            cfg=cfg,
        )
    elif scenario == "continuation":
        dec = eval_continuation(
            direction=direction,
            trend_dir=(inp.get("trend_dir") or None),
            hidden_ctx_recent=bool(int(inp.get("hidden_ctx_recent", 0))),
            iceberg_strict=iceberg_strict,
            obi_stable=obi_stable,
            cont_ctx_recent=bool(int(inp.get("cont_ctx_recent", 0))),
            abs_lvl_ok=abs_lvl_ok,
            cfg=cfg,
        )
    else:
        return {}

    score = float(dec.have / dec.need) if int(dec.need) > 0 else 0.0
    ofc = OFConfirmV3(
        v=3,
        symbol=(inp.get("symbol", "")),
        ts_ms=int(inp.get("ts_ms", 0)),
        direction=direction,
        scenario=str(dec.scenario),
        ok=int(bool(dec.ok)),
        score=score,
        have=int(dec.have),
        need=int(dec.need),
        gate_bits=int(getattr(dec, "gate_bits", 0)),
        reason=str(getattr(dec, "reason", "")),
        evidence={},  # golden compares the normalized stream output file, not full evidence
        contrib={},
    )
    return ofc.to_dict()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--inputs", required=True, help="NDJSON with OFInputsV1 dict per line")
    ap.add_argument("--out", required=True, help="NDJSON output (replay rows)")
    ap.add_argument("--max-rows", type=int, default=int(os.getenv("OF_REPLAY_MAX_ROWS", "500000")))
    args = ap.parse_args()

    n = 0
    with open(args.inputs, encoding="utf-8") as f_in, open(args.out, "w", encoding="utf-8") as f_out:
        for line in f_in:
            if n >= int(args.max_rows):
                break
            line = line.strip()
            if not line:
                continue
            inp = _safe_loads(line)
            if not inp:
                continue
            row = replay_one(inp)
            f_out.write(json.dumps(row, ensure_ascii=False) + "\n")
            n += 1

    print(json.dumps({"ok": True, "wrote": n, "out": args.out}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
