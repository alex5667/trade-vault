from __future__ import annotations

import argparse
import json
import os
from typing import Any, Dict, Optional

try:
    import joblib  # type: ignore
except Exception:
    joblib = None  # type: ignore


def _safe_loads(line: str) -> Optional[Dict[str, Any]]:
    try:
        d = json.loads(line)
        return d if isinstance(d, dict) else None
    except Exception:
        return None


def _key(inp: Dict[str, Any]) -> str:
    sym = str(inp.get("symbol", "") or "").upper()
    ts = str(int(float(inp.get("ts_ms", 0) or 0)))
    direction = str(inp.get("direction", "") or "")
    sc = str(inp.get("scenario_v4", inp.get("scenario", "")) or "")
    return f"{sym}|{ts}|{direction}|{sc}"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--inputs", required=True, help="NDJSON inputs (payload from stream:ml_confirm:inputs)")
    ap.add_argument("--out", required=True, help="NDJSON outputs")
    ap.add_argument("--mode", default=os.getenv("ML_CONFIRM_MODE", "ENFORCE"), help="OFF|SHADOW|ENFORCE")
    ap.add_argument("--fail-policy", default=os.getenv("ML_CONFIRM_FAIL_POLICY", "OPEN"), help="OPEN|CLOSED")
    ap.add_argument("--max-rows", type=int, default=int(os.getenv("ML_REPLAY_MAX_ROWS", "500000")))
    args = ap.parse_args()

    # reuse exact production logic by importing MLConfirmGate and driving it with cfg/model from record
    from services.ml_confirm_gate import MLConfirmGate, _safe_loads as _cfg_loads  # type: ignore

    # dummy redis (not used in replay path below)
    import redis
    r = redis.Redis.from_url(os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0"), decode_responses=True)
    gate = MLConfirmGate(r=r, mode=str(args.mode), fail_policy=str(args.fail_policy),
                         champion_key=os.getenv("ML_CFG_CHAMPION_KEY", "cfg:ml_confirm:champion"),
                         challenger_key=os.getenv("ML_CFG_CHALLENGER_KEY", "cfg:ml_confirm:challenger"))

    n = 0
    with open(args.inputs, "r", encoding="utf-8") as f_in, open(args.out, "w", encoding="utf-8") as f_out:
        for line in f_in:
            if n >= int(args.max_rows):
                break
            line = line.strip()
            if not line:
                continue
            inp = _safe_loads(line)
            if not inp:
                continue

            cfg = inp.get("cfg", {}) if isinstance(inp.get("cfg", {}), dict) else {}
            model_path = str(cfg.get("model_path", "") or "")
            if joblib is None or not model_path:
                # cannot replay without model
                out = {"k": _key(inp), "ok": 0, "status": "ERR_NO_MODEL_PATH", "symbol": inp.get("symbol", ""), "ts_ms": inp.get("ts_ms", 0)}
                f_out.write(json.dumps(out, ensure_ascii=False) + "\n")
                n += 1
                continue

            try:
                model = joblib.load(model_path)
            except Exception:
                out = {"k": _key(inp), "ok": 0, "status": "ERR_MODEL_LOAD", "symbol": inp.get("symbol", ""), "ts_ms": inp.get("ts_ms", 0)}
                f_out.write(json.dumps(out, ensure_ascii=False) + "\n")
                n += 1
                continue

            # force gate to use cfg/model from record (no redis reads)
            gate._cfg = dict(cfg)
            gate._model = model
            gate._cache_loaded_ms = 10**18  # prevent refresh

            dec = gate.check(
                symbol=str(inp.get("symbol", "")),
                ts_ms=int(float(inp.get("ts_ms", 0) or 0)),
                direction=str(inp.get("direction", "")),
                scenario=str(inp.get("scenario_v4", inp.get("scenario", "")) or ""),
                indicators=inp.get("indicators", {}) if isinstance(inp.get("indicators", {}), dict) else {},
                rule_score=float(inp.get("rule_score", 0.0) or 0.0),
                rule_have=int(inp.get("rule_have", 0) or 0),
                rule_need=int(inp.get("rule_need", 0) or 0),
                cancel_spike_veto=int(inp.get("cancel_spike_veto", 0) or 0),
                ok_rule=int(inp.get("ok_rule", 0) or 0),
            )

            out = {
                "k": _key(inp),
                "symbol": str(inp.get("symbol", "") or "").upper(),
                "ts_ms": int(float(inp.get("ts_ms", 0) or 0)),
                "direction": str(inp.get("direction", "") or ""),
                "scenario_v4": str(inp.get("scenario_v4", inp.get("scenario", "")) or ""),
                "allow": int(bool(dec.allow)),
                "abstain": int(bool(getattr(dec, "abstain", False))),
                "status": str(getattr(dec, "status", "") or ""),
                "p_edge": float(dec.p_edge),
                "p_min": float(dec.p_min),
                "p_margin": float(getattr(dec, "p_margin", 0.0)),
                "best_h_ms": int(dec.best_h_ms or 0),
                "bucket": str(dec.bucket or ""),
                "model_run_id": str(dec.model_run_id or ""),
                "reason": str(dec.reason or "")[:160],
            }
            f_out.write(json.dumps(out, ensure_ascii=False) + "\n")
            n += 1

    print(json.dumps({"ok": True, "wrote": n, "out": args.out}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()


