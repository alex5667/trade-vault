from __future__ import annotations

import argparse, json
from typing import Any, Dict, List


KEEP_EVIDENCE = {
    "delta_z", "weak_progress", "sweep_recent", "reclaim_recent",
    "obi_stable", "iceberg_strict",
    "abs_lvl_ok", "abs_lvl_score", "abs_lvl_bias",
    "abs_lvl_ladder", "abs_lvl_poc_edge", "abs_lvl_eff",
    "fp_move_bp", "fp_eff_quote", "fp_quote_delta",
}


def load_ndjson(path: str) -> List[Dict[str, Any]]:
    out = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            out.append(json.loads(line))
    return out


def norm_float(x: Any, nd: int = 4) -> Any:
    try:
        return round(float(x), nd)
    except Exception:
        return x


def normalize_of_confirm(payload: Dict[str, Any]) -> Dict[str, Any]:
    e = payload.get("evidence") or {}
    e2 = {}
    for k in KEEP_EVIDENCE:
        if k in e:
            e2[k] = norm_float(e[k], 4) if isinstance(e[k], (float, int)) else e[k]
    out = {
        "v": int(payload.get("v", payload.get("version", 0)) or 0),
        "symbol": str(payload.get("symbol", "")),
        "ts_ms": int(payload.get("ts_ms", 0) or 0),
        "direction": str(payload.get("direction", "")),
        "scenario": str(payload.get("scenario", "")),
        "ok": int(payload.get("ok", 0) or 0),
        "have": int(payload.get("have", 0) or 0),
        "need": int(payload.get("need", 0) or 0),
        "gate_bits": int(payload.get("gate_bits", 0) or 0),
        "reason": str(payload.get("reason", "")),
        "score": norm_float(payload.get("score", 0.0), 4),
        "evidence": e2,
    }
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="inp", required=True)
    ap.add_argument("--out", dest="outp", required=True)
    args = ap.parse_args()

    rows = load_ndjson(args.inp)
    out_rows = []
    for r in rows:
        p = r.get("payload")
        if isinstance(p, str):
            payload = json.loads(p)
        else:
            payload = p
        out_rows.append(normalize_of_confirm(payload))

    out_rows.sort(key=lambda x: (x["ts_ms"], x["symbol"], x["direction"], x["scenario"]))
    with open(args.outp, "w", encoding="utf-8") as f:
        for x in out_rows:
            f.write(json.dumps(x, ensure_ascii=False, sort_keys=True) + "\n")


if __name__ == "__main__":
    main()
