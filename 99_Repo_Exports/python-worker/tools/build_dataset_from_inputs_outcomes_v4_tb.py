from __future__ import annotations

import argparse
import json
from typing import Any, Dict, Iterable, List

import pandas as pd


def _read_ndjson(path: str) -> Iterable[Dict[str, Any]]:
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            yield json.loads(line)


def _get_payload(obj: Dict[str, Any]) -> Dict[str, Any]:
    if "payload" in obj and isinstance(obj["payload"], str) and obj["payload"].strip().startswith("{"):
        try:
            return json.loads(obj["payload"])
        except Exception:
            return obj
    return obj


def _norm_sid(sid: str) -> str:
    if not sid:
        return ""
    if sid.startswith("crypto-of:"):
        return sid[len("crypto-of:") :]
    return sid


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--inputs", required=True)
    ap.add_argument("--tb-labels", required=True)
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    tb: Dict[str, Dict[str, Any]] = {}
    for obj in _read_ndjson(args.tb_labels):
        sid = _norm_sid(str(obj.get("sid", "") or ""))
        if sid:
            tb[sid] = obj

    rows: List[Dict[str, Any]] = []
    miss = 0
    for obj in _read_ndjson(args.inputs):
        o = _get_payload(obj)
        sid = _norm_sid(str(o.get("sid", "") or ""))
        if not sid:
            continue
        t = tb.get(sid)
        if not t:
            miss += 1
            continue

        rows.append({
            "sid": sid,
            "ts_ms": int(o.get("ts_ms", o.get("ts", 0)) or 0),
            "symbol": str(o.get("symbol", "") or ""),
            "direction": str(o.get("direction", "") or ""),
            "scenario_v4": str(o.get("scenario_v4", o.get("scenario", "")) or ""),
            "indicators": {
                k: (json.dumps(v) if isinstance(v, (dict, list)) else v)
                for k, v in (o.get("indicators") if isinstance(o.get("indicators"), dict) else {}).items()
            },
            "y_edge": int(t.get("y_edge", 0) or 0),
            "tb_outcome": str(t.get("tb_outcome", "") or ""),
            "mae_bps": float(t.get("mae_bps", 0.0) or 0.0),
            "mfe_bps": float(t.get("mfe_bps", 0.0) or 0.0),
            "adverse_proxy": float(t.get("adverse_proxy", 0.0) or 0.0),
            "mae_r": float(t.get("mae_r", 0.0) or 0.0),
            "mfe_r": float(t.get("mfe_r", 0.0) or 0.0),
        })

    df = pd.DataFrame(rows)
    df.to_parquet(args.out, index=False)

    summary = {"inputs_rows": len(rows) + miss, "joined_rows": len(rows), "missing_tb": miss, "pos_rate": float(df["y_edge"].mean()) if len(df) else 0.0}
    with open(args.out + ".json", "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)


if __name__ == "__main__":
    main()
