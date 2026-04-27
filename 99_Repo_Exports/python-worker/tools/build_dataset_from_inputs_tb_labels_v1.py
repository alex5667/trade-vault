from __future__ import annotations

import argparse
import json
from typing import Any, Dict, Iterable, List

import pandas as pd


def _read_ndjson(path: str) -> Iterable[Dict[str, Any]]:
    """Read NDJSON file line by line."""
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if not s:
                continue
            yield json.loads(s)


def _get_payload(obj: Dict[str, Any]) -> Dict[str, Any]:
    """Extract payload from object (handle nested payload field)."""
    if "payload" in obj and isinstance(obj["payload"], str) and obj["payload"].strip().startswith("{"):
        try:
            return json.loads(obj["payload"])
        except Exception:
            return obj
    return obj


def main() -> None:
    ap = argparse.ArgumentParser(description="Build ML dataset from OF inputs + TB labels")
    ap.add_argument("--inputs", required=True, help="OF inputs NDJSON file")
    ap.add_argument("--tb", required=True, help="TB labels NDJSON file (from export_tb_labels_ndjson_v1)")
    ap.add_argument("--out", required=True, help="Output parquet file path")
    ap.add_argument("--primary-h-ms", type=int, default=180000, help="Primary horizon in ms")
    args = ap.parse_args()

    # Load TB labels by sid
    tb: Dict[str, Dict[str, Any]] = {}
    for obj in _read_ndjson(args.tb):
        sid = str(obj.get("sid", "") or "")
        if sid:
            tb[sid] = obj

    # Join inputs with TB labels
    rows: List[Dict[str, Any]] = []
    miss = 0
    for obj in _read_ndjson(args.inputs):
        o = _get_payload(obj)
        sid = str(o.get("sid", "") or "")
        if not sid:
            continue
        t = tb.get(sid)
        if not t:
            miss += 1
            continue

        primary = (t.get("primary") or {})
        # fallback: look into horizons by primary_h_ms
        if not primary and isinstance(t.get("horizons"), dict):
            primary = (t["horizons"].get(str(args.primary_h_ms)) or {})

        indicators = o.get("indicators") if isinstance(o.get("indicators"), dict) else {}

        rows.append({
            "sid": sid,
            "ts_ms": int(o.get("ts_ms", o.get("ts", 0)) or 0),
            "symbol": str(o.get("symbol", "") or ""),
            "direction": str(o.get("direction", "") or ""),
            "scenario_v4": str(o.get("scenario_v4", o.get("scenario", "")) or ""),
            "indicators": indicators,
            "y_edge": int(primary.get("y_edge", 0) or 0),
            "tb_label": str(primary.get("label", "") or ""),
            "tb_r_mult": float(primary.get("r_mult", 0.0) or 0.0),
            "tb_ret_bps": float(primary.get("ret_bps", 0.0) or 0.0),
            "tb_mae_bps": float(primary.get("mae_bps", 0.0) or 0.0),
            "tb_mfe_bps": float(primary.get("mfe_bps", 0.0) or 0.0),
            "tb_adverse_proxy": float(primary.get("adverse_proxy", 0.0) or 0.0),
            "meta": t.get("meta") or {},
        })

    df = pd.DataFrame(rows)
    df.to_parquet(args.out, index=False)

    summary = {
        "inputs_rows": len(rows) + miss,
        "joined_rows": len(rows),
        "missing_tb": miss,
        "pos_rate": float(df["y_edge"].mean()) if len(df) else 0.0,
    }
    with open(args.out + ".json", "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    print(f"✅ Dataset built: {len(rows)} rows, {miss} missing TB labels")
    print(f"   Positive rate (y_edge=1): {summary['pos_rate']:.2%}")


if __name__ == "__main__":
    main()

