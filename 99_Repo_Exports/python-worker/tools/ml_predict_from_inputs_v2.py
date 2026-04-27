
from __future__ import annotations

import argparse
import json
from typing import Any, Dict, List

import numpy as np
import joblib  # type: ignore

from core.ml_feature_schema import build_features


def load_payload_ndjson(path: str) -> List[Dict[str, Any]]:
    out = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            row = json.loads(line)
            payload = row.get("payload")
            if payload is not None:
                if isinstance(payload, str):
                    payload = json.loads(payload)
                out.append(payload)
            else:
                if isinstance(row, dict) and "v" in row and "symbol" in row and "ts_ms" in row:
                    out.append(row)
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--model", required=True)
    ap.add_argument("--inputs", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--p-min", type=float, default=0.55)
    args = ap.parse_args()

    model = joblib.load(args.model)
    inputs = load_payload_ndjson(args.inputs)

    with open(args.out, "w", encoding="utf-8") as f:
        for inp in inputs:
            feat = build_features(inp)
            X = np.asarray([feat.x], dtype=np.float32)
            p = float(model.predict_proba(X)[0][1])
            out = {
                "sid": inp.get("sid"),
                "ts_ms": int(inp.get("ts_ms", 0)),
                "symbol": str(inp.get("symbol","")),
                "scenario": str(inp.get("scenario","")),
                "direction": str(inp.get("direction","")),
                "p_edge": p,
                "p_min": float(args.p_min),
                "allow": int(p >= float(args.p_min)),
            }
            f.write(json.dumps(out, ensure_ascii=False) + "\n")

if __name__ == "__main__":
    main()

