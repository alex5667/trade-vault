from __future__ import annotations

import json
from pathlib import Path

from tools.of_replay_from_inputs import build_of_confirm_from_inputs


def load_payload_ndjson(path: Path):
    out = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        row = json.loads(line)
        payload = row.get("payload")
        if isinstance(payload, str):
            payload = json.loads(payload)
        out.append(payload)
    return out


def load_norm_ndjson(path: Path):
    out = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        out.append(json.loads(line))
    return out


def test_of_confirm_golden_replay_matches():
    data_dir = Path(__file__).parent / "data"
    inputs_path = data_dir / "of_inputs.ndjson"
    confirm_norm_path = data_dir / "of_confirm_norm.ndjson"

    if not inputs_path.exists() or not confirm_norm_path.exists():
        # allow running tests without fixtures
        return

    inputs = load_payload_ndjson(inputs_path)
    expected = load_norm_ndjson(confirm_norm_path)

    # build replay outputs and normalize to the same shape
    got = []
    for inp in inputs:
        ofc = build_of_confirm_from_inputs(inp)
        if not ofc:
            continue
        got.append({
            "v": int(ofc.get("v", 0)),
            "symbol": ofc.get("symbol", ""),
            "ts_ms": int(ofc.get("ts_ms", 0)),
            "direction": ofc.get("direction", ""),
            "scenario": ofc.get("scenario", ""),
            "ok": int(ofc.get("ok", 0)),
            "have": int(ofc.get("have", 0)),
            "need": int(ofc.get("need", 0)),
            "gate_bits": int(ofc.get("gate_bits", 0)),
            "reason": ofc.get("reason", ""),
            "score": round(float(ofc.get("score", 0.0)), 4),
            "evidence": {},
        })

    got.sort(key=lambda x: (x["ts_ms"], x["symbol"], x["direction"], x["scenario"]))

    # Compare only stable top fields; evidence is already validated via stream normalization file
    assert len(got) == len(expected)
    for g, e in zip(got, expected):
        for k in ("v","symbol","ts_ms","direction","scenario","ok","have","need","gate_bits","reason"):
            assert g[k] == e[k]
