#!/usr/bin/env python3
import argparse
import hashlib
import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


def _read_ndjson(path: Path, limit: Optional[int] = None) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
            if limit is not None and len(rows) >= limit:
                break
    return rows


def _hash_obj(obj: Any) -> str:
    b = json.dumps(obj, sort_keys=True, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(b).hexdigest()[:16]


def _assert_fields(row: Dict[str, Any], required: Tuple[str, ...]) -> List[str]:
    missing = []
    for k in required:
        if k not in row:
            missing.append(k)
    return missing


def main() -> int:
    ap = argparse.ArgumentParser(description="Validate OFC golden replay capture determinism.")
    ap.add_argument("path", help="Path to OFC capture NDJSON (OFC_CAPTURE_PATH)")
    ap.add_argument("--limit", type=int, default=200, help="Max rows to validate")
    ap.add_argument("--strict", action="store_true", help="Fail on any non-determinism across repeated runs")
    ap.add_argument("--report", action="store_true", help="Print per-row hashes and key stats")
    args = ap.parse_args()

    cap_path = Path(args.path)
    rows = _read_ndjson(cap_path, limit=args.limit)

    from core.of_confirm_engine import OFConfirmEngine  # type: ignore

    engine = OFConfirmEngine()
    engine.set_replay_time_ms(0)  # Enable replay mode (freeze time)

    required = ("symbol", "direction", "tick_ts_ms", "price", "delta_z", "cfg", "indicators")
    bad = 0
    nondet = 0

    for i, row in enumerate(rows):
        miss = _assert_fields(row, required)
        if miss:
            bad += 1
            if args.report:
                print(json.dumps({"row": i, "error": "missing_fields", "missing": miss}, ensure_ascii=False))
            continue

        # Restore cancel gate if present (optional)
        try:
            if isinstance(row.get("cancel_gate_state", None), dict):
                engine.restore_cancel_gate_state(row.get("cancel_gate_state"))
        except Exception:
            pass

        indicators = row.get("indicators") or {}
        if not isinstance(indicators, dict):
            indicators = {}

        rt_snap = row.get("runtime_snapshot")
        if isinstance(rt_snap, dict):
            indicators = dict(indicators)
            indicators["runtime_snapshot"] = rt_snap

        # Minimal runtime stub; runtime snapshot is provided via indicators
        runtime_stub: Dict[str, Any] = {"symbol": row.get("symbol")}
        try:
            out1 = engine.build(
                runtime=runtime_stub,
                symbol=row["symbol"],
                tf=row.get("tf", "1s"),
                direction=row["direction"],
                tick_ts_ms=int(row["tick_ts_ms"]),
                price=float(row["price"]),
                delta_z=float(row["delta_z"]),
                cfg=row.get("cfg") or {},
                indicators=indicators,
            )
            out1_dict = out1[0].to_dict() if out1[0] else {}
            out2 = engine.build(
                runtime=runtime_stub,
                symbol=row["symbol"],
                tf=row.get("tf", "1s"),
                direction=row["direction"],
                tick_ts_ms=int(row["tick_ts_ms"]),
                price=float(row["price"]),
                delta_z=float(row["delta_z"]),
                cfg=row.get("cfg") or {},
                indicators=indicators,
            )
            out2_dict = out2[0].to_dict() if out2[0] else {}
        except Exception as e:
            bad += 1
            if args.report:
                print(json.dumps({"row": i, "error": f"{type(e).__name__}: {e}"}, ensure_ascii=False))
            continue

        h1, h2 = _hash_obj(out1_dict), _hash_obj(out2_dict)
        if h1 != h2:
            nondet += 1
            if args.report:
                print(json.dumps({"row": i, "nondet": True, "h1": h1, "h2": h2}, ensure_ascii=False))

        if args.report:
            print(json.dumps({"row": i, "ok": True, "hash": h1, "symbol": row["symbol"], "dir": row["direction"]}, ensure_ascii=False))

    summary = {"rows": len(rows), "bad": bad, "nondet": nondet, "strict": bool(args.strict)}
    if args.report:
        print(json.dumps({"summary": summary}, ensure_ascii=False))

    if bad > 0:
        return 2
    if args.strict and nondet > 0:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

