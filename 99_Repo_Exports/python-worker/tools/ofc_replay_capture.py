#!/usr/bin/env python3
"""
Replay OFC capture (NDJSON) to validate determinism and snapshot completeness.

Usage:
  python -m tools.ofc_replay_capture --in /tmp/ofc_inputs.ndjson --out /tmp/ofc_replayed.ndjson --limit 1000

Notes:
- Expects each NDJSON row to contain: symbol, tf, direction, tick_ts_ms, price, delta_z, indicators, absorption, runtime_snapshot, cfg
- Produces rows with: ofc (dict), ok (int), reason (str), missing_snapshot (list)
"""
from __future__ import annotations

import argparse
import json
import sys
from typing import Any, Dict, Optional

from core.of_confirm_engine import OFConfirmEngine


def _load_ndjson(path: str):
    with open(path, "r", encoding="utf-8") as f:
        for ln in f:
            ln = ln.strip()
            if not ln:
                continue
            try:
                yield json.loads(ln)
            except Exception:
                continue


def main() -> int:
    ap = argparse.ArgumentParser(description="Replay OFC capture NDJSON")
    ap.add_argument("--in", dest="inp", required=True, help="input NDJSON with OFC inputs")
    ap.add_argument("--out", dest="out", required=True, help="output NDJSON with replay results")
    ap.add_argument("--limit", type=int, default=0, help="max rows to process (0 = all)")
    ap.add_argument("--strict", action="store_true", help="exit non-zero if any missing snapshot fields")
    args = ap.parse_args()

    eng = OFConfirmEngine()
    n = 0
    n_missing = 0

    with open(args.out, "w", encoding="utf-8") as w:
        for row in _load_ndjson(args.inp):
            n += 1
            if args.limit and n > args.limit:
                break

            try:
                tick_ts_ms = int(row.get("tick_ts_ms") or row.get("ts_ms") or 0)
            except Exception:
                tick_ts_ms = 0

            snap = row.get("runtime_snapshot") or {}
            missing = eng.validate_runtime_snapshot_contract(snap) if isinstance(snap, dict) else ["snap_not_dict"]
            if missing:
                n_missing += 1

            runtime = eng.build_runtime_from_snapshot(snap) if isinstance(snap, dict) and snap else None
            indicators = row.get("indicators") if isinstance(row.get("indicators"), dict) else {}
            # Restore cancellation gate state (if captured)
            if isinstance(snap, dict) and "cancel_gate_state" in snap and isinstance(snap.get("cancel_gate_state"), dict):
                indicators["cancel_gate_state"] = snap.get("cancel_gate_state")

            absorption = row.get("absorption") if isinstance(row.get("absorption"), dict) else None
            cfg = row.get("cfg") if isinstance(row.get("cfg"), dict) else {}

            eng.set_replay_time_ms(tick_ts_ms)
            try:
                ofc, dec = eng.build(
                    symbol=str(row.get("symbol") or ""),
                    tf=str(row.get("tf") or ""),
                    direction=str(row.get("direction") or ""),
                    tick_ts_ms=tick_ts_ms,
                    price=float(row.get("price") or 0.0),
                    delta_z=float(row.get("delta_z") or 0.0),
                    cfg=cfg,
                    indicators=indicators,
                    absorption=absorption,
                    runtime=runtime,
                )
                out_row: Dict[str, Any] = dict(row)
                out_row["missing_snapshot"] = missing
                out_row["ofc_ok"] = int(getattr(ofc, "ok", 0) or 0) if ofc is not None else 0
                out_row["ofc_reason"] = str(getattr(ofc, "reason", "") or "") if ofc is not None else ""
                # best-effort OFConfirmV3 -> dict
                try:
                    out_row["ofc"] = ofc.to_dict() if hasattr(ofc, "to_dict") else dict(ofc)  # type: ignore
                except Exception:
                    out_row["ofc"] = None
            except Exception as e:
                out_row = dict(row)
                out_row["missing_snapshot"] = missing
                out_row["ofc_ok"] = 0
                out_row["ofc_reason"] = "replay_error"
                out_row["ofc_error"] = str(e)

            w.write(json.dumps(out_row, ensure_ascii=False) + "\n")

    if args.strict and n_missing > 0:
        print(f"missing_snapshot_rows={n_missing} total_rows={n}", file=sys.stderr)
        return 2
    print(f"replay_done rows={n} missing_snapshot_rows={n_missing}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

