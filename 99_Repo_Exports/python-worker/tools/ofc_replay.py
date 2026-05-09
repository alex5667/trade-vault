#!/usr/bin/env python3

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]

sys.path.insert(0, str(ROOT))


from core.of_confirm_engine import OFConfirmEngine  # noqa

from tools.ofc_common import ReplayRuntime, iter_ndjson, write_ndjson  # noqa



def _bool(x: Any) -> bool:

    return bool(int(x)) if isinstance(x, (int, str)) else bool(x)



def main() -> int:

    ap = argparse.ArgumentParser(description="Replay OFC_CAPTURE ndjson deterministically and validate outputs.")

    ap.add_argument("--input", required=True, help="Path to OFC_CAPTURE ndjson")

    ap.add_argument("--out", default="", help="Optional path to write replay outputs as ndjson")

    ap.add_argument("--max-rows", type=int, default=0, help="Limit number of rows (0 = all)")

    ap.add_argument("--sort", default="bucket_id", choices=["bucket_id", "tick_ts_ms", "none"], help="Sort rows before replay")

    ap.add_argument("--gate-state", default="import_before", choices=["import_before", "chain_after", "fresh", "none"],

                    help="How to handle stateful gate state (CancelSpikeGate)")

    ap.add_argument("--strict", action="store_true", help="Exit non-zero if any mismatch vs expected")

    args = ap.parse_args()


    rows: list[dict[str, Any]] = list(iter_ndjson(args.input))

    if args.max_rows and args.max_rows > 0:

        rows = rows[: args.max_rows]


    if args.sort != "none":

        key = args.sort

        rows.sort(key=lambda r: (r.get(key) is None, r.get(key, 0)))


    out_rows: list[dict[str, Any]] = []

    mismatches = 0


    engine: OFConfirmEngine | None = None


    prev_state_after = None

    for r in rows:

        if args.gate_state == "fresh" or engine is None:

            engine = OFConfirmEngine(version=3)


        symbol = (r.get("symbol", "") or "")

        tf = (r.get("tf", "1s") or "1s")

        direction = (r.get("direction", "") or "")

        tick_ts_ms = int(r.get("tick_ts_ms", 0) or 0)

        price = float(r.get("price", 0.0) or 0.0)

        delta_z = float(r.get("delta_z", 0.0) or 0.0)

        cfg = r.get("cfg") or {}

        indicators = dict(r.get("indicators") or {})

        absorption = r.get("absorption") if isinstance(r.get("absorption"), dict) else None

        snap = r.get("runtime_snapshot") or {}


        # Ensure bucket_id is present for cancellation gate determinism

        if indicators.get("bucket_id") is None and r.get("bucket_id") is not None:

            indicators["bucket_id"] = r.get("bucket_id")


        runtime = ReplayRuntime.from_snapshot(symbol=symbol, snap=snap)


        # Gate state management

        if args.gate_state == "import_before":

            st = r.get("cancel_spike_state_before")

            if st is not None and hasattr(engine, "import_cancel_spike_state"):

                engine.import_cancel_spike_state(st, replace=True)

        elif args.gate_state == "chain_after":

            if prev_state_after is not None and hasattr(engine, "import_cancel_spike_state"):

                engine.import_cancel_spike_state(prev_state_after, replace=True)


        t0 = time.perf_counter_ns()

        ofc, dec = engine.build(

            symbol=symbol,

            tf=tf,

            direction=direction,

            tick_ts_ms=tick_ts_ms,

            price=price,

            delta_z=delta_z,

            runtime=runtime,

            cfg=cfg,

            indicators=indicators,

            absorption=absorption,

        )

        build_us = int((time.perf_counter_ns() - t0) / 1000)


        # Capture state_after for chaining

        try:

            if hasattr(engine, "export_cancel_spike_state"):

                prev_state_after = engine.export_cancel_spike_state(symbol=symbol)

        except Exception:

            prev_state_after = None


        got = {

            "ok": int(getattr(ofc, "ok", False)) if ofc is not None else 0,

            "ok_soft": int(getattr(ofc, "ok_soft", False)) if ofc is not None else 0,

            "score": float(getattr(ofc, "score", 0.0) or 0.0) if ofc is not None else 0.0,

            "reason": str(getattr(ofc, "reason", "") or "") if ofc is not None else "none",

            "build_us": build_us,

        }

        try:

            ev = getattr(ofc, "evidence", None) or {}

            if isinstance(ev, dict):

                got["scenario_v4"] = (ev.get("scenario_v4", "") or "")

        except Exception:

            pass


        exp = r.get("expected") or {}

        same = True

        if exp:

            same = (

                int(exp.get("ok", 0) or 0) == got["ok"]

                and int(exp.get("ok_soft", 0) or 0) == got["ok_soft"]

                and abs(float(exp.get("score", 0.0) or 0.0) - got["score"]) <= 1e-9

            )

        if exp and not same:

            mismatches += 1


        out_rows.append(

            {

                "symbol": symbol,

                "tf": tf,

                "direction": direction,

                "tick_ts_ms": tick_ts_ms,

                "bucket_id": indicators.get("bucket_id"),

                "got": got,

                "expected": exp if exp else None,

                "match": bool(same) if exp else None,

            }

        )


    if args.out:

        write_ndjson(args.out, out_rows)


    if args.strict and mismatches > 0:

        return 2

    return 0



if __name__ == "__main__":

    raise SystemExit(main())
