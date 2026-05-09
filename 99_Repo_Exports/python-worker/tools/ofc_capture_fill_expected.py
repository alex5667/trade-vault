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



def main() -> int:

    ap = argparse.ArgumentParser(description="Fill expected outputs into OFC_CAPTURE ndjson (best-effort).")

    ap.add_argument("--input", required=True)

    ap.add_argument("--output", required=True)

    ap.add_argument("--gate-state", default="import_before", choices=["import_before", "chain_after", "fresh", "none"])

    ap.add_argument("--sort", default="bucket_id", choices=["bucket_id", "tick_ts_ms", "none"])

    args = ap.parse_args()


    rows = list(iter_ndjson(args.input))

    if args.sort != "none":

        rows.sort(key=lambda r: (r.get(args.sort) is None, r.get(args.sort, 0)))


    out: list[dict[str, Any]] = []


    engine: OFConfirmEngine | None = None

    prev_state_after = None


    for r in rows:

        if engine is None or args.gate_state == "fresh":

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


        if indicators.get("bucket_id") is None and r.get("bucket_id") is not None:

            indicators["bucket_id"] = r.get("bucket_id")


        runtime = ReplayRuntime.from_snapshot(symbol=symbol, snap=snap)


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


        try:

            if hasattr(engine, "export_cancel_spike_state"):

                prev_state_after = engine.export_cancel_spike_state(symbol=symbol)

        except Exception:

            prev_state_after = None


        if not isinstance(r.get("expected"), dict):

            exp = {

                "ok": int(getattr(ofc, "ok", False)) if ofc is not None else 0,

                "ok_soft": int(getattr(ofc, "ok_soft", False)) if ofc is not None else 0,

                "score": float(getattr(ofc, "score", 0.0) or 0.0) if ofc is not None else 0.0,

            }

            try:

                ev = getattr(ofc, "evidence", None) or {}

                if isinstance(ev, dict):

                    exp["scenario_v4"] = (ev.get("scenario_v4", "") or "")

            except Exception:

                pass

            r["expected"] = exp

        r["build_us"] = int(r.get("build_us", 0) or build_us)

        if r.get("cancel_spike_state_after") is None:

            r["cancel_spike_state_after"] = prev_state_after

        out.append(r)


    write_ndjson(args.output, out)

    return 0



if __name__ == "__main__":

    raise SystemExit(main())
