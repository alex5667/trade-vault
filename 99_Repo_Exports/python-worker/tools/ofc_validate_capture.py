#!/usr/bin/env python3

from __future__ import annotations


import argparse

from pathlib import Path

from typing import Any, Dict, List, Set

import sys


ROOT = Path(__file__).resolve().parents[1]

sys.path.insert(0, str(ROOT))


from tools.ofc_common import iter_ndjson  # noqa



# Map engine runtime getattr -> snapshot keys

ALIAS = {

    "pressure": "pressure_hi",

    "last_wp": "last_wp_weak_any",

}



def _is_dict(x: Any) -> bool:

    return isinstance(x, dict)



def main() -> int:

    ap = argparse.ArgumentParser(description="Validate OFC_CAPTURE schema for golden replay.")

    ap.add_argument("--input", required=True, help="Path to capture ndjson")

    ap.add_argument("--max-rows", type=int, default=0)

    args = ap.parse_args()


    errors: List[str] = []

    n = 0

    for row in iter_ndjson(args.input):

        n += 1

        if args.max_rows and n > args.max_rows:

            break


        if row.get("schema") != "ofc_capture_v1":

            errors.append(f"row#{n}: bad schema={row.get('schema')!r}")


        for k in ["symbol", "tf", "direction", "tick_ts_ms", "price", "delta_z", "cfg", "indicators", "runtime_snapshot"]:

            if k not in row:

                errors.append(f"row#{n}: missing key {k}")

        if not _is_dict(row.get("indicators")):

            errors.append(f"row#{n}: indicators must be dict")

        if not _is_dict(row.get("cfg")):

            errors.append(f"row#{n}: cfg must be dict")

        snap = row.get("runtime_snapshot")

        if not _is_dict(snap):

            errors.append(f"row#{n}: runtime_snapshot must be dict")

            continue


        # Required snapshot fields (minimal set used by engine and evidence)

        required = {

            "last_obi_event",

            "last_iceberg_event",

            "last_ofi_event",

            "last_bar",

            "last_fp_edge",

            "last_div",

            "last_regime",

            "book_churn_hi",

            "dynamic_cfg",

            "pressure_hi",

            "cont_ctx_ts_ms",

            "liq_regime",

            "last_sweep",

            "last_reclaim",

            "last_wp_weak_any",

        }

        missing = [k for k in sorted(required) if k not in snap]

        if missing:

            errors.append(f"row#{n}: runtime_snapshot missing {missing}")


        # Gate state presence (optional, but recommended)

        if row.get("cancel_spike_state_before") is None:

            errors.append(f"row#{n}: cancel_spike_state_before missing (recommend capture for deterministic replay)")


        # bucket_id determinism

        ind = row.get("indicators") or {}

        if ind.get("bucket_id") is None and row.get("bucket_id") is None:

            errors.append(f"row#{n}: bucket_id missing in indicators/top-level; cancel gate replay may diverge")


    if errors:

        print("FAIL")

        for e in errors[:200]:

            print(" -", e)

        if len(errors) > 200:

            print(f"... {len(errors)-200} more")

        return 2

    print("OK")

    return 0



if __name__ == "__main__":

    raise SystemExit(main())
