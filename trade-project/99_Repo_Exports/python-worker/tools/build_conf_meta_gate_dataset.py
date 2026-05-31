"""CLI: build the confidence meta-gate training dataset.

Usage:
    python -m tools.build_conf_meta_gate_dataset \
        --since-ms 1700000000000 \
        --until-ms 1700864000000 \
        --out /tmp/conf_meta_gate_dataset.ndjson \
        --horizons-ms 600000,1800000,3600000 \
        --tp-min-bps 5 --tp-max-bps 80 \
        --sl-min-bps 5 --sl-max-bps 60

Reads TRADES_DB_DSN unless --dsn is given. Default time window is the
last 14 days when --since/--until are omitted.
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
import time

from calibration.conf_meta_gate_dataset import (
    CompatibilityFilter,
    build_dataset,
    write_ndjson,
)


def _parse_horizons(raw: str) -> frozenset[int]:
    if not raw or not raw.strip():
        return frozenset()
    out: set[int] = set()
    for chunk in raw.split(","):
        chunk = chunk.strip()
        if not chunk:
            continue
        try:
            out.add(int(chunk))
        except ValueError:
            continue
    return frozenset(out)


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--dsn", default=os.environ.get("TRADES_DB_DSN", ""))
    p.add_argument("--since-ms", type=int, default=0)
    p.add_argument("--until-ms", type=int, default=0)
    p.add_argument("--days", type=int, default=14,
                   help="window length when since/until omitted")
    p.add_argument("--out", required=True)
    p.add_argument("--horizons-ms", default="",
                   help="comma-separated allowlist; empty = any")
    p.add_argument("--tp-min-bps", type=float, default=1.0)
    p.add_argument("--tp-max-bps", type=float, default=200.0)
    p.add_argument("--sl-min-bps", type=float, default=1.0)
    p.add_argument("--sl-max-bps", type=float, default=200.0)
    p.add_argument("--max-rows", type=int, default=200_000)
    p.add_argument("--log-level", default="INFO")
    args = p.parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    if not args.dsn:
        print("DSN required: pass --dsn or set TRADES_DB_DSN", file=sys.stderr)
        return 2

    now_ms = int(time.time() * 1000)
    until_ms = args.until_ms or now_ms
    since_ms = args.since_ms or (until_ms - args.days * 86_400_000)

    import psycopg2  # type: ignore

    conn = psycopg2.connect(args.dsn)
    try:
        flt = CompatibilityFilter(
            horizon_ms_allowed=_parse_horizons(args.horizons_ms),
            tp_bps_min=args.tp_min_bps,
            tp_bps_max=args.tp_max_bps,
            sl_bps_min=args.sl_min_bps,
            sl_bps_max=args.sl_max_bps,
        )
        rows = build_dataset(
            conn,
            since_ms=since_ms,
            until_ms=until_ms,
            flt=flt,
            max_rows_per_cohort=args.max_rows,
        )
    finally:
        conn.close()

    n = write_ndjson(rows, args.out)
    print(f"wrote {n} rows → {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
