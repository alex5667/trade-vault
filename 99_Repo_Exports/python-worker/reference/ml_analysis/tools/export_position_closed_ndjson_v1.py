#!/usr/bin/env python3
"""export_position_closed_ndjson_v1.py

Phase2 helper: export POSITION_CLOSED events from Postgres/Timescale into NDJSON.

Why:
  - build_dataset_from_inputs_outcomes_v2.py expects an NDJSON of closed trades
    (historically exported from events:trades). In this codebase we usually archive
    those events into Postgres table `position_events`.

Output:
  - One JSON dict per line, event payload merged with key columns:
    {sid, symbol, ts_ms, event_type, ...payload_json..., meta?...}

Usage:
  python3 ml_analysis/tools/export_position_closed_ndjson_v1.py \
    --dsn "$TRADES_DB_DSN" \
    --start-ts-ms 1700000000000 --end-ts-ms 1700864000000 \
    --out /tmp/closed.ndjson

Env fallback for DSN:
  - TRADES_DB_DSN | DATABASE_URL | PG_DSN
"""

from __future__ import annotations

import argparse
import json
import os
from typing import Any, Dict, Optional

import psycopg2


def pick_dsn(cli_dsn: str) -> str:
    dsn = (cli_dsn or '').strip()
    if dsn:
        return dsn
    for k in ("TRADES_DB_DSN", "DATABASE_URL", "PG_DSN"):
        v = os.getenv(k, "").strip()
        if v:
            return v
    return ""


def _loads_maybe_json(v: Any) -> Dict[str, Any]:
    if isinstance(v, dict):
        return v
    if v is None:
        return {}
    if isinstance(v, bytes):
        try:
            v = v.decode("utf-8", "ignore")
        except Exception:
            return {}
    if isinstance(v, str):
        s = v.strip()
        if not s:
            return {}
        try:
            obj = json.loads(s)
            return obj if isinstance(obj, dict) else {}
        except Exception:
            return {}
    return {}


def main() -> None:
    ap = argparse.ArgumentParser(description="Export POSITION_CLOSED events from position_events table")
    ap.add_argument("--dsn", default="", help="Postgres/Timescale DSN; falls back to TRADES_DB_DSN/DATABASE_URL")
    ap.add_argument("--table", default="position_events", help="table name (default: position_events)")
    ap.add_argument("--start-ts-ms", type=int, required=True, help="start timestamp in ms (inclusive)")
    ap.add_argument("--end-ts-ms", type=int, required=True, help="end timestamp in ms (exclusive)")
    ap.add_argument("--out", required=True, help="output ndjson path")
    ap.add_argument("--symbol", default="", help="optional symbol filter (e.g. BTCUSDT)")
    ap.add_argument("--limit", type=int, default=0, help="optional LIMIT (0=unlimited)")

    args = ap.parse_args()

    dsn = pick_dsn(str(args.dsn))
    if not dsn:
        raise SystemExit("No DSN provided. Set --dsn or TRADES_DB_DSN/DATABASE_URL.")

    table = str(args.table)
    sym = str(args.symbol or "").upper().strip()
    limit = int(args.limit or 0)

    os.makedirs(os.path.dirname(str(args.out)) or ".", exist_ok=True)

    q = f"""
        SELECT
            ts_ms,
            sid,
            symbol,
            event_type,
            meta_json::text as meta_json,
            payload_json::text as payload_json
        FROM {table}
        WHERE ts_ms >= %s AND ts_ms < %s
          AND event_type IN ('POSITION_CLOSED','CLOSE')
    """

    params = [int(args.start_ts_ms), int(args.end_ts_ms)]
    if sym:
        q += " AND UPPER(symbol) = %s"
        params.append(sym)

    q += " ORDER BY ts_ms ASC"
    if limit > 0:
        q += f" LIMIT {limit}"

    n = 0
    with psycopg2.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(q, params)
            with open(str(args.out), "w", encoding="utf-8") as f:
                for (ts_ms, sid, symbol, event_type, meta_json, payload_json) in cur:
                    payload = _loads_maybe_json(payload_json)
                    meta = _loads_maybe_json(meta_json)

                    # Ensure minimal fields expected by dataset builder / labeling.
                    if sid and "sid" not in payload:
                        payload["sid"] = sid
                    if symbol and "symbol" not in payload:
                        payload["symbol"] = symbol
                    if event_type and "event_type" not in payload:
                        payload["event_type"] = event_type
                    if ts_ms and "ts_ms" not in payload:
                        payload["ts_ms"] = int(ts_ms)

                    # Keep meta for risk/pnl fallbacks.
                    if meta and "meta" not in payload:
                        payload["meta"] = meta

                    f.write(json.dumps(payload, ensure_ascii=False) + "\n")
                    n += 1

    print(json.dumps({
        "table": table,
        "start_ts_ms": int(args.start_ts_ms),
        "end_ts_ms": int(args.end_ts_ms),
        "symbol": sym,
        "rows": n,
        "out": str(args.out),
    }, ensure_ascii=False))


if __name__ == "__main__":
    main()
