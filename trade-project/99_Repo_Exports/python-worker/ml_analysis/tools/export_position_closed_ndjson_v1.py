#!/usr/bin/env python3
from __future__ import annotations

"""export_position_closed_ndjson_v1.py

Phase2 helper: export POSITION_CLOSED events from Postgres/Timescale into NDJSON.

Why:
  - build_dataset_from_inputs_outcomes_v2.py expects an NDJSON of closed trades
    (historically exported from events:trades). In this codebase we usually archive
    those events into Postgres table `position_events`.
  - Also supports `trades_closed` table directly (column mapping applied automatically).

Output:
  - One JSON dict per line, event payload merged with key columns:
    {sid, symbol, ts_ms, event_type, r_mult, pnl, pnl_net, risk_usd, close_reason, ...}

Usage:
  # From position_events (default):
  python3 ml_analysis/tools/export_position_closed_ndjson_v1.py \
    --dsn "$TRADES_DB_DSN" \
    --start-ts-ms 1700000000000 --end-ts-ms 1700864000000 \
    --out /tmp/closed.ndjson

  # From trades_closed (fallback when position_events is empty):
  python3 ml_analysis/tools/export_position_closed_ndjson_v1.py \
    --dsn "$TRADES_DB_DSN" --table trades_closed \
    --start-ts-ms 1700000000000 --end-ts-ms 1700864000000 \
    --out /tmp/closed.ndjson

Env fallback for DSN:
  - TRADES_DB_DSN | DATABASE_URL | PG_DSN
"""


import argparse
import json
import os
from typing import Any

import psycopg2


# Columns present in trades_closed but absent/differently-named in position_events.
# Mapping: trades_closed column → canonical output field expected by ml_labeling.py
_TRADES_CLOSED_MAPPING = {
    "r_multiple": "r_mult",
    "pnl_net": "pnl",
    "notional_usd": "risk_usd",
    "close_reason": "reason",
    "close_reason_raw": "reason_raw",
    "exit_ts_ms": "ts_ms",
}


def pick_dsn(cli_dsn: str) -> str:
    dsn = (cli_dsn or '').strip()
    if dsn:
        return dsn
    for k in ("TRADES_DB_DSN", "DATABASE_URL", "PG_DSN"):
        v = os.getenv(k, "").strip()
        if v:
            return v
    return ""


def _loads_maybe_json(v: Any) -> dict[str, Any]:
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


def _build_trades_closed_query(sym: str, limit: int) -> tuple[str, list[Any], list[str]]:
    """Build SELECT for trades_closed with canonical field aliases."""
    cols = [
        "exit_ts_ms",
        "sid",
        "symbol",
        "direction",
        "r_multiple",
        "pnl_net",
        "notional_usd",
        "close_reason",
        "close_reason_raw",
        "is_virtual",
        "source",
        "entry_ts_ms",
        "entry_price",
        "exit_price",
    ]
    select = ", ".join(cols)
    q = f"""
        SELECT {select}
        FROM trades_closed
        WHERE exit_ts_ms >= %s AND exit_ts_ms < %s
          AND sid IS NOT NULL AND sid != ''
    """
    params: list[Any] = []
    if sym:
        q += " AND UPPER(symbol) = %s"
        params.append(sym)
    q += " ORDER BY exit_ts_ms ASC"
    if limit > 0:
        q += f" LIMIT {limit}"
    return q, params, cols


def _row_trades_closed(cols: list[str], values: tuple) -> dict[str, Any]:
    """Convert trades_closed row to canonical output payload."""
    raw = dict(zip(cols, values))
    payload: dict[str, Any] = {}
    for col, val in raw.items():
        canonical = _TRADES_CLOSED_MAPPING.get(col, col)
        payload[canonical] = val
        # Also keep original name so callers that know trades_closed naming work too
        if canonical != col:
            payload[col] = val
    # Ensure expected top-level fields
    payload.setdefault("event_type", "POSITION_CLOSED")
    if isinstance(payload.get("is_virtual"), bool):
        payload["is_virtual"] = 1 if payload["is_virtual"] else 0
    return payload


def main() -> None:
    ap = argparse.ArgumentParser(description="Export POSITION_CLOSED events from position_events or trades_closed")
    ap.add_argument("--dsn", default="", help="Postgres/Timescale DSN; falls back to TRADES_DB_DSN/DATABASE_URL")
    ap.add_argument("--table", default=os.getenv("CLOSED_SOURCE_TABLE", "position_events"),
                    help="table: position_events (default) or trades_closed")
    ap.add_argument("--start-ts-ms", type=int, required=True, help="start timestamp in ms (inclusive)")
    ap.add_argument("--end-ts-ms", type=int, required=True, help="end timestamp in ms (exclusive)")
    ap.add_argument("--out", required=True, help="output ndjson path")
    ap.add_argument("--symbol", default="", help="optional symbol filter (e.g. BTCUSDT)")
    ap.add_argument("--limit", type=int, default=0, help="optional LIMIT (0=unlimited)")

    args = ap.parse_args()

    dsn = pick_dsn(str(args.dsn))
    if not dsn:
        raise SystemExit("No DSN provided. Set --dsn or TRADES_DB_DSN/DATABASE_URL.")

    table = str(args.table).strip().lower()
    sym = str(args.symbol or "").upper().strip()
    limit = int(args.limit or 0)

    os.makedirs(os.path.dirname(str(args.out)) or ".", exist_ok=True)

    n = 0
    with psycopg2.connect(dsn) as conn, conn.cursor() as cur:
        if table == "trades_closed":
            q, extra_params, cols = _build_trades_closed_query(sym, limit)
            params: list[Any] = [int(args.start_ts_ms), int(args.end_ts_ms)] + extra_params
            cur.execute(q, params)
            with open(str(args.out), "w", encoding="utf-8") as f:
                for row in cur:
                    payload = _row_trades_closed(cols, row)
                    f.write(json.dumps(payload, ensure_ascii=False) + "\n")
                    n += 1
        else:
            # Default: position_events
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

            cur.execute(q, params)
            with open(str(args.out), "w", encoding="utf-8") as f:
                for (ts_ms, sid, symbol, event_type, meta_json, payload_json) in cur:
                    payload = _loads_maybe_json(payload_json)
                    meta = _loads_maybe_json(meta_json)

                    if sid and "sid" not in payload:
                        payload["sid"] = sid
                    if symbol and "symbol" not in payload:
                        payload["symbol"] = symbol
                    if event_type and "event_type" not in payload:
                        payload["event_type"] = event_type
                    if ts_ms and "ts_ms" not in payload:
                        payload["ts_ms"] = int(ts_ms)
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
