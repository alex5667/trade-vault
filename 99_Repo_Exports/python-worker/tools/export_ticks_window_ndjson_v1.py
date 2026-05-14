from __future__ import annotations

import argparse
import json
import os
import time
from typing import Any

import redis


def _get_ny_time_millis() -> int:
    return int(time.time() * 1000)


def _safe_json(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"))


def _i(x: Any, d: int = 0) -> int:
    try:
        return int(float(x))
    except Exception:
        return d


def _f(x: Any, d: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return d


def _stream_id_ms(msg_id: str) -> int:
    try:
        return int(msg_id.split("-", 1)[0])
    except Exception:
        return 0


def _parse_payload(fields: dict[str, Any], payload_field: str) -> dict[str, Any]:
    """Parse payload from Redis stream fields.
    
    Supports:
    - Direct JSON in payload field: {"payload": "{\"ts\":...,\"price\":...}"}
    - Nested data field: {"data": "{\"ts\":...,\"price\":...}"}
    - Flat fields: {"ts": "...", "price": "..."}
    """
    # Try payload field first
    raw = fields.get(payload_field)
    if not raw:
        # Fallback to "data" field (common in tick streams)
        raw = fields.get("data")
    if not raw:
        # Fallback to flat fields
        return dict(fields)

    if isinstance(raw, bytes):
        try:
            raw = raw.decode("utf-8", "ignore")
        except Exception:
            return dict(fields)

    s = str(raw)
    if not s.strip().startswith("{"):
        # Not JSON, return flat fields
        return dict(fields)

    try:
        obj = json.loads(s)
        if isinstance(obj, dict):
            return obj
    except Exception:
        pass

    # Fallback to flat fields
    return dict(fields)


def export(
    *,
    r: redis.Redis,
    stream: str,
    since_ms: int,
    symbols: list[str] | None,
    out_path: str,
    max_scan: int,
    payload_field: str,
    ts_field: str,
    price_field: str,
    symbol_field: str,
) -> tuple[int, int]:
    scanned = 0
    rows: list[dict[str, Any]] = []
    last_id = "+"

    symset = set([s.upper() for s in symbols]) if symbols else None

    while scanned < max_scan:
        batch = r.xrevrange(stream, max=last_id, min="-", count=2000)
        if not batch:
            break
        if len(batch) == 1 and batch[0][0] == last_id:
            break
        for msg_id, fields in batch:
            scanned += 1
            if msg_id == last_id:
                continue
            last_id = msg_id
            if not isinstance(fields, dict):
                continue

            obj = _parse_payload(fields, payload_field) if payload_field else dict(fields)
            if not obj:
                obj = dict(fields)

            ingest_ms = _stream_id_ms(msg_id)
            ts = _i(obj.get(ts_field, 0), 0)
            if ts <= 0:
                ts = ingest_ms

            if ingest_ms and ingest_ms < since_ms:
                scanned = max_scan
                break

            sym = (obj.get(symbol_field, "") or "").upper()
            if symset is not None and sym not in symset:
                continue

            # Try multiple price fields (mid > price > last > bid/ask average)
            px = _f(obj.get(price_field, 0.0), 0.0)
            if px <= 0.0:
                # Fallback to common price field names
                px = _f(obj.get("mid", 0.0), 0.0)
            if px <= 0.0:
                px = _f(obj.get("price", 0.0), 0.0)
            if px <= 0.0:
                px = _f(obj.get("last", 0.0), 0.0)
            if px <= 0.0:
                # Try bid/ask average
                bid = _f(obj.get("bid", 0.0), 0.0)
                ask = _f(obj.get("ask", 0.0), 0.0)
                if bid > 0.0 and ask > 0.0:
                    px = (bid + ask) / 2.0

            if px <= 0.0:
                continue

            rows.append({"ts_ms": int(ts), "ingest_time_ms": int(ingest_ms), "symbol": sym, "price": float(px)})

    rows.reverse()
    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        for r0 in rows:
            f.write(_safe_json(r0) + "\n")
    return (len(rows), scanned)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--redis-url", default=os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0"))
    ap.add_argument("--stream", default="", help="Single stream name, or empty to use --symbols with stream:tick_<SYMBOL> pattern")
    ap.add_argument("--since-hours", type=float, default=24.0)
    ap.add_argument("--symbols", default="", help="comma-separated symbols; if --stream empty, uses stream:tick_<SYMBOL> for each")
    ap.add_argument("--out", required=True)

    ap.add_argument("--payload-field", default=os.getenv("TICKS_PAYLOAD_FIELD", "data"))
    ap.add_argument("--ts-field", default=os.getenv("TICKS_TS_FIELD", "ts"))
    ap.add_argument("--price-field", default=os.getenv("TICKS_PRICE_FIELD", "mid"))
    ap.add_argument("--symbol-field", default=os.getenv("TICKS_SYMBOL_FIELD", "symbol"))

    ap.add_argument("--max-scan", type=int, default=800_000)
    args = ap.parse_args()

    r = redis.Redis.from_url(args.redis_url, decode_responses=True)

    since_ms = int(_get_ny_time_millis()) - int(args.since_hours * 3600_000)
    symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()] if args.symbols else None

    # If stream is provided, use single stream export
    if args.stream:
        written, scanned = export(
            r=r,
            stream=args.stream,
            since_ms=since_ms,
            symbols=symbols,
            out_path=args.out,
            max_scan=args.max_scan,
            payload_field=args.payload_field,
            ts_field=args.ts_field,
            price_field=args.price_field,
            symbol_field=args.symbol_field,
        )
        print(_safe_json({"written": written, "scanned": scanned, "out": args.out}))
    elif symbols:
        # Multi-stream export: export from stream:tick_<SYMBOL> for each symbol
        all_rows: list[dict[str, Any]] = []
        total_scanned = 0
        for sym in symbols:
            stream_name = f"stream:tick_{sym}"
            # Extract symbol from stream name for symbol_field fallback
            temp_out = args.out + f".{sym}.tmp"
            written, scanned = export(
                r=r,
                stream=stream_name,
                since_ms=since_ms,
                symbols=None,  # Already filtered by stream
                out_path=temp_out,
                max_scan=args.max_scan,
                payload_field=args.payload_field,
                ts_field=args.ts_field,
                price_field=args.price_field,
                symbol_field=args.symbol_field,
            )
            total_scanned += scanned
            # Read and merge rows, ensuring symbol is set
            if os.path.exists(temp_out):
                with open(temp_out, encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            row = json.loads(line)
                            # Ensure symbol is set
                            if not row.get("symbol"):
                                row["symbol"] = sym
                            all_rows.append(row)
                        except Exception:
                            pass
                os.remove(temp_out)

        # Sort by timestamp and write merged output
        all_rows.sort(key=lambda x: x.get("ingest_time_ms", 0))
        os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
        with open(args.out, "w", encoding="utf-8") as f:
            for row in all_rows:
                f.write(_safe_json(row) + "\n")
        print(_safe_json({"written": len(all_rows), "scanned": total_scanned, "out": args.out}))
    else:
        print(_safe_json({"error": "Either --stream or --symbols must be provided"}))
        return


if __name__ == "__main__":
    main()
