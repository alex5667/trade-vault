from __future__ import annotations

import argparse
import json
import os
from typing import Any

from core.eff_quote_calibrator import EffQuoteCalibrator


def load_payload_ndjson(path: str) -> list[dict[str, Any]]:
    out = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            row = json.loads(line)
            p = row.get("payload")
            if isinstance(p, str):
                p = json.loads(p)
            out.append(p)
    return out


def _to_str(x: Any) -> str:
    try:
        if x is None:
            return ""
        if isinstance(x, (bytes, bytearray)):
            return x.decode("utf-8", "ignore")
        return str(x)
    except Exception:
        return ""


def load_payload_redis_streams(
    *,
    redis_url: str,
    stream: str,
    symbols_set: str,
    start_id: str,
    count: int,
    max_batches: int,
) -> list[dict[str, Any]]:
    """
    Split-streams migration helper:
      - if stream contains '{sym}', expand per symbol from symbols_set and read per-symbol streams
      - otherwise read the stream directly

    Each stream record may have:
      - field 'payload' as JSON string => used
      - or flat fields => treated as payload dict
    """
    import redis  # redis-py

    r = redis.Redis.from_url(redis_url, decode_responses=True)
    syms: list[str] = []
    if "{sym}" in stream:
        try:
            syms = sorted([_to_str(x) for x in (r.smembers(symbols_set) or set()) if _to_str(x)])
        except Exception:
            syms = []
    else:
        syms = ["_single_"]

    out: list[dict[str, Any]] = []
    for sym in syms:
        skey = stream.format(sym=sym) if sym != "_single_" else stream
        last = start_id
        for _ in range(max_batches):
            items = r.xread({skey: last}, count=count, block=0) or []
            if not items:
                break
            _sname, entries = items[0]
            if not entries:
                break
            for msg_id, fields in entries:
                last = _to_str(msg_id) or last
                d: dict[str, Any] = dict(fields or {})
                p = d.get("payload")
                if isinstance(p, str) and p and (p.startswith("{") or p.startswith("[")):
                    try:
                        p0 = json.loads(p)
                        if isinstance(p0, dict):
                            out.append(p0)
                            continue
                    except Exception:
                        pass
                out.append(d)
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--inputs", help="NDJSON with {'payload':{...}} or payload dict per line")
    ap.add_argument("--redis_url", default=os.getenv("REDIS_URL", "redis://localhost:6379/0"))
    ap.add_argument("--redis_stream", default=os.getenv("MICROBAR_PER_SYMBOL_STREAM_TEMPLATE", "events:microbar_closed:{sym}"), help="Redis stream key; supports '{sym}' expansion")
    ap.add_argument("--symbols_set", default=os.getenv("MICROBAR_SYMBOLS_SET", "events:microbar_closed:symbols"))
    ap.add_argument("--start_id", default="0-0")
    ap.add_argument("--count", type=int, default=1000)
    ap.add_argument("--max_batches", type=int, default=1000)
    ap.add_argument("--out", required=True)
    ap.add_argument("--min_samples", type=int, default=300)
    args = ap.parse_args()

    if args.redis_stream:
        rows = load_payload_redis_streams(
            redis_url=str(args.redis_url),
            stream=str(args.redis_stream),
            symbols_set=str(args.symbols_set),
            start_id=str(args.start_id),
            count=int(args.count),
            max_batches=int(args.max_batches),
        )
    else:
        if not args.inputs:
            raise SystemExit("--inputs is required when --redis_stream is not provided")
        rows = load_payload_ndjson(args.inputs)
    rows.sort(key=lambda x: (int(x.get("ts_ms", 0)), x.get("symbol", ""), x.get("scenario", "")))

    # per symbol calibrator
    cal: dict[str, EffQuoteCalibrator] = {}
    out: list[dict[str, Any]] = []

    for r in rows:
        sym = (r.get("symbol", ""))
        regime = (r.get("regime", "na") or "na")
        ts_ms = int(r.get("ts_ms", 0) or 0)
        effq = float(r.get("fp_eff_quote", 0.0) or 0.0)
        qd = float(r.get("fp_quote_delta", 0.0) or 0.0)
        cfg = r.get("cfg") or {}

        if sym not in cal:
            cal[sym] = EffQuoteCalibrator(min_samples=int(args.min_samples))

        c = cal[sym]
        if effq > 0 and qd > 0:
            c.update(regime=regime, eff_quote=effq, quote_delta=qd)

        th = c.thresholds(
            regime=regime,
            default_eff_th=float(cfg.get("abs_lvl_eff_quote_th", 0.0020)),
            default_min_qd=float(cfg.get("abs_lvl_min_quote_delta", 0.0)),
        )

        # Emit "audit-like" row (normalized later)
        out.append({
            "v": 1,
            "symbol": sym,
            "regime": regime,
            "ts_ms": ts_ms,
            "src": th.src,
            "n": th.n,
            "eff_quote_th": th.eff_quote_th,
            "min_quote_delta": th.min_quote_delta,
        })

    out.sort(key=lambda x: (x["ts_ms"], x["symbol"], x["regime"]))
    with open(args.out, "w", encoding="utf-8") as f:
        for x in out:
            f.write(json.dumps(x, ensure_ascii=False, sort_keys=True) + "\n")


if __name__ == "__main__":
    main()
