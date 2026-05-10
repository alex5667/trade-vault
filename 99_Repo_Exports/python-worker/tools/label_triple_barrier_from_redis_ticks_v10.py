# python-worker/tools/label_triple_barrier_from_redis_ticks_v10.py
from __future__ import annotations

import argparse
import json
import math
import os
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any

import redis

from utils.time_utils import get_ny_time_millis
import contextlib
from core.redis_keys import RedisStreams as RS


# =============================================================================
# v10 Triple-Barrier labeling using Redis tick streams: stream:tick_{SYMBOL}
#
# Tick schema:
#   - flat hash fields: symbol, ts, price, ...
#   - optional "data" JSON string: merged over flat fields
#
# Price selection (microstructure-friendly):
#   mid > price > last > (bid+ask)/2
#
# Barriers:
#   stop_bps (preferred) -> atr_bps -> fallback bps
#
# IMPORTANT: your tick streams are capped by MAXLEN=200k, so retention is ~1-2h.
# =============================================================================
def _i(x: Any, d: int = 0) -> int:
    try:
        if x is None:
            return d
        if isinstance(x, bool):
            return int(x)
        if isinstance(x, (int, float)):
            return int(x)
        s = str(x).strip()
        if not s:
            return d
        return int(float(s))
    except Exception:
        return d


def _f(x: Any, d: float = 0.0) -> float:
    try:
        if x is None:
            return d
        if isinstance(x, bool):
            return float(int(x))
        if isinstance(x, (int, float)):
            v = float(x)
        else:
            s = str(x).strip()
            if not s:
                return d
            v = float(s)
        return v if math.isfinite(v) else d
    except Exception:
        return d


def _safe_json_loads(s: Any) -> dict[str, Any]:
    try:
        if not s:
            return {}
        if isinstance(s, dict):
            return s
        return json.loads(s)
    except Exception:
        return {}


def _merge_tick_fields(flat: dict[str, Any]) -> dict[str, Any]:
    """
    Merges optional JSON string field 'data' over flat fields.
    This matches your tick stream examples.
    """
    merged = dict(flat)
    nested = _safe_json_loads(flat.get("data"))
    if isinstance(nested, dict) and nested:
        merged.update(nested)
    return merged


def _pick_tick_ts_ms(obj: dict[str, Any]) -> int:
    # Primary: ts (epoch ms). Fallbacks: ts_ms, timestamp.
    ts = _i(obj.get("ts"), 0)
    if ts <= 0:
        ts = _i(obj.get("ts_ms"), 0)
    if ts <= 0:
        ts = _i(obj.get("timestamp"), 0)
    return ts


def _pick_tick_price(obj: dict[str, Any]) -> float:
    """
    Price priority:
      mid > price > last > (bid+ask)/2
    """
    px = _f(obj.get("mid"), 0.0)
    if px <= 0.0:
        px = _f(obj.get("price"), 0.0)
    if px <= 0.0:
        px = _f(obj.get("last"), 0.0)
    if px <= 0.0:
        bid = _f(obj.get("bid"), 0.0)
        ask = _f(obj.get("ask"), 0.0)
        if bid > 0.0 and ask > 0.0:
            px = (bid + ask) / 2.0
    return px


# -----------------------------
# Triple barrier logic
# -----------------------------
@dataclass
class Barriers:
    tp_bps: float
    sl_bps: float
    scale_bps: float  # which scale was used (stop_bps or atr_bps or 0)


def infer_tp_sl_bps(
    indicators: dict[str, Any],
    *,
    tp_k_atr: float,
    sl_k_atr: float,
    fallback_tp_bps: float,
    fallback_sl_bps: float,
) -> Barriers:
    stop_bps = _f(indicators.get("stop_bps", 0.0), 0.0)
    atr_bps = _f(indicators.get("atr_bps", 0.0), 0.0)
    if stop_bps > 1e-6:
        return Barriers(tp_bps=tp_k_atr * stop_bps, sl_bps=sl_k_atr * stop_bps, scale_bps=stop_bps)
    if atr_bps > 1e-6:
        return Barriers(tp_bps=tp_k_atr * atr_bps, sl_bps=sl_k_atr * atr_bps, scale_bps=atr_bps)
    return Barriers(tp_bps=fallback_tp_bps, sl_bps=fallback_sl_bps, scale_bps=0.0)


def _signed_ret_bps(direction: str, entry_px: float, px: float) -> float:
    if entry_px <= 0.0 or px <= 0.0:
        return 0.0
    ret = 10000.0 * (px - entry_px) / entry_px
    return ret if (direction or "").upper() == "LONG" else -ret


def _pick_entry_price(path: list[tuple[int, float]]) -> float:
    # First tick price in path
    for _, px in path:
        if px > 0.0:
            return px
    return 0.0


def _slice_path(series: list[tuple[int, float]], ts0: int, ts1: int) -> list[tuple[int, float]]:
    # series is sorted by ts ascending; keep [ts0..ts1]
    if ts1 <= ts0:
        return []
    out: list[tuple[int, float]] = []
    for ts, px in series:
        if ts < ts0:
            continue
        if ts > ts1:
            break
        out.append((ts, px))
    return out


def label_one(
    inp: dict[str, Any],
    tick_series: list[tuple[int, float]],
    *,
    h_ms: int,
    tp_k_atr: float,
    sl_k_atr: float,
    fallback_tp_bps: float,
    fallback_sl_bps: float,
) -> dict[str, Any]:
    out = dict(inp)
    sym = (inp.get("symbol") or "").upper()
    ts0 = _i(inp.get("ts_ms", inp.get("ts", 0)), 0)
    direction = (inp.get("direction", "") or "").upper()

    indicators = inp.get("indicators") if isinstance(inp.get("indicators"), dict) else {}
    b = infer_tp_sl_bps(
        indicators,
        tp_k_atr=tp_k_atr,
        sl_k_atr=sl_k_atr,
        fallback_tp_bps=fallback_tp_bps,
        fallback_sl_bps=fallback_sl_bps,
    )

    ts1 = ts0 + int(h_ms)
    path = _slice_path(tick_series, ts0, ts1)

    entry_px = _f(inp.get("entry_px", 0.0), 0.0)
    if entry_px <= 0.0:
        entry_px = _pick_entry_price(path)

    # Defaults
    out["tb_tp_bps"] = float(b.tp_bps)
    out["tb_sl_bps"] = float(b.sl_bps)
    out["tb_scale_bps"] = float(b.scale_bps)
    out["tb_entry_px"] = float(entry_px)
    out["tb_entry_ts_ms"] = int(ts0)

    if not sym or ts0 <= 0 or direction not in ("LONG", "SHORT") or entry_px <= 0.0 or not path:
        out["tb_label"] = "NO_TICKS"
        out["tb_y_edge"] = 0
        out["tb_ret_bps"] = 0.0
        out["tb_r_mult"] = 0.0
        out["tb_t_hit_ms"] = int(ts1)
        return out

    tp = float(b.tp_bps)
    sl = float(b.sl_bps)
    hit = "TIMEOUT"
    t_hit = ts1
    ret_bps_at_hit = 0.0

    for ts, px in path:
        if px <= 0.0:
            continue
        r_bps = _signed_ret_bps(direction, entry_px, px)

        if tp > 0.0 and r_bps >= tp:
            hit = "TP"
            t_hit = ts
            ret_bps_at_hit = r_bps
            break
        if sl > 0.0 and r_bps <= -sl:
            hit = "SL"
            t_hit = ts
            ret_bps_at_hit = r_bps
            break

        ret_bps_at_hit = r_bps

    out["tb_label"] = hit
    out["tb_t_hit_ms"] = int(t_hit)
    out["tb_ret_bps"] = float(ret_bps_at_hit)
    out["tb_y_edge"] = 1 if hit == "TP" else 0
    out["tb_r_mult"] = float(ret_bps_at_hit / b.scale_bps) if b.scale_bps > 1e-9 else 0.0

    return out


# -----------------------------
# Redis tick loading (XRANGE)
# -----------------------------
def _stream_id_for_ms_start(ms: int) -> str:
    return f"{int(ms)}-0"


def _stream_id_for_ms_end(ms: int) -> str:
    return f"{int(ms)}-999999"


def _stream_age_hours(r: redis.Redis, stream: str) -> float:
    """
    Estimate available history from stream IDs (ms-seq).
    Returns 0.0 if not available.
    """
    try:
        first = r.xrange(stream, min="-", max="+", count=1)
        last = r.xrevrange(stream, max="+", min="-", count=1)
        if not first or not last:
            return 0.0
        f_id = first[0][0]
        l_id = last[0][0]
        if isinstance(f_id, bytes):
            f_id = f_id.decode("utf-8", "ignore")
        if isinstance(l_id, bytes):
            l_id = l_id.decode("utf-8", "ignore")
        f_ms = int(str(f_id).split("-", 1)[0])
        l_ms = int(str(l_id).split("-", 1)[0])
        if l_ms <= f_ms:
            return 0.0
        return (l_ms - f_ms) / 1000.0 / 3600.0
    except Exception:
        return 0.0


def load_ticks_for_symbol(
    r: redis.Redis,
    *,
    stream: str,
    start_ms: int,
    end_ms: int,
    max_rows: int,
) -> list[tuple[int, float]]:
    """
    Reads ticks forward via XRANGE within [start_ms, end_ms] using stream IDs.
    Assumes stream IDs are '<ms>-<seq>' (confirmed in your repo).
    """
    start_id = _stream_id_for_ms_start(start_ms)
    end_id = _stream_id_for_ms_end(end_ms)

    out: list[tuple[int, float]] = []
    last_id = start_id
    scanned = 0

    while scanned < max_rows:
        batch = r.xrange(stream, min=last_id, max=end_id, count=2000)
        if not batch:
            break

        for msg_id, fields in batch:
            scanned += 1
            if msg_id == last_id and out:
                continue

            if not isinstance(fields, dict):
                continue

            merged = _merge_tick_fields(fields)
            ts_ms = _pick_tick_ts_ms(merged)
            if ts_ms <= 0:
                continue
            if ts_ms < start_ms:
                continue
            if ts_ms > end_ms:
                return out

            px = _pick_tick_price(merged)
            if px <= 0.0:
                continue

            out.append((int(ts_ms), float(px)))
            last_id = msg_id

        # advance last_id by +1 seq to avoid inclusive min re-reading
        if isinstance(last_id, bytes):
            last_id = last_id.decode("utf-8", "ignore")
        if isinstance(last_id, str) and "-" in last_id:
            ms_s, seq_s = last_id.split("-", 1)
            with contextlib.suppress(Exception):
                last_id = f"{int(ms_s)}-{int(seq_s) + 1}"

    return out


def read_ndjson(path: str) -> Iterable[dict[str, Any]]:
    with open(path, encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if not s:
                continue
            try:
                obj = json.loads(s)
            except Exception:
                continue
            if isinstance(obj, dict):
                yield obj


def write_ndjson(path: str, rows: Iterable[dict[str, Any]]) -> None:
    with open(path, "w", encoding="utf-8") as f:
        for r0 in rows:
            f.write(json.dumps(r0, ensure_ascii=False, separators=(",", ":")))
            f.write("\n")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--inputs", required=True, help="NDJSON from signals:of:inputs export")
    ap.add_argument("--out", required=True, help="NDJSON output (adds tb_* fields)")

    # Ticks are in a dedicated Redis instance (scanner-redis-ticks). Keep separate URL.
    ap.add_argument("--ticks-redis-url", default=os.getenv("TICKS_REDIS_URL", os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")))
    ap.add_argument("--tick-stream-prefix", default=RS.TB_TICK_PREFIX, help="final stream = prefix + SYMBOL (upper)")

    # IMPORTANT: retention is ~1-2 hours due to MAXLEN=200k.
    ap.add_argument("--since-hours", type=float, default=1.0, help="how far back to allow scanning ticks (suggest 0.5..1.0)")
    ap.add_argument("--max-rows-per-symbol", type=int, default=200_000, help="hard cap (MAXLEN is 200k in your tick streams)")

    ap.add_argument("--print-stream-age", action="store_true", help="print per-symbol stream age estimate")

    ap.add_argument("--h-ms", type=int, default=180_000, help="time barrier horizon in ms (default 3m)")
    ap.add_argument("--tp-k-atr", type=float, default=1.0)
    ap.add_argument("--sl-k-atr", type=float, default=1.0)
    ap.add_argument("--fallback-tp-bps", type=float, default=30.0)
    ap.add_argument("--fallback-sl-bps", type=float, default=30.0)

    args = ap.parse_args()

    inputs = list(read_ndjson(args.inputs))
    if not inputs:
        raise SystemExit("inputs is empty or unreadable")

    # Determine per-symbol time windows:
    # start = min(ts_ms) - small pad; end = max(ts_ms + h_ms) + pad
    by_sym: dict[str, list[dict[str, Any]]] = {}
    for x in inputs:
        sym = (x.get("symbol") or "").upper()
        if not sym:
            continue
        by_sym.setdefault(sym, []).append(x)

    now_ms = get_ny_time_millis()
    since_ms = now_ms - int(float(args.since_hours) * 3600.0 * 1000.0)

    # Ticks Redis should be decode_responses=True: we want str fields.
    r_ticks = redis.Redis.from_url(args.ticks_redis_url, decode_responses=True)

    tick_map: dict[str, list[tuple[int, float]]] = {}

    for sym, rows in by_sym.items():
        ts_vals = [ _i(v.get("ts_ms", v.get("ts", 0)), 0) for v in rows ]
        ts_vals = [t for t in ts_vals if t > 0]
        if not ts_vals:
            tick_map[sym] = []
            continue

        ts_min = min(ts_vals)
        ts_max = max(ts_vals)

        # bound by since-hours (because ticks are not retained long)
        start_ms = max(since_ms, ts_min - 5_000)
        end_ms = ts_max + int(args.h_ms) + 5_000

        stream = f"{args.tick_stream_prefix}{sym}"

        if args.print_stream_age:
            age_h = _stream_age_hours(r_ticks, stream)
            print(f"tick_stream_age_hours {sym} {age_h:.3f}")

        series = load_ticks_for_symbol(
            r_ticks,
            stream=stream,
            start_ms=start_ms,
            end_ms=end_ms,
            max_rows=int(args.max_rows_per_symbol),
        )
        tick_map[sym] = series

    labeled = [
        label_one(
            x,
            tick_map.get((x.get('symbol') or '').upper(), []),
            h_ms=int(args.h_ms),
            tp_k_atr=float(args.tp_k_atr),
            sl_k_atr=float(args.sl_k_atr),
            fallback_tp_bps=float(args.fallback_tp_bps),
            fallback_sl_bps=float(args.fallback_sl_bps),
        )
        for x in inputs
    ]

    write_ndjson(args.out, labeled)


if __name__ == "__main__":
    main()

