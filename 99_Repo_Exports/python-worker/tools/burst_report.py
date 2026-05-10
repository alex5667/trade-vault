import asyncio
import json
import os
import time
from collections import defaultdict

import redis.asyncio as aioredis

from utils.time_utils import get_ny_time_millis
import contextlib
from core.redis_keys import RedisStreams as RS

STREAM = os.getenv("BURST_AUDIT_STREAM", RS.BURST_AUDIT)
OUT_KEY_JSON = os.getenv("BURST_REPORT_KEY_JSON", "reports:burst:last")
OUT_KEY_MD = os.getenv("BURST_REPORT_KEY_MD", "reports:burst:last_md")
LOOKBACK_MS = int(os.getenv("BURST_REPORT_LOOKBACK_MS", "1200000"))  # 20 min
NOTIFY_STREAM = os.getenv("NOTIFY_STREAM", RS.NOTIFY_TELEGRAM)

def _now_ms() -> int:
    return get_ny_time_millis()

async def main() -> None:
    redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
    r = aioredis.from_url(redis_url, decode_responses=True, socket_connect_timeout=10, socket_timeout=30, max_connections=50)
    now = _now_ms()
    start_ms = now - LOOKBACK_MS
    # XREAD from approximate start by timestamp (best-effort): we scan last N via XREVRANGE
    try:
        rows = await r.xrevrange(STREAM, max="+", min="-", count=50000)
    except Exception:
        rows = []
    rows.reverse()

    agg = defaultdict(lambda: {"blocked":0,"replaced":0,"emit_pending":0,"emit_current":0,"by_event":defaultdict(int), "pressure":[], "spread":[], "regime":defaultdict(int)})
    total = 0
    for _id, fields in rows:
        try:
            ts_ms = int(fields.get("ts_ms") or 0)
            if ts_ms < start_ms:
                continue
            total += 1
            sym = (fields.get("symbol") or "NA")
            ev = (fields.get("event") or "NA")
            ind = json.loads(fields.get("ind") or "{}")
            rg = (ind.get("regime","na") or "na").lower()
            scn = (ind.get("scenario","") or "")
            key = f"{sym}|{rg}|{scn}"
            a = agg[key]
            a["by_event"][ev] += 1
            a["regime"][rg] += 1
            if ev.startswith("COOLDOWN_BLOCK"):
                a["blocked"] += 1
                if ev.endswith("REPLACE"):
                    a["replaced"] += 1
            if ev == "COOLDOWN_EMIT_PENDING":
                a["emit_pending"] += 1
            if ev == "COOLDOWN_EMIT_CURRENT":
                a["emit_current"] += 1
            try:
                a["pressure"].append(float(ind.get("pressure_sps",0.0) or 0.0))
                a["spread"].append(float(ind.get("spread_bp",0.0) or 0.0))
            except Exception:
                pass
        except Exception:
            continue

    # build report
    out = {"ts_ms": now, "lookback_ms": LOOKBACK_MS, "stream": STREAM, "total_events": total, "groups": {}}
    md_lines = [f"📊 <b>Burst Report (last {LOOKBACK_MS//60000}m)</b>", f"Ts: {time.strftime('%Y-%m-%d %H:%M:%S')}", f"Total events: {total}", ""]

    for k, a in agg.items():
        p = a["pressure"]
        s = a["spread"]
        def pct(x, q):
            if not x: return 0.0
            xs = sorted(x)
            i = int(q * (len(xs)-1))
            return float(xs[i])

        info = {
            "blocked": a["blocked"],
            "replaced": a["replaced"],
            "emit_pending": a["emit_pending"],
            "emit_current": a["emit_current"],
            "p50_pressure_sps": pct(p, 0.50),
            "p90_pressure_sps": pct(p, 0.90),
            "p50_spread_bp": pct(s, 0.50),
            "p90_spread_bp": pct(s, 0.90),
            "by_event": dict(a["by_event"]),
        }
        out["groups"][k] = info

    # markdown top lines
    import html
    md_lines.append("<b>Groups (sym|rg|scn):</b>")
    # Take top 10 most active groups by blocked count
    sorted_groups = sorted(out["groups"].items(), key=lambda x: x[1]["blocked"], reverse=True)[:10]
    for k, v in sorted_groups:
        k_esc = html.escape(str(k), quote=False)
        md_lines.append(f"• <code>{k_esc}</code>: blk={v['blocked']} repl={v['replaced']} emit_p={v['emit_pending']} p90_press={v['p90_pressure_sps']:.3f} p90_spr={v['p90_spread_bp']:.1f}")

    md = "\n".join(md_lines) + "\n"

    await r.set(OUT_KEY_JSON, json.dumps(out, ensure_ascii=False, separators=(",", ":")), ex=3600)
    await r.set(OUT_KEY_MD, md, ex=3600)

    # Send to Telegram
    if total > 0:
        with contextlib.suppress(Exception):
            await r.xadd(NOTIFY_STREAM, {"text": md}, maxlen=20000, approximate=True)

    await r.close()

if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--interval", type=int, default=int(os.getenv("BURST_REPORT_INTERVAL_SEC", "900")))
    ap.add_argument("--once", action="store_true")
    args = ap.parse_args()

    if args.once:
        asyncio.run(main())
    else:
        print(f"Starting burst_report loop, interval={args.interval}s")
        while True:
            try:
                asyncio.run(main())
            except Exception as e:
                print(f"burst_report error: {e}")
            time.sleep(args.interval)
