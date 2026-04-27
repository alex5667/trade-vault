import os, json, time, hashlib, asyncio
import redis.asyncio as aioredis

REPORT_KEY = os.getenv("BURST_REPORT_KEY_JSON", "reports:burst:last")
META_PREFIX = "cfg:suggestions:entry_policy:meta"
APPROVALS_PREFIX = "cfg:suggestions:entry_policy:approvals"
APPLIED_PREFIX = "cfg:suggestions:entry_policy:applied"
LATEST_PREFIX = "cfg:suggestions:entry_policy:latest:cooldown_policy"

def _now_ms() -> int:
    return int(time.time() * 1000)

def _sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()

def _clamp(x: float, lo: float, hi: float) -> float:
    return lo if x < lo else hi if x > hi else x

def propose_from_group(cur: dict, *, blocked: int, replaced: int, emit_pending: int, emit_current: int,
                       p90_pressure: float, p90_spread: float, regime: str, scenario: str) -> dict:
    """
    Heuristic tuner (safe-first):
      - if blocked high in thin/news or wide spread => increase cooldown 10-25%
      - if blocked almost zero and pressure low => decrease cooldown 10% (bounded)
      - pressure_hi_sps set near 0.8*p90_pressure (bounded)
    """
    cd_rev = float(cur.get("cooldown_reversal_sec", 30))
    cd_con = float(cur.get("cooldown_continuation_sec", 15))
    # targets (per 20m report)
    blocked_hi = 40  # per 20m
    blocked_lo = 2

    is_thin = regime in ("thin","news","illiquid")
    wide = (p90_spread >= 18.0) if not is_thin else (p90_spread >= 25.0)

    bump = 1.0
    if blocked >= blocked_hi or is_thin or wide:
        bump = 1.15 if scenario == "continuation" else 1.20
        if is_thin or wide:
            bump = 1.25
    elif blocked <= blocked_lo and p90_pressure <= 0.05:
        bump = 0.90

    if scenario == "reversal":
        cd_rev = _clamp(cd_rev * bump, 8.0, 180.0)
    else:
        cd_con = _clamp(cd_con * bump, 5.0, 120.0)

    # pressure_hi threshold suggestion: 0.8*p90, bounded
    p_hi = float(cur.get("pressure_hi_sps", 0.12))
    p_sug = _clamp(0.8 * float(p90_pressure or 0.0), 0.06, 0.30)
    # don’t churn too much: change only if delta >= 15%
    if p_hi > 0 and abs(p_sug - p_hi) / p_hi < 0.15:
        p_sug = p_hi

    out = dict(cur)
    out["cooldown_reversal_sec"] = float(cd_rev)
    out["cooldown_continuation_sec"] = float(cd_con)
    out["pressure_hi_sps"] = float(p_sug)
    return out

async def main() -> None:
    redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
    r = aioredis.from_url(redis_url, decode_responses=True, socket_connect_timeout=10, socket_timeout=30, max_connections=50)

    raw = await r.get(REPORT_KEY)
    if not raw:
        await r.close()
        return
    rep = json.loads(raw)
    groups = rep.get("groups", {}) or {}
    now = _now_ms()

    # current defaults (global)
    cur_defaults = {
        "cooldown_reversal_sec": float(os.getenv("COOLDOWN_REV_DEFAULT", "30")),
        "cooldown_continuation_sec": float(os.getenv("COOLDOWN_CON_DEFAULT", "15")),
        "pressure_hi_sps": float(os.getenv("PRESSURE_HI_DEFAULT", "0.12")),
    }

    for k, g in groups.items():
        try:
            sym, regime, scenario = k.split("|", 2)
        except Exception:
            continue
        blocked = int(g.get("blocked", 0) or 0)
        replaced = int(g.get("replaced", 0) or 0)
        emit_pending = int(g.get("emit_pending", 0) or 0)
        emit_current = int(g.get("emit_current", 0) or 0)
        p90_pressure = float(g.get("p90_pressure_sps", 0.0) or 0.0)
        p90_spread = float(g.get("p90_spread_bp", 0.0) or 0.0)

        proposed = propose_from_group(
            cur_defaults,
            blocked=blocked, replaced=replaced,
            emit_pending=emit_pending, emit_current=emit_current,
            p90_pressure=p90_pressure, p90_spread=p90_spread,
            regime=str(regime).lower(), scenario=str(scenario).lower(),
        )

        meta = {
            "kind": "cooldown_policy_v1",
            "ts_ms": now,
            "symbol": str(sym).upper(),
            "regime": str(regime).lower(),
            "scenario": str(scenario).lower(),
            "input_report_ts_ms": int(rep.get("ts_ms", 0) or 0),
            "stats": {
                "blocked": blocked,
                "replaced": replaced,
                "emit_pending": emit_pending,
                "emit_current": emit_current,
                "p90_pressure_sps": p90_pressure,
                "p90_spread_bp": p90_spread,
            },
            "proposed": proposed,
            "apply": {
                "override_key": f"cfg:crypto_of:overrides:{str(sym).upper()}",
                "allow_keys": list(proposed.keys()),
            },
            "rationale": "Auto-tune cooldown/pressure from burst report; safe-first; bounded; requires 2-man approval.",
        }

        sid = _sha1(json.dumps(meta, sort_keys=True, separators=(",", ":")))
        latest_key = f"{LATEST_PREFIX}:{str(sym).upper()}:{str(regime).lower()}:{str(scenario).lower()}"

        try:
            prev = await r.get(latest_key)
            if str(prev or "") == sid:
                continue
        except Exception:
            pass

        await r.set(f"{META_PREFIX}:{sid}", json.dumps(meta, ensure_ascii=False, separators=(",", ":")), ex=7*24*3600)
        await r.set(latest_key, sid, ex=7*24*3600)
        await r.delete(f"{APPROVALS_PREFIX}:{sid}")
        await r.delete(f"{APPLIED_PREFIX}:{sid}")

    await r.close()

if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--interval", type=int, default=int(os.getenv("COOLDOWN_POLICY_INTERVAL_SEC", "900")))
    ap.add_argument("--once", action="store_true")
    args = ap.parse_args()

    if args.once:
        asyncio.run(main())
    else:
        print(f"Starting cooldown_policy_suggester loop, interval={args.interval}s")
        while True:
            try:
                asyncio.run(main())
            except Exception as e:
                print(f"cooldown_policy_suggester error: {e}")
            time.sleep(args.interval)
