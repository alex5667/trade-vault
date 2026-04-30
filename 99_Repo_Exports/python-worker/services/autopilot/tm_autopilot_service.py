# -*- coding: utf-8 -*-
"""
tm_autopilot_service:
  - hourly/daily export of closed trades
  - run policy tuner
  - send Telegram report
  - optional: write auto-proposal to cfg:suggestions:* with apply_kind=overrides_v1

This service is designed to run INSIDE a container (no systemd).
It uses a Redis SETNX lock to prevent duplicate runs.
"""
from __future__ import annotations
from utils.time_utils import get_ny_time_millis
import asyncio
import hashlib
import json
import os
import time
from typing import Any, Dict, List, Tuple

import redis.asyncio as aioredis
import redis

from tools.send_telegram import send_telegram
from tools.export_trade_closed_ndjson import iter_closed_from_redis
from core.entry_policy_overrides_v1 import EntryPolicyOverridesV1

LOCK_KEY = os.getenv("AUTOPILOT_LOCK_KEY", "lock:autopilot:tm_policy")
LOCK_TTL_SEC = int(os.getenv("AUTOPILOT_LOCK_TTL_SEC", "3300"))  # 55m

TRADE_EVENTS_STREAM = os.getenv("TRADE_EVENTS_STREAM", "events:trades")
REDIS_URL = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")

SUG_META_PREFIX = os.getenv("SUG_META_PREFIX", "cfg:suggestions:entry_policy:meta")
SUG_APPROVALS_PREFIX = os.getenv("SUG_APPROVALS_PREFIX", "cfg:suggestions:entry_policy:approvals")
SUG_APPLIED_PREFIX = os.getenv("SUG_APPLIED_PREFIX", "cfg:suggestions:entry_policy:applied")
SUG_LATEST_PREFIX = os.getenv("SUG_LATEST_PREFIX", "cfg:suggestions:entry_policy:latest:overrides_v1")

def _now_ms() -> int:
    return get_ny_time_millis()

def _sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()

async def _try_lock(r: "aioredis.Redis") -> bool:
    try:
        # value = ts for debugging
        return bool(await r.set(LOCK_KEY, str(_now_ms()), nx=True, ex=LOCK_TTL_SEC))
    except Exception:
        return False

def _regime_group(rg: str) -> str:
    rg = (rg or "na").lower()
    return "thin" if rg in ("thin", "news", "illiquid") else "default"

def _winner_to_override(
    *, symbol: str, regime: str, scenario: str, winner: str, updated_ts_ms: int
) -> EntryPolicyOverridesV1:
    """
    Auto-proposal: encode winner as force_active_arm for that scope.
    apply-kind=overrides_v1 -> EntryPolicyApplyRunner writes overrides key.
    """
    return EntryPolicyOverridesV1(
        updated_ts_ms=int(updated_ts_ms)
        enabled=1
        symbol=str(symbol).upper()
        regime=str(regime).lower()
        scenario=str(scenario).lower()
        group=_regime_group(regime)
        force_active_arm=str(winner).upper()
        freeze_active=0
        ab_split_b=int(os.getenv("AUTOPILOT_AB_SPLIT_B", "10"))
        ab_split_c=int(os.getenv("AUTOPILOT_AB_SPLIT_C", "10"))
        ab_salt=str(os.getenv("AUTOPILOT_AB_SALT", "v1"))
    )

async def run_once() -> None:
    r = aioredis.from_url(REDIS_URL, decode_responses=True)
    if not await _try_lock(r):
        await r.close()
        return
    now = _now_ms()
    since_h = float(os.getenv("AUTOPILOT_SINCE_HOURS", "168"))
    since_ms = now - int(since_h * 3600.0 * 1000.0)

    # 1) Read closed trades into memory (7d should be OK given stream maxlen)
    rows: List[Dict[str, Any]] = []
    # Use sync redis for XRANGE iteration (simple + reliable)
    rs = redis.from_url(REDIS_URL, decode_responses=True)
    for row in iter_closed_from_redis(r=rs, stream=TRADE_EVENTS_STREAM, since_ms=since_ms, limit=int(os.getenv("AUTOPILOT_LIMIT", "200000"))):
        rows.append(row)

    # 2) Compute winners via simple LCB logic using tm_policy_tuner core (inline, dependency-free)
    # Group by (symbol, regime, scenario) and pick best arm by mean R (LCB approximation).
    bucket: Dict[Tuple[str, str, str], Dict[str, List[float]]] = {}
    for rr in rows:
        sym = str(rr.get("symbol", "")).upper()
        rg = str(rr.get("regime", "na")).lower()
        scn = str(rr.get("scenario", "")).lower()
        arm = str(rr.get("ab_arm", "A")).upper()
        if scn not in ("continuation", "reversal"):
            continue
        if arm not in ("A", "B", "C"):
            arm = "A"
        r_mult = float(rr.get("r_mult", rr.get("r_multiple", 0.0)) or 0.0)
        bucket.setdefault((sym, rg, scn), {}).setdefault(arm, []).append(r_mult)

    # z per regime
    def z_rg(rg: str) -> float:
        return 1.96 if rg in ("thin", "news", "illiquid") else 1.645
    def stat(xs: List[float], z: float) -> Tuple[int, float, float]:
        n = len(xs)
        if n <= 1:
            m = xs[0] if n == 1 else 0.0
            return n, m, m
        m = sum(xs)/n
        var = sum((x-m)**2 for x in xs)/max(1, n-1)
        std = (var**0.5) if var > 0 else 0.0
        se = std / (n**0.5) if std > 0 else 0.0
        lcb = m - z*se
        return n, m, lcb

    min_n = int(os.getenv("AUTOPILOT_MIN_N", "30"))
    min_edge = float(os.getenv("AUTOPILOT_MIN_EDGE_R", "0.05"))

    recs: List[Dict[str, Any]] = []
    for (sym, rg, scn), by_arm in sorted(bucket.items()):
        z = z_rg(rg)
        A = stat(by_arm.get("A", []), z)
        B = stat(by_arm.get("B", []), z)
        C = stat(by_arm.get("C", []), z)
        # pick by LCB with constraints
        win = "A"
        win_lcb = A[2]
        for arm, st in (("B", B), ("C", C)):
            if st[0] >= min_n and st[2] > win_lcb:
                win = arm
                win_lcb = st[2]
        # require edge over A
        if win != "A" and win_lcb < (A[2] + min_edge):
            win = "A"
        recs.append({
            "symbol": sym, "regime": rg, "scenario": scn
            "winner_arm": win
            "A": {"n": A[0], "mean": A[1], "lcb": A[2]}
            "B": {"n": B[0], "mean": B[1], "lcb": B[2]}
            "C": {"n": C[0], "mean": C[1], "lcb": C[2]}
        })

    # 3) Telegram report (summary)
    top = recs[:20]
    msg_lines = []
    if top:
        msg_lines.append(f"*Autopilot report* (since_hours={since_h:.0f}, n_ctx={len(recs)})")
        msg_lines.append("")
        for x in top:
            msg_lines.append(f"- `{x['symbol']}` `{x['regime']}` `{x['scenario']}` → *{x['winner_arm']}* | "
                             f"A.lcb={x['A']['lcb']:.2f} B.lcb={x['B']['lcb']:.2f} C.lcb={x['C']['lcb']:.2f}")
        send_telegram("\n".join(msg_lines), parse_mode="Markdown")

    # 4) Auto-proposal (apply_kind=overrides_v1)
    if int(os.getenv("AUTOPILOT_PROPOSE_OVERRIDES", "1")) == 1:
        for x in recs:
            ovr = _winner_to_override(
                symbol=x["symbol"], regime=x["regime"], scenario=x["scenario"]
                winner=x["winner_arm"], updated_ts_ms=now
            )
            ok, _ = ovr.validate()
            if not ok:
                continue
            sid = _sha1(json.dumps({"k": "overrides_v1", "sym": ovr.symbol, "rg": ovr.regime, "scn": ovr.scenario, "grp": ovr.group, "ts": now}, separators=(",", ":")))
            meta = {
                "kind": "overrides_v1"
                "apply_kind": "overrides_v1"
                "sid": sid
                "symbol": ovr.symbol
                "regime": ovr.regime
                "scenario": ovr.scenario
                "group": ovr.group
                "overrides": json.loads(ovr.to_json())
                "stats": x
                "updated_ts_ms": now
            }
            meta_key = f"{SUG_META_PREFIX}:{sid}"
            latest_key = f"{SUG_LATEST_PREFIX}:{ovr.symbol}:{ovr.regime}:{ovr.group}:{ovr.scenario}"
            # write meta + latest pointer (no approvals here)
            try:
                pipe = r.pipeline()
                pipe.set(meta_key, json.dumps(meta, ensure_ascii=False, separators=(",", ":")))
                pipe.set(latest_key, sid)
                await pipe.execute()
            except Exception:
                continue

    await r.close()

async def run_forever() -> None:
    # Hourly cadence by default; jitter avoids herd.
    every_sec = int(os.getenv("AUTOPILOT_EVERY_SEC", "3600"))
    jitter_ms = int(os.getenv("AUTOPILOT_JITTER_MS", "15000"))
    while True:
        try:
            await run_once()
        except Exception:
            pass
        # sleep with jitter
        await asyncio.sleep(max(10, every_sec) + (jitter_ms / 1000.0))

if __name__ == "__main__":
    asyncio.run(run_forever())
