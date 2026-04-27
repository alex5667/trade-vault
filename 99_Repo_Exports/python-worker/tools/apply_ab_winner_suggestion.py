from utils.time_utils import get_ny_time_millis

import asyncio
import json
import os
import sys
import time
import argparse
import redis.asyncio as aioredis

# This tool acts as the "ApplyRunner" for AB Winner keys.
# It can read "latest" suggestion for a bucket, OR apply a specific SID.

async def apply_one_sid(r, sid: str) -> None:
    raw = await r.get(f"cfg:suggestions:entry_policy:meta:{sid}")
    if not raw:
        print(f"Meta not found for sid={sid}")
        return
    
    try:
        meta = json.loads(raw)
    except:
        return

    sym = str(meta.get("symbol") or "").upper()
    rg = str(meta.get("regime") or "na").lower()
    grp = str(meta.get("group") or "default").lower()
    win = str(meta.get("winner_arm") or meta.get("winner") or "").upper()
    scn = str(meta.get("scenario") or "").lower()

    if not sym:
        print("Missing symbol in meta")
        return
    if win not in ("A", "B", "C"):
        print(f"Invalid winner '{win}'")
        return

    print(f"Applying SID={sid} Sym={sym} Regime={rg} Grp={grp} Scn={scn} Win={win}")

    # base key
    k_base = f"cfg:entry_policy:active_arm:{sym}:{rg}:{grp}"
    await r.set(k_base, win, ex=14*24*3600)
    print(f"  -> Set {k_base} = {win}")

    # scenario key
    if scn in ("continuation", "reversal"):
        k_scn = f"cfg:entry_policy:active_arm:{sym}:{rg}:{grp}:{scn}"
        await r.set(k_scn, win, ex=14*24*3600)
        print(f"  -> Set {k_scn} = {win}")

    # Mark applied
    now_ms = get_ny_time_millis()
    await r.set(f"cfg:suggestions:entry_policy:applied:{sid}", str(now_ms), ex=30*24*3600)


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--sid", help="Apply specific suggestion SID")
    parser.add_argument("--all-latest", action="store_true", help="Apply all latest suggestions from registry")
    args = parser.parse_args()

    r = aioredis.from_url(os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0"), decode_responses=True)

    if args.sid:
        await apply_one_sid(r, args.sid)
    
    if args.all_latest:
        # Scan all latest suggestions?
        # Since we don't have a reliable registry of ALL active suggestions easily accessible 
        # without scanning keys or using the agg registry from suggester,
        # we can use the suggester's bucket registry.
        reg_key = "ab:agg:registry:v1"
        try:
            members = await r.smembers(reg_key)
            for m in members:
                # sym|rg|grp|scn
                try:
                    sym, rg, gr, scn = str(m).split("|", 3)
                    latest_key = f"cfg:suggestions:entry_policy:latest:ab_winner:{sym}:{rg}:{gr}"
                    # or scn key
                    # Let's try to get latest from base bucket + scenario
                    # Wait, if we use the bucket `scn`, we should check `latest_scn`?
                    # The suggester writes `latest_scn_key`.
                    l_scn_key = f"cfg:suggestions:entry_policy:latest:ab_winner_scn:{sym}:{rg}:{gr}:{scn}"
                    sid = await r.get(l_scn_key)
                    if sid:
                        await apply_one_sid(r, sid)
                except:
                    pass
        except Exception as e:
            print(f"Error scanning latest: {e}")

    await r.close()

if __name__ == "__main__":
    asyncio.run(main())
