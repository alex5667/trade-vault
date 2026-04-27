from __future__ import annotations

import asyncio
import json
import os
import sys
from collections import Counter
from typing import Dict, Any

import redis.asyncio as aioredis


async def main() -> None:
    # 1. Load Replay Results
    replay_file = os.getenv("REPLAY_FILE", "entry_policy_replay.ndjson")
    if not os.path.exists(replay_file):
        print(f"Error: Replay file {replay_file} not found.")
        sys.exit(1)

    replay_map: Dict[str, Dict[str, Any]] = {}  # msg_id -> result
    print(f"Loading replay results from {replay_file}...")
    with open(replay_file, "r") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rec = json.loads(line)
            mid = rec.get("msg_id")
            if mid:
                replay_map[mid] = rec

    print(f"Loaded {len(replay_map)} replay records.")

    # 2. Fetch Audit Stream from Redis
    redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
    audit_stream = os.getenv("TRADE_ENTRY_AUDIT_STREAM", "stream:trade:entry_audit")
    
    r = aioredis.from_url(redis_url, decode_responses=True)
    
    print(f"Fetching audit stream {audit_stream}...")
    # Read last N items or iterate? For verification, we assume specific time window.
    # We'll just read reasonably many last items to find matches.
    # A robust tool would allow time-range query, but XREVRANGE is good enough for recent check.
    
    audits = await r.xrevrange(audit_stream, count=5000)
    
    matches = 0
    mismatches = 0
    missing_in_audit = 0
    
    print(f"Fetched {len(audits)} audit records. Comparing...")
    
    for msg_id, fields in audits:
        payload_raw = fields.get("payload", "{}")
        try:
            payload = json.loads(payload_raw)
        except Exception:
            continue
            
        # Audit doesn't link back to candidate msg_id directly in payload unless we added it.
        # But wait, replay uses candidate msg_id. capture uses candidate msg_id.
        # The service audit does NOT explicitly log the candidate's Redis ID in the payload 
        # (it logs ts_ms, symbol, etc).
        
        # Heuristic match: Symbol + TS_MS. 
        # (Assuming low collision in 1ms for same symbol).
        
        sym = payload.get("symbol")
        ts = str(payload.get("ts_ms"))
        key = f"{sym}:{ts}"
        
        # We need to build a lookup for replay by key too
        # Redo replay map
        pass

    # Re-indexing replay by (Symbol, TS)
    replay_by_key = {}
    for mid, rec in replay_map.items():
        # Replay record doesn't have original TS in top level?
        # Replay record: {msg_id, symbol, regime, ok, reason_code, notes}
        # We need the original candidate TS. 
        # Ah, 'replay_entry_policy.py' output is minimal.
        
        # To verify accurately, we might need to modify replay tool to include ts_ms 
        # OR rely on capture input.
        
        # Let's assume user runs replay output and we have access to inputs too? 
        # Or simpler: The user wants to compare "Allow/Deny".
        
        # Let's use the 'msg_id' of the AUDIT message vs the REPLAY? 
        # No, they are different streams.
        
        # Plan B: Match by content hash?
        # EntryPolicyService computes a hash:
        # hash = _sha1(json.dumps({"ok": ..., "reason": ..., "sym": ..., "z": ..., "side": ...}))
        # We could compute this hash in replay validation?
        
        pass

    # Actually, simpler approach for SRE verify:
    # Just check aggregate stats or exact Key=(Symbol, SetupTS, Zone) match.
    
    # We will iterate Audit stream. For each audit:
    #   Find corresponding Replay result (we need to match them).
    #   Ideally we match by (Symbol, Bundle, Zone, TS).
    
    # Let's rebuild replay map with a Composite Key.
    # But replay output (from previous step) lacks detailed fields like 'bundle' or 'ts'.
    # It only has: msg_id (candidate msg id), symbol, regime, ok, reason_code.
    
    # Does Audit payload contain candidate msg_id? No.
    # Does Capture contain candidate msg_id? Yes.
    
    # So: Audit -> (Symbol, TS) <- Candidate (Source of Truth) -> Capture -> Replay.
    # We can link Audit to Candidate via Symbol+TS.
    
    audit_map = {}
    for mid, fields in audits:
        p = json.loads(fields.get("payload", "{}"))
        s = p.get("symbol")
        t = str(p.get("ts_ms"))
        k = f"{s}:{t}"
        audit_map[k] = p

    # Iterate Replay (which corresponds to Capture inputs)
    # Replay output has msg_id. We need to look up input to get TS? 
    # Or strict match?
    
    # WAIT. The user instruction said: "Сравнить allow/deny на replay с audit".
    # This implies we can match them.
    # If we run Capture, we get NDJSON with "cand": {"ts_ms": ...}.
    # We can read the INPUT file to get the keys, and REPLAY file to get the decisions.
    
    input_file = os.getenv("INPUT_FILE", "entry_policy_inputs.ndjson")
    if not os.path.exists(input_file):
        print(f"Warning: Input file {input_file} not found. Cannot match by TS. Exiting.")
        return

    print(f"Loading inputs from {input_file} to map IDs to TS...")
    id_to_key = {}
    with open(input_file, "r") as f:
        for line in f:
            if not line.strip(): continue
            rec = json.loads(line)
            mid = rec.get("msg_id")
            cand = rec.get("cand", {})
            sym = cand.get("symbol")
            ts = str(cand.get("ts_ms"))
            key = f"{sym}:{ts}"
            id_to_key[mid] = key

    # Now compare
    print("Comparing Replay Decisions vs Live Audits...")
    
    for rid, r_res in replay_map.items():
        key = id_to_key.get(rid)
        if not key:
            continue # Can't link
            
        audit = audit_map.get(key)
        if not audit:
            missing_in_audit += 1
            continue
            
        matches += 1
        
        # Compare
        replay_ok = r_res["ok"]
        replay_code = r_res["reason_code"]
        
        audit_ok = audit["ok"]
        audit_code = audit["reason_code"]
        
        # In Shadow Mode, audit uses "ALLOW_SHADOW" for OK.
        # Replay (pure) updates "ALLOW".
        # So: Replay=ALLOW <-> Audit=ALLOW or ALLOW_SHADOW.
        
        a_is_allow = (audit_code in ("ALLOW", "ALLOW_SHADOW"))
        r_is_allow = (replay_code == "ALLOW")
        
        consistent = True
        if a_is_allow != r_is_allow:
            consistent = False
        elif not a_is_allow and (replay_code != audit_code):
             # If both deny, codes should match (ideally)
             # But minor diffs (e.g. order of checks) might exist if code diverged?
             # They share core now, so should match.
             consistent = False
             
        if not consistent:
            mismatches += 1
            print(f"MISMATCH [{key}]: Replay={replay_code} vs Audit={audit_code}")
            
    print("-" * 40)
    print(f"Total Replayed: {len(replay_map)}")
    print(f"Matched with Audit: {matches}")
    print(f"Missing in Audit: {missing_in_audit} (Traffic gap or lag?)")
    print(f"Mismatches: {mismatches}")
    
    if mismatches == 0 and matches > 0:
        print("SUCCESS: Logic is consistent.")
    elif matches == 0:
        print("WARNING: No matches found. Check streams/timestamps.")
    else:
        print("FAILURE: Discrepancies detected.")

    await r.close()

if __name__ == "__main__":
    asyncio.run(main())
