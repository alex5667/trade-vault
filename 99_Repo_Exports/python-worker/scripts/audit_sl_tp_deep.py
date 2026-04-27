#!/usr/bin/env python3
"""
Deep SL/TP audit: checks all open positions for calculation correctness.
Facts / Assumptions / Risks output.
"""
import redis
import json
import sys

def main():
    r = redis.from_url('redis://127.0.0.1:63791/0', decode_responses=True)
    open_ids = r.smembers("orders:open")
    if not open_ids:
        print("No open positions found.")
        return

    positions = []
    for pid in open_ids:
        p_data = r.hgetall(f"order:{pid}")
        if not p_data:
            continue
        try:
            entry_ts = int(p_data.get("entry_ts_ms", 0))
        except ValueError:
            entry_ts = 0
        p_data["_pid"] = pid
        positions.append((entry_ts, p_data))

    positions.sort(key=lambda x: x[0], reverse=True)
    
    print(f"=== DEEP SL/TP AUDIT ({len(positions)} positions) ===\n")
    
    issues = []
    
    for i, (ts, pos) in enumerate(positions[:20]):
        pid = pos["_pid"]
        direction = str(pos.get("direction", "LONG")).upper()
        try:
            entry = float(pos.get("entry_price", 0))
            sl = float(pos.get("sl", 0))
        except (ValueError, TypeError):
            issues.append(f"POS {pid}: Cannot parse entry/sl")
            continue
        
        if entry <= 0:
            issues.append(f"POS {pid}: entry_price = 0")
            continue
            
        # Parse tp_levels
        tp_levels = []
        try:
            tp_raw = pos.get("tp_levels")
            if tp_raw:
                tp_levels = json.loads(tp_raw)
        except Exception:
            pass
        if not tp_levels:
            for k in ("tp1", "tp2", "tp3"):
                v = pos.get(k)
                if v:
                    try:
                        tp_levels.append(float(v))
                    except:
                        pass
        
        # Parse signal_payload for ATR
        sp = {}
        try:
            sp = json.loads(pos.get("signal_payload", "{}"))
        except:
            pass
        
        atr = float(sp.get("atr", 0))
        indicators = sp.get("indicators", {})
        
        # Compute distances
        sl_dist = abs(entry - sl)
        sl_bps = (sl_dist / entry) * 10000 if entry > 0 else 0
        sl_atr = sl_dist / atr if atr > 0 else 0
        
        # Check SL direction
        sl_correct_dir = True
        if direction == "LONG" and sl >= entry:
            sl_correct_dir = False
            issues.append(f"❌ POS {pid} ({pos.get('symbol')}): LONG but SL ({sl}) >= entry ({entry})")
        if direction == "SHORT" and sl <= entry:
            sl_correct_dir = False
            issues.append(f"❌ POS {pid} ({pos.get('symbol')}): SHORT but SL ({sl}) <= entry ({entry})")
        
        # Check TP direction
        for j, tp in enumerate(tp_levels):
            tp_val = float(tp)
            if direction == "LONG" and tp_val <= entry:
                issues.append(f"❌ POS {pid} ({pos.get('symbol')}): LONG but TP{j+1} ({tp_val}) <= entry ({entry})")
            if direction == "SHORT" and tp_val >= entry:
                issues.append(f"❌ POS {pid} ({pos.get('symbol')}): SHORT but TP{j+1} ({tp_val}) >= entry ({entry})")
        
        # Check if SL is too tight (< 0.5 ATR)
        if atr > 0 and sl_atr < 0.5:
            issues.append(f"⚠️ POS {pid} ({pos.get('symbol')}): SL too tight: {sl_atr:.2f} ATR (< 0.5x floor)")
        
        # Check TP1 profitability
        tp1_val = float(tp_levels[0]) if tp_levels else 0
        tp1_dist = abs(tp1_val - entry) if tp1_val else 0
        tp1_bps = (tp1_dist / entry) * 10000 if entry > 0 else 0
        tp1_atr = tp1_dist / atr if atr > 0 else 0
        
        if tp1_bps < 8:  # Less than 8bps = likely unprofitable after fees
            issues.append(f"⚠️ POS {pid} ({pos.get('symbol')}): TP1 too tight: {tp1_bps:.1f} bps (< 8bps fee floor)")
            
        # Check R:R
        rr = tp1_dist / sl_dist if sl_dist > 0 else 0
        
        # Check indicators for flooring
        sl_floored = int(indicators.get("sl_atr_mult_floored", 0))
        sl_orig = float(indicators.get("sl_atr_mult_original", 0))
        atr_bad = int(indicators.get("atr_bad", 0))
        
        symbol = pos.get("symbol", "?")
        virtual = pos.get("is_virtual", "0")
        
        print(f"[{i+1}] {symbol} {direction} {'(VIRTUAL)' if virtual == '1' else ''}")
        print(f"    entry={entry:.6f}  sl={sl:.6f}  tp1={tp1_val:.6f}")
        print(f"    SL: {sl_bps:.1f} bps / {sl_atr:.2f} ATR  |  TP1: {tp1_bps:.1f} bps / {tp1_atr:.2f} ATR  |  R:R = {rr:.2f}")
        print(f"    ATR={atr:.6f} (bad={atr_bad})  SL_floored={sl_floored}", end="")
        if sl_floored:
            print(f" (original={sl_orig:.4f})", end="")
        print(f"  SL_dir_ok={sl_correct_dir}")
        print()

    # Summary
    print("\n=== SUMMARY ===")
    if not issues:
        print("✅ No issues found across all audited positions.")
    else:
        print(f"⚠️ Found {len(issues)} issues:")
        for issue in issues:
            print(f"  {issue}")
    
    # Key metrics
    sl_atrs = []
    tp1_atrs = []
    for _, pos in positions:
        try:
            sp = json.loads(pos.get("signal_payload", "{}"))
            inds = sp.get("indicators", {})
            v_sl = float(inds.get("sl_atr", 0))
            v_tp1 = float(inds.get("tp1_atr", 0))
            if v_sl > 0:
                sl_atrs.append(v_sl)
            if v_tp1 > 0:
                tp1_atrs.append(v_tp1)
        except:
            pass
    
    if sl_atrs:
        import statistics
        print(f"\n📊 SL ATR distribution (n={len(sl_atrs)}):")
        print(f"   min={min(sl_atrs):.3f}  median={statistics.median(sl_atrs):.3f}  max={max(sl_atrs):.3f}  mean={statistics.mean(sl_atrs):.3f}")
    if tp1_atrs:
        import statistics
        print(f"📊 TP1 ATR distribution (n={len(tp1_atrs)}):")
        print(f"   min={min(tp1_atrs):.3f}  median={statistics.median(tp1_atrs):.3f}  max={max(tp1_atrs):.3f}  mean={statistics.mean(tp1_atrs):.3f}")

if __name__ == '__main__':
    main()
