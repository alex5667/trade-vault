import redis
import json

def main():
    r = redis.from_url('redis://127.0.0.1:63791/0', decode_responses=True)
    open_ids = r.smembers("orders:open")
    if not open_ids:
        print("Нет открытых позиций.")
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
            
        p_data["id"] = pid
        positions.append((entry_ts, p_data))
        
    positions.sort(key=lambda x: x[0], reverse=True)

    for ts, pos in positions[:1]:  # Just one
        print("Order PID:", pos.get("id"))
        print("DIR:", pos.get("direction"))
        print("ENTRY:", pos.get("entry_price"))
        print("SL:", pos.get("sl"))
        print("TP1:", pos.get("tp1"))
        
        sp_str = pos.get("signal_payload", "{}")
        try:
            sp = json.loads(sp_str)
            print("ATR:", sp.get("atr"))
            # Print ALL indicators
            inds = sp.get("indicators", {})
            for k in sorted(inds.keys()):
                if "atr" in k or "mult" in k or "sl" in k or "tp" in k:
                    print(f"  {k} = {inds[k]}")
                    
        except Exception as e:
            print("Error parsing signal_payload:", e)

if __name__ == '__main__':
    main()
