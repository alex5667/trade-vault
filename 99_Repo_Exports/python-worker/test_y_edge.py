import json, sys
wins = 0
losses = 0
y_edge_wins = 0
y_edge_losses = 0

try:
    with open('/var/lib/trade/training/latest_outcomes.ndjson', 'r') as f:
        outcomes = {}
        for line in f:
            if line.strip():
                d = json.loads(line)
                outcomes[d['sid']] = d
    print(f"Loaded {len(outcomes)} outcomes")
except Exception as e:
    print(e)
    sys.exit(1)
    
with open('/var/lib/trade/training/latest_confirm_train_v7.ndjson', 'r') as f:
    for line in f:
        if not line.strip(): continue
        d = json.loads(line)
        sid = d.get('sid')
        # Check rule or direct
        y_edge = d.get('y_edge', d.get('y_edge_60000', 0))
        if y_edge == 0 and 'outcomes' in d: # fallbacks
            y_edge = d['outcomes'].get('y_edge_60000', 0)
        
        if sid in outcomes:
            pnl = outcomes[sid].get('pnl', 0.0)
            if pnl > 0:
                wins += 1
                if y_edge == 1: y_edge_wins += 1
            elif pnl < 0:
                losses += 1
                if y_edge == 1: y_edge_losses += 1

print(f"Wins: {wins}, y_edge=1 in wins: {y_edge_wins}")
print(f"Losses: {losses}, y_edge=1 in losses: {y_edge_losses}")
