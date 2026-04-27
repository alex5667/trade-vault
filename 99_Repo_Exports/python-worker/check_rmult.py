import json
import numpy as np
path = '/var/lib/trade/ml_models/edge_stack_v1/runs/20260331_033638/edge_train.jsonl'
r_buy_win, r_buy_loss = [], []
r_sell_win, r_sell_loss = [], []

def _get_direction(r):
    ind = r.get('indicators', {})
    k = str(ind.get('meta_enforce_key') or r.get('key', ''))
    if '|LONG|' in k: return 'BUY'
    if '|SHORT|' in k: return 'SELL'
    direction = str(r.get('direction', ''))
    if direction: return direction
    return ''

for line in open(path):
    if not line.strip(): continue
    r = json.loads(line)
    y = int(r.get('y_closed') or r.get('y', 0))
    val = r.get('r_mult')
    val = 0.1 if val is None else float(val)
    d = _get_direction(r)
    
    if d == 'BUY':
        if y == 1: r_buy_win.append(val)
        else: r_buy_loss.append(val)
    elif d == 'SELL':
        if y == 1: r_sell_win.append(val)
        else: r_sell_loss.append(val)

print(f"BUY Wins avg r_mult: {np.mean(r_buy_win):.4f} (n={len(r_buy_win)})")
print(f"BUY Losses avg r_mult: {np.mean(r_buy_loss):.4f} (n={len(r_buy_loss)})")
print(f"SELL Wins avg r_mult: {np.mean(r_sell_win):.4f} (n={len(r_sell_win)})")
print(f"SELL Losses avg r_mult: {np.mean(r_sell_loss):.4f} (n={len(r_sell_loss)})")
