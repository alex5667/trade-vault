import json
import numpy as np
path = '/var/lib/trade/ml_models/edge_stack_v1/runs/20260331_033638/edge_train.jsonl'
w_win, w_loss = [], []
for line in open(path):
    if not line.strip(): continue
    r = json.loads(line)
    y = int(r.get('y_closed') or r.get('y', 0))
    val = r.get('r_mult')
    val = 0.1 if val is None else abs(float(val))
    weight = np.clip(val, 0.1, 10.0)
    if y == 1:
        w_win.append(weight)
    else:
        w_loss.append(weight)

print(f"Wins average weight: {np.mean(w_win):.4f} (n={len(w_win)})")
print(f"Losses average weight: {np.mean(w_loss):.4f} (n={len(w_loss)})")
