import numpy as np
import json
import joblib

path = '/var/lib/trade/ml_models/edge_stack_v1/champions/edge_stack_v1_champion.joblib'
obj = joblib.load(path)
train_path = path.replace('champions/edge_stack_v1_champion.joblib', 'runs/20260331_033306/edge_train.jsonl')

y_z = []
w_z = []
for line in open(train_path):
    if not line.strip(): continue
    r = json.loads(line)
    y_z.append(int(r.get('y_closed') or r.get('y', 0)))
    val = r.get('r_mult')
    val = 0.1 if val is None else abs(float(val))
    w_z.append(np.clip(val, 0.1, 10.0))

y_z = np.array(y_z)
w_z = np.array(w_z)

# The report contains the OOF logloss explicitly!
rep = obj['report']['oof']
print("OOF LR LogLoss (unweighted inside report):", rep.get('lr', {}).get('logloss'))
print("OOF GBDT LogLoss (unweighted inside report):", rep.get('gbdt', {}).get('logloss'))

from sklearn.metrics import log_loss
# Evaluate weighted manually
# Since we don't have the explicit OOF predictions Z here, we must parse them.
# Wait, obj['report'] only has unweighted log loss!
# For LR, it was 3.91 unweighted logloss! For GBDT it was 5.52.
print("Meta weights:", obj['meta'].coef_, obj['meta'].intercept_)
