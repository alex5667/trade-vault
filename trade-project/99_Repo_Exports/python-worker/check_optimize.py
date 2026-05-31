import numpy as np
import json
import joblib
from scipy.optimize import minimize
from sklearn.metrics import log_loss

path = '/var/lib/trade/ml_models/edge_stack_v1/champions/edge_stack_v1_champion.joblib'
obj = joblib.load(path)
train_path = path.replace('champions/edge_stack_v1_champion.joblib', 'runs/20260331_033306/edge_train.jsonl')

y_z, w_z, p_lr, p_gbdt = [], [], [], []
from ml_analysis.tools.train_edge_stack_v1_oof import build_feature_row  # type: ignore

def _get_direction(r):
    ind = r.get('indicators', {})
    k = str(ind.get('meta_enforce_key') or r.get('key', ''))
    if '|LONG|' in k: return 'BUY'
    if '|SHORT|' in k: return 'SELL'
    direction = str(r.get('direction', ''))
    if direction: return direction
    return ''

def _get_ts_ms(r, i):
    return int(r.get('end_ts_ms') or r.get('timestamp', i * 1000))

X_list = []
for i, line in enumerate(open(train_path)):
    if not line.strip(): continue
    r = json.loads(line)
    y_z.append(int(r.get('y_closed') or r.get('y', 0)))
    val = r.get('r_mult')
    val = 0.1 if val is None else abs(float(val))
    w_z.append(np.clip(val, 0.1, 10.0))
    x_row = build_feature_row(
        feature_cols=obj['feature_cols'],
        indicators=r.get('indicators', {}),
        direction=_get_direction(r),
        scenario=str(r.get('scenario_v4', '')),
        ts_ms=_get_ts_ms(r, i),
        feature_transforms=obj.get('feature_transforms', {}),
        robust_scaler_params=obj.get('robust_scaler', {}),
        session_cfg=None, spread_bucket_edges=None, liq_cfg=None
    )
    X_list.append(x_row)

X = np.array(X_list, dtype=np.float32)
y_z = np.array(y_z)
w_z = np.array(w_z)

# Fake OOF predictions using the base models on the FULL train set
# It's an approximation of OOF
p_lr = obj['lr'].predict_proba(X)[:, 1]
p_gbdt = obj['gbdt'].predict_proba(X)[:, 1]

def _meta_loss_weighted(w):
    p = 1.0 / (1.0 + np.exp(-(w[0]*p_lr + w[1]*p_gbdt + w[2])))
    p = np.clip(p, 1e-15, 1 - 1e-15)
    return np.average(- (y_z * np.log(p) + (1 - y_z) * np.log(1 - p)), weights=w_z)

def _meta_loss_unweighted(w):
    p = 1.0 / (1.0 + np.exp(-(w[0]*p_lr + w[1]*p_gbdt + w[2])))
    p = np.clip(p, 1e-15, 1 - 1e-15)
    return np.mean(- (y_z * np.log(p) + (1 - y_z) * np.log(1 - p)))

print("Weighted Loss for LR only  :", _meta_loss_weighted([1.0, 0.0, 0.0]))
print("Weighted Loss for GBDT only:", _meta_loss_weighted([0.0, 1.0, 0.0]))
print("Unweighted Loss for LR only  :", _meta_loss_unweighted([1.0, 0.0, 0.0]))
print("Unweighted Loss for GBDT only:", _meta_loss_unweighted([0.0, 1.0, 0.0]))

res_w = minimize(_meta_loss_weighted, [1, 1, 0], bounds=[(0, 20), (0, 20), (-20, 20)])
print("Optimal Weights (Weighted Target):", res_w.x)

res_u = minimize(_meta_loss_unweighted, [1, 1, 0], bounds=[(0, 20), (0, 20), (-20, 20)])
print("Optimal Weights (Unweighted Target):", res_u.x)
