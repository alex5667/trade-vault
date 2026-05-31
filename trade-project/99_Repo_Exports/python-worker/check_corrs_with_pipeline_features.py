import json
import joblib
import numpy as np

# Load the model
path = '/var/lib/trade/ml_models/edge_stack_v1/champions/edge_stack_v1_champion.joblib'
obj = joblib.load(path)

# Extract pipeline components
feature_cols = obj['feature_cols']
lr = obj['lr']
gbdt = obj['gbdt']
meta = obj['meta']
feature_transforms = obj.get('feature_transforms', {})
robust_scaler = obj.get('robust_scaler', {})

from ml_analysis.tools.train_edge_stack_v1_oof import build_feature_row  # type: ignore

def _get_direction(r):
    ind = r.get('indicators', {})
    k = str(ind.get('meta_enforce_key') or r.get('key', ''))
    if '|LONG|' in k: return 'BUY'
    if '|SHORT|' in k: return 'SELL'
    direction = str(r.get('direction', ''))
    if direction: return direction
    return ''

def _get_scenario(r):
    return str(r.get('scenario_v4', ''))

def _get_indicators(r):
    return r.get('indicators', {})

def _get_ts_ms(r, i):
    return int(r.get('end_ts_ms') or r.get('timestamp', i * 1000))

y_true = []
pnl_list = []
X_list = []
is_buy_list = []

# use the exactly matched train file 
train_path = path.replace('champions/edge_stack_v1_champion.joblib', 'runs/20260331_032716/edge_train.jsonl')

with open(train_path, 'r') as f:
    for i, line in enumerate(f):
        if not line.strip(): continue
        row = json.loads(line)
        ind = _get_indicators(row)
        dir_str = _get_direction(row)
        scen_str = _get_scenario(row)
        ts_ms = _get_ts_ms(row, i)
        
        x_row = build_feature_row(
            feature_cols=feature_cols,
            indicators=ind,
            direction=dir_str,
            scenario=scen_str,
            ts_ms=ts_ms,
            feature_transforms=feature_transforms,
            robust_scaler_params=robust_scaler,
            session_cfg=None, spread_bucket_edges=None, liq_cfg=None
        )
        X_list.append(x_row)
        y_true.append(row.get('y', 0))
        pnl_list.append(row.get('pnl', 0.0))
        is_buy_list.append(1 if dir_str == 'BUY' else 0)

X = np.array(X_list, dtype=np.float32)
y_arr = np.array(y_true)
pnl_arr = np.array(pnl_list)
buy_arr = np.array(is_buy_list)

p_lr = lr.predict_proba(X)[:, 1]
p_gbdt = gbdt.predict_proba(X)[:, 1]

# Our custom non-negative stacking logistic regression
coef = meta.coef_[0]
intercept = meta.intercept_[0]
Z = np.column_stack([p_lr, p_gbdt])
z_logit = np.dot(Z, coef) + intercept
p_meta = 1.0 / (1.0 + np.exp(-z_logit))

corr_meta_buy = np.corrcoef(p_meta, buy_arr)[0, 1]
corr_meta_pnl = np.corrcoef(p_meta, pnl_arr)[0, 1]

corr_gbdt_buy = np.corrcoef(p_gbdt, buy_arr)[0, 1]
corr_gbdt_pnl = np.corrcoef(p_gbdt, pnl_arr)[0, 1]

corr_lr_buy = np.corrcoef(p_lr, buy_arr)[0, 1]
corr_lr_pnl = np.corrcoef(p_lr, pnl_arr)[0, 1]

print(f"Meta weights: LR={coef[0]:.4f}, GBDT={coef[1]:.4f}, Intercept={intercept:.4f}")
print(f"LR Base model -> Corr w/ BUY: {corr_lr_buy:.4f}, Corr w/ PNL: {corr_lr_pnl:.4f}")
print(f"GBDT Base model -> Corr w/ BUY: {corr_gbdt_buy:.4f}, Corr w/ PNL: {corr_gbdt_pnl:.4f}")
print(f"Meta Stacking -> Corr w/ BUY: {corr_meta_buy:.4f}, Corr w/ PNL: {corr_meta_pnl:.4f}")

# Sanity Check
buy_pnl = pnl_arr[buy_arr == 1]
sell_pnl = pnl_arr[buy_arr == 0]
print(f"Average PNL for BUY trades: {np.mean(buy_pnl):.4f}")
print(f"Average PNL for SELL trades: {np.mean(sell_pnl):.4f}")
