import numpy as np
import json
import joblib

path = '/var/lib/trade/ml_models/edge_stack_v1/champions/edge_stack_v1_champion.joblib'
obj = joblib.load(path)
gbdt = obj['gbdt']
print("GBDT Classes:", gbdt.classes_)

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

train_path = path.replace('champions/edge_stack_v1_champion.joblib', 'runs/20260331_033306/edge_train.jsonl')

X_list, buy_list = [], []
with open(train_path, 'r') as f:
    for i, line in enumerate(f):
        if not line.strip(): continue
        row = json.loads(line)
        dir_str = _get_direction(row)
        x_row = build_feature_row(
            feature_cols=obj['feature_cols'],
            indicators=row.get('indicators', {}),
            direction=dir_str,
            scenario=str(row.get('scenario_v4', '')),
            ts_ms=_get_ts_ms(row, i),
            feature_transforms=obj.get('feature_transforms', {}),
            robust_scaler_params=obj.get('robust_scaler', {}),
            session_cfg=None, spread_bucket_edges=None, liq_cfg=None
        )
        X_list.append(x_row)
        buy_list.append(1 if dir_str == 'BUY' else 0)

X = np.array(X_list, dtype=np.float32)
p_gbdt = gbdt.predict_proba(X)[:, 1]
buy_arr = np.array(buy_list)

print(f"GBDT Proba on BUY: {np.mean(p_gbdt[buy_arr == 1]):.4f}")
print(f"GBDT Proba on SELL: {np.mean(p_gbdt[buy_arr == 0]):.4f}")
