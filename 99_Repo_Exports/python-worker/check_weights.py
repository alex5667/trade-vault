import joblib
import numpy as np
m = joblib.load('/var/lib/trade/ml_models/edge_stack_v1/runs/20260331_032133/edge_stack_v1.joblib')
print("Meta weights:", m['meta'].coef_, m['meta'].intercept_)
print("LR coefs:", m['lr'].coef_[0][:10]) # first 10
features = m['feature_cols']
buy_idx = features.index('direction_BUY')
sell_idx = features.index('direction_SELL')
print("BUY base LR coef:", m['lr'].coef_[0][buy_idx])
print("SELL base LR coef:", m['lr'].coef_[0][sell_idx])
