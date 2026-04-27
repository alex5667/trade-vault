import joblib
m = joblib.load('/var/lib/trade/ml_models/edge_stack_v1/runs/20260331_032716/edge_stack_v1.joblib')
print("Meta coef:", m['meta'].coef_, m['meta'].intercept_)
buy_idx = m['feature_cols'].index('direction_BUY')
sell_idx = m['feature_cols'].index('direction_SELL')
print("BUY base LR coef:", m['lr'].coef_[0][buy_idx])
print("SELL base LR coef:", m['lr'].coef_[0][sell_idx])
