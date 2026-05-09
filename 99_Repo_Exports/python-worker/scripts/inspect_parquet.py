
import pandas as pd

pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)
try:
    df = pd.read_parquet('/var/lib/trade/of_reports/datasets/nightly_meta_v4.parquet')
    cols = ['ts_ms', 'y', 'r_mult', 'closed_event_type']
    for c in ['closed_pnl', 'closed_risk_usd', 'closed_reason']:
        if c in df.columns:
            cols.append(c)
    print(df[cols].head(20))
except Exception as e:
    print(e)
