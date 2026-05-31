
import pandas as pd
try:
    df = pd.read_parquet('/var/lib/trade/of_reports/datasets/nightly_meta_v4.parquet')
    print(df[['symbol', 'ts_ms', 'y', 'r_mult', 'label_source', 'closed_event_type', 'closed_reason', 'closed_pnl']].head(20))
    print("\nValue Counts for y:")
    print(df['y'].value_counts())
    print("\nR_mult stats:")
    print(df['r_mult'].describe())
except Exception as e:
    print(e)
