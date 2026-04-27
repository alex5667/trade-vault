import pandas as pd
import sys

path = '/var/lib/trade/ml_models/tb_v10_4_20260207_054222_961974/dataset_mh.parquet'
try:
    df = pd.read_parquet(path)
    print(f"Dataset shape: {df.shape}")
    
    cols_to_check = ['y_util_pos_60000', 'util_r_60000', 'f_ofi', 'f_delta_z']
    for col in cols_to_check:
        if col in df.columns:
            print(f"\nStats for {col}:")
            print(f"Mean: {df[col].mean():.5f}")
            print(f"Max: {df[col].max():.5f}")
            print(f"Min: {df[col].min():.5f}")
            print(f"Sum: {df[col].sum():.5f}")
            print(f"NaN count: {df[col].isna().sum()}")
        else:
            print(f"{col} not in dataframe")
except Exception as e:
    print(f"Error reading parquet: {e}")
