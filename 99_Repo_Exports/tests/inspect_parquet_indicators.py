
import pandas as pd
import sys

def inspect_indicators(parquet_path):
    try:
        df = pd.read_parquet(parquet_path)
        if "indicators" not in df.columns:
            print("indicators column not found")
            return

        print(f"Total rows: {len(df)}")
        print(f"Top-level columns: {df.columns.tolist()}")

        sample = df["indicators"].dropna().head(1)
        if not sample.empty:
            print("Sample indicator keys:")
            print(list(sample.iloc[0].keys()))
        else:
            print("indicators column is empty or all null")

    except Exception as e:
        print(f"Error reading parquet: {e}")

if __name__ == "__main__":
    if len(sys.argv) > 1:
        inspect_indicators(sys.argv[1])
    else:
        print("Usage: python3 inspect_parquet_indicators.py <parquet_path>")
