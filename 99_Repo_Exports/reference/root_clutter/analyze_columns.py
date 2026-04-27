import pandas as pd
import json

file_path = '/home/alex/front/trade/scanner_infra/python-worker/of_reports_out/run_regress_20260220_053433/of_replay.ndjson'
records = []

with open(file_path, 'r', encoding='utf-8') as f:
    for line in f:
        s = line.strip()
        if s:
            records.append(json.loads(s))

print(f"Loaded {len(records)} signals.")
if len(records) > 0:
    df = pd.json_normalize(records)
    print(f"Total columns found: {len(df.columns)}")
    
    print("\nColumn Fill Analysis:")
    print(f"{'Column':<35} | {'Filled %':<10} | {'Missing %':<10}")
    print("-" * 65)
    
    for col in sorted(df.columns):
        series = df[col]
        
        # Calculate exactly what's populated
        if series.dtype == object:
            is_empty_str = series == ""
            is_empty_list = series.apply(lambda x: isinstance(x, list) and len(x) == 0)
            is_empty_dict = series.apply(lambda x: isinstance(x, dict) and len(x) == 0)
            is_null = series.isnull()
            
            non_empty_mask = ~(is_empty_str | is_empty_list | is_empty_dict | is_null)
        else:
            non_empty_mask = ~series.isnull()
            
        filled_count = non_empty_mask.sum()
        total = len(df)
        
        filled_pct = (filled_count / total) * 100
        missing_pct = 100 - filled_pct
        
        print(f"{col:<35} | {filled_pct:>8.2f}% | {missing_pct:>8.2f}%")
