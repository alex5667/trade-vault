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
    
    missing_data = []
    for col in df.columns:
        # Calculate nulls, NaNs, None, and empty strings
        null_count = df[col].isnull().sum()
        empty_str_count = (df[col] == "").sum() if df[col].dtype == object else 0
        blank_list_count = df[col].apply(lambda x: isinstance(x, list) and len(x) == 0).sum()
        blank_dict_count = df[col].apply(lambda x: isinstance(x, dict) and len(x) == 0).sum()
        
        total_missing = null_count + empty_str_count + blank_list_count + blank_dict_count
        missing_pct = (total_missing / len(df)) * 100
        
        if total_missing > 0:
            missing_data.append({
                'Column': col,
                'Missing Count': total_missing,
                'Missing %': missing_pct,
                'Details': f"Nulls:{null_count}, EmptyStr:{empty_str_count}, EmptyList:{blank_list_count}, EmptyDict:{blank_dict_count}"
            })
    
    missing_data.sort(key=lambda x: x['Missing %'], reverse=True)
    
    print("\nColumns with MISSING/EMPTY data:")
    for m in missing_data:
        print(f"{m['Column']:<35} | {m['Missing Count']:<6} ({m['Missing %']:>6.2f}%) | {m['Details']}")
        
    print("\nColumns that are ALWAYS EMPTY (100% missing):")
    for m in missing_data:
        if m['Missing %'] == 100.0:
            print(f"- {m['Column']}")
