import os
import re

sql_dir = "/home/alex/front/trade/scanner_infra/sql"
files = [
    "001_entry_policy_audit.sql",
    "002_position_events.sql",
    "003_tb_labels.sql",
    "004_signal_outcomes.sql",
    "edge_gate_events.sql",
    "ml_phase1_8_v1.sql",
    "news_timescale.sql"
]

for filename in files:
    filepath = os.path.join(sql_dir, filename)
    if not os.path.exists(filepath):
        continue
    with open(filepath, 'r') as f:
        content = f.read()
    
    tables = re.findall(r'CREATE TABLE IF NOT EXISTS\s+(\w+)', content, re.IGNORECASE)
    if tables:
        down_filename = filename.replace('.sql', '_down.sql')
        down_filepath = os.path.join(sql_dir, down_filename)
        with open(down_filepath, 'w') as out_f:
            for t in reversed(tables):
                out_f.write(f"DROP TABLE IF EXISTS {t} CASCADE;\n")
        print(f"Created {down_filename} dropping {tables}")

