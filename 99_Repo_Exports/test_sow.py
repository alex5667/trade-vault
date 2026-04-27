import re

with open("/home/alex/front/trade/scanner_infra/python-worker/services/signal_outcome_writer.py") as f:
    text = f.read()

idx_insert = text.find("INSERT INTO signal_outcomes")
if idx_insert != -1:
    idx_values = text.find("VALUES (", idx_insert)
    idx_conflict = text.find("ON CONFLICT", idx_values)
    vals = text[idx_values:idx_conflict]
    print("SOW Placeholders:", vals.count("%s"))

