import re

with open('/tmp/orig/binance_execution/binance_executor.py.rej', 'r') as f:
    text = f.read()

print(f"Rej length: {len(text)}")
