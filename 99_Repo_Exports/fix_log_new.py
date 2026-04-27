import os
import re

def process_file(filepath):
    with open(filepath, 'r') as f:
        content = f.read()

    # match cfg.Logger = zap.S(), ... )
    content_new = re.sub(r'zap\.S\(\),[^)]+\)', 'zap.S()', content)
    
    if content_new != content:
        with open(filepath, 'w') as f:
            f.write(content_new)
        print(f"Fixed {filepath}")

for root, dirs, files in os.walk('go-worker/internal/marketdata'):
    for file in files:
        if file.endswith('.go'):
            process_file(os.path.join(root, file))

