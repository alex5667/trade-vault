import os
import re

def process_file(filepath):
    with open(filepath, 'r') as f:
        content = f.read()

    # Match missing dots: c.loggerErrorf -> c.logger.Errorf
    content_new = re.sub(r'(\.logger|\.Logger|\.log)(Errorf|Infof|Warnf|Fatal|Fatalf|Error|Warn|Info)\b', r'\1.\2', content)
    
    if content_new != content:
        with open(filepath, 'w') as f:
            f.write(content_new)
        print(f"Fixed dots in {filepath}")

for root, dirs, files in os.walk('go-worker'):
    for file in files:
        if file.endswith('.go'):
            process_file(os.path.join(root, file))

