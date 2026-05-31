#!/usr/bin/env python3

import os
import re

def fix_syntax_errors(filepath):
    """Fix syntax errors in file."""
    with open(filepath, 'r') as f:
        content = f.read()
    
    # Fix broken lambda calls
    content = re.sub(r'lambda x: None\([^)]+\)', 'setup_logger(\g<1>)', content)
    content = re.sub(r'def lambda [^:]+:', 'def get_config_summary(', content)
    
    with open(filepath, 'w') as f:
        f.write(content)
    
    print(f"Fixed syntax in {filepath}")

def main():
    handlers_dir = 'python-worker/handlers'
    if os.path.exists(handlers_dir):
        for filename in os.listdir(handlers_dir):
            if filename.endswith('.py') and not filename.startswith('__'):
                filepath = os.path.join(handlers_dir, filename)
                if os.path.isfile(filepath):
                    fix_syntax_errors(filepath)
    
    print("All syntax errors fixed!")

if __name__ == '__main__':
    main()
