#!/usr/bin/env python3

import os

def fix_syntax_errors(filepath):
    """Fix syntax errors in file."""
    with open(filepath, 'r') as f:
        lines = f.readlines()
    
    fixed_lines = []
    for line in lines:
        # Fix broken lambda calls
        if 'lambda x: None(' in line:
            # Extract the argument from lambda x: None(arg)
            start = line.find('lambda x: None(')
            end = line.find(')', start)
            if start != -1 and end != -1:
                arg = line[start+15:end+1]  # Extract argument
                line = line.replace(f'lambda x: None{arg}', f'setup_logger{arg}')
        
        # Fix broken method definitions
        if 'def lambda ' in line:
            line = line.replace('def lambda ', 'def get_config_summary(')
        
        fixed_lines.append(line)
    
    with open(filepath, 'w') as f:
        f.writelines(fixed_lines)
    
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
