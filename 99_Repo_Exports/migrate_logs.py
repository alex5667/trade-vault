import os
import re

def find_matching_paren(s, start_idx):
    count = 0
    for i in range(start_idx, len(s)):
        if s[i] == '(': count += 1
        elif s[i] == ')': count -= 1
        if count == 0: return i
    return -1

def process_file(filepath):
    with open(filepath, 'r') as f:
        content = f.read()

    if 'logging/file_logger.go' in filepath or '/logging/' in filepath:
        return

    made_changes = False
    
    # We will search for log.Printf, log.Println, log.Fatal, log.Fatalf, log.Panic, log.Panicf
    pattern = re.compile(r'(?m)^([ \t]*(?://[ \t]*)?)log\.(Printf|Print|Println|Fatalf|Fatal|Panicf|Panic)\s*\(')
    
    pos = 0
    while True:
        match = pattern.search(content, pos)
        if not match:
            break
            
        start_paren = match.end() - 1
        end_paren = find_matching_paren(content, start_paren)
        
        if end_paren == -1:
            pos = match.end()
            continue
            
        indent = match.group(1)
        method = match.group(2)
        args = content[start_paren+1:end_paren]
        
        args_lower = args.lower()
        
        if 'fatal' in method.lower():
            new_method = 'Fatalf' if method.endswith('f') else 'Fatal'
        elif 'panic' in method.lower():
            new_method = 'Panicf' if method.endswith('f') else 'Panic'
        elif method == 'Println' or method == 'Print':
            if '❌' in args or 'error' in args_lower or 'err' in args_lower or 'fail' in args_lower:
                new_method = 'Error'
            elif '⚠️' in args or 'warn' in args_lower:
                new_method = 'Warn'
            else:
                new_method = 'Info'
        else: # Printf
            if '❌' in args or 'error' in args_lower or 'err' in args_lower or 'fail' in args_lower:
                new_method = 'Errorf'
            elif '⚠️' in args or 'warn' in args_lower:
                new_method = 'Warnf'
            else:
                new_method = 'Infof'
                
        replacement = f"{indent}zap.S().{new_method}({args})"
        old_str = content[match.start():end_paren+1]
        
        content = content[:match.start()] + replacement + content[end_paren+1:]
        made_changes = True
        pos = match.start() + len(replacement)

    if made_changes:
        # Avoid double imports
        if '"go.uber.org/zap"' not in content:
            import_blocks = list(re.finditer(r'import\s+\((.*?)\)', content, flags=re.DOTALL))
            if import_blocks:
                last_import = import_blocks[-1]
                inner = last_import.group(1)
                new_inner = inner + '\n\t"go.uber.org/zap"\n'
                content = content[:last_import.start(1)] + new_inner + content[last_import.end(1):]
            else:
                pkg_match = re.search(r'package\s+\w+\n', content)
                if pkg_match:
                    content = content[:pkg_match.end()] + '\nimport "go.uber.org/zap"\n' + content[pkg_match.end():]
        
        with open(filepath, 'w') as f:
            f.write(content)
        print(f"Updated {filepath}")

def main():
    paths = ['go-worker', 'go-gateway']
    for p in paths:
        for root, dirs, files in os.walk(p):
            for file in files:
                if file.endswith('.go'):
                    process_file(os.path.join(root, file))

if __name__ == '__main__':
    main()
