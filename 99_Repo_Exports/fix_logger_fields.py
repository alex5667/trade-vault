import re
import os

def process_file(filepath):
    with open(filepath, 'r') as f:
        content = f.read()

    made_changes = False

    # Fix .Printf( -> .Infof(
    # Wait, some might be ERROR. We can just replace Printf with Infof for simplicity, they can adjust if needed, or we can use the same logic
    def repl(m):
        nonlocal made_changes
        made_changes = True
        prefix = m.group(1)
        method = m.group(2)
        args = m.group(3)
        args_lower = args.lower()

        if 'fatal' in method.lower():
            new_method = 'Fatalf' if method.endswith('f') else 'Fatal'
        elif method == 'Println' or method == 'Print':
            if '❌' in args or 'error' in args_lower or 'err' in args_lower or 'fail' in args_lower:
                new_method = 'Error'
            elif '⚠️' in args or 'warn' in args_lower:
                new_method = 'Warn'
            else:
                new_method = 'Info'
        else:
            if '❌' in args or 'error' in args_lower or 'err' in args_lower or 'fail' in args_lower:
                new_method = 'Errorf'
            elif '⚠️' in args or 'warn' in args_lower:
                new_method = 'Warnf'
            else:
                new_method = 'Infof'

        return f"{prefix}{new_method}({args})"

    # match anything.Logger.Printf
    content = re.sub(r'(?m)^([ \t]*[\w\.\*&]+(?:\.log|\.Logger|\.Log))\.(Printf|Print|Println|Fatalf|Fatal)\s*\(([\s\S]*?)\)', lambda m: repl(m), content)
    # also standard `log` might have been missed if it was `\t\tlog.Printf` without matching properly 
    
    # replace log.New(...) with zap.S()
    content_new = re.sub(r'log\.New\([^\)]+\)', 'zap.S()', content)
    if content_new != content:
        made_changes = True
        content = content_new

    # replace log.SetOutput
    content_new = re.sub(r'log\.SetOutput\([^\)]+\)', '', content)
    if content_new != content:
        made_changes = True
        content = content_new

    if made_changes:
        with open(filepath, 'w') as f:
            f.write(content)
        print(f"Updated {filepath}")

def main():
    paths = ['go-worker', 'go-gateway']
    for p in paths:
        for root, dirs, files in os.walk(p):
            for file in files:
                if file.endswith('.go') and 'file_logger.go' not in file:
                    process_file(os.path.join(root, file))

if __name__ == '__main__':
    main()
