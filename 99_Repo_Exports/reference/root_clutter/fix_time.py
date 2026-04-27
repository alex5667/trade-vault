import os
import re

TARGET_DIR = "python-worker"
IMPORT_STMT = "from utils.time_utils import get_ny_time_millis"

# Regex examples to cover:
# int(time.time() * 1000)
# int(time.time()*1000)
# int(time.time() * 1000.0) -> wait, maybe just 1000? Let's just do \s*\*\s*1000\)? Wait! It's better to explicitly match int(time.time() * 1000) and (time.time() * 1000)
PATTERN1 = r'int\(\s*time\.time\(\)\s*\*\s*1000\s*\)'
PATTERN2 = r'\(\s*time\.time\(\)\s*\*\s*1000\s*\)'
PATTERN3 = r'time\.time\(\)\s*\*\s*1000'

def process_file(filepath):
    try:
        with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()
    except Exception:
        return False

    # Skip files that don't have time.time()
    if 'time.time()' not in content:
        return False

    original_content = content

    # Replace variations
    content = re.sub(PATTERN1, 'get_ny_time_millis()', content)
    content = re.sub(PATTERN2, 'get_ny_time_millis()', content)
    content = re.sub(PATTERN3, 'get_ny_time_millis()', content)

    if content == original_content:
        return False

    # Add import if missing
    if IMPORT_STMT not in content:
        # Find a good place to inject the import
        # Usually after other imports
        lines = content.split('\n')
        import_index = 0
        for i, line in enumerate(lines):
            if line.startswith('import ') or line.startswith('from '):
                import_index = i
        
        if import_index == 0:
            # Just insert at beginning
            lines.insert(0, IMPORT_STMT)
        else:
            # Insert after the last import
            lines.insert(import_index + 1, IMPORT_STMT)
            
        content = '\n'.join(lines)

    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(content)

    return True

fixed_count = 0
for root, dirs, files in os.walk(TARGET_DIR):
    if '.venv' in dirs:
        dirs.remove('.venv')
    if '__pycache__' in dirs:
        dirs.remove('__pycache__')
    
    for f in files:
        if f.endswith('.py') and f != "time_utils.py":
            path = os.path.join(root, f)
            if process_file(path):
                fixed_count += 1
                print(f"Fixed {path}")

print(f"Total files fixed: {fixed_count}")
