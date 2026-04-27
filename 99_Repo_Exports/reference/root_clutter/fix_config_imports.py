#!/usr/bin/env python3

import os
import re

def fix_config_imports(filepath):
    """Fix config imports in file."""
    with open(filepath, 'r') as f:
        content = f.read()
    
    # Replace config imports
    replacements = [
        (r'from core\.config import.*', '# from core.config import ...'),
        (r'from config\.gpu_config import.*', '# from config.gpu_config import ...'),
        (r'from health_metrics import.*', '# from health_metrics import ...'),
        (r'get_config', 'lambda symbol, **kwargs: None'),
        (r'GPU_ENABLE', 'True'),
        (r'GPU_MIN_N', '1000'), 
        (r'GPU_BACKEND', '"numpy"'),
        (r'HealthMetrics', 'object'),
        (r'setup_logger', 'lambda x: None'),
    ]
    
    for pattern, replacement in replacements:
        content = re.sub(pattern, replacement, content)
    
    with open(filepath, 'w') as f:
        f.write(content)

def main():
    handlers_dir = 'python-worker/handlers'
    if os.path.exists(handlers_dir):
        for filename in os.listdir(handlers_dir):
            if filename.endswith('.py') and not filename.startswith('__'):
                filepath = os.path.join(handlers_dir, filename)
                if os.path.isfile(filepath):
                    fix_config_imports(filepath)
    
    print("Config imports fixed!")

if __name__ == '__main__':
    main()
