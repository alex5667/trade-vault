import os
import subprocess

errors = []
for root, dirs, files in os.walk('tests'):
    for file in files:
        if file.startswith('test_') and file.endswith('.py'):
            path = os.path.join(root, file)
            # Run pytest on this file alone, grep for ImportError/ModuleNotFoundError
            try:
                out = subprocess.check_output(
                    ['pytest', '--collect-only', path],
                    stderr=subprocess.STDOUT, text=True
                )
            except subprocess.CalledProcessError as e:
                for line in e.output.split('\n'):
                    if 'ImportError:' in line or 'ModuleNotFoundError:' in line:
                        errors.append(f"{path}: {line.strip()}")
                        break

for e in errors:
    print(e)
print(f"Total isolated broken test files: {len(errors)}")
