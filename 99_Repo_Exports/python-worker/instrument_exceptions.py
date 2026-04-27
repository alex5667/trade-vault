#!/usr/bin/env python3
"""
Automated instrumentation of exception blocks.
Applies log_silent_error to critical exception handlers.
"""
import re
from pathlib import Path

file_path = Path("/home/alex/front/trade/scanner_infra/python-worker/services/crypto_orderflow_service.py")
content = file_path.read_text()
lines = content.split('\n')

# Define patterns and their instrumentation
patterns = [
    # ACK failures (already done one, do remaining)
    {
        'match': r'except RedisError as exc:\s*\n\s*logger\.warning\("⚠️.*Не удалось ACK',
        'kind': 'ack_failure',
        'insert_before_logger': True
    },
    # Persist failures
    {
        'match': r'(persist_\w+.*\n(?:.*\n)*?)\s*except Exception:\s*\n\s*pass',
        'kind': 'persist_failure',
        'replace_pass': True
    },
    # Calibrator load failures  
    {
        'match': r'(ensure_\w+_loaded.*\n(?:.*\n)*?)\s*except Exception:\s*\n\s*pass',
        'kind': 'calib_load_failure',
        'replace_pass': True
    },
]

modifications = []

# Find all except Exception: pass blocks
for i, line in enumerate(lines):
    if 'except Exception:' in line or 'except RedisError' in line:
        # Check next line for pass or existing logging
        if i + 1 < len(lines):
            next_line = lines[i + 1]
            if 'pass' in next_line and 'log_silent_error' not in next_line:
                # Determine context
                context_lines = lines[max(0, i-10):i]
                context_text = '\n'.join(context_lines).lower()
                
                # Categorize
                kind = 'unknown'
                symbol_var = 'symbol'
                
                if 'ack' in context_text:
                    kind = 'ack_failure'
                elif 'persist' in context_text:
                    kind = 'persist_failure'
                elif 'publish' in context_text:
                    kind = 'publish_failure'
                elif 'ensure' in context_text and 'load' in context_text:
                    kind = 'calib_load_failure'
                elif 'redis' in context_text and ('set' in context_text or 'expire' in context_text):
                    kind = 'redis_write_failure'
                
                # Extract symbol variable name
                for ctx_line in reversed(context_lines):
                    if 'symbol' in ctx_line:
                        match = re.search(r'(\w+symbol\w*)', ctx_line)
                        if match:
                            symbol_var = match.group(1)
                            break
                
                if kind != 'unknown':
                    modifications.append({
                        'line': i + 1,
                        'kind': kind,
                        'symbol_var': symbol_var,
                        'indent': len(next_line) - len(next_line.lstrip())
                    })

print(f"Found {len(modifications)} blocks to instrument")
print("\nTop 20 modifications:")
for mod in modifications[:20]:
    print(f"Line {mod['line']}: {mod['kind']} (symbol={mod['symbol_var']})")

# Generate patch
print("\n=== Generating instrumentation code ===")
for mod in modifications[:10]:  # Start with top 10
    indent = ' ' * mod['indent']
    print(f"\n# Line {mod['line']} - {mod['kind']}")
    print(f"{indent}except Exception as exc:")
    print(f"{indent}    log_silent_error(exc, '{mod['kind']}', {mod['symbol_var']}, context='')")
    print(f"{indent}    pass")
