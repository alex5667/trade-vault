#!/usr/bin/env python3
"""
Batch instrumentation - process 5 blocks at a time.
"""
from pathlib import Path
import re

file_path = Path("/home/alex/front/trade/scanner_infra/python-worker/services/crypto_orderflow_service.py")
content = file_path.read_text()
lines = content.split('\n')

# Find all uninstrumented exception blocks
uninstrumented = []
for i, line in enumerate(lines):
    if 'except Exception:' in line or 'except RedisError' in line:
        # Check if already instrumented
        if i + 1 < len(lines):
            next_line = lines[i + 1]
            if 'log_silent_error' not in next_line and ('pass' in next_line or 'return' in next_line):
                # Get context
                context = '\n'.join(lines[max(0, i-10):i]).lower()
                
                # Categorize
                kind = 'unknown'
                if 'persist' in context:
                    kind = 'persist_failure'
                elif 'ensure' in context and 'load' in context:
                    kind = 'calib_load_failure'
                elif 'ack' in context:
                    kind = 'ack_failure'
                elif 'publish' in context:
                    kind = 'publish_failure'
                elif 'redis' in context and ('sadd' in context or 'expire' in context or 'set' in context):
                    kind = 'redis_write_failure'
                elif 'config.get' in context or 'parse' in context:
                    kind = 'config_parse_failure'
                elif 'init' in context or '__post_init__' in context:
                    kind = 'init_failure'
                
                uninstrumented.append({
                    'line': i
                    'kind': kind
                    'context': context[:60]
                })

print(f"Total uninstrumented blocks: {len(uninstrumented)}")
print(f"\nProcessing first 5 blocks:")

# Process first 5 blocks (in reverse order)
batch = uninstrumented[:5]
batch.reverse()

modified_count = 0
for block in batch:
    line_idx = block['line']
    kind = block['kind']
    
    line = lines[line_idx]
    if 'except Exception:' in line or 'except RedisError' in line:
        # Get indentation
        indent = len(line) - len(line.lstrip())
        spaces = ' ' * indent
        
        # Replace with instrumented version
        if 'except RedisError' in line:
            lines[line_idx] = f"{spaces}except RedisError as exc:"
        else:
            lines[line_idx] = f"{spaces}except Exception as exc:"
        
        # Insert log_silent_error
        if line_idx + 1 < len(lines):
            next_line = lines[line_idx + 1]
            next_indent = len(next_line) - len(next_line.lstrip())
            next_spaces = ' ' * next_indent
            
            # Determine symbol variable
            symbol_var = 'self.symbol'
            for ctx_line in reversed(lines[max(0, line_idx-20):line_idx]):
                if re.search(r'\bsymbol\s*=', ctx_line) and 'self.symbol' not in ctx_line:
                    symbol_var = 'symbol'
                    break
            
            context_str = block['context'][:40].replace("'", "\\'")
            log_line = f"{next_spaces}log_silent_error(exc, '{kind}', {symbol_var}, '{context_str}')"
            lines.insert(line_idx + 1, log_line)
            modified_count += 1
            print(f"  ✓ Line {line_idx + 1}: {kind}")

# Write back
if modified_count > 0:
    new_content = '\n'.join(lines)
    file_path.write_text(new_content)
    print(f"\n✅ Batch complete: {modified_count} blocks instrumented")
    print(f"📝 Remaining: {len(uninstrumented) - modified_count}")
else:
    print("\n⚠️ No blocks modified")
