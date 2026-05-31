#!/usr/bin/env python3
"""
Apply instrumentation to remaining top-20 critical blocks.
Uses pattern matching to find current line numbers.
"""
import re
from pathlib import Path

file_path = Path("/home/alex/front/trade/scanner_infra/python-worker/services/crypto_orderflow_service.py")
content = file_path.read_text()
lines = content.split('\n')

# Find blocks by pattern matching
def find_blocks_by_pattern(pattern, kind, max_results=5):
    """Find exception blocks matching pattern"""
    results = []
    for i, line in enumerate(lines):
        if 'except Exception:' in line:
            # Check if already instrumented
            if i + 1 < len(lines) and 'log_silent_error' in lines[i + 1]:
                continue

            # Check next line for pass/return
            if i + 1 < len(lines) and ('pass' in lines[i + 1] or 'return' in lines[i + 1]):
                # Check context (10 lines before)
                context = '\n'.join(lines[max(0, i-10):i]).lower()
                if pattern.lower() in context:
                    results.append((i, kind, pattern))
                    if len(results) >= max_results:
                        break
    return results

# Patterns to find
patterns = [
    ('persist_book_rate_regime', 'persist_failure', 2),
    ('ensure_book_rate_loaded', 'calib_load_failure', 2),
    ('persist_atr_tf_regime', 'persist_failure', 1),
    ('persist_calibration_regime', 'persist_failure', 3),
    ('persist_burst_calibration', 'persist_failure', 1),
    ('await helper.ack(stream_name, msg_id)', 'ack_failure', 2),
]

# Collect all blocks to modify
all_blocks = []
for pattern, kind, max_count in patterns:
    blocks = find_blocks_by_pattern(pattern, kind, max_count)
    all_blocks.extend(blocks)
    print(f"Found {len(blocks)} blocks for '{pattern}' ({kind})")

print(f"\nTotal blocks to instrument: {len(all_blocks)}")

# Apply modifications (in reverse order to preserve line numbers)
all_blocks.sort(key=lambda x: x[0], reverse=True)
modified_count = 0

for line_idx, kind, pattern in all_blocks:
    line = lines[line_idx]
    if 'except Exception:' in line:
        # Get indentation
        indent = len(line) - len(line.lstrip())
        spaces = ' ' * indent

        # Replace with instrumented version
        lines[line_idx] = f"{spaces}except Exception as exc:"

        # Insert log_silent_error before pass/return
        if line_idx + 1 < len(lines):
            next_line = lines[line_idx + 1]
            next_indent = len(next_line) - len(next_line.lstrip())
            next_spaces = ' ' * next_indent

            # Determine symbol variable
            symbol_var = 'self.symbol'
            if 'symbol' in '\n'.join(lines[max(0, line_idx-20):line_idx]):
                # Check for local symbol variable
                for ctx_line in reversed(lines[max(0, line_idx-20):line_idx]):
                    if re.search(r'\bsymbol\s*=', ctx_line):
                        symbol_var = 'symbol'
                        break

            context_str = pattern[:30]  # Truncate long patterns
            log_line = f"{next_spaces}log_silent_error(exc, '{kind}', {symbol_var}, '{context_str}')"
            lines.insert(line_idx + 1, log_line)
            modified_count += 1
            print(f"✓ Line {line_idx + 1}: {kind} ({pattern[:40]})")

# Write back
if modified_count > 0:
    new_content = '\n'.join(lines)
    file_path.write_text(new_content)
    print(f"\n✅ Modified {modified_count} blocks")
    print(f"📝 File updated: {file_path}")
else:
    print("\n⚠️ No blocks modified (all may already be instrumented)")
