#!/usr/bin/env python3
"""
Apply instrumentation to specific line numbers.
"""
from pathlib import Path

file_path = Path("/home/alex/front/trade/scanner_infra/python-worker/services/crypto_orderflow_service.py")
content = file_path.read_text()
lines = content.split('\n')

# Top-20 critical blocks to instrument (line numbers from analysis)
# Format: (line_num, kind, context)
targets = [
    (1346, 'redis_write_failure', 'persist_dn_regime:sadd/expire')
    (1348, 'persist_failure', 'persist_dn_regime')
    (1631, 'redis_write_failure', 'ensure_book_rate_loaded:sadd/expire')
    (1639, 'persist_failure', 'persist_book_rate_regime')
    (1657, 'calib_load_failure', 'ensure_book_rate_loaded')
    (1714, 'calib_load_failure', 'ensure_book_rate_loaded')
    (1721, 'persist_failure', 'persist_book_rate_regime')
    (1755, 'persist_failure', 'persist_atr_tf_regime')
    (1844, 'persist_failure', 'persist_calibration_regime')
    (1917, 'persist_failure', 'persist_calibration_regime')
    (1951, 'persist_failure', 'persist_calibration_regime')
    (2676, 'persist_failure', 'persist_burst_calibration')
    (3120, 'ack_failure', 'consume_books')
    (3906, 'ack_failure', 'consume_books')
]

# Apply changes (in reverse order to preserve line numbers)
modified_count = 0
for line_num, kind, context in reversed(targets):
    idx = line_num - 1  # Convert to 0-indexed
    if idx < len(lines):
        line = lines[idx]
        if 'except Exception:' in line and 'log_silent_error' not in line:
            # Get indentation
            indent = len(line) - len(line.lstrip())
            spaces = ' ' * indent
            
            # Replace with instrumented version
            lines[idx] = f"{spaces}except Exception as exc:"
            
            # Check next line for pass/return
            if idx + 1 < len(lines):
                next_line = lines[idx + 1]
                if 'pass' in next_line or 'return' in next_line:
                    # Insert log_silent_error before pass/return
                    next_indent = len(next_line) - len(next_line.lstrip())
                    next_spaces = ' ' * next_indent
                    log_line = f"{next_spaces}log_silent_error(exc, '{kind}', self.symbol, '{context}')"
                    lines.insert(idx + 1, log_line)
                    modified_count += 1
                    print(f"✓ Line {line_num}: {kind}")

# Write back
new_content = '\n'.join(lines)
file_path.write_text(new_content)

print(f"\n✅ Modified {modified_count} blocks")
print(f"📝 File updated: {file_path}")
