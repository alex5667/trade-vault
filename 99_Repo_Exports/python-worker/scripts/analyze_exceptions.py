#!/usr/bin/env python3
"""
Analyze all except blocks in crypto_orderflow_service.py
Categorize by criticality and generate instrumentation plan.
"""
import re
from pathlib import Path

file_path = Path("/home/alex/front/trade/scanner_infra/python-worker/services/crypto_orderflow_service.py")
content = file_path.read_text()
lines = content.split('\n')

# Find all except blocks with context
except_blocks = []
for i, line in enumerate(lines):
    if 'except Exception:' in line or 'except:' in line:
        # Get context: 5 lines before and 3 lines after
        start = max(0, i - 5)
        end = min(len(lines), i + 4)
        context = '\n'.join(f"{j+1:4d}: {lines[j]}" for j in range(start, end))
        
        # Extract function/method name from context
        func_name = "unknown"
        for j in range(i, max(0, i-50), -1):
            if 'def ' in lines[j]:
                match = re.search(r'def\s+(\w+)', lines[j])
                if match:
                    func_name = match.group(1)
                break
        
        except_blocks.append({
            'line': i + 1
            'function': func_name
            'context': context
        })

# Categorize
critical_keywords = ['ack', 'publish', 'persist', 'save', 'write']
important_keywords = ['calib', 'load', 'ensure', 'init', 'metric']
acceptable_keywords = ['config', 'get(', 'parse', 'optional', 'fallback']

critical = []
important = []
acceptable = []
unknown = []

for block in except_blocks:
    ctx_lower = block['context'].lower()
    func_lower = block['function'].lower()
    
    if any(kw in ctx_lower or kw in func_lower for kw in critical_keywords):
        critical.append(block)
    elif any(kw in ctx_lower or kw in func_lower for kw in important_keywords):
        important.append(block)
    elif any(kw in ctx_lower or kw in func_lower for kw in acceptable_keywords):
        acceptable.append(block)
    else:
        unknown.append(block)

# Report
print(f"=== Exception Block Analysis ===")
print(f"Total: {len(except_blocks)}")
print(f"CRITICAL: {len(critical)} (ACK, publish, persist)")
print(f"IMPORTANT: {len(important)} (calibration, metrics, state)")
print(f"ACCEPTABLE: {len(acceptable)} (config, optional features)")
print(f"UNKNOWN: {len(unknown)} (needs manual review)")
print()

print("=== CRITICAL BLOCKS (need logging + metrics) ===")
for block in critical[:10]:  # Show first 10
    print(f"\nLine {block['line']} in {block['function']}:")
    print(block['context'][:200])
    print("...")

print(f"\n... and {len(critical) - 10} more critical blocks")

print("\n=== IMPORTANT BLOCKS (need logging) ===")
for block in important[:5]:
    print(f"\nLine {block['line']} in {block['function']}:")
    print(block['context'][:150])

print(f"\n... and {len(important) - 5} more important blocks")
