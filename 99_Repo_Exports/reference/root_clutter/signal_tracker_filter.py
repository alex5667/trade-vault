#!/usr/bin/env python3
import sys
import re

# Pattern to match the signal tracker log messages about zero trades
pattern = r'scanner-signal-tracker.*\|.*\|.*PeriodicReporter.*\|.*(?:⚠️ Нет сделок|Итого собрано 0 сделок)'

counter = 0

for line in sys.stdin:
    if re.search(pattern, line):
        counter += 1
        if counter % 1000 == 0:
            print(f"[{counter}] {line.rstrip()}")
