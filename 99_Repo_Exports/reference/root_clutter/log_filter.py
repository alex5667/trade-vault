#!/usr/bin/env python3
import sys
import re

# Pattern to match the log messages
# Supports multiple log types via command line argument
import sys

log_type = sys.argv[1] if len(sys.argv) > 1 else 'prometheus'

patterns = {
    'prometheus': r'scanner-prometheus.*(?:write block completed|Head GC|Creating checkpoint|compact blocks|Deleting obsolete block)',
    'grafana': r'scanner-grafana.*(?:Update check succeeded|All modules healthy|Starting MultiOrg Alertmanager)',
    'background': r'logger=backgroundsvcs\.managerAdapter.*msg="module (?:starting|stopped)" module=\*.*'
}

pattern = patterns.get(log_type, patterns['prometheus'])

counter = 0

for line in sys.stdin:
    if re.search(pattern, line):
        counter += 1
        if counter % 10000 == 0:
            print(f"[{counter}] {line.rstrip()}")
