#!/usr/bin/env python3
import sys
import re
import argparse

def main():
    parser = argparse.ArgumentParser(description='Filter log messages showing every Nth occurrence')
    parser.add_argument('--interval', '-n', type=int, default=10000,
                       help='Show every Nth message (default: 10000)')
    parser.add_argument('--pattern', '-p',
                       default=r'scanner-prometheus.*(?:write block completed|Head GC|Creating checkpoint|compact blocks|Deleting obsolete block)',
                       help='Regex pattern to match')
    parser.add_argument('--count-only', action='store_true',
                       help='Only show count, not the messages')
    parser.add_argument('--type', '-t', choices=['prometheus', 'grafana', 'background'],
                       default='prometheus',
                       help='Type of logs to filter (default: prometheus)')
    
    args = parser.parse_args()

    # Define patterns for different log types
    patterns = {
        'prometheus': r'scanner-prometheus.*(?:write block completed|Head GC|Creating checkpoint|compact blocks|Deleting obsolete block)',
        'grafana': r'scanner-grafana.*(?:Update check succeeded|All modules healthy|Starting MultiOrg Alertmanager)',
        'background': r'logger=backgroundsvcs\.managerAdapter.*msg="module (?:starting|stopped)" module=\*.*'
    }

    # Use provided pattern or select by type
    if args.pattern != parser.get_default('pattern'):
        pattern_str = args.pattern
    else:
        pattern_str = patterns.get(args.type, patterns['prometheus'])

    counter = 0
    pattern = re.compile(pattern_str)
    
    for line in sys.stdin:
        if pattern.search(line):
            counter += 1
            if counter % args.interval == 0:
                if args.count_only:
                    print(f"Processed {counter} matching messages")
                else:
                    print(f"[{counter}] {line.rstrip()}")

if __name__ == '__main__':
    main()
