#!/usr/bin/env python3
import sys
import re
import argparse

def main():
    parser = argparse.ArgumentParser(description='Filter signal tracker zero trades log messages')
    parser.add_argument('--interval', '-n', type=int, default=1000, 
                       help='Show every Nth message (default: 1000)')
    parser.add_argument('--pattern', '-p', 
                       default=r'scanner-signal-tracker.*\|.*\|.*PeriodicReporter.*\|.*(?:⚠️ Нет сделок|Итого собрано 0 сделок)',
                       help='Regex pattern to match')
    parser.add_argument('--count-only', action='store_true',
                       help='Only show count, not the messages')
    parser.add_argument('--symbols', nargs='*', 
                       help='Filter by specific symbols (e.g., BTCUSDT ETHUSDT)')
    
    args = parser.parse_args()
    
    counter = 0
    pattern = re.compile(args.pattern)
    
    for line in sys.stdin:
        if pattern.search(line):
            # Additional symbol filtering if specified
            if args.symbols:
                symbol_found = False
                for symbol in args.symbols:
                    if symbol.upper() in line.upper():
                        symbol_found = True
                        break
                if not symbol_found:
                    continue
            
            counter += 1
            if counter % args.interval == 0:
                if args.count_only:
                    print(f"Processed {counter} zero-trade messages")
                else:
                    print(f"[{counter}] {line.rstrip()}")

if __name__ == '__main__':
    main()
