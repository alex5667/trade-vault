import sys
import argparse
import json
import logging
from core.golden_replay import GoldenReplayRunner

def main():
    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser(description="Golden Replay Runner")
    parser.add_argument("--in", dest="input_file", required=True, help="Input NDJSON/concatenated file with OF_INPUTS_V1")
    parser.add_argument("--out", dest="output_file", required=True, help="Output NDJSON file for results")
    parser.add_argument("--fail-on-mismatch", action="store_true", help="Exit with code 1 if any expectation mismatch found")
    
    args = parser.parse_args()
    
    runner = GoldenReplayRunner()
    results = runner.run_file(args.input_file)
    
    total = 0
    passed = 0
    mismatches = 0
    
    with open(args.output_file, 'w', encoding='utf-8') as f:
        for r in results:
            f.write(json.dumps(r) + "\n")
            total += 1
            if r.get("pass", True) is False: # if check was performed and failed
                mismatches += 1
            else:
                passed += 1
                
    print(f"Replay complete: {total} cases, {mismatches} mismatches.")
    
    if args.fail_on_mismatch and mismatches > 0:
        sys.exit(1)

if __name__ == "__main__":
    main()





















