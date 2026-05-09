import argparse
import collections
import json
import statistics

from core.ndjson_utils import read_concatenated_json


def analyze_trades(input_path: str, output_path: str):
    """
    Analyzes POSITION_CLOSED events from NDJSON/concatenated log.
    Metrics per (scenario, of_confirm_ok):
    - count
    - winrate (R > 0)
    - mean R, median R
    - p10, p50, p90 R
    - R distribution buckets (<= -1, >= 1, >= 2)
    """

    # Aggregation key: (scenario, ok) -> list of R values
    groups = collections.defaultdict(list)

    with open(input_path, encoding='utf-8') as f:
        content = f.read()

    for obj in read_concatenated_json(content):
        if not isinstance(obj, dict): continue

        # We look for "POSITION_CLOSED" or objects that look like trade results
        # Assuming payload has "r_mult" and "indicators"

        r = obj.get("r_mult")
        if r is None: continue

        try:
            r_val = float(r)
        except Exception:
            continue

        ind = obj.get("indicators", {})
        # Extract classification tags
        # If of_confirm_v3 is present, use it. Else fallbacks.
        ofc = ind.get("of_confirm_v3", {})

        scenario = ofc.get("scenario") or ind.get("strong_gate_scn") or "unknown"
        ok = int(ofc.get("ok", -1))
        if ok == -1:
            ok = int(ind.get("of_confirm_ok", -1))

        key = (str(scenario), int(ok))
        groups[key].append(r_val)

    # Calculate stats
    stats = []

    for (scn, ok_flag), r_list in groups.items():
        n = len(r_list)
        if n == 0: continue

        wins = sum(1 for x in r_list if x > 0)
        winrate = wins / n
        mean_r = statistics.mean(r_list)
        median_r = statistics.median(r_list)

        r_list.sort()
        p10 = r_list[int(n * 0.1)]
        p90 = r_list[int(n * 0.9)]

        r_ge_1 = sum(1 for x in r_list if x >= 1.0)
        r_ge_2 = sum(1 for x in r_list if x >= 2.0)
        r_le_neg1 = sum(1 for x in r_list if x <= -1.0)

        stats.append({
            "scenario": scn,
            "of_confirm_ok": ok_flag,
            "n": n,
            "winrate": float(f"{winrate:.3f}"),
            "mean_r": float(f"{mean_r:.3f}"),
            "median_r": float(f"{median_r:.3f}"),
            "p10_r": float(f"{p10:.3f}"),
            "p90_r": float(f"{p90:.3f}"),
            "frac_R_ge_1": float(f"{r_ge_1/n:.3f}"),
            "frac_R_ge_2": float(f"{r_ge_2/n:.3f}"),
            "frac_R_le_neg1": float(f"{r_le_neg1/n:.3f}"),
        })

    # Sort by N desc
    stats.sort(key=lambda x: x["n"], reverse=True)

    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump({"stats": stats}, f, indent=2)

    print(f"Analysis complete. Found {len(stats)} groups. Saved to {output_path}")

def main():
    parser = argparse.ArgumentParser(description="Analyze Trade Closed NDJSON")
    parser.add_argument("--in", dest="input_file", required=True)
    parser.add_argument("--out", dest="output_file", required=True)

    args = parser.parse_args()
    analyze_trades(args.input_file, args.output_file)

if __name__ == "__main__":
    main()





















