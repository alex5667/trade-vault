"""v15_of_coverage_report.py — per-feature coverage audit for v15_of schema.

Reads N samples from signals:of:inputs Redis stream and computes for each
of the 531 v15_of keys:

  coverage      fraction of samples where key is present and non-NaN
  missing_rate  1 - coverage (key absent from indicators dict)
  stale_rate    fraction where value is NaN (stale upstream sentinel)
  zero_rate     fraction of non-missing non-stale samples where value == 0.0
  source_group  schema group the key belongs to (from ml_feature_schema_v15_of)

Outputs JSON report + Prometheus-compatible .prom text file.
Features below --min-coverage (default 0.80) are flagged.

Usage:
    python -m tools.v15_of_coverage_report \\
        --samples 2000 \\
        --out /tmp/v15_coverage.json \\
        --prom /tmp/v15_coverage.prom

Key outputs:
    /tmp/v15_coverage.json  — full per-feature report + summary
    /tmp/v15_coverage.prom  — Prometheus metrics for node_exporter textfile
    stdout                  — human-readable summary of low-coverage features
"""
from __future__ import annotations

import argparse
import json
import math
import os
import sys
import time
from collections import defaultdict
from typing import Any


# ── Source group mapping ───────────────────────────────────────────────────────

def _build_source_map() -> dict[str, str]:
    """Map each v15_of key to its schema group name."""
    try:
        import importlib
        mod = importlib.import_module("core.ml_feature_schema_v15_of")
    except ImportError:
        return {}

    group_map: dict[str, str] = {}
    group_vars = {
        "_GROUP_P82_TIME_CYC": "p82_time_cyc",
        "_GROUP_P83_TAKER_FORCE": "p83_taker_force",
        "_GROUP_P84_HAWKES_VPIN": "p84_hawkes_vpin",
        "_GROUP_P85_XV_CROSS_VENUE": "p85_cross_venue",
        "_GROUP_P85_XVI_COINGECKO": "p85_coingecko",
        "_GROUP_P85_XVII_DERIBIT_EXT": "p85_deribit_ext",
        "_GROUP_P85_XVIII_DEFILLAMA": "p85_defillama",
        "_GROUP_P1_DERIBIT_TERM": "p1_deribit_term",
        "_GROUP_P1_BREADTH_5M": "p1_breadth",
        "_GROUP_P1_RELSTR": "p1_relstr",
        "_GROUP_P2_BYBIT": "p2_bybit",
        "_GROUP_P3_FG_DELTA": "p3_fear_greed",
        "_GROUP_P3_COINPAPRIKA": "p3_coinpaprika",
        "_GROUP_P3_COINMARKETCAP": "p3_coinmarketcap",
        "_GROUP_P3_DEFILLAMA_EXT": "p3_defillama_ext",
        "_GROUP_DERIV_BASE": "deriv_base",
        "_GROUP_PIT_PRIORS": "pit_priors",
        "_GROUP_MACRO_CAL": "macro_cal",
        "_GROUP_SECTOR_AGG": "sector_agg",
        "_GROUP_LIQMAP_ALIAS": "liqmap_alias",
        "_GROUP_LIQMAP_GATE_DETAIL": "liqmap_gate",
        "_GROUP_LEADER_FLAGS": "leader_flags",
        "_GROUP_BREADTH_RET": "breadth_ret",
        "_GROUP_SIGNAL_DQ_CONFIRM": "signal_dq",
        "_GROUP_REGIME_CONFIRM_BINARY": "regime_confirm",
        "_GROUP_TICK_SIGNAL_FLAGS": "tick_flags",
        "_GROUP_STREAM_GATE_FLOW": "stream_gate",
        "_GROUP_GATE_FLAGS": "gate_flags",
    }
    for var, label in group_vars.items():
        keys = getattr(mod, var, [])
        for k in keys:
            if k not in group_map:  # first match wins (some keys appear in multiple groups)
                group_map[k] = label

    # Keys from v14_of base
    try:
        from core.ml_feature_schema_v14_of import V14_OF_NUMERIC_KEYS
        from core.ml_feature_schema_v15_of import V15_OF_NUMERIC_KEYS
        for k in V15_OF_NUMERIC_KEYS:
            if k not in group_map and k in V14_OF_NUMERIC_KEYS:
                group_map[k] = "v14_base"
    except Exception:
        pass

    return group_map


# ── Redis reader ───────────────────────────────────────────────────────────────

def _read_samples_from_redis(
    redis_url: str,
    stream_key: str,
    n_samples: int,
) -> list[dict[str, Any]]:
    """Read up to n_samples entries from the signals:of:inputs stream."""
    try:
        import redis
    except ImportError:
        print("ERROR: redis-py not installed; install with 'pip install redis'", file=sys.stderr)
        return []

    r = redis.from_url(redis_url, decode_responses=True)
    entries = []
    try:
        # Read from tail to get most recent samples
        raw = r.xrevrange(stream_key, count=n_samples)
        entries = list(reversed(raw))  # chronological order
    except Exception as e:
        print(f"ERROR reading {stream_key}: {e}", file=sys.stderr)
        return []
    print(f"Read {len(entries)} entries from {stream_key}", flush=True)
    return entries


def _parse_indicators(entry_fields: dict[str, Any]) -> dict[str, Any] | None:
    """Extract indicators dict from a stream entry."""
    # Try structured JSON field first
    for key in ("indicators", "data", "payload"):
        raw = entry_fields.get(key)
        if raw:
            try:
                obj = json.loads(raw) if isinstance(raw, str) else raw
                if isinstance(obj, dict):
                    inds = obj.get("indicators") or obj
                    if isinstance(inds, dict) and inds:
                        return inds
            except Exception:
                pass
    # Flat field scan
    flat = {k: v for k, v in entry_fields.items() if not k.startswith("_")}
    return flat if flat else None


# ── Coverage computation ───────────────────────────────────────────────────────

def compute_coverage(
    samples: list[dict[str, Any]],
    schema_keys: list[str],
    *,
    min_coverage: float = 0.80,
) -> dict[str, dict]:
    """Compute per-key metrics over sample set."""
    n = len(samples)
    if n == 0:
        return {}

    counts: dict[str, dict[str, int]] = {
        k: {"present": 0, "stale": 0, "zero": 0, "nonzero": 0}
        for k in schema_keys
    }

    for inds in samples:
        for k in schema_keys:
            if k not in inds:
                continue
            val = inds[k]
            # Stale check
            try:
                if isinstance(val, float) and math.isnan(val):
                    counts[k]["stale"] += 1
                    continue
            except Exception:
                pass
            counts[k]["present"] += 1
            try:
                fv = float(val)
                if fv == 0.0:
                    counts[k]["zero"] += 1
                else:
                    counts[k]["nonzero"] += 1
            except (TypeError, ValueError):
                counts[k]["nonzero"] += 1

    result: dict[str, dict] = {}
    for k in schema_keys:
        c = counts[k]
        present = c["present"]
        stale = c["stale"]
        non_missing = present + stale
        missing = n - non_missing
        coverage = present / n
        stale_rate = stale / n
        missing_rate = missing / n
        non_stale_present = present
        zero_rate = c["zero"] / non_stale_present if non_stale_present > 0 else 0.0
        result[k] = {
            "coverage": round(coverage, 4),
            "missing_rate": round(missing_rate, 4),
            "stale_rate": round(stale_rate, 4),
            "zero_rate": round(zero_rate, 4),
            "n_present": present,
            "n_stale": stale,
            "n_missing": missing,
            "n_total": n,
            "below_threshold": coverage < min_coverage,
        }

    return result


# ── Summary ────────────────────────────────────────────────────────────────────

def print_summary(
    coverage_report: dict[str, dict],
    source_map: dict[str, str],
    min_coverage: float,
) -> None:
    below = [(k, v) for k, v in coverage_report.items() if v["below_threshold"]]
    above = [(k, v) for k, v in coverage_report.items() if not v["below_threshold"]]

    print(f"\n{'='*70}")
    print(f"v15_of Coverage Report  — {len(coverage_report)} features total")
    print(f"  Threshold: min_coverage={min_coverage:.0%}")
    print(f"  Above threshold: {len(above)}")
    print(f"  Below threshold (training-excluded): {len(below)}")
    print(f"{'='*70}")

    if below:
        print(f"\n{'─'*70}")
        print(f"LOW-COVERAGE FEATURES (< {min_coverage:.0%})  [excluded from training]")
        print(f"{'─'*70}")
        # Group by source
        by_source: dict[str, list] = defaultdict(list)
        for k, v in sorted(below, key=lambda x: x[1]["coverage"]):
            src = source_map.get(k, "unknown")
            by_source[src].append((k, v))

        for src in sorted(by_source):
            print(f"\n  [{src}]")
            for k, v in by_source[src]:
                print(
                    f"    {k:<50} cov={v['coverage']:.1%}  "
                    f"miss={v['missing_rate']:.1%}  "
                    f"stale={v['stale_rate']:.1%}  "
                    f"zero={v['zero_rate']:.1%}"
                )

    # Group summary by source
    print(f"\n{'─'*70}")
    print("SOURCE GROUP SUMMARY")
    print(f"{'─'*70}")
    group_stats: dict[str, dict] = defaultdict(lambda: {"total": 0, "above": 0, "below": 0})
    for k, v in coverage_report.items():
        src = source_map.get(k, "unknown")
        group_stats[src]["total"] += 1
        if v["below_threshold"]:
            group_stats[src]["below"] += 1
        else:
            group_stats[src]["above"] += 1

    for src in sorted(group_stats):
        s = group_stats[src]
        pct_ok = s["above"] / s["total"] * 100 if s["total"] else 0
        status = "✓" if s["below"] == 0 else "✗"
        print(f"  {status} {src:<30} {s['above']:3}/{s['total']:3} ok  ({pct_ok:.0f}%)")

    print()


# ── Prometheus text format output ──────────────────────────────────────────────

def write_prom(
    coverage_report: dict[str, dict],
    source_map: dict[str, str],
    out_path: str,
    ts_ms: int,
) -> None:
    lines = [
        "# HELP v15_of_feature_coverage Fraction of signals where feature is non-NaN",
        "# TYPE v15_of_feature_coverage gauge",
        "# HELP v15_of_feature_stale_rate Fraction of signals with NaN (stale sentinel)",
        "# TYPE v15_of_feature_stale_rate gauge",
        "# HELP v15_of_feature_zero_rate Fraction of non-missing samples where value==0.0",
        "# TYPE v15_of_feature_zero_rate gauge",
        "# HELP v15_of_feature_missing_rate Fraction of signals where key is absent",
        "# TYPE v15_of_feature_missing_rate gauge",
    ]

    for k in sorted(coverage_report):
        v = coverage_report[k]
        src = source_map.get(k, "unknown")
        lbl = f'feature="{k}",group="{src}"'
        lines.append(f"v15_of_feature_coverage{{{lbl}}} {v['coverage']}")
        lines.append(f"v15_of_feature_stale_rate{{{lbl}}} {v['stale_rate']}")
        lines.append(f"v15_of_feature_zero_rate{{{lbl}}} {v['zero_rate']}")
        lines.append(f"v15_of_feature_missing_rate{{{lbl}}} {v['missing_rate']}")

    # Summary gauge
    below_count = sum(1 for v in coverage_report.values() if v["below_threshold"])
    lines += [
        "# HELP v15_of_features_below_threshold Count of features with coverage < threshold",
        "# TYPE v15_of_features_below_threshold gauge",
        f"v15_of_features_below_threshold {below_count}",
        f"v15_of_coverage_report_ts_ms {ts_ms}",
    ]

    with open(out_path, "w") as f:
        f.write("\n".join(lines) + "\n")
    print(f"Prometheus metrics written → {out_path}")


# ── Main ───────────────────────────────────────────────────────────────────────

def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="v15_of feature coverage report")
    ap.add_argument(
        "--redis-url",
        default=os.getenv("REDIS_WORKER_1_URL", os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")),
        help="Redis URL for signals:of:inputs stream",
    )
    ap.add_argument(
        "--stream",
        default=os.getenv("OF_INPUTS_STREAM", "signals:of:inputs"),
        help="Redis stream key",
    )
    ap.add_argument(
        "--samples", type=int, default=2000,
        help="Number of stream entries to sample (default 2000)",
    )
    ap.add_argument(
        "--min-coverage", type=float, default=0.80,
        help="Coverage threshold; features below this are flagged (default 0.80)",
    )
    ap.add_argument(
        "--out", default="/tmp/v15_coverage.json",
        help="Output JSON report path",
    )
    ap.add_argument(
        "--prom", default="",
        help="Optional Prometheus .prom output path",
    )
    ap.add_argument(
        "--schema-ver", default="v15_of",
        help="Feature schema version to audit (default v15_of)",
    )
    args = ap.parse_args(argv)

    # Load schema keys
    try:
        from core.feature_registry import get_schema_info
        schema_info = get_schema_info(args.schema_ver)
        schema_keys = [n[2:] for n in schema_info.feature_names if n.startswith("n:")]
        print(f"Schema {args.schema_ver}: {len(schema_keys)} numeric keys")
    except Exception as e:
        print(f"ERROR loading schema {args.schema_ver}: {e}", file=sys.stderr)
        return 1

    # Build source map
    source_map = _build_source_map()

    # Read samples
    raw_entries = _read_samples_from_redis(args.redis_url, args.stream, args.samples)
    if not raw_entries:
        print(f"ERROR: no samples read from {args.stream}", file=sys.stderr)
        return 2

    # Parse indicators
    samples: list[dict[str, Any]] = []
    for _entry_id, fields in raw_entries:
        inds = _parse_indicators(fields)
        if inds:
            samples.append(inds)

    if not samples:
        print("ERROR: could not parse any indicators from stream entries", file=sys.stderr)
        return 3

    print(f"Parsed {len(samples)} samples with indicators")

    # Compute coverage
    t0 = time.time()
    coverage_report = compute_coverage(samples, schema_keys, min_coverage=args.min_coverage)
    print(f"Coverage computed in {time.time()-t0:.2f}s")

    # Attach source group
    for k, v in coverage_report.items():
        v["source_group"] = source_map.get(k, "unknown")

    # Build report
    below_count = sum(1 for v in coverage_report.values() if v["below_threshold"])
    above_count = len(coverage_report) - below_count
    ts_ms = int(time.time() * 1000)

    report = {
        "schema_ver": args.schema_ver,
        "n_keys": len(schema_keys),
        "n_samples": len(samples),
        "min_coverage_threshold": args.min_coverage,
        "generated_at_ms": ts_ms,
        "summary": {
            "above_threshold": above_count,
            "below_threshold": below_count,
            "pct_ok": round(above_count / len(schema_keys) * 100, 1),
        },
        "features": coverage_report,
    }

    # Write JSON
    os.makedirs(os.path.dirname(os.path.abspath(args.out)), exist_ok=True)
    with open(args.out, "w") as f:
        json.dump(report, f, indent=2)
    print(f"Coverage report written → {args.out}")

    # Write Prometheus
    if args.prom:
        write_prom(coverage_report, source_map, args.prom, ts_ms)

    # Print human-readable summary
    print_summary(coverage_report, source_map, args.min_coverage)

    return 0 if below_count == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
