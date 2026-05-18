#!/usr/bin/env python3
"""Check v13_of feature coverage in signals:of:inputs stream.

Samples recent entries and reports which of the 278 v13_of schema features
are present and non-zero — split by group.

Run on main host (Redis accessible):
    cd python-worker
    python -m tools.check_v13_feature_coverage [--sample 200] [--schema v13_of]

Exit codes:
    0 — all groups >= 70% non-zero coverage
    1 — some groups below threshold (prints details)
    2 — Redis unreachable or stream empty
"""
from __future__ import annotations

import argparse
import json
import os
import sys


# ── Group ND: loaded via maybe_load_crossasset_v13 into runtime attrs ─────────
_GROUP_ND = [
    "btc_dominance_momentum",
    "oi_weighted_funding",
    "total_market_oi_delta",
    "liq_heatmap_distance_bps",
    "long_short_ratio",
]

# ── Groups NA/NB/NC/NE/NF/NX: from V13RuntimeTracker (warm-up required) ───────
_GROUP_NA = ["garman_klass_vol", "parkinson_vol", "yang_zhang_vol", "vol_of_vol"]
_GROUP_NB = [
    "amihud_illiquidity", "corwin_schultz_spread",
    "hasbrouck_info_share", "depth_resilience_half_life",
]
_GROUP_NC = [
    "pin_estimate", "aggressive_sweep_ratio",
    "lambda_asym", "toxicity_regime_score",
]
_GROUP_NE = ["price_entropy_50", "order_size_gini", "mutual_info_price_volume"]
_GROUP_NF = ["half_life_mean_reversion", "adf_pvalue_50", "zscore_mid_to_vwap"]
_GROUP_NX = [
    "vpin_x_funding", "hurst_x_vol_regime",
    "entropy_x_spread", "depth_resil_x_sweep", "amihud_x_oi_delta",
]

_NEW_GROUPS: dict[str, list[str]] = {
    "NA (vol estimators)":     _GROUP_NA,
    "NB (acad. liquidity)":    _GROUP_NB,
    "NC (flow toxicity)":      _GROUP_NC,
    "ND (cross-asset macro)":  _GROUP_ND,
    "NE (entropy)":            _GROUP_NE,
    "NF (mean reversion)":     _GROUP_NF,
    "NX (interactions)":       _GROUP_NX,
}

MIN_COVERAGE = 0.70
WARM_UP_NOTE_GROUPS = {"NA (vol estimators)", "NB (acad. liquidity)", "NC (flow toxicity)",
                       "NE (entropy)", "NF (mean reversion)", "NX (interactions)"}


def _redis():
    import redis as _r
    url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
    try:
        r = _r.Redis.from_url(url, decode_responses=True, socket_connect_timeout=3)
        r.ping()
        return r
    except Exception as e:
        print(f"ERROR: Redis unreachable at {url!r} — {e}")
        sys.exit(2)


def _parse_payload(data: dict) -> dict:
    raw = data.get("payload") or data.get("data") or ""
    if isinstance(raw, str) and raw.strip().startswith("{"):
        try:
            p = json.loads(raw)
            ind = p.get("indicators")
            return ind if isinstance(ind, dict) else p
        except Exception:
            pass
    ind = data.get("indicators")
    return ind if isinstance(ind, dict) else data


def _is_nonzero(v) -> bool:
    try:
        return float(v) != 0.0
    except Exception:
        return bool(v)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--sample", type=int, default=200)
    ap.add_argument("--schema", default="v13_of")
    ap.add_argument("--stream", default=os.getenv("OF_INPUTS_STREAM", "signals:of:inputs"))
    ap.add_argument("--min-coverage", type=float, default=MIN_COVERAGE)
    args = ap.parse_args()

    r = _redis()
    entries: list = list(r.xrevrange(args.stream, count=args.sample))  # type: ignore[arg-type]
    if not entries:
        print(f"FAIL: stream '{args.stream}' is empty or does not exist")
        sys.exit(2)

    n = len(entries)
    print(f"\nstream:  {args.stream}")
    print(f"schema:  {args.schema}")
    print(f"sampled: {n} entries\n")

    # Optionally load schema feature list from registry
    # Feature registry keys may carry a type-prefix (n:foo, b:foo).
    # Strip prefix for payload lookup since indicators use bare key names.
    schema_keys: list[str] = []
    try:
        sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        from core.feature_registry import get_schema_info
        info = get_schema_info(args.schema)
        raw_names = list(info.feature_names or [])
        # Strip n:/b: prefix (vectorizer adds it; payload uses bare names)
        schema_keys = [k[2:] if k[:2] in ("n:", "b:") else k for k in raw_names]
        print(f"Registry: {len(schema_keys)} schema keys for {args.schema} "
              f"(prefixes stripped for payload lookup)")
    except Exception as e:
        print(f"(registry load failed: {e} — using hardcoded new-group keys only)")

    # Count presence and non-zero for each new group key
    present: dict[str, int] = {}
    nonzero: dict[str, int] = {}
    all_new_keys: list[str] = []
    for keys in _NEW_GROUPS.values():
        all_new_keys.extend(keys)

    # Also count for all schema keys
    schema_nonzero: dict[str, int] = {k: 0 for k in schema_keys}
    schema_present: dict[str, int] = {k: 0 for k in schema_keys}

    for _, data in entries:
        payload = _parse_payload(data)
        for k in all_new_keys:
            v = payload.get(k)
            if v is not None:
                present[k] = present.get(k, 0) + 1
                if _is_nonzero(v):
                    nonzero[k] = nonzero.get(k, 0) + 1
        for k in schema_keys:
            v = payload.get(k)
            if v is not None:
                schema_present[k] += 1
                if _is_nonzero(v):
                    schema_nonzero[k] += 1

    # ── Report: new v13 groups ─────────────────────────────────────────────
    print("=" * 68)
    print("v13_of NEW GROUP COVERAGE (non-zero rate in sampled signals)")
    print("=" * 68)

    any_fail = False
    for group_name, keys in _NEW_GROUPS.items():
        group_pct = [nonzero.get(k, 0) / n for k in keys]
        avg = sum(group_pct) / len(group_pct) if group_pct else 0.0
        ok = avg >= args.min_coverage
        if not ok and group_name in WARM_UP_NOTE_GROUPS:
            note = "(needs warm-up)"
        elif not ok and "ND" in group_name:
            note = "⚠ WIRING BUG? Check of_confirm_engine.py"
        else:
            note = ""
        status = "✓" if ok else "✗"
        print(f"\n[{status}] {group_name}  avg={avg:.1%}  {note}")
        for k, pct in zip(keys, group_pct):
            bar = "▓" * int(pct * 20) + "░" * (20 - int(pct * 20))
            nz_count = nonzero.get(k, 0)
            pr_count = present.get(k, 0)
            print(f"    {k:<38} {bar} {pct:5.1%}  nz={nz_count} pr={pr_count}/{n}")
        if not ok:
            any_fail = True

    # ── Report: top-20 zero-rate schema keys ──────────────────────────────
    if schema_keys:
        print("\n" + "=" * 68)
        print("TOP-20 ZERO-RATE KEYS in schema (most missing non-zero values)")
        print("=" * 68)
        zero_rate = {
            k: 1.0 - (schema_nonzero[k] / n)
            for k in schema_keys
        }
        top20 = sorted(zero_rate.items(), key=lambda x: -x[1])[:20]
        for k, zr in top20:
            nz = schema_nonzero[k]
            pr = schema_present[k]
            print(f"  {k:<42} zero={zr:5.1%}  present={pr}/{n}  nonzero={nz}/{n}")

        # Overall schema coverage
        total_nz = sum(1 for k in schema_keys if schema_nonzero[k] > 0)
        print(f"\nSchema coverage: {total_nz}/{len(schema_keys)} keys have ≥1 non-zero value "
              f"in {n} samples ({total_nz/len(schema_keys):.1%})")

    print()
    if any_fail:
        print("NOT OK: some new-group keys below threshold — see details above")
        sys.exit(1)
    else:
        print("OK: all new-group keys meet coverage threshold")
        sys.exit(0)


if __name__ == "__main__":
    main()
