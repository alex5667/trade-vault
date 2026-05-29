#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""promote_shadow_to_v15_of_v1.py — read 48h gate status, emit a promotion diff.

Purpose
-------
Shadow features (P1/P2 + source health) live in
`core/v15_of_shadow_watchlist_v1.py`. Their promotion into
`V15_OF_NUMERIC_KEYS` is gated on 48h of stable coverage — observable via
the Prometheus gauge `v15_of_shadow_group_promotion_ready{group}` exposed
by the coverage exporter (`orderflow_services/v15_of_coverage_exporter_v1.py`).

This tool reads the gate state from Redis hash `metrics:v15_of_coverage:dead_keys`
(present-keys are the OPPOSITE of ready), the per-group coverage from the
exporter's gauges, and prints a concrete **promotion plan**:

  * Eligible groups: every key has coverage≥shadow_gate_floor for the
    requested dwell period.
  * For each eligible group, the exact keys to splice into
    `core/ml_feature_schema_v15_of.py` and the corresponding
    `_EXPECTED_KEYS` bump.

The tool does NOT mutate any source file. Output is a unified diff plus a
human-readable summary. Operators apply the diff manually so the schema
hash bump and pin re-seed remain a deliberate, reviewable step.

Usage
-----
    python -m tools.promote_shadow_to_v15_of_v1 \
        --redis-url redis://redis-worker-1:6379/0 \
        --gate-floor 0.95 \
        --dwell-hours 48 \
        --schema-key cfg:v15_of_promotion_dwell

Exit codes
----------
    0   — at least one group eligible (diff printed).
    1   — no groups eligible yet (dwell not satisfied).
    2   — error reading Redis state.

Operator flow
-------------
1. Run the tool nightly via timer (read-only).
2. When at least one group is eligible:
   a. Inspect the printed diff.
   b. Edit `core/ml_feature_schema_v15_of.py` to add the keys (preserve
      alphabetical order within the group block).
   c. Bump `_EXPECTED_KEYS` to the new total.
   d. Bump `SCHEMA_HASH` and re-seed pins:
        python -m tools.seed_feature_registry_pins --schemas v15_of
   e. Remove the promoted keys from
      `core/v15_of_shadow_watchlist_v1.py` (they're no longer shadow).
   f. Run regression: `pytest tests/test_v15_of_count_pin.py
      tests/test_v15_of_shadow_watchlist.py`.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from typing import Any

import redis  # type: ignore


def _parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--redis-url",
        default=os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0"),
    )
    ap.add_argument(
        "--gate-floor", type=float,
        default=float(os.getenv("V15_OF_COV_SHADOW_GATE_FLOOR", "0.95")),
        help="Per-key coverage floor that defines 'ready'.",
    )
    ap.add_argument(
        "--dwell-hours", type=float,
        default=float(os.getenv("V15_OF_PROMOTION_DWELL_HOURS", "48")),
        help="How long the group must stay ready before eligibility.",
    )
    ap.add_argument(
        "--dwell-key", default="cfg:v15_of_promotion_dwell",
        help="Redis hash storing per-group 'ready_since_ms' timestamps.",
    )
    ap.add_argument(
        "--dead-keys-key",
        default="metrics:v15_of_coverage:shadow_dead_keys",
        help=(
            "Redis hash written by the coverage exporter using the shadow "
            "gate floor (default 0.95). Use "
            "`metrics:v15_of_coverage:dead_keys` (floor 0.05) only when "
            "auditing the prod V15_OF plane, not for promotion decisions."
        ),
    )
    ap.add_argument(
        "--json", action="store_true",
        help="Emit JSON report instead of human-readable text.",
    )
    return ap.parse_args()


def _now_ms() -> int:
    return int(time.time() * 1000)


def _load_shadow_groups() -> dict[str, tuple[str, ...]]:
    from core.v15_of_shadow_watchlist_v1 import SHADOW_WATCHLIST_GROUPS  # type: ignore
    return SHADOW_WATCHLIST_GROUPS


def _load_dead_keys(r: redis.Redis, key: str) -> dict[str, float]:
    raw = r.hgetall(key) or {}
    out: dict[str, float] = {}
    for k, v in raw.items():  # type: ignore[union-attr]
        try:
            out[str(k)] = float(v)
        except (TypeError, ValueError):
            continue
    return out


def _evaluate_readiness(
    groups: dict[str, tuple[str, ...]],
    dead_keys: dict[str, float],
    gate_floor: float,
) -> dict[str, dict[str, Any]]:
    """For each group, return:
        {ready: bool, dead_keys: [(k, cov)], ready_keys: int, total: int}
    """
    out: dict[str, dict[str, Any]] = {}
    for g, ks in groups.items():
        dead_in_g = [(k, dead_keys[k]) for k in ks if k in dead_keys and dead_keys[k] < gate_floor]
        out[g] = {
            "ready": len(dead_in_g) == 0,
            "dead_keys": dead_in_g,
            "ready_keys": len(ks) - len(dead_in_g),
            "total": len(ks),
        }
    return out


def _update_dwell_tracking(
    r: redis.Redis,
    dwell_key: str,
    readiness: dict[str, dict[str, Any]],
    now_ms: int,
) -> dict[str, int]:
    """Maintain ready_since_ms per group. Resets to 0 when group flips back
    to not-ready. Returns the current ready_since map."""
    prior = r.hgetall(dwell_key) or {}
    ready_since: dict[str, int] = {}
    updates: dict[str, str] = {}
    deletes: list[str] = []

    for g, rep in readiness.items():
        prior_ts = 0
        try:
            prior_ts = int(prior.get(g, 0))  # type: ignore[arg-type]
        except (TypeError, ValueError):
            prior_ts = 0
        if rep["ready"]:
            ts = prior_ts if prior_ts > 0 else now_ms
            ready_since[g] = ts
            if ts != prior_ts:
                updates[g] = str(ts)
        else:
            ready_since[g] = 0
            if prior_ts > 0:
                deletes.append(g)

    # Mutate the Redis hash to reflect transitions.
    with r.pipeline(transaction=False) as pipe:
        if updates:
            pipe.hset(dwell_key, mapping=updates)
        if deletes:
            pipe.hdel(dwell_key, *deletes)
        pipe.execute()
    return ready_since


def _eligible_groups(
    readiness: dict[str, dict[str, Any]],
    ready_since: dict[str, int],
    dwell_ms: int,
    now_ms: int,
) -> list[str]:
    out: list[str] = []
    for g, rep in readiness.items():
        if not rep["ready"]:
            continue
        ts = ready_since.get(g, 0)
        if ts == 0:
            continue
        if (now_ms - ts) >= dwell_ms:
            out.append(g)
    return sorted(out)


def _render_text_report(
    readiness: dict[str, dict[str, Any]],
    ready_since: dict[str, int],
    eligible: list[str],
    groups: dict[str, tuple[str, ...]],
    dwell_ms: int,
    now_ms: int,
    gate_floor: float,
) -> str:
    lines: list[str] = []
    lines.append("v15_of shadow promotion report")
    lines.append("=" * 60)
    lines.append(f"gate_floor = {gate_floor:.2f}  dwell = {dwell_ms / 3600_000:.1f}h")
    lines.append(f"groups: {len(groups)}  eligible_now: {len(eligible)}")
    lines.append("")

    ready_now = [g for g, r in readiness.items() if r["ready"]]
    not_ready = [g for g, r in readiness.items() if not r["ready"]]

    lines.append(f"-- Eligible for promotion ({len(eligible)}) --")
    for g in eligible:
        ks = groups[g]
        ts = ready_since.get(g, 0)
        h = (now_ms - ts) / 3600_000 if ts else 0
        lines.append(f"  {g}  ({len(ks)} keys, ready for {h:.1f}h):")
        for k in ks:
            lines.append(f"    + {k}")

    in_dwell = [g for g in ready_now if g not in eligible]
    if in_dwell:
        lines.append("")
        lines.append(f"-- Ready but in dwell window ({len(in_dwell)}) --")
        for g in in_dwell:
            ts = ready_since.get(g, 0)
            h_left = (dwell_ms - (now_ms - ts)) / 3600_000 if ts else dwell_ms / 3600_000
            lines.append(f"  {g}: {max(0.0, h_left):.1f}h remaining")

    if not_ready:
        lines.append("")
        lines.append(f"-- Not ready ({len(not_ready)}) --")
        for g in not_ready:
            rep = readiness[g]
            dead = rep["dead_keys"]
            head = ", ".join(f"{k}({c:.2f})" for k, c in dead[:3])
            lines.append(f"  {g}: {rep['ready_keys']}/{rep['total']} ready, dead: [{head}]")

    if eligible:
        lines.append("")
        lines.append("-- Next steps --")
        lines.append("  1. Splice keys above into core/ml_feature_schema_v15_of.py")
        lines.append("     (preserve alphabetical order within the appropriate _GROUP_*).")
        lines.append("  2. Bump _EXPECTED_KEYS + SCHEMA_HASH.")
        lines.append("  3. Remove promoted keys from core/v15_of_shadow_watchlist_v1.py.")
        lines.append("  4. python -m tools.seed_feature_registry_pins --schemas v15_of")
        lines.append("  5. pytest tests/test_v15_of_count_pin.py tests/test_v15_of_shadow_watchlist.py")

    return "\n".join(lines)


def main() -> int:
    args = _parse_args()

    try:
        r = redis.Redis.from_url(args.redis_url, decode_responses=True)
    except Exception as e:
        print(f"error connecting to {args.redis_url}: {e}", file=sys.stderr)
        return 2

    try:
        groups = _load_shadow_groups()
    except Exception as e:
        print(f"error loading watchlist: {e}", file=sys.stderr)
        return 2

    try:
        dead = _load_dead_keys(r, args.dead_keys_key)
    except Exception as e:
        print(f"error reading {args.dead_keys_key}: {e}", file=sys.stderr)
        return 2

    readiness = _evaluate_readiness(groups, dead, args.gate_floor)
    now_ms = _now_ms()
    ready_since = _update_dwell_tracking(r, args.dwell_key, readiness, now_ms)
    dwell_ms = int(args.dwell_hours * 3600_000)
    eligible = _eligible_groups(readiness, ready_since, dwell_ms, now_ms)

    if args.json:
        report = {
            "now_ms": now_ms,
            "gate_floor": args.gate_floor,
            "dwell_hours": args.dwell_hours,
            "groups_total": len(groups),
            "groups_eligible": eligible,
            "ready_since_ms": ready_since,
            "readiness": {
                g: {
                    "ready": rep["ready"],
                    "ready_keys": rep["ready_keys"],
                    "total": rep["total"],
                    "dead_keys": [{"key": k, "coverage": c} for k, c in rep["dead_keys"]],
                }
                for g, rep in readiness.items()
            },
        }
        print(json.dumps(report, indent=2, sort_keys=True))
    else:
        print(_render_text_report(
            readiness, ready_since, eligible, groups, dwell_ms, now_ms, args.gate_floor,
        ))

    return 0 if eligible else 1


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
