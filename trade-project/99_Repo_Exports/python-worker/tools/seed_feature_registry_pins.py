#!/usr/bin/env python3
"""Seed feature_registry contract-check pins in Redis for active schemas.

Wraps `orderflow_services.feature_registry_contract_check_v1 --seed-pin 1`
for every schema in `--schemas` (default: v13_of prod + v14_of canary), so the
P94 SRE monitor can detect accidental schema/feature_cols drift on either.

Operational use (run once after deploy of a new schema version):

    python -m tools.seed_feature_registry_pins
    # or pin a specific schema only:
    python -m tools.seed_feature_registry_pins --schemas v13_of
    # or pin under a custom prefix:
    python -m tools.seed_feature_registry_pins --pin-key-prefix cfg:feature_registry:

For a new schema (e.g. v15_of shadow rollout), opt-in explicitly:

    python -m tools.seed_feature_registry_pins --schemas v15_of

Each schema gets its own pin key: ``<prefix><schema_ver>``, e.g.
``cfg:feature_registry:edge_stack:v13_of``. Older single-key callers
(``cfg:feature_registry:edge_stack``) still work — pass ``--pin-key`` to
override the per-schema key construction entirely.
"""
from __future__ import annotations

import argparse
import os
import subprocess
import sys


# Current prod+canary default. On v14→v15 transition:
#   change to ("v14_of", "v15_of") and run `make seed-registry-pins`
# Or use `make transition-to-v15of` for the full guided procedure.
_DEFAULT_SCHEMAS = ("v14_of", "v15_of")
_DEFAULT_PREFIX = "cfg:feature_registry:edge_stack:"


def _run_one(schema_ver: str, pin_key: str, redis_url: str) -> int:
    cmd = [
        sys.executable,
        "-m", "orderflow_services.feature_registry_contract_check_v1",
        "--schema-ver", schema_ver,
        "--pin-key", pin_key,
        "--redis-url", redis_url,
        "--seed-pin", "1",
        "--require-pins", "0",
    ]
    print(f"[seed] {schema_ver} → {pin_key}")
    return subprocess.call(cmd)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--schemas", nargs="+", default=list(_DEFAULT_SCHEMAS))
    ap.add_argument("--pin-key-prefix", default=_DEFAULT_PREFIX)
    ap.add_argument(
        "--pin-key", default=None,
        help="If set, used as the single pin key for every schema (legacy mode).",
    )
    ap.add_argument(
        "--redis-url",
        default=os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0"),
    )
    args = ap.parse_args()

    worst = 0
    for s in args.schemas:
        pin_key = args.pin_key or f"{args.pin_key_prefix}{s}"
        rc = _run_one(s, pin_key, args.redis_url)
        if rc > worst:
            worst = rc
    return worst


if __name__ == "__main__":
    raise SystemExit(main())
