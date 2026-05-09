#!/usr/bin/env python3
from __future__ import annotations

"""
approve_meta_enforce_cov_suggestion.py

P33: Approver for meta ENFORCE coverage-bucket suggestion.

Adds approver identity into:
  {PREFIX}:approvals:{sid}

ENV
  REDIS_URL (default redis://localhost:6379/0)
  META_ENFORCE_COV_PREFIX (default cfg:suggestions:meta_enforce_cov)
  APPROVER_ID (default manual)
"""


import argparse
import os

try:
    import redis  # type: ignore
except Exception:
    redis = None  # type: ignore


def _redis() -> any:
    if redis is None:
        raise RuntimeError("redis library is not available")
    url = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
    return redis.Redis.from_url(url, decode_responses=True)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--sid", required=True)
    ap.add_argument("--who", default="")
    args = ap.parse_args()

    prefix = os.environ.get("META_ENFORCE_COV_PREFIX", "cfg:suggestions:meta_enforce_cov")
    who = (args.who or os.environ.get("APPROVER_ID", "manual") or "manual").strip()
    sid = str(args.sid).strip()

    r = _redis()
    key = f"{prefix}:approvals:{sid}"
    r.sadd(key, who)
    # keep approvals sets for some time
    r.expire(key, 14 * 24 * 3600)
    print(f"OK approvals+ {who} sid={sid}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
