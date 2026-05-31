"""
preflight_reader_flip.py — go/no-go check before flipping
ADAPTIVE_TTL_READ_ENABLED or ENSEMBLE_WEIGHTS_READ_ENABLED to 1.

Runs 4 checks per reader; exits 0 only if ALL pass.

Usage:
    python -m tools.preflight_reader_flip --reader=adaptive_ttl
    python -m tools.preflight_reader_flip --reader=ensemble
    python -m tools.preflight_reader_flip --reader=both --json

ENV:
    REDIS_URL                  Redis URL (worker-1)
    PREFLIGHT_MAX_AGE_MIN      max snapshot age (default 120)
    PREFLIGHT_MIN_RECS         min recs in adaptive_ttl snapshot (default 1)
    PREFLIGHT_MIN_SYMBOLS      min symbols for ensemble (default 1)
    PREFLIGHT_MIN_SOURCES      min sources per ensemble HASH (default 2)
    PREFLIGHT_WEIGHT_SUM_TOL   tolerance for |sum(weights)-1| (default 0.01)
    METRICS_HOST               for Prometheus probes (default localhost)
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, asdict
from typing import Any

import redis  # type: ignore


_ADAPTIVE_KEY = "adaptive_ttl:state"
_ENSEMBLE_PATTERN = "ensemble:weights:*"


@dataclass
class Check:
    name: str
    passed: bool
    detail: str = ""


@dataclass
class Report:
    reader: str
    passed: bool
    checks: list[Check]


# ─── Helpers ─────────────────────────────────────────────────────────────────


def _redis() -> Any:
    url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
    return redis.from_url(url, decode_responses=True)


def _fetch_metrics(port: int) -> dict[str, float]:
    """Fetch and parse Prometheus metrics from localhost:<port>/metrics."""
    host = os.getenv("METRICS_HOST", "localhost")
    url = f"http://{host}:{port}/metrics"
    out: dict[str, float] = {}
    try:
        with urllib.request.urlopen(url, timeout=3) as resp:
            for line in resp.read().decode("utf-8", errors="ignore").splitlines():
                if line.startswith("#") or not line.strip():
                    continue
                parts = line.rsplit(" ", 1)
                if len(parts) == 2:
                    try:
                        out[parts[0]] = float(parts[1])
                    except ValueError:
                        continue
    except (urllib.error.URLError, OSError):
        pass
    return out


# ─── Reader: adaptive_ttl ────────────────────────────────────────────────────


def check_adaptive_ttl() -> Report:
    rc = _redis()
    checks: list[Check] = []

    # 1. key exists
    try:
        exists = bool(rc.exists(_ADAPTIVE_KEY))
    except Exception as e:
        return Report("adaptive_ttl", False, [Check("redis_ping", False, str(e))])
    checks.append(Check("key_exists", exists, f"{_ADAPTIVE_KEY}={exists}"))
    if not exists:
        return Report("adaptive_ttl", False, checks)

    # 2. freshness + structure
    raw = rc.get(_ADAPTIVE_KEY) or "{}"
    try:
        payload = json.loads(raw)
    except Exception as e:
        checks.append(Check("payload_json", False, str(e)))
        return Report("adaptive_ttl", False, checks)

    now_ms = int(time.time() * 1000)
    gen_at = int(payload.get("generated_at_ms") or 0)
    age_min = (now_ms - gen_at) / 60_000 if gen_at else 1e9
    max_age = float(os.getenv("PREFLIGHT_MAX_AGE_MIN", "120"))
    checks.append(
        Check(
            "freshness",
            age_min <= max_age,
            f"age_min={age_min:.1f} max={max_age:.1f}",
        )
    )

    # 3. recs sanity (non-empty + no degenerate barriers)
    min_recs = int(os.getenv("PREFLIGHT_MIN_RECS", "1"))
    recs = payload.get("recs") or []
    n = len(recs)
    degen = sum(
        1
        for r in recs
        if (r.get("tp_r") or 0) <= 0
        or (r.get("sl_r") or 0) <= 0
        or (r.get("n") or 0) < 30
    )
    checks.append(
        Check(
            "recs_sanity",
            n >= min_recs and degen == 0,
            f"n={n} degen={degen} min={min_recs}",
        )
    )

    # 4. publisher metrics healthy
    metrics = _fetch_metrics(9915)
    published = sum(
        v for k, v in metrics.items()
        if k.startswith('adaptive_ttl_cycle_total{') and 'status="published"' in k
    )
    errors = sum(
        v for k, v in metrics.items()
        if k.startswith('adaptive_ttl_cycle_total{') and 'status="error"' in k
    )
    recs_g = metrics.get("adaptive_ttl_recs_total", 0.0)
    checks.append(
        Check(
            "publisher_health",
            (published > 0 and recs_g > 0) or not metrics,
            f"published={published:.0f} errors={errors:.0f} recs_g={recs_g:.0f} "
            f"metrics_reachable={bool(metrics)}",
        )
    )

    passed = all(c.passed for c in checks)
    return Report("adaptive_ttl", passed, checks)


# ─── Reader: ensemble_weights ────────────────────────────────────────────────


def check_ensemble_weights() -> Report:
    rc = _redis()
    checks: list[Check] = []

    # 1. at least one HASH exists
    try:
        keys = list(rc.scan_iter(match=_ENSEMBLE_PATTERN, count=100))
    except Exception as e:
        return Report("ensemble", False, [Check("redis_ping", False, str(e))])

    min_symbols = int(os.getenv("PREFLIGHT_MIN_SYMBOLS", "1"))
    checks.append(
        Check(
            "symbols_present",
            len(keys) >= min_symbols,
            f"symbols={len(keys)} min={min_symbols}",
        )
    )
    if not keys:
        return Report("ensemble", False, checks)

    # 2. each HASH has weights summing to ~1 with min sources
    min_sources = int(os.getenv("PREFLIGHT_MIN_SOURCES", "2"))
    tol = float(os.getenv("PREFLIGHT_WEIGHT_SUM_TOL", "0.01"))
    bad_hashes: list[str] = []
    for key in keys:
        mapping = rc.hgetall(key) or {}
        try:
            weights = {k: float(v) for k, v in mapping.items()}
        except Exception:
            bad_hashes.append(f"{key}:parse")
            continue
        n_src = sum(1 for w in weights.values() if w > 0)
        s = sum(weights.values())
        if n_src < min_sources or abs(s - 1.0) > tol:
            bad_hashes.append(f"{key}:sources={n_src},sum={s:.3f}")
    checks.append(
        Check(
            "weights_sanity",
            not bad_hashes,
            f"bad={len(bad_hashes)} sample={bad_hashes[:3]}",
        )
    )

    # 3. TTLs set (not -1 = persistent forever, not -2 = missing)
    bad_ttl: list[str] = []
    for key in keys[:20]:  # sample first 20
        ttl = rc.ttl(key)
        if ttl < 0:
            bad_ttl.append(f"{key}:ttl={ttl}")
    checks.append(
        Check(
            "ttl_set",
            not bad_ttl,
            f"bad={len(bad_ttl)} sample={bad_ttl[:3]}",
        )
    )

    # 4. publisher metrics healthy
    metrics = _fetch_metrics(9916)
    syms_g = metrics.get("ensemble_weights_symbols", 0.0)
    published = sum(
        v for k, v in metrics.items()
        if k.startswith('ensemble_weights_cycle_total{') and 'status="published"' in k
    )
    checks.append(
        Check(
            "publisher_health",
            (published > 0 and syms_g > 0) or not metrics,
            f"published={published:.0f} syms_g={syms_g:.0f} "
            f"metrics_reachable={bool(metrics)}",
        )
    )

    passed = all(c.passed for c in checks)
    return Report("ensemble", passed, checks)


# ─── CLI ─────────────────────────────────────────────────────────────────────


def _print_human(reports: list[Report]) -> None:
    for rep in reports:
        marker = "✅" if rep.passed else "❌"
        print(f"\n{marker} {rep.reader}: {'PASS' if rep.passed else 'FAIL'}")
        for c in rep.checks:
            m = "  ✓" if c.passed else "  ✗"
            print(f"{m} {c.name:20s} {c.detail}")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--reader",
        choices=("adaptive_ttl", "ensemble", "both"),
        default="both",
    )
    ap.add_argument("--json", action="store_true", help="emit JSON instead of human-readable")
    args = ap.parse_args()

    reports: list[Report] = []
    if args.reader in ("adaptive_ttl", "both"):
        reports.append(check_adaptive_ttl())
    if args.reader in ("ensemble", "both"):
        reports.append(check_ensemble_weights())

    if args.json:
        print(json.dumps([asdict(r) for r in reports], indent=2))
    else:
        _print_human(reports)

    return 0 if all(r.passed for r in reports) else 1


if __name__ == "__main__":
    sys.exit(main())
