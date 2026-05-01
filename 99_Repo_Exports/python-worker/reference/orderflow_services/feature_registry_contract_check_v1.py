#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations
"""P94 — Feature Registry contract smoke-check (v1)

Goal
-----
Detect accidental changes in Feature Registry outputs (schema_hash / feature_cols_hash)
without an explicit schema version bump.

Mechanics
---------
- Computes:
    - schema_hash  : sha256(feature_names list) from core.feature_registry.get_schema_info()
    - feature_cols_hash : sha256(feature_cols list) from core.feature_registry.get_edge_stack_feature_spec()
- Compares them to *pinned* expected values stored in Redis hash (cfg key).
- Writes the last status to Redis hash (metrics key) for Prometheus exporter/alerts.

Exit codes
----------
  0  OK
  2  ALERT (pins missing or mismatch)
  1  ERROR (exception, redis unavailable, registry import failure)

ENV (defaults)
--------------
  REDIS_URL                                  redis://redis-worker-1:6379/0
  FEATURE_REGISTRY_PIN_KEY                   cfg:feature_registry:edge_stack
  FEATURE_REGISTRY_CONTRACT_METRICS_KEY      metrics:feature_registry_contract:last

  FEATURE_SCHEMA_VER                         v4_of
  FEATURE_MAX_NUMERIC                        128
  FEATURE_SCENARIO_PREFIX                    bucket:
  FEATURE_INCLUDE_TIME_ONEHOT                (auto, registry default)
  FEATURE_INCLUDE_DIRECTION                  1
  FEATURE_INCLUDE_SCENARIO                   1
  FEATURE_STRICT_FEATURE_COLS                0
  FEATURE_FORBID_SCENARIO_V4_ONEHOT          1

  FEATURE_REGISTRY_REQUIRE_PINS              1

CLI
---
  python -m orderflow_services.feature_registry_contract_check_v1
  python orderflow_services/feature_registry_contract_check_v1.py

Optional:
  --seed-pin 1   (one-time: writes current hashes into cfg hash)
"""

from utils.time_utils import get_ny_time_millis

import argparse
import json
import logging
import os
import sys
import time
from dataclasses import asdict
from typing import Any, Dict, Optional, Tuple

try:
    import redis  # type: ignore
except Exception:  # pragma: no cover
    redis = None

logger = logging.getLogger("feature_registry_contract_check")


def _now_ms() -> int:
    return get_ny_time_millis()


def _ensure_import_paths() -> None:
    """Make core.feature_registry importable when running from different working dirs."""
    here = os.path.abspath(os.path.dirname(__file__))
    repo_root = os.path.abspath(os.path.join(here, ".."))
    tick_root = os.path.join(repo_root, "tick_flow_full")

    # Prefer tick_flow_full on sys.path so `import core.*` resolves correctly.
    if os.path.isdir(tick_root) and tick_root not in sys.path:
        sys.path.insert(0, tick_root)
    if repo_root not in sys.path:
        sys.path.insert(0, repo_root)


def _as_int(v: Any, default: int = 0) -> int:
    try:
        return int(float(v))
    except Exception:
        return int(default)


def _as_str(v: Any, default: str = "") -> str:
    try:
        s = "" if v is None else str(v)
        return s
    except Exception:
        return default


def _connect_redis(url: str):
    if redis is None:
        raise RuntimeError("redis dependency is missing")
    return redis.Redis.from_url(url, decode_responses=True)


def _hgetall_safe(r, key: str) -> Dict[str, str]:
    try:
        d = r.hgetall(key) or {}
        if not isinstance(d, dict):
            return {}
        return {str(k): str(v) for k, v in d.items()}
    except Exception:
        return {}


def _hset_safe(r, key: str, mapping: Dict[str, Any]) -> None:
    m = {str(k): str(v) for k, v in (mapping or {}).items()}
    if not m:
        return
    try:
        r.hset(key, mapping=m)
    except Exception:
        # best-effort only
        pass


def _compute_current(schema_ver: str,
                     *,
                     max_numeric: int,
                     scenario_prefix: str,
                     include_time_onehot: Optional[bool],
                     include_direction: bool,
                     include_scenario: bool,
                     strict_feature_cols: bool,
                     forbid_scenario_v4_onehot: bool) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    _ensure_import_paths()

    from core import feature_registry as fr  # type: ignore

    info = fr.get_schema_info(schema_ver)
    spec = fr.get_edge_stack_feature_spec(
        schema_ver,
        max_numeric=int(max_numeric),
        scenario_prefix=str(scenario_prefix),
        include_time_onehot=include_time_onehot,
        include_direction=bool(include_direction),
        include_scenario=bool(include_scenario),
        strict_feature_cols=bool(strict_feature_cols),
        forbid_scenario_v4_onehot=bool(forbid_scenario_v4_onehot),
    )

    current = {
        "schema_ver": str(info.ver),
        "schema_hash": str(info.schema_hash),
        "n_schema_features": int(len(info.feature_names or [])),
        "feature_cols_hash": str(spec.feature_cols_hash),
        "n_feature_cols": int(len(spec.feature_cols or [])),
        "params": {
            "max_numeric": int(max_numeric),
            "scenario_prefix": str(scenario_prefix),
            "include_time_onehot": None if include_time_onehot is None else (1 if include_time_onehot else 0),
            "include_direction": 1 if include_direction else 0,
            "include_scenario": 1 if include_scenario else 0,
            "strict_feature_cols": 1 if strict_feature_cols else 0,
            "forbid_scenario_v4_onehot": 1 if forbid_scenario_v4_onehot else 0,
        }
    }

    # Provide also the raw dataclasses payload for debugging (bounded)
    dbg = {
        "schema_info": asdict(info),
        "feature_spec": asdict(spec),
    }
    return current, dbg


def _compare_pins(pins: Dict[str, str], current: Dict[str, Any]) -> Tuple[bool, Dict[str, Any]]:
    """Return (ok, details)."""
    want_ver = _as_str(pins.get("schema_ver") or pins.get("feature_schema_ver") or "").strip()
    want_schema_hash = _as_str(pins.get("schema_hash") or "").strip()
    want_cols_hash = _as_str(pins.get("feature_cols_hash") or "").strip()

    got_ver = _as_str(current.get("schema_ver") or "").strip()
    got_schema_hash = _as_str(current.get("schema_hash") or "").strip()
    got_cols_hash = _as_str(current.get("feature_cols_hash") or "").strip()

    mismatch_ver = 1 if (want_ver and got_ver and want_ver != got_ver) else 0
    mismatch_schema = 1 if (want_schema_hash and got_schema_hash and want_schema_hash != got_schema_hash) else 0
    mismatch_cols = 1 if (want_cols_hash and got_cols_hash and want_cols_hash != got_cols_hash) else 0

    ok = (mismatch_ver == 0) and (mismatch_schema == 0) and (mismatch_cols == 0)

    details = {
        "expected_schema_ver": want_ver,
        "expected_schema_hash": want_schema_hash,
        "expected_feature_cols_hash": want_cols_hash,
        "mismatch_schema_ver": mismatch_ver,
        "mismatch_schema_hash": mismatch_schema,
        "mismatch_feature_cols_hash": mismatch_cols,
    }
    return ok, details


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--schema-ver", default=os.getenv("FEATURE_SCHEMA_VER", "v4_of"))
    ap.add_argument("--redis-url", default=os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0"))
    ap.add_argument("--pin-key", default=os.getenv("FEATURE_REGISTRY_PIN_KEY", "cfg:feature_registry:edge_stack"))
    ap.add_argument("--metrics-key", default=os.getenv("FEATURE_REGISTRY_CONTRACT_METRICS_KEY", "metrics:feature_registry_contract:last"))
    ap.add_argument("--require-pins", type=int, default=_as_int(os.getenv("FEATURE_REGISTRY_REQUIRE_PINS", "1"), 1))
    ap.add_argument("--seed-pin", type=int, default=0, help="Write current hashes to pin-key (one-time).")
    ap.add_argument("--emit-debug", type=int, default=0, help="Include debug payload (schema_info/spec) in stdout JSON.")

    ap.add_argument("--max-numeric", type=int, default=_as_int(os.getenv("FEATURE_MAX_NUMERIC", "128"), 128))
    ap.add_argument("--scenario-prefix", default=os.getenv("FEATURE_SCENARIO_PREFIX", "bucket:"))
    ap.add_argument("--include-time-onehot", default=os.getenv("FEATURE_INCLUDE_TIME_ONEHOT", ""))
    ap.add_argument("--include-direction", type=int, default=_as_int(os.getenv("FEATURE_INCLUDE_DIRECTION", "1"), 1))
    ap.add_argument("--include-scenario", type=int, default=_as_int(os.getenv("FEATURE_INCLUDE_SCENARIO", "1"), 1))
    ap.add_argument("--strict-feature-cols", type=int, default=_as_int(os.getenv("FEATURE_STRICT_FEATURE_COLS", "0"), 0))
    ap.add_argument("--forbid-scenario-v4-onehot", type=int, default=_as_int(os.getenv("FEATURE_FORBID_SCENARIO_V4_ONEHOT", "1"), 1))

    args = ap.parse_args()

    include_time_onehot: Optional[bool]
    it = str(args.include_time_onehot or "").strip().lower()
    if it in ("1", "true", "yes", "on"):
        include_time_onehot = True
    elif it in ("0", "false", "no", "off"):
        include_time_onehot = False
    else:
        include_time_onehot = None

    out: Dict[str, Any] = {
        "tool": "feature_registry_contract_check_v1",
        "ts_ms": _now_ms(),
        "pin_key": str(args.pin_key),
        "metrics_key": str(args.metrics_key),
        "require_pins": int(args.require_pins),
    },

    rc = 0,
    try:
        current, dbg = _compute_current(
            str(args.schema_ver),
            max_numeric=int(args.max_numeric),
            scenario_prefix=str(args.scenario_prefix),
            include_time_onehot=include_time_onehot,
            include_direction=bool(int(args.include_direction)),
            include_scenario=bool(int(args.include_scenario)),
            strict_feature_cols=bool(int(args.strict_feature_cols)),
            forbid_scenario_v4_onehot=bool(int(args.forbid_scenario_v4_onehot)),
        ),
        out.update({"current": current}),
        if int(args.emit_debug) == 1:
            # Debug payload may be large; keep it bounded.
            out["debug"] = {
                "schema_info": {
                    "ver": dbg.get("schema_info", {}).get("ver"),
                    "schema_hash": dbg.get("schema_info", {}).get("schema_hash"),
                    "n": len(dbg.get("schema_info", {}).get("feature_names") or []),
                },
                "feature_spec": {
                    "ver": dbg.get("feature_spec", {}).get("ver"),
                    "feature_cols_hash": dbg.get("feature_spec", {}).get("feature_cols_hash"),
                    "n": len(dbg.get("feature_spec", {}).get("feature_cols") or []),
                }
            }

        # Redis I/O (pins + metrics)
        r = _connect_redis(str(args.redis_url))
        pins = _hgetall_safe(r, str(args.pin_key))

        if not pins:
            out["pins_present"] = 0
            if int(args.seed_pin) == 1:
                seed = {
                    "schema_ver": str(current.get("schema_ver") or ""),
                    "schema_hash": str(current.get("schema_hash") or ""),
                    "feature_cols_hash": str(current.get("feature_cols_hash") or ""),
                    "updated_ts_ms": str(_now_ms()),
                }
                _hset_safe(r, str(args.pin_key), seed)
                out["seeded_pin"] = 1
                out["status"] = "ok"
                out["reason"] = "pins_seeded"
                rc = 0
            else:
                out["seeded_pin"] = 0
                out["status"] = "alert" if int(args.require_pins) == 1 else "ok"
                out["reason"] = "pins_missing"
                rc = 2 if int(args.require_pins) == 1 else 0
                out["mismatch_schema_ver"] = 0
                out["mismatch_schema_hash"] = 0
                out["mismatch_feature_cols_hash"] = 0
                out["expected_schema_ver"] = ""
                out["expected_schema_hash"] = ""
                out["expected_feature_cols_hash"] = ""
        else:
            out["pins_present"] = 1
            ok, details = _compare_pins(pins, current)
            out.update(details)

            if ok:
                out["status"] = "ok"
                out["reason"] = "ok"
                rc = 0
            else:
                # Prefer the most specific reason.
                if int(details.get("mismatch_schema_ver", 0)) == 1:
                    reason = "schema_ver_mismatch"
                elif int(details.get("mismatch_schema_hash", 0)) == 1 and int(details.get("mismatch_feature_cols_hash", 0)) == 1:
                    reason = "schema_and_feature_hash_mismatch"
                elif int(details.get("mismatch_schema_hash", 0)) == 1:
                    reason = "schema_hash_mismatch"
                else:
                    reason = "feature_cols_hash_mismatch"

                out["status"] = "alert"
                out["reason"] = reason
                rc = 2

        # Persist last metrics (best-effort)
        metrics = {
            "status": out.get("status", ""),
            "reason": out.get("reason", ""),
            "success": 1 if rc == 0 else 0,
            "pins_present": int(out.get("pins_present", 0)),
            "mismatch_schema_ver": int(out.get("mismatch_schema_ver", 0)),
            "mismatch_schema_hash": int(out.get("mismatch_schema_hash", 0)),
            "mismatch_feature_cols_hash": int(out.get("mismatch_feature_cols_hash", 0)),
            "schema_ver": str(current.get("schema_ver") or ""),
            "schema_hash": str(current.get("schema_hash") or ""),
            "feature_cols_hash": str(current.get("feature_cols_hash") or ""),
            "expected_schema_ver": str(out.get("expected_schema_ver") or ""),
            "expected_schema_hash": str(out.get("expected_schema_hash") or ""),
            "expected_feature_cols_hash": str(out.get("expected_feature_cols_hash") or ""),
            "updated_ts_ms": str(_now_ms()),
        }
        # keep params to help debugging (low-cardinality)
        params = current.get("params") or {}
        if isinstance(params, dict):
            for k in ("max_numeric", "scenario_prefix", "include_time_onehot", "include_direction", "include_scenario"):
                if k in params:
                    metrics[f"param_{k}"] = str(params.get(k))

        _hset_safe(r, str(args.metrics_key), metrics)

    except Exception as e:
        out["status"] = "error"
        out["reason"] = f"exception:{type(e).__name__}"
        out["error"] = str(e)[:300]
        rc = 1

    # Print a single JSON line for orchestration parsers.
    try:
        print(json.dumps(out, ensure_ascii=False, sort_keys=True))
    except Exception:
        print(str(out))

    return int(rc)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    raise SystemExit(main())
