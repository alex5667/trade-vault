#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Prometheus exporter for v15_of feature coverage / zero-rate / dead-keys.

Reads recent entries from Redis stream ``signals:of:inputs`` (the canonical
of-inputs payload bridged from the gate / pipeline) and computes, per v15_of
feature group, the share of samples where each key was present and non-zero.
Aggregates per-group to keep Prometheus cardinality bounded (~30 groups, not
531 keys).

Metrics:
  v15_of_coverage_exporter_up{}                       (1 if Redis read works)
  v15_of_samples_processed_total{}                    (counter)
  v15_of_window_size{}                                (last batch size)
  v15_of_features_total_keys{}                        (=531, schema invariant)
  v15_of_features_covered_keys{coverage_floor}        (count where coverage≥floor)
  v15_of_feature_group_coverage_ratio{group}          (mean present-share in group)
  v15_of_feature_group_zero_rate{group}               (mean zero-share in group)
  v15_of_feature_group_dead_keys{group}               (count of keys w/ coverage<floor)

Dead-keys detail (low-cardinality alternative):
  Writes a Redis hash ``metrics:v15_of_coverage:dead_keys`` with one field per
  dead key → last-seen coverage ratio, so audits can fetch the list without a
  per-key Prometheus series.

ENV:
  REDIS_URL                              default redis://redis-worker-1:6379/0
  V15_OF_COV_STREAM_KEY                  default signals:of:inputs
  V15_OF_COV_EXPORTER_PORT               default 9902
  V15_OF_COV_EXPORTER_INTERVAL_S         default 60
  V15_OF_COV_BATCH_SIZE                  default 2000   (XREVRANGE COUNT)
  V15_OF_COV_MIN_SAMPLES                 default 100    (skip emit until reached)
  V15_OF_COV_DEAD_KEY_FLOOR              default 0.05   (coverage<floor → dead)
  V15_OF_COV_DEAD_KEY_HASH_KEY           default metrics:v15_of_coverage:dead_keys
"""
from __future__ import annotations

import json
import logging
import os
import time
from collections import defaultdict
from typing import Any

import redis  # type: ignore
from prometheus_client import Counter, Gauge, start_http_server

logger = logging.getLogger("v15_of_coverage_exporter")


# ── Schema group map: key → group_name ───────────────────────────────────────

def _build_key_to_group() -> tuple[dict[str, str], dict[str, int], int]:
    """Build (key→group, group→size, total_keys). Reads group lists directly
    from core.ml_feature_schema_v15_of so the exporter never drifts from the
    schema module — bump the schema, restart the exporter, done."""
    from core import ml_feature_schema_v15_of as M  # type: ignore

    groups: dict[str, list[str]] = {}
    for attr in dir(M):
        if not attr.startswith("_GROUP_"):
            continue
        val = getattr(M, attr, None)
        if isinstance(val, list) and all(isinstance(x, str) for x in val):
            # Normalize group label: _GROUP_P84_HAWKES_VPIN → p84_hawkes_vpin
            label = attr.removeprefix("_GROUP_").lower()
            groups[label] = list(val)

    # v14_of base is the residual (anything in V15_OF not in any _GROUP_*).
    in_groups = {k for ks in groups.values() for k in ks}
    v15_all = set(M.V15_OF_NUMERIC_KEYS)
    residual = sorted(v15_all - in_groups)
    if residual:
        groups["v14_of_base"] = residual

    key_to_group: dict[str, str] = {}
    for g, ks in groups.items():
        for k in ks:
            key_to_group.setdefault(k, g)

    group_sizes = {g: len(ks) for g, ks in groups.items()}
    total = len(v15_all)
    return key_to_group, group_sizes, total


KEY_TO_GROUP, GROUP_SIZES, TOTAL_KEYS = _build_key_to_group()


# ── Shadow watchlist (P1/P2 + source health) ────────────────────────────────
# Separate from V15_OF — features pending the 48h coverage gate before they
# can be promoted into V15_OF_NUMERIC_KEYS. The exporter tracks both planes
# independently so prod schema metrics aren't polluted by shadow noise.

def _build_shadow_key_to_group() -> tuple[dict[str, str], dict[str, int], int]:
    try:
        from core.v15_of_shadow_watchlist_v1 import (  # type: ignore
            SHADOW_WATCHLIST_GROUPS,
        )
    except Exception:
        return {}, {}, 0
    key_to_group: dict[str, str] = {}
    for g, ks in SHADOW_WATCHLIST_GROUPS.items():
        for k in ks:
            key_to_group.setdefault(k, g)
    sizes = {g: len(ks) for g, ks in SHADOW_WATCHLIST_GROUPS.items()}
    return key_to_group, sizes, len(key_to_group)


SHADOW_KEY_TO_GROUP, SHADOW_GROUP_SIZES, SHADOW_TOTAL_KEYS = _build_shadow_key_to_group()


# ── Prometheus metrics ────────────────────────────────────────────────────────

UP = Gauge("v15_of_coverage_exporter_up", "1 if exporter can read Redis")
SAMPLES_PROCESSED = Counter("v15_of_samples_processed_total", "Records consumed since boot")
WINDOW_SIZE = Gauge("v15_of_window_size", "Number of records in the last computed window")
TOTAL_KEYS_G = Gauge("v15_of_features_total_keys", "Schema invariant: total numeric keys in v15_of")
COVERED_KEYS = Gauge(
    "v15_of_features_covered_keys",
    "Count of v15_of keys with coverage≥floor in last window",
    ["coverage_floor"],
)
GROUP_COVERAGE = Gauge(
    "v15_of_feature_group_coverage_ratio",
    "Mean present-share for all keys in the group over the last window",
    ["group"],
)
GROUP_ZERO_RATE = Gauge(
    "v15_of_feature_group_zero_rate",
    "Mean share of zero-valued samples for all keys in the group",
    ["group"],
)
GROUP_DEAD_KEYS = Gauge(
    "v15_of_feature_group_dead_keys",
    "Count of keys in the group with coverage<floor (dead source)",
    ["group"],
)

# ── Shadow watchlist gauges ──────────────────────────────────────────────────
# Tracks features that are emitted in shadow but NOT yet in V15_OF_NUMERIC_KEYS
# (P1/P2 phase, source health). When a group's coverage stays ≥ gate_floor for
# 48h the keys are eligible for schema promotion.
SHADOW_TOTAL_KEYS_G = Gauge(
    "v15_of_shadow_total_keys",
    "Total tracked shadow-watchlist keys (P1/P2 + source health)",
)
SHADOW_GROUP_COVERAGE = Gauge(
    "v15_of_shadow_feature_group_coverage_ratio",
    "Mean present-share for shadow group over the window",
    ["group"],
)
SHADOW_GROUP_ZERO_RATE = Gauge(
    "v15_of_shadow_feature_group_zero_rate",
    "Mean zero-share for shadow group over the window",
    ["group"],
)
SHADOW_GROUP_DEAD_KEYS = Gauge(
    "v15_of_shadow_feature_group_dead_keys",
    "Count of shadow keys with coverage<gate_floor (not yet ready for promotion)",
    ["group"],
)
SHADOW_GROUP_PROMOTION_READY = Gauge(
    "v15_of_shadow_group_promotion_ready",
    "1 if every key in the group has coverage≥gate_floor (48h-gate eligible)",
    ["group"],
)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _extract_indicators(fields: dict[str, Any]) -> dict[str, Any]:
    """signals:of:inputs entries store the feature payload either flat or as a
    JSON blob in `data`/`indicators`. Try both shapes."""
    if not fields:
        return {}
    blob = (
        fields.get("payload")
        or fields.get("indicators")
        or fields.get("data")
        or fields.get("payload_json")
    )
    if isinstance(blob, str):
        try:
            obj = json.loads(blob)
        except Exception:
            obj = {}
        if isinstance(obj, dict):
            ind = obj.get("indicators")
            if isinstance(ind, dict):
                return ind
            return obj
    if isinstance(fields.get("indicators"), dict):
        return fields["indicators"]  # type: ignore[return-value]
    # Fallback: flat fields — any key matching a v15_of key.
    return {k: v for k, v in fields.items() if k in KEY_TO_GROUP}


def _is_present(v: Any) -> bool:
    if v is None:
        return False
    if isinstance(v, str):
        return v.strip() != ""
    return True


def _is_zero(v: Any) -> bool:
    try:
        return float(v) == 0.0
    except Exception:
        return False


def _compute_window(
    records: list[dict[str, Any]],
    keys: dict[str, str] | None = None,
) -> dict[str, dict[str, float]]:
    """Returns key → {present, nonzero, count} aggregates over the window.

    Args:
        records: list of stream-entry field dicts.
        keys:    optional key→group map to iterate (defaults to KEY_TO_GROUP).
                 Pass SHADOW_KEY_TO_GROUP to compute shadow-watchlist stats.
    """
    target = keys if keys is not None else KEY_TO_GROUP
    agg: dict[str, dict[str, int]] = defaultdict(lambda: {"present": 0, "nonzero": 0, "count": 0})
    for rec in records:
        ind = _extract_indicators(rec)
        # Empty extraction is a valid sample (producer emitted nothing for
        # any v15_of key) — count it as all-missing rather than dropping,
        # so coverage reflects the true present-share over real records.
        if ind is None:
            continue
        for k in target:
            v = ind.get(k)
            agg[k]["count"] += 1
            if _is_present(v):
                agg[k]["present"] += 1
                if not _is_zero(v):
                    agg[k]["nonzero"] += 1
    out: dict[str, dict[str, float]] = {}
    for k, a in agg.items():
        c = a["count"] or 1
        cov = a["present"] / c
        zr = (a["present"] - a["nonzero"]) / a["present"] if a["present"] else 0.0
        out[k] = {"coverage": cov, "zero_rate": zr, "n": float(c)}
    return out


def _write_dead_keys_hash(r: redis.Redis, hash_key: str, dead: dict[str, float]) -> None:
    try:
        with r.pipeline(transaction=False) as pipe:
            pipe.delete(hash_key)
            if dead:
                pipe.hset(hash_key, mapping={k: f"{v:.4f}" for k, v in dead.items()})
                pipe.expire(hash_key, 3600)  # 1h TTL — refreshed every cycle
            pipe.execute()
    except Exception as e:
        logger.warning("dead_keys hash write failed: %s", e)


# ── Main loop ─────────────────────────────────────────────────────────────────

def main() -> None:  # pragma: no cover — integration entry
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
    )

    redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
    stream_key = os.getenv("V15_OF_COV_STREAM_KEY", "signals:of:inputs")
    port = int(os.getenv("V15_OF_COV_EXPORTER_PORT", "9902"))
    interval_s = float(os.getenv("V15_OF_COV_EXPORTER_INTERVAL_S", "60"))
    batch_size = int(os.getenv("V15_OF_COV_BATCH_SIZE", "2000"))
    # min_samples: skip the cycle until at least this many records are in
    # the window. Default 100 → ±2% binomial standard error at 95%
    # coverage, robust enough for promotion-gate alerting. The companion
    # `V15_OF_COV_WINDOW_MS` (default 4h) is sized so even a slow shadow
    # stream (~36 signals/h) accumulates ≥100 records before each cycle.
    min_samples = int(os.getenv("V15_OF_COV_MIN_SAMPLES", "100"))
    dead_floor = float(os.getenv("V15_OF_COV_DEAD_KEY_FLOOR", "0.05"))
    dead_hash_key = os.getenv("V15_OF_COV_DEAD_KEY_HASH_KEY", "metrics:v15_of_coverage:dead_keys")
    # 48h-gate promotion floor: a shadow group is promotion-ready when every
    # key in it has coverage≥this floor over the last window.
    shadow_gate_floor = float(os.getenv("V15_OF_COV_SHADOW_GATE_FLOOR", "0.95"))
    # Time-window cap (ms). When > 0, only records with stream-id timestamp
    # within `now − window_ms` are counted. Lets coverage track the live
    # producer state without being diluted by stale rolling-window records
    # left over from a producer/enricher restart. 0 = disabled (legacy mode).
    # 4h default → fits ≥100 records at the canonical shadow signal rate
    # (~36/h). Tune up for prod streams or down when accelerating restarts.
    window_ms = int(os.getenv("V15_OF_COV_WINDOW_MS", "14400000"))  # 4h
    shadow_dead_hash_key = os.getenv(
        "V15_OF_COV_SHADOW_DEAD_KEY_HASH_KEY",
        "metrics:v15_of_coverage:shadow_dead_keys",
    )

    r = redis.Redis.from_url(redis_url, decode_responses=True)
    start_http_server(port)
    TOTAL_KEYS_G.set(TOTAL_KEYS)
    SHADOW_TOTAL_KEYS_G.set(SHADOW_TOTAL_KEYS)

    logger.info(
        "v15_of_coverage_exporter started: port=%d stream=%s total_keys=%d "
        "groups=%d shadow_keys=%d shadow_groups=%d shadow_gate_floor=%.2f",
        port, stream_key, TOTAL_KEYS, len(GROUP_SIZES),
        SHADOW_TOTAL_KEYS, len(SHADOW_GROUP_SIZES), shadow_gate_floor,
    )

    while True:
        try:
            # Time-window filter: ask Redis for only entries within the cap
            # so a slow signal flow rate can't dilute coverage with stale
            # pre-restart records. When window_ms<=0 the legacy behavior
            # (last `batch_size` regardless of age) is preserved.
            if window_ms > 0:
                cutoff_ms = int(time.time() * 1000) - window_ms
                entries = r.xrange(  # type: ignore[misc]
                    stream_key,
                    min=f"{cutoff_ms}-0",
                    count=batch_size,
                )
            else:
                entries = r.xrevrange(stream_key, count=batch_size)  # type: ignore[misc]
            records = [fields for _id, fields in (entries or [])]  # type: ignore[misc]
            WINDOW_SIZE.set(len(records))
            UP.set(1)

            if len(records) < min_samples:
                logger.debug("waiting for %d samples (have %d)", min_samples, len(records))
                time.sleep(interval_s)
                continue

            stats = _compute_window(records)
            SAMPLES_PROCESSED.inc(len(records))

            # Per-group aggregates
            per_group_cov: dict[str, list[float]] = defaultdict(list)
            per_group_zr: dict[str, list[float]] = defaultdict(list)
            per_group_dead: dict[str, int] = defaultdict(int)
            dead_keys_map: dict[str, float] = {}
            covered_floor_005 = 0
            covered_floor_050 = 0

            for k, group in KEY_TO_GROUP.items():
                s = stats.get(k, {"coverage": 0.0, "zero_rate": 0.0})
                cov = s["coverage"]
                zr = s["zero_rate"]
                per_group_cov[group].append(cov)
                per_group_zr[group].append(zr)
                if cov < dead_floor:
                    per_group_dead[group] += 1
                    dead_keys_map[k] = cov
                if cov >= 0.05:
                    covered_floor_005 += 1
                if cov >= 0.50:
                    covered_floor_050 += 1

            for g in GROUP_SIZES:
                cov_list = per_group_cov.get(g, [])
                zr_list = per_group_zr.get(g, [])
                GROUP_COVERAGE.labels(group=g).set(
                    sum(cov_list) / len(cov_list) if cov_list else 0.0
                )
                GROUP_ZERO_RATE.labels(group=g).set(
                    sum(zr_list) / len(zr_list) if zr_list else 0.0
                )
                GROUP_DEAD_KEYS.labels(group=g).set(float(per_group_dead.get(g, 0)))

            COVERED_KEYS.labels(coverage_floor="0.05").set(covered_floor_005)
            COVERED_KEYS.labels(coverage_floor="0.50").set(covered_floor_050)

            _write_dead_keys_hash(r, dead_hash_key, dead_keys_map)
            # Shadow-plane dead-keys hash (gated at shadow_gate_floor=0.95,
            # not the prod dead_floor=0.05) — consumed by the promotion tool
            # `tools/promote_shadow_to_v15_of_v1.py` to decide 48h-gate
            # eligibility. Empty hash ⇔ every shadow key cleared the gate.
            # ── Shadow-watchlist aggregates (P1/P2 + source health) ─────────
            shadow_ready_groups = 0
            shadow_dead_keys_map: dict[str, float] = {}
            if SHADOW_KEY_TO_GROUP:
                shadow_stats = _compute_window(records, keys=SHADOW_KEY_TO_GROUP)
                shadow_per_group_cov: dict[str, list[float]] = defaultdict(list)
                shadow_per_group_zr: dict[str, list[float]] = defaultdict(list)
                shadow_per_group_dead: dict[str, int] = defaultdict(int)
                for k, group in SHADOW_KEY_TO_GROUP.items():
                    s = shadow_stats.get(k, {"coverage": 0.0, "zero_rate": 0.0})
                    cov = s["coverage"]
                    shadow_per_group_cov[group].append(cov)
                    shadow_per_group_zr[group].append(s["zero_rate"])
                    if cov < shadow_gate_floor:
                        shadow_per_group_dead[group] += 1
                        shadow_dead_keys_map[k] = cov
                for g in SHADOW_GROUP_SIZES:
                    cov_list = shadow_per_group_cov.get(g, [])
                    zr_list = shadow_per_group_zr.get(g, [])
                    SHADOW_GROUP_COVERAGE.labels(group=g).set(
                        sum(cov_list) / len(cov_list) if cov_list else 0.0
                    )
                    SHADOW_GROUP_ZERO_RATE.labels(group=g).set(
                        sum(zr_list) / len(zr_list) if zr_list else 0.0
                    )
                    dead_in_g = shadow_per_group_dead.get(g, 0)
                    SHADOW_GROUP_DEAD_KEYS.labels(group=g).set(float(dead_in_g))
                    ready = 1.0 if (cov_list and dead_in_g == 0) else 0.0
                    SHADOW_GROUP_PROMOTION_READY.labels(group=g).set(ready)
                    if ready > 0:
                        shadow_ready_groups += 1
                # Always write the hash — empty value means "no shadow dead
                # keys", which is the signal the promotion tool needs.
                _write_dead_keys_hash(r, shadow_dead_hash_key, shadow_dead_keys_map)

            logger.info(
                "cycle ok: records=%d covered≥5%%=%d covered≥50%%=%d dead=%d "
                "shadow_groups_ready=%d/%d",
                len(records), covered_floor_005, covered_floor_050,
                len(dead_keys_map), shadow_ready_groups, len(SHADOW_GROUP_SIZES),
            )
        except Exception as e:
            UP.set(0)
            logger.warning("cycle failed: %s", e)

        time.sleep(interval_s)


if __name__ == "__main__":  # pragma: no cover
    main()
