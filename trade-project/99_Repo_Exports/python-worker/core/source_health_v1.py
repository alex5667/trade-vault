"""core/source_health_v1.py — canonical helper for external-source health
features (`<source>_data_available` / `<source>_data_age_ms` /
`<source>_data_stale`).

Background:
    Earlier each source (fg/cmc/dl/deribit/bybit/cg/cp) hand-rolled its own
    health-feature emission inside `feature_enricher_v1.py`. Drift was easy:
    one source forgot `_age_ms`, another used a different stale threshold,
    new sources (cg/cp) had no health features at all. The audit flagged
    this — see `project_p1_phase1_shadow_emit_2026_05_28.md` and the v15_of
    rollout audit (2026-05-28).

This module provides the single source of truth:
    * `SOURCE_REGISTRY` — declarative mapping `source_name` → `SourceSpec`
      (prefix, stale_threshold_ms, primary Redis key, snapshot kind).
    * `compute_source_health(snapshot, now_ms, max_lag_ms)` — pure helper
      returning `(available, age_ms, stale)`. Mirrors the historical
      `_source_age_and_health` logic so existing producers can migrate
      incrementally without behavior change.
    * `make_source_health_features(source, snapshot, now_ms)` — flat
      `<prefix>_data_available/_age_ms/_data_stale` dict for one source.
    * `build_all_source_health_features(snapshots, now_ms)` — batched
      version that takes a `{source_name: snapshot_dict}` map and emits
      the union of feature keys.
    * `source_health_feature_keys()` — deterministic tuple of every
      feature this module can emit (for schema-routing / shadow watchlist
      / coverage exporter wiring).

The module is **read-only** from a Redis perspective: it only computes
features from snapshots the caller already loaded. Producers stay
responsible for fetching their own source snapshots; this helper just
turns a snapshot into the standard three-feature health triple.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping


# ── Snapshot kinds ────────────────────────────────────────────────────────────
# JSON: redis GET → json.loads → dict
# HASH: redis HGETALL → dict
SNAP_KIND_JSON = "json"
SNAP_KIND_HASH = "hash"


@dataclass(frozen=True)
class SourceSpec:
    """Declarative description of one external data source."""

    name: str
    prefix: str           # feature-name prefix (e.g. "cg" → "cg_data_available")
    redis_key: str        # canonical key the producer should refresh
    snap_kind: str        # SNAP_KIND_JSON / SNAP_KIND_HASH
    max_lag_ms: int       # source-specific stale threshold

    @property
    def feature_keys(self) -> tuple[str, str, str]:
        return (
            f"{self.prefix}_data_available",
            f"{self.prefix}_data_age_ms",
            f"{self.prefix}_data_stale",
        )


# ── Registry — single source of truth ────────────────────────────────────────
# Add new sources here; producers and schema consumers pick them up
# automatically via SOURCE_HEALTH_FEATURE_KEYS.
SOURCE_REGISTRY: tuple[SourceSpec, ...] = (
    SourceSpec(
        name="fear_greed",
        prefix="fg",
        redis_key="cache:fear_greed",
        snap_kind=SNAP_KIND_JSON,
        max_lag_ms=30 * 60_000,  # 30min — Fear&Greed updates every few hours
    ),
    SourceSpec(
        name="coinmarketcap",
        prefix="cmc",
        redis_key="runtime:provider:coinmarketcap:global",
        snap_kind=SNAP_KIND_HASH,
        max_lag_ms=5 * 60_000,
    ),
    SourceSpec(
        name="defillama",
        prefix="dl",
        redis_key="runtime:provider:defillama:eth_dex",
        snap_kind=SNAP_KIND_HASH,
        max_lag_ms=60 * 60_000,  # 1h — DefiLlama refresh cadence
    ),
    SourceSpec(
        name="deribit",
        prefix="deribit",
        redis_key="ctx:deribit:global",
        snap_kind=SNAP_KIND_JSON,
        max_lag_ms=60_000,
    ),
    SourceSpec(
        name="bybit",
        prefix="bybit",
        redis_key="runtime:bybit:{symbol}",  # template — caller fills symbol
        snap_kind=SNAP_KIND_HASH,
        max_lag_ms=10_000,
    ),
    SourceSpec(
        name="coingecko",
        prefix="cg",
        redis_key="runtime:coingecko:global",
        snap_kind=SNAP_KIND_HASH,
        max_lag_ms=5 * 60_000,
    ),
    SourceSpec(
        name="coinpaprika",
        prefix="cp",
        redis_key="runtime:provider:coinpaprika:global",
        snap_kind=SNAP_KIND_HASH,
        max_lag_ms=5 * 60_000,
    ),
)


_SPEC_BY_NAME: dict[str, SourceSpec] = {s.name: s for s in SOURCE_REGISTRY}
_SPEC_BY_PREFIX: dict[str, SourceSpec] = {s.prefix: s for s in SOURCE_REGISTRY}


def get_source_spec(name_or_prefix: str) -> SourceSpec | None:
    """Lookup by registry name or feature prefix; None on miss."""
    return _SPEC_BY_NAME.get(name_or_prefix) or _SPEC_BY_PREFIX.get(name_or_prefix)


# ── Pure helper — turn a snapshot into (available, age_ms, stale) ───────────

def compute_source_health(
    snapshot: Mapping[str, Any] | None,
    now_ms: float,
    max_lag_ms: float,
) -> tuple[float, float, float]:
    """Derive (available, age_ms, stale) from one snapshot dict.

    Semantics (mirrors the historical `_source_age_and_health` in
    `feature_enricher_v1.py` exactly so producers can migrate without
    changing emitted values):

        empty snapshot      → (0.0, 0.0, 1.0)   — absent / never loaded
        no ts in snapshot   → (1.0, 0.0, 0.0)   — loaded but no timestamp;
                                                  treat as fresh (cannot
                                                  prove staleness)
        bad ts value        → (1.0, 0.0, 0.0)   — same as no-ts
        age > max_lag_ms    → (1.0, age, 1.0)   — stale
        else                → (1.0, age, 0.0)   — healthy

    `age_ms` is clamped to ≥0 (defensive: clock skew shouldn't surface as
    negative ages).
    """
    if not snapshot:
        return 0.0, 0.0, 1.0
    ts_raw = (
        snapshot.get("ts_ms")
        or snapshot.get("updated_at_ms")
        or snapshot.get("ts")
    )
    if ts_raw is None:
        return 1.0, 0.0, 0.0
    try:
        age_ms = max(0.0, float(now_ms) - float(ts_raw))
    except (TypeError, ValueError):
        return 1.0, 0.0, 0.0
    stale = 1.0 if age_ms > float(max_lag_ms) else 0.0
    return 1.0, age_ms, stale


# ── Per-source feature builders ──────────────────────────────────────────────

def make_source_health_features(
    source: str,
    snapshot: Mapping[str, Any] | None,
    now_ms: float,
    *,
    max_lag_ms: float | None = None,
) -> dict[str, float]:
    """Flat health dict for one source.

    Args:
        source:       name (e.g. ``"coingecko"``) or prefix (``"cg"``).
        snapshot:     producer-supplied snapshot dict (may be None / empty).
        now_ms:       caller's monotonic-ish ms reference.
        max_lag_ms:   override the spec threshold (rare; used by tests).

    Returns:
        ``{<prefix>_data_available, <prefix>_data_age_ms, <prefix>_data_stale}``.
        When the source is unknown to the registry, returns ``{}`` (silent
        fail-open — keeps callers safe for new prefixes).
    """
    spec = get_source_spec(source)
    if spec is None:
        return {}
    lag = float(max_lag_ms) if max_lag_ms is not None else float(spec.max_lag_ms)
    avail, age, stale = compute_source_health(snapshot, now_ms, lag)
    out: dict[str, float] = {
        f"{spec.prefix}_data_available": avail,
        f"{spec.prefix}_data_stale": stale,
    }
    # Match the legacy contract: `_age_ms` is emitted only when >0 to avoid
    # implying a fresh snapshot when no ts was present.
    if age > 0.0:
        out[f"{spec.prefix}_data_age_ms"] = age
    return out


def build_all_source_health_features(
    snapshots: Mapping[str, Mapping[str, Any] | None],
    now_ms: float,
) -> dict[str, float]:
    """Batched health builder.

    Args:
        snapshots: ``{source_name_or_prefix: snapshot}`` map. Sources absent
                   from the map are emitted as unavailable (0/0/1) so the
                   downstream schema is always populated.
        now_ms:    caller's reference timestamp.
    """
    out: dict[str, float] = {}
    for spec in SOURCE_REGISTRY:
        snap = snapshots.get(spec.name) or snapshots.get(spec.prefix)
        out.update(make_source_health_features(spec.name, snap, now_ms))
        # Even when omitted, ensure available/stale keys exist for shape
        # determinism (consumers expect every registered source).
        out.setdefault(f"{spec.prefix}_data_available", 0.0)
        out.setdefault(f"{spec.prefix}_data_stale", 1.0)
    return out


# ── Schema accessors ─────────────────────────────────────────────────────────

def source_health_feature_keys() -> tuple[str, ...]:
    """Deterministic tuple of every health feature this module can emit.

    Used by:
      * `core/external_features_payload_v1.py`: append to `_V12_BASE_OPTIONAL_KEYS`.
      * `core/v15_of_shadow_watchlist_v1.py`: track shadow coverage.
      * `orderflow_services/v15_of_coverage_exporter_v1.py`: per-group rollup.
    """
    keys: list[str] = []
    for spec in SOURCE_REGISTRY:
        a, age, s = spec.feature_keys
        keys.append(a)
        keys.append(age)
        keys.append(s)
    return tuple(keys)


SOURCE_HEALTH_FEATURE_KEYS: tuple[str, ...] = source_health_feature_keys()
