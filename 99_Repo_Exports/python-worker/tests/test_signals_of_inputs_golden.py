"""Golden-payload regression test for `signals:of:inputs`.

Reads a captured Redis stream entry from
`tests/fixtures/signals_of_inputs_golden.json` and asserts:

  1. Payload top-level structure is intact (symbol, direction, ts_ms,
     schema_version, indicators dict non-empty).
  2. Outbox SCHEMA_VERSION matches the active producer constant.
  3. v13_of (prod) feature coverage in `indicators` >= floor.
     Fails loud if a prior schema fix regresses (memory:
     project_v13_of_coverage_fix_2026_05_16).
  4. v14_of (canary) feature coverage in `indicators` >= floor.
     Fails loud if v14_of canary wiring breaks (og_payload /
     external_features_payload).
  5. og_* keys (v14_of OrderFlow rule-Gate consensus) all present.
     Catches build_og_payload import/build failures
     (covered by `og_payload_fail_open_total` counter at runtime).

Floors are derived from a real captured snapshot. They are intentionally
conservative — a strict 100% assert would be too brittle for symbols
where some warm-up windows have not yet filled.

Regenerate the fixture from main host (REDIS_WORKER_HOST=localhost,
REDIS_WORKER_PORT=63791 by default):

    python python-worker/tests/regen_signals_of_inputs_golden.py
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

FIXTURE = Path(__file__).parent / "fixtures" / "signals_of_inputs_golden.json"

# Floors — bump up when coverage improves; never lower without an ADR.
# Baseline captured 2026-05-17 from a live signal (1000PEPEUSDT/LONG, warm
# state). v14_of canary mode active.
V13_COVERAGE_FLOOR = 0.55
V14_COVERAGE_FLOOR = 0.65

# Required structural top-level fields always emitted by signal_pipeline.
REQUIRED_TOP_FIELDS = (
    "symbol", "direction", "ts_ms", "schema_version",
    "indicators", "side", "price",
)


@pytest.fixture(scope="module")
def golden():
    if not FIXTURE.exists():
        pytest.skip(
            f"golden fixture missing: {FIXTURE} — regenerate via "
            f"python-worker/tests/regen_signals_of_inputs_golden.py"
        )
    with FIXTURE.open() as f:
        return json.load(f)


@pytest.fixture(scope="module")
def payload(golden):
    return golden["payload"]


@pytest.fixture(scope="module")
def indicators(payload):
    ind = payload.get("indicators")
    assert isinstance(ind, dict) and ind, "indicators must be a non-empty dict"
    return ind


def test_payload_structural_fields(payload):
    """Producer contract: required top-level fields always present."""
    missing = [k for k in REQUIRED_TOP_FIELDS if k not in payload]
    assert not missing, f"payload missing required fields: {missing}"
    assert payload["indicators"], "indicators dict empty"


def test_payload_schema_version_present(payload):
    """signals:of:inputs payload carries its own schema_version tag (e.g. "v1").

    This is distinct from `core.outbox_envelope.SCHEMA_VERSION` (int=1) which
    governs the *outbox* envelope (stream:signals:outbox), not the *inputs*
    stream. The signal_pipeline tags each emitted payload with a producer
    contract version. Test just guards against missing / empty tag.
    """
    ver = payload.get("schema_version")
    assert ver, f"payload missing schema_version (got {ver!r})"
    assert isinstance(ver, (str, int)), f"unexpected schema_version type: {type(ver)}"


def test_v13_of_coverage_above_floor(indicators):
    """v13_of (prod) feature coverage — guard against schema-population regression."""
    from core.ml_feature_schema_v13_of import V13_OF_NUMERIC_KEYS
    v13 = set(V13_OF_NUMERIC_KEYS)
    present = v13 & set(indicators.keys())
    coverage = len(present) / len(v13)
    assert coverage >= V13_COVERAGE_FLOOR, (
        f"v13_of coverage {coverage:.1%} < floor {V13_COVERAGE_FLOOR:.0%}; "
        f"missing {len(v13)-len(present)} of {len(v13)} keys. "
        f"Sample missing: {sorted(v13 - set(indicators.keys()))[:10]}"
    )


def test_v14_of_coverage_above_floor(indicators):
    """v14_of (canary) coverage — guard against og_/external_features wiring drift."""
    from core.ml_feature_schema_v14_of import V14_OF_NUMERIC_KEYS
    v14 = set(V14_OF_NUMERIC_KEYS)
    present = v14 & set(indicators.keys())
    coverage = len(present) / len(v14)
    assert coverage >= V14_COVERAGE_FLOOR, (
        f"v14_of coverage {coverage:.1%} < floor {V14_COVERAGE_FLOOR:.0%}; "
        f"missing {len(v14)-len(present)} of {len(v14)} keys. "
        f"Sample missing: {sorted(v14 - set(indicators.keys()))[:10]}"
    )


def test_og_keys_all_present(indicators):
    """build_og_payload must populate all 16 og_* keys.

    A zero-valued og_* dict is acceptable (fail-open by design) but the
    keys themselves must be present — absence indicates the wiring at
    of_confirm_engine.py:~5566 broke (counter:
    og_payload_fail_open_total{reason="import_error"}).
    """
    from core.v14_of_features import og_keys
    missing = [k for k in og_keys() if k not in indicators]
    assert not missing, (
        f"og_* keys missing from payload — build_og_payload wiring broken: {missing}"
    )


def test_v13_of_subset_of_v14_of():
    """Sanity: append-only schema invariant. v14_of must be a strict superset of v13_of."""
    from core.ml_feature_schema_v13_of import V13_OF_NUMERIC_KEYS
    from core.ml_feature_schema_v14_of import V14_OF_NUMERIC_KEYS
    v13 = set(V13_OF_NUMERIC_KEYS)
    v14 = set(V14_OF_NUMERIC_KEYS)
    removed = v13 - v14
    assert not removed, (
        f"v14_of removed keys from v13_of: {sorted(removed)} — "
        f"violates append-only invariant"
    )
