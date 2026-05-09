from __future__ import annotations

"""
Regression: StreamRetention (Go) ↔ STREAM_RETENTION (Python) parity (merge-blocker).

Validates that:
1. Every stream key present in Go StreamRetention exists in Python STREAM_RETENTION
   with the SAME maxlen value.
2. Every stream key present in Python STREAM_RETENTION exists in Go StreamRetention
   with the SAME maxlen value.
3. All keys in Python STREAM_RETENTION resolve to values that exist in RedisStreams
   (no orphaned literals — all keys MUST be RS.* field values).

Run:
    cd python-worker && python -m pytest tests/test_stream_retention_parity.py -v
"""

import dataclasses
import os
import re

import pytest

from core.redis_keys import STREAM_RETENTION
from core.redis_keys import RedisStreams as RS

# ---------------------------------------------------------------------------
# Parse Go StreamRetention
# ---------------------------------------------------------------------------

def _parse_go_retention() -> dict[str, int]:
    """
    Parse Go `StreamRetention` map from keys.go.

    Handles entries like:
        OFGateMetrics:  10_000,
        CandleDataStream: 10_000,

    Returns dict {resolved_string_value: maxlen}.
    """
    go_file = os.path.join(
        os.path.dirname(__file__), "..", "..",
        "go-worker", "internal", "streams", "keys.go",
    )
    if not os.path.exists(go_file):
        pytest.skip(f"Go keys file not found at {go_file}", allow_module_level=True)

    with open(go_file, encoding="utf-8") as f:
        content = f.read()

    # --- Step 1: build {GoConstantName: string_value} from the const blocks ---
    const_pattern = re.compile(
        r'^\s*(?:const\s+)?([A-Za-z0-9_]+)\s*=\s*"([^"]+)"',
        re.MULTILINE,
    )
    go_const_map: dict[str, str] = dict(const_pattern.findall(content))

    # --- Step 2: extract StreamRetention map body ---
    retention_match = re.search(
        r'var\s+StreamRetention\s*=\s*map\[string\]StreamMaxLen\s*\{(.*?)\}',
        content,
        re.DOTALL,
    )
    if not retention_match:
        pytest.skip("Could not find StreamRetention map in keys.go", allow_module_level=False)

    retention_body = retention_match.group(1)

    # --- Step 3: parse each entry: ConstantName: numeric_value ---
    # Strip block and inline comments to avoid formatting brittleness
    retention_body = re.sub(r'/\*.*?\*/', '', retention_body, flags=re.DOTALL)
    retention_body = re.sub(r'//.*', '', retention_body)

    entry_pattern = re.compile(r'([A-Za-z0-9_]+)\s*:\s*([\d_]+)')

    result: dict[str, int] = {}
    for const_name, raw_maxlen in entry_pattern.findall(retention_body):
        string_value = go_const_map.get(const_name)
        if string_value is None:
            pytest.fail(
                f"Go StreamRetention references constant '{const_name}' "
                f"which was not found in const blocks of keys.go. "
                f"Possibly a template const or typo."
            )
        maxlen = int(raw_maxlen.replace("_", ""))
        result[string_value] = maxlen

    return result


# ---------------------------------------------------------------------------
# Build Python retention map {string_value: maxlen}
# (STREAM_RETENTION keys are already resolved RS.* instances, i.e. plain strings)
# ---------------------------------------------------------------------------

def _py_all_stream_values() -> set[str]:
    """All string values defined in RedisStreams dataclass."""
    values: set[str] = set()
    for f in dataclasses.fields(RS):
        val = getattr(RS, f.name)
        if isinstance(val, str):
            values.add(val)
    return values


try:
    _GO_RETENTION = _parse_go_retention()
except pytest.skip.Exception:
    _GO_RETENTION = {}

_PY_RETENTION: dict[str, int] = STREAM_RETENTION
_ALL_STREAM_VALUES = _py_all_stream_values()
_ALL_KEYS = sorted(set(_GO_RETENTION.keys()) | set(_PY_RETENTION.keys()))


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestStreamRetentionParity:
    """Validate StreamRetention ↔ STREAM_RETENTION cross-language symmetry."""

    def test_py_retention_not_empty(self) -> None:
        """Sanity: STREAM_RETENTION must have at least 30 entries."""
        assert len(_PY_RETENTION) >= 30, (
            f"STREAM_RETENTION has only {len(_PY_RETENTION)} entries — "
            "something may have been accidentally wiped."
        )

    def test_go_retention_not_empty(self) -> None:
        """Sanity: Go StreamRetention must have at least 30 entries."""
        if not _GO_RETENTION:
            pytest.skip("Go StreamRetention could not be loaded")
        assert len(_GO_RETENTION) >= 30, (
            f"Go StreamRetention has only {len(_GO_RETENTION)} entries — "
            "something may have been accidentally wiped."
        )

    def test_expected_minimum_keys_parsed_golden(self) -> None:
        """
        Golden-test/Fuzz check: ensure our parser is not silently skipping lines
        due to formatting issues (like inline comments or indentation).
        Expect >= 40 keys.
        """
        if not _GO_RETENTION:
            pytest.skip("Go StreamRetention could not be loaded")
        assert len(_GO_RETENTION) >= 40, (
            f"Expected parsing >= 40 keys, but got only {len(_GO_RETENTION)}. "
            "Check regex parser robustness against keys.go formatting!"
        )

    def test_py_retention_keys_are_known_constants(self) -> None:
        """
        Every key in STREAM_RETENTION must be the value of an RS.* field.
        Guards against bare string literals being used as keys.
        """
        orphans = [k for k in _PY_RETENTION if k not in _ALL_STREAM_VALUES]
        assert not orphans, (
            f"STREAM_RETENTION contains keys that are NOT RS.* field values "
            f"(possible string literals or typos): {orphans}"
        )

    @pytest.mark.parametrize("stream_val", _ALL_KEYS)
    def test_retention_value_parity(self, stream_val: str) -> None:
        """
        For every stream key, maxlen must be identical in Go and Python.
        Missing from one side → test fails (merge blocker).
        """
        if not _GO_RETENTION:
            pytest.skip("Go StreamRetention could not be loaded")

        in_go = stream_val in _GO_RETENTION
        in_py = stream_val in _PY_RETENTION

        if in_go and not in_py:
            pytest.fail(
                f"Stream key '{stream_val}' has MAXLEN={_GO_RETENTION[stream_val]} "
                f"in Go StreamRetention but is MISSING from Python STREAM_RETENTION."
            )

        if in_py and not in_go:
            pytest.fail(
                f"Stream key '{stream_val}' has MAXLEN={_PY_RETENTION[stream_val]} "
                f"in Python STREAM_RETENTION but is MISSING from Go StreamRetention."
            )

        if in_go and in_py:
            go_val = _GO_RETENTION[stream_val]
            py_val = _PY_RETENTION[stream_val]
            assert go_val == py_val, (
                f"MAXLEN mismatch for stream '{stream_val}': "
                f"Go={go_val}, Python={py_val}. "
                f"Both maps must be updated together."
            )
