"""Tests for ML_CONFIRM_ENFORCE_KINDS_CSV canary scope filter (P1.2, 2026-05-26).

The filter scopes the SHADOW→ENFORCE canary promotion in `services/ml_confirm/gate.py`
to a CSV-allowlist of kinds. When unset/empty, behaviour is unchanged.
"""

from __future__ import annotations

import os
import re
import sys


def _read_gate_source() -> str:
    path = os.path.join(
        os.path.dirname(__file__), "..", "services", "ml_confirm", "gate.py"
    )
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


def test_kinds_csv_block_lives_inside_canary_branch():
    """ML_CONFIRM_ENFORCE_KINDS_CSV must be read inside the SHADOW canary branch
    BEFORE the u01 routing, so non-allowlisted kinds skip the promotion entirely."""
    src = _read_gate_source()
    assert "ML_CONFIRM_ENFORCE_KINDS_CSV" in src, "env var must be referenced"
    # Locate the canary block: 'if effective_mode == "SHADOW":' through u01 check.
    m = re.search(
        r'if effective_mode == "SHADOW":.*?_stable_u01',
        src,
        flags=re.DOTALL,
    )
    assert m is not None, "expected SHADOW canary block in gate source"
    block = m.group(0)
    assert "ML_CONFIRM_ENFORCE_KINDS_CSV" in block, (
        "kinds filter must be evaluated before u01 routing"
    )
    # And the filter must zero-out enforce_share for non-matching kinds.
    assert "enforce_share = 0.0" in block, (
        "kinds filter must clamp enforce_share to 0.0 on miss"
    )


def test_kinds_csv_filter_semantics_empty_is_passthrough():
    """Empty CSV → all kinds eligible (legacy behaviour)."""
    src = _read_gate_source()
    # The filter must be gated on `if kinds_csv:` so empty CSV is no-op.
    m = re.search(
        r'kinds_csv\s*=\s*os\.getenv\("ML_CONFIRM_ENFORCE_KINDS_CSV"[^)]*\)\.strip\(\)\s*\n\s*if kinds_csv:',
        src,
    )
    assert m is not None, "empty CSV must be a passthrough (legacy behaviour)"


def test_kinds_csv_filter_normalizes_case_and_whitespace():
    """CSV parsing must lowercase and strip whitespace to match kind.lower()."""
    src = _read_gate_source()
    # Look for the parse: {k.strip().lower() for k in kinds_csv.split(",") if k.strip()}
    m = re.search(
        r'\{[^}]*k\.strip\(\)\.lower\(\)\s+for\s+k\s+in\s+kinds_csv\.split\(","\)[^}]*\}',
        src,
    )
    assert m is not None, "CSV must be normalized to lowercase + stripped"
    # And match against kind.lower()
    assert "kind.lower() not in allowed" in src, (
        "comparison must use kind.lower() against normalized allowlist"
    )


if __name__ == "__main__":
    test_kinds_csv_block_lives_inside_canary_branch()
    test_kinds_csv_filter_semantics_empty_is_passthrough()
    test_kinds_csv_filter_normalizes_case_and_whitespace()
    print("OK")
