# -*- coding: utf-8 -*-
"""
Regression: XADD MAXLEN enforcement lint check (merge-blocker).

Scans all Python source files for `xadd(` or `.xadd(` calls and verifies
each includes a `maxlen=` argument. Unbounded XADD is the primary cause
of Redis OOM in production.

Run:
    cd python-worker && python -m pytest tests/test_xadd_maxlen_lint.py -v
"""
from __future__ import annotations

import os
import re
import pytest


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

# Directories to scan
SCAN_DIRS = [
    os.path.join(os.path.dirname(__file__), ".."),  # python-worker root
]

# Files/patterns to exclude (test mocks, etc.)
EXCLUDE_PATTERNS = [
    "tests/",
    "/test_",
    "scripts/",
    "reference/",
    "__pycache__",
    ".pyc",
    "fake_redis",
    "fakeredis",
]

# Acceptable MAXLEN patterns
MAXLEN_RE = re.compile(r"maxlen\s*=", re.IGNORECASE)

# XADD call pattern — captures simple `.xadd(` or `xadd(` invocations
XADD_RE = re.compile(r"\.xadd\s*\(", re.IGNORECASE)


# ---------------------------------------------------------------------------
# Scanner
# ---------------------------------------------------------------------------

def _find_unbounded_xadd() -> list[tuple[str, int, str]]:
    """Return list of (filepath, line_no, line_content) for XADD without MAXLEN."""
    violations = []

    for scan_dir in SCAN_DIRS:
        abs_dir = os.path.abspath(scan_dir)
        for root, _dirs, files in os.walk(abs_dir):
            for fname in files:
                if not fname.endswith(".py"):
                    continue

                fpath = os.path.join(root, fname)
                rel = os.path.relpath(fpath, abs_dir)

                # Check exclusions
                if any(excl in rel or excl in fpath for excl in EXCLUDE_PATTERNS):
                    continue

                try:
                    lines = open(fpath, encoding="utf-8", errors="ignore").readlines()
                except IOError:
                    continue

                for i, line in enumerate(lines, 1):
                    if XADD_RE.search(line):
                        # Look in the current line and the next 5 lines for maxlen=
                        context = "".join(lines[i - 1 : min(i + 5, len(lines))])
                        if not MAXLEN_RE.search(context):
                            violations.append((rel, i, line.strip()))

    return violations


# ---------------------------------------------------------------------------
# Test
# ---------------------------------------------------------------------------

class TestXAddMaxlenEnforcement:
    def test_no_unbounded_xadd(self) -> None:
        """Every XADD call in production code MUST include maxlen=."""
        violations = _find_unbounded_xadd()
        if violations:
            report = "\n".join(
                f"  {f}:{ln}: {code}" for f, ln, code in violations
            )
            pytest.fail(
                f"Found {len(violations)} XADD call(s) without maxlen=:\n{report}\n\n"
                f"Fix: add maxlen=EXEC_STREAM_MAXLEN (50000) to each XADD call."
            )

    def test_scanner_has_python_files(self) -> None:
        """Sanity: at least 10 .py files should be scanned."""
        count = 0
        for scan_dir in SCAN_DIRS:
            for root, _, files in os.walk(os.path.abspath(scan_dir)):
                for f in files:
                    if f.endswith(".py"):
                        rel = os.path.relpath(os.path.join(root, f), scan_dir)
                        if not any(excl in rel for excl in EXCLUDE_PATTERNS):
                            count += 1
        assert count >= 10, f"Expected to scan ≥10 Python files, found {count}"
