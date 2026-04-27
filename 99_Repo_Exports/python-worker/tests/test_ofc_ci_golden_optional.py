from __future__ import annotations

import os
import subprocess
from pathlib import Path


def test_ci_strict_replay_on_env_golden_capture():
    """
    Optional CI test.

    If OFC_GOLDEN_CAPTURE_PATH is set (nightly/CI), run strict replay against that golden NDJSON.
    Otherwise skip.
    """
    p = os.getenv("OFC_GOLDEN_CAPTURE_PATH", "").strip()
    if not p:
        return
    path = Path(p)
    assert path.exists(), f"OFC_GOLDEN_CAPTURE_PATH does not exist: {path}"

    # Strict replay: mismatch => tool exits 2.
    r = subprocess.run(
        ["python", "tools/ofc_replay.py", "--input", str(path), "--strict"],
        capture_output=True,
        text=True,
    )
    if r.returncode != 0:
        raise AssertionError(f"strict replay failed rc={r.returncode}\nstdout:\n{r.stdout}\nstderr:\n{r.stderr}")

