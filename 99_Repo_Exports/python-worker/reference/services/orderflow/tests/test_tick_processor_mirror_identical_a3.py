from __future__ import annotations

from pathlib import Path


def _read(p: Path) -> str:
    # Normalize newlines and trailing whitespace to avoid false positives from editors.
    return "\n".join([ln.rstrip() for ln in p.read_text(encoding="utf-8", errors="replace").splitlines()]) + "\n"


def test_a3_tick_processor_mirror_is_identical() -> None:
    """A3: enforce strict 1:1 mirror equivalence for TickProcessor.

    After A3 we intentionally keep the two TickProcessor implementations identical:
      - SoT:    python-worker/tick_flow_full/services/orderflow/components/tick_processor.py
      - mirror: python-worker/services/orderflow/components/tick_processor.py

    Rationale: Train==Serve, deterministic DQ + gates, and lower review burden.

    If this fails, apply the change to BOTH files or re-run the mirror sync patch.
    """

    repo_root = Path(__file__).resolve().parents[4]

    sot = repo_root / "python-worker/tick_flow_full/services/orderflow/components/tick_processor.py"
    mirror = repo_root / "python-worker/services/orderflow/components/tick_processor.py"

    assert sot.exists(), f"missing SoT file: {sot}"
    assert mirror.exists(), f"missing mirror file: {mirror}"

    sot_text = _read(sot)
    mirror_text = _read(mirror)

    assert sot_text == mirror_text, (
        "TickProcessor files are not identical (SoT vs mirror). "
        "Keep them 1:1 to prevent train/serve drift."
    )
