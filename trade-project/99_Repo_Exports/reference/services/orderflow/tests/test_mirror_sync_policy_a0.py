from __future__ import annotations

from pathlib import Path


def test_a0_mirror_sync_policy_markers_present() -> None:
    """A0 groundwork: prevent silent loss of mirror sync intent.

    This test is intentionally *lightweight*:
    - It does NOT enforce full 1:1 equivalence of the mirrored files yet.
      (At A0 we only document the policy and add markers; full sync is done in
      later commits where we intentionally align the implementations.)
    - It DOES ensure that both trees keep an explicit, greppable marker so that
      reviewers will notice when a change touches only one side.
    """

    # Locate the repo root relative to this test file.
    # Layout: python-worker/services/orderflow/tests/test_mirror_sync_policy_a0.py
    # → parents[3] == python-worker/, parents[4] == repo root
    repo_root = Path(__file__).resolve().parents[4]

    sot = repo_root / "python-worker/tick_flow_full/services/orderflow/components/tick_processor.py"
    mirror = repo_root / "python-worker/services/orderflow/components/tick_processor.py"

    assert sot.exists(), f"missing SoT file: {sot}"
    assert mirror.exists(), f"missing mirror file: {mirror}"

    sot_text = sot.read_text(encoding="utf-8", errors="replace")
    mirror_text = mirror.read_text(encoding="utf-8", errors="replace")

    marker = "MIRROR SYNC POLICY (A0)"
    for text, name in ((sot_text, "SoT"), (mirror_text, "mirror")):
        assert marker in text, f"{name} file missing mirror policy marker"
        assert "MIRROR_SYNC_POLICY.md" in text, f"{name} file missing policy doc link"
        assert "tick_flow_full/services/orderflow/components/tick_processor.py" in text, (
            f"{name} file missing SoT path reference"
        )
        assert "services/orderflow/components/tick_processor.py" in text, (
            f"{name} file missing mirror path reference"
        )

    # The marker block itself should stay identical at the top of both files.
    # (This does not guarantee full equality of the implementations.)
    sot_head = "\n".join(sot_text.splitlines()[:30])
    mirror_head = "\n".join(mirror_text.splitlines()[:30])
    assert sot_head == mirror_head, (
        "SoT and mirror differ in the first 30 lines — the header marker block "
        "must remain identical. Check MIRROR_SYNC_POLICY.md."
    )
