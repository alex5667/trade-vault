from __future__ import annotations

from pathlib import Path


def _extract_block(text: str, marker: str, n_lines: int = 28) -> str:
    lines = text.splitlines()
    for i, ln in enumerate(lines):
        if marker in ln:
            return "\n".join(lines[i:i + n_lines])
    raise AssertionError(f"marker not found: {marker}")


def test_b11_liqmap_gate_prom_counter_emission_present_and_mirrored() -> None:
    """B1.1 wiring contract.

    We intentionally do *not* execute TickProcessor here (heavy imports & runtime deps).
    The contract is enforced as a cheap static check:
      - both SoT + mirror tick_processor include the liqmap gate counter emission block
      - the block stays identical (prevents silent drift)

    This complements the existing A0 mirror policy marker test.
    """

    # Repo layout assumption matches test_mirror_sync_policy_a0.py
    repo_root = Path(__file__).resolve().parents[4]

    sot = repo_root / "python-worker/tick_flow_full/services/orderflow/components/tick_processor.py"
    mirror = repo_root / "python-worker/services/orderflow/components/tick_processor.py"

    assert sot.exists(), f"missing SoT file: {sot}"
    assert mirror.exists(), f"missing mirror file: {mirror}"

    sot_text = sot.read_text(encoding="utf-8", errors="replace")
    mirror_text = mirror.read_text(encoding="utf-8", errors="replace")

    marker = "LiqMap gate counter emission (B1.1)"

    for text, name in ((sot_text, "SoT"), (mirror_text, "mirror")):
        assert marker in text, f"{name} tick_processor missing B1.1 marker"
        assert "liqmap_gate_shadow_hit_total" in text, f"{name} tick_processor missing shadow counter"
        assert "liqmap_gate_veto_total" in text, f"{name} tick_processor missing veto counter"

    sot_block = _extract_block(sot_text, marker)
    mirror_block = _extract_block(mirror_text, marker)
    assert sot_block == mirror_block, (
        "SoT and mirror differ in B1.1 liqmap gate prom emission block. "
        "Sync the mirrored tick_processor files (see MIRROR_SYNC_POLICY.md)."
    )
