# -*- coding: utf-8 -*-
"""Mirror sync test for D1 (apply_liqmap_tp_sl_adjustment).

We keep the overlay logic 1:1 between:
  - tick_flow_full/services/orderflow/liqmap_features.py (SoT)
  - services/orderflow/liqmap_features.py (mirror)

This reduces Train!=Serve drift and avoids silent behavioral differences.
"""

from pathlib import Path



def _extract_block(text: str) -> str:
    begin = "# --- apply_liqmap_tp_sl_adjustment v1 (MIRROR SYNC D1) BEGIN ---"
    end = "# --- apply_liqmap_tp_sl_adjustment v1 (MIRROR SYNC D1) END ---"
    i = text.find(begin)
    j = text.find(end)
    assert i >= 0, "BEGIN marker missing"
    assert j >= 0, "END marker missing"
    assert j > i, "invalid markers order"
    return text[i : j + len(end)].strip() + "\n"


def test_d1_overlay_block_is_identical_between_sot_and_mirror():
    # The repo typically looks like:
    #   <repo_root>/python-worker/services/orderflow/tests/...
    repo_root = Path(__file__).resolve().parents[4]
    pw = repo_root / "python-worker"

    sot = pw / "tick_flow_full" / "services" / "orderflow" / "liqmap_features.py"
    mirror = pw / "services" / "orderflow" / "liqmap_features.py"

    assert sot.exists(), f"SoT file missing: {sot}"
    assert mirror.exists(), f"Mirror file missing: {mirror}"

    sot_block = _extract_block(sot.read_text(encoding="utf-8"))
    mir_block = _extract_block(mirror.read_text(encoding="utf-8"))

    assert sot_block == mir_block
