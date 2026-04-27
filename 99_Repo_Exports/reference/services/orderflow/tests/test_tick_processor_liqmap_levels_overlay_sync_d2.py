"""D2: anti-drift sync test — verifies SoT and mirror tick_processor.py
have identical D2_LIQMAP_LEVELS_OVERLAY blocks (delimited by markers).

Run from repo root:
    pytest services/orderflow/tests/test_tick_processor_liqmap_levels_overlay_sync_d2.py
"""
from pathlib import Path


def _extract_block(text: str) -> str:
    begin = "# BEGIN D2_LIQMAP_LEVELS_OVERLAY"
    end = "# END D2_LIQMAP_LEVELS_OVERLAY"
    b = text.find(begin)
    e = text.find(end)
    assert b >= 0, "missing D2 overlay begin marker"
    assert e > b, "missing D2 overlay end marker"
    return text[b : e + len(end)]


def test_tick_processor_liqmap_levels_overlay_block_is_identical_sot_vs_mirror():
    root = Path(__file__).resolve().parents[3]  # repo root
    sot = root / "tick_flow_full" / "services" / "orderflow" / "components" / "tick_processor.py"
    mir = root / "services" / "orderflow" / "components" / "tick_processor.py"

    sot_txt = sot.read_text("utf-8", errors="ignore")
    mir_txt = mir.read_text("utf-8", errors="ignore")

    assert _extract_block(sot_txt) == _extract_block(mir_txt)
