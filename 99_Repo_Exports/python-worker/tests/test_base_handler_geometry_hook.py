from pathlib import Path


def _read_first_existing(paths):
    for p in paths:
        if p.exists():
            return p.read_text(encoding="utf-8", errors="ignore")
    raise AssertionError(f"None of paths exist: {paths}")


def test_geometry_hook_is_before_signal_generation_comment():
    """
    Structural regression test:
    ensure _update_geometry_liquidity_context(ctx) hook stays right before
    'Signal generation: unified pipeline approach' section in _process_tick bucket-boundary.
    """
    repo_root = Path(__file__).resolve().parents[1]

    # Try common locations (keep test robust to small repo reshuffles)
    # First: legacy monolithic version (deprecated)
    candidates = [
        repo_root / "handlers" / "base_orderflow_handler.py",  # Correct modular version (PRIMARY)
        repo_root / "orderflow" / "base_handler_legacy.py",
        repo_root / "base_handler.py",  # Fallback
    ]
    text = _read_first_existing(candidates)

    # Find the section header and ensure the hook appears shortly above it.
    marker = "Bucket boundary финализация + генерация сигналов"
    idx = text.find(marker)
    assert idx != -1, "Marker comment not found; update test if comment changed."

    window = text[max(0, idx - 1200) : idx]
    assert "_update_geometry_liquidity_context(ctx)" in window

    # Minimal order check: hook line must appear after ctx formed section and before marker.
    # (We only enforce relative order vs marker to avoid brittle exact-line assertions.)
    hook_pos = window.rfind("_update_geometry_liquidity_context(ctx)")
    assert hook_pos != -1

    # Ensure it's guarded (fail-open) to avoid breaking non-crypto handlers.
    assert "hasattr(self, \"_update_geometry_liquidity_context\")" in window or \
           "hasattr(self, '_update_geometry_liquidity_context')" in window
