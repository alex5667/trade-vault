from pathlib import Path
import importlib.util
import sys

root = Path(__file__).resolve().parents[2]
if str(root) not in sys.path:
    sys.path.insert(0, str(root))

mod_path = root / 'services' / 'active_symbol_guard_semantics.py'
spec = importlib.util.spec_from_file_location('services.active_symbol_guard_semantics_p8', mod_path)
mod = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = mod
assert spec.loader is not None
spec.loader.exec_module(mod)


def test_guard_view_active_vs_released_tombstone():
    active = mod.guard_view({'symbol': 'btcusdt', 'sid': 'sid-1', 'guard_status': 'active', 'guard_version': 2}, now_ms=2000)
    assert active['symbol'] == 'BTCUSDT'
    assert active['is_blocking'] is True
    assert active['is_released'] is False
    assert active['tombstone_age_ms'] == 0

    released = mod.guard_view({'symbol': 'ETHUSDT', 'sid': 'sid-2', 'guard_status': 'released', 'released_at_ms': 1000}, now_ms=3500)
    assert released['is_blocking'] is False
    assert released['is_released'] is True
    assert released['tombstone_age_ms'] == 2500
    assert mod.active_guard_doc({'symbol': 'ETHUSDT', 'sid': 'sid-2', 'guard_status': 'released'}) == {}
