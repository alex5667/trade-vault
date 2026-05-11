import importlib.util
import sys
from pathlib import Path

mod_path = Path(__file__).parent.parent / 'execution_contracts.py'
spec = importlib.util.spec_from_file_location('execution_contracts_p104', mod_path)
mod = importlib.util.module_from_spec(spec)  # type: ignore
sys.modules[spec.name] = mod  # type: ignore
assert spec.loader is not None  # type: ignore
spec.loader.exec_module(mod)  # type: ignore


def test_materialized_state_builds_nested_algo_refs():
    doc = mod.build_materialized_state_view({
        'binance_order_id': 11,
        'entry_client_order_id': 'ent-1',
        'sl_algo_id': 21,
        'sl_client_algo_id': 'sl-1',
        'tp1_algo_id': 31,
        'tp1_client_algo_id': 'tp-1',
        'tp2_algo_id': 32,
        'tp2_client_algo_id': 'tp-2',
        'trail_algo_id': 41,
        'trail_client_id': 'tr-1',
    })
    assert doc['entry']['order_id'] == 11
    assert doc['protective']['sl_algo_id'] == 21
    assert doc['protective']['tp_algo_ids'] == [31, 32]
    assert doc['trailing']['algo_id'] == 41
    assert doc['state_schema_ver'] == 'execution_state:v2'
