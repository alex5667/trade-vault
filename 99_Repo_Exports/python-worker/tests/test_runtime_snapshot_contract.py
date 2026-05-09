import ast
from pathlib import Path

ALIAS = {

    "pressure": "pressure_hi",

    "last_wp": "last_wp_weak_any",

}



REQUIRED_SNAPSHOT_KEYS = {
    "last_obi_event",
    "last_iceberg_event",
    "last_ofi_event",
    "last_bar",
    "last_fp_edge",
    "last_div",
    "last_regime",
    "book_churn_hi",
    "dynamic_cfg",
    "pressure_hi",
    "cont_ctx_ts_ms",
    "liq_regime",
    "last_sweep",
    "last_reclaim",
    "last_wp_weak_any",
    "book_churn_score",
    "book_rate_z",
    "book_state",
    "hawkes_snapshot",
    "l3_stats",
    "last_book",
    "last_of_confirm_have_need_ratio",
    "last_spread_z",
    "pressure_sps",
    "prev_book",
}



def test_runtime_snapshot_contract_matches_engine_getattr():

    root = Path(__file__).resolve().parents[1]  # python-worker/

    engine_path = root / "core" / "of_confirm_engine.py"

    src = engine_path.read_text(encoding="utf-8")

    tree = ast.parse(src)


    attrs = set()

    for node in ast.walk(tree):

        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "getattr":

            if len(node.args) >= 2:

                a0, a1 = node.args[0], node.args[1]

                if isinstance(a0, ast.Name) and a0.id == "runtime" and isinstance(a1, ast.Constant) and isinstance(a1.value, str):

                    attrs.add(a1.value)


    # Map via alias where needed

    snap_keys = set()

    for a in attrs:

        snap_keys.add(ALIAS.get(a, a))


    # Snapshot must contain everything engine reads (mapped)

    missing = sorted(list(snap_keys - REQUIRED_SNAPSHOT_KEYS))

    assert not missing, f"runtime_snapshot missing keys required by engine getattr(runtime, ...): {missing}"
