from __future__ import annotations
"""Smoke tests for v7 schema wiring in ML analysis CLI tools.

We keep this test intentionally lightweight:
  - '--help' should work for each CLI tool (argparse choices include v7).
  - a minimal dataset build (inputs+closed) with --emit-wide-cols=1 and --schema-ver=v7_of
    should produce parquet + summary json without crashing on FeatureRegistry.
"""


import json
import os
from pathlib import Path
import sys

import pytest


# Ensure tick_flow_full is importable as top-level packages (core/, services/, common/).
_REPO_ROOT = Path(__file__).resolve().parents[2]
_TFF = _REPO_ROOT / "tick_flow_full"
if _TFF.is_dir() and str(_TFF) not in sys.path:
    sys.path.insert(0, str(_TFF))


def _expect_help_ok(main_fn, *, takes_argv: bool) -> None:
    if takes_argv:
        with pytest.raises(SystemExit) as e:
            main_fn(["--help"])
        assert int(e.value.code or 0) == 0
        return

    # Some tools use sys.argv directly.
    old_argv = list(sys.argv)
    try:
        sys.argv = [old_argv[0], "--help"]
        with pytest.raises(SystemExit) as e:
            main_fn()
        assert int(e.value.code or 0) == 0
    finally:
        sys.argv = old_argv


def test_v7_schema_choices_help_smoke() -> None:
    # Import via package path (ml_analysis is a package; conftest adds tick_flow_full to sys.path).
    from ml_analysis.tools import build_dataset_from_inputs_outcomes_v2 as t_build
    from ml_analysis.tools import build_edge_stack_dataset_from_redis as t_redis
    from ml_analysis.tools import feature_selection_loop_v1 as t_fs
    from ml_analysis.tools import train_edge_stack_v1_oof as t_train
    from ml_analysis.tools import nightly_edge_stack_v1_train_bundle as t_bundle
    from ml_analysis.tools import nightly_feature_selection_loop_bundle_v1 as t_fs_bundle

    _expect_help_ok(t_build.main, takes_argv=False)
    _expect_help_ok(t_redis.main, takes_argv=True)
    _expect_help_ok(t_fs.main, takes_argv=True)
    _expect_help_ok(t_bundle.main, takes_argv=True)
    _expect_help_ok(t_fs_bundle.main, takes_argv=True)
    with pytest.raises(SystemExit) as e:
        # train tool exits via argparse on --help
        t_train.main(["--help"])  # type: ignore
    assert int(e.value.code or 0) == 0


def test_build_dataset_wide_cols_v7_smoke(tmp_path: Path) -> None:
    from ml_analysis.tools import build_dataset_from_inputs_outcomes_v2 as t_build

    inputs_path = tmp_path / "inputs.ndjson"
    closed_path = tmp_path / "closed.ndjson"
    out_path = tmp_path / "ds.csv"

    sid = "crypto-of:BTCUSDT:1700000000000"
    ts_ms = 1_700_000_000_000

    # minimal signal/input row; indicators are taken from the whole object
    inputs_rows = [
        {
            "sid": sid,
            "ts_ms": ts_ms,
            "symbol": "BTCUSDT",
            "direction": "LONG",
            "scenario_v4": "trend",
            # a few indicator keys that exist in v5/v6/v7
            "delta_z": 0.1,
            "ofi_z": 0.2,
            "spread_bps": 3.0,
            "cancel_spike_veto": False,
        }
    ]
    closed_rows = [
        {
            "sid": sid,
            "event_type": "POSITION_CLOSED",
            "pnl": 1.0,
            "risk_usd": 1.0,
            "exit_ts_ms": ts_ms + 60_000,
        }
    ]

    inputs_path.write_text("\n".join(json.dumps(r, ensure_ascii=False) for r in inputs_rows) + "\n", encoding="utf-8")
    closed_path.write_text("\n".join(json.dumps(r, ensure_ascii=False) for r in closed_rows) + "\n", encoding="utf-8")

    # Ensure tick_flow_full and repo root are discoverable for imports in the tool.
    # conftest already inserts tick_flow_full into sys.path for the test process,
    # but the tool itself relies on optional imports; keep env consistent.
    env_backup = dict(os.environ)
    os.environ["ML_FEATURE_SCHEMA_VER"] = "v7_of"

    try:
        old_argv = list(sys.argv)
        try:
            sys.argv = [
                old_argv[0],
                "--inputs",
                str(inputs_path),
                "--closed",
                str(closed_path),
                "--out",
                str(out_path),
                "--emit-wide-cols",
                "1",
                "--schema-ver",
                "v7_of",
                "--out-format",
                "csv",
            ]
            t_build.main()
        finally:
            sys.argv = old_argv
    finally:
        os.environ.clear()
        os.environ.update(env_backup)

    assert out_path.exists()
    summary_path = Path(str(out_path) + ".json")
    assert summary_path.exists()

    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    assert summary.get("emit_wide_cols") == 1
    assert str(summary.get("schema_ver")) in ("v7_of", "v7")
    # when registry is available, schema_hash should be included
    assert "schema_hash" in summary
