from __future__ import annotations

import csv
import json
from pathlib import Path

from services.nightly.feature_drift_report_v1 import build_feature_drift_report, main


def _write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    cols = list(rows[0].keys())
    with path.open('w', encoding='utf-8', newline='') as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for r in rows:
            w.writerow(r)


def test_feature_drift_report_stable_vs_shift(tmp_path: Path) -> None:
    ref = tmp_path / 'ref.csv'
    cur = tmp_path / 'cur.csv'
    _write_csv(ref, [
        {'ofi_norm': 0.1, 'dw_obi': 0.2, 'spread_bps': 5.0, 'depth_slope_bid': 1.0},
        {'ofi_norm': 0.2, 'dw_obi': 0.1, 'spread_bps': 4.9, 'depth_slope_bid': 1.1},
        {'ofi_norm': 0.0, 'dw_obi': 0.2, 'spread_bps': 5.1, 'depth_slope_bid': 1.0},
    ] * 100)
    _write_csv(cur, [
        {'ofi_norm': 2.0, 'dw_obi': 0.1, 'spread_bps': 9.0, 'depth_slope_bid': 0.1},
        {'ofi_norm': 2.1, 'dw_obi': 0.2, 'spread_bps': 9.2, 'depth_slope_bid': 0.2},
        {'ofi_norm': 1.9, 'dw_obi': 0.0, 'spread_bps': 8.8, 'depth_slope_bid': 0.1},
    ] * 100)

    rep = build_feature_drift_report(reference_path=str(ref), current_path=str(cur))
    assert rep['summary']['features_evaluated'] >= 3
    feats = {r['feature']: r for r in rep['features']}
    assert feats['ofi_norm']['flag_crit'] == 1
    assert feats['spread_bps']['flag_warn'] == 1


def test_feature_drift_report_cli_writes_json_and_csv(tmp_path: Path) -> None:
    ref = tmp_path / 'ref.csv'
    cur = tmp_path / 'cur.csv'
    _write_csv(ref, [{'ofi_norm': 0.0, 'dw_obi': 0.1}] * 100)
    _write_csv(cur, [{'ofi_norm': 0.0, 'dw_obi': 0.1}] * 100)
    out_json = tmp_path / 'report.json'
    out_csv = tmp_path / 'report.csv'
    rc = main([
        '--reference_path', str(ref),
        '--current_path', str(cur),
        '--out_json', str(out_json),
        '--out_csv', str(out_csv),
    ])
    assert rc == 0
    obj = json.loads(out_json.read_text(encoding='utf-8'))
    assert obj['tool'] == 'feature_drift_report_v1'
    assert out_csv.exists()
