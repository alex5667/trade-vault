from __future__ import annotations

import json
from pathlib import Path

from ml_analysis.tools.autogen_feature_denylist_proposal_v1 import _load_drift_hints, _select_candidates


def test_load_drift_hints_and_boost_candidates(tmp_path: Path) -> None:
    rep = {
        'features': [
            {
                'feature': 'n_depth_slope_bid'
                'flag_warn': 1
                'flag_crit': 1
                'denylist_suggested': 1
                'shadow_disable_suggested': 1
                'psi': 0.5
                'ks_stat': 0.4
            }
        ]
    }
    path = tmp_path / 'drift.json'
    path.write_text(json.dumps(rep), encoding='utf-8')
    hints = _load_drift_hints(path)
    cands = _select_candidates(
        rows=[{'feature': 'n:depth_slope_bid', 'global_perm_auc_drop': '0.0', 'regime_cv': '1.0'}]
        extras_num={'depth_slope_bid'}
        extras_bool=set()
        v5_num={'depth_slope_bid'}
        v5_bool=set()
        max_features=5
        min_importance=0.01
        max_cv=0.5
        drift_hints=hints
    )
    assert cands
    assert 'drift_denylist_suggested=1' in cands[0].reason
