import os
import json
import pytest
import tempfile
from ml_analysis.tools.edge_stack_train_bundle_utils_p59 import compare_with_champion

def test_champion_comparison_rejection():
    """
    Test that a challenger is rejected if its Brier score or ECE regression exceeds the max thresholds.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        champ_bundle_path = os.path.join(tmpdir, "champion_bundle.json")
        
        # Mock champion bundle
        champion_data = {
            "train": {
                "report": {
                    "oof": {
                        "meta": {
                            "brier": 0.1500,
                            "ece": 0.0200,
                        }
                    }
                }
            }
        }
        with open(champ_bundle_path, "w") as f:
            json.dump(champion_data, f)
            
        # Challenger with worse Brier (brier + regression_brier_max exceeded)
        challenger_report_bad = {
            "oof": {
                "meta": {
                    "brier": 0.1600,  # 0.16 > 0.15 + 0.005 threshold
                    "ece": 0.0200,
                }
            }
        }
        
        cmp_result_bad = compare_with_champion(
            challenger_train_report=challenger_report_bad,
            champion_bundle_path=champ_bundle_path,
            brier_max_regression=0.005,
            ece_max_regression=0.005,
        )
        
        assert not cmp_result_bad.should_promote
        assert "regression_blocked" in cmp_result_bad.reason.lower()
        
        # Challenger with good metrics
        challenger_report_good = {
            "oof": {
                "meta": {
                    "brier": 0.1400,  # better than champion
                    "ece": 0.0100,    # better than champion
                }
            }
        }
        
        cmp_result_good = compare_with_champion(
            challenger_train_report=challenger_report_good,
            champion_bundle_path=champ_bundle_path,
            brier_max_regression=0.005,
            ece_max_regression=0.005,
        )
        
        assert cmp_result_good.should_promote
        assert "challenger_wins_or_equal" in cmp_result_good.reason.lower()

def test_champion_comparison_no_champion():
    """
    Test behaviour when no champion exists.
    """
    cmp_result = compare_with_champion(
        challenger_train_report={"oof": {"meta": {"brier": 0.15, "ece": 0.02}}},
        champion_bundle_path="/tmp/nonexistent_champion.json",
        brier_max_regression=0.005,
        ece_max_regression=0.005,
    )
    
    assert cmp_result.should_promote
    assert cmp_result.no_champion is True
    assert cmp_result.reason == "no_champion_bundle_first_run"
