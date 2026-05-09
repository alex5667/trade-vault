import json

from ml_analysis.tools.edge_stack_train_bundle_utils_p59 import (
    compare_with_champion,
)


def test_promote_blocked_on_brier_regression(tmp_path):
    # Setup champion bundle
    champ_path = tmp_path / "champion.json"
    champ_data = {
        "train": {
            "report": {
                "oof": {"meta": {"brier": 0.150, "ece": 0.050}}
            }
        }
    }
    champ_path.write_text(json.dumps(champ_data))

    # Challenger report
    challenger_rep = {
        "oof": {"meta": {"brier": 0.156, "ece": 0.050}} # brier regression by 0.006 (max is 0.005)
    }

    cmp = compare_with_champion(
        challenger_train_report=challenger_rep,
        champion_bundle_path=str(champ_path),
        brier_max_regression=0.005,
        ece_max_regression=0.010,
    )

    assert not cmp.should_promote
    assert "brier_regression" in cmp.reason


def test_promote_blocked_on_ece_regression(tmp_path):
    champ_path = tmp_path / "champion.json"
    champ_data = {
        "train": {
            "report": {
                "oof": {"meta": {"brier": 0.150, "ece": 0.050}}
            }
        }
    }
    champ_path.write_text(json.dumps(champ_data))

    challenger_rep = {
        "oof": {"meta": {"brier": 0.150, "ece": 0.061}} # ece regression by 0.011 (max is 0.010)
    }

    cmp = compare_with_champion(
        challenger_train_report=challenger_rep,
        champion_bundle_path=str(champ_path),
        brier_max_regression=0.005,
        ece_max_regression=0.010,
    )

    assert not cmp.should_promote
    assert "ece_regression" in cmp.reason


def test_promote_passes_both_gates(tmp_path):
    champ_path = tmp_path / "champion.json"
    champ_data = {
        "train": {
            "report": {
                "oof": {"meta": {"brier": 0.150, "ece": 0.050}}
            }
        }
    }
    champ_path.write_text(json.dumps(champ_data))

    challenger_rep = {
        "oof": {"meta": {"brier": 0.151, "ece": 0.055}} # within regressions
    }

    cmp = compare_with_champion(
        challenger_train_report=challenger_rep,
        champion_bundle_path=str(champ_path),
        brier_max_regression=0.005,
        ece_max_regression=0.010,
    )

    assert cmp.should_promote
    assert "challenger_wins_or_equal" in cmp.reason

def test_promote_first_run_no_champion(tmp_path):
    champ_path = tmp_path / "does_not_exist.json"

    challenger_rep = {
        "oof": {"meta": {"brier": 0.150, "ece": 0.050}}
    }

    cmp = compare_with_champion(
        challenger_train_report=challenger_rep,
        champion_bundle_path=str(champ_path),
        brier_max_regression=0.005,
        ece_max_regression=0.010,
    )

    assert cmp.should_promote
    assert "no_champion" in cmp.reason
