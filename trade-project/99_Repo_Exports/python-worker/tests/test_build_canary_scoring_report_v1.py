from pathlib import Path


def test_canary_script_exists():
    assert (Path(__file__).resolve().parents[2] / 'scripts' / 'build_canary_scoring_report.py').exists()
