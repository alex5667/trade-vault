"""Unit tests for nightly_golden_replay_job_v1: prune_old and _iter_policy_dirs."""

from datetime import UTC, datetime, timedelta
from pathlib import Path


def _make_dated_dir(base: Path, name: str) -> Path:
    d = base / name
    d.mkdir(parents=True, exist_ok=True)
    (d / "dummy.txt").write_text("x")
    sub = d / "sub"
    sub.mkdir()
    (sub / "file.ndjson").write_text("{}\n")
    return d


def test_prune_old_removes_stale_dirs(tmp_path: Path):
    from ml_analysis.tools.nightly_golden_replay_job_v1 import prune_old

    now_utc = datetime.now(tz=UTC)

    # Create old dir (15d ago) and recent dir (2d ago)
    old_name = (now_utc - timedelta(days=15)).strftime("%Y%m%d")
    recent_name = (now_utc - timedelta(days=2)).strftime("%Y%m%d")

    old_dir = _make_dated_dir(tmp_path, old_name)
    recent_dir = _make_dated_dir(tmp_path, recent_name)

    prune_old(tmp_path, keep_days=10)

    assert not old_dir.exists(), f"Old dir {old_name} should have been pruned"
    assert recent_dir.exists(), f"Recent dir {recent_name} should survive"


def test_prune_old_keep_days_zero_is_noop(tmp_path: Path):
    from ml_analysis.tools.nightly_golden_replay_job_v1 import prune_old

    now_utc = datetime.now(tz=UTC)
    old_name = (now_utc - timedelta(days=30)).strftime("%Y%m%d")
    old_dir = _make_dated_dir(tmp_path, old_name)

    prune_old(tmp_path, keep_days=0)
    assert old_dir.exists(), "keep_days=0 should be a no-op"


def test_prune_old_ignores_non_date_dirs(tmp_path: Path):
    from ml_analysis.tools.nightly_golden_replay_job_v1 import prune_old

    misc_dir = tmp_path / "policy_abc"
    misc_dir.mkdir()
    (misc_dir / "file.txt").write_text("keep me")

    prune_old(tmp_path, keep_days=1)
    assert misc_dir.exists(), "Non-date dirs must not be touched"


def test_iter_policy_dirs_returns_sorted(tmp_path: Path):
    from ml_analysis.tools.nightly_golden_replay_job_v1 import _iter_policy_dirs

    day = "20260304"
    base = tmp_path / day
    (base / "policy_b").mkdir(parents=True)
    (base / "policy_a").mkdir()
    (base / "other").mkdir()  # should be excluded

    result = _iter_policy_dirs(tmp_path, day)
    names = [p.name for p in result]
    assert names == ["policy_a", "policy_b"]
    assert "other" not in names


def test_concat_ndjson_guards_missing_file(tmp_path: Path):
    from ml_analysis.tools.nightly_golden_replay_job_v1 import _concat_ndjson

    # One real file, one non-existent
    real = tmp_path / "real.ndjson"
    real.write_text('{"a":1}\n{"b":2}\n', encoding="utf-8")
    missing = tmp_path / "ghost.ndjson"

    out = tmp_path / "merged.ndjson"
    n = _concat_ndjson([real, missing], out, limit=0)

    # Should not raise; real file lines are counted
    assert n == 2
    assert out.exists()
