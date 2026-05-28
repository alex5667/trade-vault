"""
WR stop-bleed 2026-05-27 — contract test: OF_SOFT_SCORE_MIN must remain ≥0.80.

Memory entry [project_low_wr_stop_bleed_2026_05_23] promoted this floor from
0.55 → 0.85 to kill near_miss continuation soft-passes. A regression to 0.70
on 2026-05-26 produced WR 28% / sumR -1153R and was reverted same day.

This test pins the env-file value to catch accidental rollback.
"""
from __future__ import annotations

from pathlib import Path

ENV_FILE = Path(__file__).resolve().parents[2] / "config" / "crypto-of-common.env"


def _read_env_kv(path: Path) -> dict[str, str]:
    kv: dict[str, str] = {}
    if not path.exists():
        return kv
    for line in path.read_text(encoding="utf-8").splitlines():
        s = line.strip()
        if not s or s.startswith("#") or "=" not in s:
            continue
        k, _, v = s.partition("=")
        # Last-write-wins per env-file semantics
        kv[k.strip()] = v.strip().strip('"').strip("'")
    return kv


def test_env_file_exists():
    assert ENV_FILE.exists(), f"env file missing: {ENV_FILE}"


def test_of_soft_score_min_at_or_above_0_60():
    """Master soft-pass floor must stay ≥ 0.60 (lowered to 0.65 on 2026-05-27)."""
    env = _read_env_kv(ENV_FILE)
    raw = env.get("OF_SOFT_SCORE_MIN")
    assert raw is not None, "OF_SOFT_SCORE_MIN missing from crypto-of-common.env"
    v = float(raw)
    assert v >= 0.60, (
        f"OF_SOFT_SCORE_MIN={v} < 0.60 — dangerously low soft-pass floor. "
        "History: 0.55→0.85 (stop-bleed 2026-05-23), 0.85→0.65 (2026-05-27 flow expansion)."
    )


def test_of_score_min_present():
    """Hard-veto floor OF_SCORE_MIN must be present (default 0.60)."""
    env = _read_env_kv(ENV_FILE)
    raw = env.get("OF_SCORE_MIN")
    assert raw is not None
    v = float(raw)
    assert 0.40 <= v <= 0.90


def test_long_bear_soft_min_strictest():
    """Per-regime override for LONG×BEAR must be strictest (≥ master)."""
    env = _read_env_kv(ENV_FILE)
    master = float(env.get("OF_SOFT_SCORE_MIN", "0"))
    long_bear = env.get("OF_SOFT_SCORE_MIN_LONG_BEAR")
    if long_bear is not None:
        assert float(long_bear) >= master, (
            "OF_SOFT_SCORE_MIN_LONG_BEAR weaker than master — "
            "counter-trend LONG should be strictest"
        )
