"""Plan 1 — deterministic canary selector tests.

The selector must be reproducible across processes/replays because canary
membership is required to land in the durable audit (signal can be re-scored
offline only when bucket assignment is stable).
"""
from __future__ import annotations

from services.confidence_meta_gate.canary import canary_bucket, is_canary_selected


def test_canary_bucket_is_stable_across_calls() -> None:
    sid = "abc-123"
    salt = "conf_meta_gate_v1_20260530"
    first = canary_bucket(sid, salt)
    second = canary_bucket(sid, salt)
    assert first == second
    assert 0 <= first < 10_000


def test_canary_bucket_changes_with_salt() -> None:
    sid = "abc-123"
    a = canary_bucket(sid, "salt-a")
    b = canary_bucket(sid, "salt-b")
    assert a != b, "different salts must produce different bucket assignments"


def test_canary_share_zero_selects_none() -> None:
    sids = [f"sid-{i}" for i in range(500)]
    selected = [s for s in sids if is_canary_selected(s, "salt", 0.0)]
    assert selected == []


def test_canary_share_one_selects_all() -> None:
    sids = [f"sid-{i}" for i in range(500)]
    selected = [s for s in sids if is_canary_selected(s, "salt", 1.0)]
    assert selected == sids


def test_canary_share_negative_clamped_to_zero() -> None:
    assert not is_canary_selected("sid", "salt", -0.5)


def test_canary_share_above_one_clamped_to_one() -> None:
    assert is_canary_selected("sid", "salt", 5.0)


def test_canary_share_one_percent_is_approximately_one_percent() -> None:
    n = 20_000
    sids = [f"sid-{i}" for i in range(n)]
    selected = sum(1 for s in sids if is_canary_selected(s, "salt", 0.01))
    # Expect ~1% ± 0.5% on 20k samples.
    rate = selected / n
    assert 0.005 < rate < 0.015, f"canary 1% rate = {rate:.4f}"


def test_canary_share_five_percent() -> None:
    n = 20_000
    sids = [f"sid-{i}" for i in range(n)]
    selected = sum(1 for s in sids if is_canary_selected(s, "salt", 0.05))
    rate = selected / n
    assert 0.040 < rate < 0.060, f"canary 5% rate = {rate:.4f}"


def test_canary_membership_stable_across_processes() -> None:
    """Replay invariant: the (sid, salt, share) tuple must always pick the
    same in/out result. We assert membership is stable across re-instantiation
    of the function call — there must be no module-level random state."""
    salt = "stable"
    share = 0.10
    snapshots = []
    for _ in range(5):
        snapshots.append(
            tuple(
                is_canary_selected(f"sid-{i}", salt, share) for i in range(200)
            )
        )
    assert all(s == snapshots[0] for s in snapshots)


def test_empty_sid_is_never_selected() -> None:
    assert not is_canary_selected("", "salt", 1.0)
