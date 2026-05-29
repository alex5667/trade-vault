"""test_promote_shadow_to_v15_of.py — pure-Python tests for the promotion tool.

Covers:
  1. _evaluate_readiness with all keys ready → group.ready=True.
  2. _evaluate_readiness with one dead key → group.ready=False, dead listed.
  3. _update_dwell_tracking starts a fresh ready_since when group becomes ready.
  4. _update_dwell_tracking resets to 0 when group flips back to not-ready.
  5. _eligible_groups respects the dwell window strictly.
  6. _render_text_report contains the expected sections.
"""
from __future__ import annotations

from typing import Any


class _FakeRedis:
    """Minimal redis stub for dwell tracking."""

    def __init__(self, initial: dict[str, dict[str, str]] | None = None) -> None:
        self.hashes: dict[str, dict[str, str]] = initial or {}

    def hgetall(self, key: str) -> dict[str, str]:
        return dict(self.hashes.get(key, {}))

    def pipeline(self, *, transaction: bool = False) -> "_FakeRedis":
        return self  # piggyback; pipe ops apply immediately for the fake

    def hset(self, key: str, mapping: dict[str, str]) -> None:
        h = self.hashes.setdefault(key, {})
        h.update({str(k): str(v) for k, v in mapping.items()})

    def hdel(self, key: str, *fields: str) -> None:
        h = self.hashes.get(key, {})
        for f in fields:
            h.pop(f, None)

    def execute(self) -> None:
        pass

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


def test_evaluate_readiness_all_ready():
    from tools.promote_shadow_to_v15_of_v1 import _evaluate_readiness
    groups = {"g1": ("k1", "k2", "k3")}
    out = _evaluate_readiness(groups, dead_keys={}, gate_floor=0.95)
    assert out["g1"]["ready"] is True
    assert out["g1"]["dead_keys"] == []
    assert out["g1"]["ready_keys"] == 3
    assert out["g1"]["total"] == 3


def test_evaluate_readiness_one_dead():
    from tools.promote_shadow_to_v15_of_v1 import _evaluate_readiness
    groups = {"g1": ("k1", "k2", "k3")}
    dead = {"k2": 0.30}
    out = _evaluate_readiness(groups, dead, gate_floor=0.95)
    assert out["g1"]["ready"] is False
    assert out["g1"]["dead_keys"] == [("k2", 0.30)]
    assert out["g1"]["ready_keys"] == 2


def test_evaluate_readiness_key_above_floor_not_dead():
    from tools.promote_shadow_to_v15_of_v1 import _evaluate_readiness
    groups = {"g1": ("k1",)}
    # 0.97 > 0.95 floor — even though it's in dead_keys hash (older snapshot),
    # the live coverage is above floor → treat as ready.
    out = _evaluate_readiness(groups, {"k1": 0.97}, gate_floor=0.95)
    assert out["g1"]["ready"] is True
    assert out["g1"]["dead_keys"] == []


def test_dwell_tracking_starts_when_ready():
    from tools.promote_shadow_to_v15_of_v1 import _update_dwell_tracking
    r = _FakeRedis()
    readiness = {"g1": {"ready": True}}
    out = _update_dwell_tracking(r, "cfg:dwell", readiness, now_ms=1000)
    assert out["g1"] == 1000
    assert r.hashes["cfg:dwell"]["g1"] == "1000"


def test_dwell_tracking_preserves_prior_when_still_ready():
    from tools.promote_shadow_to_v15_of_v1 import _update_dwell_tracking
    r = _FakeRedis({"cfg:dwell": {"g1": "500"}})
    readiness = {"g1": {"ready": True}}
    out = _update_dwell_tracking(r, "cfg:dwell", readiness, now_ms=10_000)
    assert out["g1"] == 500  # untouched


def test_dwell_tracking_resets_when_flips_not_ready():
    from tools.promote_shadow_to_v15_of_v1 import _update_dwell_tracking
    r = _FakeRedis({"cfg:dwell": {"g1": "500"}})
    readiness = {"g1": {"ready": False}}
    out = _update_dwell_tracking(r, "cfg:dwell", readiness, now_ms=10_000)
    assert out["g1"] == 0
    assert "g1" not in r.hashes.get("cfg:dwell", {})


def test_eligible_groups_respects_dwell_window():
    from tools.promote_shadow_to_v15_of_v1 import _eligible_groups
    readiness = {"g1": {"ready": True}, "g2": {"ready": True}, "g3": {"ready": False}}
    ready_since = {"g1": 0, "g2": 0, "g3": 0}
    # Both ready but dwell not satisfied — set timestamps
    ready_since["g1"] = 1000  # 23h ago
    ready_since["g2"] = 100   # 49h ago
    now_ms = 1000 + 23 * 3_600_000
    dwell_ms = 24 * 3_600_000
    out = _eligible_groups(readiness, ready_since, dwell_ms, now_ms)
    assert "g1" not in out
    # g2 was ready since 100 → now_ms - 100 = 23h+ which is < 24h. Adjust.
    # Use explicit deltas:
    now_ms = 100 + 25 * 3_600_000  # 25h after g2 ready_since
    out = _eligible_groups(readiness, ready_since, dwell_ms, now_ms)
    assert "g2" in out
    assert "g3" not in out


def test_eligible_groups_zero_ready_since_not_eligible():
    from tools.promote_shadow_to_v15_of_v1 import _eligible_groups
    readiness = {"g1": {"ready": True}}
    ready_since = {"g1": 0}
    out = _eligible_groups(readiness, ready_since, dwell_ms=1, now_ms=999_999)
    assert out == []


def test_render_report_has_expected_sections():
    from tools.promote_shadow_to_v15_of_v1 import _render_text_report
    groups = {"g_eligible": ("k1", "k2"), "g_dwell": ("kx",), "g_dead": ("ky",)}
    readiness = {
        "g_eligible": {"ready": True, "dead_keys": [], "ready_keys": 2, "total": 2},
        "g_dwell": {"ready": True, "dead_keys": [], "ready_keys": 1, "total": 1},
        "g_dead": {"ready": False, "dead_keys": [("ky", 0.1)], "ready_keys": 0, "total": 1},
    }
    ready_since = {"g_eligible": 0, "g_dwell": 999, "g_dead": 0}
    eligible = ["g_eligible"]
    txt = _render_text_report(
        readiness, ready_since, eligible, groups,
        dwell_ms=24 * 3_600_000, now_ms=1000, gate_floor=0.95,
    )
    assert "Eligible for promotion" in txt
    assert "Ready but in dwell window" in txt
    assert "Not ready" in txt
    assert "+ k1" in txt
    assert "+ k2" in txt
    assert "Next steps" in txt
