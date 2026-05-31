from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from common.qf_codes import QF
from signal_scoring.reason_codes import ReasonCode
from signal_scoring.reason_policy import (
    LegacyMapAlertConfig,
    ReasonMismatchMonitor,
    patch_validation_reason_for_kind,
)


class FakeMetrics:
    def __init__(self) -> None:
        self.calls: list[tuple[str, int, dict[str, str] | None]] = []

    def incr(self, name: str, value: int = 1, tags: dict[str, str] | None = None) -> None:
        self.calls.append((name, int(value), dict(tags or {})))

    def count(self, name: str) -> int:
        return sum(v for (n, v, _t) in self.calls if n == name)

    def find(self, name: str) -> list[dict[str, str]]:
        return [t or {} for (n, _v, t) in self.calls if n == name]


@dataclass
class _V:
    veto: bool
    conf_factor01: float
    flags: list[int]
    reason_code: str
    reason: str = ""
    reason_u16: int = 0
    parts: dict | None = None


def test_reason_legacy_mapped_total_metric_emitted() -> None:
    m = FakeMetrics()
    mon = ReasonMismatchMonitor(metrics=m, notify=None, alert_cfg=LegacyMapAlertConfig(window_s=60, min_events=10, cooldown_s=60))

    v = _V(veto=True, conf_factor01=0.0, flags=[], reason_code="bo_l2_stale", parts={})
    patched = patch_validation_reason_for_kind(validation=v, kind="breakout", monitor=mon)

    assert patched.reason_code == ReasonCode.VETO_L2_STALE.value
    assert int(QF.REASON_LEGACY_MAPPED) in (patched.flags or [])
    assert m.count("reason_legacy_mapped_total") == 1

    tags = m.find("reason_legacy_mapped_total")[0]
    assert tags.get("kind") == "breakout"
    assert tags.get("from") == "bo_l2_stale"
    assert tags.get("to") == ReasonCode.VETO_L2_STALE.value


def test_reason_legacy_spike_alert_triggers_on_threshold() -> None:
    m = FakeMetrics()
    out: list[dict[str, Any]] = []

    def notify(payload: dict[str, Any]) -> None:
        out.append(payload)

    # low threshold for test
    cfg = LegacyMapAlertConfig(window_s=10, min_events=3, cooldown_s=999)
    mon = ReasonMismatchMonitor(metrics=m, notify=notify, alert_cfg=cfg)

    # emulate 3 events in the same window
    for _ in range(3):
        v = _V(veto=True, conf_factor01=0.0, flags=[], reason_code="bo_l2_stale", parts={})
        patch_validation_reason_for_kind(validation=v, kind="breakout", monitor=mon)

    assert len(out) == 1
    p = out[0]
    assert p.get("kind") == "diag_reason_legacy_spike"
    labels = p.get("labels") or {}
    assert labels.get("kind") == "breakout"
    assert labels.get("from") == "bo_l2_stale"
    assert labels.get("to") == ReasonCode.VETO_L2_STALE.value
    assert int(labels.get("count") or 0) >= 3


def test_reason_legacy_spike_alert_respects_cooldown() -> None:
    out: list[dict[str, Any]] = []

    def notify(payload: dict[str, Any]) -> None:
        out.append(payload)

    cfg = LegacyMapAlertConfig(window_s=10, min_events=2, cooldown_s=3600)
    mon = ReasonMismatchMonitor(metrics=None, notify=notify, alert_cfg=cfg)

    # first trigger
    for _ in range(2):
        v = _V(veto=True, conf_factor01=0.0, flags=[], reason_code="bo_l2_stale", parts={})
        patch_validation_reason_for_kind(validation=v, kind="breakout", monitor=mon)
    assert len(out) == 1

    # immediately try to trigger again -> should be suppressed by cooldown
    for _ in range(5):
        v = _V(veto=True, conf_factor01=0.0, flags=[], reason_code="bo_l2_stale", parts={})
        patch_validation_reason_for_kind(validation=v, kind="breakout", monitor=mon)
    assert len(out) == 1
