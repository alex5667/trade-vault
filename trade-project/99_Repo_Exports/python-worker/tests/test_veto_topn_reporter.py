from dataclasses import dataclass

from common.reason_normalizer import reason_family
from common.veto_reason_reporter import VetoTopNReporter


class FakeEmitter:
    def __init__(self) -> None:
        self.payloads = []

    def emit(self, payload, *, labels=None, dedup=True) -> bool:
        self.payloads.append(payload)
        return True


class FakeLogger:
    def exception(self, msg: str) -> None:
        return


@dataclass
class Ctx:
    symbol: str = "BTCUSDT"




def test_change_alert_emits_when_dominant_reason_changes(monkeypatch):
    monkeypatch.setenv("VETO_TOPN_WINDOW_MS", "1000")
    monkeypatch.setenv("VETO_TOPN_MIN_TOTAL", "6")
    monkeypatch.setenv("VETO_TOPN_ALERT_SHARE", "0.99")  # disable normal summary
    monkeypatch.setenv("VETO_TOPN_CHANGE_MIN_SHARE", "0.35")
    monkeypatch.setenv("VETO_TOPN_CHANGE_COOLDOWN_MS", "0")  # no cooldown for test

    now = 0
    def now_ms():
        return now

    em = FakeEmitter()
    rep = VetoTopNReporter(emitter=em, logger=FakeLogger(), now_ms_fn=now_ms)
    ctx = Ctx()

    # window 1: top = bo_l2_fail_closed (share 4/6=0.67)
    for _ in range(4):
        rep.record(ctx=ctx, kind="breakout", reason_norm="bo_l2_fail_closed", reason_family=reason_family("bo_l2_fail_closed"), reason_raw="bo_l2_missing")
    for _ in range(2):
        rep.record(ctx=ctx, kind="breakout", reason_norm="conf_below_min_veto", reason_family=reason_family("conf_below_min_veto"), reason_raw="conf_below_min_veto")
    now = 1001
    rep.maybe_flush(ctx=ctx)
    assert em.payloads == []  # first window has no "prev", so no change alert

    # window 2: top changes to conf_below_min_veto (share 5/6=0.83) -> change alert expected
    now = 1100
    for _ in range(5):
        rep.record(ctx=ctx, kind="breakout", reason_norm="conf_below_min_veto", reason_family=reason_family("conf_below_min_veto"), reason_raw="conf_below_min_veto")
    rep.record(ctx=ctx, kind="breakout", reason_norm="bo_l2_fail_closed", reason_family=reason_family("bo_l2_fail_closed"), reason_raw="bo_l2_stale")
    now = 2101
    rep.maybe_flush(ctx=ctx)
    assert len(em.payloads) == 1
    p = em.payloads[0]
    assert p.get("kind") == "label_update"
    assert p.get("labels", {}).get("type") == "veto_topn_change"
    assert p.get("labels", {}).get("prev_top_reason") == "bo_l2_fail_closed"
    assert p.get("labels", {}).get("new_top_reason") == "conf_below_min_veto"


def test_family_change_alert_emits_when_dominant_family_changes(monkeypatch):
    # окно маленькое, summary выключаем, оставляем только family-change
    monkeypatch.setenv("VETO_TOPN_WINDOW_MS", "1000")
    monkeypatch.setenv("VETO_TOPN_MIN_TOTAL", "6")
    monkeypatch.setenv("VETO_TOPN_ALERT_SHARE", "0.99")  # выключить обычный summary
    monkeypatch.setenv("VETO_TOPN_CHANGE_MIN_SHARE", "0.99")  # выключить reason-change
    monkeypatch.setenv("VETO_TOPN_FAMILY_CHANGE_MIN_SHARE", "0.45")
    monkeypatch.setenv("VETO_TOPN_FAMILY_CHANGE_MIN_DELTA", "0.0")  # disable delta gate for this test
    monkeypatch.setenv("VETO_TOPN_FAMILY_CHANGE_MIN_TOTAL_DELTA", "0")  # disable volume gate for this test
    monkeypatch.setenv("VETO_TOPN_FAMILY_CHANGE_MIN_TOTAL_RATIO", "0.0")  # disable volume gate for this test
    monkeypatch.setenv("VETO_TOPN_FAMILY_CHANGE_COOLDOWN_MS", "0")

    now = 0
    def now_ms():
        return now

    em = FakeEmitter()
    rep = VetoTopNReporter(emitter=em, logger=FakeLogger(), now_ms_fn=now_ms)
    ctx = Ctx()

    # window 1: family = book_l2_gate dominates (4/6)
    for _ in range(4):
        rep.record(ctx=ctx, kind="breakout", reason_norm="bo_l2_fail_closed", reason_family=reason_family("bo_l2_fail_closed"), reason_raw="bo_l2_missing")
    for _ in range(2):
        rep.record(ctx=ctx, kind="breakout", reason_norm="l2_wall_distance", reason_family=reason_family("l2_wall_distance"), reason_raw="l2_wall_distance")
    now = 1001
    rep.maybe_flush(ctx=ctx)
    assert em.payloads == []  # первый flush без prev_family

    # window 2: family = confidence_gate dominates (5/6) -> family-change alert expected
    now = 1100
    for _ in range(5):
        rep.record(ctx=ctx, kind="breakout", reason_norm="conf_below_min_veto", reason_family=reason_family("conf_below_min_veto"), reason_raw="conf_below_min_veto")
    rep.record(ctx=ctx, kind="breakout", reason_norm="bo_l2_fail_closed", reason_family=reason_family("bo_l2_fail_closed"), reason_raw="bo_l2_stale")
    now = 2101
    rep.maybe_flush(ctx=ctx)

    assert len(em.payloads) == 1
    p = em.payloads[0]
    assert p.get("kind") == "label_update"
    assert p.get("labels", {}).get("type") == "veto_topn_family_change"
    assert p.get("labels", {}).get("prev_family") == "book_l2_gate"
    assert p.get("labels", {}).get("new_family") == "confidence_gate"


def test_family_change_delta_gate_suppresses_small_shifts(monkeypatch):
    monkeypatch.setenv("VETO_TOPN_WINDOW_MS", "1000")
    monkeypatch.setenv("VETO_TOPN_MIN_TOTAL", "10")
    monkeypatch.setenv("VETO_TOPN_ALERT_SHARE", "0.99")  # выключить summary
    monkeypatch.setenv("VETO_TOPN_CHANGE_MIN_SHARE", "0.99")  # выключить reason-change
    monkeypatch.setenv("VETO_TOPN_FAMILY_CHANGE_MIN_SHARE", "0.45")
    monkeypatch.setenv("VETO_TOPN_FAMILY_CHANGE_MIN_DELTA", "0.20")
    monkeypatch.setenv("VETO_TOPN_FAMILY_CHANGE_MIN_TOTAL_DELTA", "0")  # disable volume gate for delta gate test
    monkeypatch.setenv("VETO_TOPN_FAMILY_CHANGE_MIN_TOTAL_RATIO", "0.0")  # disable volume gate for delta gate test
    monkeypatch.setenv("VETO_TOPN_FAMILY_CHANGE_COOLDOWN_MS", "0")

    now = 0
    def now_ms():
        return now

    em = FakeEmitter()
    rep = VetoTopNReporter(emitter=em, logger=FakeLogger(), now_ms_fn=now_ms)
    ctx = Ctx()

    # window 1: top family share 0.50 (5/10) book_l2_gate
    for _ in range(5):
        rep.record(ctx=ctx, kind="breakout", reason_norm="bo_l2_fail_closed", reason_family=reason_family("bo_l2_fail_closed"), reason_raw="bo_l2_missing")
    for _ in range(5):
        rep.record(ctx=ctx, kind="breakout", reason_norm="conf_below_min_veto", reason_family=reason_family("conf_below_min_veto"), reason_raw="conf_below_min_veto")
    now = 1001
    rep.maybe_flush(ctx=ctx)
    assert em.payloads == []  # первый flush

    # window 2: family changes, but concentration shift small: new share 0.52 (delta +0.02) -> suppressed
    now = 1100
    for _ in range(52):
        rep.record(ctx=ctx, kind="breakout", reason_norm="conf_below_min_veto", reason_family=reason_family("conf_below_min_veto"), reason_raw="conf_below_min_veto")
    for _ in range(48):
        rep.record(ctx=ctx, kind="breakout", reason_norm="bo_l2_fail_closed", reason_family=reason_family("bo_l2_fail_closed"), reason_raw="bo_l2_stale")
    now = 2101
    rep.maybe_flush(ctx=ctx)
    assert em.payloads == []

    # window 3: family changes to spread_gate with big shift: new share 0.80 (delta +0.30 vs prev 0.50) -> emits
    now = 2200
    for _ in range(80):
        rep.record(ctx=ctx, kind="breakout", reason_norm="spread_filter_veto", reason_family=reason_family("spread_filter_veto"), reason_raw="spread_too_wide_veto")
    for _ in range(20):
        rep.record(ctx=ctx, kind="breakout", reason_norm="bo_l2_fail_closed", reason_family=reason_family("bo_l2_fail_closed"), reason_raw="bo_l2_missing")
    now = 3201
    rep.maybe_flush(ctx=ctx)
    assert len(em.payloads) == 1
    p = em.payloads[0]
    assert p.get("labels", {}).get("type") == "veto_topn_family_change"
    assert p.get("labels", {}).get("prev_family") == "confidence_gate"
    assert p.get("labels", {}).get("new_family") == "spread_gate"
    assert p.get("labels", {}).get("new_family_share_delta") is not None
    assert float(p["labels"]["new_family_share_delta"]) >= 0.20


def test_family_change_volume_gate_requires_real_worsening(monkeypatch):
    """
    "¼ гайки": даже если family сменилась и концентрация выросла,
    но общий total_veto не ухудшился — алерт НЕ отправляем.
    Отправляем только когда total_veto заметно растёт (abs или ratio).
    """
    monkeypatch.setenv("VETO_TOPN_WINDOW_MS", "1000")
    monkeypatch.setenv("VETO_TOPN_MIN_TOTAL", "10")
    monkeypatch.setenv("VETO_TOPN_ALERT_SHARE", "0.99")          # выключить summary
    monkeypatch.setenv("VETO_TOPN_CHANGE_MIN_SHARE", "0.99")     # выключить reason-change
    monkeypatch.setenv("VETO_TOPN_FAMILY_CHANGE_MIN_SHARE", "0.45")
    monkeypatch.setenv("VETO_TOPN_FAMILY_CHANGE_MIN_DELTA", "0.0")  # disable delta gate for volume gate test
    monkeypatch.setenv("VETO_TOPN_FAMILY_CHANGE_MIN_TOTAL_DELTA", "7")
    monkeypatch.setenv("VETO_TOPN_FAMILY_CHANGE_MIN_TOTAL_RATIO", "1.10")
    monkeypatch.setenv("VETO_TOPN_FAMILY_CHANGE_COOLDOWN_MS", "0")

    now = 0
    def now_ms():
        return now

    em = FakeEmitter()
    rep = VetoTopNReporter(emitter=em, logger=FakeLogger(), now_ms_fn=now_ms)
    ctx = Ctx()

    # window 1: total=100, доминирует book_l2_gate (80%)
    for _ in range(80):
        rep.record(ctx=ctx, kind="breakout", reason_norm="bo_l2_fail_closed", reason_family=reason_family("bo_l2_fail_closed"), reason_raw="bo_l2_missing")
    for _ in range(20):
        rep.record(ctx=ctx, kind="breakout", reason_norm="conf_below_min_veto", reason_family=reason_family("conf_below_min_veto"), reason_raw="conf_below_min_veto")
    now = 1001
    rep.maybe_flush(ctx=ctx)
    assert em.payloads == []

    # window 2: family сменилась и стала сильнее (conf 80%), но total всё ещё 100 => volume gate блокирует
    now = 1100
    for _ in range(80):
        rep.record(ctx=ctx, kind="breakout", reason_norm="conf_below_min_veto", reason_family=reason_family("conf_below_min_veto"), reason_raw="conf_below_min_veto")
    for _ in range(20):
        rep.record(ctx=ctx, kind="breakout", reason_norm="bo_l2_fail_closed", reason_family=reason_family("bo_l2_fail_closed"), reason_raw="bo_l2_stale")
    now = 2101
    rep.maybe_flush(ctx=ctx)
    assert em.payloads == []

    # window 3: family меняется на spread_gate с тем же total (100) => volume gate блокирует
    now = 2200
    for _ in range(80):
        rep.record(ctx=ctx, kind="breakout", reason_norm="spread_filter_veto", reason_family=reason_family("spread_filter_veto"), reason_raw="spread_too_wide_veto")
    for _ in range(20):
        rep.record(ctx=ctx, kind="breakout", reason_norm="bo_l2_fail_closed", reason_family=reason_family("bo_l2_fail_closed"), reason_raw="bo_l2_missing")
    now = 3201
    rep.maybe_flush(ctx=ctx)
    assert em.payloads == []  # volume gate blocks despite family change

    # window 4: family меняется на touch_gate, но total тот же (100) => volume gate блокирует
    now = 3300
    for _ in range(80):
        rep.record(ctx=ctx, kind="breakout", reason_norm="touch_suppressed", reason_family=reason_family("touch_suppressed"), reason_raw="touch_suppressed")
    for _ in range(20):
        rep.record(ctx=ctx, kind="breakout", reason_norm="bo_l2_fail_closed", reason_family=reason_family("bo_l2_fail_closed"), reason_raw="bo_l2_missing")
    now = 4301
    rep.maybe_flush(ctx=ctx)
    assert em.payloads == []  # volume gate blocks despite family change

    # window 5: family меняется на cooldown_gate, но total тот же (100) => volume gate блокирует
    now = 4400
    for _ in range(80):
        rep.record(ctx=ctx, kind="breakout", reason_norm="cooldown", reason_family=reason_family("cooldown"), reason_raw="cooldown_active")
    for _ in range(20):
        rep.record(ctx=ctx, kind="breakout", reason_norm="bo_l2_fail_closed", reason_family=reason_family("bo_l2_fail_closed"), reason_raw="bo_l2_missing")
    now = 5401
    rep.maybe_flush(ctx=ctx)
    assert em.payloads == []  # volume gate blocks despite family change

    # window 6: family меняется на l3_quality, total вырос (140) => volume gate пропускает
    now = 5500
    for _ in range(112):  # 80%
        rep.record(ctx=ctx, kind="breakout", reason_norm="l3_missing", reason_family=reason_family("l3_missing"), reason_raw="l3_missing")
    for _ in range(28):
        rep.record(ctx=ctx, kind="breakout", reason_norm="bo_l2_fail_closed", reason_family=reason_family("bo_l2_fail_closed"), reason_raw="bo_l2_missing")
    now = 6501
    rep.maybe_flush(ctx=ctx)
    assert len(em.payloads) == 1
    p = em.payloads[0]
    assert p.get("labels", {}).get("type") == "veto_topn_family_change"
    assert p.get("labels", {}).get("prev_family") == "cooldown_gate"
    assert p.get("labels", {}).get("new_family") == "l3_quality"
    assert int(p["labels"]["total_veto_delta"]) >= 7 or float(p["labels"]["total_veto_ratio"]) >= 1.10
