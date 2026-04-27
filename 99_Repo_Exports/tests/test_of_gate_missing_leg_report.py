from tools.of_gate_missing_leg_report import _extract_miss_leg, _is_relevant


def test_extract_miss_leg_field_wins():
    assert _extract_miss_leg({"miss_leg": "sweep", "reason": "miss:other"}) == "sweep"


def test_extract_miss_leg_from_reason():
    assert _extract_miss_leg({"reason": "no_sweep|miss:sweep"}) == "sweep"


def test_extract_miss_leg_empty():
    assert _extract_miss_leg({"reason": "no_sweep"}) == ""


def test_is_relevant_requires_have_less_need_and_ok0():
    assert _is_relevant({"ok": "0", "have": "1", "need": "2"}) is True
    assert _is_relevant({"ok": "1", "have": "1", "need": "2"}) is False
    assert _is_relevant({"ok": "0", "have": "2", "need": "2"}) is False





