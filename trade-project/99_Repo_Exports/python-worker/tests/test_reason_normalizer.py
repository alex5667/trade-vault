from common.reason_normalizer import normalize_reason, reason_family


def test_breakout_l2_missing_and_stale_collapse():
    assert normalize_reason("bo_l2_missing", kind="breakout") == "bo_l2_fail_closed"
    assert normalize_reason("bo_l2_stale", kind="breakout") == "bo_l2_fail_closed"


def test_conf_below_min_is_stable():
    assert normalize_reason("conf_below_min_veto", kind="absorption") == "conf_below_min_veto"
    assert normalize_reason("CONF BELOW MIN VETO", kind="absorption") == "conf_below_min_veto"


def test_protective_buckets():
    assert normalize_reason("spread_too_wide_veto", kind="breakout") == "spread_filter_veto"
    assert normalize_reason("cooldown_active", kind="breakout") == "cooldown"
    assert normalize_reason("touch_suppressed", kind="breakout") == "touch_suppressed"


def test_fallback_compression_removes_numbers_and_caps_tokens():
    # For breakout with bo_l2_ prefix -> bo_l2_veto
    r = normalize_reason("bo_l2_wall_distance_123.45_too_far_veto", kind="breakout")
    assert r == "bo_l2_veto"

    # For non-breakout -> fallback compression
    r2 = normalize_reason("some_other_wall_distance_123.45_too_far_veto", kind="absorption")
    assert r2 == "some_other_wall_distance"  # numbers removed, trailing "veto" dropped


def test_reason_family_is_low_cardinality():
    assert reason_family("bo_l2_fail_closed") == "book_l2_gate"
    assert reason_family("conf_below_min_veto") == "confidence_gate"
    assert reason_family("spread_filter_veto") == "spread_gate"
    assert reason_family("touch_suppressed") == "touch_gate"
    assert reason_family("cooldown") == "cooldown_gate"
    assert reason_family("l3_missing") == "l3_quality"
