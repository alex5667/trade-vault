from news_pipeline.feature_store_service import _apply_grade_cooldown


def test_cooldown_allows_first_write():
    g, ts, frozen = _apply_grade_cooldown(
        prev_grade_id=0,
        prev_change_ts_ms=0,
        new_grade_id=3,
        now_ts_ms=1000,
        cooldown_up_sec=900,
        cooldown_down_sec=300,
    )
    assert g == 3
    assert ts == 1000
    assert frozen is False


def test_cooldown_blocks_fast_increase():
    g, ts, frozen = _apply_grade_cooldown(
        prev_grade_id=1,
        prev_change_ts_ms=1000,
        new_grade_id=3,
        now_ts_ms=1000 + 60_000,  # +60s
        cooldown_up_sec=900,
        cooldown_down_sec=300,
    )
    assert g == 1
    assert ts == 1000
    assert frozen is True


def test_cooldown_allows_decrease_after_down_window():
    g, ts, frozen = _apply_grade_cooldown(
        prev_grade_id=3,
        prev_change_ts_ms=1000,
        new_grade_id=1,
        now_ts_ms=1000 + 400_000,  # +400s
        cooldown_up_sec=900,
        cooldown_down_sec=300,
    )
    assert g == 1
    assert ts == 1000 + 400_000
    assert frozen is False
