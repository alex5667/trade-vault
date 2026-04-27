from __future__ import annotations

from services.abc_router import choose_arm_abc, stable_bucket_0_99


def test_stable_bucket():
    assert stable_bucket_0_99("k") == stable_bucket_0_99("k")
    b = stable_bucket_0_99("k")
    assert 0 <= b <= 99


def test_choose_arm_abc_bounds():
    assert choose_arm_abc(key="k", split_b=100, split_c=0, salt="s") == "B"
    assert choose_arm_abc(key="k", split_b=0, split_c=100, salt="s") == "C"
    assert choose_arm_abc(key="k", split_b=0, split_c=0, salt="s") == "A"
