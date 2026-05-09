import time

from common.confidence_auto_train import decide_should_train


def test_decide_train_by_new_cnt():
    ok, reason = decide_should_train(
        min_new=300,
        force_after_sec=7*24*3600,
        last_trained_at=int(time.time()) - 3600,
        new_eligible=500,
    )
    assert ok is True

def test_decide_train_by_force_after():
    ok, reason = decide_should_train(
        min_new=300,
        force_after_sec=7*24*3600,
        last_trained_at=int(time.time()) - 10*24*3600,
        new_eligible=10,
    )
    assert ok is True
