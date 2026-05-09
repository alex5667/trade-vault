from common.log_sampling import TimeSampler


def test_time_sampler_basic():
    s = TimeSampler(every_ms=1000)
    assert s.maybe(0) is True      # first call -> True
    assert s.maybe(999) is False
    assert s.maybe(1000) is True
    assert s.maybe(1500) is False
    assert s.maybe(2000) is True

def test_time_sampler_force():
    s = TimeSampler(every_ms=1000)
    assert s.maybe(0) is True
    assert s.maybe(10) is False
    s.force()
    assert s.maybe(11) is True
    assert s.maybe(12) is False
