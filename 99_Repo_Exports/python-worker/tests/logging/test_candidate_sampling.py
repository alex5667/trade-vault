from __future__ import annotations

from handlers.crypto_orderflow.components.sampler import _SampleEveryMs


def test_sample_every_ms_first_call_logs():
    g = _SampleEveryMs(every_ms=10_000)
    assert g.should_log(1000) is True  # first call logs


def test_sample_every_ms_blocks_until_interval():
    g = _SampleEveryMs(every_ms=10_000)
    assert g.should_log(1000) is True
    assert g.should_log(5000) is False
    assert g.should_log(10_999) is False
    assert g.should_log(11_000) is True


def test_sample_every_ms_force_always_logs_and_moves_last():
    g = _SampleEveryMs(every_ms=10_000)
    assert g.should_log(1000) is True
    assert g.should_log(2000, force=True) is True
    # after force at 2000, interval restarts from 2000
    assert g.should_log(11_999) is False
    assert g.should_log(12_000) is True


def test_sample_every_ms_zero_disables():
    g = _SampleEveryMs(every_ms=0)
    assert g.should_log(1000) is False
    assert g.should_log(2000, force=True) is True  # force still works (useful for regime-change forcing)
