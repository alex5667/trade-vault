from core.bucket_allowlist_v1 import bucket_allowed


def test_bucket_allowed_all():
    assert bucket_allowed('NORMAL', 'all')
    assert bucket_allowed('HIGH_VOL_LOW_LIQ', '*')


def test_bucket_allowed_none():
    assert bucket_allowed('HIGH_VOL_LOW_LIQ', 'none') is False
    assert bucket_allowed('HIGH_VOL_LOW_LIQ', '0') is False


def test_bucket_allowed_default_when_empty():
    assert bucket_allowed('HIGH_VOL_LOW_LIQ', '') is True
    assert bucket_allowed('NORMAL', '') is False


def test_bucket_allowed_list():
    allow = 'HIGH_VOL, LOW_LIQ'
    assert bucket_allowed('HIGH_VOL', allow) is True
    assert bucket_allowed('LOW_LIQ', allow) is True
    assert bucket_allowed('NORMAL', allow) is False
