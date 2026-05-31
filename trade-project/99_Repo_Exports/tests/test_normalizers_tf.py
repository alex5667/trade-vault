from __future__ import annotations

from domain.normalizers import canon_tf, tf_variants


def test_canon_tf_maps_legacy_m1_m5_to_1m_5m():
    assert canon_tf("M1") == "1m"
    assert canon_tf("m1") == "1m"
    assert canon_tf("1m") == "1m"

    assert canon_tf("M5") == "5m"
    assert canon_tf("m5") == "5m"
    assert canon_tf("5m") == "5m"


def test_tf_variants_contains_canon_and_legacy():
    v = tf_variants("M1")
    assert "1m" in v
    assert "m1" in v

    v2 = tf_variants("1m")
    assert "1m" in v2
    assert "m1" in v2
