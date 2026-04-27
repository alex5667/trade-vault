from __future__ import annotations

from common.qf_codes import QF, pack_qf_u16, unpack_qf_u16, qf_labels_from_codes


def test_pack_unpack_roundtrip_u16():
    src = [int(QF.L2_STALE), int(QF.BO_FAKE_BREAKOUT_VETO), 65535]
    b64 = pack_qf_u16(src)
    out = unpack_qf_u16(b64)
    assert out == src


def test_labels_from_codes_known_and_unknown():
    labels = qf_labels_from_codes([int(QF.L2_STALE), 4242])
    assert labels["qf/l2.stale"] == 1
    assert labels["qf/unknown_4242"] == 1
