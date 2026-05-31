import pytest

from signal_scoring.wire_u16 import pack_u16, unpack_u16


def test_pack_unpack_u16_examples() -> None:
    for v in [0, 1, 2, 9, 255, 256, 65535]:
        s = pack_u16(v)
        vv = unpack_u16(s)
        assert vv == (v & 0xFFFF)


def test_unpack_invalid_returns_none() -> None:
    assert unpack_u16("") is None
    assert unpack_u16("%%%") is None


@pytest.mark.parametrize("v", [0, 1, 2, 9, 255, 256, 65534, 65535])
def test_pack_unpack_property(v: int) -> None:
    s = pack_u16(v)
    vv = unpack_u16(s)
    assert vv == v
