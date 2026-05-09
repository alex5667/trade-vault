from __future__ import annotations

import base64
import struct


def pack_u16(v: int) -> str:
    """
    Pack uint16 to compact base64 string (no padding).
    Big-endian to be consistent and deterministic.
    """
    vv = int(v) & 0xFFFF
    raw = struct.pack(">H", vv)
    s = base64.b64encode(raw).decode("ascii")
    return s.rstrip("=")


def unpack_u16(s: str) -> int | None:
    """
    Unpack base64(no padding) -> uint16.
    Returns None if decode fails.
    """
    if not s:
        return None
    try:
        ss = s
        # restore padding
        pad = "=" * ((4 - (len(ss) % 4)) % 4)
        raw = base64.b64decode(ss + pad)
        if len(raw) != 2:
            return None
        (vv,) = struct.unpack(">H", raw)
        return int(vv)
    except Exception:
        return None
