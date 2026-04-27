from __future__ import annotations

import base64
import struct
from typing import Iterable

def pack_u16_list(xs: Iterable[int]) -> str:
    """
    Packs list[int] (0..65535) into base64 string.
    Wire-friendly compact format.
    """
    arr = []
    for x in xs:
        try:
            v = int(x)
        except Exception:
            continue
        if 0 <= v <= 65535:
            arr.append(v)
    if not arr:
        return ""
    # little-endian u16
    buf = b"".join(struct.pack("<H", v) for v in arr)
    return base64.b64encode(buf).decode("ascii")
