from __future__ import annotations

from enum import IntEnum
from typing import Iterable, Any
import base64
import struct


class QF(IntEnum):
    """
    Compact uint16 quality-flag codes.
    IMPORTANT: never renumber existing codes (append-only).
    """
    # --- dependency / data availability (100..199)
    L2_EMPTY = 100
    L2_BAD_TOP = 101
    L2_STALE = 102
    L2_MISSING = 103
    L3_MISSING_NEUTRAL = 110
    GEO_MISSING_NEUTRAL = 120
    HLC_FALLBACK = 130

    # --- book sanity / spread (200..299)
    SPREAD_TOO_WIDE = 200
    SPREAD_SOFT_PENALTY = 201
    SPREAD_HARD_VETO = 202
    NO_WALL = 210
    WALL_TOO_FAR = 211

    # --- breakout quality (300..399)
    BO_L2_FAIL_CLOSED = 300
    BO_FAKE_BREAKOUT_VETO = 301
    BO_CONTINUATION_PENALTY = 302

    # --- absorption quality (400..499)
    AB_NEED_2OF2_VETO = 400
    AB_LOW_TAKER_VETO = 401

    # --- extreme (500..599)
    EXT_L2_MISSING_OR_STALE_PENALTY = 500
    EXT_SPOOFY_MICRO_PENALTY = 501

    # --- OBI spike (600..699)
    OBI_NOT_SUSTAINED_PENALTY = 600
    OBI_SPOOF_CANCEL_PENALTY = 601

    # --- final gating (900..999)
    CONF_BELOW_MIN_VETO = 900

    # --- meta / contract drift (60000..60999)
    REASON_LEGACY_MAPPED = 60999
    REASON_KIND_MISMATCH = 61000



# Human-readable codes (stable public surface), used ONLY in formatter/publisher.
QF_STR: dict[int, str] = {
    int(QF.L2_EMPTY): "l2.empty"
    int(QF.L2_BAD_TOP): "l2.bad_top"
    int(QF.L2_STALE): "l2.stale"
    int(QF.L2_MISSING): "l2.missing"
    int(QF.L3_MISSING_NEUTRAL): "l3.missing_neutral"
    int(QF.GEO_MISSING_NEUTRAL): "geo.missing_neutral"
    int(QF.HLC_FALLBACK): "hlc.fallback"
    int(QF.SPREAD_TOO_WIDE): "spread.too_wide"
    int(QF.SPREAD_SOFT_PENALTY): "spread.soft_penalty"
    int(QF.SPREAD_HARD_VETO): "spread.hard_veto"
    int(QF.NO_WALL): "wall.none"
    int(QF.WALL_TOO_FAR): "wall.too_far"
    int(QF.BO_L2_FAIL_CLOSED): "bo.l2.fail_closed"
    int(QF.BO_FAKE_BREAKOUT_VETO): "bo.fake_breakout.veto"
    int(QF.BO_CONTINUATION_PENALTY): "bo.continuation.penalty"
    int(QF.AB_NEED_2OF2_VETO): "ab.need_2of2.veto"
    int(QF.AB_LOW_TAKER_VETO): "ab.low_taker.veto"
    int(QF.EXT_L2_MISSING_OR_STALE_PENALTY): "ext.l2.missing_or_stale.penalty"
    int(QF.EXT_SPOOFY_MICRO_PENALTY): "ext.micro.spoofy.penalty"
    int(QF.OBI_NOT_SUSTAINED_PENALTY): "obi.not_sustained.penalty"
    int(QF.OBI_SPOOF_CANCEL_PENALTY): "obi.spoof.cancel.penalty"
    int(QF.CONF_BELOW_MIN_VETO): "conf.below_min.veto"
    int(QF.REASON_KIND_MISMATCH): "reason.kind_mismatch"
    int(QF.REASON_LEGACY_MAPPED): "reason.legacy_mapped"
}


def _as_u16(x: Any) -> int | None:
    try:
        v = int(x)
        if 0 <= v <= 0xFFFF:
            return v
        return None
    except Exception:
        return None


def pack_qf_u16(codes: Iterable[int]) -> str:
    """
    Pack uint16 list into base64 (little-endian). Payload-friendly and smaller than list[int].
    """
    buf = bytearray()
    for c in codes or []:
        v = _as_u16(c)
        if v is None:
            continue
        buf += struct.pack("<H", v)
    return base64.b64encode(bytes(buf)).decode("ascii")


def unpack_qf_u16(b64: Any) -> list[int]:
    try:
        s = str(b64 or "")
        if not s:
            return []
        raw = base64.b64decode(s.encode("ascii"), validate=False)
        if len(raw) % 2 != 0:
            return []
        out: list[int] = []
        for i in range(0, len(raw), 2):
            (v,) = struct.unpack("<H", raw[i : i + 2])
            out.append(int(v))
        return out
    except Exception:
        return []


def qf_labels_from_codes(codes: Iterable[int]) -> dict[str, int]:
    """
    Expand numeric QF codes -> labels dict:
      qf/<stable-string-code> => 1
    Unknown codes are preserved as qf/unknown_<n>.
    """
    out: dict[str, int] = {}
    for c in codes or []:
        v = _as_u16(c)
        if v is None:
            continue
        s = QF_STR.get(v) or f"unknown_{v}"
        out[f"qf/{s}"] = 1
    return out
