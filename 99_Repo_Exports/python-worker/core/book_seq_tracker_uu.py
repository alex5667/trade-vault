from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class BookSeqDecision:
    """Decision for one depthUpdate message.

    Fields:
      has_seq_fields: whether both U and u are available and usable
      reason: init/ok/overlap/gap/dup/reorder/no_seq_fields
      gap: number of missing updateIds (only for reason == "gap")
      missing_event: 1.0 on "gap", else 0.0
      next_last_u: monotone candidate for last_u
    """

    has_seq_fields: bool
    reason: str
    gap: int
    missing_event: float
    next_last_u: int


def ema_update_clamped(prev: float, x: float, alpha: float) -> float:
    """EMA update with alpha clamped into [0, 1]."""

    try:
        a = float(alpha)
    except Exception:
        a = 0.1

    if a <= 0.0:
        return float(prev)
    if a >= 1.0:
        return float(x)
    return (1.0 - a) * float(prev) + a * float(x)


def decide_book_seq_uu(*, prev_u: int, cur_U: int, cur_u: int) -> BookSeqDecision:
    """Strict continuity decision for Binance depthUpdate (U/u).

    Correct continuity check:
        GAP if prev_u + 1 != cur_U (equivalently: cur_U > prev_u + 1)
        gap_size = cur_U - prev_u - 1

    Binance typically sends overlapping ranges (normal):
        U <= prev_u+1 <= u

    We treat only GAP as missing_event=1. Duplicates/old messages do NOT count.
    """

    p = int(prev_u or 0)
    U = int(cur_U or 0)
    u = int(cur_u or 0)

    # Without both U and u, strict continuity is impossible.
    if U <= 0 or u <= 0:
        return BookSeqDecision(False, "no_seq_fields", 0, 0.0, p)

    # First seen update id.
    if p <= 0:
        return BookSeqDecision(True, "init", 0, 0.0, u)

    expected_U = p + 1

    # DUP/OLD: event is entirely behind our last_u.
    if u <= p:
        return BookSeqDecision(True, "dup", 0, 0.0, p)

    # GAP: missing one or more book updates.
    if U > expected_U:
        gap = U - p - 1
        if gap < 0:
            gap = 0
        return BookSeqDecision(True, "gap", int(gap), 1.0, u)

    # OK / OVERLAP / REORDER
    if U < expected_U and u >= expected_U:
        return BookSeqDecision(True, "overlap", 0, 0.0, u)

    if U < expected_U and u < expected_U:
        # Old/reordered chunk that does not cover the expected id.
        return BookSeqDecision(True, "reorder", 0, 0.0, u)

    return BookSeqDecision(True, "ok", 0, 0.0, u)
