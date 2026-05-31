from __future__ import annotations

"""Book sequence / staleness tracker (v2).

Why this exists
--------------

Binance offers multiple order book streams:

* Full depth diff stream (depthUpdate) contains **U/u** (first/last update ids).
  This allows a strict missing-sequence check.

* Partial depth streams (e.g. @depth20@100ms) often provide only a **snapshot**
  with a single update id ("u" or "lastUpdateId"). There is no reliable U/u pair,
  so missing-seq cannot be measured exactly. For these streams we fall back to
  **time-gap based** estimation: if the wall-clock / ingest gap is large enough,
  we treat it as "missing".

The output of this module is intentionally small and deterministic so it can be
unit-tested independently of the runtime/service stack.
"""


from dataclasses import dataclass
from typing import Any


def _safe_int(v: Any, default: int = 0) -> int:
    try:
        if v is None:
            return default
        # bool is also int in Python; treat explicitly
        if isinstance(v, bool):
            return int(v)
        return int(v)
    except Exception:
        return default


@dataclass(frozen=True)
class BookSeqUpdate:
    """One update step for book sequence/staleness tracking."""

    # Monotone last seen update id (u / lastUpdateId). 0 when unknown.
    last_u: int

    # Last ingest timestamp (ms). 0 when unknown.
    last_ingest_ts_ms: int

    # Human-readable reason bucket (debug).
    reason: str

    # Estimated number of missed updates for diagnostics.
    #
    # * For strict U/u streams: exact count.
    # * For partial snapshot streams: estimate based on ingest dt.
    gap_missing_updates: int

    # Whether this update should be treated as a "gap event" for EMA.
    is_gap_event: bool


def compute_book_seq_update(
    *,
    prev_last_u: int,
    prev_ingest_ts_ms: int,
    payload: dict[str, Any],
    ingest_ts_ms: int,
    expected_interval_ms: int,
    min_missing_updates: int,
) -> BookSeqUpdate:
    """Compute the next state for book missing-seq tracking.

    Parameters
    ----------
    prev_last_u:
        Previously observed last update id (u). 0 if unknown.

    prev_ingest_ts_ms:
        Previously observed ingest timestamp in ms. 0 if unknown.

    payload:
        Normalized book payload. Expected keys (when present):
          * U: first update id
          * u: last update id (or lastUpdateId)

    ingest_ts_ms:
        Ingest timestamp in ms (monotone-ish wall clock from receiver).

    expected_interval_ms:
        Nominal stream interval. For @depth20@100ms this is 100.

    min_missing_updates:
        Minimum estimated missing updates to raise a gap event.

    Returns
    -------
    BookSeqUpdate
    """

    prev_last_u = _safe_int(prev_last_u, 0)
    prev_ingest_ts_ms = _safe_int(prev_ingest_ts_ms, 0)
    ingest_ts_ms = _safe_int(ingest_ts_ms, 0)

    U = _safe_int(payload.get("U"), 0)
    u = _safe_int(payload.get("u"), 0)

    # -------- strict path (U/u present) --------
    if U > 0 and u > 0 and prev_last_u > 0:
        if u <= prev_last_u:
            # Old/dup message. Do NOT count as missing.
            reason = "dup" if u == prev_last_u else "reorder"
            gap = 0
            is_gap = False
        elif prev_last_u + 1 < U:
            # True missing seq.
            gap = U - prev_last_u - 1
            reason = "gap"
            is_gap = gap >= 1
        else:
            # Overlap is normal for Binance depthUpdate.
            gap = 0
            reason = "ok" if prev_last_u + 1 == U else "overlap"
            is_gap = False

        new_last_u = max(prev_last_u, u)
        new_last_ingest = ingest_ts_ms if ingest_ts_ms > 0 else prev_ingest_ts_ms
        return BookSeqUpdate(
            last_u=new_last_u,
            last_ingest_ts_ms=new_last_ingest,
            reason=reason,
            gap_missing_updates=gap,
            is_gap_event=is_gap,
        )

    # -------- fallback path (partial depth snapshots) --------
    # No reliable U/u pair => estimate missing by time gap.
    if prev_ingest_ts_ms <= 0 or ingest_ts_ms <= 0:
        new_last_u = max(prev_last_u, u)
        new_last_ingest = ingest_ts_ms if ingest_ts_ms > 0 else prev_ingest_ts_ms
        return BookSeqUpdate(
            last_u=new_last_u,
            last_ingest_ts_ms=new_last_ingest,
            reason="init",
            gap_missing_updates=0,
            is_gap_event=False,
        )

    dt_ms = ingest_ts_ms - prev_ingest_ts_ms
    if dt_ms < 0:
        # Bad time (clock jump or out-of-order ingest). Do not count as missing.
        new_last_u = max(prev_last_u, u)
        return BookSeqUpdate(
            last_u=new_last_u,
            last_ingest_ts_ms=ingest_ts_ms,
            reason="reorder",
            gap_missing_updates=0,
            is_gap_event=False,
        )

    # If expected_interval_ms is 0/None, fallback to "no gap".
    exp_ms = _safe_int(expected_interval_ms, 0)
    if exp_ms <= 0:
        new_last_u = max(prev_last_u, u)
        return BookSeqUpdate(
            last_u=new_last_u,
            last_ingest_ts_ms=ingest_ts_ms,
            reason="no_interval",
            gap_missing_updates=0,
            is_gap_event=False,
        )

    # Missing estimate: how many full expected intervals fit into dt, minus the current message.
    # Example for 100ms stream:
    #   dt=100ms  => 0 missing
    #   dt=350ms  => floor(3) - 1 = 2 missing
    #   dt=1200ms => floor(12) - 1 = 11 missing
    missing_est = max(0, int(dt_ms // exp_ms) - 1)
    gap = int(missing_est)

    # We deliberately gate the EMA update by a minimum missing-count.
    # This avoids treating small scheduling jitter as a data-quality incident.
    min_miss = max(1, _safe_int(min_missing_updates, 1))
    is_gap = gap >= min_miss

    # Additional sanity: lastUpdateId is not strictly continuous for snapshot streams.
    # We only flag regression for debug; it must NOT become a missing-event.
    if u > 0 and prev_last_u > 0 and u < prev_last_u:
        reason = "reorder"
        is_gap = False
        gap = 0
    else:
        reason = "gap" if is_gap else "ok"

    new_last_u = max(prev_last_u, u)
    return BookSeqUpdate(
        last_u=new_last_u,
        last_ingest_ts_ms=ingest_ts_ms,
        reason=reason,
        gap_missing_updates=gap,
        is_gap_event=is_gap,
    )
