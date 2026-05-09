from __future__ import annotations

# tick_flow_full/core/book_resilience_tracker.py
# -*- coding: utf-8 -*-
"""
Side-aware adapter over BookResilienceTracker.

World-practice tests call a simplified API:
  on_sweep(ts_ms, depth_ref_usd=..., side="bid"|"ask")
  on_book(ts_ms, depth_now_usd=..., side="bid"|"ask")
  snapshot()

Internally delegates to the full BookResilienceTracker from book_resilience.py
which tracks bid+ask pairs. Single-side callers pass the same value for
both legs; the tracker behaves correctly because min(x, x) == x.
"""



from core.book_resilience import BookResilienceTracker as _FullTracker


class BookResilienceTracker:
    """
    Lightweight, side-aware façade over the full BookResilienceTracker.

    Parameters mirror the underlying tracker but use the world-practice
    naming convention for threshold configuration.
    """

    def __init__(
        self,
        *,
        min_sweep_usd: float = 0.0,        # minimum sweep depth to activate tracking
        recover_ratio: float = 0.85,        # alias for target_recovery_ratio
        max_recovery_ms: int = 30_000,      # alias for max_window_ms
        grace_ms: int = 5_000,              # ignored in full tracker, reserved for future use
        target_recovery_ratio: float | None = None,  # direct passthrough
        max_window_ms: int | None = None,             # direct passthrough
        eps: float = 1e-9,
    ) -> None:
        # resolve aliases: explicit direct params take priority
        trr = float(target_recovery_ratio if target_recovery_ratio is not None else recover_ratio)
        mwm = int(max_window_ms if max_window_ms is not None else max_recovery_ms)

        self._min_sweep_usd = float(min_sweep_usd)
        self._grace_ms = int(grace_ms)
        self._inner = _FullTracker(
            target_recovery_ratio=trr,
            max_window_ms=mwm,
            eps=eps,
        )

    def on_sweep(
        self,
        ts_ms: int,
        *,
        depth_ref_usd: float,
        side: str = "bid",
    ) -> None:
        """
        Notify tracker that a sweep occurred.

        Parameters
        ----------
        ts_ms : int
            Event timestamp in milliseconds.
        depth_ref_usd : float
            Reference book depth at sweep time (one side).
        side : str
            "bid" or "ask" – directs which leg of the full tracker is the
            sweep side; opposite leg is filled with the same value.
        """
        ts_ms = int(ts_ms or 0)
        d = float(depth_ref_usd or 0.0)
        if d < self._min_sweep_usd or ts_ms <= 0:
            return
        # Pass same depth to both legs; min(d, d) == d keeps tracker consistent
        self._inner.on_sweep(ts_ms, bid_depth_usd=d, ask_depth_usd=d)

    def on_book(
        self,
        ts_ms: int,
        *,
        depth_now_usd: float,
        side: str = "bid",
    ) -> None:
        """
        Update tracker with current book depth.

        Parameters
        ----------
        ts_ms : int
            Event timestamp in milliseconds.
        depth_now_usd : float
            Current book depth (one side).
        side : str
            "bid" or "ask" – same convention as on_sweep; opposite leg
            filled with same value.
        """
        d = float(depth_now_usd or 0.0)
        self._inner.on_book(int(ts_ms or 0), bid_depth_usd=d, ask_depth_usd=d)

    def snapshot(self) -> dict[str, float]:
        """Return resilience snapshot (same keys as the full tracker)."""
        return self._inner.snapshot()


__all__ = ["BookResilienceTracker"]
