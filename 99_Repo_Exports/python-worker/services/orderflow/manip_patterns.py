from __future__ import annotations

"""
Phase E / P4: Manipulation Pattern Tracker (Quote Stuffing + Layering)

Computes two composite manipulation scores from L2 book dynamics:

1. quote_stuffing_score (0..1):
   Triggered when book update rate Z AND cancel rate Z are both above thresholds.
   Uses QUOTE_STUFF_MSG_Z_THR and QUOTE_STUFF_CANCEL_Z_THR env vars.
   score = min(1.0, (msg_z - thr_msg) / max(thr_msg, 1.0) * 0.5
                   + (cancel_z - thr_cancel) / max(thr_cancel, 1.0) * 0.5)

2. layering_score (0..1):
   L2 approximation of build→pull "ladder" (layering / spoofing):
   - build phase: depth grows by LAYERING_BUILD_MULT × baseline on one side
   - revert phase: depth snaps back by LAYERING_REVERT_FRAC within LAYERING_REVERT_MS
   - only fires when trade_rate is LOW (LAYERING_TRADE_RATE_LOW_HZ)
   - only fires when peak depth USD > LAYERING_MIN_PEAK_USD

manip_flags: comma-separated string of active codes:
   "QUOTE_STUFFING", "LAYERING"

All ENV defaults set to conservative / disabled values.
Fail-open: any exception → silent pass.
"""


import logging
import os

logger = logging.getLogger("orderflow_manip_patterns")

# Default thresholds – fallback values only (can be overridden at any time via ENV)
# NOTE: these are read dynamically inside the methods to allow runtime/test override.
_QS_MSG_Z_THR_DEFAULT = 4.0
_QS_CANCEL_Z_THR_DEFAULT = 3.5
_LAY_BUILD_MULT_DEFAULT = 1.6
_LAY_RATIO_MIN_DEFAULT = 0.45
_LAY_REVERT_FRAC_DEFAULT = 0.35
_LAY_REVERT_MS_DEFAULT = 900.0
_LAY_MIN_PEAK_USD_DEFAULT = 5000.0
_LAY_TRADE_RATE_LOW_HZ_DEFAULT = 2.0


class ManipulationTracker:
    """
    Stateful L2 manipulation pattern detector.

    update_from_book() is called by BookProcessor after every book snapshot.
    All outputs are public floats / str; safe to read from strategy.
    """

    def __init__(self) -> None:
        # ── Quote stuffing score ──────────────────────────────────────────────
        self.quote_stuffing_score: float = 0.0

        # ── Layering state machine: "idle" → "build" → (revert detected → score) ──
        self.layering_score: float = 0.0
        self._lay_state: str = "idle"          # idle | build
        self._lay_peak_bid_usd: float = 0.0    # peak depth tracked during build phase
        self._lay_peak_ask_usd: float = 0.0
        self._lay_build_ts_ms: int = 0          # when build phase started
        self._lay_baseline_bid_usd: float = 0.0
        self._lay_baseline_ask_usd: float = 0.0
        # ema baseline for depth (smoothed reference)
        self._lay_bid_ema_usd: float = 0.0
        self._lay_ask_ema_usd: float = 0.0
        self._lay_ema_alpha: float = 0.05       # slow EMA for baseline

        # ── Combined flags string ─────────────────────────────────────────────
        self.manip_flags: str = ""

    # ── Public API ────────────────────────────────────────────────────────────

    def update_from_book(
        self,
        *,
        ts_ms: int,
        bid_depth_usd: float,
        ask_depth_usd: float,
        book_update_rate_z: float,
        cancel_rate_z: float,
        trade_msg_rate_hz: float,
        mid_px: float,
    ) -> None:
        """
        Called by BookProcessor on every book snapshot.

        Args:
            ts_ms: event timestamp (epoch ms)
            bid_depth_usd: top-5 bid depth in USD
            ask_depth_usd: top-5 ask depth in USD
            book_update_rate_z: z-score of book update rate (from MessageRateTracker)
            cancel_rate_z: z-score of L3-lite cancel rate (from MessageRateTracker)
            trade_msg_rate_hz: current EMA trade message rate (Hz)
            mid_px: mid price (for depth USD normalization; unused if depths already USD)
        """
        try:
            flags = []

            # 1. Quote stuffing detection
            self._update_quote_stuffing(
                book_update_rate_z=float(book_update_rate_z),
                cancel_rate_z=float(cancel_rate_z),
                flags=flags,
            )

            # 2. Layering detection
            self._update_layering(
                ts_ms=int(ts_ms),
                bid_depth_usd=float(bid_depth_usd),
                ask_depth_usd=float(ask_depth_usd),
                trade_msg_rate_hz=float(trade_msg_rate_hz),
                flags=flags,
            )

            self.manip_flags = ",".join(flags) if flags else ""

        except Exception:
            pass

    # ── Internal: Quote Stuffing ──────────────────────────────────────────────

    def _update_quote_stuffing(
        self,
        book_update_rate_z: float,
        cancel_rate_z: float,
        flags: list,
    ) -> None:
        """
        Quote stuffing score = composite of book update rate Z + cancel rate Z.

        Disabled when thresholds are <= 0.
        score in [0..1]; 0 = not active.
        """
        try:
            # Read ENV dynamically so values can be changed at runtime or in tests
            thr_msg = float(os.getenv("QUOTE_STUFF_MSG_Z_THR", str(_QS_MSG_Z_THR_DEFAULT)) or _QS_MSG_Z_THR_DEFAULT)
            thr_cancel = float(os.getenv("QUOTE_STUFF_CANCEL_Z_THR", str(_QS_CANCEL_Z_THR_DEFAULT)) or _QS_CANCEL_Z_THR_DEFAULT)

            # Disabled if both thresholds are <= 0 (explicit disable)
            if thr_msg <= 0.0 and thr_cancel <= 0.0:
                self.quote_stuffing_score = 0.0
                return

            # Compute component scores (0 when below threshold, >0 when above)
            s_msg = 0.0
            if thr_msg > 0.0 and book_update_rate_z > thr_msg:
                s_msg = (book_update_rate_z - thr_msg) / max(thr_msg, 1.0)

            s_cancel = 0.0
            if thr_cancel > 0.0 and cancel_rate_z > thr_cancel:
                s_cancel = (cancel_rate_z - thr_cancel) / max(thr_cancel, 1.0)

            # Both components must fire for quote stuffing signal
            if s_msg > 0.0 and s_cancel > 0.0:
                # Combined score capped at 1.0
                raw = 0.5 * s_msg + 0.5 * s_cancel
                self.quote_stuffing_score = float(min(1.0, raw))
                flags.append("QUOTE_STUFFING")
            else:
                # Decay towards 0
                self.quote_stuffing_score = float(self.quote_stuffing_score * 0.7)
        except Exception:
            pass

    # ── Internal: Layering ────────────────────────────────────────────────────

    def _update_layering(
        self,
        ts_ms: int,
        bid_depth_usd: float,
        ask_depth_usd: float,
        trade_msg_rate_hz: float,
        flags: list,
    ) -> None:
        """
        Layering state machine (L2 approximation).

        Logic:
          - "idle":
              Update EMA baselines for bid/ask depth.
              Transition to "build" if:
                - trade_rate < LOW_HZ  (low trade activity = opportunity for layering)
                - depth on either side grows by > BUILD_MULT × baseline
                - absolute peak depth > MIN_PEAK_USD

          - "build":
              Track peak depth.
              Transition to "idle" (layering confirmed) if:
                - depth reverts by REVERT_FRAC within REVERT_MS
                - Update layering_score

          Decay layering_score when not in "build" and no recent trigger.
        """
        try:
            # Update baseline EMAs (slow, always)
            ema_a = self._lay_ema_alpha
            if self._lay_bid_ema_usd <= 0.0:
                self._lay_bid_ema_usd = bid_depth_usd
                self._lay_ask_ema_usd = ask_depth_usd
            else:
                self._lay_bid_ema_usd = ema_a * bid_depth_usd + (1.0 - ema_a) * self._lay_bid_ema_usd
                self._lay_ask_ema_usd = ema_a * ask_depth_usd + (1.0 - ema_a) * self._lay_ask_ema_usd

            state = self._lay_state

            if state == "idle":
                # Check if build condition met
                # Read ENV dynamically for runtime/test override
                low_trade_hz = float(os.getenv("LAYERING_TRADE_RATE_LOW_HZ", str(_LAY_TRADE_RATE_LOW_HZ_DEFAULT)) or _LAY_TRADE_RATE_LOW_HZ_DEFAULT)
                low_trade = trade_msg_rate_hz < low_trade_hz
                min_peak = float(os.getenv("LAYERING_MIN_PEAK_USD", str(_LAY_MIN_PEAK_USD_DEFAULT)) or _LAY_MIN_PEAK_USD_DEFAULT)
                build_mult = float(os.getenv("LAYERING_BUILD_MULT", str(_LAY_BUILD_MULT_DEFAULT)) or _LAY_BUILD_MULT_DEFAULT)

                bid_ratio = 0.0
                ask_ratio = 0.0
                if self._lay_bid_ema_usd > 0.0:
                    bid_ratio = bid_depth_usd / self._lay_bid_ema_usd
                if self._lay_ask_ema_usd > 0.0:
                    ask_ratio = ask_depth_usd / self._lay_ask_ema_usd

                # Only trigger if ratio exceeds build threshold and absolute size is meaningful
                build_bid = (bid_ratio >= build_mult) and (bid_depth_usd >= min_peak)
                build_ask = (ask_ratio >= build_mult) and (ask_depth_usd >= min_peak)

                if low_trade and (build_bid or build_ask):
                    self._lay_state = "build"
                    self._lay_build_ts_ms = ts_ms
                    self._lay_peak_bid_usd = bid_depth_usd
                    self._lay_peak_ask_usd = ask_depth_usd
                    self._lay_baseline_bid_usd = self._lay_bid_ema_usd
                    self._lay_baseline_ask_usd = self._lay_ask_ema_usd
                else:
                    # Decay score when idle
                    self.layering_score = float(self.layering_score * 0.85)

            elif state == "build":
                # Update peak (track highest depth seen during build)
                self._lay_peak_bid_usd = max(self._lay_peak_bid_usd, bid_depth_usd)
                self._lay_peak_ask_usd = max(self._lay_peak_ask_usd, ask_depth_usd)

                elapsed_ms = float(ts_ms - self._lay_build_ts_ms)
                revert_ms = float(os.getenv("LAYERING_REVERT_MS", str(_LAY_REVERT_MS_DEFAULT)) or _LAY_REVERT_MS_DEFAULT)
                revert_frac = float(os.getenv("LAYERING_REVERT_FRAC", str(_LAY_REVERT_FRAC_DEFAULT)) or _LAY_REVERT_FRAC_DEFAULT)
                ratio_min = float(os.getenv("LAYERING_RATIO_MIN", str(_LAY_RATIO_MIN_DEFAULT)) or _LAY_RATIO_MIN_DEFAULT)

                # Check revert condition: depth snapped back quickly
                bid_reverted = False
                ask_reverted = False

                if self._lay_peak_bid_usd > 0.0:
                    bid_drop = (self._lay_peak_bid_usd - bid_depth_usd) / self._lay_peak_bid_usd
                    bid_reverted = bid_drop >= revert_frac

                if self._lay_peak_ask_usd > 0.0:
                    ask_drop = (self._lay_peak_ask_usd - ask_depth_usd) / self._lay_peak_ask_usd
                    ask_reverted = ask_drop >= revert_frac

                # Max ratio during build (how much bigger than baseline)
                bid_build_ratio = (
                    self._lay_peak_bid_usd / max(self._lay_baseline_bid_usd, 1.0)
                )
                ask_build_ratio = (
                    self._lay_peak_ask_usd / max(self._lay_baseline_ask_usd, 1.0)
                )
                build_ratio = max(bid_build_ratio, ask_build_ratio)

                if elapsed_ms <= revert_ms and (bid_reverted or ask_reverted):
                    # Layering confirmed: compute score from build ratio
                    # score = min(1.0, (ratio - ratio_min) / max(ratio_min, 1.0))
                    if build_ratio >= ratio_min:
                        raw_score = (build_ratio - ratio_min) / max(ratio_min, 1.0)
                        self.layering_score = float(min(1.0, raw_score))
                        flags.append("LAYERING")
                    self._lay_state = "idle"

                elif elapsed_ms > revert_ms:
                    # Build phase timed out without revert → not layering
                    self._lay_state = "idle"
                    self.layering_score = float(self.layering_score * 0.85)

                # else: still in build window, no revert yet → wait

        except Exception:
            pass

    def snapshot(self) -> dict:
        """Lightweight dict for strategy / indicators / sidecar."""
        return {
            "quote_stuffing_score": float(self.quote_stuffing_score),
            "layering_score": float(self.layering_score),
            "manip_flags": str(self.manip_flags),
        }
