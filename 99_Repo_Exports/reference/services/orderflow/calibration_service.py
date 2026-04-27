import logging
from typing import Dict
from services.orderflow.metrics import log_silent_error

logger = logging.getLogger("crypto_orderflow.calibration_service")

class CalibrationService:
    def __init__(self, *, repo, logger_service=None):
        self.repo = repo
        self.log = logger_service or logger

    async def ensure_loaded(self, runtime) -> None:
        """
        Orchestrates loading of all calibrations for a given runtime.
        Uses fail-open principle to prevent retry-storms.
        """
        if getattr(runtime, "_calib_loaded", False) and \
           getattr(runtime, "_book_calib_loaded", False) and \
           getattr(runtime, "_dn_calib_loaded", False) and \
           getattr(runtime, "_atr_bps_loaded", False) and \
           getattr(runtime, "_atr_sanity_loaded", False) and \
           getattr(runtime, "_atr_tf_loaded", False):
            return

        try:
            await self.repo.load_all(runtime)
        except Exception as exc:
            log_silent_error(exc, "calib_load_failure", runtime.symbol, "calib_svc:ensure_loaded")
        finally:
            # Mark all as loaded even on failure to avoid infinite retry loops (fail-open)
            runtime._calib_loaded = True
            runtime._book_calib_loaded = True
            runtime._dn_calib_loaded = True
            runtime._dn_loaded = True
            runtime._atr_bps_loaded = True
            runtime._atr_sanity_loaded = True
            runtime._atr_tf_loaded = True

    async def persist_effq(self, runtime, regime: str, ts_ms: int) -> None:
        """
        Saves EffQuote calibration state.
        """
        try:
            await self.repo.save_effq(runtime, regime=regime, ts_ms=ts_ms)
        except Exception as exc:
            log_silent_error(exc, "persist_failure", runtime.symbol, "calib_svc:persist_effq")

    async def persist_bookrate(self, runtime, regime: str, ts_ms: int) -> None:
        """
        Saves BookRate calibration state.
        """
        try:
            await self.repo.save_bookrate(runtime, regime=regime, ts_ms=ts_ms)
        except Exception as exc:
            log_silent_error(exc, "persist_failure", runtime.symbol, "calib_svc:persist_bookrate")

    async def persist_dn(self, runtime, regime: str, ts_ms: int) -> None:
        """
        Saves DeltaNotional calibration state.
        """
        try:
            await self.repo.save_dn(runtime, regime=regime, ts_ms=ts_ms)
        except Exception as exc:
            log_silent_error(exc, "persist_failure", runtime.symbol, "calib_svc:persist_dn")

    async def persist_tick_dn(self, runtime, regime: str, ts_ms: int) -> None:
        """
        Saves Tick DeltaNotional calibration state.
        """
        try:
            await self.repo.save_tick_dn(runtime, regime=regime, ts_ms=ts_ms)
        except Exception as exc:
            log_silent_error(exc, "persist_failure", runtime.symbol, "calib_svc:persist_tick_dn")

    async def persist_atr_bps(self, runtime, regime: str, ts_ms: int) -> None:
        """
        Saves ATR BPS calibration state.
        """
        try:
            await self.repo.save_atr_bps(runtime, regime=regime, ts_ms=ts_ms)
        except Exception as exc:
            log_silent_error(exc, "persist_failure", runtime.symbol, "calib_svc:persist_atr_bps")

    async def persist_atr_sanity(self, runtime, tf_norm: str, ts_ms: int) -> None:
        """
        Saves ATR sanity calibration state.
        """
        try:
            await self.repo.save_atr_sanity(runtime, tf_norm=tf_norm, ts_ms=ts_ms)
        except Exception as exc:
            log_silent_error(exc, "persist_failure", runtime.symbol, "calib_svc:persist_atr_sanity")

    async def persist_atr_tf_choice(self, runtime, choice_state: Dict, ts_ms: int) -> None:
        """
        Saves ATR TF choice.
        """
        try:
            await self.repo.save_atr_tf_choice(runtime, choice_state=choice_state, ts_ms=ts_ms)
        except Exception as exc:
            log_silent_error(exc, "persist_failure", runtime.symbol, "calib_svc:persist_atr_tf_choice")

    async def persist_atr_tf_regime(self, runtime, regime: str, ts_ms: int) -> None:
        """
        Saves ATR TF regime state.
        """
        try:
            await self.repo.save_atr_tf_regime(runtime, regime=regime, ts_ms=ts_ms)
        except Exception as exc:
            log_silent_error(exc, "persist_failure", runtime.symbol, "calib_svc:persist_atr_tf_regime")
