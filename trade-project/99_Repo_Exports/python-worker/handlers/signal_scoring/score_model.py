from __future__ import annotations

import logging
import math
import os
import time
from dataclasses import dataclass, field
from typing import Any

from common.calibration_store import CalibStore
from common.kind_normalize import normalize_kind
import contextlib

logger = logging.getLogger(__name__)

# NOTE: sigmoid fallback stays local to keep scoring independent from calibration availability.


def _f(x: Any, default: float = 0.0) -> float:
    try:
        v = float(x)
        if not math.isfinite(v):
            return default
        return float(v)
    except Exception:
        return default


def _clamp01(x: float) -> float:
    return 0.0 if x <= 0.0 else (1.0 if x >= 1.0 else float(x))


def _sigmoid(x: float) -> float:
    # стабильная сигмоида (stable-ish sigmoid)
    if x >= 50:
        return 1.0
    if x <= -50:
        return 0.0
    return 1.0 / (1.0 + math.exp(-x))


def _norm_side(x: Any) -> str:
    """
    Нормализация стороны для ключей калибровки.
    Поддерживаем разные формы:
      - "LONG"/"SHORT"
      - "buy"/"sell"
      - 1/-1
    """
    if x is None:
        return "*"
    # IMPORTANT: bool is a subclass of int -> avoid True => LONG / False => LONG surprises.
    if isinstance(x, bool):
        return "*"
    # Numeric path without exceptions / float() on weird objects.
    if isinstance(x, (int, float)):
        v = float(x)
        if math.isfinite(v):
            return "LONG" if v >= 0 else "SHORT"
        return "*"
    # String-ish path (fail-open)
    try:
        s = str(x).strip().upper()
    except Exception:
        return "*"
    if s in ("LONG", "BUY", "BULL"):
        return "LONG"
    if s in ("SHORT", "SELL", "BEAR"):
        return "SHORT"
    # Common numeric string values.
    if s in ("1", "+1"):
        return "LONG"
    if s in ("-1",):
        return "SHORT"
    return "*"


@dataclass(frozen=True)
class ScoreOut:
    final_score: float
    conf_factor01: float
    confidence_pct: float
    parts: dict[str, float] = field(default_factory=dict)
    # String/debug metadata MUST NOT live in parts (parts is numeric-only and can be consumed by metrics pipes).
    meta: dict[str, str] = field(default_factory=dict)


class ScoreModel:
    """
    Одна ось:
      conf_factor01 = f(regime, geometry, liquidity, l3, micro_quality) in [0..1]
      final_score   = raw_score * conf_factor01
      confidence_pct = calibration(abs(final_score), kind, symbol, side)
    """
    def __init__(self) -> None:
        self._k = float(os.getenv("CONF_CAL_K", "2.4"))      # крутизна (steepness)
        self._b = float(os.getenv("CONF_CAL_B", "0.10"))     # смещение (offset)
        self._cap = float(os.getenv("CONF_PCT_CAP", "99.0"))
        # Optional diagnostics (rate-limited).
        self._log_calib_errors = (os.getenv("CONF_CAL_LOG_ERRORS", "0") == "1")
        self._log_calib_errors_every_sec = float(os.getenv("CONF_CAL_LOG_ERRORS_EVERY_SEC", "30") or "30")
        self._last_calib_err_ts = 0.0

        # Calibration mode:
        #   sigmoid   - default, stateless
        #   isotonic  - uses CalibStore (JSON trained offline)
        self._mode = (os.getenv("CONF_CAL_MODE", "sigmoid") or "sigmoid").strip().lower()
        self._cal_path = (os.getenv("CONF_CAL_PATH", "") or "")
        self._min_samples = int(os.getenv("CONF_CAL_MIN_SAMPLES", "300") or "300")
        self._reload_sec = int(os.getenv("CONF_CAL_RELOAD_SEC", "30") or "30")

        self._shrink_strength = float(os.getenv("CONF_CAL_SHRINK_STRENGTH", "800.0"))
        self._calib_store: CalibStore | None = None
        if self._mode == "isotonic" and self._cal_path:
            # fail-open inside CalibStore (empty if missing/broken)
            self._calib_store = CalibStore(self._cal_path, min_samples=self._min_samples, reload_sec=self._reload_sec)

    def _log_calib_exc(self, e: Exception, *, kind: str, symbol: str) -> None:
        """
        Rate-limited logging for calibration failures.
        We keep scoring fail-open; logging is opt-in to avoid noisy hot path.
        """
        if not self._log_calib_errors:
            return
        now = time.monotonic()
        if (now - self._last_calib_err_ts) < self._log_calib_errors_every_sec:
            return
        self._last_calib_err_ts = now
        try:
            logger.warning(
                "ScoreModel calibration failed (mode=isotonic). Falling back to sigmoid. kind=%s symbol=%s err=%s",
                kind, symbol, repr(e),
            )
        except Exception:
            # never break scoring due to logging
            return

    def _ctx_symbol(self, ctx: Any) -> str:
        try:
            s = getattr(ctx, "symbol", "*") or "*"
            return str(s)
        except Exception:
            return "*"

    def _ctx_side(self, ctx: Any) -> str:
        """
        В проекте side бывает как 'LONG'/'SHORT', или direction, или enum.
        Нормализуем к 'LONG'/'SHORT'/'*'.
        """
        v = None
        try:
            v = getattr(ctx, "side", None)
        except Exception:
            v = None
        if v is None:
            try:
                v = getattr(ctx, "direction", None)
            except Exception:
                v = None
        if v is None:
            return "*"
        try:
            sv = str(v).upper()
        except Exception:
            return "*"
        if "LONG" in sv:
            return "LONG"
        if "SHORT" in sv:
            return "SHORT"
        return "*"

    def score(self, *, raw_score: float, conf_factor01: float, kind: str, ctx: Any, parts_in: dict[str, float]) -> ScoreOut:
        cf = _clamp01(_f(conf_factor01, 0.0))
        rs = _f(raw_score, 0.0)
        final = float(rs * cf)

        # ---- Calibration ----
        # abs(final) => уверенность не зависит от направления, только от "силы" сигнала.
        #
        # Fail-open стратегия:
        #   - если isotonic включён, но нет подходящей группы / файл битый / мало семплов:
        #       -> fallback на sigmoid
        abs_final = float(abs(final))

        pct: float
        cal_used = "sigmoid"
        cal_key = ""
        cal_n = 0
        meta: dict[str, str] = {}

        if self._mode == "isotonic" and self._calib_store is not None:
            try:
                # лёгкий reload по mtime (не чаще CONF_CAL_RELOAD_SEC)
                self._calib_store.maybe_reload()
                symbol = self._ctx_symbol(ctx)
                side = self._ctx_side(ctx)
                k_norm = normalize_kind((kind or "*"))
                g, k_used = self._calib_store.get_group(kind=str(k_norm), symbol=symbol, side=side)
                if g is not None and k_used:
                    cal_key = str(k_used)
                    cal_n = int(g.n or 0)
                    p_group = float(_clamp01(g.calibrator.predict(abs_final)))
                    # shrink к global, если это не global и есть strength
                    p = p_group
                    if self._shrink_strength > 0 and cal_key != "global":
                        g0, _k0 = self._calib_store.get_group(kind="*", symbol="*")
                        if g0 is not None:
                            p0 = float(_clamp01(g0.calibrator.predict(abs_final)))
                            alpha = float(cal_n / max(1.0, (cal_n + self._shrink_strength))) if cal_n > 0 else 0.0
                            p = float(_clamp01(alpha * p_group + (1.0 - alpha) * p0))
                    pct = float(min(self._cap, max(0.0, 100.0 * p)))
                    cal_used = "isotonic"
                else:
                    # fallback -> sigmoid
                    x = (abs_final - self._b) * self._k
                    pct = float(min(self._cap, max(0.0, 100.0 * _sigmoid(x))))
            except Exception as e:
                # сверх-защита: не ломаем scoring
                with contextlib.suppress(Exception):
                    self._log_calib_exc(e, kind=(kind or "*"), symbol=self._ctx_symbol(ctx))
                x = (abs_final - self._b) * self._k
                pct = float(min(self._cap, max(0.0, 100.0 * _sigmoid(x))))
        else:
            x = (abs_final - self._b) * self._k
            pct = float(min(self._cap, max(0.0, 100.0 * _sigmoid(x))))

        parts = dict(parts_in or {})
        parts["raw_score"] = float(rs)
        parts["conf_factor01"] = float(cf)
        parts["final_score"] = float(final)
        parts["confidence_pct"] = float(pct)
        # Numeric flags only (parts is dict[str,float])
        parts["confidence_calibration_isotonic"] = float(1.0 if cal_used == "isotonic" else 0.0)
        parts["confidence_p_win"] = float(pct / 100.0)  # convenience/debug
        parts["confidence_calib_n"] = float(cal_n)
        # Put string/debug info into meta, not parts.
        meta["confidence_calib_key"] = (cal_key or "")
        meta["confidence_calibration_used"] = str(cal_used)
        return ScoreOut(final_score=float(final), conf_factor01=float(cf), confidence_pct=float(pct), parts=parts, meta=meta)
