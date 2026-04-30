from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Dict, Any, List, Tuple
import json
import os
import time
import logging

from common.isotonic_calibration import IsotonicCalibrator, sanitize_breakpoints

logger = logging.getLogger(__name__)


def _clamp01(x: float) -> float:
    return 0.0 if x <= 0.0 else (1.0 if x >= 1.0 else float(x))


def _repair_monotonic(x: list[float], p: list[float]) -> tuple[list[float], list[float]]:
    """
    Fail-open repair:
      - сортируем по x
      - p clamp в [0..1]
      - enforce non-decreasing p (скан max)
    Не заменяет PAV, но защищает от битых файлов.
    """
    pairs = [(float(xx), float(pp)) for xx, pp in zip(x, p)]
    pairs.sort(key=lambda t: t[0])
    xs: list[float] = []
    ps: list[float] = []
    last_p = 0.0
    for xx, pp in pairs:
        pp2 = _clamp01(pp)
        if pp2 < last_p:
            pp2 = last_p
        last_p = pp2
        xs.append(xx)
        ps.append(pp2)
    return xs, ps


def _norm_side(x: Any) -> str:
    """
    Нормализует side для ключей калибровки.
    Поддерживаем разные формы, но основной контракт у вас уже: "LONG"/"SHORT".
    """
    if x is None:
        return "*"
    # IMPORTANT: bool is subclass of int => avoid True/False mapping to LONG/SHORT.
    if isinstance(x, bool):
        return "*"
    # Numeric path without exceptions.
    if isinstance(x, (int, float)):
        v = float(x)
        if not (v == v) or v in (float("inf"), float("-inf")):
            return "*"
        return "LONG" if v >= 0 else "SHORT"
    # String-ish path (fail-open)
    try:
        s = str(x).strip().upper()
    except Exception:
        return "*"
    if s in ("LONG", "BUY", "+1", "1"):
        return "LONG"
    if s in ("SHORT", "SELL", "-1"):
        return "SHORT"
    return "*"


@dataclass(frozen=True)
class CalibGroup:
    calibrator: IsotonicCalibrator
    n: int = 0  # число семплов, использованных при обучении (если известно)


class CalibStore:
    """
    Хранилище калибровок из JSON файла.
@@
    Fallback порядок (с side, и backward-compatible без side):
      1) kind:{kind}|symbol:{symbol}|side:{side}
      2) kind:{kind}|symbol:{symbol}|side:*
      3) kind:{kind}|symbol:*|side:{side}
      4) kind:*|symbol:{symbol}|side:{side}
      5) kind:{kind}|symbol:*|side:*
      6) kind:*|symbol:{symbol}|side:*
      7) global
    """

    def __init__(
        self
        path: str
        *
        min_samples: int = 300
        reload_sec: int = 30
        logger: Optional[logging.Logger] = None
    ) -> None:
        self.path = str(path or "")
        self.min_samples = int(min_samples)
        self.reload_sec = int(reload_sec)
        self.log = logger or logging.getLogger(__name__)

        self._groups: Dict[str, CalibGroup] = {}
        self._last_mtime: float = 0.0
        self._last_reload_ts: float = 0.0
        # Rate-limited warnings to keep hot paths quiet.
        self._warn_last_ts: Dict[str, float] = {}
        self._warn_every_sec: float = float(os.getenv("CALIB_WARN_EVERY_SEC", "30") or "30")

        # eager initial load (fail-open)
        self.load()

    def _warn_rl(self, code: str, msg: str, *args: Any) -> None:
        """
        Rate-limited warnings. Never raises.
        Used for load/sanitize errors to avoid log spam on reload loops.
        """
        try:
            now = time.monotonic()
            last = float(self._warn_last_ts.get(code, 0.0))
            if (now - last) < self._warn_every_sec:
                return
            self._warn_last_ts[code] = now
            logger.warning(msg, *args)
        except Exception:
            return

    def load(self) -> None:
        if not self.path:
            self._groups = {}
            return
        try:
            st = os.stat(self.path)
            mtime = float(st.st_mtime)
        except Exception:
            # file missing/unreadable -> empty (fail-open)
            self._groups = {}
            return

        try:
            with open(self.path, "r", encoding="utf-8") as f:
                obj = json.load(f)
            raw_groups = (obj or {}).get("groups", {}) or {}
            out: Dict[str, CalibGroup] = {}
            for key, v in raw_groups.items():
                vv = v or {}
                if vv.get("type") != "isotonic":
                    continue
                x = list(vv.get("x", []) or [])
                p = list(vv.get("p", []) or [])
                mode = str(vv.get("mode", "linear") or "linear").strip().lower()
                n = int(vv.get("n", 0) or 0)
                # sanitize_breakpoints can throw on broken content -> skip group (fail-open)
                try:
                    cal = sanitize_breakpoints(x, p, mode=mode)
                except Exception as e:
                    self._warn_rl("calib_sanitize_breakpoints", "sanitize_breakpoints failed key=%s err=%s", key, repr(e))
                    continue
                # минимальная валидация
                if not cal.x or not cal.p or len(cal.x) != len(cal.p):
                    continue
                # лёгкая санитация (fail-open, но deterministic)
                try:
                    cal = cal.sanitize()
                except Exception as e:
                    self._warn_rl("calib_cal_sanitize", "IsotonicCalibrator.sanitize failed key=%s err=%s", key, repr(e))
                    continue
                # repair monotonic (fail-open, но deterministic)
                try:
                    rx, rp = _repair_monotonic(list(cal.x), list(cal.p))
                    if rx and rp and len(rx) == len(rp):
                        cal = IsotonicCalibrator(x=rx, p=rp, mode=mode)
                except Exception as e:
                    self._warn_rl("calib_repair_monotonic", "repair_monotonic failed key=%s err=%s", key, repr(e))
                    continue
                out[str(key)] = CalibGroup(calibrator=cal, n=n)

            # Atomic replace: if parse succeeded, swap groups.
            self._groups = out
            self._last_mtime = mtime
        except Exception as e:
            # fail-open: keep old groups if any, but log once per reload attempt
            self._warn_rl("calib_load_failed", "CalibStore.load failed: %s", repr(e))

    def maybe_reload(self, now_ts: Optional[float] = None) -> None:
        """
        Дешёвый re-load:
          - не чаще reload_sec
          - только если mtime изменился
        """
        if not self.path:
            return
        now = float(now_ts if now_ts is not None else time.time())
        if self.reload_sec > 0 and (now - self._last_reload_ts) < float(self.reload_sec):
            return
        self._last_reload_ts = now

        try:
            st = os.stat(self.path)
            mtime = float(st.st_mtime)
        except Exception:
            return
        if mtime <= self._last_mtime + 1e-12:
            return
        self.load()

    def _pick(self, keys: List[str]) -> Tuple[Optional[CalibGroup], Optional[str]]:
        """
        Общая логика выбора группы с учётом min_samples и базовой валидации.
        Fail-open: если группа плохая/малая — просто пропускаем.
        """
        for k in keys:
            g = self._groups.get(k)
            if not g:
                continue
            # min_samples gate:
            # - если n известно (n>0) и меньше порога -> не используем
            # - если n==0 (не указано) -> допускаем, но лучше при обучении всегда писать n
            if g.n and g.n < self.min_samples:
                continue
            cal = g.calibrator
            if cal.x and cal.p and len(cal.x) == len(cal.p):
                return g, k
        return None, None


    def get_group(self, *, kind: str, symbol: str, side: Any = None) -> Tuple[Optional[CalibGroup], Optional[str]]:
        """
        Единственный публичный API выбора группы.
        Возвращает (group, key), где key — фактический выбранный ключ.

        Backward-compatible:
          - если side не задан -> ищем side:* и legacy (без side)
          - если side задан -> сначала side-aware ключи, затем side:*, затем legacy
        """
        kind_s = str(kind or "*")
        symbol_s = str(symbol or "*")
        side_s = _norm_side(side) if side is not None else "*"

        keys: List[str] = []
        # Side-aware (most specific first)
        if side is not None:
            keys.extend([
                f"kind:{kind_s}|symbol:{symbol_s}|side:{side_s}"
                f"kind:{kind_s}|symbol:{symbol_s}|side:*"
                f"kind:{kind_s}|symbol:*|side:{side_s}"
                f"kind:*|symbol:{symbol_s}|side:{side_s}"
            ])
        # Side-agnostic (new format)
        keys.extend([
            f"kind:{kind_s}|symbol:{symbol_s}|side:*"
            f"kind:{kind_s}|symbol:*|side:*"
            f"kind:*|symbol:{symbol_s}|side:*"
            "global"
        ])

        # Backward-compatible keys (older calibration.json without side)
        keys_legacy = [
            f"kind:{kind_s}|symbol:{symbol_s}"
            f"kind:{kind_s}|symbol:*"
            f"kind:*|symbol:{symbol_s}"
            "global"
        ]
        g, k = self._pick(keys)
        if g is not None:
            return g, k
        return self._pick(keys_legacy)

    def get_group_obj(self, *, kind: str, symbol: str, side: Any = None) -> Optional[CalibGroup]:
        """Convenience wrapper when only group is needed."""
        g, _k = self.get_group(kind=kind, symbol=symbol, side=side)
        return g

    def get(self, *, kind: str, symbol: str, side: Any = None) -> Optional[IsotonicCalibrator]:
        g, _k = self.get_group(kind=kind, symbol=symbol, side=side)
        return g.calibrator if g else None
