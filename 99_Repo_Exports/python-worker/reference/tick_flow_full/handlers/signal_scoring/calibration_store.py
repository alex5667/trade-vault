from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional, Tuple
import json
import os
import time

from .isotonic import IsotonicCalibrator


def _now_monotonic() -> float:
    return time.monotonic()


def _safe_int(x: Any, default: int = 0) -> int:
    try:
        v = int(x)
        return v
    except Exception:
        return int(default)


@dataclass(frozen=True)
class CalibGroup:
    cal: IsotonicCalibrator
    n: int


class CalibStore:
    """
    Хранилище калибраторов (isotonic) с fail-open:
      - битый файл/нет группы/мало семплов -> просто вернём None (ScoreModel уйдёт в sigmoid)

    JSON schema (пример):
    {
      "version": 2
      "trained_at": 1730000000
      "groups": {
        "global": { "type":"isotonic", "x":[...], "p":[...], "n": 12000 }
        "kind:absorption|symbol:BTCUSDT|side:LONG": { "type":"isotonic", "x":[...], "p":[...], "n": 980 }
      }
    }
    """

    def __init__(self, path: str, *, min_samples: int = 300, reload_sec: float = 5.0) -> None:
        self.path = str(path or "")
        self.min_samples = int(min_samples)
        self.reload_sec = float(reload_sec)

        self._groups: dict[str, CalibGroup] = {}
        self._last_mtime: float = 0.0
        self._last_reload_mono: float = 0.0

    def load(self) -> None:
        if not self.path:
            self._groups = {}
            return
        try:
            st = os.stat(self.path)
            mtime = float(st.st_mtime)
            with open(self.path, "r", encoding="utf-8") as f:
                obj = json.load(f)
            raw_groups = (obj or {}).get("groups", {}) or {}

            out: dict[str, CalibGroup] = {}
            for key, v in raw_groups.items():
                vv = v or {}
                if vv.get("type") != "isotonic":
                    continue
                x = list(vv.get("x", []) or [])
                p = list(vv.get("p", []) or [])
                n = _safe_int(vv.get("n", 10**9), 10**9)  # если нет n -> считаем "достаточно"
                if n < self.min_samples:
                    continue
                cal = IsotonicCalibrator(x=x, p=p, mode=str(vv.get("mode", "linear") or "linear"))
                if not cal.x or not cal.p:
                    continue
                out[str(key)] = CalibGroup(cal=cal, n=int(n))

            self._groups = out
            self._last_mtime = mtime
        except Exception:
            # fail-open: просто очищаем, чтобы ScoreModel ушёл в sigmoid
            self._groups = {}

    def maybe_reload(self) -> None:
        # ограничиваем частоту проверок, чтобы не дёргать stat на каждом тике
        now = _now_monotonic()
        if (now - self._last_reload_mono) < self.reload_sec:
            return
        self._last_reload_mono = now

        if not self.path:
            return
        try:
            mtime = float(os.stat(self.path).st_mtime)
            if mtime > self._last_mtime + 1e-9:
                self.load()
        except Exception:
            # fail-open
            return

    def get_group(self, *, kind: str, symbol: str, side: str) -> Optional[Tuple[IsotonicCalibrator, int]]:
        """
        Возвращает (calibrator, n) или None.

        Порядок fallback:
          1) kind+symbol+side
          2) kind+symbol+*
          3) kind+*+side
          4) kind+*+*
          5) *+symbol+side
          6) *+symbol+*
          7) *+*+side
          8) global

        Плюс legacy fallback (без side) для старых файлов:
          - kind+symbol
          - kind+*
          - *+symbol
        """
        k = str(kind or "*")
        s = str(symbol or "*")
        sd = str(side or "*")

        keys = [
            f"kind:{k}|symbol:{s}|side:{sd}"
            f"kind:{k}|symbol:{s}|side:*"
            f"kind:{k}|symbol:*|side:{sd}"
            f"kind:{k}|symbol:*|side:*"
            f"kind:*|symbol:{s}|side:{sd}"
            f"kind:*|symbol:{s}|side:*"
            f"kind:*|symbol:*|side:{sd}"
            "global"
            # legacy:
            f"kind:{k}|symbol:{s}"
            f"kind:{k}|symbol:*"
            f"kind:*|symbol:{s}"
        ]
        for kk in keys:
            g = self._groups.get(kk)
            if g and g.cal and g.cal.x and g.cal.p:
                return (g.cal, int(g.n))
        return None

    def get(self, *, kind: str, symbol: str) -> Optional[IsotonicCalibrator]:
        # legacy API: без side
        gg = self.get_group(kind=kind, symbol=symbol, side="*")
        return gg[0] if gg else None
