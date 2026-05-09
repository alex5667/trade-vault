#!/usr/bin/env python3

from __future__ import annotations

import json
import math
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from typing import Any


def _i(x: Any, d: int = 0) -> int:

    try:

        return int(x)

    except Exception:

        return d



def _f(x: Any, d: float = 0.0) -> float:

    try:

        v = float(x)

        return v if math.isfinite(v) else d

    except Exception:

        return d



class _PressureStub:

    def __init__(self, is_hi: bool):

        self._is_hi = bool(is_hi)



    def is_pressure_hi(self, now_ts_ms: int, per_min: float) -> bool:

        return bool(self._is_hi)



@dataclass

class ReplayRuntime:

    symbol=""

    # evidence

    last_obi_event: dict[str, Any] | None = None

    last_iceberg_event: dict[str, Any] | None = None

    last_ofi_event: dict[str, Any] | None = None

    last_bar: Any = None

    last_fp_edge: Any = None

    last_div: Any = None

    last_regime: Any = None

    book_churn_hi: int = 0

    dynamic_cfg: dict[str, Any] = None

    pressure: Any = None

    cont_ctx_ts_ms: int = 0

    liq_regime: Any = None

    last_sweep: Any = None

    last_reclaim: Any = None

    # weak progress (engine reads runtime.last_wp.weak_any)

    last_wp: Any = None



    def __post_init__(self):

        if self.dynamic_cfg is None:

            self.dynamic_cfg = {}

        if self.pressure is None:

            self.pressure = _PressureStub(False)



    @staticmethod

    def from_snapshot(symbol: str, snap: dict[str, Any]) -> ReplayRuntime:

        rt = ReplayRuntime(symbol=(symbol or ""))

        # dict-like events

        rt.last_obi_event = snap.get("last_obi_event")

        rt.last_iceberg_event = snap.get("last_iceberg_event")

        rt.last_ofi_event = snap.get("last_ofi_event")

        rt.last_regime = snap.get("last_regime")

        rt.book_churn_hi = _i(snap.get("book_churn_hi"), 0)

        rt.dynamic_cfg = snap.get("dynamic_cfg") or {}

        rt.cont_ctx_ts_ms = _i(snap.get("cont_ctx_ts_ms"), 0)

        rt.liq_regime = snap.get("liq_regime")

        rt.last_sweep = snap.get("last_sweep")

        rt.last_reclaim = snap.get("last_reclaim")



        # pressure

        rt.pressure = _PressureStub(bool(_i(snap.get("pressure_hi"), 0) == 1))



        # last_wp

        class _WP:

            def __init__(self, weak_any: bool):

                self.weak_any = bool(weak_any)

        rt.last_wp = _WP(bool(_i(snap.get("last_wp_weak_any"), 0) == 1))



        # last_bar / last_fp_edge / last_div (object-like access)

        def _obj_from_dict(d: Any):

            if d is None or isinstance(d, (str, int, float, bool)):

                return d

            if isinstance(d, dict):

                class _O:

                    pass

                o = _O()

                for k, v in d.items():

                    setattr(o, k, v)

                return o

            return d



        rt.last_bar = _obj_from_dict(snap.get("last_bar"))

        rt.last_fp_edge = _obj_from_dict(snap.get("last_fp_edge"))

        rt.last_div = _obj_from_dict(snap.get("last_div"))

        rt.last_sweep = _obj_from_dict(rt.last_sweep)

        rt.last_reclaim = _obj_from_dict(rt.last_reclaim)



        return rt



def iter_ndjson(path: str) -> Iterator[dict[str, Any]]:

    with open(path, encoding="utf-8") as f:

        for line in f:

            s = line.strip()

            if not s:

                continue

            yield json.loads(s)



def write_ndjson(path: str, rows: Iterable[dict[str, Any]]) -> None:

    with open(path, "w", encoding="utf-8") as f:

        for r in rows:

            f.write(json.dumps(r, ensure_ascii=False) + "\n")

