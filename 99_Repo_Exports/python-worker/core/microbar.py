from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any

from core.crypto_orderflow_detectors import classify_signed_qty
from core.footprint_lite import FootprintLite, FootprintSnapshot
import contextlib


def _safe_float(x: Any) -> float | None:
    try:
        if x is None:
            return None
        v = float(x)
        if math.isfinite(v):
            return v
        return None
    except Exception:
        return None


def _safe_int(x: Any) -> int | None:
    try:
        if x is None:
            return None
        v = int(x)
        return v
    except Exception:
        return None


@dataclass
class MicroBar:
    """
    Микро-бар (строится из тиков).

    Поля:
    - OHLC: по trade price (tick.price)
    - vol: суммарный qty по тикам
    - delta_sum: суммарный signed qty (агрессивная дельта)
    - cvd_close: значение tick-level CVD на закрытии бара
    - vwap: sum(price*qty)/sum(qty)
    - spread_mid/spread: на момент последнего тика
    """

    symbol: str
    tf_ms: int
    start_ts_ms: int
    end_ts_ms: int

    open: float
    high: float
    low: float
    close: float

    vol: float = 0.0
    delta_sum: float = 0.0
    cvd_close: float = 0.0

    vwap: float = 0.0
    _pv_sum: float = 0.0

    bid_last: float | None = None
    ask_last: float | None = None
    mid_last: float | None = None
    spread_last: float | None = None

    tick_count: int = 0

    # --- Phase D: footprint-lite (optional) ---
    fp_enabled: bool = False
    fp_bucket_px: float = 0.0
    fp_max_buckets: int = 0
    fp_evictions: int = 0
    fp_bad_price: int = 0

    fp_n_buckets: int = 0
    fp_max_imbalance: float = 0.0
    fp_peak_delta: float = 0.0
    fp_peak_total: float = 0.0
    fp_peak_bucket_px: float = 0.0
    fp_delta_concentration: float = 0.0
    fp_progress: float = 0.0
    fp_absorb_score: float = 0.0
    fp_absorption_bias: str = "NONE"

    # NEW: ratio & ladder & POC & efficiency (Round 6)
    fp_max_imb_ratio: float = 1.0
    fp_ladder_low_len: int = 0
    fp_ladder_high_len: int = 0
    fp_poc_total: float = 0.0
    fp_poc_bucket_px: float = 0.0
    fp_poc_on_edge: int = 0
    fp_poc_edge_side: str = "NONE"
    fp_eff_delta: float = 0.0
    fp_move_bp: float = 0.0
    fp_quote_delta: float = 0.0
    fp_eff_quote: float = 0.0
    fp_eff_vol: float = 0.0

    _fp: FootprintLite | None = None

    def update_from_tick(self, tick: dict[str, Any], cvd_current: float) -> None:
        px = _safe_float(tick.get("price") or tick.get("last") or tick.get("mid"))
        if px is None:
            return

        qty = _safe_float(tick.get("qty") or tick.get("volume") or 0.0)
        if qty < 0:
            qty = abs(qty)

        # OHLC
        self.close = px
        self.high = max(self.high, px)
        self.low = min(self.low, px)

        # volume + VWAP
        self.vol += qty
        self._pv_sum += px * qty
        if self.vol > 0:
            self.vwap = self._pv_sum / self.vol

        # signed delta
        d = classify_signed_qty(tick, override_qty=qty)
        self.delta_sum += d

        # book fields (optional)
        bid = _safe_float(tick.get("bid"))
        ask = _safe_float(tick.get("ask"))
        if bid is not None and ask is not None and ask >= bid:
            self.bid_last = bid
            self.ask_last = ask
            self.mid_last = 0.5 * (bid + ask)
            self.spread_last = ask - bid

        self.cvd_close = float(cvd_current)
        self.tick_count += 1

        # Phase D: footprint-lite update (O(1))
        try:
            if self.fp_enabled and self._fp is not None and self.fp_bucket_px > 0:
                self._fp.update(price=px, qty=qty, signed_qty=d)
        except Exception:
            # fail-open: footprint must not break bar aggregation
            pass

    def finalize_footprint(self) -> None:
        """
        Вызывается на bar_close (в MicroBarAggregator._finalize_cur()).
        Делает O(max_buckets) скан по bucket'ам и записывает признаки в бар.
        """
        if not self.fp_enabled or self._fp is None:
            return
        try:
            snap: FootprintSnapshot = self._fp.finalize(
                bar_open=float(self.open),
                bar_close=float(self.close),
                bar_high=float(self.high),
                bar_low=float(self.low),
                bar_delta_sum=float(self.delta_sum),
                bar_vol=float(self.vol),
            )
            self.fp_bucket_px = float(snap.bucket_px)
            self.fp_n_buckets = int(snap.n_buckets)
            self.fp_max_imbalance = float(snap.max_imbalance)
            self.fp_peak_delta = float(snap.peak_delta)
            self.fp_peak_total = float(snap.peak_total)
            self.fp_peak_bucket_px = float(snap.peak_bucket_px)
            self.fp_delta_concentration = float(snap.delta_concentration)
            self.fp_progress = float(snap.progress)
            self.fp_absorb_score = float(snap.absorb_score)
            self.fp_absorption_bias = str(snap.absorption_bias)

            # Round 6: New stats (robust: support .get or .extra or dict)
            extract = snap.get if hasattr(snap, "get") else None
            if not extract and hasattr(snap, "extra") and isinstance(snap.extra, dict):
                extract = snap.extra.get
            elif isinstance(snap, dict):
                extract = snap.get

            if extract:
                self.fp_max_imb_ratio = float(extract("fp_max_imb_ratio", 1.0))
                self.fp_ladder_low_len = int(extract("fp_ladder_low_len", 0))
                self.fp_ladder_high_len = int(extract("fp_ladder_high_len", 0))
                self.fp_poc_total = float(extract("fp_poc_total", 0.0))
                self.fp_poc_bucket_px = float(extract("fp_poc_bucket_px", 0.0))
                self.fp_poc_on_edge = int(extract("fp_poc_on_edge", 0))
                self.fp_poc_edge_side = str(extract("fp_poc_edge_side", "NONE"))
                self.fp_eff_delta = float(extract("fp_eff_delta", 0.0))
                self.fp_move_bp = float(extract("fp_move_bp", 0.0))
                self.fp_quote_delta = float(extract("fp_quote_delta", 0.0))
                self.fp_eff_quote = float(extract("fp_eff_quote", 0.0))
                self.fp_eff_vol = float(extract("fp_eff_vol", 0.0))

            self.fp_evictions = int(self._fp.evictions)
            self.fp_bad_price = int(self._fp.bad_price)
        except Exception:
            pass


class MicroBarAggregator:
    """
    Агрегатор микро-баров per-symbol.
    """

    def __init__(
        self,
        symbol: str,
        mode: str = "time",
        tf_ms: int = 1000,
        volume_target: float = 0.0,
        tick_size: float = 0.0,
    ):
        self.symbol = symbol
        self.mode = m = mode  # "time" | "volume"
        self.tf_ms = tf_ms
        self.volume_target = volume_target
        self.tick_size = float(tick_size)

        # Phase D footprint config
        self.fp_enabled: bool = False
        self.fp_bucket_px: float = 0.0
        self.fp_bucket_bp: float = 2.0
        self.fp_max_buckets: int = 200

        self.cur: MicroBar | None = None
        self.cur_bucket: int | None = None
        self.last_ts_ms: int | None = None

        self.bad_time_count = 0
        self.empty_price_count = 0

    def apply_config(self, cfg: dict[str, Any]) -> None:
        try:
            m = str(cfg.get("microbar_mode", self.mode) or self.mode)
            if m in ("time", "volume"):
                self.mode = m
        except Exception:
            pass
        try:
            tf = int(cfg.get("microbar_tf_ms", self.tf_ms))
            if tf >= 200:
                self.tf_ms = tf
        except Exception:
            pass
        try:
            vt = float(cfg.get("microbar_volume_target", self.volume_target))
            if vt >= 0:
                self.volume_target = vt
        except Exception:
            pass

        # Phase D: footprint-lite params
        with contextlib.suppress(Exception):
            self.fp_enabled = bool(cfg.get("fp_enabled", self.fp_enabled))
        try:
            self.fp_bucket_px = float(cfg.get("fp_bucket_px", self.fp_bucket_px))
            if self.fp_bucket_px < 0:
                self.fp_bucket_px = 0.0
        except Exception:
            pass
        try:
            self.fp_bucket_bp = float(cfg.get("fp_bucket_bp", self.fp_bucket_bp))
            if self.fp_bucket_bp < 0:
                self.fp_bucket_bp = 0.0
        except Exception:
            pass
        try:
            self.fp_max_buckets = int(cfg.get("fp_max_buckets", self.fp_max_buckets))
            if self.fp_max_buckets < 16:
                self.fp_max_buckets = 16
        except Exception:
            pass
        try:
            ts = float(cfg.get("tick_size", self.tick_size))
            if ts >= 0:
                self.tick_size = ts
        except Exception:
            pass

    def _start_new_bar(self, ts_ms: int, px: float) -> None:
        if self.mode == "time":
            bucket = ts_ms // self.tf_ms
            start = bucket * self.tf_ms
            end = start + self.tf_ms
            self.cur_bucket = int(bucket)
        else:
            start = ts_ms
            end = ts_ms
            self.cur_bucket = None

        self.cur = MicroBar(
            symbol=self.symbol,
            tf_ms=self.tf_ms,
            start_ts_ms=int(start),
            end_ts_ms=int(end),
            open=px,
            high=px,
            low=px,
            close=px,
        )

        # Phase D: init footprint-lite for this bar (optional)
        try:
            self.cur.fp_enabled = bool(self.fp_enabled)
            self.cur.fp_max_buckets = int(self.fp_max_buckets)

            bucket_px = 0.0
            if self.fp_bucket_px and self.fp_bucket_px > 0:
                bucket_px = float(self.fp_bucket_px)
            elif self.fp_bucket_bp and self.fp_bucket_bp > 0 and px > 0:
                # bucket_px per bar (фиксируем на старте бара)
                bucket_px = float(px) * (float(self.fp_bucket_bp) / 10000.0)
                # защита от слишком мелких bucket на дешёвых инструментах
                bucket_px = max(bucket_px, 1e-9)

            # NEW: snap bucket to tick grid (if tick_size known)
            tick_size = float(self.tick_size or 0.0)
            if tick_size > 0 and bucket_px > 0:
                bucket_px = max(bucket_px, tick_size)
                bucket_px = round(bucket_px / tick_size) * tick_size

            self.cur.fp_bucket_px = float(bucket_px)
            if self.cur.fp_enabled and bucket_px > 0:
                self.cur._fp = FootprintLite(bucket_px=bucket_px, max_buckets=self.fp_max_buckets)
        except Exception:
            # fail-open
            self.cur.fp_enabled = False
            self.cur._fp = None

    def _finalize_cur(self, ts_ms_close: int) -> MicroBar | None:
        if not self.cur:
            return None
        if self.mode == "volume":
            self.cur.end_ts_ms = int(ts_ms_close)

        # Phase D: finalize footprint metrics
        with contextlib.suppress(Exception):
            self.cur.finalize_footprint()

        out = self.cur
        self.cur = None
        self.cur_bucket = None
        return out

    def push_tick(self, tick: dict[str, Any], cvd_current: float) -> list[MicroBar]:
        """
        Возвращает список закрытых баров (обычно 0 или 1).
        """
        out: list[MicroBar] = []

        ts_ms = _safe_int(tick.get("ts"))
        if ts_ms is None or ts_ms <= 0:
            self.bad_time_count += 1
            return out

        # out-of-order: игнорируем (fail-open), но учитываем
        if self.last_ts_ms is not None and ts_ms < self.last_ts_ms:
            self.bad_time_count += 1
            return out
        self.last_ts_ms = ts_ms

        px = _safe_float(tick.get("price") or tick.get("last") or tick.get("mid"))
        if px is None:
            self.empty_price_count += 1
            return out

        # --- time-bars: close on bucket change
        if self.mode == "time":
            bucket = ts_ms // self.tf_ms
            if self.cur is None:
                self._start_new_bar(ts_ms, px)
            elif self.cur_bucket is not None and bucket != self.cur_bucket:
                # закрываем предыдущий бар и открываем новый
                closed = self._finalize_cur(ts_ms_close=ts_ms)
                if closed is not None:
                    out.append(closed)
                self._start_new_bar(ts_ms, px)

            # обновляем текущий бар
            if self.cur is not None:
                self.cur.update_from_tick(tick, cvd_current=cvd_current)
            return out

        # --- volume-bars: close when vol >= target
        if self.cur is None:
            self._start_new_bar(ts_ms, px)
        if self.cur is not None:
            self.cur.update_from_tick(tick, cvd_current=cvd_current)
            if self.volume_target > 0 and self.cur.vol >= self.volume_target:
                closed = self._finalize_cur(ts_ms_close=ts_ms)
                if closed is not None:
                    out.append(closed)
        return out
