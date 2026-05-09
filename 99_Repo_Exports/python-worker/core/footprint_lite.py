import math
from collections import OrderedDict
from dataclasses import dataclass, field
from typing import Any

from core.footprint_features import compute_bucket_stats, compute_edge_ladders, compute_poc, poc_on_edge


@dataclass
class FootprintSnapshot:
    """
    Итоговые признаки footprint-lite по одному микро-бару (v2).
    """
    n_buckets: int
    bucket_px: float
    max_imbalance: float
    peak_delta: float
    peak_total: float
    peak_bucket_px: float
    delta_concentration: float
    progress: float
    absorb_score: float
    absorption_bias: str

    # NEW: Round 6 fields + extensibility
    extra: dict[str, Any] = field(default_factory=dict)

    def get(self, key: str, default: Any = None) -> Any:
        return self.extra.get(key, default)


class FootprintLite:
    """
    Footprint-lite: агрегируем тики в price buckets внутри микро-бара.

    bucket_id = round(price / bucket_px)
    bucket хранит buy_qty/sell_qty (в qty базового актива).

    Ограничение памяти:
    - LRU eviction через OrderedDict: если bucket'ов > max_buckets, выкидываем самый старый.
    Это детерминировано (зависит только от последовательности тиков).

    Производительность:
    - update(): O(1) на тик
    - finalize(): O(max_buckets) на bar_close
    """

    def __init__(self, bucket_px: float, max_buckets: int = 200, eps: float = 1e-9) -> None:
        self.bucket_px = float(bucket_px)
        self.max_buckets = int(max_buckets)
        self.eps = float(eps)
        if self.bucket_px <= 0:
            # fail-open: footprint disabled effectively
            self.bucket_px = 0.0
        if self.max_buckets < 16:
            self.max_buckets = 16

        # bucket_id -> (buy_qty, sell_qty)
        self._m: OrderedDict[int, tuple[float, float]] = OrderedDict()
        self.evictions: int = 0
        self.bad_price: int = 0

    def update(self, *, price: float, qty: float, signed_qty: float) -> None:
        if self.bucket_px <= 0:
            return
        if not math.isfinite(price) or price <= 0:
            self.bad_price += 1
            return
        q = float(qty)
        if not math.isfinite(q) or q <= 0:
            return

        bid = int(round(price / self.bucket_px))
        cur = self._m.get(bid)
        if cur is None:
            buy = 0.0
            sell = 0.0
        else:
            buy, sell = cur

        # signed_qty семантика: +qty=агрессивный BUY, -qty=агрессивный SELL
        if signed_qty >= 0:
            buy += q
        else:
            sell += q

        # LRU touch
        self._m[bid] = (buy, sell)
        self._m.move_to_end(bid, last=True)

        if len(self._m) > self.max_buckets:
            self._m.popitem(last=False)
            self.evictions += 1

    def finalize(
        self,
        *,
        bar_open: float,
        bar_close: float,
        bar_high: float,
        bar_low: float,
        bar_delta_sum: float,
        bar_vol: float,
    ) -> FootprintSnapshot:
        m = self._m
        n = len(m)
        if n == 0 or self.bucket_px <= 0:
            return FootprintSnapshot(
                n_buckets=0,
                bucket_px=float(self.bucket_px),
                max_imbalance=0.0,
                peak_delta=0.0,
                peak_total=0.0,
                peak_bucket_px=0.0,
                delta_concentration=0.0,
                progress=0.0,
                absorb_score=0.0,
                absorption_bias="NONE",
                extra={
                    "fp_max_imb_ratio": 1.0,
                    "fp_ladder_low_len": 0,
                    "fp_ladder_high_len": 0,
                    "fp_poc_total": 0.0,
                    "fp_poc_bucket_px": 0.0,
                    "fp_poc_on_edge": 0,
                    "fp_poc_edge_side": "NONE",
                    "fp_eff_delta": 0.0,
                    "fp_move_bp": 0.0,
                    "fp_quote_delta": 0.0,
                    "fp_eff_quote": 0.0,
                    "fp_eff_vol": 0.0,
                }
            )

        max_imb = -1.0
        max_imb_ratio = 1.0
        peak_delta = 0.0
        peak_total = 0.0
        peak_bid = 0

        # Compute per-bucket stats (testable helpers)
        keys, st = compute_bucket_stats(m, self.eps)

        # Legacy peak scan
        for bid in keys:
            buy = st[bid]["buy"]
            sell = st[bid]["sell"]
            total = buy + sell
            delta = buy - sell
            imb = st[bid]["imb_frac"]
            imb_ratio = st[bid]["imb_ratio"]

            if imb > max_imb:
                max_imb = imb
                peak_delta = delta
                peak_total = total
                peak_bid = bid
            if imb_ratio > max_imb_ratio:
                max_imb_ratio = imb_ratio

        if max_imb < 0:
            max_imb = 0.0

        # NEW: edge ladders (absorption walls)
        # Defaults provided via attribute or defaults
        ladder_ratio_th = float(getattr(self, "ladder_ratio_th", 3.0))
        edge_buckets = int(getattr(self, "ladder_edge_buckets", 4))
        ladder_low_len, ladder_high_len = compute_edge_ladders(
            keys, st, ratio_th=ladder_ratio_th, edge_buckets=edge_buckets
        )

        # NEW: POC (max volume)
        poc_bucket, poc_total = compute_poc(keys, st)
        poc_edge_tol = int(getattr(self, "poc_edge_tol_buckets", 1))
        poc_edge, poc_edge_side = poc_on_edge(
            poc_bucket=poc_bucket, keys=keys, edge_tol_buckets=poc_edge_tol
        )

        rng = max(self.eps, float(bar_high) - float(bar_low))
        progress = abs(float(bar_close) - float(bar_open)) / rng  # 0..1 (usually)
        progress = max(0.0, min(1.0, progress))

        # Концентрация: насколько "локальный" кластер доминирует над общим bar_delta
        denom = max(self.eps, abs(float(bar_delta_sum)))
        delta_conc = abs(peak_delta) / denom
        # clamp (иначе при маленьком bar_delta может улетать)
        delta_conc = min(5.0, delta_conc)

        # Эвристика абсорбции:
        # - хотим большой локальный дисбаланс (max_imb)
        # - хотим большой "peak_delta" относительно bar_vol
        # - хотим маленький price progress (low progress)
        vol = max(self.eps, float(bar_vol))
        peak_ratio = abs(peak_delta) / vol  # 0..1+
        absorb_score = (1.0 - progress) * max_imb * (1.0 + 2.0 * peak_ratio) * (1.0 + 0.3 * delta_conc)

        # NEW: portable efficiency: bps move per quote-delta
        mid = 0.5 * (abs(float(bar_open)) + abs(float(bar_close)))
        if mid <= self.eps:
            mid = max(self.eps, abs(float(bar_close)))
        move_bp = 10000.0 * abs(float(bar_close) - float(bar_open)) / mid
        quote_delta = abs(float(bar_delta_sum)) * mid
        eff_quote = move_bp / max(self.eps, quote_delta)

        # optional: bps move per quote-volume
        quote_vol = max(self.eps, float(bar_vol) * mid)
        eff_vol = move_bp / quote_vol

        # Convert bucket IDs to PX proxy
        peak_bucket_px = float(peak_bid) * float(self.bucket_px) if self.bucket_px > 0 else 0.0
        poc_bucket_px = float(poc_bucket) * float(self.bucket_px) if self.bucket_px > 0 else 0.0

        # Bias from stronger ladder
        abs_bias = "NONE"
        if ladder_low_len > ladder_high_len:
            abs_bias = "LONG"
        elif ladder_high_len > ladder_low_len:
            abs_bias = "SHORT"

        # Original logic as fallback for VERY low progress if bias not yet set?
        # Let's keep existing logic but allow ladder bias to override if strong.
        if abs_bias == "NONE" and (1.0 - progress) > 0.5:
            move = float(bar_close) - float(bar_open)
            move_side = "UP" if move > 0 else ("DOWN" if move < 0 else "FLAT")
            if peak_delta < 0 and move_side != "DOWN":
                abs_bias = "LONG"
            elif peak_delta > 0 and move_side != "UP":
                abs_bias = "SHORT"

        snap = FootprintSnapshot(
            n_buckets=int(n),
            bucket_px=float(self.bucket_px),
            max_imbalance=float(max_imb),
            peak_delta=float(peak_delta),
            peak_total=float(peak_total),
            peak_bucket_px=float(peak_bucket_px),
            delta_concentration=float(delta_conc),
            progress=float(progress),
            absorb_score=float(absorb_score),
            absorption_bias=str(abs_bias),
        )
        # Use extra for newer features for robustness/extensibility
        snap.extra.update({
            "fp_max_imb_ratio": float(max_imb_ratio),
            "fp_ladder_low_len": int(ladder_low_len),
            "fp_ladder_high_len": int(ladder_high_len),
            "fp_poc_total": float(poc_total),
            "fp_poc_bucket_px": float(poc_bucket_px),
            "fp_poc_on_edge": int(poc_edge),
            "fp_poc_edge_side": str(poc_edge_side),
            "fp_eff_delta": float(eff_quote),  # Mapping old name to new portable logic
            "fp_move_bp": float(move_bp),
            "fp_quote_delta": float(quote_delta),
            "fp_eff_quote": float(eff_quote),
            "fp_eff_vol": float(eff_vol),
        })
        return snap
