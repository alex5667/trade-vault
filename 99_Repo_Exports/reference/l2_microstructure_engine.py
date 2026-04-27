# l2_microstructure_engine.py
from __future__ import annotations
from dataclasses import dataclass
from collections import deque
from typing import Deque, Optional, Tuple

# берите реальные классы из contexts.py
from contexts import SimpleL2Snapshot, BucketState, L2Level


@dataclass(slots=True)
class WallObs:
    ts_ms: int
    found: bool
    price: float
    size: float


class L2MicrostructureEngine:
    def __init__(self, config, specs=None, gpu_processor=None):
        self.config = config
        self.specs = specs
        self.gpu_processor = gpu_processor

        m = int(getattr(config, "wall_hist_m", 5))
        self._bid_hist: Deque[WallObs] = deque(maxlen=m)
        self._ask_hist: Deque[WallObs] = deque(maxlen=m)

        # можно хранить "последнюю подтвержденную" стену
        self._last_bid_wall: Optional[WallObs] = None
        self._last_ask_wall: Optional[WallObs] = None

        # OBI processing state
        maxlen = int(getattr(config, "obi_samples_maxlen", 200))
        maxlen20 = int(getattr(config, "obi20_samples_maxlen", 200))
        self._obi_samples = deque(maxlen=maxlen)
        self._obi20_samples = deque(maxlen=maxlen20)
        self._obi_ema = 0.0
        self._obi20_ema = 0.0

    # ---------------------------
    # helpers (no duplicates)
    # ---------------------------
    @staticmethod
    def _clip_obi(x: float) -> float:
        if x > 1.0:
            return 1.0
        if x < -1.0:
            return -1.0
        return float(x)

    @staticmethod
    def _obi_from_depth(bid_depth: float, ask_depth: float) -> float:
        denom = bid_depth + ask_depth
        if denom <= 0.0:
            return 0.0
        return L2MicrostructureEngine._clip_obi((bid_depth - ask_depth) / denom)

    @staticmethod
    def _ema_update(prev: float, x: float, alpha: float) -> float:
        # alpha in (0..1)
        a = float(alpha)
        if a <= 0.0:
            return float(prev)
        if a >= 1.0:
            return float(x)
        return float(a * x + (1.0 - a) * prev)

    @staticmethod
    def _band_limits(mid: float, half_band_bps: float) -> Tuple[float, float]:
        # [mid - band, mid + band] in price; half_band_bps is half-width around mid
        if mid <= 0.0:
            return 0.0, 0.0
        k = float(half_band_bps) / 1e4
        return float(mid * (1.0 - k)), float(mid * (1.0 + k))

    def _sustained_tail_abs(self, dq, thr: float, k: int) -> bool:
        # dq is deque[float]
        if k <= 0 or len(dq) < k:
            return False
        t = float(thr)
        for i in range(1, k + 1):
            if abs(float(dq[-i])) < t:
                return False
        return True

    # ---------------------------
    # band depth (early break)
    # ---------------------------
    def _band_depth(
        self,
        bids, asks,
        mid: float,
        half_band_bps: float,
    ) -> Tuple[float, float, int, int]:
        """
        bids: отсортированы по цене убыванию
        asks: по возрастанию
        half_band_bps: "полуширина" band вокруг mid в bps
        """
        if mid <= 0.0:
            return 0.0, 0.0, 0, 0

        low_px, high_px = self._band_limits(mid, half_band_bps)
        bid_sum = 0.0
        ask_sum = 0.0
        n_bid = 0
        n_ask = 0

        # bids: desc; include prices >= low_px (early break)
        for lvl in bids:
            p = float(lvl.price)
            if p < low_px:
                break
            sz = float(lvl.size)
            if sz > 0.0:
                bid_sum += sz
                n_bid += 1

        # asks: asc; include prices <= high_px (early break)
        for lvl in asks:
            p = float(lvl.price)
            if p > high_px:
                break
            sz = float(lvl.size)
            if sz > 0.0:
                ask_sum += sz
                n_ask += 1

        return float(bid_sum), float(ask_sum), int(n_bid), int(n_ask)

    def _select_half_band_bps(self, spread_bps: float, which: str) -> float:
        """
        which: "5" или "20"
        mode:
          - "bps": фиксированные half-band bps
          - "spread": half-band = k * spread_bps (clamp)
        """
        mode = str(getattr(self.config, "obi_band_mode", "spread")).lower()
        if mode == "bps":
            if which == "5":
                return float(getattr(self.config, "obi_band_5_bps", 10.0))
            return float(getattr(self.config, "obi_band_20_bps", 20.0))

        # spread-mult mode
        if which == "5":
            k = float(getattr(self.config, "obi_band_k_spread_5", 3.0))
        else:
            k = float(getattr(self.config, "obi_band_k_spread_20", 10.0))

        min_bps = float(getattr(self.config, "obi_band_min_bps", 5.0))
        max_bps = float(getattr(self.config, "obi_band_max_bps", 200.0))
        hb = float(spread_bps) * float(k)
        if hb < min_bps:
            hb = min_bps
        if hb > max_bps:
            hb = max_bps
        return float(hb)

    @staticmethod
    def _bps_dist(price: float, ref: float) -> float:
        if ref <= 0.0:
            return 1e18
        return abs(price - ref) / ref * 1e4

    def _is_same_level(self, p: float, ref_p: float) -> bool:
        tol = float(getattr(self.config, "wall_price_tol_bps", 2.0))
        return self._bps_dist(p, ref_p) <= tol

    def _persist_ratio(self, hist: Deque[WallObs], cur: WallObs) -> float:
        # сколько раз в истории встречалась стенка "примерно на том же уровне"
        if not hist:
            return 0.0
        same = 0
        total = len(hist)
        for obs in hist:
            if obs.found and cur.found and self._is_same_level(obs.price, cur.price):
                same += 1
        return same / total if total > 0 else 0.0

    def _is_persistent(self, ratio: float) -> bool:
        m = int(getattr(self.config, "wall_hist_m", 5))
        p = int(getattr(self.config, "wall_persist_p", 3))
        # ratio >= p/m
        return ratio >= (p / max(m, 1))

    def _drop_ratio(self, prev: Optional[WallObs], cur: WallObs) -> float:
        """
        drop_ratio ~ cur.size/prev.size для той же стены.
        Если prev была, а cur исчезла -> 0.0
        Если prev нет -> 1.0 (нет базы)
        """
        if prev is None or not prev.found:
            return 1.0
        if not cur.found:
            return 0.0
        # если уровни разные — не сравниваем
        if not self._is_same_level(cur.price, prev.price):
            return 1.0
        if prev.size <= 0.0:
            return 1.0
        return float(cur.size / prev.size)

    def _suspicious(self, cur: WallObs, persist_ratio: float, drop_ratio: float) -> bool:
        drop_min = float(getattr(self.config, "wall_drop_ratio_min", 0.35))
        # подозрительно если:
        # 1) стенка появилась сейчас, но не имеет persistence
        # 2) стенка резко схлопнулась
        if cur.found and not self._is_persistent(persist_ratio):
            return True
        if cur.found and drop_ratio < drop_min:
            return True
        # доп: была стенка и резко исчезла — тоже подозрительно (спуф-пул)
        if (not cur.found) and drop_ratio == 0.0:
            return True
        return False

    # --- WALL DETECT (ваша функция, но возвращаем price/size) ---
    # --- WALL DETECT (оптимизировано без list-аллокаций) ---
    def _detect_wall_near_mid(
        self,
        levels: list[L2Level],
        mid: float,
        side: str,
        near_bps: float,
        mult_vs_avg: float,
    ) -> Tuple[bool, float, float, float]:
        """
        Возвращает: found, wall_price, wall_size, dist_bps
        Логика: внутри near-band находим уровень, size которого >= mult_vs_avg * среднее_по_остальным_уровням
        """
        if mid <= 0.0 or near_bps <= 0.0:
            return False, 0.0, 0.0, 0.0

        low = mid * (1.0 - near_bps / 1e4)
        high = mid * (1.0 + near_bps / 1e4)

        # 1) одним проходом собираем count и sum sizes внутри band (без списка)
        cnt = 0
        sum_sz = 0.0
        if side == "bid":
            # bids DESC
            for lvl in levels:
                p = float(lvl.price)
                if p < low:
                    break
                if p > high:
                    continue
                sz = float(lvl.size)
                if sz > 0.0:
                    cnt += 1
                    sum_sz += sz
        else:
            # asks ASC
            for lvl in levels:
                p = float(lvl.price)
                if p > high:
                    break
                if p < low:
                    continue
                sz = float(lvl.size)
                if sz > 0.0:
                    cnt += 1
                    sum_sz += sz

        if cnt < 2:
            return False, 0.0, 0.0, 0.0

        # 2) второй проход: ищем candidate >= mult * avg(others)
        mv = float(mult_vs_avg)
        if side == "bid":
            for lvl in levels:
                p = float(lvl.price)
                if p < low:
                    break
                if p > high:
                    continue
                sz = float(lvl.size)
                if sz <= 0.0:
                    continue
                avg_others = (sum_sz - sz) / max(cnt - 1, 1)
                if avg_others > 0.0 and sz >= (mv * avg_others):
                    dist = (mid - p) / mid * 1e4
                    return True, float(p), float(sz), float(dist)
        else:
            for lvl in levels:
                p = float(lvl.price)
                if p > high:
                    break
                if p < low:
                    continue
                sz = float(lvl.size)
                if sz <= 0.0:
                    continue
                avg_others = (sum_sz - sz) / max(cnt - 1, 1)
                if avg_others > 0.0 and sz >= (mv * avg_others):
                    dist = (p - mid) / mid * 1e4
                    return True, float(p), float(sz), float(dist)

        return False, 0.0, 0.0, 0.0

    # --- main update ---
    def update(self, snap: SimpleL2Snapshot, ts_ms: int, st: BucketState) -> None:
        bids = snap.bids
        asks = snap.asks
        if not bids or not asks:
            return

        # best bid/ask (assume sorted; parser should sort)
        best_bid = bids[0].price
        best_ask = asks[0].price

        if best_bid <= 0.0 or best_ask <= 0.0 or best_ask < best_bid:
            # inverted/invalid
            st.obi_valid = False
            st.obi_20_valid = False
            return

        mid = 0.5 * (best_bid + best_ask)
        spread = best_ask - best_bid
        spread_bps = (spread / mid * 1e4) if mid > 0.0 else 0.0

        # --- band depths (5/20) ---
        hb5 = self._select_half_band_bps(spread_bps, "5")
        hb20 = self._select_half_band_bps(spread_bps, "20")

        bid5, ask5, n_b5, n_a5 = self._band_depth(bids, asks, mid, hb5)
        bid20, ask20, n_b20, n_a20 = self._band_depth(bids, asks, mid, hb20)

        # Calculate band limits for slope calculations
        low20, high20 = self._band_limits(mid, hb20)

        # --- validity rules ("band пустой") ---
        min_lv = int(getattr(self.config, "obi_min_levels_each_side", 2))
        min_depth = float(getattr(self.config, "obi_min_total_depth", 0.0))
        min_depth20 = float(getattr(self.config, "obi_min_total_depth_20", min_depth))

        valid5 = (
            (n_b5 >= min_lv) and (n_a5 >= min_lv) and
            ((bid5 + ask5) >= min_depth)
        )
        valid20 = (
            (n_b20 >= min_lv) and (n_a20 >= min_lv) and
            ((bid20 + ask20) >= min_depth20)
        )

        # OBI raw
        obi = self._obi_from_depth(bid5, ask5) if valid5 else 0.0
        obi20 = self._obi_from_depth(bid20, ask20) if valid20 else 0.0

        # EMA smoothing (update only if valid)
        alpha = float(getattr(self.config, "obi_ema_alpha", 0.20))
        if valid5:
            self._obi_ema = self._ema_update(self._obi_ema, obi, alpha)
        if valid20:
            self._obi20_ema = self._ema_update(self._obi20_ema, obi20, alpha)

        # sustained on raw samples: if invalid -> append 0 to break sustained
        thr = float(getattr(self.config, "obi_thr", 0.10))
        k_s5 = int(getattr(self.config, "obi_sustain_k5", 5))
        k_s20 = int(getattr(self.config, "obi_sustain_k20", 5))

        self._obi_samples.append(obi if valid5 else 0.0)
        self._obi20_samples.append(obi20 if valid20 else 0.0)

        obi_sust = self._sustained_tail_abs(self._obi_samples, thr, k_s5)
        obi20_sust = self._sustained_tail_abs(self._obi20_samples, thr, k_s20)

        # microprice shift (top1, cheap, useful as contradiction filter)
        bid_sz1 = float(bids[0].size) if bids else 0.0
        ask_sz1 = float(asks[0].size) if asks else 0.0
        denom = bid_sz1 + ask_sz1
        microprice = (best_ask * bid_sz1 + best_bid * ask_sz1) / denom if denom > 0 else mid
        micro_shift_bps = ((microprice - mid) / mid * 1e4) if mid > 0 else 0.0

        # slope metrics (простая форма: depth per price-range внутри band20)
        # Чем больше ликвидности ближе к mid, тем "круче" slope.
        # Используем среднюю дистанцию до mid, взвешенную размером.
        def _avg_dist_bps_bid(bids, low_px):
            w = 0.0
            ws = 0.0
            for lvl in bids:
                p = lvl.price
                if p < low_px:
                    break
                sz = lvl.size
                if sz <= 0.0:
                    continue
                dist_bps = ((mid - p) / mid * 1e4) if mid > 0.0 else 0.0
                w += sz * dist_bps
                ws += sz
            return (w / ws) if ws > 0.0 else 0.0

        def _avg_dist_bps_ask(asks, high_px):
            w = 0.0
            ws = 0.0
            for lvl in asks:
                p = lvl.price
                if p > high_px:
                    break
                sz = lvl.size
                if sz <= 0.0:
                    continue
                dist_bps = ((p - mid) / mid * 1e4) if mid > 0.0 else 0.0
                w += sz * dist_bps
                ws += sz
            return (w / ws) if ws > 0.0 else 0.0

        avg_bid_dist = _avg_dist_bps_bid(bids, low20)
        avg_ask_dist = _avg_dist_bps_ask(asks, high20)

        eps = 1e-6
        slope_bid_20 = 1.0 / (avg_bid_dist + eps) if avg_bid_dist > 0.0 else 0.0
        slope_ask_20 = 1.0 / (avg_ask_dist + eps) if avg_ask_dist > 0.0 else 0.0

        # --- wall anti-spoof (confirmed vs raw) ---
        near_bps = float(getattr(self.config, "wall_near_bps", 10.0))
        mult = float(getattr(self.config, "wall_mult_vs_avg", 4.0))

        bid_found, bid_p, bid_s, bid_dist = self._detect_wall_near_mid(bids, mid, "bid", near_bps, mult)
        ask_found, ask_p, ask_s, ask_dist = self._detect_wall_near_mid(asks, mid, "ask", near_bps, mult)

        cur_bid = WallObs(ts_ms=ts_ms, found=bid_found, price=bid_p, size=bid_s)
        cur_ask = WallObs(ts_ms=ts_ms, found=ask_found, price=ask_p, size=ask_s)

        bid_ratio = self._persist_ratio(self._bid_hist, cur_bid)
        ask_ratio = self._persist_ratio(self._ask_hist, cur_ask)

        bid_drop = self._drop_ratio(self._last_bid_wall, cur_bid)
        ask_drop = self._drop_ratio(self._last_ask_wall, cur_ask)

        bid_susp = self._suspicious(cur_bid, bid_ratio, bid_drop)
        ask_susp = self._suspicious(cur_ask, ask_ratio, ask_drop)

        self._bid_hist.append(cur_bid)
        self._ask_hist.append(cur_ask)

        bid_confirmed = bool(cur_bid.found and self._is_persistent(bid_ratio) and not bid_susp)
        ask_confirmed = bool(cur_ask.found and self._is_persistent(ask_ratio) and not ask_susp)

        if bid_confirmed:
            self._last_bid_wall = cur_bid
        if ask_confirmed:
            self._last_ask_wall = cur_ask

        # OBI raw
        obi = self._obi_from_depth(bid5, ask5) if valid5 else 0.0
        obi20 = self._obi_from_depth(bid20, ask20) if valid20 else 0.0

        # EMA smoothing (update only if valid)
        alpha = float(getattr(self.config, "obi_ema_alpha", 0.20))
        if not hasattr(self, "_obi_ema"):
            self._obi_ema = 0.0
        if not hasattr(self, "_obi20_ema"):
            self._obi20_ema = 0.0

        if valid5:
            self._obi_ema = self._ema_update(self._obi_ema, obi, alpha)
        # else: keep EMA as-is (do not contaminate)

        if valid20:
            self._obi20_ema = self._ema_update(self._obi20_ema, obi20, alpha)

        # sustained on raw samples: if invalid -> append 0 to break sustained
        thr = float(getattr(self.config, "obi_thr", 0.10))
        k_s5 = int(getattr(self.config, "obi_sustain_k5", 5))
        k_s20 = int(getattr(self.config, "obi_sustain_k20", 5))

        if not hasattr(self, "_obi_samples"):
            from collections import deque
            self._obi_samples = deque(maxlen=200)
        if not hasattr(self, "_obi20_samples"):
            from collections import deque
            self._obi20_samples = deque(maxlen=200)

        self._obi_samples.append(obi if valid5 else 0.0)
        self._obi20_samples.append(obi20 if valid20 else 0.0)

        obi_sust = self._sustained_tail_abs(self._obi_samples, thr, k_s5)
        obi20_sust = self._sustained_tail_abs(self._obi20_samples, thr, k_s20)

        # microprice shift (top1, cheap, useful as contradiction filter)
        bid_sz1 = float(bids[0].size) if bids else 0.0
        ask_sz1 = float(asks[0].size) if asks else 0.0
        denom = bid_sz1 + ask_sz1
        if denom > 0.0:
            microprice = (best_ask * bid_sz1 + best_bid * ask_sz1) / denom
            micro_shift_bps = ((microprice - mid) / mid * 1e4) if mid > 0.0 else 0.0
        else:
            microprice = mid
            micro_shift_bps = 0.0

        # slope metrics (простая стабильная версия: liquidity closeness)
        # Чем больше ликвидности ближе к mid, тем "круче" slope.
        # Используем среднюю дистанцию до mid, взвешенную размером.
        def _avg_dist_bps_bid(bids, low_px):
            w = 0.0
            ws = 0.0
            for lvl in bids:
                p = lvl.price
                if p < low_px:
                    break
                sz = lvl.size
                if sz <= 0.0:
                    continue
                dist_bps = ((mid - p) / mid * 1e4) if mid > 0.0 else 0.0
                w += sz * dist_bps
                ws += sz
            return (w / ws) if ws > 0.0 else 0.0

        def _avg_dist_bps_ask(asks, high_px):
            w = 0.0
            ws = 0.0
            for lvl in asks:
                p = lvl.price
                if p > high_px:
                    break
                sz = lvl.size
                if sz <= 0.0:
                    continue
                dist_bps = ((p - mid) / mid * 1e4) if mid > 0.0 else 0.0
                w += sz * dist_bps
                ws += sz
            return (w / ws) if ws > 0.0 else 0.0

        avg_bid_dist = _avg_dist_bps_bid(bids, low20)
        avg_ask_dist = _avg_dist_bps_ask(asks, high20)

        eps = 1e-6
        slope_bid_20 = 1.0 / (avg_bid_dist + eps) if avg_bid_dist > 0.0 else 0.0
        slope_ask_20 = 1.0 / (avg_ask_dist + eps) if avg_ask_dist > 0.0 else 0.0

        # --- wall anti-spoof (ваш код, но с важной правкой "confirmed vs raw") ---
        near_bps = float(getattr(self.config, "wall_near_bps", 10.0))
        mult = float(getattr(self.config, "wall_mult_vs_avg", 4.0))

        bid_found, bid_p, bid_s, bid_dist = self._detect_wall_near_mid(bids, mid, "bid", near_bps, mult)
        ask_found, ask_p, ask_s, ask_dist = self._detect_wall_near_mid(asks, mid, "ask", near_bps, mult)

        cur_bid = WallObs(ts_ms=ts_ms, found=bid_found, price=bid_p, size=bid_s)
        cur_ask = WallObs(ts_ms=ts_ms, found=ask_found, price=ask_p, size=ask_s)

        bid_ratio = self._persist_ratio(self._bid_hist, cur_bid)
        ask_ratio = self._persist_ratio(self._ask_hist, cur_ask)

        bid_drop = self._drop_ratio(self._last_bid_wall, cur_bid)
        ask_drop = self._drop_ratio(self._last_ask_wall, cur_ask)

        bid_susp = self._suspicious(cur_bid, bid_ratio, bid_drop)
        ask_susp = self._suspicious(cur_ask, ask_ratio, ask_drop)

        self._bid_hist.append(cur_bid)
        self._ask_hist.append(cur_ask)

        bid_confirmed = bool(cur_bid.found and self._is_persistent(bid_ratio) and not bid_susp)
        ask_confirmed = bool(cur_ask.found and self._is_persistent(ask_ratio) and not ask_susp)

        if bid_confirmed:
            self._last_bid_wall = cur_bid
        if ask_confirmed:
            self._last_ask_wall = cur_ask

        # --- write to BucketState (Единственный источник правды по L2) ---
        st.l2_ts = int(ts_ms)
        st.best_bid = float(best_bid)
        st.best_ask = float(best_ask)
        st.mid = float(mid)
        st.spread = float(spread)

        st.depth_bid_5 = float(bid5)
        st.depth_ask_5 = float(ask5)
        st.depth_bid_20 = float(bid20)
        st.depth_ask_20 = float(ask20)

        st.obi_valid = bool(valid5)
        st.obi_20_valid = bool(valid20)

        st.obi = float(obi)
        st.obi_avg = float(self._obi_ema)
        st.obi_sustained = bool(obi_sust)

        st.obi_20 = float(obi20)
        st.obi_avg_20 = float(self._obi20_ema)
        st.obi_sustained_20 = bool(obi20_sust)

        st.microprice = float(microprice)
        st.microprice_shift_bps_20 = float(micro_shift_bps)

        st.slope_bid_20 = float(slope_bid_20)
        st.slope_ask_20 = float(slope_ask_20)

        # стенки: wall_* теперь = confirmed (для скоринга)
        st.wall_bid = bool(bid_confirmed)
        st.wall_ask = bool(ask_confirmed)

        st.wall_bid_dist_bps = float(bid_dist)
        st.wall_ask_dist_bps = float(ask_dist)

        st.wall_bid_persist_ratio = float(bid_ratio)
        st.wall_ask_persist_ratio = float(ask_ratio)

        st.wall_bid_drop_ratio = float(bid_drop)
        st.wall_ask_drop_ratio = float(ask_drop)

        st.wall_bid_suspicious = bool(bid_susp)
        st.wall_ask_suspicious = bool(ask_susp)

        st.wall_bid_price = float(bid_p)
        st.wall_bid_size = float(bid_s)
        st.wall_ask_price = float(ask_p)
        st.wall_ask_size = float(ask_s)

        # --- GPU Offloading (Shadow/Active Mode) ---
        if self.gpu_processor and bool(getattr(self.config, "gpu_offload_enabled", False)):
            try:
                # Convert L2Level objects to tuples (price, size)
                # Note: list comprehension is fast, but for very large books this is part of overhead
                bids_t = [(l.price, l.size) for l in bids]
                asks_t = [(l.price, l.size) for l in asks]
                
                # Execute on GPU
                gpu_res = self.gpu_processor.process_l2_snapshot(bids_t, asks_t)
                
                # If active mode, potentially override or store extra metrics
                # For now, we mainly validate the pipeline.
                # Example: override microprice if GPU is authoritative
                # st.microprice = gpu_res.get('microprice', st.microprice)
                
            except Exception:
                # Fail-open: GPU failure shouldn't kill the tick
                pass
