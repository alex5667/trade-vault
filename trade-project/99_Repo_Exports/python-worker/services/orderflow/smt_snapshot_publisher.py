import json
import logging
from typing import Any

from core.smt_symbol_snapshot import SymbolSnapshot
from core.dyn_cfg_keys import DynCfgKeys as DK
from utils.task_manager import safe_create_task
from utils.time_utils import get_ny_time_millis

logger = logging.getLogger("crypto_smt_snapshot_publisher")

class SMTSnapshotPublisher:
    def __init__(self, facade: Any):
        self.facade = facade
        self.redis = facade.redis
        self.market_state = facade.market_state

    async def publish_smt_snapshot(self, runtime: Any, bar: Any) -> None:
        try:
            now_ts = int(getattr(bar, "end_ts_ms", 0) or 0)
            if now_ts <= 0:
                now_ts = get_ny_time_millis()

            snap_every_ms = int(runtime.config.get("smt_snapshot_every_ms", 1000))
            if snap_every_ms < 250:
                snap_every_ms = 250

            if (now_ts - int(getattr(runtime, "last_snapshot_ts_ms", 0) or 0)) >= snap_every_ms:
                runtime.last_snapshot_ts_ms = now_ts

                # --- Persist MicroBar to PostgreSQL (Redundancy) ---
                try:
                    from services.persistence_manager import get_persistence_manager
                    pm = (getattr(runtime, 'pm', None) or get_persistence_manager())
                    b_dict = {
                        "ts_ms": bar.end_ts_ms,
                        "open": bar.open,
                        "high": bar.high,
                        "low": bar.low,
                        "close": bar.close,
                        "vol": bar.vol,
                        "cvd": bar.cvd_close
                    }
                    safe_create_task(pm.save_microbar(runtime.symbol, b_dict))
                except Exception:
                    pass

                # 1. BOS / Structure Proxy
                close_px = float(getattr(bar, "close", 0.0) or 0.0)
                close_cross = 0
                close_cross_dir = "NONE"
                close_cross_level = 0.0

                if getattr(runtime, "last_swing_high", None):
                    lvl = runtime.last_swing_high.price
                    if lvl > 0 and close_px > lvl:
                        close_cross = 1
                        close_cross_dir = "UP"
                        close_cross_level = lvl

                if getattr(runtime, "last_swing_low", None):
                    lvl = runtime.last_swing_low.price
                    if lvl > 0 and close_px < lvl:
                        close_cross = 1
                        close_cross_dir = "DOWN"
                        close_cross_level = lvl

                # Trend Dir Proxy (Hidden Div > CloseCross > NONE)
                trend_dir = "NONE"
                if getattr(runtime, "last_div", None):
                    k = runtime.last_div.kind
                    if k == "bullish_hidden": trend_dir = "UP"
                    elif k == "bearish_hidden": trend_dir = "DOWN"

                if trend_dir == "NONE" and close_cross_dir in ("UP", "DOWN"):
                    trend_dir = close_cross_dir

                # 2. Strong OF Context
                of_valid_ms = int(runtime.config.get("smt_of_strong_valid_ms", 120000))
                of_strong = 0
                if getattr(runtime, "last_of_strong_ts_ms", 0) > 0:
                     if (now_ts - runtime.last_of_strong_ts_ms) <= of_valid_ms:
                         of_strong = 1
                of_dir = str(getattr(runtime, "last_of_dir", "NONE") or "NONE").upper()

                # 3. Detectors state
                wp = 1 if (getattr(runtime, "last_wp", None) and runtime.last_wp.weak_any) else 0

                reclaim = 0
                reclaim_dir = "NONE"
                reclaim_ts = 0
                if getattr(runtime, "last_reclaim", None):
                    reclaim_ts = runtime.last_reclaim.ts_ms
                    if now_ts - reclaim_ts <= int(runtime.config.get("smt_reclaim_valid_ms", 120000)):
                        reclaim = 1
                        reclaim_dir = runtime.last_reclaim.direction_bias.upper()

                sweep = 0
                sweep_dir = "NONE"
                sweep_ts = 0
                if getattr(runtime, "last_sweep", None):
                    sweep_ts = runtime.last_sweep.ts_ms
                    if now_ts - sweep_ts <= int(runtime.config.get("smt_sweep_valid_ms", 120000)):
                        sweep = 1
                        sweep_dir = runtime.last_sweep.direction_bias.upper()

                obi_stable_sec = 0.0
                if getattr(runtime, "last_obi_event", None):
                     obi_stable_sec = float(runtime.last_obi_event.get("stable_secs", 0.0) or 0.0)

                iceberg_strict = 0
                if getattr(runtime, "last_iceberg_event", None):
                    refresh = int(runtime.last_iceberg_event.get("refresh", 0) or 0)
                    dur = float(runtime.last_iceberg_event.get("duration", 0.0) or 0.0)
                    r_min = int(runtime.config.get("iceberg_strict_refresh_min", 3))
                    d_min = float(runtime.config.get("iceberg_strict_duration_min", 1.5))
                    if refresh >= r_min and dur >= d_min:
                        iceberg_strict = 1

                div_kind = "none"
                div_ts = 0
                if getattr(runtime, "last_div", None):
                    div_kind = runtime.last_div.kind
                    div_ts = runtime.last_div.ts_ms

                # Ranking features
                rsi14 = runtime.rsi_price.value if (hasattr(runtime, "rsi_price") and runtime.rsi_price.value is not None) else 0.0
                cvd_slope = getattr(runtime.cvd_state, "cvd_slope", 0.0) if hasattr(runtime, "cvd_state") else 0.0

                sh0 = runtime.last_swing_high.price if getattr(runtime, "last_swing_high", None) else 0.0
                sh1 = runtime.prev_swing_high.price if getattr(runtime, "prev_swing_high", None) else 0.0
                sl0 = runtime.last_swing_low.price if getattr(runtime, "last_swing_low", None) else 0.0
                sl1 = runtime.prev_swing_low.price if getattr(runtime, "prev_swing_low", None) else 0.0
                tsh0 = runtime.last_swing_high.ts_ms if getattr(runtime, "last_swing_high", None) else 0
                tsh1 = runtime.prev_swing_high.ts_ms if getattr(runtime, "prev_swing_high", None) else 0
                tsl0 = runtime.last_swing_low.ts_ms if getattr(runtime, "last_swing_low", None) else 0
                tsl1 = runtime.prev_swing_low.ts_ms if getattr(runtime, "prev_swing_low", None) else 0

                retrace_atr = 0.0
                if getattr(runtime, "last_retrace", None):
                     retrace_atr = float(getattr(runtime.last_retrace, "depth_atr", 0.0) or 0.0)

                # --- SMT snapshot extra fields ---
                delta_z = (getattr(runtime, "last_delta_z", 0.0) or 0.0)
                delta_eff_norm = (getattr(runtime, "last_delta_eff_norm", 0.0) or 0.0)
                
                # HTF zones
                zone_id = ""
                zone_type = ""
                zone_src = ""
                zone_side = ""
                zone_px_lo = 0.0
                zone_px_hi = 0.0
                zone_ts_ms = 0
                zone_weight = 0.0
                zone_dist_bp = 0.0
                near_zone = 0
                zone_ok = 0

                try:
                    await runtime.maybe_load_htf_zones(now_ts_ms=now_ts, redis_client=self.redis)
                    px = close_px or 0.0
                    pack = getattr(runtime, "zones_pack", None)
                    if pack is not None and px > 0:
                        z, d_bp, inside = pack.nearest(px)
                        if z is not None:
                            zone_id = str(z.id)
                            zone_type = str(z.type)
                            zone_src = str(z.src)
                            zone_side = str(z.side)
                            zone_px_lo = z.px_lo
                            zone_px_hi = z.px_hi
                            zone_ts_ms = z.ts_ms
                            zone_weight = z.weight
                            zone_dist_bp = d_bp
                            near_bp = float(runtime.config.get("smt_near_zone_bp", runtime.config.get("smt_zone_max_bp", 15.0)) or 15.0)
                            ok_bp = float(runtime.config.get("smt_zone_max_bp", 15.0) or 15.0)
                            near_zone = 1 if (inside or (zone_dist_bp > 0 and zone_dist_bp <= near_bp)) else 0
                            zone_ok = 1 if (inside or (zone_dist_bp > 0 and zone_dist_bp <= ok_bp)) else 0
                except Exception:
                    pass

                # Fallback to swing proxy if HTF zones missing
                if zone_ok == 0 and (not zone_id):
                    try:
                        z_level = close_cross_level or 0.0
                        z_px = close_px or 0.0
                        if z_level > 0 and z_px > 0:
                            mid = 0.5 * (abs(z_px) + abs(z_level))
                            zone_dist_bp = (10000.0 * abs(z_px - z_level) / mid) if mid > 0 else 0.0
                        near_bp = float(runtime.config.get("smt_near_zone_bp") or runtime.config.get("smt_zone_max_bp") or 15.0)
                        ok_bp = float(runtime.config.get("smt_zone_max_bp") or 15.0)
                        near_zone = 1 if (zone_dist_bp > 0 and zone_dist_bp <= near_bp) else 0
                        zone_ok = 1 if (near_zone == 1 and int(close_cross or 0) == 1 and zone_dist_bp <= ok_bp) else 0
                        zone_id = "SWING_PROXY"
                        zone_type = "LEVEL"
                        zone_src = "swing"
                        zone_side = "NA"
                        zone_px_lo = z_level
                        zone_px_hi = z_level
                        zone_ts_ms = now_ts
                        zone_weight = 0.1
                    except Exception:
                       pass

                abs_lvl_ok = int(getattr(runtime, "last_abs_lvl_ok", 0) or 0)

                # ADX strength quantile
                adx14 = 0.0
                adx_q = 0.5
                try:
                    adx14 = await self.market_state.get_adx(symbol=runtime.symbol, now_ms=now_ts)
                    rq = await self.market_state.get_regime_quantiles(symbol=runtime.symbol, tf="1m", now_ms=now_ts)
                    if isinstance(rq, dict):
                        p40 = float(rq.get("adx_p40") or 0.0)
                        p60 = float(rq.get("adx_p60") or 0.0)
                        p75 = float(rq.get("adx_p75") or 0.0)
                        if p40 > 0 and p60 > 0 and p75 > 0 and (p40 <= p60 <= p75):
                            from core.regime_quantiles_store import approx_quantile_adx
                            adx_q = approx_quantile_adx(adx14, p40, p60, p75)
                except Exception:
                    pass

                # Data-quality
                spread_bp = float(getattr(runtime, "last_spread_bps", 0.0) or 0.0)
                book_age_ms = 10**9
                try:
                    bts = int(getattr(runtime, "last_book_ts_ms", 0) or 0)
                    if bts > 0:
                        book_age_ms = max(0, now_ts - bts)
                except Exception:
                    pass
                obi_age_ms = 10**9
                try:
                    if getattr(runtime, "last_obi_event", None):
                        ots = int(runtime.last_obi_event.get("ts_ms") or 0)
                        if ots > 0: obi_age_ms = max(0, now_ts - ots)
                except Exception:
                    pass
                iceberg_age_ms = 10**9
                try:
                    if getattr(runtime, "last_iceberg_event", None):
                        its = int(runtime.last_iceberg_event.get("ts_ms") or 0)
                        if its > 0: iceberg_age_ms = max(0, now_ts - its)
                except Exception:
                    pass

                from contexts import MARKET_REGIME_NA, normalize_regime_label
                snap = SymbolSnapshot(
                    symbol=runtime.symbol,
                    ts_ms=now_ts,
                    trend_dir=trend_dir,
                    close_px=close_px,
                    close_cross=close_cross,
                    close_cross_dir=close_cross_dir,
                    close_cross_level=close_cross_level,
                    swing_high_0=sh0,
                    swing_high_1=sh1,
                    swing_low_0=sl0,
                    swing_low_1=sl1,
                    swing_ts_high_0=tsh0,
                    swing_ts_high_1=tsh1,
                    swing_ts_low_0=tsl0,
                    swing_ts_low_1=tsl1,
                    of_strong=of_strong,
                    of_dir=of_dir,
                    of_ts_ms=getattr(runtime, "last_of_strong_ts_ms", 0) or 0,
                    weak_progress=wp,
                    reclaim=reclaim,
                    reclaim_dir=reclaim_dir,
                    reclaim_ts_ms=reclaim_ts,
                    sweep=sweep,
                    sweep_dir=sweep_dir,
                    sweep_ts_ms=sweep_ts,
                    obi_stable_sec=obi_stable_sec,
                    iceberg_strict=iceberg_strict,
                    div_kind=div_kind,
                    div_ts_ms=div_ts,
                    rsi14=rsi14,
                    cvd_slope=cvd_slope,
                    retrace_atr=retrace_atr,
                    delta_z=delta_z,
                    delta_eff_norm=delta_eff_norm,
                    zone_dist_bp=zone_dist_bp,
                    zone_ok=zone_ok,
                    near_zone=near_zone,
                    abs_lvl_ok=abs_lvl_ok,
                    zone_id=zone_id,
                    zone_type=zone_type,
                    zone_src=zone_src,
                    zone_side=zone_side,
                    zone_px_lo=zone_px_lo,
                    zone_px_hi=zone_px_hi,
                    zone_ts_ms=zone_ts_ms,
                    zone_weight=zone_weight,
                    regime=normalize_regime_label(getattr(runtime, "last_regime", MARKET_REGIME_NA)),
                    atr=float(getattr(runtime, "last_atr", 0.0) or 0.0),
                    abs_lvl_ready=int(1 if int(runtime.dynamic_cfg.get(DK.ABS_LVL_CALIB_N, 0) or 0) >= int(runtime.config.get("abs_lvl_calib_min_samples", 300)) else 0),
                    delta_z_window=int(runtime.config.get("delta_window_n", 60) or 60),
                    book_rate_hz=float(getattr(runtime, "book_rate_ema", 0.0) or 0.0),
                    book_age_ms=int(max(0, now_ts - (getattr(runtime, "last_book_ts_ms", 0) or 0))) if (getattr(runtime, "last_book_ts_ms", 0) or 0) > 0 else 10**9,
                    book_rate_ok_min_hz=float(runtime.dynamic_cfg.get(DK.BOOK_RATE_OK_MIN_HZ, runtime.config.get("book_rate_min_hz", 5.0)) or 5.0),
                    book_rate_crit_hz=float(runtime.dynamic_cfg.get(DK.BOOK_RATE_CRIT_HZ, runtime.config.get("book_rate_crit_hz", 2.0)) or 2.0),
                    book_rate_ready=int(runtime.dynamic_cfg.get(DK.BOOK_RATE_READY, 0) or 0),
                    book_rate_src=str(runtime.dynamic_cfg.get(DK.BOOK_RATE_CALIB_SRC, "static") or "static"),
                    book_health_ok=int(getattr(runtime, "last_book_health_ok", 1)),
                    book_health=str(getattr(runtime, "last_book_health", "OK")),
                    abs_lvl_th_unstable=int(runtime.dynamic_cfg.get(DK.ABS_LVL_TH_UNSTABLE, 0) or 0),
                    of_confirm_score=getattr(runtime, "last_of_confirm_score", 0.0) or 0.0,
                    strong_gate_have=getattr(runtime, "last_strong_gate_have", 0) or 0,
                    strong_gate_need=getattr(runtime, "last_strong_gate_need", 0) or 0,
                    strong_gate_scn=getattr(runtime, "last_strong_gate_scn", "") or "",
                    adx_q=adx_q,
                    adx14=adx14,
                    pressure_sps=getattr(runtime, "pressure_sps", 0.0) or 0.0,
                    pressure_hi=getattr(runtime, "pressure_hi", 0) or 0,
                    spread_bp=spread_bp,
                    obi_age_ms=obi_age_ms,
                    iceberg_age_ms=iceberg_age_ms,
                    cooldown_sps=getattr(runtime, "cooldown_hits_ema", 0.0) or 0.0,
                    spread_z=getattr(runtime, "last_spread_z", 0.0) or 0.0,
                )

                ttl_sec = int(runtime.config.get("smt_snapshot_ttl_sec", 30))
                if ttl_sec < 5: ttl_sec = 5

                key = f"smt:snap:{runtime.symbol}"
                safe_create_task(self.redis.set(key, snap.to_json(), ex=ttl_sec))
        except Exception:
            pass
