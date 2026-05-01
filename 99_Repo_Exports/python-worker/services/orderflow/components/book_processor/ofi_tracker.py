import logging
from typing import Dict, Any, Optional
from services.orderflow.runtime import SymbolRuntime, BookSnapshot
from core.dyn_cfg_keys import DynCfgKeys as DK

logger = logging.getLogger("orderflow_ofi_tracker")

class OFITracker:
    @staticmethod
    def update(runtime: SymbolRuntime, snap: BookSnapshot, prev_snap: Optional[BookSnapshot], book_ts_ms: int, book_raw: Dict[str, Any]) -> None:
        """
        Updates OFI (Order Flow Imbalance), Depth (L3-lite), and Resilience trackers.
        """
        try:
             bids = book_raw.get("bids") or []
             asks = book_raw.get("asks") or []
             if bids and asks:
                 bb_px = float(bids[0][0] or 0.0); bb_q = float(bids[0][1] or 0.0)
                 ba_px = float(asks[0][0] or 0.0); ba_q = float(asks[0][1] or 0.0)
                 
                 if bb_px > 0 and ba_px > 0:
                     mid = 0.5 * (bb_px + ba_px)
                     runtime.last_book_mid = float(mid)
                     spr = float(ba_px - bb_px)
                     # Fix: guard against crossed BBO (ask <= bid).
                     # If crossed, we do NOT zero-out last_spread_bps_l2 (that would
                     # trigger spread_missing on next tick).  Instead we keep the last
                     # good value and set a diagnostic flag on runtime.
                     if spr > 0 and mid > 0:
                         runtime.last_spread_bps_l2 = float((spr / mid) * 10_000.0)
                         runtime.last_spread_bps_l2_ts_ms = int(book_ts_ms)  # track freshness
                         runtime.book_crossed = 0
                         # Mark first successful book snapshot (used for cold-start grace period)
                         if not getattr(runtime, "first_book_ts_ms", 0):
                             runtime.first_book_ts_ms = int(book_ts_ms)
                     else:
                         # Crossed or zero spread: annotate but preserve last good value
                         runtime.book_crossed = 1
                     
                     # Depth USD (Top 5)
                     db = 0.0; da = 0.0
                     for lv in bids[:5]:
                         try: db += float(lv[1] or 0.0)
                         except Exception: pass
                     for lv in asks[:5]:
                         try: da += float(lv[1] or 0.0)
                         except Exception: pass
                     runtime.last_depth_bid_5 = float(db)
                     runtime.last_depth_ask_5 = float(da)
                     runtime.last_depth_min_5_usd = float(min(db, da) * mid)

                     # L3-lite book updates (ETA fill, cancel-to-trade proxies)
                     try:
                         if getattr(runtime, "l3_stats", None) is not None:
                             runtime.l3_stats.on_book(book_ts_ms, depth_bid_5=float(db), depth_ask_5=float(da))
                     except Exception:
                         pass

                     # Resilience tracker: depth replenishment after sweep (use USD notional)
                     try:
                         if getattr(runtime, "resilience", None) is not None:
                             runtime.resilience.on_book(
                                 book_ts_ms,
                                 bid_depth_usd=float(db) * float(mid),
                                 ask_depth_usd=float(da) * float(mid),
                             )
                     except Exception:
                         pass

                     # ------------------------------------------------------------------
                     # Resilience / Replenishment proxy (book_resilience secondary tracker)
                     # ------------------------------------------------------------------
                     try:
                         br = getattr(runtime, "book_resilience", None)
                         if br is not None and book_ts_ms > 0:
                             depth_min_usd = float(runtime.last_depth_min_5_usd)
                             if hasattr(br, "on_book"):
                                 br.on_book(int(book_ts_ms), depth_now_usd=depth_min_usd)
                             elif hasattr(br, "on_depth"):
                                 br.on_depth(ts_ms=int(book_ts_ms), depth_min_usd=depth_min_usd)
                             elif hasattr(br, "update"):
                                 br.update(ts_ms=int(book_ts_ms), depth_min_usd=depth_min_usd)

                             st = None
                             try:
                                 if hasattr(br, "snapshot"):
                                     st = br.snapshot()
                                 elif hasattr(br, "state"):
                                     st = br.state()
                             except Exception:
                                 st = None

                             if isinstance(st, dict):
                                 runtime.dynamic_cfg[DK.RES_ACTIVE]     = int(st.get("res_active",     st.get("active",     0)) or 0)
                                 runtime.dynamic_cfg[DK.RES_RECOVERED]   = int(st.get("res_recovered",  st.get("recovered",  0)) or 0)
                                 runtime.dynamic_cfg[DK.RES_RECOVERY_MS] = int(st.get("res_recovery_ms", st.get("t_recover_ms", 0)) or 0)
                                 runtime.dynamic_cfg[DK.RES_MIN_RATIO]   = float(st.get("res_min_ratio", st.get("depth_ratio", 0.0)) or 0.0)
                                 runtime.dynamic_cfg[DK.RES_CURR_RATIO]  = float(st.get("res_curr_ratio", st.get("curr_ratio", 0.0)) or 0.0)
                                 runtime.dynamic_cfg[DK.RES_SPEED_PER_S] = float(st.get("res_speed_per_s", st.get("speed", 0.0)) or 0.0)
                             else:
                                 runtime.dynamic_cfg[DK.RES_ACTIVE]     = int(getattr(br, "res_active",     getattr(br, "active",     0)) or 0)
                                 runtime.dynamic_cfg[DK.RES_RECOVERED]   = int(getattr(br, "res_recovered",  getattr(br, "recovered",  0)) or 0)
                                 runtime.dynamic_cfg[DK.RES_RECOVERY_MS] = int(getattr(br, "res_recovery_ms", getattr(br, "t_recover_ms", 0)) or 0)
                                 runtime.dynamic_cfg[DK.RES_MIN_RATIO]   = float(getattr(br, "res_min_ratio", getattr(br, "depth_ratio", 0.0)) or 0.0)
                                 runtime.dynamic_cfg[DK.RES_CURR_RATIO]  = float(getattr(br, "res_curr_ratio", getattr(br, "curr_ratio", 0.0)) or 0.0)
                                 runtime.dynamic_cfg[DK.RES_SPEED_PER_S] = float(getattr(br, "res_speed_per_s", getattr(br, "speed", 0.0)) or 0.0)
                     except Exception:
                         pass

             # OFI calculation requires prev snap
             if prev_snap and prev_snap.bids and prev_snap.asks and snap.bids and snap.asks:
                 prev_b_px = float(prev_snap.bids[0][0]); prev_b_q = float(prev_snap.bids[0][1])
                 prev_a_px = float(prev_snap.asks[0][0]); prev_a_q = float(prev_snap.asks[0][1])
                 curr_b_px = float(snap.bids[0][0]); curr_b_q = float(snap.bids[0][1])
                 curr_a_px = float(snap.asks[0][0]); curr_a_q = float(snap.asks[0][1])

                 delta_bid = 0.0
                 if curr_b_px > prev_b_px: delta_bid = curr_b_q
                 elif curr_b_px == prev_b_px: delta_bid = curr_b_q - prev_b_q

                 delta_ask = 0.0
                 if curr_a_px < prev_a_px: delta_ask = curr_a_q
                 elif curr_a_px == prev_a_px: delta_ask = curr_a_q - prev_a_q

                 # Store OFI (signed, positive = buying pressure)
                 runtime.last_ofi = delta_bid - delta_ask
                 
                 # P1 OFI EMA track for better stability
                 ofi_alpha = float(runtime.config.get("ofi_ema_alpha", 0.1))
                 if getattr(runtime, "last_ofi_ema", None) is None:
                     runtime.last_ofi_ema = runtime.last_ofi
                 else:
                     runtime.last_ofi_ema = ofi_alpha * runtime.last_ofi + (1 - ofi_alpha) * runtime.last_ofi_ema

             # OFI Update
             try:
                 ev = runtime.ofi_tracker.update(
                     ts_ms=book_ts_ms,
                     bid_px=bb_px, bid_qty=bb_q,
                     ask_px=ba_px, ask_qty=ba_q,
                 )
                 if ev is not None:
                     # Reclaim Bonus Logic
                     try:
                         bias = str(getattr(ev, "direction_bias", "") or "").upper()
                         if runtime.last_sweep_ts_ms > 0 and bias in ("LONG", "SHORT"):
                             pass
                     except Exception:
                         pass
                     
                     runtime.last_ofi_event = {
                         "ts_ms": _safe_int(ev.ts_ms),
                         "direction": str(ev.direction),
                         "ofi": float(ev.ofi),
                         "ofi_usd": float(ev.ofi_usd),
                         "ofi_z": float(ev.ofi_z),
                         "stable_secs": float(ev.stable_secs),
                         "stability_score": float(ev.stability_score),
                     }
             except Exception:
                 pass
             
             # Best Level OFI (if prev_snap)
             if prev_snap is not None:
                 try:
                     ofi_raw = runtime.ofi_tracker.compute_ofi_best_level(
                         prev_bid_px=float(prev_snap.best_bid_px),
                         prev_bid_qty=float(prev_snap.best_bid_qty),
                         prev_ask_px=float(prev_snap.best_ask_px),
                         prev_ask_qty=float(prev_snap.best_ask_qty),
                         bid_px=float(snap.best_bid_px),
                         bid_qty=float(snap.best_bid_qty),
                         ask_px=float(snap.best_ask_px),
                         ask_qty=float(snap.best_ask_qty),
                     )
                     depth_qty = float(min(snap.depth_5_bid_vol, snap.depth_5_ask_vol))
                     ofi_z, stable_secs, score = runtime.ofi_tracker.update(
                         ts_ms=_safe_int(book_ts_ms),
                         ofi=float(ofi_raw),
                         depth_qty=depth_qty,
                         deadband_abs=float(runtime.config.get("ofi_deadband_abs", 0.0) or 0.0),
                         deadband_frac_depth=float(runtime.config.get("ofi_deadband_frac_depth", 0.02) or 0.02),
                         z_full=float(runtime.config.get("ofi_z_full", 3.0) or 3.0),
                     )
                     is_stable = bool(stable_secs >= 1.0 and score >= 0.8)
                     
                     from services.orderflow.metrics_events import OFIEvent # I'll just use a dict directly if I can't import
                     ev_ofi = {
                         "ts_ms": _safe_int(book_ts_ms),
                         "ofi": float(ofi_raw),
                         "ofi_z": float(ofi_z),
                         "stable_secs": float(stable_secs),
                         "stability_score": float(score),
                         "stable": _safe_int(is_stable),
                     }
                     runtime.last_ofi_event = ev_ofi
                 except Exception:
                     pass

        except Exception as exc:
            from services.orderflow.metrics import log_silent_error
            log_silent_error(exc, "ofi_failure", runtime.symbol, "OFITracker:update")
