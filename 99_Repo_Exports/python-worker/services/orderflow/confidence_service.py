import hashlib
import logging
import math
from typing import Any, Sequence
from types import SimpleNamespace
from utils.time_utils import get_ny_time_millis
from services.orderflow.metrics import record_confirmation_seen, record_evidence_used
from services.orderflow.utils import session_utc

try:
    from orderflow_services.confidence_cal_metrics import inc_ab_arm, inc_apply, inc_bucket_hit, obs_delta_abs
except Exception:  # pragma: no cover
    inc_apply = None  # type: ignore
    obs_delta_abs = None  # type: ignore
    inc_bucket_hit = None # type: ignore
    inc_ab_arm = None # type: ignore

class ConfidenceService:
    def __init__(self, facade: Any):
        self.facade = facade
        self.conf_scorer = facade.conf_scorer
        self.logger = facade.logger
        self.conf_cal_ab_mode = facade.conf_cal_ab_mode
        self.conf_cal_ab_sticky_key = facade.conf_cal_ab_sticky_key
        self.conf_cal_ab_share = facade.conf_cal_ab_share
        self.conf_cal_ab_shadow = facade.conf_cal_ab_shadow
        self.conf_cal_runtime = facade.conf_cal_runtime
        self.conf_cal_challenger_runtime = facade.conf_cal_challenger_runtime

    def apply_confidence_calibration(self, runtime: Any, indicators: dict[str, Any], conf_raw: float, ctx: dict[str, Any]) -> float:
        """
        Applies calibration using Champion/Challenger bundles with A/B testing and Shadow mode.
        Returns the final calibrated confidence (conf_v1).
        Updated for World Practice A/B + Shadow + Metrics.
        """
        # 1. Prepare Context & Keys
        symbol = runtime.symbol

        # A/B Logic
        ab_mode = self.conf_cal_ab_mode # off, shadow, ab
        use_challenger = False
        in_shadow = False

        # Determine Arm
        arm = "champion"
        h_input = "none"

        # Sticky Hashing
        if ab_mode in ("shadow", "ab"):
            # hash key: symbol|session (default)
            sticky_key_parts = []
            sk_def = self.conf_cal_ab_sticky_key
            if "symbol" in sk_def: sticky_key_parts.append(symbol)
            if "session" in sk_def: sticky_key_parts.append((ctx.get("session", "")))

            h_input = "|".join(sticky_key_parts)
            # deterministic 0..1
            h_val = float(int(hashlib.md5(h_input.encode("utf-8")).hexdigest(), 16) % 10000) / 10000.0

            if h_val < self.conf_cal_ab_share:
                arm = "challenger"

        if ab_mode == 'ab' and arm == 'challenger':
             use_challenger = True

        # Champion Run
        champ_rt = self.conf_cal_runtime
        res_champ = {"result": conf_raw, "method": "identity", "bucket_level": "none"}
        if champ_rt:
            champ_rt.maybe_reload(get_ny_time_millis())
            res_champ = champ_rt.get_calibrated_confidence(conf_raw, ctx)

        # Challenger Run
        chall_rt = self.conf_cal_challenger_runtime
        res_chall = {"result": conf_raw, "method": "identity", "bucket_level": "none"}
        chall_computed = False

        if chall_rt:
             # Load if we strictly need it OR if shadow enabled
             need_challenger = use_challenger or (self.conf_cal_ab_shadow) or (ab_mode == 'shadow')
             if need_challenger:
                 chall_rt.maybe_reload(get_ny_time_millis())
                 res_chall = chall_rt.get_calibrated_confidence(conf_raw, ctx)
                 chall_computed = True

        # Final Decision
        # In AB mode: if use_challenger -> use res_chall
        # In Shadow mode: always use res_champ (but log res_chall)
        if use_challenger and chall_computed:
            final_res = res_chall
            # arm is already "challenger"
            arm_taken = "challenger"
        else:
            final_res = res_champ
            arm_taken = "champion"
            # If we were assigned challenger but didn't have runtime, we fall back to champion
            if arm == "challenger" and not chall_computed:
                indicators["confidence_cal_fallback_to_champion"] = 1

        # Metrics & Indicators
        conf_final = round(float(final_res.get("result", conf_raw)), 6)

        # 2. Metadata
        indicators["confidence_cal_ab_mode"] = ab_mode
        indicators["confidence_cal_p_challenger"] = self.conf_cal_ab_share
        indicators["confidence_cal_sticky_key"] = h_input
        indicators["confidence_cal_bucket"] = -1

        indicators["confidence_cal_arm_assigned"] = arm
        indicators["confidence_cal_arm_taken"] = arm_taken

        indicators["confidence_cal_champion"] = round(float(res_champ.get("result", conf_raw)), 6)
        indicators["confidence_cal_challenger"] = round(float(res_chall.get("result", 0.0)), 6) if chall_computed else 0.0

        indicators["confidence_cal_method"] = final_res.get("method", "identity")
        indicators["confidence_cal_bucket_by"] = final_res.get("bucket_by", "none")
        indicators["confidence_cal_bucket_level"] = final_res.get("bucket_level", "none")
        indicators["confidence_cal_fallback_depth"] = int(final_res.get("fallback_depth", 0) or 0)
        indicators["confidence_cal_schema_version"] = int(final_res.get("schema_version", 0) or 0)

        if chall_computed:
            delta = float(res_chall.get("result", conf_raw)) - float(res_champ.get("result", conf_raw))
            indicators["confidence_cal_shadow_delta"] = round(delta, 6)
            indicators["confidence_cal_shadow_delta_abs"] = round(abs(delta), 6)

        # Prom metrics
        try:
            if inc_bucket_hit:
                inc_bucket_hit(symbol, arm_taken, str(indicators["confidence_cal_bucket_by"]), str(indicators["confidence_cal_bucket_level"]), str(indicators["confidence_cal_method"]))
            if inc_ab_arm:
                inc_ab_arm(symbol, arm_taken)
            if inc_apply:
                inc_apply(symbol, "confidence_v1")
            if obs_delta_abs and chall_computed:
                obs_delta_abs(symbol, "champ_vs_chall", abs(float(indicators.get("confidence_cal_shadow_delta", 0.0))))
        except Exception:
            pass

        # V2: calibrate if present
        try:
            conf_v2_raw = indicators.get("confidence_v2")
            if conf_v2_raw is not None:
                conf_v2_raw = float(conf_v2_raw)
                res2_champ = {"result": max(0.0, min(1.0, conf_v2_raw))}
                res2_chall = {"result": max(0.0, min(1.0, conf_v2_raw))}
                if champ_rt:
                    r2 = champ_rt.get_calibrated_confidence(conf_v2_raw, ctx)
                    if isinstance(r2, dict):
                        res2_champ.update(r2)
                if chall_rt and chall_computed:
                    r2 = chall_rt.get_calibrated_confidence(conf_v2_raw, ctx)
                    if isinstance(r2, dict):
                        res2_chall.update(r2)
                final2 = res2_chall if arm_taken == "challenger" and chall_computed else res2_champ
                indicators["confidence_cal_v2"] = round(max(0.0, min(1.0, final2.get("result", conf_v2_raw))), 6)
        except Exception:
            pass

        return conf_final

    async def compute_confidence(
        self,
        runtime: Any,
        indicators: dict[str, Any],
        confirmations: Sequence[str],
        *,
        side: str,
        kind: str,
        features: list[str] | None = None
    ) -> float:
        def _get(name: str, default=0.0):
            v = indicators.get(name)
            return v if v is not None else default

        ctx = SimpleNamespace(
            z_delta=_get("delta_z", _get("z", 0.0)),
            delta=_get("delta", 0.0),
            obi_avg=_get("obi", 0.0),
            obi_sustained=bool(indicators.get("obi_sustained", False)),
            obi_avg_20=_get("obi_20", 0.0),
            obi_sustained_20=bool(indicators.get("obi_sustained_20", False)),
            microprice_shift_bps_20=_get("microprice_shift_bps_20", 0.0),
            wall_bid=bool(indicators.get("wall_bid", False)),
            wall_ask=bool(indicators.get("wall_ask", False)),
            wall_bid_dist_bps=_get("wall_bid_dist_bps", 0.0),
            wall_ask_dist_bps=_get("wall_ask_dist_bps", 0.0),
            depletion_score=_get("depletion_score", 0.0),
            refill_score=_get("refill_score", 0.0),
            impact_proxy=_get("impact_proxy", 0.0),
            spread_bps=_get("spread_bps", 0.0),
            realized_ema_bps=_get("realized_ema_bps", 0.0),
            adverse_ratio_ema=_get("adverse_ratio_ema", 0.0),
            market_mode=indicators.get("market_mode", "mixed") or "mixed",
            l2_age_ms=_get("l2_age_ms", 0.0),
            l2_is_stale=bool(indicators.get("l2_is_stale", False)),
            taker_buy_rate_ema=_get("taker_buy_rate_ema", 0.0),
            taker_sell_rate_ema=_get("taker_sell_rate_ema", 0.0),
            cancel_to_trade_ask=_get("cancel_to_trade_ask", 0.0),
            cancel_to_trade_bid=_get("cancel_to_trade_bid", 0.0),
            eta_fill_ask_sec=_get("eta_fill_ask_sec", 0.0),
            eta_fill_bid_sec=_get("eta_fill_bid_sec", 0.0),
            weak_progress=bool(indicators.get("weak_progress", False)),
            weak_recent_cnt=int((indicators.get("weak_recent_cnt") if indicators.get("weak_recent_cnt") is not None else indicators.get("weak_recent_count", 0)) or 0),
            weak_recent_window=int(indicators.get("weak_recent_window", 0) or 0),
            obi_stable_secs=float(indicators.get("obi_stable_secs", 0.0) or 0.0),
            obi_stability_score=float(indicators.get("obi_stability_score", 0.0) or 0.0),
            ofi_stable_secs=float(indicators.get("ofi_stable_secs", 0.0) or 0.0),
            ofi_stability_score=float(indicators.get("ofi_stability_score", 0.0) or 0.0),
            liq_score=float(indicators.get("liq_score", 0.0) or 0.0),
            liq_regime=(indicators.get("liq_regime", getattr(runtime, "liq_regime", "normal")) or "normal"),
            fp_edge_absorb=bool(indicators.get("fp_edge_absorb", False)),
            fp_edge_absorb_strength=float((indicators.get("fp_edge_absorb_strength") if indicators.get("fp_edge_absorb_strength") is not None else indicators.get("fp_edge_strength", 0.0)) or 0.0),
            iceberg_refresh=_get("iceberg_refresh", 0.0),
            iceberg_duration=_get("iceberg_duration", 0.0),
            absorption_volume=_get("absorption_volume", 0.0),
            confirmations=list(confirmations or []),
            fp_absorb_min_score=float(runtime.config.get("fp_absorb_min_score", 1.0)),
            fp_absorb_bonus_w=float(runtime.config.get("fp_absorb_bonus_w", 0.06)),
            fp_imb_bonus_w=float(runtime.config.get("fp_imb_bonus_w", 0.03)),
            fp_bonus_cap=float(runtime.config.get("fp_bonus_cap", 0.08)),
        )

        indicators["l3_spread_bps"] = float(_get("l3_spread_bps", _get("spread_bps", 0.0)))
        indicators["l3_microprice_shift_bps_20"] = float(_get("l3_microprice_shift_bps_20", _get("microprice_shift_bps_20", 0.0)))
        indicators["l3_microprice_velocity_bps"] = float(_get("l3_microprice_velocity_bps", 0.0))
        indicators["l3_obi_5"] = float(_get("l3_obi_5", 0.0))
        indicators["l3_obi_20"] = float(_get("l3_obi_20", _get("obi_20", 0.0)))
        indicators["l3_obi_50"] = float(_get("l3_obi_50", 0.0))
        indicators["l3_obi_persistence_score"] = float(_get("l3_obi_persistence_score", 0.0))
        indicators["l3_cancel_to_trade_bid_5s"] = float(_get("l3_cancel_to_trade_bid_5s", _get("cancel_to_trade_bid", 0.0)))
        indicators["l3_cancel_to_trade_ask_5s"] = float(_get("l3_cancel_to_trade_ask_5s", _get("cancel_to_trade_ask", 0.0)))
        indicators["l3_cancel_to_trade_bid_20s"] = float(_get("l3_cancel_to_trade_bid_20s", 0.0))
        indicators["l3_cancel_to_trade_ask_20s"] = float(_get("l3_cancel_to_trade_ask_20s", 0.0))
        indicators["l3_queue_pressure_bid"] = float(_get("l3_queue_pressure_bid", 0.0))
        indicators["l3_queue_pressure_ask"] = float(_get("l3_queue_pressure_ask", 0.0))
        indicators["l3_market_depth_imbalance"] = float(_get("l3_market_depth_imbalance", 0.0))

        try:
            ts_val = indicators.get("ts_event_ms", 0)
            sess_name = session_utc(ts_val)

            for c_str in (confirmations or []):
                if "=" in c_str:
                    ckqr, cval_s = c_str.split("=", 1)
                    ckqr = ckqr.strip()
                    try:
                        cval = float(cval_s.strip())
                    except ValueError:
                        cval = 1.0
                else:
                    ckqr = c_str.strip()
                    cval = 1.0

                if not ckqr:
                    continue

                indicators[f"conf_{ckqr}"] = cval
                record_confirmation_seen(runtime.symbol, c_str)
                record_evidence_used(runtime.symbol, sess_name, c_str)

        except Exception:
            pass

        try:
            conf, parts = await self.conf_scorer.score(kind=kind or "custom", side=side, ctx=ctx)
            indicators["confidence_breakdown"] = {
                "base": round(parts.get("base", 0.0), 4),
                "mult": round(parts.get("mult", 1.0), 4),
                "pen_total": round(parts.get("pen_total", 0.0), 4),
            }
            conf_v1 = round(conf, 4)
            indicators["confidence_v1"] = conf_v1

            try:
                if int(runtime.config.get("confidence_shadow_enable", 0) or 0) == 1:
                    ctx2 = SimpleNamespace(**ctx.__dict__)
                    ctx2.sweep_legacy_fallback = int(runtime.config.get("conf_v2_sweep_legacy_fallback", 1) or 1)
                    ctx2.sweep_simple_strength = float(runtime.config.get("conf_v2_sweep_simple_strength", 0.4) or 0.4)
                    ctx2.rsi_bonus_w = float(runtime.config.get("conf_v2_rsi_bonus_w", 0.06) or 0.06)
                    ctx2.div_bonus_w = float(runtime.config.get("conf_v2_div_bonus_w", 0.07) or 0.07)
                    ctx2.sweep_bonus_w = float(runtime.config.get("conf_v2_sweep_bonus_w", 0.08) or 0.08)

                    conf2, parts2 = await self.conf_scorer.score(kind=kind or "custom", side=side, ctx=ctx2)
                    conf_v2 = round(conf2, 4)
                    if math.isfinite(conf_v2):
                        indicators["confidence_v2"] = conf_v2

                    attach = int(runtime.config.get("confidence_parts_attach_v2", 0) or 0)
                    if attach == 1:
                        indicators["confidence_breakdown_v2"] = {
                            "base": round(parts2.get("base", 0.0), 4),
                            "mult": round(parts2.get("mult", 1.0), 4),
                            "pen_total": round(parts2.get("pen_total", 0.0), 4),
                        }
            except Exception:
                pass

            try:
                ctx_bucket = {
                    "session": indicators.get("session"),
                    "regime": indicators.get("liq_regime"),
                    "symbol": runtime.symbol,
                }
                if not ctx_bucket["regime"]:
                    ctx_bucket["regime"] = str(getattr(runtime, "last_regime", "neutral"))

                conf_cal_v1 = self.apply_confidence_calibration(runtime, indicators, conf_v1, ctx_bucket)
                indicators["confidence_cal"] = conf_cal_v1
                indicators["confidence_cal_v1"] = conf_cal_v1

                indicators["confidence_raw"] = indicators.get("confidence_v1")
                if indicators.get("confidence_v2") is not None:
                    indicators["confidence_raw_v2"] = indicators.get("confidence_v2")

            except Exception as e:
                self.logger.error("Calibration failed: %s", e)
                pass

            return indicators.get("confidence_v1") or conf_v1
        except Exception as exc:
            self.logger.warning("confidence scorer fallback due to error: %s", exc)
            return 0.1
