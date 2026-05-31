    def _handle_tick(self, runtime: SymbolRuntime, tick: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        # Initialize variables that may not be set if exceptions occur
        ofc = None
        dec = None

        # Быстрый ранний выход: некорректный тик
        if not tick or not isinstance(tick, dict):
            return None
        runtime.tick_count += 1
        runtime.heartbeat_counter += 1
        # Нормализуем qty/volume, чтобы downstream не падал
        if "qty" not in tick and "volume" in tick:
            tick["qty"] = tick.get("volume")
        if tick.get("qty") is None and tick.get("volume") is None:
            tick["qty"] = 0.0
        if tick.get("price") is None:
            # Без цены не обрабатываем
            return None
        if not hasattr(self, "logger"):
            self.logger = logger
        
        # ------------------------------------------------------------------
        # Robust Time Normalization (Expert Recommendation 3, Patch 1)
        # ------------------------------------------------------------------
        if tick.get("mock_force"):
             self.logger.warning("🔍 (%s) _handle_tick: START tick_ts=%s", runtime.symbol, tick.get("ts_ms"))
        tick_ts = int(
            tick.get("ts_ms")
            or tick.get("ts")
            or tick.get("event_time")
            or tick.get("written_at")
            or 0
        )
        # Only fallback if 0
        if tick_ts <= 0:
            return None

        indicators: Dict[str, Any] = {}

        # Monotonicity check (Expert Recommendation 3: detect -> sanitize -> quarantine)
        MAX_BACK_MS = int(os.getenv("TIME_MAX_BACK_MS", "2000"))
        prev_ts = int(getattr(runtime, "last_ts_ms", 0) or 0)

        if prev_ts > 0 and tick_ts < prev_ts:
            # backward time
            # indicators["tick_ts_backwards"] = 1
            back = prev_ts - tick_ts
            # indicators["tick_ts_back_ms"] = int(back)
            if tick_ts_backwards_total:
                tick_ts_backwards_total.labels(symbol=runtime.symbol).inc()

            if back <= MAX_BACK_MS:
                # sanitize: clamp slightly forward to keep deterministic monotonicity
                tick_ts = prev_ts + 1
                if tick_ts_clamped_total:
                     tick_ts_clamped_total.labels(symbol=runtime.symbol).inc()
                # indicators["tick_ts_clamped"] = 1
            else:
                # quarantine: too large rollback — fail-closed
                if tick_ts_quarantined_total:
                     tick_ts_quarantined_total.labels(symbol=runtime.symbol).inc()
                return None



        runtime.last_ts_ms = int(tick_ts)

        # Expert Recommendation 4: Track timestamp for Gap Cap
        lt_seen = int(getattr(runtime, "last_tick_seen_ts", 0) or 0)
        if lt_seen > 0 and tick_ts > lt_seen:
             gap = tick_ts - lt_seen
             try:
                 runtime.tick_gaps_ms.append(int(gap))
             except Exception:
                 pass
        runtime.last_tick_seen_ts = int(tick_ts)

        # Runtime overrides (cooldown/pressure tuning) — throttled, fail-open
        try:
            # async call: we are in sync function; use fire-and-forget
            asyncio.create_task(self._maybe_poll_symbol_overrides(runtime, int(tick_ts)))
        except Exception:
            pass

        # Initialize early
        confirmations: List[str] = []
        
        # Book health: check gaps and staleness
        book_ts_base = int(getattr(runtime, "last_book_ts_ms", 0) or 0)
        book_gap = int(tick_ts - book_ts_base) if book_ts_base > 0 else 0
        book_stale_ms = int(runtime.config.get("book_stale_ms", 5000))
        book_ok = 1 if (book_ts_base > 0 and book_gap < book_stale_ms) else 0
        indicators["book_health_ok"] = int(book_ok)
        indicators["book_ts_gap_ms"] = int(book_gap)

        # Track tick gaps (Section 5: Burst Calibrator)
        try:
            runtime.tick_gaps.record(int(tick_ts))
        except Exception:
            pass

        # Periodic calibration (every 200 ticks)
        if runtime.tick_count % 200 == 0:
            try:
                # Update window/max_age only if burst is not currently active
                # using the lock for safety although st.active check is usually okay
                with runtime.burst_mu:
                    is_active = getattr(runtime.burst.st, "active", False)
                    if not is_active:
                        gaps = runtime.tick_gaps.snapshot()
                        p_snap = runtime.pressure.snapshot(now_ms=int(tick_ts))
                        
                        w, ma = runtime.burst_cal.compute(
                            gap_p50_ms=float(gaps.get("p50", 0.0)),
                            cand_per_min=float(p_snap.per_min_ema)
                        )
                        runtime.burst.window_ms = int(w)
                        runtime.burst.max_age_ms = int(ma)
                        
                        # Metrics visibility
                        burst_window_ms_gauge.labels(symbol=runtime.symbol).set(float(w))
                        tick_gap_p50_ms_gauge.labels(symbol=runtime.symbol).set(float(gaps.get("p50", 0.0)))
            except Exception:
                pass
            
        # --- Book Health Gating (Stop Evidence) ---
        # If book is unhealthy, we cannot trust OBI or Iceberg signals.
        # We nullify them (force 0.0) so they don't contribute to the score.
        if int(indicators.get("book_health_ok", 1)) == 0:
            # We don't VETO the entire signal (maybe price action is valid),
            # but we remove microstructure evidence component.
            # (unless it's a super-strong price move > strong_z, handled elsewhere)
            # Nullify indicators for downstream
            indicators["obi"] = 0.0
            indicators["obi_z"] = 0.0
            indicators["iceberg_refresh"] = 0
            indicators["iceberg_avg_qty"] = 0.0
            # Optional: Log throttling?
            pass

        if runtime.heartbeat_counter >= 5000:
            self.logger.info(
                "💓 (%s) Heartbeat: processed 5000 ticks (total=%d) | last_price=%.2f | delta_triggers=%d",
                runtime.symbol,
                runtime.tick_count,
                float(tick.get("price") or 0.0),
                runtime.delta_triggers
            )
            runtime.heartbeat_counter = 0
        
        # Check side classification
        s = str(tick.get("side") or "").upper()
        if s not in ("BUY", "SELL"):
             ticks_side_unknown_total.labels(symbol=runtime.symbol).inc()

        # Tick-CVD update (Phase A) BEFORE delta_detector.push()

        try:
            if runtime.cvd_state:
                runtime.cvd_state.update(tick)
        except Exception:
            pass

        # MicroBar aggregation (Phase B)
        try:
            if runtime.microbar:
                cvd_val = getattr(runtime.cvd_state, "cvd_tick", 0.0)
                closed_bars = runtime.microbar.push_tick(tick, cvd_val)
                if closed_bars:
                    for b in closed_bars:
                        # === Microstructure spread robust stats (per-symbol) ===
                        try:
                            mid = float(getattr(b, "mid_last", 0.0) or 0.0)
                            spr = float(getattr(b, "spread_last", 0.0) or 0.0)
                            if mid > 0 and spr > 0:
                                spread_bps = 10000.0 * (spr / mid)
                                runtime.last_spread_bps = float(spread_bps)
                                runtime.spread_stats.update(float(spread_bps))
                                runtime.last_spread_z = float(runtime.spread_stats.z(float(spread_bps)))
                        except Exception:
                            pass
                        
                        # Fire async microbar closed handler
                        try:
                            asyncio.create_task(self._on_microbar_closed(runtime, b))
                        except Exception:
                            pass
        except Exception:
            pass

        # --- L3-lite (Reconciliation metrics) ---
        try:
            # 1. Feed trade
            runtime.l3_queue.on_trade(
                qty=float(tick.get("qty") or 0.0),
                is_buy=(str(tick.get("side")).upper() == "BUY")
            )
            
            # 2. Check bucket advancement
            bucket_ms = runtime.l3_queue.bucket_ms or 1000
            cur_bucket_id = int(tick_ts // bucket_ms)
            if runtime._last_l3_bucket_id is None:
                runtime._last_l3_bucket_id = cur_bucket_id
            elif cur_bucket_id > runtime._last_l3_bucket_id:
                # advance bucket and store stats
                runtime.l3_stats = runtime.l3_queue.on_bucket_advance(bucket_id=runtime._last_l3_bucket_id)
                runtime._last_l3_bucket_id = cur_bucket_id
        except Exception:
            pass

        delta_event = runtime.delta_detector.push(tick)
        if delta_event:
             # DEBUG: Confirm event creation immediately
             logger.info("🔍 [DELTA-EVENT] (%s) Event created: delta=%.2f z=%.2f", runtime.symbol, delta_event.get("delta", 0.0), delta_event.get("z", 0.0))
        price = _safe_float(tick.get("price")) or _safe_float(tick.get("last")) or _safe_float(tick.get("mid"))
        if price <= 0:
            return None

        # Pressure metric: raw triggers rate (pre-cooldown)
        try:
            if delta_event:
                runtime.pressure.on_raw_trigger(ts_ms=int(tick_ts))
            ps = runtime.pressure.snapshot(now_ms=int(tick_ts))
            indicators["pressure_per_min_ema"] = float(ps.per_min_ema)
            indicators["cooldown_hit_rate_ema"] = float(ps.cd_rate_ema)
            runtime.pressure_sps = float(ps.per_min_ema) / 60.0
        except Exception:
            pass

        # DN prefilter (dynamic tiers): reject weak delta_notional_usd early
        try:
            if delta_event:
                delta_val = float(delta_event.get("delta", 0.0) or 0.0)
                dn_usd_tick = abs(delta_val) * float(price)
                rg = str(getattr(runtime, "last_regime", "na") or "na")
                tier = int(runtime.dynamic_cfg.get("dn_tier", 1) or 1)
                # Threshold computed on bar_close (dynamic_cfg), fallback to config tiers
                th = float(runtime.dynamic_cfg.get("dn_th_usd", 0.0) or 0.0)
                if th <= 0:
                    # fallback bootstrap tiers
                    th = float(runtime.config.get(f"dn_tier{tier}_usd", 0.0) or 0.0)
                indicators["dn_usd_tick"] = float(dn_usd_tick)
                indicators["dn_th_usd"] = float(th)
                if th > 0 and dn_usd_tick < th:
                    indicators["dn_prefiltered"] = 1
                    logger.warning(
                        "🛑 [DN-PREFILTER-1] (%s) VETO: dn_usd=%.2f < th=%.2f. Signal blocked.",
                        runtime.symbol, dn_usd_tick, th
                    )
                    return None
        except Exception:
            pass
        
        # --- Prefilter: delta_notional_usd tiers (self-calibrating via dn_calib) ---
        try:
            if delta_event:
                dn_usd = abs(float(delta_event.get("delta", 0.0) or 0.0)) * float(price)
                regime = str(getattr(runtime, "last_regime", "na") or "na")
                # prefer dynamic threshold (computed on bar_close); fallback to config/bootstrap
                th = float(runtime.dynamic_cfg.get("dn_th_usd", 0.0) or 0.0)
                if th <= 0:
                    th = float(runtime.config.get("dn_tier1_usd", 0.0) or 0.0)
                indicators["dn_usd"] = float(dn_usd)
                indicators["dn_th_usd"] = float(th)
                if th > 0 and dn_usd < th:
                    # fail-closed for signal quality: too small notional under current tier policy
                    logger.warning(
                        "🛑 [DN-PREFILTER] (%s) VETO: dn_usd=$%.2f < th=$%.2f (regime=%s) - Signal blocked",
                        runtime.symbol, dn_usd, th, regime
                    )
                    return None
        except Exception:
            pass
        
        # Check against USD threshold if present
        if delta_event:
            delta_val = float(delta_event.get("delta", 0.0))
            delta_usd = abs(delta_val) * price
            min_usd = float(runtime.config.get("delta_abs_min_usd", 0.0) or 0.0)
            if min_usd > 1.0 and delta_usd < min_usd:
                 # Vetoed by USD threshold
                 logger.warning(
                     "🛑 [MIN-USD] (%s) VETO: delta_usd=$%.2f < min=$%.2f - Signal blocked",
                     runtime.symbol, delta_usd, min_usd
                 )
                 return None

        if not delta_event:
            self._log_metrics(runtime)
            return None

        # Trigger Event!
        runtime.delta_triggers += 1
        
        # --- Pressure tracking: candidate attempts (deterministic by tick_ts) ---
        try:
            runtime.signal_attempt_ts_ms.append(int(tick_ts))
            psps = _calc_pressure_sps(list(runtime.signal_attempt_ts_ms), int(tick_ts), 60_000)
            # light smoothing (EMA)
            a = float(runtime.config.get("pressure_ema_alpha", 0.20))
            if a <= 0 or a > 1: a = 0.20
            runtime.pressure_sps = float((1.0 - a) * float(getattr(runtime, "pressure_sps", 0.0) or 0.0) + a * psps)
            indicators["pressure_sps"] = float(runtime.pressure_sps)
            # pressure_hi flag
            thr = float(runtime.config.get("pressure_hi_sps", 0.12))  # ~7.2 кандидатов/мин
            runtime.pressure_hi = 1 if runtime.pressure_sps >= thr else 0
            indicators["pressure_hi"] = int(runtime.pressure_hi)
        except Exception:
            pass

        # Update indicators with trigger context
        indicators["delta_z"] = delta_event.get("z", 0.0)
        
        # Диагностика: логируем срабатывание детектора (по флагу)
        if DEBUG_DELTAS:
            # Sampled debug log for delta trigger
            if runtime.delta_log_sampler.should_log("delta_trigger"):
                logger.debug(
                    "🔍 (%s) Delta detector triggered: delta=%.2f, z=%.2f, threshold=%.2f",
                    runtime.symbol,
                    delta_event.get("delta", 0.0),
                    delta_event.get("z", 0.0),
                    runtime.delta_detector.z_threshold,
                )

        # Determine signal direction
        direction = "LONG" if delta_event["delta"] >= 0 else "SHORT"

        # ------------------------------------------------------------------
        # ATR floor veto (tier-by-regime) — FIX BROKEN CHAIN
        # ВАЖНО:
        #   - раньше читали atr_bps_th, но не выбирали tier -> th оставался 0.0
        #   - теперь выбираем tier прямо здесь (safety), используя runtime.dynamic_cfg + bootstrap.
        # Fail-open:
        #   - если чего-то не хватает -> не блокируем (как и было), но всё логируем в indicators.
        # ------------------------------------------------------------------
        try:
            rg = str(getattr(runtime, "last_regime", "na") or "na").lower()
            # current ATR(bps) from dynamic cfg if available (bar-close)
            atr_bps = float(runtime.dynamic_cfg.get("atr_bps", 0.0) or 0.0)
            # if not available, attempt quick compute from last_atr and current tick price
            if atr_bps <= 0:
                p = float(tick.get("price") or 0.0)
                a = float(getattr(runtime, "last_atr", 0.0) or 0.0)
                if p > 0 and a > 0:
                    atr_bps = 10000.0 * (a / p)

            tier = int(runtime.config.get("atr_floor_tier_default", 1))
            if rg in ("trend", "trending_bull", "trending_bear"):
                tier = int(runtime.config.get("atr_floor_tier_trend", 0))
            elif rg in ("thin", "news", "illiquid"):
                tier = int(runtime.config.get("atr_floor_tier_thin", 2))
            else:
                tier = int(runtime.config.get("atr_floor_tier_range", 1))

            # prefer selected threshold if already computed
            atr_bps_th = float(runtime.dynamic_cfg.get("atr_bps_th", 0.0) or 0.0)
            picked = 0.0
            if atr_bps_th <= 0:
                t0 = float(runtime.dynamic_cfg.get("atr_floor_t0_bps", runtime.config.get("atr_floor_t0_bps", 0.0)) or 0.0)
                t1 = float(runtime.dynamic_cfg.get("atr_floor_t1_bps", runtime.config.get("atr_floor_t1_bps", 0.0)) or 0.0)
                t2 = float(runtime.dynamic_cfg.get("atr_floor_t2_bps", runtime.config.get("atr_floor_t2_bps", 0.0)) or 0.0)
                picked = t1
                if tier <= 0:
                    picked = t0
                elif tier >= 2:
                    picked = t2
                static_min = float(runtime.config.get("atr_bps_min_static", 0.0) or 0.0)
                atr_bps_th = max(static_min, float(picked or 0.0))

            indicators["atr_bps"] = float(atr_bps)
            indicators["atr_bps_th"] = float(atr_bps_th)
            indicators["atr_floor_tier"] = int(tier)
            indicators["atr_floor_rg"] = str(rg)
            if picked > 0:
                indicators["atr_floor_picked_bps"] = float(picked)

            audit_only = bool(int(runtime.config.get("atr_gate_audit_only", 0) or 0))
            if atr_bps_th > 0 and atr_bps > 0 and atr_bps < atr_bps_th:
                indicators["atr_gate_veto"] = 1
                if not audit_only:
                    atr_gate_veto_total.labels(symbol=runtime.symbol, reason="low_volatility", mode="ENFORCE").inc()
                    logger.warning(
                        "🛑 [ATR-GATE-EARLY] (%s) VETO: atr_bps=%.2f < th=%.2f (tier=%d rg=%s) - Signal blocked",
                        runtime.symbol, atr_bps, atr_bps_th, tier, rg
                    )
                    return None
        except Exception:
            pass

        # Deterministic "now" (tick time preferred; wall-time fallback only if missing)
        now_ts = tick_ts if tick_ts > 0 else int(time.time() * 1000)

        indicators.update({
            "delta": delta_event.get("delta", 0.0),
            "delta_z": delta_event.get("z", 0.0),
        })

        # ------------------------------------------------------------------
        # Variant A: Publish delta_spike event for decentralized OFConfirm service
        # ------------------------------------------------------------------
        try:
            spike_out = {
                "type": "delta_spike",
                "symbol": runtime.symbol,
                "ts_ms": now_ts,
                "price": float(price),
                "direction": direction,
                "delta": float(delta_event.get("delta", 0.0)),
                "delta_z": float(delta_event.get("z", 0.0))
            }
            # Optional: if we already have features from runtime
            abs_feat = runtime.absorption_detector.push(tick, runtime.last_book, price)
            if abs_feat:
                spike_out["absorption"] = abs_feat
            
            # Enrich with OBI/Iceberg (if not stale)
            now_ms = int(time.time() * 1000)
            obi_ttl = int(runtime.config.get("obi_event_ttl_ms", 15000))
            if runtime.last_obi_event and (now_ms - runtime.last_obi_event.get("ts_ms", 0)) < obi_ttl:
                spike_out["obi"] = runtime.last_obi_event
            
            ice_ttl = int(runtime.config.get("iceberg_event_ttl_ms", 15000))
            if runtime.last_iceberg_event and (now_ms - runtime.last_iceberg_event.get("ts_ms", 0)) < ice_ttl:
                spike_out["iceberg"] = runtime.last_iceberg_event
            
            # Enrich with L3-lite stats
            if runtime.l3_stats:
                spike_out.update({
                    "cancel_bid_rate_ema": float(runtime.l3_stats.cancel_bid_rate_ema),
                    "cancel_ask_rate_ema": float(runtime.l3_stats.cancel_ask_rate_ema),
                    "taker_buy_rate_ema": float(runtime.l3_stats.taker_buy_rate_ema),
                    "taker_sell_rate_ema": float(runtime.l3_stats.taker_sell_rate_ema),
                })

            asyncio.create_task(
                self.main.xadd(
                    "events:delta_spike",
                    {"payload": json.dumps(spike_out, ensure_ascii=False)},
                    maxlen=20000,
                    approximate=True
                )
            )
        except Exception as e:
            logger.error(f"Failed to publish delta_spike event: {e}")

        # Attach Tick-CVD indicators
        try:
            if runtime.cvd_state:
                indicators.update(runtime.cvd_state.indicators_light())
                indicators.update(runtime.cvd_state.robust_snapshot())
        except Exception:
            pass

        # Attach Phase B structure snapshots
        try:
            if runtime.last_bar:
                b = runtime.last_bar
                indicators.update({
                    "microbar_tf_ms": int(b.tf_ms),
                    "microbar_start_ts": int(b.start_ts_ms),
                    "microbar_end_ts": int(b.end_ts_ms),
                    "microbar_open": float(b.open),
                    "microbar_high": float(b.high),
                    "microbar_low": float(b.low),
                    "microbar_close": float(b.close),
                    "microbar_vol": float(b.vol),
                    "microbar_delta_sum": float(b.delta_sum),
                    "microbar_cvd_close": float(b.cvd_close),
                    "microbar_vwap": float(b.vwap),
                    "microbar_mid": float(b.mid_last) if b.mid_last is not None else None,
                    "microbar_spread": float(b.spread_last) if b.spread_last is not None else None,
                    "microbar_ticks": int(b.tick_count),
                })
            
            # RSI indicators (if available)
            if hasattr(runtime, "rsi_price") and runtime.rsi_price.value is not None:
                indicators["rsi_price"] = float(runtime.rsi_price.value)
            if hasattr(runtime, "rsi_cvd") and runtime.rsi_cvd.value is not None:
                indicators["rsi_cvd"] = float(runtime.rsi_cvd.value)

            # RSI Confirmation check
            rp = float(indicators.get("rsi_price", 50.0))
            rc = float(indicators.get("rsi_cvd", 50.0))
            if direction == "LONG" and rp > 50 and rc > 50:
                confirmations.append("rsi_agree=1")
            elif direction == "SHORT" and rp < 50 and rc < 50:
                confirmations.append("rsi_agree=1")

            if runtime.last_swing_high:
                sh = runtime.last_swing_high
                indicators.update({
                    "swing_high_ts": int(sh.ts_ms),
                    "swing_high_px": float(sh.price),
                    "swing_high_cvd": float(sh.cvd),
                })
            if runtime.last_swing_low:
                sl = runtime.last_swing_low
                indicators.update({
                    "swing_low_ts": int(sl.ts_ms),
                    "swing_low_px": float(sl.price),
                    "swing_low_cvd": float(sl.cvd),
                })
            if runtime.last_div:
                dv = runtime.last_div
                indicators.update({
                    "div_kind": str(dv.kind),
                    "div_ts": int(dv.ts_ms),
                    "div_strength": float(dv.strength),
                    "div_price_prev": float(dv.price_prev),
                    "div_price_curr": float(dv.price_curr),
                    "div_cvd_prev": float(dv.cvd_prev),
                    "div_cvd_curr": float(dv.cvd_curr),
                })
        except Exception:
            pass

        # Phase C/D: Metadata for Payload (Sweep, Footprint, Weak Progress)
        try:
            ev = runtime.last_sweep
            if ev is not None:
                div = runtime.last_div
                div_match = False
                if div is not None:
                    if ev.direction_bias == "SHORT" and str(div.kind).startswith("bearish"):
                        div_match = True
                    if ev.direction_bias == "LONG" and str(div.kind).startswith("bullish"):
                        div_match = True
                indicators["sweep_div_match"] = int(1 if div_match else 0)
                if div_match: confirmations.append("div_match=1")

            b = runtime.last_bar
            if b is not None and getattr(b, "fp_enabled", False):
                indicators.update({
                    "fp_bucket_px": float(getattr(b, "fp_bucket_px", 0.0) or 0.0),
                    "fp_max_imbalance": float(getattr(b, "fp_max_imbalance", 0.0) or 0.0),
                    "fp_absorb_score": float(getattr(b, "fp_absorb_score", 0.0) or 0.0),
                })
                fp_confs = fp_confirmations_from_microbar(b, direction, runtime.config)
                for c in fp_confs:
                    confirmations.append(c)
            
            wp = runtime.last_wp
            if wp is not None:
                indicators.update({"weak_range_atr": wp.range_atr, "weak_body_atr": wp.body_atr, "weak_eff": wp.eff})
        except Exception:
            pass

        # ------------------------------------------------------------
        # OFConfirm Engine (single source of truth for decision & score)
        # ------------------------------------------------------------
        try:
            # Re-read absorption for the engine
            absorption = runtime.absorption_detector.push(tick, runtime.last_book, price)
            
            # --- BOOK HEALTH GATE ---
            # Robust gate using pre-computed health (lines 1728+)
            book_ok = int(indicators.get("book_health_ok", 1))
            book_health = str(indicators.get("book_health", "OK"))
            
            # Additional check: explicitly verify threshold from dynamic config (if computed)
            try:
                # If health logic says OK but we have strict calibrated thresholds that fail:
                br = float(indicators.get("book_rate_hz", 0.0))
                min_hz = float(runtime.dynamic_cfg.get("book_rate_min_hz", 0.0))
                if min_hz > 0 and br < min_hz:
                    book_ok = 0
                    indicators["book_health_ok"] = 1 # Keep indicator raw but...
                    # Wait, expert says: "mark indicators['book_health_ok']=0"
                    indicators["book_health_ok"] = 0
                    indicators["book_health"] = "LOW_RATE_CALIB"
            except Exception:
                pass
            
            if book_ok == 0:
                # Stale or Unhealthy -> Disable Microstructure Evidence
                # We do NOT return None (fail-close for signal), but we zero-out 
                # book-dependent evidence so OFConfirmEngine sees "no evidence".
                indicators["obi"] = 0
                indicators["iceberg_refresh"] = 0
                indicators["iceberg_avg_qty"] = 0
                
                # Verify removal of any other book-dependent components if needed? 
                # Currently these are the main ones feeding score.
                
                # Check for debug logs
                if bool(int(os.getenv("DEBUG_DELTAS", "0"))):
                     logger.debug("⚠️ (%s) Book Health Fail: %s (OBI/Iceberg disabled)", runtime.symbol, book_health)
            
            # --- PRESSURE PROXY LAYER START ---
            # 1. Update meters
            # Note: We do NOT add tick_ts to pressure here. Pressure tracks *candidates*, recorded later.
            
            # 2. Compute metrics
            p_snap = runtime.pressure.snapshot(now_ms=int(tick_ts))
            pres_per_min = float(p_snap.per_min_ema)
            cd_per_min = float(p_snap.cd_rate_ema)
            
            hit_rate = cd_per_min # It's already an EMA rate

            runtime.last_pressure_per_min = pres_per_min
            runtime.last_cd_hit_rate = hit_rate
            indicators["pressure_per_min"] = pres_per_min
            indicators["cooldown_hit_rate"] = hit_rate

            # 3. Dynamic Thresholds
            p_hi = float(runtime.config.get("pressure_hi_per_min", 0.0) or 0.0)
            p_ext = float(runtime.config.get("pressure_extreme_per_min", 0.0) or 0.0)
            
            pressure_hi = int(p_hi > 0 and pres_per_min >= p_hi)
            pressure_extreme = int(p_ext > 0 and pres_per_min >= p_ext)
            
            runtime.dynamic_cfg["pressure_per_min"] = pres_per_min
            runtime.dynamic_cfg["pressure_hi"] = pressure_hi
            runtime.dynamic_cfg["pressure_extreme"] = pressure_extreme
            indicators["pressure_hi_flag"] = pressure_hi
            indicators["pressure_extreme_flag"] = pressure_extreme

            # 4. Strictness escalation (Need=3)
            # If pressure is high, increase required confirmations (reversal/continuation need -> 3)
            # Only if strong_dynamic_need_enable=1 (default)
            if bool(int(runtime.config.get("strong_dynamic_need_enable", 1))):
                # Base needs from current dynamic or config
                base_r = int(runtime.dynamic_cfg.get("strong_need_reversal", runtime.config.get("strong_need_reversal", 2)) or 2)
                base_c = int(runtime.dynamic_cfg.get("strong_need_continuation", runtime.config.get("strong_need_continuation", 2)) or 2)
                
                if pressure_hi or pressure_extreme:
                    runtime.dynamic_cfg["strong_need_reversal"] = max(base_r, 3)
                    runtime.dynamic_cfg["strong_need_continuation"] = max(base_c, 3)
            
            # 5. Delta Notional Tier Gating (Robust Anti-Noise Filter)
            # Tiers: 0=Trend, 1=Range, 2=Noise/News.
            # Base selection by regime, escalated by pressure.
            
            # A) Resolve Tiers
            # Try config first, else defaults
            tiers_cfg = runtime.config.get("delta_diff_tiers") 
            if not tiers_cfg:
                # from core.instrument_config import get_default_delta_tiers
                tiers_cfg = get_default_delta_tiers(runtime.symbol)
            else:
                 # Ensure it's dict of floats
                 pass

            # B) Determine Base Tier
            # If we detect Trend mode -> Tier 0. Default -> Tier 1 (Mixed/Range).
            # Note: "market_mode" might be populated by previous ticks or other logic?
            # It's usually "mixed", "trend_up", etc.
            mm = str(indicators.get("market_mode", "mixed")).lower()
            base_tier_idx = 0 if "trend" in mm else 1
            
            # C) Apply Pressure Escalation
            # If pressure is high, bump +1 (e.g. Trend->Range, Range->News)
            if pressure_hi:
                base_tier_idx += 1
            if pressure_extreme:
                 # Should we bump +2 or just stick to max? Logic says "raise tier on +1".
                 # Extreme might imply Tier 2 immediately.
                 base_tier_idx = max(base_tier_idx, 2)
            
            # Clip to max tier available (usually Tier 2)
            base_tier_idx = min(base_tier_idx, 2)
            
            current_tier_key = f"tier{base_tier_idx}"
            tier_threshold = float(tiers_cfg.get(current_tier_key, tiers_cfg.get("tier1", 100000.0)))
            
            delta_val_abs = abs(delta_event.get("delta", 0.0))
            notional_usd = delta_val_abs * price
            indicators["delta_notional_usd"] = notional_usd
            indicators["pressure_tier_active"] = base_tier_idx
            indicators["pressure_tier_threshold"] = tier_threshold
            
            # D) Filter
            if tier_threshold > 1.0 and notional_usd < tier_threshold:
                 # "ne schitaem... validnym sobytiem" -> Drop immediately.
                 ticks_pressure_filtered_total.labels(symbol=runtime.symbol, reason=f"tier{base_tier_idx}").inc()
                 
                 # FORCE LOG for diagnostics
                 logger.warning(
                     "🛑 [PRESSURE-TIER] (%s) VETO: Filtered by Tier%d (P=%d, M=%s): USD=%.1f < %.1f", 
                     runtime.symbol, base_tier_idx, int(pressure_hi), mm, notional_usd, tier_threshold
                 )
                 
                 if bool(int(os.getenv("DEBUG_DELTAS", "0"))):
                     logger.debug(
                         "🚫 (%s) Filtered by Tier%d (P=%d, M=%s): USD=%.1f < %.1f", 
                         runtime.symbol, base_tier_idx, int(pressure_hi), mm, notional_usd, tier_threshold
                     )
                 return None
            # --- PRESSURE PROXY LAYER END ---

            # Merge static cfg + dynamic calibrated thresholds
            cfg2 = dict(runtime.config)
            try:
                dyn = getattr(runtime, "dynamic_cfg", {}) or {}
                if bool(int(cfg2.get("abs_lvl_use_dynamic_th", 1))):
                    cfg2.update(dyn)
                else:
                    indicators["abs_lvl_dynamic_disabled"] = 1
            except Exception:
                pass

            try:
                # readiness gate
                min_samples = int(cfg2.get("eff_calib_min_samples", cfg2.get("EFF_CALIB_MIN_SAMPLES", 300)) or 300)
                calib_n = int(cfg2.get("abs_lvl_calib_n", 0) or 0)
                calib_src = str(cfg2.get("abs_lvl_calib_src", "static") or "static")
                abs_ready = int((calib_n >= min_samples) and (calib_src != "static"))
                
                # safety switch: unstable -> disable ready
                if int(cfg2.get("abs_lvl_th_unstable", 0) or 0) == 1:
                    abs_ready = 0
                    indicators["abs_lvl_disabled_by_unstable"] = 1
                    
                cfg2["abs_lvl_calib_ready"] = abs_ready
                indicators["abs_lvl_ready"] = abs_ready
            except Exception:
                pass
                
            # Continuation context update: if this spike is counter-trend + weak progress, record it.
            # This enables Bit C in eval_continuation for future trend-aligned signals.
            try:
                div_k = getattr(runtime.last_div, "kind", None) if runtime.last_div else None
                t_dir = hidden_trend_dir(div_k)
                if t_dir is not None and direction != t_dir:
                    if runtime.last_wp and runtime.last_wp.weak_any:
                        runtime.cont_ctx_ts_ms = now_ts
                        runtime.cont_ctx_trend_dir = t_dir
            except Exception:
                pass

            # Continuation veto logic
            try:
                div_k = getattr(runtime.last_div, "kind", None) if runtime.last_div else None
                t_dir = hidden_trend_dir(div_k)
                veto_th = float(cfg2.get("abs_lvl_cont_veto_score", 0.75))
                abs_bias = str(indicators.get("abs_lvl_bias", "NONE") or "NONE").upper()
                abs_score = float(indicators.get("abs_lvl_score", 0.0) or 0.0)
                if int(indicators.get("abs_lvl_ready", 0)) == 1 and t_dir is not None:
                    if abs_bias in ("LONG","SHORT") and abs_bias != str(t_dir).upper() and abs_score >= veto_th:
                        indicators["abs_lvl_cont_veto"] = 1
            except Exception:
                pass

            # Threshold and weighting overrides: relax 0.65 -> 0.45
            cfg2["of_score_min"] = float(cfg2.get("of_score_min", 0.45))
            if cfg2["of_score_min"] == 0.65:
                cfg2["of_score_min"] = 0.45 # Force lower if it was stick at old default

            # Divergence Sensitivity
            cfg2["div_strength_min"] = float(cfg2.get("div_strength_min", 1.5))
            cfg2["div_min_price_bp"] = float(cfg2.get("div_min_price_bp", 3.0))
            if hasattr(runtime, "divergence") and runtime.divergence:
                runtime.divergence.apply_config(cfg2)

            ofc, dec = self.of_engine.build(
                symbol=runtime.symbol,
                tf=str(runtime.config.get("micro_tf", "1s")),
                direction=direction,
                tick_ts_ms=tick_ts,
                price=float(price),
                delta_z=float(delta_event.get("z", 0.0)),
                runtime=runtime,
                cfg=cfg2,
                indicators=indicators,
                absorption=absorption if isinstance(absorption, dict) else None,
            )

            # expose calibration diagnostics
            indicators["abs_lvl_eff_quote_th"] = float(cfg2.get("abs_lvl_eff_quote_th", 0.0) or 0.0)
            indicators["abs_lvl_min_quote_delta"] = float(cfg2.get("abs_lvl_min_quote_delta", 0.0) or 0.0)
            indicators["abs_lvl_calib_n"] = int(cfg2.get("abs_lvl_calib_n", 0) or 0)
            indicators["abs_lvl_calib_src"] = str(cfg2.get("abs_lvl_calib_src", "static"))

            if ofc:
                ev = ofc.evidence
                indicators["of_confirm"] = ofc.to_dict()
                indicators["of_confirm_v3"] = ofc.to_dict()
                indicators["of_confirm_ok"] = int(ofc.ok)
                
                # Use dec directly from build() instead of overwriting with None
                if dec and hasattr(dec, "need") and hasattr(dec, "have"):
                    indicators["of_confirm_score"] = float(dec.have / dec.need) if dec.need > 0 else 0.0
                    
                    # Persist last strong-gate diagnostics for SMT snapshot / entry policy.
                    try:
                        indicators["strong_gate_have"] = int(dec.have)
                        indicators["strong_gate_need"] = int(dec.need)
                        indicators["strong_gate_scn"] = str(dec.scenario)
                        indicators["strong_need_reason"] = str(getattr(dec, "need_reason", "") or "")

                        runtime.last_of_confirm_score = float(indicators.get("of_confirm_score", 0.0) or 0.0)
                        runtime.last_strong_gate_have = int(indicators.get("strong_gate_have", 0) or 0)
                        runtime.last_strong_gate_need = int(indicators.get("strong_gate_need", 0) or 0)
                        runtime.last_strong_gate_scn = str(indicators.get("strong_gate_scn", "") or "")
                    except Exception:
                        pass
                indicators["strong_gate_bits"] = int(ofc.gate_bits)
                indicators["strong_gate_reason"] = str(ofc.reason)
                indicators["strong_gate_ok"] = int(ofc.ok)  # Explicitly expose for metrics
                indicators["of_gate_mode"] = "SHADOW" if bool(runtime.config.get("strong_gate_shadow", False)) else "ENFORCE"

                # --- NEW: record last strong-pass dir/ts ONLY when gate passed (ok==1) ---
                # This is the value SMT/EntryPolicy should trust as "leader confirmed by OF".
                try:
                    if int(ofc.ok) == 1:
                        runtime.last_strong_pass_ts_ms = int(tick_ts)
                        runtime.last_strong_pass_dir = str(direction).upper()
                except Exception:
                    pass




                # Rate limit logs: only 1 in 50
                sg_cnt = self.strong_gate_counters.get(runtime.symbol, 0) + 1
                self.strong_gate_counters[runtime.symbol] = sg_cnt

                if sg_cnt % 50 == 0:
                    self.logger.info(
                        "🔥 Signal Strong-Gate Decision: symbol=%s, scenario=%s, ok=%d, score=%.2f, have=%d, need=%d, reason=%s (x%d)",
                        runtime.symbol, ofc.scenario, ofc.ok, ofc.score, ofc.have, ofc.need, ofc.reason, sg_cnt
                    )

                # ENFORCE / SHADOW logic
                if bool(runtime.config.get("require_strong_confirmation", False)) and ofc.ok == 0:
                    if bool(runtime.config.get("strong_gate_shadow", False)):
                        indicators["strong_gate_shadow_veto"] = 1
                    else:
                        strong_gate_veto_total.labels(symbol=runtime.symbol, scenario=ofc.scenario, reason="engine_veto", mode="ENFORCE").inc()
                        # Add explicit visibility for dropped signals
                        self.logger.warning(
                            "🚫 Signal filtered by Strong Gate (ENFORCE): symbol=%s, scenario=%s, reason=%s. "
                            "To fix, enable strong_gate_shadow=1 or disable require_strong_confirmation.",
                            runtime.symbol, ofc.scenario, ofc.reason
                        )
                        return None

                # Audit Confirmations (mirror resulting evidence)
                # Note: We append these to confirmations list for Telegram/UI
                if ev.get("sweep"):
                    div_match = bool(indicators.get("sweep_div_match", 0))
                    require_div = bool(runtime.config.get("sweep_require_divergence", 0))
                    if (not require_div) or div_match:
                         kind = indicators.get("sweep_kind", "")
                         confirmations.insert(0, "sweep_eqh=1" if kind == "EQH_SWEEP" else "sweep_eql=1")
                
                if ev.get("absorption"): confirmations.append(f"absorption={ev.get('absorption_volume', 0.0):.2f}")
                if ev.get("weak_progress"): confirmations.append("weak_progress=1")
                if ev.get("abs_lvl_ok"): confirmations.append(f"abs_lvl={ev.get('abs_lvl_score', 0.0):.2f}")

                # ------------------------------------------------------------
                # Phase E: OBI quality, FP Edge Absorb, Weak Trend (Scoring/Telemetry)
                # ------------------------------------------------------------
                try:
                    now_ms_det = int(now_ms)
                    # OBI stability (quality-gated)
                    if runtime.last_obi_event:
                        age = now_ms_det - int(runtime.last_obi_event.get("ts_ms", 0) or 0)
                        ttl = int(runtime.config.get("obi_event_ttl_ms", 15000))
                        if 0 <= age <= ttl:
                            indicators["obi_event_age_ms"] = int(age)
                            indicators["obi_dir"] = str(runtime.last_obi_event.get("direction") or "")
                            indicators["obi"] = float(runtime.last_obi_event.get("obi", 0.0) or 0.0)
                            indicators["obi_z"] = float(runtime.last_obi_event.get("obi_z", 0.0) or 0.0)
                            indicators["obi_stable_secs"] = float(runtime.last_obi_event.get("stable_secs", 0.0) or 0.0)
                            indicators["obi_stability_score"] = float(runtime.last_obi_event.get("stability_score", 0.0) or 0.0)
                            indicators["obi_sustained"] = bool(int(runtime.last_obi_event.get("stable", 0) or 0) == 1)
                            if str(runtime.last_obi_event.get("direction") or "").upper() == direction:
                                if indicators["obi_sustained"]:
                                    confirmations.append(f"obi_stable={float(indicators['obi_stable_secs']):.2f}")

                    # Footprint edge absorb (recent, no range expansion)
                    fe = getattr(runtime, "last_fp_edge", None)
                    if fe is not None:
                        valid = int(runtime.config.get("fp_edge_valid_ms", 30000))
                        age = now_ms_det - int(getattr(fe, "ts_ms", 0) or 0)
                        if 0 <= age <= valid:
                            p90 = float(getattr(fe, "p90", 0.0) or 0.0)
                            val = float(getattr(fe, "value", 0.0) or 0.0)
                            strength = (val / p90) if p90 > 0 else 0.0
                            bias = str(getattr(fe, "bias", "") or "").upper()
                            rng = int(getattr(fe, "range_expansion", 0) or 0)
                            # Logic: LONG signal needs BUY bias edge (support?), SHORT needs SELL bias?
                            # Actually, tick-level fp_edge side "BID" means absorption on bid (support).
                            # If bias is present, use it.
                            ok = 1 if (bias == direction and rng == 0 and strength > 0) else 0
                            indicators["fp_edge_absorb"] = int(ok)
                            indicators["fp_edge_strength"] = float(strength)
                            indicators["fp_edge_range_expansion"] = int(rng)
                            indicators["fp_edge_age_ms"] = int(age)
                            if ok:
                                confirmations.append(f"fp_edge_absorb={strength:.2f}")

                    # Weak progress trend (history)
                    try:
                        wp_det = getattr(runtime, "weak_progress_det", None)
                        if wp_det is not None:
                            indicators["weak_recent_window"] = int(getattr(wp_det, "recent_window", 0) or 0)
                            indicators["weak_recent_count"] = int(wp_det.recent_weak_count())
                            w = int(indicators["weak_recent_window"] or 0)
                            c = int(indicators["weak_recent_count"] or 0)
                            ratio = float(c / w) if w > 0 else 0.0
                            indicators["weak_recent_ratio"] = ratio
                            
                            # Legacy boolean for Scorer fallback
                            min_weak = int(runtime.config.get("weak_recent_min_cnt", 3))
                            indicators["weak_progress"] = bool(ev.get("weak_progress") or (c >= min_weak))
                            if c >= min_weak:
                                confirmations.append(f"weak_recent={c}/{w}")
                    except Exception:
                        pass
                except Exception:
                    pass
                    
                # Iceberg (Strict/Recent)
                if runtime.last_iceberg_event:
                     ice_ts = int(runtime.last_iceberg_event.get("ts_ms") or 0)
                     if (tick_ts - ice_ts) < 5000:
                         confirmations.append(f"iceberg={runtime.last_iceberg_event.get('total_refresh_qty')}")
                         # strict direction check
                         ice_side = str(runtime.last_iceberg_event.get("side")).upper()
                         spike_side = "BUY" if float(delta_event.get("delta", 0)) > 0 else "SELL"
                         iceberg_side = "BUY" if ice_side == "BID" else "SELL" # iceberg is limit
                         # We want opposing iceberg for absorption
                         if spike_side != iceberg_side:
                              confirmations.append("ice_strict=1")


                # Optional Redis Publication (v3 asychronous)
                if bool(int(runtime.config.get("publish_of_confirm", 0))):
                    stream = str(runtime.config.get("of_confirm_stream", "signals:of:confirm"))
                    try:
                        asyncio.create_task(
                            self.ticks.xadd(
                                stream,
                                fields={"payload": json.dumps(ofc.to_dict(), ensure_ascii=False)},
                                maxlen=int(runtime.config.get("of_confirm_stream_maxlen", 50000)),
                                approximate=True,
                            )
                        )
                    except Exception:
                        pass
                    
                    # ------------------------------------------------------------
                    # Publish deterministic decision inputs for golden replay
                    # ------------------------------------------------------------
                # ------------------------------------------------------------
                # Publish deterministic decision inputs for golden replay
                # ------------------------------------------------------------
                try:
                    # logger.error("DEBUG: 1. accessing OFI config")
                    pub_val = runtime.config.get("publish_of_inputs", 0)
                    should_pub = bool(int(pub_val))
                    
                    if should_pub:
                        # logger.error("DEBUG: 2. Entering OFI Logic")
                        # continuation context
                        trend_dir = "NONE"
                        hidden_ctx_recent = 0
                        cont_ctx_recent = 0
                        try:
                            div = getattr(runtime, "last_div", None)
                            td = hidden_trend_dir(getattr(div, "kind", None) if div else None)
                            if td:
                                trend_dir = str(td).upper()
                            # hidden ctx
                            if div and td:
                                now_ts = int(tick_ts) if int(tick_ts) > 0 else int(time.time() * 1000)
                                hidden_ms = int(runtime.config.get("hidden_ctx_valid_ms", 120_000))
                                age = now_ts - int(getattr(div, "ts_ms", now_ts))
                                hidden_ctx_recent = 1 if (0 <= age <= hidden_ms) else 0
                            # cont ctx
                            now_ts = int(tick_ts) if int(tick_ts) > 0 else int(time.time() * 1000)
                            cts = int(getattr(runtime, "cont_ctx_ts_ms", 0) or 0)
                            cv = int(runtime.config.get("cont_ctx_valid_ms", 120_000))
                            cont_ctx_recent = 1 if (cts > 0 and 0 <= now_ts - cts <= cv) else 0
                        except Exception as ex_ctx:
                            logger.error(f"DEBUG: Context calc error: {ex_ctx}")

                        # 2. Extract evidence
                        ev_weak = int(indicators.get("weak_progress", 0))
                        ev_sweep = int(indicators.get("sweep", 0))
                        ev_reclaim = int(indicators.get("reclaim", 0))
                        ev_obi_stable = int(indicators.get("obi_stable", 0))
                        ev_ice_strict = int(indicators.get("ice_strict", 0))
                        ev_abs_lvl_ok = int(indicators.get("abs_lvl_ok", 0))
                        
                        if ofc and hasattr(ofc, "evidence") and isinstance(ofc.evidence, dict):
                            ev_weak = int(ofc.evidence.get("weak_progress", ev_weak))
                            ev_sweep = int(ofc.evidence.get("sweep", ev_sweep))
                            ev_reclaim = int(ofc.evidence.get("reclaim", ev_reclaim))
                            ev_obi_stable = int(ofc.evidence.get("obi_stable", ev_obi_stable))
                            ev_ice_strict = int(ofc.evidence.get("iceberg_strict", ev_ice_strict))
                            ev_abs_lvl_ok = int(ofc.evidence.get("abs_lvl_ok", ev_abs_lvl_ok))
                        
                        # 4. Create Object
                        # logger.error("DEBUG: 4. Creating OFI Object")
                        
                        # Safe CFG
                        cfg_safe = {}
                        
                        ofi = OFInputsV1(
                            v=1,
                            symbol=str(runtime.symbol),
                            ts_ms=int(tick_ts),
                            regime=str(getattr(runtime, "last_regime", "na")),
                            direction=str(direction),
                            scenario=str(dec.scenario),
                            delta_z=float(delta_event.get("z", 0.0)),
                            weak_progress=ev_weak,
                            sweep_recent=ev_sweep,
                            reclaim_recent=ev_reclaim,
                            obi_stable=ev_obi_stable,
                            iceberg_strict=ev_ice_strict,
                            abs_lvl_ok=ev_abs_lvl_ok,
                            trend_dir=str(trend_dir or "NONE"),
                            hidden_ctx_recent=int(hidden_ctx_recent),
                            cont_ctx_recent=int(cont_ctx_recent),
                            cfg=cfg_safe, 
                            fp_eff_quote=float(getattr(runtime.last_bar, "fp_eff_quote", 0.0) if runtime.last_bar else 0.0),
                            fp_quote_delta=float(getattr(runtime.last_bar, "fp_quote_delta", 0.0) if runtime.last_bar else 0.0),
                        )
                        # logger.error("DEBUG: 5. Serializing...")
                        blob = json.dumps(ofi.to_dict(), ensure_ascii=False)
                        
                        in_stream = str(runtime.config.get("of_inputs_stream", "stream:of:inputs"))
                        
                        logger.error("DEBUG: 7. Publishing to Redis...")
                        asyncio.create_task(
                            self.ticks.xadd(
                                in_stream,
                                fields={"payload": blob},
                                maxlen=int(runtime.config.get("of_inputs_stream_maxlen", 200000)),
                                approximate=True,
                            )
                        )
                        logger.error("DEBUG: 8. PublishedTask Created")

                except Exception as e_main:
                     logger.error(f"DEBUG: OFInputs Block error: {e_main}")
                     pass

        except Exception as ex:
            logger.error(f"OFConfirm engine error: {ex}")


        # ------------------------------------------------------------
        # min_confirmations gate (hard vs soft)
        # По умолчанию fp_imb не увеличивает hard_count, иначе pass-rate станет выше.
        # ------------------------------------------------------------
        # ------------------------------------------------------------
        from core.footprint_policy import is_soft_confirmation # Ensure import or use existing
        
        if tick.get("mock_force"):
             self.logger.warning("TRACE 3: Approaching Gate Check")

        delta_abs = abs(delta_event.get("delta", 0.0))
        min_delta = runtime.config["delta_abs_min_confirm"]
        min_confirmations = int(runtime.config.get("min_confirmations", 0))
        
        fp_imb_counts = bool(runtime.config.get("fp_imb_counts_for_min_confirmations", False))
        if fp_imb_counts:
            hard_count = len(confirmations)
        else:
            hard_count = 0
            for c in confirmations:
                if is_soft_confirmation(c):
                    continue
                hard_count += 1

        if delta_abs < min_delta and hard_count < min_confirmations:
            # FORCE LOG for diagnostics
            logger.warning(
                "🛑 [MIN-CONF] (%s) Signal filtered: delta_abs=%.2f < %.2f AND hard_confirmations=%d < %d",
                runtime.symbol,
                delta_abs,
                min_delta,
                hard_count,
                min_confirmations,
            )
            return None

        # Deterministic now
        now_ms = int(tick_ts)

        signal_id = f"crypto-of:{runtime.symbol}:{now_ms}"
        primary_reason = "delta_spike"
        if confirmations:
            primary_reason = confirmations[0].split("=", 1)[0]

        # ------------------------------------------------------------------
        # 🚦 ATR FLOOR GATE (EARLY) — FIX "broken chain"
        #
        # Этот gate использует tier-by-regime порог atr_bps_th.
        # Источник atr_bps:
        #   - runtime.dynamic_cfg["atr_bps"] обновляется на bar_close (1s microbar).
        #   - Если нет данных (cold start) -> fail-open или static_min.
        #
        # Важно:
        #   - Это "ранний" грубый фильтр (до publish_signal).
        #   - Финальный unified gate (fees-aware) применяется позже в publish_signal.
        # ------------------------------------------------------------------
        try:
            rg = str(getattr(runtime, "last_regime", "na") or "na").lower()
            atr_bps = float(runtime.dynamic_cfg.get("atr_bps", 0.0) or 0.0)
            # read cached threshold if present
            atr_bps_th = float(runtime.dynamic_cfg.get("atr_bps_th", 0.0) or 0.0)
            if not (atr_bps_th > 0):
                # recompute from floors (safe)
                t0 = float(runtime.dynamic_cfg.get("atr_floor_t0_bps", runtime.config.get("atr_floor_t0_bps", 0.0)) or 0.0)
                t1 = float(runtime.dynamic_cfg.get("atr_floor_t1_bps", runtime.config.get("atr_floor_t1_bps", 0.0)) or 0.0)
                t2 = float(runtime.dynamic_cfg.get("atr_floor_t2_bps", runtime.config.get("atr_floor_t2_bps", 0.0)) or 0.0)
                pick = compute_atr_bps_threshold(regime=rg, cfg=runtime.config, t0=t0, t1=t1, t2=t2)
                atr_bps_th = float(pick.th_bps)
                indicators["atr_floor_tier"] = int(pick.tier)
                indicators["atr_floor_picked_bps"] = float(pick.picked_bps)
            indicators["atr_bps"] = float(atr_bps)
            indicators["atr_bps_th"] = float(atr_bps_th)
            indicators["atr_floor_rg"] = str(rg)

            audit_only = bool(int(runtime.config.get("atr_gate_audit_only", 0) or 0))
            if atr_bps_th > 0 and atr_bps > 0 and atr_bps < atr_bps_th:
                if audit_only:
                    indicators["atr_gate_veto_audit"] = 1
                else:
                    self.logger.warning("🛑 (%s) ATR floor VETO: atr_bps=%.2f < th=%.2f (rg=%s)", runtime.symbol, atr_bps, atr_bps_th, rg)
                    atr_gate_veto_total.labels(symbol=runtime.symbol, reason="low_atr_floor", mode="ENFORCE").inc()
                    if bool(int(os.getenv("DEBUG_VETO", "0"))):
                         self.logger.debug("🛑 VETO (atr_floor): %s atr_bps=%.2f th=%.2f", runtime.symbol, atr_bps, atr_bps_th)
                    return None
        except Exception:
            # fail-open
            pass

        # ------------------------------------------------------------
        # Phase E: OBI stability evidence (TTL + book health)
        # ------------------------------------------------------------
        # Populate indicators so scorer/Telegram can use stability duration + quality.
        # Fail-open: if no book evidence or TTL expired, do nothing.
        try:
            if int(indicators.get("book_health_ok", 1) or 1) == 1:
                obe = getattr(runtime, "last_obi_event", None)
                if isinstance(obe, dict):
                    ots = int(obe.get("ts_ms", 0) or 0)
                    ttl = int(runtime.config.get("obi_event_ttl_ms", 15000) or 15000)
                    if ots > 0 and 0 <= (now_ms - ots) <= ttl:
                        # raw OBI values
                        indicators["obi"] = float(obe.get("obi", indicators.get("obi", 0.0) or 0.0) or 0.0)
                        indicators["obi_z"] = float(obe.get("obi_z", 0.0) or 0.0)
                        # stability
                        indicators["obi_stable_secs"] = float(obe.get("stable_secs", 0.0) or 0.0)
                        # quality score may be missing (legacy); default 1.0 if duration present
                        q = obe.get("stability_score", None)
                        if q is None:
                            q = 1.0 if float(indicators.get("obi_stable_secs", 0.0) or 0.0) > 0 else 0.0
                        indicators["obi_stability_score"] = float(q)
                        indicators["obi_stable"] = int(obe.get("stable", 0) or 0)
        except Exception:
            pass

        # ------------------------------------------------------------
        # Phase E: CVD Reclaim (bonus-layer)
        # ------------------------------------------------------------
        # Add as SOFT confirmation after gates (won't affect min_confirmations).
        # Stored only when reclaim was confirmed.
        try:
            if int(runtime.config.get("cvd_reclaim_enable", 1) or 0) == 1:
                ev = getattr(runtime, "last_cvd_reclaim", None)
                if isinstance(ev, dict):
                    ets = int(ev.get("ts_ms", 0) or 0)
                    valid_ms = int(runtime.config.get("cvd_reclaim_valid_ms", 120000) or 120000)
                    if ets > 0 and 0 <= (now_ms - ets) <= valid_ms:
                        if str(ev.get("bias", "")).upper() == str(direction).upper():
                            indicators["cvd_reclaim_ok"] = int(ev.get("ok", 0) or 0)
                            indicators["cvd_reclaim_ratio"] = float(ev.get("ratio", 0.0) or 0.0)
                            indicators["cvd_reclaim_cvd_delta"] = float(ev.get("cvd_delta", 0.0) or 0.0)
                            indicators["cvd_reclaim_n"] = int(ev.get("n", 0) or 0)
                            indicators["cvd_reclaim_baseline"] = float(ev.get("baseline", 0.0) or 0.0)
                            if int(indicators.get("cvd_reclaim_ok", 0) or 0) == 1:
                                confirmations.append(f"cvdR={float(indicators.get('cvd_reclaim_ratio', 0.0) or 0.0):.2f}")
        except Exception:
            pass

        if tick.get("mock_force"):
             self.logger.warning("TRACE 5: Computing Confidence")

        confidence = self._compute_confidence(runtime, indicators, confirmations, side=direction, kind=primary_reason)
        indicators["confidence"] = confidence

        # Log the confidence for this signal
        # Log the confidence for this signal (sampled)
        if primary_reason == "weak_progress":
            if runtime.weak_signal_log_sampler.should_log("weak_progress"):
                self.logger.info("emit signal %s conf=%.1f%%", primary_reason, confidence * 100.0)
        else:
            # Log other signals sampled at 1/1000
            if runtime.signal_emit_log_sampler.should_log(primary_reason):
                self.logger.info("emit signal %s conf=%.1f%%", primary_reason, confidence * 100.0)

        # Фильтр по минимальной уверенности
        try:
            min_conf_pct = float(os.getenv("CRYPTO_SIGNAL_MIN_CONF", "70"))
        except Exception:
            min_conf_pct = 80.0

        # Override из symbol spec, если указано (signal_min_conf или min_conf)
        try:
            spec = get_symbol_info(runtime.symbol)
            if isinstance(spec, dict):
                spec_min_conf = spec.get("signal_min_conf", spec.get("min_conf"))
                if spec_min_conf is not None:
                    min_conf_pct = float(spec_min_conf)
        except Exception:
            pass

        min_conf = min_conf_pct / 100.0
        min_conf = min_conf_pct / 100.0

        if tick.get("mock_force"):
             self.logger.warning("TRACE 6: Confidence Check. conf=%f min=%f", confidence, min_conf)

        # Strict confidence filter
        if confidence < min_conf:
             logger.warning(
                 "🛑 [LOW-CONF] (%s) Signal filtered: conf=%.2f%% < min_conf=%.2f%%. (x%d)",
                 runtime.symbol, confidence * 100.0, min_conf_pct, self.low_conf_counters.get(runtime.symbol, 0)
             )
             return None

        runtime.signal_count += 1
        
        # Initialize payload early for candidate/pressure enrichment
        payload = {
            "symbol": runtime.symbol,
            "ts_ms": int(tick_ts),
            "tick_ts": int(tick_ts),
            "price": float(price),
            "entry": float(price),
            "direction": direction,
            "side": direction.lower(),
            "indicators": indicators,
            "confirmations": list(confirmations),
            "confidence": float(confidence),
            "signal_id": str(signal_id),
        }
        
        self._log_metrics(runtime)


        # === Pressure snapshot attached to every candidate payload ===
        try:
            ps = runtime.pressure.snapshot(now_ms=int(tick_ts))
            payload["pressure"] = {
                "per_min_ema": float(ps.per_min_ema),
                "cd_rate_ema": float(ps.cd_rate_ema),
                "n_raw": int(ps.n_raw),
                "n_cd": int(ps.n_cd),
            }
            hi_th = float(runtime.config.get("pressure_hi_per_min", 60.0))
            payload["pressure"]["pressure_hi"] = 1 if ps.per_min_ema >= hi_th else 0
        except Exception:
            pass

        # Attach microstructure context (from last book/bar)
        try:
            payload.setdefault("micro", {})
            payload["micro"]["spread_bps"] = float(getattr(runtime, "last_spread_bps", 0.0) or 0.0)
            payload["micro"]["spread_z"] = float(getattr(runtime, "last_spread_z", 0.0) or 0.0)
            # book freshness/rate
            bts = int(getattr(runtime, "last_book_ts_ms", 0) or 0)
            book_stale_ms = int(tick_ts - bts) if (bts > 0 and tick_ts > 0 and tick_ts >= bts) else int(10**9)
            payload["micro"]["book_stale_ms"] = int(book_stale_ms)
            payload["micro"]["book_rate_ema"] = float(getattr(runtime, "book_rate_ema", 0.0) or 0.0)
            payload["micro"]["book_rate_z"] = float(getattr(runtime, "book_rate_z", 0.0) or 0.0)
            payload["micro"]["book_churn_score"] = float(getattr(runtime, "book_churn_score", 0.0) or 0.0)
            payload["micro"]["book_churn_hi"] = int(getattr(runtime, "book_churn_hi", 0) or 0)
            if book_stale_ms_gauge is not None:
                book_stale_ms_gauge.labels(symbol=runtime.symbol).set(float(book_stale_ms))
        except Exception:
            pass

        if runtime.last_book:
            payload["book_ts"] = runtime.last_book.get("ts")
            bids = runtime.last_book.get("bids") or []
            asks = runtime.last_book.get("asks") or []
            if bids:
                payload["best_bid"] = bids[0][0]
            if asks:
                payload["best_ask"] = asks[0][0]

        # --- Cooldown (deterministic) check BEFORE Burst/Emission ---
        scenario = str(indicators.get("strong_gate_scn", "") or "")
        if not scenario:
            # fallback: if sweep_recent => reversal else continuation
            scenario = "reversal" if int(indicators.get("sweep", 0) or 0) == 1 else "continuation"
            
        cooldown_ms = _cooldown_ms_for(runtime, scenario=scenario, now_ms=tick_ts)
        last_emit_ts = int(getattr(runtime, "last_signal_ts", 0) or 0)
        age = int(tick_ts) - last_emit_ts if last_emit_ts > 0 else 10**9

        # define score for candidate selection (always)
        score = float(getattr(ofc, "score", 0.0) or 0.0) if ofc is not None else 0.0
        if score <= 0:
            score = float(confidence)

        if age < cooldown_ms:
            # --- Pressure Proxy: record deterministic cooldown hit ---
            try:
                runtime.pressure.on_cooldown_hit(ts_ms=int(tick_ts))
                ps = runtime.pressure.snapshot(now_ms=int(tick_ts))
                indicators["pressure_per_min_ema"] = float(ps.per_min_ema)
                indicators["cooldown_hit_rate_ema"] = float(ps.cd_rate_ema)
            except Exception:
                pass

            # Buffer into pending_payload for post-cooldown emission
            cand_score = float(score)
            if runtime.pending_payload is None or cand_score > float(getattr(runtime, "pending_score", 0.0) or 0.0):
                runtime.pending_payload = payload
                runtime.pending_score = float(cand_score)
                runtime.pending_ts_ms = int(tick_ts)
                runtime.pending_replaced += 1
            
            logger.warning(
                "🛑 [COOLDOWN] (%s) Signal buffered (age=%dms < %dms). Pending updated=%s",
                runtime.symbol, age, cooldown_ms, "YES"
            )
            return None

        # Cooldown window open: check if we have better pending
        if runtime.pending_payload is not None:
            pending_score = float(getattr(runtime, "pending_score", 0.0) or 0.0)
            cur_score = float(score)
            if pending_score >= cur_score:
                payload = runtime.pending_payload
                cand_score = pending_score
            runtime.pending_payload = None
            runtime.pending_score = 0.0


        # Burst Mode Check (Consolidated)
        force_burst = bool(indicators.get("pressure_extreme_flag", 0))
        use_burst = bool(int(runtime.config.get("burst_enable", 1))) or force_burst
        
        # DEBUG: Log that signal passed all filters and is about to enter burst
        logger.info(
            "✅ [PRE-BURST] (%s) Signal passed all filters: dir=%s conf=%.1f%% score=%.2f delta_z=%.2f",
            runtime.symbol, direction, confidence*100, score, delta_event.get("z", 0.0)
        )
        
        if use_burst:
            try:
                with runtime.burst_mu:
                    was_active = runtime.burst.st.active
                    runtime.burst.consider(
                        ts_ms=int(tick_ts),
                        cand=BurstCandidate(ts_ms=int(tick_ts), score=float(score), payload=payload),
                    )
                    # DEBUG: Log when burst starts or candidate is added
                    if runtime.burst.st.active:
                        if not was_active:
                            logger.info(
                                "🚀 [BURST-START] (%s) Started burst: ts=%d deadline=%d window=%dms score=%.2f dir=%s",
                                runtime.symbol, tick_ts, runtime.burst.st.deadline_ts_ms, 
                                runtime.burst.window_ms, score, payload.get("direction")
                            )
                        else:
                            logger.debug(
                                "📊 [BURST-ADD] (%s) Added candidate: ts=%d score=%.2f (best=%.2f)",
                                runtime.symbol, tick_ts, score, 
                                runtime.burst.st.best.score if runtime.burst.st.best else 0.0
                            )
                    burst_active_gauge.labels(symbol=runtime.symbol).set(1 if runtime.burst.st.active else 0)
                # Do not emit now; we will flush at deadline.
                return None
            except Exception:
                runtime.last_signal_ts = int(tick_ts)
                runtime.pressure.record_emit(int(tick_ts))
                return payload


        # No burst: emit immediately
        runtime.last_signal_ts = int(tick_ts)
        runtime.pressure.record_emit(int(tick_ts))
        return payload

