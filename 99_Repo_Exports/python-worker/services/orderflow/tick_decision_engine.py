import asyncio
import contextlib
import hashlib
import json
import logging
import math
import os
import time
from typing import Any

from common.normalization import generate_signal_id, normalize_direction
from common.time_utils import normalize_epoch_ms_v2
from core.atr_floor_policy import compute_atr_bps_threshold
from core.cvd_reclaim import compute_cvd_reclaim
from core.data_health import compute_data_health, apply_book_evidence_policy, apply_shadow_only_policy
from core.dyn_cfg_keys import DynCfgKeys as DK
from core.exec_regime_bucket_v1 import compute_exec_regime_bucket
from core.footprint_policy import is_soft_confirmation, fp_confirmations_from_microbar
from core.instrument_config import get_default_delta_tiers, symbol_env_prefix
from core.microbar import MicroBar
from core.of_inputs_contract import OFInputsV1, OFInputsV2
from services.orderflow.of_inputs_v3_circuit import refresh_disabled_state
from core.redis_keys import RedisStreams as RS
from core.strong_of_gate import hidden_trend_dir
from core.weak_progress import compute_weak_progress
from core.slippage_model import expected_slippage_bps
from core.expected_slippage_decomp_v1 import expected_slippage_decomp_bps
from domain.evidence_keys import MetaKeys
from handlers.crypto_orderflow.utils.log_sampler import sampled_info, sampled_warning
from services.orderflow.configuration import _ensure_list_levels, _safe_float, _safe_int, _to_bool
from services.orderflow.log_sampler import sampled_debug
from services.orderflow.metrics import *
from services.orderflow.of_gate_metrics_contract import enrich_schema_fields
from services.orderflow.runtime import SymbolRuntime
from services.orderflow.utils import LogSamplerFactory, _calc_pressure_sps, _should_sample, hour_of_week_utc, session_utc
from services.orderflow.utils import _normalize_epoch_ms as normalize_epoch_ms
from services.signal_preprocess import preprocess_signal_for_publish
from services.tp_config import parse_tp_ratio
from utils.task_manager import safe_create_task
from utils.time_utils import get_ny_time_millis

logger = logging.getLogger("crypto_tick_engine")

# ── OrderFlow Gate Metrics Constants ──────────────────────────────────────────
OF_GATE_METRICS_STREAM = os.getenv("OF_GATE_METRICS_STREAM", RS.OF_GATE_METRICS)
OF_GATE_METRICS_ENABLE = os.getenv("OF_GATE_METRICS_ENABLE", "1").strip().lower() in ("1", "true", "yes", "on")
OF_GATE_METRICS_SAMPLE = float(os.getenv("OF_GATE_METRICS_SAMPLE", "0.10") or 0.10)
OF_GATE_METRICS_MAXLEN = int(os.getenv("OF_GATE_METRICS_MAXLEN", "200000") or 200000)
OF_GATE_METRICS_QUARANTINE_STREAM = os.getenv("OF_GATE_METRICS_QUARANTINE_STREAM", RS.OF_GATE_METRICS_QUARANTINE)
OF_GATE_METRICS_QUARANTINE_ENABLE = os.getenv("OF_GATE_METRICS_QUARANTINE_ENABLE", "1").strip().lower() in ("1", "true", "yes", "on")
OF_GATE_METRICS_QUARANTINE_MAXLEN = int(os.getenv("OF_GATE_METRICS_QUARANTINE_MAXLEN", "200000") or 200000)
OF_GATE_METRICS_SAMPLE_SALT = os.getenv("OF_GATE_METRICS_SAMPLE_SALT", "").strip()
OF_GATE_METRICS_SAMPLE_KEY_MODE = "symbol_ts_v1"

# ── Fail-open Defaults ────────────────────────────────────────────────────────
DATA_HEALTH_ON_SPREAD_MISSING = float(os.getenv("DATA_HEALTH_ON_SPREAD_MISSING", "0.5") or 0.5)
DEBUG_DELTAS = os.getenv("DEBUG_DELTAS", "0") == "1"
SLIPPAGE_BPS_MISSING_DEFAULT = float(os.getenv("SLIPPAGE_BPS_MISSING_DEFAULT", "2.0") or 2.0)
SPREAD_BPS_MISSING_DEFAULT = float(os.getenv("SPREAD_BPS_MISSING_DEFAULT", "15.0") or 15.0)

def _stable_hash01(s: str) -> float:
    try:
        h = hashlib.sha256(s.encode("utf-8")).digest()
        v = int.from_bytes(h[:8], byteorder="big", signed=False)
        return v / float((1 << 64) - 1)
    except Exception:
        return 0.0

def _sample_uid_symbol_ts(symbol: str, ts_ms: int) -> int:
    """
    Sampling-invariant key: make sampling stable AND de-correlated across symbols.
    Important: key must NOT depend on ok/ok_soft to avoid bias.
    """
    b = f"{OF_GATE_METRICS_SAMPLE_SALT}|{symbol}|{ts_ms}".encode("utf-8", errors="replace")
    h = hashlib.sha1(b).digest()
    return int.from_bytes(h[:8], byteorder="big", signed=False)

def _ml_should_enforce(rollout_mode: str, sid: str, canary_rate: float) -> bool:
    m = (rollout_mode or "shadow").strip().lower()
    if m in ("off", "disabled", "0", "false", "none"):
        return False
    if m in ("full", "enforce", "on", "1", "true"):
        return True
    if m in ("canary", "canary_enforce", "canary-only"):
        r = max(0.0, min(1.0, canary_rate))
        return _stable_hash01(f"{sid}|p61") < r
    return False

class TickDecisionEngine:
    def __init__(self, facade: Any):
        self.facade = facade
        self.dn_gate_relaxed_counters: dict[str, int] = {}
        self.dn_gate_proxy_relaxed_counters: dict[str, int] = {}
        self.strong_gate_counters: dict[str, int] = {}
        self.conf_relax_counters: dict[str, int] = {}
        self.adverse_continuation_counters: dict[str, int] = {}
        self.swing_point_counters: dict[str, int] = {}

    @property
    def _env(self): return self.facade._env
    @property
    def logger(self): return self.facade.logger
    @property
    def redis(self): return self.facade.redis
    @property
    def cg_reader(self): return self.facade.cg_reader
    @property
    def market_state(self): return self.facade.market_state
    @property
    def signal_pipeline(self): return self.facade.signal_pipeline
    @property
    def _atr_sanity(self): return self.facade._atr_sanity
    @property
    def of_engine(self): return self.facade.of_engine
    @property
    def ticks(self): return self.facade.ticks
    @property
    def calib_svc(self): return getattr(self.facade, "calib_svc", None)
    @property
    def atr_cache(self): return getattr(self.facade, "atr_cache", None)
    @property
    def cg_macro_gate(self): return self.facade.cg_macro_gate
    @property
    def conf_cal_gating_mode(self): return getattr(self.facade, "conf_cal_gating_mode", "raw")
    @property
    def conf_cal_runtime(self): return getattr(self.facade, "conf_cal_runtime", None)
    @property
    def conf_cal_proof(self): return getattr(self.facade, "conf_cal_proof", None)
    @conf_cal_proof.setter
    def conf_cal_proof(self, v): self.facade.conf_cal_proof = v

    def _ensure_proof_state(self, now_ms: int):
        """Reload proof state from file if mtime changed."""
        path = getattr(self.facade, "conf_cal_proof_path", "")
        if not path or not os.path.exists(path):
            return

        try:
            mtime = os.path.getmtime(path)
            if mtime > getattr(self.facade, "conf_cal_proof_mtime", 0.0):
                with open(path, "r") as f:
                    self.facade.conf_cal_proof = json.load(f)
                self.facade.conf_cal_proof_mtime = mtime
                self.logger.info("Loaded confidence calibration proof from %s", path)
        except Exception as e:
            self.logger.error("Failed to load confidence calibration proof from %s: %s", path, e)

    async def publish_signal(self, runtime, signal: dict[str, Any]) -> None:
        await self.facade.publish_signal(runtime, signal)

    async def _emit_payload(self, runtime, payload, now_ms):
        return await self.facade._emit_payload(runtime, payload, now_ms)

    def _apply_confidence_calibration(self, runtime, indicators, conf_raw, ctx):
        return self.facade._apply_confidence_calibration(runtime, indicators, conf_raw, ctx)

    async def _compute_confidence(self, runtime, indicators, confirmations, *, side, kind, features=None):
        return await self.facade._compute_confidence(runtime, indicators, confirmations, side=side, kind=kind, features=features)

    def _get_atr_for_symbol(self, symbol, cfg, tf_override=None, runtime=None):  # type: ignore
        return self.facade._get_atr_for_symbol(symbol, cfg, tf_override, runtime)

    def _log_metrics(self, runtime):
        return self.facade._log_metrics(runtime)

    async def _maybe_poll_symbol_overrides(self, runtime, now_ms):  # type: ignore
        pass # To be migrated if needed

    async def _publish_smt_snapshot(self, runtime, bar):
        return await self.facade._publish_smt_snapshot(runtime, bar)

    async def process_tick(self, runtime: SymbolRuntime, tick: dict[str, Any]) -> dict[str, Any] | None:  # type: ignore
            # Lazy ENV refresh (every 30s, cheap monotonic check)
            self._env.maybe_refresh()

            # ------------------------------------------------------------------
            # Circuit Breaker Synchronization (P100)
            # ------------------------------------------------------------------  # type: ignore
            try:
                # Throttled refresh (default 10s internally in refresh_disabled_state)
                # This ensures parity between TickDecisionEngine and SignalDispatcher
                now_ms = get_ny_time_millis()
                disabled, until_ms, reason = await refresh_disabled_state(self.redis, runtime, now_ms)
                
                # Update Prometheus telemetry (metrics imported via wildcard at module level)
                if of_inputs_v3_circuit_disabled:
                    of_inputs_v3_circuit_disabled.labels(symbol=runtime.symbol).set(1 if disabled else 0)
                if of_inputs_v3_circuit_disabled_until_ms:
                    of_inputs_v3_circuit_disabled_until_ms.labels(symbol=runtime.symbol).set(until_ms)
            except Exception as exc:
                # Fail-open for telemetry/refresh failures
                log_silent_error(exc, 'cb_refresh_failure', runtime.symbol, 'process_tick')
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

            # ------------------------------------------------------------------
            # Robust Time Normalization (Expert Recommendation 3, Patch 1)
            # ------------------------------------------------------------------
            if tick.get("mock_force"):
                 self.logger.warning("🔍 (%s) _handle_tick: START tick_ts=%s", runtime.symbol, tick.get("ts_ms"))
            tick_ts = int(
                tick.get("ts_ms")
                or tick.get("ts")
                or tick.get("event_time")
                or tick.get("E")
                or tick.get("T")
                or tick.get("time")
                or tick.get("written_at")
                or 0
            )
            # Only fallback if 0
            if tick_ts <= 0:
                from services.orderflow.metrics import tick_ts_missing_total
                if tick_ts_missing_total:
                    tick_ts_missing_total.labels(symbol=runtime.symbol).inc()
                return None

            indicators: dict[str, Any] = {}
            cfg = runtime.config or {}

            # ------------------------------------------------------------------
            # Data Quality: tick time health (deterministic)
            # ------------------------------------------------------------------
            try:
                if tick_ts <= 0:
                    indicators["tick_ts_missing"] = 1
                else:
                    prev = (getattr(runtime, "last_ts_ms", 0) or 0)
                    if prev > 0 and tick_ts < prev:
                        indicators["tick_oood"] = 1
                    if prev > 0 and tick_ts > prev:
                        gap = tick_ts - prev
                        if gap >= _safe_int(cfg.get("tick_gap_warn_ms", 2000), 2000):
                            indicators["tick_gap_ms"] = gap
            except Exception:
                pass

            # Monotonicity check (Expert Recommendation 3: detect -> sanitize -> quarantine)
            MAX_BACK_MS = self._env.time_max_back_ms
            WARN_BACK_MS = self._env.time_warn_back_ms
            prev_ts = (getattr(runtime, "last_ts_ms", 0) or 0)

            if prev_ts > 0 and tick_ts < prev_ts:
                # backward time
                back = prev_ts - tick_ts
                if tick_ts_backwards_total:
                    tick_ts_backwards_total.labels(symbol=runtime.symbol).inc()

                if back <= MAX_BACK_MS:
                    # sanitize: clamp slightly forward to keep deterministic monotonicity
                    tick_ts = prev_ts + 1
                    if tick_ts_clamped_total:
                         tick_ts_clamped_total.labels(symbol=runtime.symbol).inc()

                    # Observability: mark degraded quality + alert-ish metric
                    indicators["tick_quality"] = "low"
                    indicators["tick_ts_back_ms"] = back
                    if back > WARN_BACK_MS:
                        if ticks_out_of_order_total:
                            with contextlib.suppress(Exception):
                                ticks_out_of_order_total.labels(symbol=runtime.symbol).inc()
                        # Optional: sampled warning
                        sampled_warning(
                            self.logger, "TIME_SKEW_DETECTED",
                            "⚠️ Time skew detected for %s: back_ms=%d (clamped)",
                            runtime.symbol, back
                        )
                else:
                    # quarantine: too large rollback — fail-closed
                    if tick_ts_quarantined_total:
                         tick_ts_quarantined_total.labels(symbol=runtime.symbol).inc()
                    return None



            runtime.last_ts_ms = tick_ts
            sess = session_utc(tick_ts)
            how = hour_of_week_utc(tick_ts)
            indicators["session"] = sess
            indicators["hour_of_week"] = how

            # ------------------------------------------------------------------
            # Source consistency guard (dual-source / CVD jump)
            # - detects implausible delta jumps and marks source_consistency_ok=0
            # - consumer policy: turn book evidences off, optionally shadow-only
            # ------------------------------------------------------------------
            try:
                px = _safe_float(tick.get("price") or 0.0, 0.0)
                cvd = (getattr(runtime, "cvd_last", 0.0) or 0.0)
                cvd_prev = (getattr(runtime, "cvd_prev", cvd) or cvd)
                # compute jump in USD
                jump_usd = 0.0
                if px > 0:
                    jump_usd = abs(cvd - cvd_prev) * px
                # thresholds: default high to avoid false triggers
                j_usd_th = _safe_float(cfg.get("source_jump_usd_th", 50_000_000.0), 50_000_000.0)
                if jump_usd > j_usd_th:
                    indicators["source_consistency_ok"] = 0
                    indicators["source_jump_usd"] = jump_usd
                    # cool down period (ms) during which we keep it marked inconsistent
                    until = tick_ts + _safe_int(cfg.get("source_inconsistent_ttl_ms", 60_000), 60_000)
                    runtime.source_inconsistent_until_ms = until
                else:
                    until = (getattr(runtime, "source_inconsistent_until_ms", 0) or 0)
                    if until > tick_ts:
                        indicators["source_consistency_ok"] = 0
                    else:
                        indicators["source_consistency_ok"] = 1
                runtime.cvd_prev = cvd
                runtime.cvd_last = cvd
            except Exception:
                pass

            # Expert Recommendation 4: Track timestamp for Gap Cap
            lt_seen = (getattr(runtime, "last_tick_seen_ts", 0) or 0)
            if lt_seen > 0 and tick_ts > lt_seen:
                 gap = tick_ts - lt_seen
                 with contextlib.suppress(Exception):
                     runtime.tick_gaps_ms.append(gap)
            runtime.last_tick_seen_ts = tick_ts

            # Runtime overrides (cooldown/pressure tuning) — throttled, fail-open
            try:
                # Legacy override poll (cfg:crypto_of:overrides) - kept for compatibility
                safe_create_task(self._maybe_poll_symbol_overrides(runtime, tick_ts))

                # SRE Versioned Overrides V1 (High Priority)
                # self.redis is safe to use here? self.redis is async client.
                safe_create_task(runtime.maybe_load_overrides(self.redis))
                
                # Expert calibration loading: get pre-computed thresholds from Redis
                atr_cache = getattr(self, "atr_cache", None)
                if atr_cache is not None:
                    calib_map = atr_cache.get_candidates(runtime.symbol, now_ms=tick_ts)
            except Exception:
                pass

            # Initialize early
            confirmations: list[str] = []

            # Inject CoinGecko macro context into indicators (SHADOW mode by default)
            try:
                cg_ind = self.cg_reader.get_snapshot(runtime.symbol, now_ms=tick_ts)
                indicators.update(cg_ind)
            except Exception as exc:
                log_silent_error(exc, 'coingecko_snapshot_error', runtime.symbol, 'process_tick')

            # --- Apply Overrides V1 into local cfg view (deterministic per tick best-effort) ---
            # We start with runtime.config (base)
            cfg = runtime.config
            try:
                o = getattr(runtime, "overrides_obj", None)
                if o is not None and (getattr(o, "enabled", 0) or 0) == 1:
                    # Canary decision:
                    #  - if canary_symbols defined -> apply only if symbol is listed
                    #  - else apply by deterministic hash-share (optional)
                    ro = getattr(o, "rollout", None)
                    apply_ovr = True
                    if ro is not None and str(getattr(ro, "mode", "full") or "full").lower() == "canary":
                        syms = set([str(x).upper() for x in (getattr(ro, "canary_symbols", []) or []) if x])
                        if syms:
                            apply_ovr = ((runtime.symbol or "").upper() in syms)
                        else:
                            # Fallback to share logic?
                            # Implement deterministic hash share if share < 1.0 (optional)
                            pass

                    if apply_ovr:
                        cfg = o.apply_to_cfg(cfg)
                        indicators["policy_sid"] = str(getattr(runtime, "overrides_sid", "") or "")
                        indicators["policy_src"] = "overrides_v1"
            except Exception:
                cfg = runtime.config

            # Book health: check gaps and staleness
            book_ts_base = (getattr(runtime, "last_book_ts_ms", 0) or 0)
            book_gap = (tick_ts - book_ts_base) if book_ts_base > 0 else 0
            book_stale_ms = int(runtime.config.get("book_stale_ms", 15000))
            book_ok = 1 if (book_ts_base > 0 and book_gap < book_stale_ms) else 0
            indicators["book_health_ok"] = book_ok
            indicators["book_ts_gap_ms"] = book_gap

            # ------------------------------------------------------------
            # Liquidity regime snapshot (risk overlay)
            # ------------------------------------------------------------
            try:
                snap = getattr(runtime, "last_book", None)
                spread_bps = (getattr(runtime, "last_spread_bps", 0.0) or 0.0)
                depth_usd_min_5 = 0.0
                if snap is not None:
                    # prefer snapshot spread if available
                    with contextlib.suppress(Exception):
                        spread_bps = (getattr(snap, "spread_bps", spread_bps) or spread_bps)
                    try:
                        bb = (getattr(snap, "best_bid_px", 0.0) or 0.0)
                        ba = (getattr(snap, "best_ask_px", 0.0) or 0.0)
                        mid = (bb + ba) / 2.0 if (bb > 0 and ba > 0) else 0.0
                        depth_qty = float(min(getattr(snap, "depth_5_bid_vol", 0.0) or 0.0,
                                              getattr(snap, "depth_5_ask_vol", 0.0) or 0.0))
                        depth_usd_min_5 = (depth_qty * max(mid, 1e-9)) if mid > 0 else 0.0
                    except Exception:
                        depth_usd_min_5 = 0.0

                stale = book_gap if book_ts_base > 0 else 10**9
                liq = runtime.liq_service.score(
                    symbol=runtime.symbol,
                    ts_ms=tick_ts,
                    spread_bps=spread_bps,
                    depth_usd_min_5=depth_usd_min_5,
                    book_rate_ema_hz=getattr(runtime, "book_rate_ema", 0.0) or 0.0,
                    book_stale_ms=stale,
                )
                runtime.liq_score = liq.liq_score
                runtime.liq_regime = liq.liq_regime
                runtime.last_liq = liq.to_dict()

                indicators["liq_score"] = liq.liq_score
                indicators["liq_regime"] = liq.liq_regime
                indicators["liq_depth_usd_min_5"] = liq.depth_usd_min_5
                indicators["liq_spread_bps"] = liq.spread_bps
                indicators["liq_book_rate_hz"] = liq.book_rate_ema_hz
                indicators["liq_book_stale_ms"] = liq.book_stale_ms
                if liq.why:
                    indicators["liq_why"] = liq.why
            except Exception:
                pass

            # Track tick gaps (Section 5: Burst Calibrator)
            with contextlib.suppress(Exception):
                runtime.tick_gaps.record(tick_ts)

            # Periodic calibration (every 200 ticks)
            if runtime.tick_count % 200 == 0:
                try:
                    # Update window/max_age only if burst is not currently active
                    # using the lock for safety although st.active check is usually okay
                    async with runtime.burst_mu:
                        is_active = getattr(runtime.burst.st, "active", False)
                        if not is_active:
                            gaps = runtime.tick_gaps.snapshot()
                            p_snap = runtime.pressure.snapshot(now_ms=tick_ts)

                            w, ma = runtime.burst_cal.compute(
                                gap_p50_ms=gaps.get("p50", 0.0),
                                cand_per_min=p_snap.per_min_ema
                            )
                            runtime.burst.window_ms = w
                            runtime.burst.max_age_ms = ma

                            # Metrics visibility
                            burst_window_ms_gauge.labels(symbol=runtime.symbol).set(w)
                            tick_gap_p50_ms_gauge.labels(symbol=runtime.symbol).set(gaps.get("p50", 0.0))
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
                    runtime.delta_triggers,
                )
                runtime.heartbeat_counter = 0

            # Check side classification
            s = (tick.get("side") or "").upper()
            if s not in ("BUY", "SELL"):
                 ticks_side_unknown_total.labels(symbol=runtime.symbol).inc()

            # Tick-CVD update (Phase A) BEFORE delta_detector.push()

            try:
                if runtime.cvd_state:
                    # Track previous CVD for consistency check
                    prev_cvd = (getattr(runtime.cvd_state, "cvd_tick", 0.0) or 0.0)
                    runtime.cvd_state.update(tick)
                    cvd_now = (getattr(runtime.cvd_state, "cvd_tick", 0.0) or 0.0)

                    # Compute delta_usd for CVD consistency guard
                    # delta_usd = delta_qty * price (approximate)
                    px = float(tick.get("price") or 0.0)
                    delta_qty = (getattr(runtime.cvd_state, "last_delta_tick", 0.0) or 0.0)
                    delta_usd = abs(delta_qty * px) if (px > 0 and delta_qty != 0) else 0.0

                    # CVD consistency guard (quarantine on jumps)
                    cvd_guard = getattr(runtime, "_cvd_guard", None)
                    if cvd_guard is None:
                        from core.cvd_consistency import CVDConsistencyGuard
                        cvd_guard = CVDConsistencyGuard()
                        runtime._cvd_guard = cvd_guard

                    ts_ms = int(tick.get("ts", 0) or 0)
                    dec = cvd_guard.update(
                        sym=runtime.symbol,
                        ts_ms=ts_ms,
                        cvd_now=cvd_now,
                        delta_usd=delta_usd
                    )
                    if dec.quarantine_active:
                        runtime.cvd_quarantine_active = 1
                        runtime.cvd_quarantine_until_ms = dec.quarantine_until_ms
                        runtime.delta_fallback_mode = "volume"
                        # IMPORTANT: disable CVD-derived deltas/divergences
                        # 1) don't update cvd-based slope/divergence features
                        # 2) compute delta_usd from volume-based aggregation (buy_qty - sell_qty) * mid
                        # (exact computation depends on your tick payload/aggregation)
                    else:
                        runtime.cvd_quarantine_active = 0
                        runtime.delta_fallback_mode = "cvd"
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
                                    if (runtime.symbol == "ETHUSDT" or "PEPE" in runtime.symbol):
                                         self.logger.warning("📊 [DEBUG-SPREAD] (%s) CALC: spr=%.8f mid=%.8f -> bps=%.4f",
                                                             runtime.symbol, spr, mid, spread_bps)
                                    runtime.last_spread_bps = spread_bps
                                    runtime.spread_stats.update(spread_bps)
                                    runtime.last_spread_z = runtime.spread_stats.z(spread_bps)
                            except Exception:
                                pass

                            # Fire async microbar closed handler
                            with contextlib.suppress(Exception):
                                safe_create_task(self._on_microbar_closed(runtime, b))
            except Exception:
                pass

            # --- L3-lite (Reconciliation metrics) ---
            try:
                # 1. Feed trade
                runtime.l3_queue.on_trade(
                    side=1 if (str(tick.get("side") or "").upper() == "BUY") else -1,
                    qty=float(tick.get("qty") or 0.0)
                )

                # 2. Check bucket advancement
                bucket_ms = runtime.l3_queue.bucket_ms or 1000
                cur_bucket_id = tick_ts // bucket_ms
                if runtime._last_l3_bucket_id is None:
                    runtime._last_l3_bucket_id = cur_bucket_id
                elif cur_bucket_id > runtime._last_l3_bucket_id:
                    # advance bucket and store stats
                    runtime.l3_stats = runtime.l3_queue.on_bucket_advance(bucket_id=runtime._last_l3_bucket_id)
                    # --- Hawkes-like online intensities (burst features) ---
                    # Uses EMA rates from runtime.l3_stats (updated on bucket advance). Cheap O(1) recursion.
                    try:
                        if runtime.l3_stats:
                            from core.hawkes_like_intensity import update_hawkes_like

                            hs = getattr(runtime, "hawkes_state", None)
                            if not isinstance(hs, dict):
                                hs = {}

                            t_now = tick_ts
                            prev_ts = int(hs.get("ts_ms", t_now))
                            dt_s = max(0.0, (t_now - prev_ts) / 1000.0)

                            st, snap = update_hawkes_like(
                                hs,
                                now_ts_ms=t_now,
                                dt_s=dt_s,
                                rates={
                                    "taker_buy_rate": float(getattr(runtime.l3_stats, "taker_buy_rate_ema", 0.0) or 0.0),
                                    "taker_sell_rate": float(getattr(runtime.l3_stats, "taker_sell_rate_ema", 0.0) or 0.0),
                                    "cancel_bid_rate": float(getattr(runtime.l3_stats, "cancel_bid_rate_ema", 0.0) or 0.0),
                                    "cancel_ask_rate": float(getattr(runtime.l3_stats, "cancel_ask_rate_ema", 0.0) or 0.0),
                                    "limit_add_rate": float(
                                        (getattr(runtime.l3_stats, "added_bid_rate_ema", 0.0) or 0.0)
                                        + (getattr(runtime.l3_stats, "added_ask_rate_ema", 0.0) or 0.0)
                                    ),
                                },
                                cfg=getattr(runtime, "config", {}) or {},
                            )
                            runtime.hawkes_state = st
                            runtime.hawkes_snapshot = snap
                    except Exception:
                        # Fail-open: Hawkes is a feature-only signal for now
                        pass
                    runtime._last_l3_bucket_id = cur_bucket_id
            except Exception:
                pass

            delta_event = runtime.delta_detector.push(tick)
            if delta_event:
                 # DEBUG: Confirm event creation immediately (every 10000th)
                 sampled_info(logger, "DELTA_EVENT", "🔍 [DELTA-EVENT] (%s) Event created: delta=%.2f z=%.2f", runtime.symbol, delta_event.get("delta", 0.0), delta_event.get("z", 0.0))
            price = _safe_float(tick.get("price")) or _safe_float(tick.get("last")) or _safe_float(tick.get("mid"))
            if price <= 0:
                return None

            # ------------------------------------------------------------
            # Publish last price (for ATR selector / diagnostics)
            # ------------------------------------------------------------
            try:
                if price > 0:
                    sym = str(getattr(runtime, "symbol", "") or "")
                    if sym:
                        ttl = self._env.last_px_ttl_sec
                        now_ms = get_ny_time_millis()
                        # Use async Redis operations
                        safe_create_task(self.redis.set(f"cfg:last_px:{sym}", str(price), ex=ttl))
                        safe_create_task(self.redis.set(f"cfg:last_px_ts_ms:{sym}", str(now_ms), ex=ttl))
            except Exception:
                pass

            # Pressure metric: raw triggers rate (pre-cooldown)
            try:
                if delta_event:
                    runtime.pressure.on_raw_trigger(ts_ms=tick_ts)
                ps = runtime.pressure.snapshot(now_ms=tick_ts)
                indicators["pressure_per_min_ema"] = ps.per_min_ema
                indicators["cooldown_hit_rate_ema"] = ps.cd_rate_ema
                runtime.pressure_sps = ps.per_min_ema / 60.0
            except Exception:
                pass

            # [REMOVED] Duplicate DN-PREFILTER-1 (Expert Check)
            # We rely on the second prefilter block (lines ~3200) which has the same logic but better context comments.


            # --- Prefilter: delta_notional_usd tiers (self-calibrating via dn_calib) ---
            # [REMOVED] Duplicate DN-PREFILTER-1 (Expert Check)
            # We rely on the second prefilter block (which has the same logic but better context comments).


            # Check against USD threshold if present
            if delta_event:
                delta_val = delta_event.get("delta", 0.0)
                delta_usd = abs(delta_val) * price
                min_usd = float(runtime.config.get("delta_abs_min_usd", 0.0) or 0.0)

                if min_usd > 1.0 and delta_usd < min_usd:
                    # Vetoed by USD threshold — always drop, no virtual bypass
                    of_g1_veto_min_usd_total.labels(runtime.symbol).inc()
                    logger.info(
                        "🛑 [G1-MIN-USD] (%s) VETO: delta_usd=$%.2f < min=$%.2f",
                        runtime.symbol, delta_usd, min_usd
                    )
                    return None

            # BURST: tick-driven flush even without new candidates (ensure signals don't get stuck)
            try:
                if bool(int(runtime.config.get("burst_enable", 1))) and getattr(runtime.burst.st, "active", False):
                    # [OPT A] Strategy only considers, background loop handles flush.
                    # Remove sync maybe_flush() to prevent "phantom" emissions or double-publish.
                    pass
            except Exception:
                pass

            if not delta_event:
                self._log_metrics(runtime)
                return None

            # Trigger Event!
            runtime.delta_triggers += 1
            of_session_outcome_total.labels(runtime.symbol, sess, "trigger_delta").inc()

            # --- Pressure tracking: candidate attempts (deterministic by tick_ts) ---
            try:
                runtime.signal_attempt_ts_ms.append(tick_ts)
                psps = _calc_pressure_sps(list(runtime.signal_attempt_ts_ms), tick_ts, 60_000)
                # light smoothing (EMA)
                a = float(runtime.config.get("pressure_ema_alpha", 0.20))
                if a <= 0 or a > 1: a = 0.20
                runtime.pressure_sps = (1.0 - a) * (getattr(runtime, "pressure_sps", 0.0) or 0.0) + a * psps
                indicators["pressure_sps"] = runtime.pressure_sps
                # pressure_hi flag
                thr = float(runtime.config.get("pressure_hi_sps", 0.12))  # ~7.2 кандидатов/мин
                runtime.pressure_hi = 1 if runtime.pressure_sps >= thr else 0
                indicators["pressure_hi"] = runtime.pressure_hi
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
            direction_norm = normalize_direction("LONG" if delta_event["delta"] >= 0 else "SHORT")
            direction = direction_norm.value

            # ------------------------------------------------------------------
            # ATR floor veto (tier-by-regime) — FIX BROKEN CHAIN
            # ВАЖНО:
            #   - раньше читали atr_bps_th, но не выбирали tier -> th оставался 0.0
            #   - теперь выбираем tier прямо здесь (safety), используя runtime.dynamic_cfg + bootstrap.
            # Fail-open:
            #   - если чего-то не хватает -> не блокируем (как и было), но всё логируем в indicators.
            # ------------------------------------------------------------------
            try:
                from contexts import MARKET_REGIME_NA, normalize_regime_label
                from core.atr_floor_policy import compute_atr_bps_threshold
                rg_floor = normalize_regime_label(getattr(runtime, "last_regime", MARKET_REGIME_NA))

                # Read bootstrap values (fallback to config if dynamic absent)
                t0 = float(runtime.dynamic_cfg.get(DK.ATR_FLOOR_T0_BPS, runtime.config.get("atr_floor_t0_bps", 0.0)) or 0.0)
                t1 = float(runtime.dynamic_cfg.get(DK.ATR_FLOOR_T1_BPS, runtime.config.get("atr_floor_t1_bps", 0.0)) or 0.0)
                t2 = float(runtime.dynamic_cfg.get(DK.ATR_FLOOR_T2_BPS, runtime.config.get("atr_floor_t2_bps", 0.0)) or 0.0)

                atr_tier, _, atr_bps_th = compute_atr_bps_threshold(
                    regime=rg_floor,
                    cfg=runtime.config,
                    t0=t0,
                    t1=t1,
                    t2=t2,
                )

                indicators["atr_floor_tier"] = atr_tier
                indicators["atr_bps_th"] = atr_bps_th

                # validation TP1 > ATR * k_max
                # "TP1/SL могут рассчитываться с неправильным ATR multiplier"
                # Explicit gate against extreme targets driven by invalid configs
                k_max = float(runtime.config.get("tp1_atr_k_max", 3.0) or 3.0) # safety upper bound
                atr_val_floor = getattr(runtime, "last_atr", 0.0) or 0.0

                if atr_val_floor > 0 and price > 0:
                    atr_bps_current = 10000.0 * (atr_val_floor / price)
                    tp_mults_str = str(runtime.config.get("tp_atr_mults", "0.6,1.0,1.5"))
                    tp_mults = [float(x.strip()) for x in tp_mults_str.split(",") if x.strip()]
                    tp1_mult = tp_mults[0] if tp_mults else 0.6

                    tp1_bps = atr_bps_current * tp1_mult
                    indicators["pred_tp1_bps"] = tp1_bps

                    if tp1_bps > (atr_bps_current * k_max):
                        if runtime.delta_log_sampler.should_log("tp1_atr_veto"):
                            self.logger.warning(
                                "🛑 [ATR-GATE] (%s) VETO: TP1 target too large (tp1_bps=%.1f > %.1f). ATR multiplier %f > k_max %f.",
                                runtime.symbol, tp1_bps, atr_bps_current * k_max, tp1_mult, k_max
                            )
                        return None

            except Exception as e:
                log_silent_error(e, 'atr_floor_veto', runtime.symbol, '_handle_tick:atr_floor_veto')
            # ------------------------------------------------------------------
            # ------------------------------------------------------------------
            # Authoritative DeltaNotional Tier Gating (Expert Recommendation)
            # ------------------------------------------------------------------
            # P2: Use TickTrigger DN Calibrator (tick_dn_calib) instead of Bar DN.
            # "tick_dn_calib" tracks the distribution of delta spikes (events), not bar sums.
            # ------------------------------------------------------------------
            from contexts import MARKET_REGIME_NA, normalize_regime_label
            rg = normalize_regime_label(getattr(runtime, "last_regime", MARKET_REGIME_NA))
            dn_tiers_decision = runtime.tick_dn_calib.tiers(
                regime=rg,
                ts_ms=(tick_ts if tick_ts > 0 else get_ny_time_millis()), # Use TS for HoW scale lookup
                default_t0=float(runtime.config.get("dn_tier0_usd", 120000.0)),
                default_t1=float(runtime.config.get("dn_tier1_usd", 350000.0)),
                default_t2=float(runtime.config.get("dn_tier2_usd", 750000.0)),
            )

            # Publish decision tiers to canonical runtime.dynamic_cfg for transparency
            runtime.dynamic_cfg[DK.DN_TIER0_USD] = dn_tiers_decision.tier0_usd
            runtime.dynamic_cfg[DK.DN_TIER1_USD] = dn_tiers_decision.tier1_usd
            runtime.dynamic_cfg[DK.DN_TIER2_USD] = dn_tiers_decision.tier2_usd
            runtime.dynamic_cfg[DK.DN_SRC] = dn_tiers_decision.src

            # Determine current tick's tier
            delta_usd = abs(delta_event.get("delta", 0.0)) * price

            tier = 0
            if delta_usd > dn_tiers_decision.tier2_usd:
                 tier = 2
            elif delta_usd > dn_tiers_decision.tier1_usd:
                 tier = 1
            elif delta_usd > dn_tiers_decision.tier0_usd:
                 tier = 0
            else:
                 tier = -1 # Sub-tier0 (noise)

            # Gate Logic:
            # Check pass-rate telemetry (if we are in a high-noise regime/session)
            # dn_gate_passrate tracks EMA(pass) per tier/session.

            min_tier = int(runtime.config.get("delta_tier_min", 0))
            passed = (tier >= min_tier)

            # EXPERT RELAXATION (2026-01-30):
            # Meme coins (1000* etc) often have very tight p50 distributions that VETO too many
            # useful calibration signals. If we are at min_tier=0, we allow a 50% tolerance
            # below T0 to capture more "warm-up" trades for the report.
            if not passed and min_tier == 0 and tier == -1:
                # prefix = symbol_env_prefix(runtime.symbol) - using top-level import
                prefix = symbol_env_prefix(runtime.symbol)
                is_meme = prefix in ("PEPE", "SHIB", "DOGE", "BONK", "FLOKI", "WIF")
                if is_meme:
                    tol_usd = dn_tiers_decision.tier0_usd * 0.50
                    if delta_usd >= tol_usd:
                        passed = True
                        tier = 0
                        indicators["dn_gate_relaxed"] = 1
                        # Log every 10,000th message
                        cnt = self.dn_gate_relaxed_counters.get(runtime.symbol, 0) + 1
                        self.dn_gate_relaxed_counters[runtime.symbol] = cnt
                        if cnt % 10000 == 0:
                            logger.info("✅ [DN-GATE] (%s) RELAXED: delta_usd=$%.0f passed via 50%% tolerance (T0=$%.0f) (x%d)",
                                        runtime.symbol, delta_usd, dn_tiers_decision.tier0_usd, cnt)

            # Telemetry update
            sess = indicators.get("session", "OFF")
            runtime.dn_passrate.update(tier=tier, session=sess, passed=passed)

            # Metrics
            res = "pass" if passed else "veto"
            dn_gate_events_total.labels(symbol=runtime.symbol, tier=str(tier), session=sess, result=res).inc()

            # Enforce Veto
            if not passed:
                 # Log veto
                 if runtime.delta_log_sampler.should_log("dn_veto"):
                      logger.info(
                          "🛑 [DN-GATE] (%s) VETO: delta_usd=$%.0f < T%d=$%.0f (tier=%d < min=%d) src=%s session=%s",
                          runtime.symbol, delta_usd, min_tier,
                          getattr(dn_tiers_decision, f"tier{min_tier}_usd", 0.0),
                          tier, min_tier, dn_tiers_decision.src, sess
                      )
                 return None

            # Feed calibrator only with events that passed the gate (not noise)
            if delta_usd > 0:
                runtime.tick_dn_calib.update(regime=rg, dn_usd=delta_usd, ts_ms=tick_ts)

            # Add indicators
            indicators["dn_tier"] = tier
            indicators["dn_usd"] = delta_usd
            indicators["dn_t1_usd"] = dn_tiers_decision.tier1_usd
            indicators["dn_src"] = dn_tiers_decision.src

            # P2: Inject Liquidity Scale (Hour-of-Week) for Risk/Conf
            indicators["liquidity_scale"] = dn_tiers_decision.scale


            # Deterministic "now" (tick time preferred; wall-time fallback only if missing)
            now_ts = tick_ts if tick_ts > 0 else get_ny_time_millis()

            indicators.update({
                "delta": delta_event.get("delta", 0.0),
                "delta_z": delta_event.get("z", 0.0),
            })

            # Pre-calculate absorption once for all consumers (Variant A + OFConfirm)
            absorption_feat = None
            with contextlib.suppress(Exception):
                absorption_feat = runtime.absorption_detector.push(tick, runtime.last_book_raw, price)

            # ------------------------------------------------------------------
            # Variant A: Publish delta_spike event for decentralized OFConfirm service
            # ------------------------------------------------------------------
            try:
                spike_out = {
                    "type": "delta_spike",
                    "symbol": runtime.symbol,
                    "ts_ms": now_ts,
                    "price": price,
                    "direction": direction,
                    "direction_norm": direction, # internal standardized
                    "side_int": direction_norm.to_side_int(),  # P0: Numeric side
                    "delta": delta_event.get("delta", 0.0),
                    "delta_z": delta_event.get("z", 0.0),
                }
                # Optional: if we already have features from runtime
                # Optional: if we already have features from runtime
                if absorption_feat:
                    spike_out["absorption"] = absorption_feat

                # Enrich with OBI/Iceberg (if not stale)
                now_ms = tick_ts # EXPERT FIX: Use tick_ts instead of wall-time
                obi_ttl = int(runtime.config.get("obi_event_ttl_ms", 30000))
                if runtime.last_obi_event and (now_ms - runtime.last_obi_event.get("ts_ms", 0)) < obi_ttl:
                    spike_out["obi"] = runtime.last_obi_event

                ice_ttl = int(runtime.config.get("iceberg_event_ttl_ms", 15000))
                if runtime.last_iceberg_event and (now_ms - runtime.last_iceberg_event.get("ts_ms", 0)) < ice_ttl:
                    spike_out["iceberg"] = runtime.last_iceberg_event

                # Enrich with L3-lite stats
                if runtime.l3_stats:
                    _cb = runtime.l3_stats.cancel_bid_rate_ema
                    _ca = runtime.l3_stats.cancel_ask_rate_ema
                    _la = getattr(runtime.l3_stats, "limit_add_total_rate_ema", 0.0) or 0.0
                    _lp_den = _la + (_cb + _ca) + 1e-9
                    spike_out.update({
                        "cancel_bid_rate_ema": _cb,
                        "cancel_ask_rate_ema": _ca,
                        "taker_buy_rate_ema": runtime.l3_stats.taker_buy_rate_ema,
                        "taker_sell_rate_ema": runtime.l3_stats.taker_sell_rate_ema,

                        "limit_add_total_rate_ema": _la,
                        "limit_add_imb": getattr(runtime.l3_stats, "limit_add_imb", 0.0) or 0.0,

                        "vpin_tox_ema": getattr(runtime.l3_stats, "vpin_tox_ema", 0.0) or 0.0,
                        "vpin_tox_z": getattr(runtime.l3_stats, "vpin_tox_z", 0.0) or 0.0,

                        # Aliases for downstream experiments
                        "lambda_trade_buy": runtime.l3_stats.taker_buy_rate_ema,
                        "lambda_trade_sell": runtime.l3_stats.taker_sell_rate_ema,
                        "lambda_limit_add": _la,
                        "liquidity_pressure": (_la - (_cb + _ca)) / _lp_den,
                        "info_flow": getattr(runtime.l3_stats, "vpin_tox_ema", 0.0) or 0.0,
                    })

                safe_create_task(
                    self.redis.xadd(
                        RS.EVENTS_DELTA_SPIKE,
                        {"payload": json.dumps(spike_out, ensure_ascii=False)},
                        maxlen=20000,
                        approximate=True,
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
                        "microbar_tf_ms": b.tf_ms,
                        "microbar_start_ts": b.start_ts_ms,
                        "microbar_end_ts": b.end_ts_ms,
                        "microbar_open": b.open,
                        "microbar_high": b.high,
                        "microbar_low": b.low,
                        "microbar_close": b.close,
                        "microbar_vol": b.vol,
                        "microbar_delta_sum": b.delta_sum,
                        "microbar_cvd_close": b.cvd_close,
                        "microbar_vwap": b.vwap,
                        "microbar_mid": b.mid_last,
                        "microbar_spread": b.spread_last,
                        "microbar_ticks": b.tick_count,
                    })

                # RSI indicators (if available)
                if hasattr(runtime, "rsi_price") and runtime.rsi_price.value is not None:
                    indicators["rsi_price"] = runtime.rsi_price.value
                if hasattr(runtime, "rsi_cvd") and runtime.rsi_cvd.value is not None:
                    indicators["rsi_cvd"] = runtime.rsi_cvd.value

                # RSI Confirmation check
                rp = indicators.get("rsi_price", 50.0)
                rc = indicators.get("rsi_cvd", 50.0)
                if direction == "LONG" and rp > 50 and rc > 50 or direction == "SHORT" and rp < 50 and rc < 50:
                    confirmations.append("rsi_agree=1")

                if runtime.last_swing_high:
                    sh = runtime.last_swing_high
                    indicators.update({
                        "swing_high_ts": sh.ts_ms,
                        "swing_high_px": sh.price,
                        "swing_high_cvd": sh.cvd,
                    })
                if runtime.last_swing_low:
                    sl = runtime.last_swing_low
                    indicators.update({
                        "swing_low_ts": sl.ts_ms,
                        "swing_low_px": sl.price,
                        "swing_low_cvd": sl.cvd,
                    })
                if runtime.last_div:
                    dv = runtime.last_div
                    indicators.update({
                        "div_kind": dv.kind,
                        "div_ts": dv.ts_ms,
                        "div_strength": dv.strength,
                        "div_price_prev": dv.price_prev,
                        "div_price_curr": dv.price_curr,
                        "div_cvd_prev": dv.cvd_prev,
                        "div_cvd_curr": dv.cvd_curr,
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
                        if ev.direction_bias == "SHORT" and div.kind.startswith("bearish"):
                            div_match = True
                        if ev.direction_bias == "LONG" and div.kind.startswith("bullish"):
                            div_match = True
                    indicators["sweep_div_match"] = 1 if div_match else 0
                    if div_match: confirmations.append("div_match=1")

                b = runtime.last_bar
                if b is not None and getattr(b, "fp_enabled", False):
                    indicators.update({
                        "fp_bucket_px": getattr(b, "fp_bucket_px", 0.0) or 0.0,
                        "fp_max_imbalance": getattr(b, "fp_max_imbalance", 0.0) or 0.0,
                        "fp_absorb_score": getattr(b, "fp_absorb_score", 0.0) or 0.0,
                    })
                    fp_confs = fp_confirmations_from_microbar(b, direction, runtime.config)
                    for c in fp_confs:
                        confirmations.append(c)

                wp = runtime.last_wp
                if wp is not None:
                    indicators.update({"weak_range_atr": wp.range_atr, "weak_body_atr": wp.body_atr, "weak_eff": wp.eff})
            except Exception:
                pass

            # ------------------------------------------------------------------
            # Unified data_health score (0..1) + policies
            # ------------------------------------------------------------------
            try:
                # Ensure basic indicators for compute_data_health
                _last_book_ts_ms = getattr(runtime, "last_book_ts_ms", 0) or 0
                indicators["book_ts_gap_ms"] = (tick_ts - _last_book_ts_ms) if _last_book_ts_ms > 0 else 10**9
                indicators["book_rate_hz"] = getattr(runtime, "book_rate_ema", 0.0) or 0.0

                # Use most recent spread from book snapshot if MicroBar hasn't updated yet or ticks lack bid/ask
                spr = getattr(runtime, "last_spread_bps", 0.0) or 0.0
                if spr <= 0 and runtime.last_book:
                    spr = runtime.last_book.spread_bps
                indicators["spread_bps"] = spr

                if (runtime.symbol == "ETHUSDT" or "PEPE" in runtime.symbol):
                    # Sample every 10000th message to reduce log spam
                    spread_debug_sampler = LogSamplerFactory.get_sampler("DEBUG_SPREAD", 10000)
                    if spread_debug_sampler.should_log(f"spread_debug_{runtime.symbol}"):
                        self.logger.warning("📊 [DEBUG-SPREAD] (%s) FINAL INDICATOR: spread_bps=%.4f (src=%s)",
                                            runtime.symbol, indicators["spread_bps"],
                                            "microbar" if runtime.last_spread_bps > 0 else "l2_snap")

                dh = compute_data_health(indicators=indicators, cfg=cfg)
                indicators["data_health"] = dh.score
                indicators["data_health_reasons"] = ",".join(list(dh.reasons or [])[:5])
                indicators["book_health_ok"] = dh.book_health_ok
                apply_book_evidence_policy(indicators=indicators, dh=dh, cfg=cfg)
                # ENFORCE: bad data health → hard veto
                # We sync this with DQ_DATA_HEALTH_HARD_MIN (default 0.70)
                try:
                    v = cfg.get("data_health_veto_below")
                    if v is None:
                        v = cfg.get("data_health_shadow_only_below", 0.70)
                    veto_thr = float(v)
                except Exception:
                    veto_thr = 0.70

                is_unhealthy = 1 if dh.score < veto_thr else 0
                is_veto = is_unhealthy

                # P0: Canary 5% Veto for Data Health
                if is_veto == 1:
                    dh_mode = (cfg.get("data_health_veto_mode", os.getenv("DATA_HEALTH_VETO_MODE", "canary"))).lower()
                    if dh_mode in ("canary", "canary_enforce", "canary-only"):
                        dh_rate = float(cfg.get("data_health_canary_rate", os.getenv("DATA_HEALTH_VETO_CANARY_RATE", "0.05")))
                        sid_str = str(getattr(runtime, "last_sid", f"{runtime.symbol}_{tick_ts}"))
                        if not _ml_should_enforce(dh_mode, sid_str, dh_rate):
                            is_veto = 0
                    elif dh_mode == "shadow":
                        is_veto = 0

                    if is_veto == 1:
                        with contextlib.suppress(Exception):
                            g4_canary_veto_total.labels(symbol=runtime.symbol).inc()

                indicators["data_health_veto_active"] = is_veto
                # shadow-only uses its own threshold (data_health_shadow_only_below, default 0.40)
                # independent from veto_thr (default 0.70) — apply_shadow_only_policy owns this
                apply_shadow_only_policy(indicators=indicators, dh=dh, cfg=cfg)

                if dh.reasons:
                    indicators["data_health_veto_reason"] = ",".join(list(dh.reasons)[:5])
                    indicators["data_health_shadow_reason"] = indicators["data_health_veto_reason"]
            except Exception:
                pass

            # data_health ENFORCE gate: drop signal if data quality is below threshold
            if indicators.get("data_health_veto_active", 0) == 1:
                self.logger.warning(
                    "🛑 [DATA-HEALTH] (%s) ENFORCE VETO: score=%.2f reasons=%s — pipeline dropped",
                    runtime.symbol,
                    indicators.get("data_health", 0.0),
                    indicators.get("data_health_veto_reason", ""),
                )
                return None

            # ------------------------------------------------------------------
            # Expected slippage model (bps) for adverse selection filtering
            # ------------------------------------------------------------------
            # CRITICAL: avoid missing/zero slippage when model fails
            indicators.setdefault("expected_slippage_bps", 0.0)
            indicators.setdefault("slippage_reason", "na")

            # --- OFI impact proxy from best-level book changes (Cont et al.) ---
            # Produces: ofi_best_qty, ofi_best_norm, depth_top5_qty (best-effort)
            try:
                book = getattr(runtime, 'last_book', None)
                prev = getattr(runtime, '_ofi_prev_book', None)
                if book is not None:
                    def _get(obj, k, d=0.0):
                        try:
                            if obj is None: return float(d)
                            if isinstance(obj, dict):
                                v = obj.get(k)
                                return float(v) if v is not None else float(d)
                            v = getattr(obj, k, None)
                            return float(v) if v is not None else float(d)
                        except Exception:
                            return float(d)
                    # best bid/ask (supports BookSnapshot or dict)
                    bbp = _get(book, 'best_bid_px', 0.0)
                    bbq = _get(book, 'best_bid_qty', 0.0)
                    bap = _get(book, 'best_ask_px', 0.0)
                    baq = _get(book, 'best_ask_qty', 0.0)
                    p_bbp = _get(prev, 'best_bid_px', 0.0)
                    p_bbq = _get(prev, 'best_bid_qty', 0.0)
                    p_bap = _get(prev, 'best_ask_px', 0.0)
                    p_baq = _get(prev, 'best_ask_qty', 0.0)
                    # OFI formula (best-level, snapshot-based approximation)
                    ofi_bid = 0.0
                    if bbp > p_bbp and bbp > 0: ofi_bid = bbq
                    elif bbp < p_bbp and p_bbp > 0: ofi_bid = -p_bbq
                    elif bbp == p_bbp and bbp > 0: ofi_bid = (bbq - p_bbq)
                    ofi_ask = 0.0
                    if bap < p_bap and bap > 0: ofi_ask = -baq
                    elif bap > p_bap and p_bap > 0: ofi_ask = p_baq
                    elif bap == p_bap and bap > 0: ofi_ask = -(baq - p_baq)
                    ofi = ofi_bid + ofi_ask
                    # depth (qty) from top5 if available
                    d_b = _get(book, 'depth_5_bid_vol', 0.0)
                    d_a = _get(book, 'depth_5_ask_vol', 0.0)
                    depth = d_b + d_a
                    if depth <= 0:
                        try:
                            bids = book.get('bids') if isinstance(book, dict) else getattr(book, 'bids', None)
                            asks = book.get('asks') if isinstance(book, dict) else getattr(book, 'asks', None)
                            if bids: depth += sum(x[1] for x in bids[:5] if x and len(x)>=2)
                            if asks: depth += sum(x[1] for x in asks[:5] if x and len(x)>=2)
                        except Exception:
                            pass
                    norm = ofi / max(depth, 1e-9)
                    indicators['ofi_best_qty'] = ofi
                    indicators['depth_top5_qty'] = depth
                    indicators['ofi_best_norm'] = norm
                    runtime._ofi_prev_book = book
            except Exception:
                pass

            # --- ATR meta & sanity flags (fail-open trading; fail-closed evidence) ---
            # if you have atr_cache.get_with_meta() use it; otherwise keep your current atr read
            try:
                from utils.atr_cache import get_atr_cache
                atr_cache = get_atr_cache()
                atr_val, atr_meta = atr_cache.get_with_meta(symbol=runtime.symbol, timeframe=None)  # None => use cfg:atr_tf:{sym}
                if atr_val is not None and atr_val > 0:
                    indicators["atr"] = atr_val
                # Don't set indicators["atr"] if atr_val is None or <= 0 - let sanity check handle it
                indicators["atr_src"] = str(atr_meta.get("picked_src") or atr_meta.get("src") or "na")
                indicators["atr_tf"] = str(atr_meta.get("picked_tf") or atr_meta.get("tf") or "na")
                indicators["atr_age_ms"] = int(atr_meta.get("age_ms") or 0)
            except Exception:
                indicators.setdefault("atr_src", str(getattr(runtime, "atr_src", "na")))
                indicators.setdefault("atr_tf", str(getattr(runtime, "atr_tf", "na")))
                indicators.setdefault("atr_age_ms", int(getattr(runtime, "atr_age_ms", 0) or 0))

            # Full robust sanity + last-good fallback (fail-open for trading)
            try:
                if self._env.atr_sanity_enable:
                    px0 = float(price or indicators.get("price", 0.0) or 0.0)
                    # Get ATR from indicators if set, otherwise from runtime.last_atr, but don't default to 0.0
                    # If ATR is None or not set, use runtime.last_atr if available, otherwise 0.0 (will be caught by sanity check)
                    atr_from_indicators = indicators.get("atr")
                    if atr_from_indicators is not None:
                        atr0 = float(atr_from_indicators)
                    else:
                        atr0 = float(getattr(runtime, "last_atr", 0.0) or 0.0)
                    age0 = int(indicators.get("atr_age_ms", 0) or 0)
                    now_ms = int(indicators.get("now_ts_ms", 0) or tick_ts or get_ny_time_millis())

                    res = self._atr_sanity.update(
                        symbol=runtime.symbol,
                        atr=atr0,
                        px=px0,
                        age_ms=age0,
                        now_ms=now_ms,
                        tf=(indicators.get("atr_tf", "1m")),  # Pass timeframe for TF-aware threshold
                    )

                    # Use sanitized ATR for downstream gates/tiers/levels
                    indicators["atr"] = res.atr_used
                    indicators["atr_bad"] = res.bad
                    indicators["atr_bad_reason"] = res.reason or ""
                    indicators["atr_used_last_good"] = res.used_last_good
                    indicators["atr_jump_count_window"] = getattr(res, "jump_count_window", 0) or 0

                    # Write monitoring key for reporter/observability (TTL)
                    try:
                        if res.bad == 1:
                            ttl = int(os.getenv("ATR_BAD_TTL_SEC", "600"))
                            reason = res.reason or "na"
                            # Write JSON (not bare "1") so alert worker can display the reason
                            _atr_bad_payload = json.dumps({"reason": reason, "ts_ms": now_ms}, ensure_ascii=False)
                            safe_create_task(self.redis.set(f"cfg:atr_bad:{runtime.symbol}", _atr_bad_payload, ex=ttl))
                            safe_create_task(self.redis.sadd("cfg:atr_bad:symbols", runtime.symbol))
                            safe_create_task(self.redis.expire("cfg:atr_bad:symbols", int(os.getenv("ATR_BAD_SYMBOLS_SET_TTL_SEC", "86400"))))
                            # SRE counter: atr_bad_total{symbol,reason} (hash field=reason)
                            try:
                                safe_create_task(self.redis.hincrby(f"metrics:atr_bad_total:{runtime.symbol}", reason, 1))
                                safe_create_task(self.redis.expire(f"metrics:atr_bad_total:{runtime.symbol}", int(os.getenv("METRICS_COUNTER_TTL_SEC", "604800"))))
                            except Exception:
                                pass
                        # Jump window counters (independent from atr_bad)
                        if (getattr(res, "jump_event", 0) or 0) == 1:
                            win = int(os.getenv("ATR_JUMP_WINDOW_SEC", "3600"))
                            safe_create_task(self.redis.incr(f"cfg:atr_jump_count:{runtime.symbol}"))
                            safe_create_task(self.redis.expire(f"cfg:atr_jump_count:{runtime.symbol}", win))
                            safe_create_task(self.redis.sadd("cfg:atr_jump:symbols", runtime.symbol))
                            safe_create_task(self.redis.expire("cfg:atr_jump:symbols", int(os.getenv("ATR_JUMP_SYMBOLS_SET_TTL_SEC", "86400"))))
                            # SRE counter: atr_jump_total{symbol}
                            try:
                                safe_create_task(self.redis.incr(f"metrics:atr_jump_total:{runtime.symbol}"))
                                safe_create_task(self.redis.expire(f"metrics:atr_jump_total:{runtime.symbol}", int(os.getenv("METRICS_COUNTER_TTL_SEC", "604800"))))
                            except Exception:
                                pass
                    except Exception:
                        pass
                else:
                    indicators.setdefault("atr_bad", 0)
                    indicators.setdefault("atr_bad_reason", "")
                    indicators.setdefault("atr_used_last_good", 0)
                    indicators.setdefault("atr_jump_count_window", 0)
            except Exception:
                indicators.setdefault("atr_bad", 0)
                indicators.setdefault("atr_bad_reason", "")
                indicators.setdefault("atr_used_last_good", 0)
                indicators.setdefault("atr_jump_count_window", 0)

            # CVD quarantine (0/1) + fallback mode
            indicators["cvd_quarantine_active"] = int(getattr(runtime, "cvd_quarantine_active", 0) or indicators.get("cvd_quarantine_active", 0) or 0)
            indicators.setdefault(
                "delta_fallback_mode",
                str(getattr(runtime, "delta_fallback_mode", "") or ("volume" if indicators["cvd_quarantine_active"] else "cvd"))
            )
            # Best-effort meta for reporting (reason/ttl)
            try:
                indicators.setdefault("cvd_quarantine_until_ms", int(getattr(runtime, "cvd_quarantine_until_ms", 0) or indicators.get("cvd_quarantine_until_ms", 0) or 0))
                indicators.setdefault("cvd_quarantine_reason", str(getattr(runtime, "cvd_quarantine_reason", "") or indicators.get("cvd_quarantine_reason", "") or ""))
            except Exception:
                pass

            # Persist quarantine meta for Telegram health reporter
            # Keys:
            #   cfg:cvd_quarantine_meta:{sym} = JSON {until_ms, reason, mode, ts_ms}
            #   cfg:cvd_quarantine:symbols = set of active quarantine symbols
            try:
                if int(indicators.get("cvd_quarantine_active", 0) or 0) == 1:
                    now_ms = int(indicators.get("now_ts_ms", 0) or tick_ts or get_ny_time_millis())
                    until_ms = int(indicators.get("cvd_quarantine_until_ms", 0) or 0)
                    reason = (indicators.get("cvd_quarantine_reason", "") or "")
                    mode = (indicators.get("delta_fallback_mode", "") or "volume")
                    ttl_sec = 900
                    if until_ms > now_ms:
                        ttl_sec = max(60, int((until_ms - now_ms) / 1000))
                    meta = {"until_ms": until_ms, "reason": reason, "mode": mode, "ts_ms": now_ms}
                    # NOTE: replace self.redis -> your redis client if it differs
                    safe_create_task(self.redis.set(f"cfg:cvd_quarantine_meta:{runtime.symbol}", json.dumps(meta, ensure_ascii=False), ex=ttl_sec))
                    safe_create_task(self.redis.sadd("cfg:cvd_quarantine:symbols", runtime.symbol))
                    safe_create_task(self.redis.expire("cfg:cvd_quarantine:symbols", int(os.getenv("CVD_Q_SYMBOLS_SET_TTL_SEC", "86400"))))
                    # SRE counter: cvd_quarantine_activations_total{symbol}
                    try:
                        safe_create_task(self.redis.incr(f"metrics:cvd_quarantine_activations_total:{runtime.symbol}"))
                        safe_create_task(self.redis.expire(f"metrics:cvd_quarantine_activations_total:{runtime.symbol}", int(os.getenv("METRICS_COUNTER_TTL_SEC", "604800"))))
                    except Exception:
                        pass
            except Exception:
                pass

            # ------------------------------------------------------------------
            # Volume-delta fallback: if CVD is quarantined, compute delta_z from signed trade volume
            # (protects against broken baselines / offset jumps). Deterministic, robust.
            # ------------------------------------------------------------------
            delta_z_used = delta_event.get("z", 0.0) if isinstance(delta_event, dict) else 0.0
            try:
                if (indicators.get("cvd_quarantine_active", 0) or 0) == 1:
                    from core.delta_volume_fallback import volume_delta_z_from_tick
                    dz, d_raw = volume_delta_z_from_tick(runtime, tick)
                    delta_z_used = dz if dz is not None else delta_z_used
                    # unify downstream: override delta_event + indicators when in fallback
                    if isinstance(delta_event, dict):
                        delta_event["z"] = delta_z_used
                        delta_event["raw"] = d_raw
                        delta_event["mode"] = "volume_fallback"
                    indicators["delta_tick"] = d_raw
                    indicators["delta_z"] = delta_z_used
                    indicators["delta_fb_raw"] = d_raw
                    indicators["delta_fb_z"] = delta_z_used
                    indicators["delta_z_source"] = "volume_fallback"
                else:
                    indicators["delta_z_source"] = "cvd"
            except Exception:
                indicators.setdefault("delta_z_source", "cvd")

            try:
                spr = indicators.get("spread_bps", 0.0) or 0.0
                churn = getattr(runtime, "book_churn_score", 0.0) or 0.0
                brz = getattr(runtime, "book_rate_z", 0.0) or 0.0
                press = getattr(runtime, "pressure_sps", 0.0) or 0.0
                # Fetch ATR bps if available
                px = price or indicators.get("price", 0.0) or 0.0
                atr = indicators.get("atr", getattr(runtime, "last_atr", 0.0)) or 0.0
                atr_bps = (atr / px * 10000.0) if (px > 0 and atr > 0) else 0.0
                indicators["atr_bps"] = atr_bps

                max_expected_slippage_bps = float(cfg.get("max_expected_slippage_bps", 18.0))

                # Discrete execution regime buckets (liq×vol)
                liq_label = getattr(runtime, "last_liq_regime", getattr(runtime, "liq_regime", "na")) or "na"
                vol_label = getattr(runtime, "dynamic_cfg", {}).get("vol_regime_label", "na") or "na"
                b = compute_exec_regime_bucket(liq_regime_label=liq_label, vol_regime_label=vol_label)
                bucket = b.bucket
                indicators["exec_regime_bucket"] = bucket
                indicators["liq_regime_label"] = liq_label
                indicators["vol_regime_label"] = vol_label
                indicators["vol_ratio_z"] = getattr(runtime, "dynamic_cfg", {}).get("vol_ratio_z", 0.0) or 0.0

                # tighten slippage allowance by bucket
                if bucket == "HIGH_VOL_LOW_LIQ": f = float(cfg.get("regime_slip_factor_high_vol_low_liq", 0.70))
                elif bucket == "HIGH_VOL":       f = float(cfg.get("regime_slip_factor_high_vol", 0.85))
                elif bucket == "LOW_LIQ":        f = float(cfg.get("regime_slip_factor_low_liq", 0.85))
                else:                            f = 1.0
                floor_bps = float(cfg.get("regime_slip_floor_bps", 6.0))
                max_expected_slippage_bps = max(floor_bps, max_expected_slippage_bps * f)
                indicators["max_expected_slippage_bps_eff"] = max_expected_slippage_bps

                est = expected_slippage_bps(
                    spread_bps=spr,
                    churn_score=churn,
                    book_rate_z=brz,
                    pressure_sps=press,
                    atr_bps=atr_bps,
                    cfg=cfg,
                )
                indicators["expected_slippage_bps"] = est.expected_bps
                indicators["expected_slippage_model_bps"] = est.expected_bps

                # Decomp
                try:
                    ip = indicators.get("impact_proxy", 0.0) or 0.0
                    size_usd = float(indicators.get("order_size_usd") or cfg.get("slippage_decomp_size_ref_usd", 10000.0) or 10000.0)
                    de = expected_slippage_decomp_bps(spread_bps=spr, impact_proxy=ip, cfg=cfg, order_size_usd=size_usd)
                    indicators["expected_slippage_spread_bps"] = de.spread_bps
                    indicators["expected_slippage_impact_bps"] = de.impact_bps
                    indicators["expected_slippage_decomp_bps"] = de.total_bps
                    if int(cfg.get("slippage_decomp_enforce_max", 0) or 0):
                        indicators["expected_slippage_bps"] = max(indicators["expected_slippage_bps"], de.total_bps)
                except Exception:
                    pass

                indicators["slippage_reason"] = est.reason or ""
                # Optional OFI add-on: convert best-level OFI into extra impact bps
                # Default k=0 => disabled. Enable via cfg['slip_ofi_k'] or env SLIP_OFI_K.
                try:
                    k = float(cfg.get('slip_ofi_k', os.getenv('SLIP_OFI_K', '0.0')) or 0.0)
                    if k > 0:
                        impact = k * abs(indicators.get('ofi_best_norm', 0.0) or 0.0)
                        if impact > 0:
                            indicators['expected_slippage_bps'] = (indicators.get('expected_slippage_bps', 0.0) or 0.0) + impact
                            indicators['slippage_reason'] = str(indicators.get('slippage_reason', 'na') or 'na') + f'|ofi+{impact:.3f}'
                except Exception:
                    pass
            except Exception:
                # keep setdefault() values above
                pass

            # ------------------------------------------------------------
            # OFConfirm Engine (single source of truth for decision & score)
            # ------------------------------------------------------------
            try:
                # absorption = absorption_feat (computed earlier)
                absorption = absorption_feat
                # Robust gate using pre-computed health (lines 1728+)
                book_ok = int(indicators.get("book_health_ok", 1))
                book_health = (indicators.get("book_health", "OK"))

                # Additional check: explicitly verify threshold from dynamic config (OR logic)
                try:
                    # Условие прохода: book_ts_gap_ms < book_stale_ms ИЛИ book_rate_hz >= book_rate_min_hz
                    br = float(indicators.get("book_rate_hz", 0.0))
                    min_hz = float(runtime.dynamic_cfg.get(DK.BOOK_RATE_MIN_HZ, 0.0))
                    book_gap = int(indicators.get("book_ts_gap_ms", 999999))
                    book_stale_ms = int(runtime.config.get("book_stale_ms", 15000))
                    has_book = int(getattr(runtime, "last_book_ts_ms", 0) or 0) > 0

                    gap_ok = (book_gap < book_stale_ms)
                    rate_ok = (min_hz > 0 and br >= min_hz)

                    if has_book and (gap_ok or rate_ok):
                        book_ok = 1
                        indicators["book_health_ok"] = 1
                        indicators["book_health"] = "OK"
                    else:
                        book_ok = 0
                        indicators["book_health_ok"] = 0
                        if not has_book:
                            indicators["book_health"] = "NO_BOOK"
                        elif not gap_ok and not rate_ok:
                            indicators["book_health"] = "STALE_AND_LOW_RATE"
                except Exception:
                    pass

                if book_ok == 0:
                    of_session_outcome_total.labels(runtime.symbol, sess, "veto_book_stale").inc()
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
                p_snap = runtime.pressure.snapshot(now_ms=tick_ts)
                pres_per_min = p_snap.per_min_ema
                cd_per_min = p_snap.cd_rate_ema

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

                runtime.dynamic_cfg[DK.PRESSURE_PER_MIN] = pres_per_min
                runtime.dynamic_cfg[DK.PRESSURE_HI] = pressure_hi
                runtime.dynamic_cfg[DK.PRESSURE_EXTREME] = pressure_extreme
                indicators["pressure_hi_flag"] = pressure_hi
                indicators["pressure_extreme_flag"] = pressure_extreme

                # 4. Strictness escalation (Need=3)
                # If pressure is high, increase required confirmations (reversal/continuation need -> 3)
                # Only if strong_dynamic_need_enable=1 (default)
                if bool(int(runtime.config.get("strong_dynamic_need_enable", 1))):
                    # [EXPERT] Fix drift: always base on static config values instead of cumulative dynamic state
                    base_r = int(runtime.config.get("strong_need_reversal", 2) or 2)
                    base_c = int(runtime.config.get("strong_need_continuation", 2) or 2)
                    need_r = base_r
                    need_c = base_c

                    if pressure_hi or pressure_extreme:
                        need_r = max(need_r, 3)
                        need_c = max(need_c, 3)
                        indicators["strong_need_reason"] = "pressure"
                    else:
                        indicators["strong_need_reason"] = "base"

                    runtime.dynamic_cfg[DK.STRONG_NEED_REVERSAL] = need_r
                    runtime.dynamic_cfg[DK.STRONG_NEED_CONTINUATION] = need_c

                # --- Delta-notional tier gate (AUTHORITATIVE: dn_calib via dynamic_cfg) ---
                tiers_cfg = runtime.config.get("delta_diff_tiers") or get_default_delta_tiers(runtime.symbol)

                rg = str(getattr(runtime, "last_regime", "na") or "na").lower()
                tier_idx = 0 if "trend" in rg else 1
                # Escalation by pressure flags (telemetry-only inputs; dn thresholds remain dn_calib)
                if int(runtime.dynamic_cfg.get(DK.PRESSURE_HI, 0) or 0) == 1:
                    tier_idx = min(tier_idx + 1, 2)
                if int(runtime.dynamic_cfg.get(DK.PRESSURE_EXTREME, 0) or 0) == 1:
                    tier_idx = 2

                tier_key = f"tier{tier_idx}"

                # Read ONLY canonical dn_calib keys; fallback to defaults
                th = float(runtime.dynamic_cfg.get(f"dn_tier{tier_idx}_usd", 0.0) or 0.0)
                if th <= 0:
                    th = float(tiers_cfg.get(tier_key) or tiers_cfg.get("tier1", 100000.0) or 100000.0)

                notional_usd = abs(delta_event.get("delta", 0.0)) * price
                indicators["delta_notional_usd"] = notional_usd
                indicators["dn_tier_active"] = tier_idx
                indicators["dn_tier_threshold"] = th

                sess = session_utc(tick_ts)

                if th > 1.0 and notional_usd < th:
                    # EXPERT RELAXATION (2026-01-30): Consistent with main DN-GATE
                    # prefix = symbol_env_prefix(runtime.symbol) - using top-level import
                    prefix = symbol_env_prefix(runtime.symbol)
                    is_meme = prefix in ("PEPE", "SHIB", "DOGE", "BONK", "FLOKI", "WIF")

                    if is_meme and notional_usd >= th * 0.50:
                        # Log every 10,000th message
                        cnt = self.dn_gate_proxy_relaxed_counters.get(runtime.symbol, 0) + 1
                        self.dn_gate_proxy_relaxed_counters[runtime.symbol] = cnt
                        if cnt % 10000 == 0:
                            logger.info("✅ [DN-GATE-PROXY] (%s) RELAXED: notional_usd=$%.2f passed via 50%% tolerance (th=$%.2f) (x%d)",
                                        runtime.symbol, notional_usd, th, cnt)
                    else:
                        ticks_pressure_filtered_total.labels(symbol=runtime.symbol, reason=tier_key).inc()
                        dn_gate_events_total.labels(symbol=runtime.symbol, tier=str(tier_idx), session=sess, result="veto").inc()
                        sampled_warning(
                            logger,
                            "DN_FILTERED",
                            "🛑 (%s) Notional Veto: $%.2f < threshold $%.2f (tier=%s)",
                            runtime.symbol,
                            notional_usd,
                            th,
                            tier_key,
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
                    calib_src = (cfg2.get("abs_lvl_calib_src", "static") or "static")
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
                    veto_th = _safe_float(cfg2.get("abs_lvl_cont_veto_score", 0.75), 0.75)
                    abs_bias = (indicators.get("abs_lvl_bias", "NONE") or "NONE").upper()
                    abs_score = indicators.get("abs_lvl_score", 0.0) or 0.0
                    if indicators.get("abs_lvl_ready", 0) == 1 and t_dir is not None:
                        if abs_bias in ("LONG","SHORT") and abs_bias != t_dir.upper() and abs_score >= veto_th:
                            indicators["abs_lvl_cont_veto"] = 1
                except Exception:
                    pass

                # Threshold and weighting overrides
                cfg2["of_score_min"] = _safe_float(cfg2.get("of_score_min", os.getenv("OF_SCORE_MIN", "0.60")), 0.60)

                # Divergence Sensitivity
                cfg2["div_strength_min"] = _safe_float(cfg2.get("div_strength_min", 1.5), 1.5)
                cfg2["div_min_price_bp"] = _safe_float(cfg2.get("div_min_price_bp", 3.0), 3.0)
                if hasattr(runtime, "divergence") and runtime.divergence:
                    runtime.divergence.apply_config(cfg2)

                # --- L3-lite (Cancellation/Add rates + VPIN for OFConfirm engine) ---
                if runtime.l3_stats:
                    indicators["cancel_bid_rate_ema"] = runtime.l3_stats.cancel_bid_rate_ema
                    indicators["cancel_ask_rate_ema"] = runtime.l3_stats.cancel_ask_rate_ema
                    indicators["taker_buy_rate_ema"] = runtime.l3_stats.taker_buy_rate_ema
                    indicators["taker_sell_rate_ema"] = runtime.l3_stats.taker_sell_rate_ema

                    # New: additions (within tracked top-K depth)
                    indicators["added_bid_rate_ema"] = getattr(runtime.l3_stats, "added_bid_rate_ema", 0.0) or 0.0
                    indicators["added_ask_rate_ema"] = getattr(runtime.l3_stats, "added_ask_rate_ema", 0.0) or 0.0
                    indicators["added_total_rate_ema"] = indicators["added_bid_rate_ema"] + indicators["added_ask_rate_ema"]

                    # limit-add + toxicity (VPIN-like) proxies
                    indicators["limit_add_total_rate_ema"] = getattr(runtime.l3_stats, "limit_add_total_rate_ema", 0.0) or 0.0
                    indicators["limit_add_imb"] = getattr(runtime.l3_stats, "limit_add_imb", 0.0) or 0.0
                    indicators["vpin_tox_ema"] = getattr(runtime.l3_stats, "vpin_tox_ema", 0.0) or 0.0
                    indicators["vpin_tox_z"] = getattr(runtime.l3_stats, "vpin_tox_z", 0.0) or 0.0

                    # Aliases (experiment-friendly names)
                    indicators["lambda_trade_buy"] = runtime.l3_stats.taker_buy_rate_ema
                    indicators["lambda_trade_sell"] = runtime.l3_stats.taker_sell_rate_ema
                    indicators["lambda_limit_add"] = getattr(runtime.l3_stats, "limit_add_total_rate_ema", 0.0) or 0.0
                    _cb = runtime.l3_stats.cancel_bid_rate_ema
                    _ca = runtime.l3_stats.cancel_ask_rate_ema
                    _la = getattr(runtime.l3_stats, "limit_add_total_rate_ema", 0.0) or 0.0
                    _den = _la + (_cb + _ca) + 1e-9
                    indicators["liquidity_pressure"] = (_la - (_cb + _ca)) / _den
                    indicators["info_flow"] = getattr(runtime.l3_stats, "vpin_tox_ema", 0.0) or 0.0

                # Hawkes burst features (computed on bucket advance; fail-open if missing)
                hsnap = getattr(runtime, "hawkes_snapshot", None)
                if isinstance(hsnap, dict):
                    indicators.update(hsnap)

                # --- Fail-open fix: spread/slippage must not silently be 0 ---
                # Guarantee spread_bps and expected_slippage_bps (not zeros silently).
                # Three failure modes are explicitly handled here:
                # 1. Crossed BBO -> book_processor guards against 0-write; see book_processor.py.
                # 2. Stale book (go-worker frozen) -> last_spread_bps_l2 keeps old value indefinitely;
                #    we skip it once book_ts_gap_ms > SPREAD_STALE_BOOK_GAP_MS.
                # 3. Cold-start race (python-worker restarted before first L2 snapshot arrives) ->
                #    suppress data_health penalty for SPREAD_MISSING_COLD_START_MS.
                try:
                    _stale_ms = _safe_int(cfg2.get(
                        "spread_stale_book_gap_ms",
                        self._env.spread_stale_book_gap_ms,
                    ), 15000)
                    _cold_start_ms = _safe_int(cfg2.get(
                        "spread_missing_cold_start_ms",
                        self._env.spread_missing_cold_start_ms,
                    ), 30000)
                    _book_ts_gap = indicators.get("book_ts_gap_ms", 0) or 0
                    _book_never_seen = _book_ts_gap >= 10**8
                    _book_stale = (not _book_never_seen) and (_book_ts_gap > _stale_ms)
                    _first_book_ts = getattr(runtime, "first_book_ts_ms", 0) or 0
                    _in_cold_start = _book_never_seen and (
                        _first_book_ts <= 0 or (tick_ts - _first_book_ts) < _cold_start_ms
                    )

                    spr = indicators.get("spread_bps", 0.0) or 0.0
                    if spr <= 0:
                        if not _book_stale and not _book_never_seen:
                            spr = getattr(runtime, "last_spread_bps_l2", 0.0) or 0.0
                        else:
                            indicators["spread_bps_stale_book"] = 1
                    if spr <= 0:
                        spr = getattr(runtime, "last_spread_bps", 0.0) or 0.0
                    if spr <= 0:
                        spr = indicators.get("liq_spread_bps", 0.0) or 0.0
                    if spr <= 0:
                        spr = _safe_float(cfg2.get("spread_bps_missing_default", SPREAD_BPS_MISSING_DEFAULT), SPREAD_BPS_MISSING_DEFAULT)
                        indicators["spread_bps_missing"] = 1
                        if not _in_cold_start:
                            dh = indicators.get("data_health", 1.0) or 1.0
                            indicators["data_health"] = min(dh, _safe_float(cfg2.get("data_health_on_spread_missing", DATA_HEALTH_ON_SPREAD_MISSING), DATA_HEALTH_ON_SPREAD_MISSING))
                            r_str = (indicators.get("data_health_reasons", ""))
                            indicators["data_health_reasons"] = (r_str + ",spread_missing") if r_str else "spread_missing"
                            indicators["book_health_ok"] = 0
                        else:
                            r_str = (indicators.get("data_health_reasons", ""))
                            indicators["data_health_reasons"] = (r_str + ",spread_cold_start") if r_str else "spread_cold_start"
                            indicators["spread_bps_cold_start"] = 1
                    indicators["spread_bps"] = spr

                    if "expected_slippage_bps" not in indicators or (indicators.get("expected_slippage_bps", 0.0) or 0.0) <= 0:
                        indicators["expected_slippage_bps"] = _safe_float(cfg2.get("expected_slippage_bps_missing_default", SLIPPAGE_BPS_MISSING_DEFAULT), SLIPPAGE_BPS_MISSING_DEFAULT)
                        indicators["expected_slippage_missing"] = 1
                except Exception:
                    pass

                # Propagate sid for deterministic canary-share ENFORCE
                # Prefer stable sid from signal pipeline or generate deterministic one
                sid = str(indicators.get("sid", "") or indicators.get("stable_sid", "") or "")
                if not sid:
                    # Generate deterministic sid for this signal candidate
                    _dir = locals().get("direction", "unknown")
                    _scen = locals().get("scenario", "unknown")
                    sid = f"{runtime.symbol}|{tick_ts}|{_dir}|{_scen}"
                indicators["sid"] = sid

                # ------------------------------------------------------------------
                # Persist anomaly keys for reporters (best-effort, async)
                # ------------------------------------------------------------------
                try:
                    ttl = _safe_int(os.getenv("REPORT_KEYS_TTL_SEC", "7200"), 7200)
                    sym = (runtime.symbol or "").upper()
                    if sym:
                        # ATR bad keys
                        if (indicators.get("atr_bad", 0) or 0) == 1:
                            o = {
                                "ts_ms": tick_ts or 0,
                                "atr_age_ms": indicators.get("atr_age_ms", 0) or 0,
                                "atr_bps": indicators.get("atr_bps", 0.0) or 0.0,
                                "reason": (indicators.get("atr_bad_reason", "") or ""),
                            }
                            safe_create_task(self.redis.set(f"cfg:atr_bad:{sym}", json.dumps(o, ensure_ascii=False), ex=ttl))
                            sset = os.getenv("ATR_BAD_SYMBOLS_SET", "cfg:atr_bad:symbols")
                            safe_create_task(self.redis.sadd(sset, sym))
                            safe_create_task(self.redis.expire(sset, ttl))

                        # CVD quarantine keys
                        if (indicators.get("cvd_quarantine_active", 0) or 0) == 1:
                            until_ms = _safe_int(indicators.get("cvd_quarantine_until_ms", 0) or getattr(runtime, "cvd_quarantine_until_ms", 0) or 0, 0)
                            o = {
                                "ts_ms": tick_ts or 0,
                                "until_ms": until_ms,
                                "reason": (indicators.get("cvd_quarantine_reason", "") or ""),
                            }
                            safe_create_task(self.redis.set(f"cfg:cvd_quarantine:{sym}", json.dumps(o, ensure_ascii=False), ex=ttl))
                            sset = os.getenv("CVD_Q_SYMBOLS_SET", "cfg:cvd_quarantine:symbols")
                            safe_create_task(self.redis.sadd(sset, sym))
                            safe_create_task(self.redis.expire(sset, ttl))
                except Exception:
                    pass

                # Capture inputs for golden replay (fail-open, sampled)
                CAP = os.getenv("OFC_CAPTURE_ENABLE", "0") == "1"
                CAP_EVERY = _safe_int(os.getenv("OFC_CAPTURE_EVERY_N", "200"), 200)
                CAP_PATH = os.getenv("OFC_CAPTURE_PATH", "/tmp/ofc_inputs.ndjson")
                if CAP and (runtime.tick_count % CAP_EVERY == 0):
                    row = {
                        "symbol": runtime.symbol,
                        "tf": str(runtime.config.get("micro_tf", "1s")),
                        "direction": direction,
                        "tick_ts_ms": tick_ts,
                        "price": price,
                        "delta_z": delta_event.get("z", 0.0),
                        "indicators": indicators,
                        "absorption": absorption if isinstance(absorption, dict) else None,
                        # cfg можно ограничить (чтобы файл не раздувался)
                        "cfg": {},
                    }
                    try:
                        with open(CAP_PATH, "a", encoding="utf-8") as f:
                            f.write(json.dumps(row, ensure_ascii=False) + "\n")
                    except Exception:
                        pass

                # Measure engine build latency for SRE monitoring
                t_build_ns0 = time.perf_counter_ns()
                ofc, dec = self.of_engine.build(
                    symbol=runtime.symbol,
                    tf=str(runtime.config.get("micro_tf", "1s")),
                    direction=direction,
                    tick_ts_ms=tick_ts,
                    price=price,
                    delta_z=delta_z_used,
                    snap_t0=getattr(runtime, "last_book", None), # Fix: Pass current book for qimb/ofi features
                    runtime=runtime,
                    cfg=cfg2,
                    indicators=indicators,
                    absorption=absorption if isinstance(absorption, dict) else None,
                )
                t_build_us = int((time.perf_counter_ns() - t_build_ns0) / 1000)

                # expose calibration diagnostics
                indicators["abs_lvl_eff_quote_th"] = float(cfg2.get("abs_lvl_eff_quote_th", 0.0) or 0.0)
                indicators["abs_lvl_min_quote_delta"] = float(cfg2.get("abs_lvl_min_quote_delta", 0.0) or 0.0)
                indicators["abs_lvl_calib_n"] = int(cfg2.get("abs_lvl_calib_n", 0) or 0)
                indicators["abs_lvl_calib_src"] = (cfg2.get("abs_lvl_calib_src", "static"))

                if ofc:
                    ev = ofc.evidence
                    # G10 reads absorption_volume from top-level indicators; ev is not accessible there.
                    if isinstance(ev, dict):
                        indicators.setdefault("absorption_volume", float(ev.get("absorption_volume", 0.0) or 0.0))
                        indicators.setdefault("absorption", int(ev.get("absorption", 0) or 0))
                    # Use dec directly from build() instead of overwriting with None
                    if dec and hasattr(dec, "need") and hasattr(dec, "have"):
                        # P2: Dynamic Confirmation Need (Expert Scaler)
                        # We lower the barrier in high liquidity (liq_score >= 0.8)
                        # and raise it if requested by regime service.
                        liq_score = float(indicators.get("liq_score", 1.0) or 1.0)
                        need_bump = 0

                        if liq_score >= 0.8:
                            # Healthy market: allow 2-leg signals in Range scenario
                            if getattr(dec, "scenario", "") == "range":
                                 dec.need = max(2, dec.need - 1)
                                 dec.reason = f"{dec.reason}|liq_relax"
                                 ofc.reason = f"{ofc.reason}|liq_relax"
                        elif liq_score < 0.35:
                            need_bump = 1

                        if indicators.get("exec_regime_bucket") == "HIGH_VOL_LOW_LIQ":
                            need_bump += int(cfg2.get("regime_need_bump_high_vol_low_liq", 1) or 0)
                            if int(cfg2.get("regime_enforce_strong_high_vol_low_liq", 1) or 0) == 1:
                                # We can force "need" higher if required
                                pass

                        if need_bump > 0:
                            indicators["strong_gate_need_bump"] = need_bump
                            indicators["strong_gate_need_reason"] = "low_liquidity_or_regime"

                        eff_need = dec.need + need_bump
                        ofc.need = eff_need
                        dec.need = eff_need

                        # Re-evaluate OK status
                        is_ok = dec.have >= eff_need
                        # Only strictify (never relax)
                        if not is_ok:
                            indicators["strong_gate_ok"] = 0
                            indicators["of_confirm_ok"] = 0
                            ofc.ok = False # Sync object
                            dec.ok = False
                            if "need_bump_veto" not in (ofc.reason or ""):
                                ofc.reason = f"need_bump_veto({dec.have}/{eff_need})|{getattr(ofc, 'reason', '')}"
                                dec.reason = ofc.reason

                        # IMPORTANT:
                        #   ofc.score is a continuous quality score (0..1).
                        #   have/need ratio is a different diagnostic.
                        # Keep both explicitly to avoid confusing audits/telemetry/Telegram.
                        indicators["of_confirm_score"] = getattr(ofc, "score", 0.0) or 0.0
                        _dec_have = getattr(dec, "have", 0)
                        indicators["of_confirm_have_need_ratio"] = (_dec_have / eff_need) if eff_need > 0 else 0.0

                        # Soft-fail diagnostics
                        indicators["of_confirm_ok_soft"] = ev.get("ok_soft", 0)
                        indicators["of_confirm_soft_reason"] = (ev.get("soft_reason", ""))

                    indicators["of_confirm"] = ofc.to_dict()
                    indicators["of_confirm_v3"] = ofc.to_dict()
                    indicators["of_confirm_ok"] = ofc.ok

                    # ------------------------------------------------------------
                    # SRE metrics emission (sampled, deterministic, fail-open)
                    # ------------------------------------------------------------
                    try:
                        if OF_GATE_METRICS_ENABLE:
                            rate = float(cfg2.get("of_gate_metrics_sample", OF_GATE_METRICS_SAMPLE) or OF_GATE_METRICS_SAMPLE)
                            if rate > 0 and _should_sample(tick_ts, rate):
                                ev = ofc.evidence or {}
                                scenario_v4 = (ev.get("scenario_v4", "") or "") or str(getattr(ofc, "scenario", "") or "")
                                missing = ev.get("missing_legs", []) if isinstance(ev, dict) else []
                                if not isinstance(missing, list):
                                    missing = []

                                ml = ev.get("ml", {}) if isinstance(ev.get("ml", {}), dict) else {}
                                # tolerate both latency_us and latency_ms from ML gate
                                ml_lat_us = 0
                                try:
                                    if "latency_us" in ml:
                                        ml_lat_us = int(float(ml.get("latency_us", 0) or 0))
                                    elif "latency_ms" in ml:
                                        ml_lat_us = int(float(ml.get("latency_ms", 0) or 0) * 1000.0)
                                except Exception:
                                    ml_lat_us = 0

                                payload = {
                                    "type": "of_gate",
                                    "ts_ms": str(normalize_epoch_ms_v2(tick_ts).ts_ms),
                                    "symbol": runtime.symbol,
                                    "direction": direction,
                                    "scenario": str(getattr(ofc, "scenario", "") or ""),
                                    "scenario_v4": scenario_v4,
                                    "ok": str(int(getattr(ofc, "ok", 0) or 0)),
                                    "ok_soft": str(int(ev.get("ok_soft", 0) or 0)),
                                    "have": str(getattr(ofc, "have", 0) or 0),
                                    "need": str(getattr(ofc, "need", 0) or 0),
                                    "score": str(getattr(ofc, "score", 0.0) or 0.0),
                                    # keep for offline debug but cap size (avoid huge cardinality strings)
                                    "reason": str(getattr(ofc, "reason", "") or "")[:120],
                                    "gate_bits": str(getattr(ofc, "gate_bits", 0) or 0),
                                    "exec_risk_bps": str(ev.get("exec_risk_bps", 0.0) or 0.0),
                                    "exec_risk_norm": str(ev.get("exec_risk_norm", 0.0) or 0.0),
                                    "latency_us": str(max(1, t_build_us)),
                                    "meta_p": str(ev.get(MetaKeys.P, -1.0) or -1.0),
                                    "meta_veto": str(int(ev.get(MetaKeys.VETO, 0) or 0)),
                                    "meta_enforce_applied": str(int(ev.get(MetaKeys.ENFORCE_APPLIED, 0) or 0)),
                                    "meta_enforce_share": str(ev.get(MetaKeys.ENFORCE_SHARE, 1.0) or 1.0),
                                    "meta_enforce_bucket": (ev.get("meta_enforce_bucket", "other") or "other"),
                                    "data_health": str(indicators.get("data_health", 1.0) or 1.0),
                                    "book_health_ok": str(indicators.get("book_health_ok", 1)),
                                    # contract from PDF: needed for SRE monitor
                                    "source_consistency_ok": str(indicators.get("source_consistency_ok", 1)),
                                    "missing_legs": json.dumps(missing[:6], ensure_ascii=False, separators=(",", ":")),

                                    # ML confirm (for p50/p95/p99 + fail rate)
                                    "ml_mode": (ml.get("mode", "") or ""),
                                    "ml_kind": (ml.get("kind", "") or ""),
                                    "ml_allow": "1" if ml.get("allow", True) else "0",
                                    "ml_bucket": (ml.get("bucket", "") or ""),
                                    "ml_p_edge": str(ml.get("p_edge", 0.0) or 0.0),
                                    "ml_p_min": str(ml.get("p_min", 0.0) or 0.0),
                                    "ml_score": str(ml.get("score", 0.0) or 0.0),
                                    "ml_floor": str(ml.get("floor", 0.0) or 0.0),
                                    "ml_latency_us": str(ml_lat_us),
                                }

                                if self.logger.isEnabledFor(logging.DEBUG):
                                    self.logger.debug("SRE_METRICS_DEBUG: %s", json.dumps(payload))

                                payload = enrich_schema_fields(payload)
                                async def _emit_ok_metrics(_payload: dict) -> None:
                                    try:
                                        await self.redis.xadd(
                                            OF_GATE_METRICS_STREAM,
                                            {k: str(v) for k, v in _payload.items()},
                                            maxlen=OF_GATE_METRICS_MAXLEN,
                                            approximate=True,
                                        )
                                        ok_metrics_emitted_total.labels("orderflow_strategy").inc()
                                    except Exception:
                                        ok_metrics_error_total.labels("orderflow_strategy", "xadd").inc()

                                safe_create_task(_emit_ok_metrics(payload))
                    except Exception:
                        pass

                        # Persist last strong-gate diagnostics for SMT snapshot / entry policy.
                        try:
                            indicators["strong_gate_have"] = getattr(dec, "have", 0)
                            indicators["strong_gate_need"] = locals().get('eff_need', getattr(dec, "need", 0))
                            indicators["strong_gate_scn"] = str(getattr(dec, "scenario", "") or "")
                            indicators["strong_need_reason"] = str(getattr(dec, "need_reason", "") or "")

                            runtime.last_of_confirm_score = indicators.get("of_confirm_score", 0.0) or 0.0
                            runtime.last_of_confirm_have_need_ratio = indicators.get("of_confirm_have_need_ratio", 0.0) or 0.0
                            runtime.last_strong_gate_have = indicators.get("strong_gate_have", 0) or 0
                            runtime.last_strong_gate_need = indicators.get("strong_gate_need", 0) or 0
                            runtime.last_strong_gate_scn = (indicators.get("strong_gate_scn", "") or "")
                        except Exception:
                            pass
                    indicators["strong_gate_bits"] = getattr(ofc, "gate_bits", 0)
                    indicators["strong_gate_reason"] = str(getattr(ofc, "reason", "") or "")
                    # indicators["strong_gate_ok"] already updated if needed
                    indicators["of_gate_mode"] = "SHADOW" if bool(runtime.config.get("strong_gate_shadow", False)) else "ENFORCE"

                    # --- NEW: record last strong-pass dir/ts ONLY when gate passed (ok==1) ---
                    # This is the value SMT/EntryPolicy should trust as "leader confirmed by OF".
                    try:
                        if ofc.ok == 1:
                            runtime.last_strong_pass_ts_ms = tick_ts
                            runtime.last_strong_pass_dir = direction.upper()
                    except Exception:
                        pass




                    # Rate limit logs: only 1 in 50
                    sg_cnt = self.strong_gate_counters.get(runtime.symbol, 0) + 1
                    self.strong_gate_counters[runtime.symbol] = sg_cnt

                    if sg_cnt % 10000 == 0:
                        self.logger.info(
                            "🔥 Signal Strong-Gate Decision: symbol=%s, scenario=%s, ok=%d, score=%.2f, have=%d, need=%d, reason=%s (x%d)",
                            runtime.symbol, ofc.scenario, ofc.ok, ofc.score, ofc.have, ofc.need, ofc.reason, sg_cnt
                        )

                    # --- REAL vs VIRTUAL (User rules) ---
                    # Real trade: passes StrongGate (ofc.ok == 1)
                    # Virtual trade: passes StrongGate in "disabled" mode (ofc.ok == 0).
                    # (Both are subjected to identical subsequent filters, including confidence).
                    if ofc.ok == 1:
                        indicators["is_virtual"] = 0
                    else:
                        indicators["is_virtual"] = 1
                        indicators["virtual_reason"] = getattr(ofc, "reason", "strong_gate_failed")
                        self.logger.info(
                            "⚠️ Signal marked as VIRTUAL (Failed StrongGate): symbol=%s, scenario=%s, reason=%s",
                            runtime.symbol, getattr(ofc, "scenario", "N/A"), getattr(ofc, "reason", "N/A")
                        )

                    # Audit Confirmations (mirror resulting evidence)
                    # Note: We append these to confirmations list for Telegram/UI
                    if ev.get("sweep"):
                        # Generic sweep flag (always emit for backward compatibility)
                        if "sweep=1" not in confirmations: # Avoid duplicate if already present
                             confirmations.insert(0, "sweep=1")
                        record_evidence_used(runtime.symbol, sess, "sweep=1")
                        div_match = bool(indicators.get("sweep_div_match", 0))
                        require_div = bool(runtime.config.get("sweep_require_divergence", 0))
                        if (not require_div) or div_match:
                             kind = indicators.get("sweep_kind", "")
                             if kind == "EQH_SWEEP":
                                  confirmations.insert(0, "sweep_eqh=1")
                                  record_evidence_used(runtime.symbol, sess, "sweep_eqh=1")
                             elif kind == "EQL_SWEEP":
                                  confirmations.insert(0, "sweep_eql=1")
                                  record_evidence_used(runtime.symbol, sess, "sweep_eql=1")

                    if ev.get("absorption"): confirmations.append(f"absorption={ev.get('absorption_volume', 0.0):.2f}")
                    if ev.get("weak_progress"): confirmations.append("weak_progress=1")
                    if ev.get("abs_lvl_ok"): confirmations.append(f"abs_lvl={ev.get('abs_lvl_score', 0.0):.2f}")

                    # ------------------------------------------------------------
                    # Phase E: OBI quality, FP Edge Absorb, Weak Trend (Scoring/Telemetry)
                    # ------------------------------------------------------------
                    try:
                        now_ms_det = locals().get("now_ms", tick_ts)
                        # OBI stability (quality-gated)
                        if runtime.last_obi_event:
                            age = now_ms_det - int(runtime.last_obi_event.get("ts_ms", 0) or 0)
                            ttl = int(runtime.config.get("obi_event_ttl_ms", 30000))
                            if 0 <= age <= ttl:
                                indicators["obi_event_age_ms"] = int(age)
                                indicators["obi_dir"] = str(runtime.last_obi_event.get("direction") or "")
                                indicators["obi"] = float(runtime.last_obi_event.get("obi", 0.0) or 0.0)
                                indicators["obi_z"] = float(runtime.last_obi_event.get("obi_z", 0.0) or 0.0)
                                indicators["obi_stable_secs"] = float(runtime.last_obi_event.get("stable_secs", 0.0) or 0.0)
                                indicators["obi_stability_score"] = float(runtime.last_obi_event.get("stability_score", 0.0) or 0.0)
                                indicators["obi_sustained"] = int(runtime.last_obi_event.get("stable", 0) or 0) == 1
                                if str(runtime.last_obi_event.get("direction") or "").upper() == direction:
                                    if indicators["obi_sustained"]:
                                        confirmations.append(f"obi_stable={indicators['obi_stable_secs']:.2f}")

                        # BUGFIX: Ensure continuous OBI is recorded for ML if no valid event was found
                        if "obi" not in indicators or indicators["obi"] == 0.0:
                            indicators["obi"] = float(getattr(runtime, "lob_dw_obi", 0.0) or 0.0)
                            indicators["obi_z"] = float(getattr(runtime, "dw_obi_z", 0.0) or 0.0)

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
                                indicators["fp_edge_strength"] = strength
                                indicators["fp_edge_range_expansion"] = rng
                                indicators["fp_edge_age_ms"] = int(age)
                                if ok:
                                    confirmations.append(f"fp_edge_absorb={strength:.2f}")

                        # Weak progress trend (history)
                        try:
                            wp_det = getattr(runtime, "weak_progress_det", None)
                            if wp_det is not None:
                                indicators["weak_recent_window"] = int(getattr(wp_det, "recent_window", 0) or 0)
                                indicators["weak_recent_count"] = int(wp_det.recent_weak_count())
                                w = indicators["weak_recent_window"] or 0
                                c = indicators["weak_recent_count"] or 0
                                ratio = (c / w) if w > 0 else 0.0
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
                                  confirmations.append("iceberg_strict=1")


                    # Optional Redis Publication (v3 asychronous)
                    if bool(int(runtime.config.get("publish_of_confirm", 0))):
                        stream = str(runtime.config.get("of_confirm_stream", RS.OF_CONFIRM))
                        with contextlib.suppress(Exception):
                            safe_create_task(
                                self.ticks.xadd(
                                    stream,
                                    fields={"payload": json.dumps(ofc.to_dict(), ensure_ascii=False)},
                                    maxlen=int(runtime.config.get("of_confirm_stream_maxlen", 50000)),
                                    approximate=True,
                                )
                            )

                    # ------------------------------------------------------------
                    # Publish deterministic decision inputs for golden replay
                    # ------------------------------------------------------------
                    try:
                        # logger.error("DEBUG: 1. accessing OFI config")
                        pub_val = runtime.config.get("publish_of_inputs", 0)
                        should_pub = bool(int(pub_val))

                        if should_pub:
                            # Deterministic time check: skip publish if tick_ts_ms <= 0
                            # This is critical for "golden replay": same ticks must produce same inputs
                            tick_ts_ms = tick_ts if (tick_ts or 0) > 0 else 0
                            if tick_ts_ms <= 0:
                                # skip publish: non-deterministic / bad tick time
                                try:
                                    from services.orderflow.metrics import of_inputs_bad_time_total
                                    of_inputs_bad_time_total.labels(symbol=runtime.symbol).inc()
                                except Exception:
                                    pass
                                should_pub = False

                            trend_dir = "NONE"
                            hidden_ctx_recent = 0
                            cont_ctx_recent = 0

                            if should_pub:
                                # logger.error("DEBUG: 2. Entering OFI Logic")
                                # continuation context
                                try:
                                    div = getattr(runtime, "last_div", None)
                                    td = hidden_trend_dir(getattr(div, "kind", None) if div else None)
                                    if td:
                                        trend_dir = td.upper()
                                    # hidden ctx - deterministic: depends only on tick_ts
                                    if div and td:
                                        now_ts = tick_ts_ms
                                        hidden_ms = int(runtime.config.get("hidden_ctx_valid_ms", 120_000))
                                        age = now_ts - int(getattr(div, "ts_ms", now_ts))
                                        hidden_ctx_recent = 1 if (0 <= age <= hidden_ms) else 0
                                    # cont ctx - deterministic: depends only on tick_ts
                                    now_ts = tick_ts_ms
                                    cts = int(getattr(runtime, "cont_ctx_ts_ms", 0) or 0)
                                    cv = int(runtime.config.get("cont_ctx_valid_ms", 120_000))
                                    cont_ctx_recent = 1 if (cts > 0 and 0 <= now_ts - cts <= cv) else 0
                                except Exception as ex_ctx:
                                    logger.debug(f"OFI: Context calc error: {ex_ctx}")

                            # 2. Extract evidence
                            # Helper functions for deterministic type conversion (sanitizes NaN/Inf, handles None)
                            def _i(v, d=0) -> int:
                                try:
                                    return int(v)
                                except Exception:
                                    try:
                                        return int(float(v))
                                    except Exception:
                                        return d

                            def _f(v, d=0.0) -> float:
                                try:
                                    x = float(v)
                                    # sanitize NaN/Inf (kills replay determinism / diffs)
                                    if x != x or x == float("inf") or x == float("-inf"):
                                        return d
                                    return x
                                except Exception:
                                    return d

                            def _s(v, d="na") -> str:
                                try:
                                    s = str(v) if v is not None else d
                                    s = s.strip()
                                    return s if s else d
                                except Exception:
                                    return d

                            # Prefer evidence snapshot (deterministic), fallback to indicators
                            ev_weak       = _i(indicators.get("weak_progress", 0), 0)
                            ev_sweep      = _i(indicators.get("sweep_recent", indicators.get("sweep", 0)), 0)
                            ev_reclaim    = _i(indicators.get("reclaim_recent", indicators.get("reclaim", 0)), 0)
                            ev_obi_stable = _i(indicators.get("obi_stable", 0), 0)
                            ev_ice_strict = _i(indicators.get("iceberg_strict", indicators.get("ice_strict", 0)), 0)
                            ev_abs_lvl_ok = _i(indicators.get("abs_lvl_ok", 0), 0)

                            if ofc and hasattr(ofc, "evidence") and isinstance(ofc.evidence, dict):
                                ev = ofc.evidence
                                ev_weak       = _i(ev.get("weak_progress", ev_weak), ev_weak)
                                # evidence uses sweep/reclaim (already "recent" semantics in your pipeline)
                                ev_sweep      = _i(ev.get("sweep", ev.get("sweep_recent", ev_sweep)), ev_sweep)
                                ev_reclaim    = _i(ev.get("reclaim", ev.get("reclaim_recent", ev_reclaim)), ev_reclaim)
                                ev_obi_stable = _i(ev.get("obi_stable", ev_obi_stable), ev_obi_stable)
                                ev_ice_strict = _i(ev.get("iceberg_strict", ev_ice_strict), ev_ice_strict)
                                ev_abs_lvl_ok = _i(ev.get("abs_lvl_ok", ev_abs_lvl_ok), ev_abs_lvl_ok)

                            # 4. Create Object
                            # logger.error("DEBUG: 4. Creating OFI Object")

                            # Safe CFG - keep only small, JSON-safe, deterministic subset for replay
                            cfg_safe = {}
                            try:
                                for _k in (
                                    "of_score_min",
                                    "of_inputs_stream",
                                    "of_inputs_stream_maxlen",
                                    "hidden_ctx_valid_ms",
                                    "cont_ctx_valid_ms",
                                ):
                                    if _k in runtime.config:
                                        _v = runtime.config.get(_k)
                                        if isinstance(_v, (int, float, str, bool)) or _v is None:
                                            cfg_safe[_k] = _v
                            except Exception:
                                cfg_safe = {}

                            # Determinism: do NOT pick version by "key presence".
                            # Emit v2 unless explicitly disabled in runtime cfg/env.
                            emit_v2_cfg = runtime.config.get("of_inputs_emit_v2", 1)
                            emit_v2 = bool(_i(emit_v2_cfg, 1))

                            # Build base OFInputs fields
                            ofi_kwargs = {
                                "v": 2 if emit_v2 else 1,
                                "symbol": _s(runtime.symbol),
                                "ts_ms": tick_ts_ms,
                                "regime": _s(getattr(runtime, "last_regime", "na")),
                                "direction": _s(direction),
                                # prefer scenario_v4 from evidence snapshot if available
                                "scenario": _s(
                                    (ofc.evidence.get("scenario_v4") if (ofc and isinstance(getattr(ofc, "evidence", None), dict)) else None)
                                    or (getattr(dec, "scenario_v4", None) if dec else None)
                                    or (getattr(dec, "scenario", None) if dec else None)
                                    or "na"
                                ),
                                # determinism: use the same delta_z used in build(), not raw delta_event
                                "delta_z": _f(delta_z_used, 0.0),
                                "weak_progress": ev_weak,
                                "sweep_recent": ev_sweep,
                                "reclaim_recent": ev_reclaim,
                                "obi_stable": ev_obi_stable,
                                "iceberg_strict": ev_ice_strict,
                                "abs_lvl_ok": ev_abs_lvl_ok,
                                "trend_dir": _s(trend_dir, "NONE").upper(),
                                "hidden_ctx_recent": _i(hidden_ctx_recent, 0),
                                "cont_ctx_recent": _i(cont_ctx_recent, 0),
                                "cfg": cfg_safe,
                                "fp_eff_quote": _f(getattr(runtime.last_bar, "fp_eff_quote", 0.0) if runtime.last_bar else 0.0, 0.0),
                                "fp_quote_delta": _f(getattr(runtime.last_bar, "fp_quote_delta", 0.0) if runtime.last_bar else 0.0, 0.0),
                            }

                            # Optional fields (only if contract supports them)
                            _ann = getattr(OFInputsV1, "__annotations__", {}) or {}
                            if "regime_group" in _ann:
                                ofi_kwargs["regime_group"] = str(getattr(runtime, "last_regime", "na"))

                            hsnap = getattr(runtime, "hawkes_snapshot", None)
                            if isinstance(hsnap, dict):
                                if "hawkes_dt_s" in _ann:
                                    ofi_kwargs["hawkes_dt_s"] = float(hsnap.get("hawkes_dt_s", 0.0) or 0.0)
                                if "hawkes_taker_lam" in _ann:
                                    ofi_kwargs["hawkes_taker_lam"] = float(hsnap.get("hawkes_taker_lam", 0.0) or 0.0)
                                if "hawkes_cancel_lam" in _ann:
                                    ofi_kwargs["hawkes_cancel_lam"] = float(hsnap.get("hawkes_cancel_lam", 0.0) or 0.0)
                                if "hawkes_churn_lam" in _ann:
                                    ofi_kwargs["hawkes_churn_lam"] = float(hsnap.get("hawkes_churn_lam", 0.0) or 0.0)

                            # Add OFI fields if using V2
                            missing_ofi = False
                            missing_fp = False
                            if emit_v2:
                                # Always include fields in v2 (deterministic schema)
                                ofi_kwargs["ofi"] = _f(indicators.get("ofi", 0.0), 0.0)
                                ofi_kwargs["ofi_z"] = _f(indicators.get("ofi_z", 0.0), 0.0)
                                ofi_kwargs["ofi_stable"] = _i(indicators.get("ofi_stable", 0), 0)
                                ofi_kwargs["ofi_dir_ok"] = _i(indicators.get("ofi_dir_ok", 0), 0)
                                ofi_kwargs["ofi_stable_secs"] = _f(indicators.get("ofi_stable_secs", 0.0), 0.0)
                                ofi_kwargs["ofi_stability_score"] = _f(indicators.get("ofi_stability_score", 0.0), 0.0)
                                ofi_kwargs["ofi_age_ms"] = _i(indicators.get("ofi_age_ms", -1), -1)

                                # FP edge fields
                                ofi_kwargs["fp_edge_absorb"] = _i(indicators.get("fp_edge_absorb", 0), 0)
                                ofi_kwargs["fp_edge_absorb_strength"] = _f(indicators.get("fp_edge_absorb_strength", indicators.get("fp_edge_strength", 0.0)), 0.0)
                                ofi_kwargs["fp_edge_age_ms"] = _i(indicators.get("fp_edge_age_ms", -1), -1)

                                # Sweep Distinction (Stage 4)
                                ofi_kwargs["sweep_eqh"] = _i(indicators.get("sweep_eqh", 0), 0)
                                ofi_kwargs["sweep_eql"] = _i(indicators.get("sweep_eql", 0), 0)

                                # Missing = age unknown AND values essentially default
                                if ofi_kwargs["ofi_age_ms"] < 0 and ofi_kwargs["ofi"] == 0.0 and ofi_kwargs["ofi_z"] == 0.0:
                                    missing_ofi = True
                                if ofi_kwargs["fp_edge_age_ms"] < 0 and ofi_kwargs["fp_edge_absorb"] == 0:
                                    missing_fp = True

                                ofi = OFInputsV2(**ofi_kwargs)
                            else:
                                ofi = OFInputsV1(**ofi_kwargs)
                                # For v1, OFI/FP are missing by definition
                                missing_ofi = True
                                missing_fp = True

                            # Record metrics
                            try:
                                from services.orderflow.metrics import (
                                    of_inputs_missing_fp_total,
                                    of_inputs_missing_ofi_total,
                                    of_inputs_version_total,
                                )
                                version_str = "v2" if emit_v2 else "v1"
                                of_inputs_version_total.labels(symbol=runtime.symbol, version=version_str).inc()
                                if missing_ofi:
                                    of_inputs_missing_ofi_total.labels(symbol=runtime.symbol).inc()
                                if missing_fp:
                                    of_inputs_missing_fp_total.labels(symbol=runtime.symbol).inc()
                            except Exception:
                                pass  # Don't fail on metrics

                            # logger.error("DEBUG: 5. Serializing...")
                            # Canonical JSON to make replay/topdiff deterministic
                            blob = json.dumps(ofi.to_dict(), ensure_ascii=False, separators=(",", ":"), sort_keys=True)

                            # Align default with actual usage
                            in_stream = str(runtime.config.get("of_inputs_stream", RS.OF_INPUTS))

                            sampled_debug(logger, "OFI_PUBLISHING", "OFI: Publishing to Redis...")
                            safe_create_task(
                                self.ticks.xadd(
                                    in_stream,
                                    fields={"payload": blob},
                                    maxlen=int(runtime.config.get("of_inputs_stream_maxlen", 200000)),
                                    approximate=True,
                                )
                            )
                            sampled_debug(logger, "OFI_PUBLISHED", "OFI: PublishedTask Created")

                    except Exception as e_main:
                         logger.debug(f"OFI: Block error: {e_main}")
                         pass

            except Exception as ex:
                logger.error(f"OFConfirm engine error: {ex}")


            # ------------------------------------------------------------
            # min_confirmations gate (hard vs soft)
            # По умолчанию fp_imb не увеличивает hard_count, иначе pass-rate станет выше.
            # ------------------------------------------------------------
            # ------------------------------------------------------------

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

            # 10) Confidence Calibration Pipeline (if enabled)
            primary_reason = runtime.last_signal_reason or (confirmations[0] if confirmations else "delta_spike")
            confidence = await self._compute_confidence(runtime, indicators, confirmations, side=direction, kind=primary_reason)
            indicators["confidence"] = confidence

            # ------------------------------------------------------------
            # Phase E: CVD Reclaim (bonus-layer)
            # ------------------------------------------------------------
            # Add as SOFT confirmation after gates (won't affect min_confirmations).
            try:
                if _safe_int(runtime.config.get("cvd_reclaim_enable", 1), 1) == 1:
                    ev = runtime.last_cvd_reclaim
                    if ev and (tick_ts - ev.ts_ms) <= 120_000:
                        if ev.bias == direction:
                            indicators["cvd_reclaim_ok"] = ev.ok
                            indicators["cvd_reclaim_score"] = ev.score
                            indicators["cvd_reclaim_delta"] = ev.cvd_delta
                            if ev.ok:
                                confirmations.append(f"cvdR={ev.score:.2f}")
                                cvd_reclaim_applied_total.labels(symbol=runtime.symbol, bias=direction).inc()
                                cvd_reclaim_age_ms_gauge.labels(symbol=runtime.symbol, bias=direction).set(tick_ts - ev.ts_ms)
            except Exception:
                pass
            # Фильтр по минимальной уверенности
            min_conf_pct = _safe_float(os.getenv("CRYPTO_SIGNAL_MIN_CONF", "70"), 70.0)
 
            # Override из config, который загрузился через OrderFlowConfigLoader
            spec_min_conf = runtime.config.get("signal_min_conf", runtime.config.get("min_conf"))
            if spec_min_conf is not None:
                min_conf_pct = _safe_float(spec_min_conf, min_conf_pct)

            # EXPERT RELAXATION (2026-01-30):
            # Meme coins often have volatile confidence scores. For calibration purposes,
            # we want to capture signals even with lower confidence (pushed to Virtual).
            # Standard floor for memes in Instance 2 is 30%.
            # Can be disabled via env: {PREFIX}_CONF_RELAX_DISABLE=true or CONF_RELAX_DISABLE=true
            # Can be overridden via env: {PREFIX}_CONF_RELAX_MAX=70 (sets max relaxation threshold)
            # prefix = symbol_env_prefix(runtime.symbol) - using top-level import
            prefix = symbol_env_prefix(runtime.symbol)
            is_meme = prefix in ("PEPE", "SHIB", "DOGE", "BONK", "FLOKI", "WIF")
            if is_meme:
                # Check for per-symbol disable
                symbol_disable = _to_bool(os.getenv(f"{prefix}_CONF_RELAX_DISABLE", ""))
                global_disable = _to_bool(os.getenv("CONF_RELAX_DISABLE", "false"))

                if symbol_disable or global_disable:
                    # Relaxation disabled for this symbol
                    pass
                else:
                    # Check for per-symbol override of max relaxation threshold
                    relax_max_str = os.getenv(f"{prefix}_CONF_RELAX_MAX", os.getenv("CONF_RELAX_MAX", "30.0"))
                    relax_max = _safe_float(relax_max_str, 30.0)

                    original_min_conf = min_conf_pct
                    min_conf_pct = min(min_conf_pct, relax_max)
                    if original_min_conf > relax_max:
                        # Log every 10,000th message
                        cnt = self.conf_relax_counters.get(runtime.symbol, 0) + 1
                        self.conf_relax_counters[runtime.symbol] = cnt
                        if cnt % 10000 == 0:
                            self.logger.info("✅ [CONF-RELAX] (%s) Relaxed min_conf: %.1f%% -> %.1f%% (meme=%s prefix=%s relax_max=%.1f%%) (x%d)",
                                             runtime.symbol, original_min_conf, min_conf_pct, is_meme, prefix, relax_max, cnt)

            # ------------------------------------------------------------
            # Phase E_macro: CoinGecko Macro Gate Overlay (Risk-Off)
            # ------------------------------------------------------------
            try:
                cg_mode = os.getenv("COINGECKO_GATE_MODE", "TIGHTEN_ONLY").strip().upper()
                cg_res = self.cg_macro_gate.evaluate(indicators, direction)

                indicators["cg_gate_risk_off"] = int(cg_res.risk_off)
                indicators["cg_gate_alt_weakness"] = int(cg_res.alt_weakness)
                indicators["cg_gate_reason"] = cg_res.reason

                if cg_mode == "TIGHTEN_ONLY" and cg_res.reason:
                    penalty_pct = cg_res.confidence_penalty * 100.0
                    min_conf_pct += penalty_pct
                    if self.conf_relax_counters.get(runtime.symbol, 0) % 100 == 0:
                        self.logger.info("🛡️ [CG-GATE] (%s) Macro Tighten: min_conf+%.1f%%, risk_mult=%.2f. Reason: %s",
                                         runtime.symbol, penalty_pct, cg_res.risk_mult, cg_res.reason)
            except Exception:
                pass

            # ------------------------------------------------------------
            # Phase E: OFI stability evidence (TTL + book health)
            # ------------------------------------------------------------
            # OFI is harder to fake than snapshot OBI because it is incremental.
            # Default: SOFT confirmation (does not affect min_confirmations).
            try:
                if int(indicators.get("book_health_ok", 1) or 1) == 1:
                    ev = getattr(runtime, "last_ofi_event", None)
                    if isinstance(ev, dict):
                        ots = int(ev.get("ts_ms", 0) or 0)
                        ttl = int(runtime.config.get("ofi_event_ttl_ms", 15000) or 15000)
                        if ots > 0 and 0 <= (tick_ts - ots) <= ttl:
                            indicators["ofi"] = float(ev.get("ofi", 0.0) or 0.0)
                            indicators["ofi_z"] = float(ev.get("ofi_z", 0.0) or 0.0)
                            indicators["ofi_stable_secs"] = float(ev.get("stable_secs", 0.0) or 0.0)
                            indicators["ofi_stability_score"] = float(ev.get("stability_score", 0.0) or 0.0)
                            indicators["ofi_stable"] = int(ev.get("stable", 0) or 0)
                            indicators["ofi_age_ms"] = tick_ts - ots

                            # direction match -> add confirmation
                            if int(ev.get("stable", 0) or 0) == 1:
                                bias = (ev.get("direction", "") or "").upper()
                                if bias == direction.upper():
                                    confirmations.append(f"ofi_stable={indicators['ofi_stable_secs']:.1f}s")
            except Exception:
                pass

            # ------------------------------------------------------------
            # Calibrated Gating (P75+)
            # ------------------------------------------------------------
            confidence_gate = confidence
            gate_mode = self.conf_cal_gating_mode
            gate_reason = "raw"

            proof = None
            should_cal = False
            if gate_mode != "raw" and self.conf_cal_runtime:
                if gate_mode == "cal_always":
                    should_cal = True
                    gate_reason = "always"
                elif gate_mode == "cal_after_proof":
                    self._ensure_proof_state(tick_ts)
                    proof = self.conf_cal_proof if isinstance(self.conf_cal_proof, dict) else None

                    # Emit proof metadata into indicators (fail-open).
                    if proof:
                        try:
                            indicators["confidence_cal_proof_valid"] = 1 if bool(proof.get("valid")) else 0
                            if "reason" in proof:
                                indicators["confidence_cal_proof_reason"] = (proof.get("reason") or "")
                            indicators["confidence_cal_proof_ts"] = int(proof.get("ts", 0) or 0)
                            indicators["confidence_cal_proof_evidence_ts"] = int(proof.get("evidence_ts", proof.get("ts", 0)) or 0)
                        except Exception:
                            pass

                    if proof and bool(proof.get("valid")):
                        # Check freshness against evidence_ts (NOT controller update ts)
                        evidence_ts = int(proof.get("evidence_ts", proof.get("ts", 0)) or 0)
                        max_age = int(runtime.config.get("confidence_cal_gating_proof_max_age_sec", 21600))

                        # Deterministic freshness relative to tick time
                        age = (int(tick_ts / 1000.0) - evidence_ts) if evidence_ts > 0 else 10**18
                        try:
                            indicators["confidence_cal_proof_age_sec"] = age if age < 10**17 else -1
                            src = proof.get("source", {}) if isinstance(proof.get("source"), dict) else {}
                            if isinstance(src, dict):
                                if "status_age_sec" in src:
                                    indicators["confidence_cal_live_status_age_sec"] = float(src.get("status_age_sec") or 0.0)
                                if "status_ts_ms" in src:
                                    indicators["confidence_cal_live_status_ts_ms"] = int(src.get("status_ts_ms") or 0)
                        except Exception:
                            pass

                        if age <= max_age:
                            should_cal = True
                            gate_reason = "proof_valid"
                        else:
                            gate_reason = "proof_stale"
                    else:
                        gate_reason = "proof_invalid" if proof else "no_proof"

                # Canary check
                if should_cal:
                    canary = float(runtime.config.get("confidence_cal_gating_canary_share", 1.0))

                    # Optional override from proof controller (canary ramp)
                    try:
                        if isinstance(proof, dict) and proof.get("canary_share") is not None:
                            canary = float(proof.get("canary_share"))
                    except Exception:
                        pass

                    canary = max(0.0, min(1.0, canary))
                    with contextlib.suppress(Exception):
                        indicators["confidence_cal_canary_share"] = canary

                    if canary < 1.0:
                        try:
                            import zlib
                            sid = runtime.symbol
                            sess = (indicators.get("session", ""))
                            h = zlib.crc32(f"{sid}|{sess}".encode()) % 100
                            if h >= int(canary * 100):
                                should_cal = False
                                gate_reason += "_canary_skip"
                        except Exception:
                            pass

            if should_cal:
                try:
                    # Calibrate
                    cal_ctx = {
                        "session": indicators.get("session"),
                        "regime": indicators.get("regime", "neutral"),
                        "symbol": runtime.symbol,
                    }
                    # Using get_calibrated_confidence from Compatibility Layer or Runtime
                    if self.conf_cal_runtime:
                        cal_res = self.conf_cal_runtime.get_calibrated_confidence(
                            raw_conf=confidence,
                            context=cal_ctx
                        )
                    else:
                        cal_res = confidence

                    if cal_res is None:
                         cal_conf = confidence
                    elif isinstance(cal_res, dict):
                         val = cal_res.get("calibrated_confidence")
                         if val is None:
                             val = cal_res.get("result", confidence)
                         cal_conf = float(val) if val is not None else confidence
                    else:
                         cal_conf = float(cal_res)

                    confidence_gate = cal_conf
                    gate_reason += f"_calibrated({confidence:.3f}->{cal_conf:.3f})"
                    confidence = cal_conf # OVERRIDE for filter
                except Exception as e:
                    gate_reason += f"_error({str(e)})"

            indicators["confidence_gate"] = confidence_gate
            indicators["confidence_gate_mode"] = gate_mode
            indicators["confidence_gate_reason"] = gate_reason
            indicators["confidence_decision"] = confidence

            min_conf = min_conf_pct / 100.0

            if tick.get("mock_force"):
                 self.logger.warning("TRACE 6: Confidence Check. conf=%f min=%f", confidence, min_conf)

            # Delegation to SignalPipeline: strategy.py only annotates confidence.
            # CONFIDENCE_GATE_OWNER = signal_pipeline

            # Telemetry: Hidden Divergence Usage
            if indicators.get("hidden_div_used"):
                 from services.orderflow.metrics import of_hidden_divergence_signal_total
                 of_hidden_divergence_signal_total.labels(symbol=runtime.symbol).inc()

            runtime.signal_count += 1

            # Executable Entry Pricing (P0)
            executable_entry = price
            try:
                if runtime.last_book:
                    bts_entry = int(getattr(runtime, "last_book_ts_ms", 0) or 0)
                    # Max staleness 2s for pricing to avoid bad fills
                    if bts_entry > 0 and (tick_ts - bts_entry) < 2000:
                        if direction == "LONG":
                            asks_entry = runtime.last_book.get("asks")
                            if asks_entry and len(asks_entry) > 0:
                                 executable_entry = float(asks_entry[0][0])
                        else:
                            bids_entry = runtime.last_book.get("bids")
                            if bids_entry and len(bids_entry) > 0:
                                 executable_entry = float(bids_entry[0][0])

                        # Sanity: if deviation > 10% from tick price, revert to tick (bad book?)
                        if abs(executable_entry - price) / (price + 1e-9) > 0.10:
                            executable_entry = price
            except Exception:
                executable_entry = price

            signal_id = generate_signal_id(
                kind="sig",
                symbol=runtime.symbol,
                direction=direction,
                ts_ms=tick_ts,
            )

            # Initialize payload early for candidate/pressure enrichment
            payload = {
                "symbol": runtime.symbol,
                "ts_ms": tick_ts,
                "tick_ts": tick_ts,
                "price": price,
                "entry": executable_entry,
                "direction": direction,
                "side": direction.lower(),
                "side_int": direction_norm.to_side_int(),  # P0: Numeric side representation
                "indicators": indicators,
                "confirmations": list(confirmations),
                "confidence": confidence,
                "signal_id": signal_id,
                "entry_tag": primary_reason,
                "is_virtual": bool(int(indicators.get("is_virtual", 0) or 0)),
            }

            self._log_metrics(runtime)


            # === Pressure snapshot attached to every candidate payload ===
            try:
                ps = runtime.pressure.snapshot(now_ms=tick_ts)
                payload["pressure"] = {
                    "per_min_ema": ps.per_min_ema,
                    "cd_rate_ema": ps.cd_rate_ema,
                    "n_raw": ps.n_raw,
                    "n_cd": ps.n_cd,
                }
                hi_th = float(runtime.config.get("pressure_hi_per_min", 60.0))
                payload["pressure"]["pressure_hi"] = 1 if ps.per_min_ema >= hi_th else 0
            except Exception:
                pass

            # Attach microstructure context (from last book/bar)
            try:
                payload["micro"] = {}
                payload["micro"]["spread_bps"] = getattr(runtime, "last_spread_bps", 0.0) or 0.0
                payload["micro"]["spread_z"] = getattr(runtime, "last_spread_z", 0.0) or 0.0
                # book freshness/rate
                bts = int(getattr(runtime, "last_book_ts_ms", 0) or 0)
                book_stale_ms = (tick_ts - bts) if (bts > 0 and tick_ts > 0 and tick_ts >= bts) else 10**9
                payload["micro"]["book_stale_ms"] = book_stale_ms
                payload["micro"]["book_rate_ema"] = getattr(runtime, "book_rate_ema", 0.0) or 0.0
                payload["micro"]["book_rate_z"] = getattr(runtime, "book_rate_z", 0.0) or 0.0
                payload["micro"]["book_churn_score"] = getattr(runtime, "book_churn_score", 0.0) or 0.0
                payload["micro"]["book_churn_hi"] = getattr(runtime, "book_churn_hi", 0) or 0
                if book_stale_ms_gauge is not None:
                    book_stale_ms_gauge.labels(symbol=runtime.symbol).set(book_stale_ms)
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

            # ------------------------------------------------------------------
            # 🛡️ ADVERSE SELECTION GATE (P0)
            # ------------------------------------------------------------------
            # 1. Reversal: Must have Reclaim or Absorption or OFI stability
            # 2. Continuation: Must wait for next microbar close (verify follow-through)
            # ------------------------------------------------------------------
            if bool(int(runtime.config.get("adverse_check_enable", 0))):
                scn = (indicators.get("strong_gate_scn", "") or "").lower()
                if not scn:
                    scn = "reversal" if int(indicators.get("sweep", 0) or 0) == 1 else "continuation"

                # REVERSAL CHECK (Immediate Veto)
                if "reversal" in scn:
                    # Evidence required: cvd_reclaim OR absorption OR obi_stable OR ofi_stable
                    has_reclaim = bool(indicators.get("cvd_reclaim_ok", 0))
                    has_absorb = bool(indicators.get("absorption_volume", 0) > 0)
                    has_obi = bool(indicators.get("obi_stable", 0))
                    has_ofi = bool(indicators.get("ofi_stable", 0))

                    if not (has_reclaim or has_absorb or has_obi or has_ofi):
                        g10_adverse_veto_total.labels(gate="G10_ADVERSE_REVERSAL").inc()
                        return None

                # CONTINUATION CHECK (Wait for Bar)
                elif "continuation" in scn:
                    # Store and WAIT. Do not emit now.
                    runtime.pending_adverse_payload = payload
                    runtime.pending_adverse_ts_ms = tick_ts
                    # logger.info("⏳ [ADVERSE] Continuation Wait: payload buffered for next microbar")
                    return None

            # ------------------------------------------------------------------
            # STAGE 4: Confirmation Features & Metrics (Patch V4)
            # ------------------------------------------------------------------
            try:
                 # 1. Telemetry: record usage of specific confirmations
                 # (Contract compliance: we must track what evidence was used for a signal)
                 sess_name = session_utc(tick_ts)
                 confs = payload.get("confirmations")
                 if not isinstance(confs, list):
                     confs = []
                 for c in confs:
                     k = c.split("=")[0] if "=" in c else c
                     record_confirmation_seen(runtime.symbol, c)
                     record_evidence_used(runtime.symbol, sess_name, c)

                 # 2. Feature Extraction: inject rich features into payload["indicators"]
                 # If "ofc" is available (local scope), use it. If not, fallback to runtime state.
                 # This aligns with V4 requirements to expose evidence age/strength as features.
                 extra_ind = {}

                 # Evidence source: ofc.evidence (best) -> runtime (fallback)
                 # Note: ofc might be named differently or unavailable in some paths
                 ev_src = {}
                 if "ofc" in locals() and ofc:
                      ev_src = getattr(ofc, "evidence", {}) or {}

                 # A. Divergence Strength
                 div = getattr(runtime, "last_div", None)
                 if div:
                      extra_ind["div_strength"] = float(div.strength)
                      extra_ind["div_age_ms"] = int(tick_ts - div.ts_ms)
                      # Div Match flag
                      div_match = 0
                      d_dir = direction.upper()
                      if d_dir == "LONG" and str(div.kind).startswith("bullish") or d_dir == "SHORT" and str(div.kind).startswith("bearish"): div_match = 1
                      extra_ind["conf_div_match"] = div_match

                 # B. Sweep Features
                 sweep = getattr(runtime, "last_sweep", None)
                 if sweep:
                      extra_ind["sweep_age_ms"] = int(tick_ts - sweep.ts_ms)
                      s_dir = str(sweep.direction_bias or "").upper()
                      d_dir = direction.upper()
                      # Exposure for training
                      extra_ind["sweep_aligned"] = 1 if s_dir == d_dir else 0

                 # C. OBI / Iceberg / Reclaim Age (if present in confirmations or indicators)
                 # We rely on payload indicators if available
                 if int(indicators.get("obi_stable", 0) or 0):
                      extra_ind["obi_age_ms"] = int(indicators.get("obi_stable_age", 0) or 0)

                 indicators.update(extra_ind)

            except Exception:
                 pass

            # ------------------------------------------------------------------
            # 🎯 P61: ML CONFIRM LIVE ROLLOUT BINDING
            # ------------------------------------------------------------------
            # Shadow/Canary/Full enforcement with drift/DQ-aware fallback
            # ------------------------------------------------------------------
            try:
                cfg2 = runtime.config or {}
                rollout_mode = (cfg2.get("ml_confirm_rollout", "shadow")).lower()
                canary_rate = float(cfg2.get("ml_confirm_canary_rate", 0.05))

                # Check drift/DQ state - if blocked, skip ML enforcement (rule-strong-only)
                drift_state = str(indicators.get("drift_state", "ok")).lower()
                dq_state = str(indicators.get("dq_state", "ok")).lower()

                if drift_state == "block" or dq_state == "block":
                    # Rule-strong-only mode: ML does not enforce
                    indicators["ml_enforce_mode"] = "rule_strong_only"
                    indicators["ml_enforce_reason"] = f"drift={drift_state},dq={dq_state}"
                else:
                    # Normal ML enforcement path
                    ev = getattr(ofc, "evidence", {}) or {}
                    ml = ev.get("ml", {}) if isinstance(ev.get("ml"), dict) else {}
                    ml_allow = int(ml.get("allow", 1))  # default allow if missing
                    ml_kind = str(ml.get("kind", "")).lower()

                    # Determine if we should enforce for this signal
                    sid = str(payload.get("signal_id", ""))
                    should_enforce = _ml_should_enforce(rollout_mode, sid, canary_rate)

                    if rollout_mode == "shadow":
                        # Shadow mode: track what would happen but don't block
                        if ml_allow == 0:
                            indicators["ml_shadow_veto"] = 1
                            indicators["ml_shadow_kind"] = ml_kind
                    elif should_enforce:
                        # Canary or Full mode: actually enforce
                        if ml_allow == 0:
                            # Check override policies for deny/abstain
                            allow_rule_strong = False
                            if ml_kind == "deny":
                                allow_rule_strong = bool(cfg2.get("ml_deny_allow_rule_strong", True))
                            elif ml_kind == "abstain":
                                allow_rule_strong = bool(cfg2.get("ml_abstain_allow_rule_strong", True))

                            if not allow_rule_strong:
                                # Real veto: block the signal
                                of_session_outcome_total.labels(
                                    symbol=runtime.symbol,
                                    session=sess,
                                    outcome="veto_ml"
                                ).inc()
                                indicators["ml_veto"] = 1
                                indicators["ml_veto_kind"] = ml_kind
                                indicators["ml_enforce_mode"] = rollout_mode
                                sampled_warning(
                                    self.logger, "ML_VETO",
                                    "🚫 [P61] ML veto: symbol=%s, mode=%s, kind=%s, sid=%s",
                                    runtime.symbol, rollout_mode, ml_kind, sid
                                )
                                return None  # Signal blocked
                            else:
                                # Override: allow rule-strong to pass
                                indicators["ml_veto_override"] = 1
                                indicators["ml_override_reason"] = f"{ml_kind}_allow_rule_strong"
            except Exception as exc:
                # Fail-open: if ML enforcement crashes, don't block the signal
                log_silent_error(exc, 'ml_rollout_failure', runtime.symbol, 'process_tick')

            return await self._emit_payload(runtime, payload, tick_ts)

    async def _on_microbar_closed(self, runtime: SymbolRuntime, bar: MicroBar) -> None:  # type: ignore
            """
            In-memory обработка события bar_close.
            Здесь можно делать более тяжелые вычисления (но только на bar_close, не на каждом тике):
            - swings
            - divergences
            - RSI(price) и RSI(CVD)  # type: ignore
            - New: CVD Snapshots & Dedicated Div Stream
            """
            now_ts = int(getattr(bar, "end_ts_ms", 0) or 0)
            now_ms = now_ts
            rg = getattr(runtime, "last_regime", "na") or "na"
            rg = rg.lower()

            try:
                getattr(runtime, "ensure_dn_loaded", lambda _: None)(self.redis) # type: ignore[attr-defined]
                # P0 FIX: deduplicated ensure_* calls (was 6, now 3 — saves 3 Redis round-trips per bar)
                if self._env.atr_tf_calib_enable:
                    await runtime.ensure_atr_tf_loaded(self.redis)
                if self._env.atr_bps_calib_enable:
                    await runtime.ensure_atr_bps_loaded(self.redis)
                # ATR sanity selector state (source preference)
                try:
                    if bool(int(runtime.config.get("atr_sanity_enable", int(self._env.atr_sanity_enable)) or 1)):
                        await runtime.ensure_atr_sanity_loaded(self.redis)
                except Exception:
                    pass
            except Exception:
                pass


            # --- ATR sanity range proxy update (roll microbars into atr_tf buckets) ---
            try:
                o = float(getattr(bar, "open", 0.0) or 0.0)
                h = float(getattr(bar, "high", 0.0) or 0.0)
                l = float(getattr(bar, "low", 0.0) or 0.0)
                c = float(getattr(bar, "close", 0.0) or 0.0)
                if now_ts > 0:
                    # ADVERSE Selection Check: Continuation Verify
                    if runtime.pending_adverse_payload:
                        sig = runtime.pending_adverse_payload
                        # Check timeout (e.g. 2 * tf or 5s)
                        age_adv = now_ts - runtime.pending_adverse_ts_ms
                        if 0 < age_adv < 5000:
                            s_dir = sig.get("direction", "").upper()
                            # Verified if bar closes in favor
                            verified = False
                            if s_dir == "LONG" and c > o or s_dir == "SHORT" and c < o: verified = True

                            if verified:
                                # Log every 10,000th message
                                cnt = self.adverse_continuation_counters.get(runtime.symbol, 0) + 1
                                self.adverse_continuation_counters[runtime.symbol] = cnt
                                if cnt % 10000 == 0:
                                    logger.info("✅ [ADVERSE] Continuation Verified! Emitting buffered signal. (x%d)", cnt)
                                # inject late metrics
                                sig["adverse_wait_ms"] = age_adv
                                # EMIT
                                final_sig = await self._emit_payload(runtime, sig, now_ts)
                                if final_sig:
                                    preprocess_signal_for_publish(final_sig, runtime.symbol, "CryptoOrderFlow", self.logger)
                                    await self.publish_signal(runtime, final_sig)
                            else:
                                g10_adverse_veto_total.labels(gate="G10_ADVERSE_CONTINUATION").inc()
                        else:
                            g10_adverse_veto_total.labels(gate="G10_ADVERSE_TIMEOUT").inc()

                        # Clear buffer after check (one-shot)
                        runtime.pending_adverse_payload = None
                        runtime.pending_adverse_ts_ms = 0

                    runtime.atr_range_agg.push_microbar(end_ts_ms=now_ts, o=o, h=h, l=l, c=c) # type: ignore[attr-defined]
                    snap = runtime.atr_range_agg.snapshot() # type: ignore[attr-defined]
                    runtime.dynamic_cfg[DK.ATR_RANGE_TF_MS] = snap.tf_ms
                    runtime.dynamic_cfg[DK.ATR_RANGE_N] = snap.n
                    runtime.dynamic_cfg[DK.ATR_RANGE_P50_BPS] = snap.p50
                    runtime.dynamic_cfg[DK.ATR_RANGE_P95_BPS] = snap.p95
            except Exception:
                pass

            # 0. Update Daily Tracker
            try:
                 # Feed microbar to daily tracker (persists on day roll)
                 runtime.daily_tracker.update(bar)
            except Exception:
                 pass

            # 0) Dynamic Regime Update
            try:
                 # Fast fetch, fall back to "na" (default)
                 # Key convention: regime:{symbol} -> string "range"|"trend"|"thin"
                 reg_key = f"regime:{runtime.symbol}"
                 # We use generic 'ticks' or 'main' redis? 'ticks' is usually for streams. 'main' is for keys.
                 # self.redis is available in CryptoOrderflowService instance (self)
                 # but we need to await it.
                 rg_val = await self.redis.get(reg_key)

                 old_regime = str(getattr(runtime, "last_regime", "na") or "na")
                 new_regime = "na"

                 if rg_val:
                     if isinstance(rg_val, bytes):
                         rg_val = rg_val.decode('utf-8', errors='ignore')
                     new_regime = str(rg_val).strip()

                 runtime.last_regime = new_regime

                 # 🔔 Notify on regime change
                 # COMMENTED OUT: Telegram notifications disabled
                 # if old_regime != "na" and new_regime != "na" and old_regime != new_regime:
                 #      try:
                 #          msg_text = (
                 #              f"🔄 <b>Regime Change</b> [{runtime.symbol}]\n"
                 #              f"Old: {old_regime}\n"
                 #              f"New: {new_regime}\n"
                 #              f"Time: {datetime.now(timezone.utc).strftime('%H:%M:%S UTC')}"
                 #          )
                 #          await self.notify_client.xadd(
                 #              self.notify_stream,
                 #              {"type": "report", "text": msg_text},
                 #              maxlen=5000,
                 #              approximate=True
                 #          )
                 #      except Exception as ex:
                 #          logger.warning(f"⚠️ Failed to send regime change notify: {ex}")
            except Exception:
                 # fail-safe
                 pass

            # ------------------------------------------------------------------
            # ATR TF Calibrator update (freshness + consistency)
            # Deterministic time: bar.end_ts_ms
            # ------------------------------------------------------------------
            try:
                close_px = float(getattr(bar, "close", 0.0) or 0.0)
                if self._env.atr_tf_calib_enable and now_ts > 0 and close_px > 0:
                    cand_str = str(runtime.config.get("atr_tf_candidates", self._env.atr_tf_candidates) or "")
                    cands = [x.strip() for x in cand_str.split(",") if x.strip()]
                    if not cands:
                        cands = ["1m", "5m", "15m"]

                    atr_bps_by_tf: dict[str, float] = {}
                    cache = getattr(self, "atr_cache", None)
                    if cache is not None:
                        for tf in cands:
                            v, _ = cache.get_with_meta(symbol=runtime.symbol, timeframe=tf, now_ms=now_ts)
                            vv = v or 0.0
                            if vv > 0:
                                atr_bps_by_tf[tf] = 10000.0 * (vv / close_px)

                    if atr_bps_by_tf:
                        runtime.atr_tf_calib.update_many(regime=rg, atr_bps_by_tf=atr_bps_by_tf)

                    target_bps = float(runtime.dynamic_cfg.get(DK.ATR_BPS_TH, 0.0) or runtime.config.get("atr_bps_min_static", 0.0) or 15.0)
                    dec = runtime.atr_tf_calib.recommend_tf(
                        regime=rg,
                        target_bps=target_bps,
                        fallback_tf=str(runtime.config.get("atr_tf", "1m") or "1m"),
                        now_ts_ms=now_ts,
                        current_tf=str(runtime.dynamic_cfg.get(DK.ATR_TF_SELECTED, "")),
                    )
                    runtime.dynamic_cfg[DK.ATR_TF_SELECTED] = dec.tf
                    runtime.dynamic_cfg[DK.ATR_TF_SRC] = dec.src
                    runtime.dynamic_cfg[DK.ATR_TF_N] = dec.n
                    runtime.dynamic_cfg[DK.ATR_TF_READY] = 1 if dec.n >= runtime.atr_tf_calib.min_samples else 0
                    runtime.dynamic_cfg[DK.ATR_TF_P50_BPS] = dec.picked_p50_bps
                    
                    calib_svc = getattr(self, "calib_svc", None)
                    if calib_svc is not None:
                        await calib_svc.persist_atr_sanity(runtime, regime=rg, ts_ms=now_ts)
                    
                    runtime.dynamic_cfg[DK.ATR_TF_TARGET_BPS] = dec.target_bps
                    runtime.dynamic_cfg[DK.ATR_TF_TFS_P50] = dec.tfs_p50
            except Exception:
                pass

            # --------------------------------------------------------
            # ATR Sanity Calibrator (Source Selection) - User Diff Integration
            # --------------------------------------------------------
            try:
                if bool(int(runtime.config.get("atr_sanity_enable", int(self._env.atr_sanity_enable)) or 1)):
                    close_ts = now_ts
                    # ATR TF
                    atr_tf = str(runtime.config.get("atr_tf", "1m") or "1m")
                    # Normalize TF
                    try:
                        if getattr(self, "atr_cache", None) is not None:
                            atr_tf_norm_func = getattr(self.atr_cache, "_normalize_tracker_tf", lambda x: str(x).upper())
                            tf_norm = atr_tf_norm_func(atr_tf) # type: ignore[attr-defined]
                        else:
                            tf_norm = str(atr_tf).upper()
                    except Exception:
                        tf_norm = atr_tf.upper()

                    cands_src = []
                    cache = getattr(self, "atr_cache", None)
                    if cache is not None:
                        try:
                            cands_src = cache.get_candidates(symbol=runtime.symbol, timeframe=atr_tf, now_ms=close_ts)
                        except Exception:
                            cands_src = []

                    dec_src = runtime.atr_sanity.decide(tf_norm=tf_norm, candidates=cands_src)

                    runtime.dynamic_cfg[DK.ATR_SRC_PREF] = dec_src.src_pref
                    runtime.dynamic_cfg[DK.ATR_SRC_READY] = int(dec_src.ok)
                    runtime.dynamic_cfg[DK.ATR_SRC_REASON] = dec_src.reason
                    runtime.dynamic_cfg[DK.ATR_SRC_MISMATCH] = dec_src.mismatch
                    runtime.dynamic_cfg[DK.ATR_SRC_N] = dec_src.n
                    runtime.dynamic_cfg[DK.ATR_SRC_MEDIAN] = dec_src.median
                    runtime.dynamic_cfg[DK.ATR_SRC_PICKED] = dec_src.picked

                    # Persist state (throttled)
                    try:
                        min_iv_ms = _safe_int(runtime.config.get("atr_sanity_persist_min_interval_ms", 300_000), 300_000)
                        min_bars = _safe_int(runtime.config.get("atr_sanity_persist_min_bars", 30), 30)
                        runtime._atr_sanity_bars_since_persist = _safe_int(getattr(runtime, "_atr_sanity_bars_since_persist", 0)) + 1
                        last_p = _safe_int(getattr(runtime, "_atr_sanity_last_persist_ts_ms", 0))
                        due_by_time = (last_p <= 0) or (close_ts - last_p >= min_iv_ms)
                        due_by_bars = runtime._atr_sanity_bars_since_persist >= min_bars

                        if dec_src.n >= 5 and (due_by_time or due_by_bars):
                            svc = getattr(self, "calib_svc", None)
                            if svc is not None:
                                await svc.persist_atr_sanity(runtime, tf_norm=str(tf_norm), ts_ms=close_ts)
                            runtime._atr_sanity_last_persist_ts_ms = close_ts
                            runtime._atr_sanity_bars_since_persist = 0
                    except Exception:
                        pass
            except Exception:
                pass


            # Throttled persist per regime
            try:
                gap_ms = _safe_int(runtime.config.get("atr_tf_calib_persist_gap_ms", self._env.atr_tf_calib_persist_gap_ms), 120_000)
                last_p = int(getattr(runtime, "_atr_tf_last_persist_ts_ms", 0) or 0)
                if gap_ms > 0 and (now_ts - last_p) >= gap_ms:
                    svc = getattr(self, "calib_svc", None)
                    if svc is not None:
                        await svc.persist_atr_tf_regime(runtime, regime=rg, ts_ms=now_ts)
                    runtime._atr_tf_last_persist_ts_ms = now_ts
            except Exception as exc:
                log_silent_error(exc, 'persist_failure', runtime.symbol, '_handle_tick:atr_tf_persist')
                pass


            # --- Dynamic calibration update (eff_quote / min_quote_delta) ---
            try:
                quote_delta = float(getattr(runtime, "last_quote_delta", 0.0) or 0.0)
                if quote_delta > 0:
                    rg = str(getattr(runtime, "last_regime", "na") or "na").lower()
                    runtime.eff_calib.update(regime=rg, eff_quote=quote_delta, quote_delta=quote_delta)

                    # ... existing eff_calib persistence ...
                    # Leaving existing EffQuote logic here as is, assumed working
                    # ...
                    if bool(int(runtime.config.get("calib_persist_enable", 1))):
                        runtime._calib_bars_since_persist = runtime._calib_bars_since_persist + 1
                        min_bars = int(runtime.config.get("calib_persist_min_bars", 60))
                        if runtime._calib_bars_since_persist >= min_bars:
                            runtime._calib_bars_since_persist = 0
                            svc = getattr(self, "calib_svc", None)
                            if svc is not None:
                                await svc.persist_effq(runtime, regime=rg, ts_ms=now_ts)

            except Exception as exc:
                log_silent_error(exc, 'calib_update_failure', runtime.symbol, '_handle_tick:eff_calib_update')
                pass

            # ------------------------------------------------------------------
            # ATR(bps) sanity floors (per-regime) -> runtime.dynamic_cfg
            # Fix "broken chain": we MUST select atr_bps_th based on regime+tier and expose it.
            # ------------------------------------------------------------------
                close_px = float(getattr(bar, "close", 0.0) or 0.0)
                atr_val = float(getattr(runtime, "last_atr", 0.0) or 0.0)
                rg = str(getattr(runtime, "last_regime", "na") or "na").lower()
                if close_px > 0 and atr_val > 0:
                    atr_bps = 10000.0 * (atr_val / close_px)
                    runtime.dynamic_cfg[DK.ATR_BPS] = atr_bps

                    # Update calibrator (fail-open)
                    if self._env.atr_bps_calib_enable:
                        runtime.atr_bps_calib.update(regime=rg, atr_bps=atr_bps)

                    # Bootstrap floors (must be >0 in config; if not, fallback to static min)
                    # --- ATR Floor Policy (Tiered) ---
                    # Check for overrides in local 'cfg'
                    cfg = runtime.config
                    d0 = float(cfg.get("atr_floor_t0_bps", 0.0) or 0.0)
                    d1 = float(cfg.get("atr_floor_t1_bps", 0.0) or 0.0)
                    d2 = float(cfg.get("atr_floor_t2_bps", 0.0) or 0.0)
                    floors = runtime.atr_bps_calib.thresholds(
                        regime=rg,
                        default_floor_t0=d0,
                        default_floor_t1=d1,
                        default_floor_t2=d2,
                    )
                    runtime.dynamic_cfg[DK.ATR_FLOOR_T0_BPS] = floors.floor_t0
                    runtime.dynamic_cfg[DK.ATR_FLOOR_T1_BPS] = floors.floor_t1
                    runtime.dynamic_cfg[DK.ATR_FLOOR_T2_BPS] = floors.floor_t2
                    runtime.dynamic_cfg[DK.ATR_BPS_SRC] = floors.src
                    runtime.dynamic_cfg[DK.ATR_BPS_N] = floors.n
                    runtime.dynamic_cfg[DK.ATR_CALIB_READY] = int(
                        1 if floors.n >= (runtime.config.get("atr_bps_calib_min_samples") or self._env.atr_bps_calib_min_samples or 500) else 0
                    )

                    # SELECT threshold by regime tier (this is the missing link)
                    tier, rg2, th = compute_atr_bps_threshold(
                        regime=rg,
                        cfg=runtime.config,
                        t0=floors.floor_t0,
                        t1=floors.floor_t1,
                        t2=floors.floor_t2,
                    )
                    runtime.dynamic_cfg[DK.ATR_FLOOR_TIER] = tier
                    runtime.dynamic_cfg[DK.ATR_BPS_TH] = th

                    # Persist (throttled)
                    try:
                        gap_ms = int(runtime.config.get("atr_bps_calib_persist_gap_ms") or self._env.atr_bps_calib_persist_gap_ms or 120000)
                        last_p = int(getattr(runtime, "_atr_bps_last_persist_ts_ms", 0) or 0)
                        if self._env.atr_bps_calib_enable and gap_ms > 0 and (bar.end_ts_ms - last_p) >= gap_ms:
                            svc = getattr(self, "calib_svc", None)
                            if svc is not None:
                                await svc.persist_atr_bps(runtime, regime=rg, ts_ms=bar.end_ts_ms)
                            runtime._atr_bps_last_persist_ts_ms = bar.end_ts_ms
                    except Exception as exc:
                        log_silent_error(exc, 'persist_failure', runtime.symbol, '_handle_tick:atr_bps_persist')
                        pass
            except Exception as exc:
                log_silent_error(exc, 'calib_update_failure', runtime.symbol, '_handle_tick:atr_bps_wrapper')
                pass

            # --- DeltaNotional tiers calibration (per regime) ---
            try:
                dn_usd = abs(float(getattr(bar, "delta_sum", 0.0) or 0.0)) * float(getattr(bar, "close", 0.0) or 0.0)
                if math.isfinite(dn_usd) and dn_usd > 0:
                    rg = str(getattr(runtime, "last_regime", "na") or "na").lower()

                    # 1. Update Calibrator (Authoritative source)
                    runtime.dn_calib.update(
                        regime=rg,
                        dn_usd=dn_usd,
                        ts_ms=bar.end_ts_ms
                    )

                    # 2. Telemetry: Check Scale & Divergence (Throttle: 1h)
                    now_ms = bar.end_ts_ms
                    if not hasattr(runtime, "last_dn_how_report_ts_ms"):
                         runtime.last_dn_how_report_ts_ms = 0

                    if now_ms - runtime.last_dn_how_report_ts_ms > 3600_000:
                        tiers_cfg = runtime.config.get("delta_diff_tiers") or get_default_delta_tiers(runtime.symbol)
                        d0 = float(tiers_cfg.get("tier0", 0.0) or 0.0)
                        d1 = float(tiers_cfg.get("tier1", 0.0) or 0.0)
                        d2 = float(tiers_cfg.get("tier2", 0.0) or 0.0)

                        t_telem = runtime.dn_calib.tiers(regime=rg, ts_ms=now_ms, default_t0=d0, default_t1=d1, default_t2=d2)
                        t_decis = runtime.dn_calib.tiers(regime=rg, ts_ms=0, default_t0=d0, default_t1=d1, default_t2=d2)

                        # Metrics
                        from services.orderflow.metrics import dn_how_scale_gauge, of_dn_how_ratio_t1_gauge
                        with contextlib.suppress(Exception):
                            dn_how_scale_gauge.labels(symbol=runtime.symbol, regime=rg).set(t_telem.scale)

                        ratio = 1.0
                        if t_decis.tier1_usd > 0:
                            ratio = t_telem.tier1_usd / t_decis.tier1_usd
                        with contextlib.suppress(Exception):
                            of_dn_how_ratio_t1_gauge.labels(symbol=runtime.symbol, regime=rg).set(ratio)

                        # Report
                        if ratio < 0.8 or ratio > 1.2:
                            msg = (
                                f"Liquidity Divergence Report ({runtime.symbol})\n"
                                f"Regime: {rg}\n"
                                f"HourOfWeek: {t_telem.hour_of_week}\n"
                                f"Global Liq (EMA): ${t_telem.g_liq_ema:,.0f}\n"
                                f"Bucket Liq (EMA): ${t_telem.b_liq_ema:,.0f}\n"
                                f"Scale Factor: {t_telem.scale:.2f}x\n"
                                f"Tier1 (Decision): ${t_decis.tier1_usd:,.0f}\n"
                                f"Tier1 (Telemetry): ${t_telem.tier1_usd:,.0f}\n"
                                f"Ratio: {ratio:.2f}"
                            )
                            await self.signal_pipeline.send_telegram_report(text=msg, source="liq_divergence", symbol=runtime.symbol)
                        runtime.last_dn_how_report_ts_ms = now_ms

                    # 3. Persistence
                    if bool(int(runtime.config.get("calib_persist_enable", 1))):
                        runtime._calib_bars_since_persist = int(getattr(runtime, "_calib_bars_since_persist", 0) or 0) + 1
                        min_bars = int(runtime.config.get("calib_persist_min_bars", 60))
                        if getattr(runtime, "_calib_bars_since_persist", 0) >= min_bars:
                            runtime._calib_bars_since_persist = 0
                            svc = getattr(self, "calib_svc", None)
                            if svc is not None:
                                await asyncio.gather(
                                    svc.persist_dn(runtime, regime=rg, ts_ms=bar.end_ts_ms),
                                    svc.persist_tick_dn(runtime, regime=rg, ts_ms=bar.end_ts_ms)
                                )

            except Exception as exc:
                 log_silent_error(exc, 'calib_update_failure', runtime.symbol, '_on_microbar_closed:dn_calib')
                 pass


            # ATR TF Selector (UNIFIED - single source of truth: atr_tf_selected)
            # Shadow mode: compute candidate, no apply. Enforce mode: apply candidate to selected.
            # ------------------------------------------------------------------
            try:
                if self._env.atr_tf_calib_enable:
                    now_ts = int(getattr(bar, "end_ts_ms", 0) or 0)
                    close_px = getattr(bar, "close", 0.0) or 0.0
                    rg = str(getattr(runtime, "last_regime", "na") or "na").lower()

                    # Throttle: do not recompute too often (Redis reads for multiple TF)
                    refresh_ms = int(runtime.config.get("atr_tf_calib_refresh_ms", 60_000))
                    last = int(runtime.dynamic_cfg.get(DK.ATR_TF_CALIB_LAST_MS, 0) or 0)
                    if refresh_ms < 10_000:
                        refresh_ms = 10_000
                    if now_ts > 0 and (now_ts - last) >= refresh_ms and close_px > 0:
                        runtime.dynamic_cfg[DK.ATR_TF_CALIB_LAST_MS] = now_ts

                        # Candidate TFs list (env-tunable)
                        tfs_raw = self._env.atr_tf_calib_tfs
                        tfs = [x.strip() for x in tfs_raw.split(",") if x.strip()]
                        if not tfs:
                            tfs = ["1m", "5m", "15m", "1h"]

                        # Compute target from fees-aware gate (rocket_v1) to avoid permanent veto
                        # NOTE: this is *sanity* target; unified gate still uses max(floor,fees).
                        target_bps = 0.0
                        try:
                            tp_ratios = parse_tp_ratio(runtime.config.get("tp_ratio") or runtime.config.get("tp_rr") or "")
                            tp1_share = tp_ratios[0] if tp_ratios else 0.5
                            # Use signal_pipeline for rocket logic
                            rocket_mult = self.signal_pipeline._get_rocket_multiplier(runtime.symbol) or 0.0
                            denom = tp1_share * rocket_mult
                            if denom > 0:
                                target_bps = (self.signal_pipeline.FEES_BPS_RT + self.signal_pipeline.TP_BPS_BUFFER) / denom
                        except Exception:
                            target_bps = 0.0

                        # Collect atr_bps for each TF (best-effort; if tf missing -> skip)
                        atr_bps_by_tf: dict[str, float] = {}
                        cache = getattr(self, "atr_cache", None)
                        if cache is not None:
                            for tf in tfs:
                                try:
                                    # Use raw cache lookup to bypass calibration logic itself
                                    atr_tf = cache.get(runtime.symbol, tf) or 0.0
                                    if atr_tf > 0:
                                        atr_bps_by_tf[tf] = 10000.0 * (atr_tf / close_px)
                                except Exception as exc:
                                    log_silent_error(exc, 'calib_update_failure', runtime.symbol, '_handle_tick:atr_tf_update')
                                    continue

                        if atr_bps_by_tf:
                            runtime.atr_tf_calib.update_many(regime=rg, atr_bps_by_tf=atr_bps_by_tf)

                            # Recommend TF (switching controlled by hold-down + hysteresis)
                            fallback_tf = str(runtime.config.get("atr_tf", os.getenv("ATR_TF", "1m")) or "1m")
                            current_tf = runtime.get_atr_tf_selected()  # Use canonical resolver
                            mode = self._env.atr_tf_selector_mode  # "shadow"|"enforce"
                            allow_switch = (mode == "enforce")
                            runtime.dynamic_cfg[DK.ATR_TF_MODE] = mode

                            choice = runtime.atr_tf_calib.recommend_tf(
                                regime=rg,
                                target_bps=target_bps,
                                fallback_tf=fallback_tf,
                                now_ts_ms=now_ts,
                                current_tf=current_tf,
                                allow_switch=allow_switch,
                            )

                            runtime.dynamic_cfg[DK.ATR_TF_TARGET_BPS] = choice.target_bps
                            runtime.dynamic_cfg[DK.ATR_TF_READY] = 1 if choice.src != "static" and choice.n >= self._env.atr_tf_calib_min_samples else 0
                            runtime.dynamic_cfg[DK.ATR_TF_SRC] = choice.src
                            runtime.dynamic_cfg[DK.ATR_TF_N] = choice.n
                            # Telemetry: always write candidate (for observability)
                            runtime.dynamic_cfg[DK.ATR_TF_CANDIDATE] = choice.tf
                            runtime.dynamic_cfg[DK.ATR_TF_CANDIDATE_SRC] = choice.src
                            runtime.dynamic_cfg[DK.ATR_TF_CANDIDATE_N] = choice.n
                            runtime.dynamic_cfg[DK.ATR_TF_CANDIDATE_SCORE] = getattr(choice, "score", 0.0) or 0.0
                            runtime.dynamic_cfg[DK.ATR_TF_CANDIDATES_BPS] = dict(atr_bps_by_tf)

                            # Update metrics
                            atr_tf_target_bps.labels(symbol=runtime.symbol).set(target_bps)
                            atr_tf_candidate_score.labels(symbol=runtime.symbol).set(getattr(choice, "score", 0.0) or 0.0)
                            candidate_diff = 1 if choice.tf != current_tf else 0
                            atr_tf_candidate_diff.labels(symbol=runtime.symbol).set(candidate_diff)

                            # Apply: ONLY in enforce mode
                            if allow_switch and choice.tf != current_tf:
                                prev_tf = current_tf
                                new_tf = choice.tf
                                runtime.dynamic_cfg[DK.ATR_TF_SELECTED] = new_tf
                                runtime.dynamic_cfg[DK.ATR_TF_LAST_SWITCH_TS_MS] = now_ts
                                # Log switch (rate-limited)
                                logger.info(
                                    "🔄 (%s) ATR-TF switch: %s → %s (target_bps=%.1f, src=%s, n=%d)",
                                    runtime.symbol, prev_tf, new_tf, target_bps, choice.src, choice.n
                                )
                                # Increment switch counter
                                atr_tf_switch_total.labels(symbol=runtime.symbol).inc()
                            elif not allow_switch:
                                # Shadow mode: ensure selected is initialized but don't change it
                                runtime.dynamic_cfg.setdefault("atr_tf_selected", current_tf)

                            # Persist selected TF (throttled, only in enforce or on init)
                            persist_gap = int(runtime.config.get("atr_tf_calib_persist_gap_ms", 300_000))
                            if persist_gap < 60_000:
                                persist_gap = 60_000
                            last_p = int(getattr(runtime, "_atr_tf_last_persist_ts_ms", 0) or 0)
                            if now_ts > 0 and (now_ts - last_p) >= persist_gap and allow_switch:
                                runtime._atr_tf_last_persist_ts_ms = now_ts
                                choice_state = {
                                    "tf": runtime.get_atr_tf_selected(),
                                    "src": choice.src,
                                    "updated_ts_ms": now_ts
                                }
                                svc = getattr(self, "calib_svc", None)
                                if svc is not None:
                                    await svc.persist_atr_tf_choice(runtime, choice_state=choice_state, ts_ms=now_ts)
            except Exception:
                pass


            # --- ADX quantile snapshot (deterministic by bar end ts) ---
            # We store in runtime.dynamic_cfg for later use in snapshot publisher.
            # Source of truth:
            #  - adx14 is in Redis key adx:{symbol} (float)
            #  - quantiles are in Redis key regime:q:{symbol}:1m (json)
            # Here we only read adx14 (cheap); adx_q is computed in snapshot publisher.
            try:
                # best-effort; fail-open
                adx_raw = await self.redis.get(f"adx:{runtime.symbol}")
                runtime.dynamic_cfg[DK.ADX14] = float(adx_raw) if adx_raw is not None else 0.0
            except Exception:
                pass

            # 1) RSI updates
            try:
                runtime.rsi_price.update(bar.close)
                runtime.rsi_cvd.update(bar.cvd_close)
            except Exception:
                pass

            # Metric: bars closed
            bars_closed_total.labels(symbol=runtime.symbol, tf=str(getattr(bar, "tf_ms", "0"))).inc()


            # ------------------------------------------------------------
            # Phase C: ATR TF selection + ATR caching for bar_close.
            # Goal:
            #  - choose best timeframe/source by freshness+consistency
            #  - store deterministic choice for later tick/execution use
            # Fail-open:
            #  - if selector fails, fall back to cfg atr_tf
            # ------------------------------------------------------------
            atr_val = float(getattr(runtime, "last_atr", 0.0) or 0.0)
            try:
                now_ts = bar.end_ts_ms
                refresh_ms = int(runtime.config.get("eq_atr_refresh_ms", 15_000))
                if refresh_ms < 1_000:
                    refresh_ms = 1_000

                if (now_ts - (getattr(runtime, "last_atr_ts_ms", 0) or 0)) >= refresh_ms:
                    close_px = float(getattr(bar, "close", 0.0) or 0.0)
                    # 1) Use canonical TF resolver (single source of truth)
                    tf_sel = runtime.get_atr_tf_selected()
                    # NOTE: legacy atr_tf_sel.choose() block removed — class never existed.
                    # Single source of truth is the unified selector in _on_microbar_closed.

                    # 2) fetch ATR using selected TF (best-effort)
                    atr_tmp = 0.0
                    atr_meta = None
                    cache = getattr(self, "atr_cache", None)
                    if cache is not None:
                        try:
                            atr_tmp, atr_meta = cache.get_with_meta(symbol=runtime.symbol, timeframe=tf_sel, now_ms=now_ts)
                            atr_tmp = atr_tmp or 0.0
                            # expose meta for audit/debug
                            if isinstance(atr_meta, dict):
                                runtime.dynamic_cfg[DK.ATR_LIVE_SRC] = (atr_meta.get("src", "na"))
                                runtime.dynamic_cfg[DK.ATR_LIVE_KEY] = (atr_meta.get("key", ""))
                                runtime.dynamic_cfg[DK.ATR_LIVE_AGE_MS] = int(atr_meta.get("age_ms", 0) or 0)
                        except Exception:
                            atr_tmp = 0.0

                    if atr_tmp > 0:
                        # Sanitize live ATR too (keeps last_atr consistent across the system)
                        try:
                            px0 = float(getattr(runtime, "last_px", 0.0) or 0.0)
                            age0 = 0
                            if isinstance(atr_meta, dict):
                                age0 = int(atr_meta.get("age_ms", 0) or 0)
                            res = self._atr_sanity.update(
                                symbol=runtime.symbol,
                                atr=atr_tmp,
                                px=px0,
                                age_ms=age0,
                                now_ms=now_ts,
                                tf=(atr_meta.get("tf", "1m")) if isinstance(atr_meta, dict) else "1m",
                            )
                            runtime.last_atr = res.atr_used
                            runtime.last_atr_ts_ms = now_ts
                            runtime.dynamic_cfg[DK.ATR_BAD] = res.bad
                            runtime.dynamic_cfg[DK.ATR_BAD_REASON] = res.reason or ""
                        except Exception:
                            runtime.last_atr = atr_tmp
                            runtime.last_atr_ts_ms = now_ts
            except Exception:
                pass

            # ------------------------------------------------------------------
            # ATR floor tiers (per-symbol/per-regime) -> runtime.dynamic_cfg
            # Purpose:
            #   Fix "broken chain": ATR tiers must be selected later by tick-gate.
            # Deterministic time:
            #   uses bar.end_ts_ms and runtime.last_regime (bar-close derived).


            # 2) Swings and Divergences
            try:
                swings = runtime.swing.update(bar)
                for sp in swings:
                    # Rate limit logs: only 1 in 50
                    sp_cnt = self.swing_point_counters.get(runtime.symbol, 0) + 1
                    self.swing_point_counters[runtime.symbol] = sp_cnt

                    if sp_cnt % 50 == 0:
                         self.logger.info("📐 Swing Point detected (%s): kind=%s, price=%.2f, ts_ms=%d (x%d)", runtime.symbol, sp.kind, sp.price, sp.ts_ms, sp_cnt)

                    if sp.kind == "high":
                        runtime.prev_swing_high = runtime.last_swing_high
                        runtime.last_swing_high = sp
                    elif sp.kind == "low":
                        runtime.prev_swing_low = runtime.last_swing_low
                        runtime.last_swing_low = sp

                    # Hidden divergence requires trend bias.
                    bias = "none"
                    if getattr(runtime, "cont_ctx_trend_dir", None):
                         td = runtime.cont_ctx_trend_dir.upper()
                         bias = "UP" if td == "LONG" else "DOWN" if td == "SHORT" else "none"
                    else:
                         if runtime.last_swing_high and bar.close >= runtime.last_swing_high.price:
                             bias = "UP"
                         elif runtime.last_swing_low and bar.close <= runtime.last_swing_low.price:
                             bias = "DOWN"

                    # Check Hidden Divergence
                    divs_swing = runtime.divergence.update_swing(sp, trend_bias=bias)
                    if divs_swing:
                        runtime.last_div = divs_swing[-1]
                        for d in divs_swing:
                            divergence_detected_total.labels(symbol=runtime.symbol, kind=d.kind).inc()
                            self.logger.info("💎 Divergence Detected (%s): kind=%s, strength=%.2f", runtime.symbol, d.kind, d.strength)

                            # --- Unified Divergence/Pools Signal Publishing ---
                            try:
                                # 1. Features
                                feats = {}
                                try:
                                    feats["deltaSpikeZ"] = 0.0  # Not directly available in swing context
                                    feats["weak_progress"] = int(getattr(runtime.last_wp, "is_weak", 0)) if runtime.last_wp else 0
                                    feats["regime"] = str(getattr(runtime, "last_regime", "na"))
                                    feats["atr_mult"] = 0.0  # Placeholder since ATR usually part of specific rule config
                                    # Additional context if available
                                    if hasattr(runtime, "last_spread_bps"):
                                        feats["spread_bps"] = runtime.last_spread_bps
                                except Exception:
                                    pass

                                # 2. Nearest Pool (mature only)
                                npool_info = None
                                try:
                                    # Find nearest pool of ANY kind to the current price
                                    pools_all = runtime.eq_pools.pools(kind=None, only_mature=True)
                                    if pools_all:
                                        # Sort by distance to bar.close
                                        pools_all.sort(key=lambda p: abs(p.level - bar.close))
                                        np = pools_all[0]
                                        npool_info = {
                                            "id": str(getattr(np, "pool_id", "")),
                                            "kind": str(getattr(np, "kind", "")),
                                            "level": float(getattr(np, "level", 0.0)),
                                            "dist_px": abs(np.level - bar.close)
                                        }
                                except Exception:
                                    pass

                                # 3. Payload
                                payload = {
                                    "signal_type": "Divergence",
                                    "symbol": runtime.symbol,
                                    "tf": str(runtime.config.get("micro_tf", "1s")),
                                    "ts_ms": d.ts_ms,
                                    "side_bias": str(bias),
                                    "divergence_kind": d.kind,
                                    "strength": d.strength,
                                    "confidence": min(0.99, d.strength / 10.0),  # Simple confidence estimation
                                    "features": feats,
                                    "nearest_pool": npool_info,
                                    "generated_at": get_ny_time_millis(),
                                    # Standard fields for compatibility
                                    "reason": f"divergence_{d.kind}",
                                    "entry": d.price_curr,
                                    "price": d.price_curr,
                                    "cvd": d.cvd_curr
                                }

                                # 4. Publish to signals:crypto:raw
                                # We use xadd directly here to ensure it goes to the unified stream immediately
                                stream_key = RS.CRYPTO_RAW
                                pl_json = json.dumps(payload, default=str, ensure_ascii=False)
                                safe_create_task(self.ticks.xadd(stream_key, {"payload": pl_json}, maxlen=20000))

                            except Exception as ex:
                                self.logger.warning(f"⚠️ Failed to publish Divergence signal: {ex}")

                    # Update EQ pools from swing points
                    with contextlib.suppress(Exception):
                        runtime.eq_pools.on_swing(sp, atr=atr_val)

            except Exception:
                pass

            # --- Dynamic calibration update (eff_quote / min_quote_delta) ---
            try:
                if bool(getattr(bar, "fp_enabled", False)):
                    eff_q = float(getattr(bar, "fp_eff_quote", 0.0) or 0.0)
                    qd = float(getattr(bar, "fp_quote_delta", 0.0) or 0.0)
                    from contexts import MARKET_REGIME_NA, normalize_regime_label
                    regime = normalize_regime_label(getattr(runtime, "last_regime", MARKET_REGIME_NA))
                    runtime.eff_calib.update(regime=regime, eff_quote=eff_q, quote_delta=qd)

                    # Tier policy by regime
                    tier = int(runtime.config.get("abs_lvl_tier_default", 1))
                    if regime in ("range",):
                        tier = int(runtime.config.get("abs_lvl_tier_range", 1))
                    elif regime in ("trend", "trending_bull", "trending_bear"):
                        tier = int(runtime.config.get("abs_lvl_tier_trend", 0))
                    elif regime in ("thin", "news", "illiquid"):
                        tier = int(runtime.config.get("abs_lvl_tier_thin", 2))

                    th = runtime.eff_calib.thresholds(
                        regime=regime,
                        default_eff_th=float(runtime.config.get("abs_lvl_eff_quote_th", 0.0020)),
                        default_min_qd=float(runtime.config.get("abs_lvl_min_quote_delta", 0.0)),
                        tier=tier,
                    )
                    runtime.dynamic_cfg[DK.ABS_LVL_EFF_QUOTE_TH] = th.eff_quote_th
                    runtime.dynamic_cfg[DK.ABS_LVL_MIN_QUOTE_DELTA] = th.min_quote_delta
                    runtime.dynamic_cfg[DK.ABS_LVL_CALIB_N] = th.n
                    runtime.dynamic_cfg[DK.ABS_LVL_CALIB_SRC] = th.src
                    runtime.dynamic_cfg[DK.ABS_LVL_TIER] = tier

                    stab = runtime._th_stab.update(th.eff_quote_th)
                    runtime.dynamic_cfg[DK.ABS_LVL_TH_EMA] = stab.ema
                    runtime.dynamic_cfg[DK.ABS_LVL_TH_DRIFT] = stab.drift
                    runtime.dynamic_cfg[DK.ABS_LVL_TH_RANGE_NORM] = stab.range_norm
                    runtime.dynamic_cfg[DK.ABS_LVL_TH_STAB_N] = stab.n

                    drift_max = float(runtime.config.get("abs_lvl_th_drift_max", 0.35))
                    range_max = float(runtime.config.get("abs_lvl_th_range_max", 1.20))
                    unstable = int((stab.drift > drift_max) or (stab.range_norm > range_max))
                    runtime.dynamic_cfg[DK.ABS_LVL_TH_UNSTABLE] = unstable

                    # Dynamic strictness: if unstable or thin/news -> need=3
                    if bool(runtime.config.get("strong_dynamic_need_enable", 1)):
                        if unstable or regime in ("thin", "news", "illiquid"):
                            runtime.dynamic_cfg[DK.STRONG_NEED_REVERSAL] = 3
                            runtime.dynamic_cfg[DK.STRONG_NEED_CONTINUATION] = 3
                        else:
                            runtime.dynamic_cfg[DK.STRONG_NEED_REVERSAL] = int(runtime.config.get("strong_need_reversal", 2))
                            runtime.dynamic_cfg[DK.STRONG_NEED_CONTINUATION] = int(runtime.config.get("strong_need_continuation", 2))

                    # --- Persist calibration (throttled, deterministic by bar time) ---
                    if bool(runtime.config.get("calib_persist_enable", 1)):
                        runtime._calib_bars_since_persist += 1
                        min_bars = int(runtime.config.get("calib_persist_min_bars", 120))
                        min_dt = int(runtime.config.get("calib_persist_min_interval_ms", 60_000))
                        ts_ms = getattr(bar, "end_ts_ms", 0) or 0
                        last = getattr(runtime, "_calib_last_persist_ts_ms", 0) or 0

                        due = (runtime._calib_bars_since_persist >= min_bars) or (ts_ms > 0 and last > 0 and (ts_ms - last) >= min_dt)
                        if due and ts_ms > 0:
                            runtime._calib_last_persist_ts_ms = ts_ms
                            runtime._calib_bars_since_persist = 0
                            # regime label should match what you used for update()
                            rg = getattr(runtime, "last_regime", "na") or "na"
                            if self.calib_svc:
                                safe_create_task(self.calib_svc.persist_effq(runtime, regime=rg, ts_ms=ts_ms))


            except Exception:
                pass

            # C) Rolling CVD Snapshot (for UI/QA)
            # Writes to LIST: cvd:snap:{symbol}
            if self._env.cvd_snapshot_enable:
                try:
                    # Format: "{ts_ms},{cvd},{cvd_ema},{cvd_slope}"
                    # For now, just cvd, others 0.0
                    val_str = f"{bar.end_ts_ms},{bar.cvd_close:.2f},0.0,0.0"
                    snap_key = f"cvd:snap:{runtime.symbol}"

                    # Use pipeline for atomicity if possible, or just gather
                    # Need to verify if self.ticks supports pipeline easily (it is redis client)
                    # Just sequential await is fine for now as it's fire-and-forget logic
                    await self.ticks.lpush(snap_key, val_str)
                    await self.ticks.ltrim(snap_key, 0, 3599) # Keep last 3600 (1 hour @ 1s)
                    await self.ticks.expire(snap_key, 21600)  # TTL 6 hours
                except Exception:
                    pass


            # 3) Footprint diagnostics
            if getattr(bar, "fp_evictions", 0) > 0:
                fp_buckets_evicted_total.labels(symbol=runtime.symbol).inc(bar.fp_evictions)


            # Phase C: sweep detection using mature pools.
            try:
                mature = runtime.eq_pools.pools(only_mature=True)
                sweeps = runtime.sweep.update_bar(bar, pools=mature)
                if sweeps:
                    sw = sweeps[-1]
                    runtime.last_sweep = sw
                    # Store baseline CVD at sweep bar close
                    try:
                        runtime.last_sweep_ts_ms = getattr(sw, "ts_ms", 0) or bar.end_ts_ms
                        runtime.last_sweep_cvd = float(getattr(bar, "cvd_close", 0.0) or 0.0)
                    except Exception:
                        pass
                    sweep_detected_total.labels(symbol=runtime.symbol, eq_kind=sw.kind).inc()
                    # start reclaim FSM on sweep return
                    runtime.reclaim.on_sweep_return(runtime.last_sweep)
                    # FIX: prevent reclaim on same bar
                    runtime.reclaim_start_ts_ms = getattr(sw, "ts_ms", 0)
            except Exception:
                pass

            # Reclaim FSM progress on each bar close
            try:
                # FIX: ignore same bar
                if (getattr(runtime, "reclaim_start_ts_ms", 0) or 0) == bar.end_ts_ms:
                    pass
                else:
                    ev = runtime.reclaim.on_bar_close(bar)
                    if ev is not None:
                        runtime.last_reclaim = ev

                        # ------------------------------------------------------------
                        # Phase E: CVD Reclaim Evidence (bonus-evidence)
                        # ------------------------------------------------------------
                        try:
                            # Always try to compute if we have sweep baseline
                            if (runtime.config.get("cvd_reclaim_enable", 1) == 1 and
                                runtime.last_sweep_ts_ms > 0):

                                res = compute_cvd_reclaim(
                                    ts_ms=ev.ts_ms,
                                    sweep_ts_ms=runtime.last_sweep_ts_ms,
                                    cvd_sweep=runtime.last_sweep_cvd,
                                    reclaim_ts_ms=ev.ts_ms,
                                    cvd_reclaim=bar.cvd_close,
                                    direction_bias=ev.direction_bias,
                                    min_abs=float(runtime.config.get("cvd_reclaim_min_abs", 0.0)),
                                    sat_abs=float(runtime.config.get("cvd_reclaim_sat_abs", 0.0)),
                                )
                                runtime.last_cvd_reclaim = res

                                cvd_reclaim_eval_total.labels(symbol=runtime.symbol, bias=ev.direction_bias).inc()
                                if res.ok:
                                    cvd_reclaim_ok_total.labels(symbol=runtime.symbol, bias=ev.direction_bias).inc()

                                self.logger.info(
                                    "CVDReclaim computed sym=%s bias=%s ok=%d score=%.3f delta=%.1f window_ms=%d",
                                    runtime.symbol, ev.direction_bias, res.ok, res.score, res.cvd_delta, (ev.ts_ms - runtime.last_sweep_ts_ms)
                                )
                        except Exception:
                            pass
            except Exception:
                pass

            # --- Weak progress snapshot ---
            try:
                runtime.last_wp = compute_weak_progress(bar, atr_val, runtime.config)
                # Update WeakProgressDetector history (trend-of-absorption)
                try:
                    if runtime.last_wp is not None:
                        runtime.weak_progress_det.push(runtime.last_wp, ts_ms=bar.end_ts_ms)
                except Exception:
                    pass
            except Exception:
                runtime.last_wp = None

            # --- Footprint edge absorb ---
            try:
                fe = runtime.fp_edge.update_bar(bar, runtime.config)
                if fe is not None:
                    runtime.last_fp_edge = fe
            except Exception:
                pass

            # ------------------------------------------------------------------
            # Variant A: Publish microbar_closed for decentralized services
            # ------------------------------------------------------------------
            try:
                bar_out = {
                    "type": "microbar_closed",
                    "symbol": runtime.symbol,
                    "ts_ms": bar.end_ts_ms,
                    "open": bar.open,
                    "high": bar.high,
                    "low": bar.low,
                    "close": bar.close,
                    "vol": bar.vol,
                    "cvd": bar.cvd_close,
                    # Metadata needed by OFConfirmEngine
                    "weak_progress": runtime.last_wp.weak_any if runtime.last_wp else False,
                    "sweep": {
                        "kind": runtime.last_sweep.kind,
                        "ts_ms": runtime.last_sweep.ts_ms
                    } if runtime.last_sweep else None,
                    "regime": getattr(runtime, "last_regime", "na"),
                    "reclaim": {
                        "hold_bars": runtime.last_reclaim.hold_bars,
                        "ts_ms": runtime.last_reclaim.ts_ms
                    } if runtime.last_reclaim else None,
                    "last_div_kind": runtime.last_div.kind if runtime.last_div else None,
                    "generated_at": get_ny_time_millis()
                }
                # Best practice: optionally split retention per symbol so minors are not evicted by majors
                from services.orderflow.microbar_publish import publish_microbar_closed
                safe_create_task(
                    publish_microbar_closed(
                        redis_client=self.redis,
                        symbol=runtime.symbol,
                        payload_obj=bar_out
                    )
                )
            except Exception as e:
                logger.error(f"Failed to publish microbar_closed event: {e}")

            # ------------------------------------------------------------------
            # Adaptive Pressure Proxy Calibration (Tick-Level)
            # ------------------------------------------------------------------
            try:
                now_ms = int(getattr(bar, "end_ts_ms", 0) or 0)
                calib_min_samples = self._env.pressure_tier_calib_min_samples
                calib_refresh_ms = self._env.pressure_tier_calib_refresh_ms

                last_update = int(getattr(runtime, "ptier_last_update_ts_ms", 0) or 0)
                if now_ms > 0 and (now_ms - last_update) >= calib_refresh_ms:
                     # Clone deque to list for sorting
                     samples = list(runtime.ptier_samples_usd)
                     if len(samples) >= calib_min_samples:
                         samples.sort()
                         n = len(samples)
                         def _q(p): return samples[int(p * (n - 1))]

                         p75 = _q(0.75)
                         p90 = _q(0.90)
                         p97 = _q(0.97)

                         # Clamp (safety)
                         min_usd = self._env.pressure_tier_min_usd
                         max_usd = self._env.pressure_tier_max_usd

                         def _clamp_usd(x): return max(min_usd, min(max_usd, x))

                         t0 = _clamp_usd(p75)
                         t1 = _clamp_usd(p90)
                         t2 = _clamp_usd(p97)

                         runtime.dynamic_cfg[DK.PRESSURE_TIER0_USD] = t0
                         runtime.dynamic_cfg[DK.PRESSURE_TIER1_USD] = t1
                         runtime.dynamic_cfg[DK.PRESSURE_TIER2_USD] = t2

                         runtime.ptier_last_update_ts_ms = now_ms

                         # Log calibration
                         self.logger.info(
                             "⚖️ [PTIER-CALIB] (%s) Updated thresholds (n=%d): T0=$%.0f, T1=$%.0f, T2=$%.0f",
                             runtime.symbol, n, t0, t1, t2
                         )
            except Exception as exc:
                log_silent_error(exc, 'calib_update_failure', runtime.symbol, '_on_microbar_closed:ptier_calib')

            # ------------------------------------------------------------
            # Pressure Tier Calibrator (Expert Recommendation - Production Ready)
            # Regime-aware quantile-based adaptive thresholds with hysteresis
            # ------------------------------------------------------------
            try:
                rg = (getattr(runtime, "last_regime", "na") or "na").lower()
                tiers = runtime.ptier_calib.maybe_recompute(now_ms=now_ms, regime=rg)

                if tiers:
                    # Update telemetry-only keys in dynamic_cfg
                    runtime.dynamic_cfg[DK.PTIER_TIER0_USD] = tiers["tier0"]
                    runtime.dynamic_cfg[DK.PTIER_TIER1_USD] = tiers["tier1"]
                    runtime.dynamic_cfg[DK.PTIER_TIER2_USD] = tiers["tier2"]

                    # Update telemetry metrics
                    ptier_tier0_usd.labels(symbol=runtime.symbol).set(tiers["tier0"])
                    ptier_tier1_usd.labels(symbol=runtime.symbol).set(tiers["tier1"])
                    ptier_tier2_usd.labels(symbol=runtime.symbol).set(tiers["tier2"])

                    # NOTE: We no longer update dn_tier*, dn_tier_active, or dn_th_usd here.
                    # dn_calib (above) is now the sole authority for those keys.
                    # [EXPERT] Persistence disabled for telemetry-only ptier results.

                    # Log calibration (telemetry only)

            except Exception as exc:
                log_silent_error(exc, 'ptier_calib_failure', runtime.symbol, '_on_microbar_closed:ptier_calib')

            # ------------------------------------------------------------
            # SMT V2: Publish compact snapshot (BOS proxy, swings, OF state)
            # ------------------------------------------------------------
            await self._publish_smt_snapshot(runtime, bar)

    def _parse_tick_payload(self, payload: dict[str, Any]) -> dict[str, Any]:
            if "data" in payload:
                try:
                    nested = json.loads(payload["data"])
                except json.JSONDecodeError:
                    nested = {}
            else:
                nested = {}

            merged = {**payload, **nested}
            ts_ms = normalize_epoch_ms(merged.get("ts") or merged.get("event_time") or 0)
            tick: dict[str, Any] = {
                "symbol": merged.get("symbol"),
                "ts": ts_ms,      # legacy epoch ms (keep)
                "ts_ms": ts_ms,   # source of truth epoch ms
                "price": _safe_float(merged.get("price") or merged.get("last") or merged.get("mid")),
                "last": _safe_float(merged.get("last")),
                "bid": _safe_float(merged.get("bid")),
                "ask": _safe_float(merged.get("ask")),
                "qty": merged.get("qty") or merged.get("volume"),
                "side": str(merged.get("side") or merged.get("trade_side") or "BUY").upper(),
                "is_buyer_maker": merged.get("is_buyer_maker"),
                "written_at": _safe_int(merged.get("written_at")),
            }

            # Нормализация числовых полей и buyer/maker + mid
            try:
                qty = float(tick.get("qty", 0.0) or 0.0)
            except (TypeError, ValueError):
                qty = 0.0
            tick["qty"] = qty

            side_upper = (tick.get("side") or "").upper()
            if side_upper == "SELL":
                tick["is_buyer_maker"] = True
            elif side_upper == "BUY":
                tick["is_buyer_maker"] = False

            bid = _safe_float(tick.get("bid"))
            ask = _safe_float(tick.get("ask"))
            if bid and ask:
                tick["mid"] = (bid + ask) / 2.0
            else:
                tick["mid"] = _safe_float(tick.get("price"))

            return tick

    def _parse_book_payload(self, payload: dict[str, Any], symbol: str) -> dict[str, Any]:
            if "data" in payload:
                try:
                    nested = json.loads(payload["data"])
                except json.JSONDecodeError:
                    nested = {}
            else:
                nested = {}

            merged = {**payload, **nested}
            bids = _ensure_list_levels(merged.get("bids"))
            asks = _ensure_list_levels(merged.get("asks"))
            ts_ms = normalize_epoch_ms(merged.get("ts") or merged.get("event_time") or 0)

            book = {
                "symbol": symbol,
                "ts": ts_ms or 0,
                "ts_ms": ts_ms or 0,  # deterministic exchange timestamp (ms)
                "first_id": _safe_int(merged.get("first_id") or merged.get("firstId") or merged.get("U")),
                "final_id": _safe_int(merged.get("final_id") or merged.get("finalId") or merged.get("u")),
                "prev_final": _safe_int(merged.get("prev_final") or merged.get("pu")),
                "bids": bids,
                "asks": asks,
            }
            return book

    def _get_atr_for_symbol(self, symbol: str, cfg: dict[str, Any], tf_override: str | None = None, runtime: Any | None = None) -> float | None:
            """
            Delegates to MarketStateService.
            """
            try:
                # Single source of truth: atr_tf_selected (via canonical resolver)
                tf = str(
                    tf_override or
                    (runtime.get_atr_tf_selected() if runtime else None) or
                    cfg.get("atr_tf") or
                    os.getenv("ATR_TF", "1m") or
                    "1m"
                )
                return self.market_state.get_atr(symbol, tf)
            except Exception:
                return None

