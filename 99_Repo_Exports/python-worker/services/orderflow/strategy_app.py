from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import time
import zlib
from typing import Any

import redis.asyncio as aioredis
from redis.exceptions import RedisError

from core.redis_keys import RedisStreams as RS
from services.async_signal_publisher import AsyncSignalPublisher
from core.of_confirm_engine import OFConfirmEngine
from utils.atr_cache import ATRCache, get_atr_cache
from services.orderflow.market_state import MarketStateService
from services.orderflow.signal_pipeline import SignalPipeline
from services.signal_confidence import ConfidenceConfig, ConfidenceScorer
from core.atr_sanity import ATRSanity
from core.coingecko_snapshot import CoinGeckoSnapshotReader
from core.coingecko_macro_gate import CoinGeckoMacroGate
from orderflow_services.confidence_calibrator_bundle_runtime import ConfidenceCalibratorBundleRuntime

from services.orderflow.metrics_emitter import MetricsEmitter
from services.orderflow.tick_decision_engine import TickDecisionEngine
from services.orderflow.smt_snapshot_publisher import SMTSnapshotPublisher
from services.orderflow.order_payload_builder import OrderPayloadBuilder
from services.orderflow.signal_payload_builder import SignalPayloadBuilder
from services.orderflow.confidence_service import ConfidenceService
from services.orderflow.atr_resolver import ATRResolver

# Hot-path ENV cache
class _StrategyEnvCache:
    """Cached ENV variables for hot-path. Avoids ~50K+ syscalls/sec."""
    __slots__ = (
        '_ts', 'time_max_back_ms', 'time_warn_back_ms', 'last_px_ttl_sec',
        'atr_tf_calib_enable', 'atr_bps_calib_enable', 'atr_sanity_enable',
        'atr_tf_calib_max_age_ms', 'atr_tf_candidates', 'atr_tf_calib_persist_gap_ms',
        'atr_bps_calib_persist_gap_ms', 'atr_bps_calib_min_samples',
        'atr_tf_calib_min_samples', 'atr_tf_calib_tfs',
        'debug_deltas', 'cvd_snapshot_enable',
        'pressure_tier_calib_min_samples', 'pressure_tier_calib_refresh_ms',
        'pressure_tier_min_usd', 'pressure_tier_max_usd',
        'of_score_min',
        'spread_stale_book_gap_ms', 'spread_missing_cold_start_ms',
        'atr_tf_selector_mode',
    )

    def __init__(self) -> None:
        self._ts: float = 0.0
        self.refresh()

    def refresh(self) -> None:
        self._ts = time.monotonic()
        self.time_max_back_ms = int(os.getenv("TIME_MAX_BACK_MS", "2000"))
        self.time_warn_back_ms = int(os.getenv("TIME_WARN_BACK_MS", "500"))
        self.last_px_ttl_sec = int(os.getenv("LAST_PX_TTL_SEC", "600"))
        self.atr_tf_calib_enable = os.getenv("ATR_TF_CALIB_ENABLE", "1") == "1"
        self.atr_bps_calib_enable = os.getenv("ATR_BPS_CALIB_ENABLE", "1") == "1"
        self.atr_sanity_enable = os.getenv("ATR_SANITY_ENABLE", "1") == "1"
        self.atr_tf_calib_max_age_ms = int(os.getenv("ATR_TF_CALIB_MAX_AGE_MS", str(10 * 60_000)))
        self.atr_tf_candidates = os.getenv("ATR_TF_CANDIDATES", "1m,5m,15m")
        self.atr_tf_calib_persist_gap_ms = int(os.getenv("ATR_TF_CALIB_PERSIST_GAP_MS", "300000"))
        self.atr_bps_calib_persist_gap_ms = int(os.getenv("ATR_BPS_CALIB_PERSIST_GAP_MS", "120000"))
        self.atr_bps_calib_min_samples = int(os.getenv("ATR_BPS_CALIB_MIN_SAMPLES", "500"))
        self.atr_tf_calib_min_samples = int(os.getenv("ATR_TF_CALIB_MIN_SAMPLES", "30"))
        self.atr_tf_calib_tfs = os.getenv("ATR_TF_CALIB_TFS", "1m,5m,15m,1h")
        self.debug_deltas = os.getenv("DEBUG_DELTAS", "0") == "1"
        self.cvd_snapshot_enable = os.getenv("CVD_SNAPSHOT_ENABLE", "0") == "1"
        self.pressure_tier_calib_min_samples = int(os.getenv("PRESSURE_TIER_CALIB_MIN_SAMPLES", "300"))
        self.pressure_tier_calib_refresh_ms = int(os.getenv("PRESSURE_TIER_CALIB_REFRESH_MS", "60000"))
        self.pressure_tier_min_usd = float(os.getenv("PRESSURE_TIER_MIN_USD", "10000.0"))
        self.pressure_tier_max_usd = float(os.getenv("PRESSURE_TIER_MAX_USD", "5000000.0"))
        self.of_score_min = float(os.getenv("OF_SCORE_MIN", "0.60"))
        self.spread_stale_book_gap_ms = int(os.getenv("SPREAD_STALE_BOOK_GAP_MS", "30000"))
        self.spread_missing_cold_start_ms = int(os.getenv("SPREAD_MISSING_COLD_START_MS", "10000"))
        self.atr_tf_selector_mode = os.getenv("ATR_TF_SELECTOR_MODE", "enforce").lower()

    def maybe_refresh(self) -> None:
        if (time.monotonic() - self._ts) > 30.0:
            self.refresh()


class OrderFlowStrategy:
    """Facade orchestrating the orderflow execution pipeline via modularized services."""
    
    @staticmethod
    def _stable_bucket_0_99(sid: str) -> int:
        try:
            return zlib.crc32((sid or "").encode("utf-8")) % 100
        except Exception:
            return 0
            
    def __init__(self, redis: aioredis.Redis, ticks: Any, publisher: AsyncSignalPublisher,
                 of_engine: OFConfirmEngine, calib_svc=None,
                 notify_client: aioredis.Redis | None = None, notify_stream: str = RS.NOTIFY_TELEGRAM,
                 orders_queue_mt5: str = "", orders_queue_binance: str = ""):
        self.redis = redis
        self.ticks = ticks
        self.publisher = publisher
        self.of_engine = of_engine
        self.calib_svc = calib_svc
        self.notify_client = notify_client
        self.notify_stream = notify_stream
        self.orders_queue_mt5 = orders_queue_mt5
        self.orders_queue_binance = orders_queue_binance
        self.burst_audit_stream = os.getenv("BURST_AUDIT_STREAM", RS.BURST_AUDIT)
        self.logger = logging.getLogger("orderflow_strategy_facade")

        self.cg_reader = CoinGeckoSnapshotReader(self.redis)
        self.cg_reader.start()
        self.cg_macro_gate = CoinGeckoMacroGate()

        self.atr_cache: ATRCache = get_atr_cache()
        self.market_state = MarketStateService(redis_client=self.redis, atr_cache=self.atr_cache)
        self.signal_pipeline = SignalPipeline(publisher=self.publisher, atr_cache=self.atr_cache)
        
        self.low_conf_counters = {}
        self.strong_gate_counters = {}
        self.dn_gate_relaxed_counters = {}
        self.dn_gate_proxy_relaxed_counters = {}
        self.conf_relax_counters = {}
        self.adverse_continuation_counters = {}
        self.swing_point_counters = {}
        self.conf_scorer = ConfidenceScorer(cfg=ConfidenceConfig())
        self._env = _StrategyEnvCache()
        self._atr_sanity = ATRSanity(window=int(os.getenv("ATR_SANITY_WINDOW", "60")))

        # Configuration for Conf Cal
        self.conf_cal_gating_mode = os.getenv("confidence_cal_gating_mode", "raw").strip().lower()
        self.conf_cal_proof_path = os.getenv("CONF_CAL_PROOF_STATE_PATH", "/tmp/conf_cal_proof_state.json")
        bundle_path = os.getenv("CONF_CAL_CHAMPION_BUNDLE_PATH", "")
        self.conf_cal_runtime = None
        if bundle_path:
             try:
                 self.conf_cal_runtime = ConfidenceCalibratorBundleRuntime(bundle_path)
             except Exception:
                 self.logger.error("Failed to init ConfidenceCalibratorBundleRuntime with %s", bundle_path)

        challenger_path = os.getenv("CONF_CAL_CHALLENGER_BUNDLE_PATH", "")
        self.conf_cal_challenger_runtime = None
        if challenger_path:
             try:
                 self.conf_cal_challenger_runtime = ConfidenceCalibratorBundleRuntime(challenger_path)
             except Exception:
                 self.logger.error("Failed to init Challenger Bundle with %s", challenger_path)

        self.conf_cal_ab_mode = os.getenv("CONF_CAL_AB_MODE", "off").strip().lower()
        self.conf_cal_ab_share = float(os.getenv("CONF_CAL_AB_SHARE", "0.0"))
        self.conf_cal_ab_shadow = os.getenv("CONF_CAL_AB_SHADOW", "false").strip().lower() in ("true", "1", "yes")
        self.conf_cal_ab_sticky_key = os.getenv("CONF_CAL_AB_STICKY_KEY", "symbol|session")

        self.conf_cal_proof = None
        self.conf_cal_proof_mtime = 0.0
        self.conf_cal_proof_last_check_ms = 0
        
        # Modularized services initialization
        self.metrics_emitter = MetricsEmitter(self)
        self.tick_decision_engine = TickDecisionEngine(self)
        self.smt_publisher = SMTSnapshotPublisher(self)
        self.order_builder = OrderPayloadBuilder(self)
        self.signal_builder = SignalPayloadBuilder(self)
        self.confidence_service = ConfidenceService(self)
        self.atr_resolver = ATRResolver(self)

    async def process_tick(self, runtime, tick: dict[str, Any]) -> dict[str, Any] | None:
        self._env.maybe_refresh()
        return await self.tick_decision_engine.process_tick(runtime, tick)
        
    async def _on_microbar_closed(self, runtime, bar) -> None:
        await self.tick_decision_engine._on_microbar_closed(runtime, bar)

    async def _emit_payload(self, runtime, payload: dict[str, Any], now_ms: int) -> dict[str, Any] | None:
        return await self.signal_builder.emit_payload(runtime, payload, now_ms)
        
    def _apply_confidence_calibration(self, runtime, indicators: dict[str, Any], conf_raw: float, ctx: dict[str, Any]) -> float:
        return self.confidence_service.apply_confidence_calibration(runtime, indicators, conf_raw, ctx)
        
    async def _compute_confidence(self, runtime, indicators, confirmations, *, side, kind, features=None):
        return await self.confidence_service.compute_confidence(runtime, indicators, confirmations, side=side, kind=kind, features=features)

    def _get_atr_for_symbol(self, symbol: str, cfg: dict[str, Any], tf_override: str | None = None, runtime: Any | None = None) -> float | None:
        return self.atr_resolver.get_atr_for_symbol(symbol, cfg, tf_override, runtime)
        
    async def publish_signal(self, runtime, signal: dict[str, Any]) -> None:
        await self.signal_pipeline.publish_signal(runtime, signal)
        
    async def _publish_orders_queue(self, runtime, signal: dict[str, Any]) -> None:
        await self.order_builder.publish_orders_queue(runtime, signal)

    def _parse_tick_payload(self, payload: dict[str, Any]) -> dict[str, Any]:
        return self.tick_decision_engine._parse_tick_payload(payload)

    def _parse_book_payload(self, payload: dict[str, Any], symbol: str) -> dict[str, Any]:
        return self.tick_decision_engine._parse_book_payload(payload, symbol)

    def _log_metrics(self, runtime) -> None:
        self.metrics_emitter.log_metrics(runtime)

    async def _publish_smt_snapshot(self, runtime, bar) -> None:
        await self.smt_publisher.publish_smt_snapshot(runtime, bar)

    async def _burst_audit(self, *, runtime, now_ms: int, event: str, payload: dict[str, Any], indicators: dict[str, Any], extra: dict[str, Any]) -> None:
        try:
            cfg = runtime.config or {}
            if not bool(int(cfg.get("burst_audit_enable", 0))):
                return
            rate = float(cfg.get("burst_audit_sample", 0.05) or 0.05)
            # if not _should_sample(now_ms, rate):
            #     return
            from contexts import MARKET_REGIME_NA, normalize_regime_label
            msg: dict[str, Any] = {
                "type": "burst_audit",
                "ts_ms": str(now_ms),
                "symbol": runtime.symbol,
                "event": event,
                "payload": json.dumps(payload, ensure_ascii=False, separators=(",", ":")),
                "ind": json.dumps({
                    "scenario": indicators.get("strong_gate_scn") or "",
                    "of_score": indicators.get("of_confirm_score", 0.0),
                    "delta_z": indicators.get("delta_z", 0.0),
                    "pressure_sps": getattr(runtime, "pressure_sps", 0.0) or 0.0,
                    "pressure_hi": getattr(runtime, "pressure_hi", 0) or 0,
                    "regime": normalize_regime_label(getattr(runtime, "last_regime", MARKET_REGIME_NA)),
                    "spread_bp": (getattr(runtime, "last_spread_bps", 0.0) or 0.0),
                    "obi_age_ms": indicators.get("obi_age_ms", -1),
                    "iceberg_age_ms": indicators.get("iceberg_age_ms", -1),
                }, ensure_ascii=False, separators=(",", ":")),
                "extra": json.dumps(extra or {}, ensure_ascii=False, separators=(",", ":")),
            }
            await self.redis.xadd(self.burst_audit_stream, msg, maxlen=200000, approximate=True)  # type: ignore[arg-type]
        except Exception as exc:
            return

    async def _maybe_poll_symbol_overrides(self, runtime, now_ms: int) -> None:
        pass
