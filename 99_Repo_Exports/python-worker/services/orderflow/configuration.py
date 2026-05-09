
import asyncio
import json
import logging
import os
from copy import deepcopy
from typing import Any
from core.redis_keys import RedisStreams as RS

# Max time (seconds) to wait for a single Redis hgetall when loading per-symbol
# config overrides. Keeps load_dynamic_symbols from blocking the event loop if
# redis-worker-1 is temporarily overloaded. Set to 0 to disable.
_CONFIG_REDIS_TIMEOUT_S: float = float(os.getenv("ORDERFLOW_CONFIG_REDIS_TIMEOUT_S", "10"))
try:
    from redis.exceptions import RedisError  # type: ignore
except Exception:  # pragma: no cover
    # Optional dependency for unit-test environment / minimal installs.
    class RedisError(Exception):
        pass

from core.instrument_config import OrderFlowConfig, get_config
from services.pnl_math import get_symbol_info

logger = logging.getLogger("crypto_orderflow.config")

def _to_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in {"1", "true", "yes", "on"}

def _safe_float(value: Any, default: float = 0.0) -> float:
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default

def _safe_int(value: Any, default: int = 0) -> int:
    if value is None:
        return default
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default

def _ensure_list_levels(raw: Any) -> list[list[float]]:
    """
    Приводит уровни книги к формату [[price, qty], ...].
    """
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except json.JSONDecodeError:
            return []
    if not isinstance(raw, list):
        return []

    result: list[list[float]] = []
    for item in raw:
        if isinstance(item, (list, tuple)) and len(item) >= 2:
            price = _safe_float(item[0])
            qty = _safe_float(item[1])
            result.append([price, qty])
    return result

_FALLBACK_SYMBOLS: list[str] = [
    "BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT",
    "PEPEUSDT", "DOGEUSDT", "SHIBUSDT", "FLOKIUSDT", "BONKUSDT",
    "WIFUSDT", "SUIUSDT", "APTUSDT", "XAUUSDT"]

# If SYMBOLS env is set — use it exclusively (full replacement, not extension).
# This ensures that crypto-orderflow-service-2 (meme-coins only) does not
# accidentally spin up readers for SOLUSDT/BTCUSDT/ETHUSDT.
_env_symbols_raw = os.getenv("SYMBOLS", "")
if _env_symbols_raw:
    DEFAULT_SYMBOLS: list[str] = [s.strip().upper() for s in _env_symbols_raw.split(",") if s.strip()]
else:
    DEFAULT_SYMBOLS = list(_FALLBACK_SYMBOLS)

DEFAULT_CONFIG: dict[str, Any] = {
    "delta_window": 120,
    # "delta_z_threshold" purposely omitted to allow fallback to SymbolSpecs
    "delta_abs_min": 0.75,
    "delta_abs_min_confirm": 1.0,
    "min_confirmations": 1,
    "fp_imb_counts_for_min_confirmations": False,
    "obi_depth": 5,
    "obi_threshold": 0.4,
    "obi_hold_secs": 1.5,
    "absorption_min_volume": 15.0,
    "absorption_price_tolerance": 5.0,
    "absorption_window_sec": 8.0,
    "iceberg_refresh": 2,
    "iceberg_duration": 1.5,
    "weak_progress_atr": 0.15,
    "signal_cooldown_sec":3,
    "tick_buffer": 500,
    "tick_gap_snapshot_every_n": 500,

    # Book missing-seq EMA alpha (Binance depthUpdate continuity by U/u). Default 0.1 ~ 10 updates memory.
    "book_missing_seq_ema_alpha": _safe_float(os.getenv("BOOK_MISSING_SEQ_EMA_ALPHA", "0.1"), 0.1),

    # --- P2/F: Data Quality Gate (DQ) -----------------------------------
    # Rollout recommendation:
    #   1) DQ_GATE_ENABLE=1, DQ_GATE_MODE=penalty, DQ_MODE=safe
    #   2) Observe dq_level/dq_veto metrics for 24h
    #   3) Switch to DQ_MODE=strict; then DQ_GATE_MODE=enforce
    "dq_gate_enable": _safe_int(os.getenv("DQ_GATE_ENABLE", "0"), 0),
    "dq_gate_mode": os.getenv("DQ_GATE_MODE", "penalty").lower(),  # off|penalty|enforce|both
    "dq_mode": os.getenv("DQ_MODE", "safe").lower(),
    "dq_pen_max": _safe_float(os.getenv("DQ_PEN_MAX", "0.10"), 0.10),
    "dq_tick_gap_min_samples": _safe_int(os.getenv("DQ_TICK_GAP_MIN_SAMPLES", "50"), 50),

    # Policy v2 thresholds (optional overrides). If None/empty, dq_gate_v1
    # will apply defaults based on dq_mode (safe/strict).
    "dq_tick_gap_p95_ms_soft": _safe_float(os.getenv("DQ_TICK_GAP_P95_MS_SOFT", "0"), 0.0) or None,
    "dq_tick_gap_p95_ms_hard": _safe_float(os.getenv("DQ_TICK_GAP_P95_MS_HARD", "0"), 0.0) or None,
    "dq_tick_gap_p95_ms_extreme": _safe_float(os.getenv("DQ_TICK_GAP_P95_MS_EXTREME", "0"), 0.0) or None,
    "dq_tick_gap_requires_seq": _safe_int(os.getenv("DQ_TICK_GAP_REQUIRES_SEQ", "1"), 1),

    # Observe-only rollout: allow disabling HARD veto based on book_missing_seq_ema for first 24–48h.
    # Even if DQ_GATE_MODE=enforce, book veto will be ignored while dq_book_veto_enabled=0
    # (or while engine uptime < dq_book_veto_warmup_s, if warmup is set).
    "dq_book_veto_enabled": _safe_int(os.getenv("DQ_BOOK_VETO_ENABLED", "0"), 0),
    "dq_book_veto_warmup_s": _safe_int(os.getenv("DQ_BOOK_VETO_WARMUP_S", "0"), 0),

    "dq_tick_missing_seq_ema_soft": _safe_float(os.getenv("DQ_TICK_MISSING_SEQ_EMA_SOFT", "0"), 0.0) or None,
    "dq_tick_missing_seq_ema_hard": _safe_float(os.getenv("DQ_TICK_MISSING_SEQ_EMA_HARD", "0"), 0.0) or None,
    "dq_book_missing_seq_ema_soft": _safe_float(os.getenv("DQ_BOOK_MISSING_SEQ_EMA_SOFT", "0"), 0.0) or None,
    "dq_book_missing_seq_ema_hard": _safe_float(os.getenv("DQ_BOOK_MISSING_SEQ_EMA_HARD", "0"), 0.0) or None,

    "dq_data_health_min": _safe_float(os.getenv("DQ_DATA_HEALTH_MIN", "0.85"), 0.85),
    "dq_data_health_hard_min": _safe_float(os.getenv("DQ_DATA_HEALTH_HARD_MIN", "0.70"), 0.70),
    "dq_tick_age_ms_max": _safe_float(os.getenv("DQ_TICK_AGE_MS_MAX", "5000"), 5000.0),
    "dq_skew_ema_ms_max": _safe_float(os.getenv("DQ_SKEW_EMA_MS_MAX", "1000"), 1000.0),
    "orders_queue_enabled": False,
    "orders_queue_type": "market",
    "orders_queue_profile": "",
    "fallback_atr": 1.0,
    "min_lot": 0.01,
    "stop_mode": "ATR",
    "stop_atr_mult": 1.0,  # was 0.6, raised to reduce noise stop-outs,
    "stop_pct": 0.2,
    "stop_points": 1.0,
    "tp_rr": "1.3,2.0,2.7",
    "publish_of_inputs": 1,
    "of_inputs_emit_v2": 1,  # Deterministic version selection: 1=v2 (default), 0=v1,
    "of_inputs_stream": RS.OF_INPUTS,
    "of_inputs_stream_maxlen": 200000,
    "of_confirm_stream_maxlen": 50000,
    "confidence_weights": {
        "delta": 0.5,
        "speed": 0.2,
        "cluster": 0.2,
        "confirm": 0.1
    },
    "confidence_floor": 0.15,
    "confidence_cap": 0.95,
    "confidence_speed_scale": 2.0,
    "confidence_confirm_bonus": {
        "obi": 0.35,
        "absorption": 0.3,
        "iceberg_refresh": 0.35,
        "generic": 0.2,
    },
    # Expert Recommendations Configuration
    "require_strong_confirmation": _to_bool(os.getenv("CRYPTO_OF_REQUIRE_STRONG_CONFIRMATION", "false")),
    "strong_gate_shadow": _to_bool(os.getenv("CRYPTO_OF_STRONG_GATE_SHADOW", "false")),

    # P61: MLConfirmGate rollout control (shadow/canary/full)
    "ml_confirm_rollout": os.getenv("ML_CONFIRM_ROLLOUT_MODE", "shadow").lower(),
    "ml_confirm_canary_rate": float(os.getenv("ML_CONFIRM_CANARY_RATE", "0.05")),
    "ml_deny_allow_rule_strong": _to_bool(os.getenv("ML_DENY_ALLOW_RULE_STRONG", "true")),
    "ml_abstain_allow_rule_strong": _to_bool(os.getenv("ML_ABSTAIN_ALLOW_RULE_STRONG", "true")),

    # Telemetry-only (hour-of-week scale monitor)
    "dn_how_alert_ratio": float(os.getenv("DN_HOW_ALERT_RATIO", "1.5")),
    "dn_how_report_cooldown_s": int(os.getenv("DN_HOW_REPORT_COOLDOWN_S", "1800")),
    # Staleness & TTL (Round 3 modularization)
    "sweep_valid_ms": 120_000,
    "reclaim_signal_valid_ms": 120_000,
    "reclaim_hold_bars": 2,
    "obi_event_ttl_ms": 30_000,
    "obi_stable_min_secs": 1.0,
    "book_stale_ms": 15_000,
    # OBI Stability Tracker: canonical key used everywhere (book_processor.py, signal_confidence.py).
    # ENV: OBI_STABLE_SCORE_MIN (docker-compose default: 0.85)
    "obi_stable_score_min": float(os.getenv("OBI_STABLE_SCORE_MIN", "0.85")),
    "obi_stable_window_ms": 3000,
    "obi_deadband": 0.05,
    "obi_grace_ms": 250,
    # Phase E: CVD Reclaim (bonus-layer)
    "cvd_reclaim_enable": 1,
    "cvd_reclaim_valid_ms": 120_000,
    "cvd_reclaim_ratio_min": 1.2,
    "cvd_reclaim_lookback_n": 120,
    "cvd_reclaim_exclude_first_bar": 1,
    "cvd_reclaim_maxlen": 7200,  # ~2h @ 1s microbars
    # Bonus params (used by services/signal_confidence.py)
    "cvd_reclaim_bonus_lo": 1.0,
    "cvd_reclaim_bonus_hi": 1.8,
    "cvd_reclaim_bonus_w": 0.02,
    "cvd_reclaim_bonus_cap": 0.03,
    # Adverse Selection Gate
    "adverse_check_enable": _to_bool(os.getenv("ADVERSE_CHECK_ENABLE", "1")),
    # OBI stability bonus params (used by services/signal_confidence.py)
    "obi_stable_bonus_w": 0.04,
    "obi_stable_bonus_q_floor": 0.35,
    # Weak progress history (trend-of-absorption)
    "weak_history_maxlen": 50,
    "weak_recent_window": int(os.getenv("WEAK_RECENT_WINDOW", "5")),
    "weak_range_max_atr": 0.30,
    "weak_body_max_atr": 0.35,
    "weak_eff_max": 0.02,
    # Footprint edge absorb staleness
    "fp_edge_valid_ms": int(os.getenv("FP_EDGE_VALID_MS", "30000")),
    "iceberg_event_ttl_ms": 15_000,
    "iceberg_strict_refresh_min": 1,
    "iceberg_strict_duration_min": 1.0,
    "iceberg_strict_dist_bp": 5.0,
    # Scoring Weights (Round 4)
    "score_z_ref": 3.0,
    "w_z": 0.30,
    "w_wp": 0.15,
    "w_reclaim": 0.20,
    "w_obi": 0.15,
    "w_ice": 0.15,
    "w_abs": 0.05,
    "of_score_min": float(os.getenv("OF_SCORE_MIN", "0.60")),

    "publish_of_confirm": _to_bool(os.getenv("CRYPTO_OF_PUBLISH_CONFIRM", "false")),
    "of_confirm_stream": os.getenv("CRYPTO_OF_CONFIRM_STREAM", RS.OF_CONFIRM),
    "atr_bps_min_static": float(os.getenv("CRYPTO_ATR_BPS_MIN_STATIC", "0.0")),
    "atr_gate_audit_only": _to_bool(os.getenv("CRYPTO_ATR_GATE_AUDIT_ONLY", "false")),
    # === Cancellation Spike Gate ===
    "cancel_spike_enable": 1,
    "cancel_spike_mode": "veto",
    "cancel_spike_alpha_slow": 0.02,
    "cancel_spike_ratio_th": 3.0,
    "cancel_spike_abs_th": 0.0,
    "cancel_spike_min_baseline": 0.0,
    "cancel_spike_use_robust_z": 1,
    "cancel_spike_window": 120,
    "cancel_spike_min_samples": 30,
    "cancel_spike_z_th": 3.5,
    "cancel_spike_min_taker_rate": 0.0,
    "disable_confidence_filter": _to_bool(os.getenv("DISABLE_CONFIDENCE_FILTER", os.getenv("CRYPTO_DISABLE_CONFIDENCE_FILTER", "false"))),
    # Confidence Calibration
    "confidence_calibrator_enable": _safe_int(os.getenv("CONFIDENCE_CALIBRATOR_ENABLE", "0")),
    "confidence_calibrator_bundle_enable": _safe_int(os.getenv("CONFIDENCE_CALIBRATOR_BUNDLE_ENABLE", "0")),

    # ML Gate Calibrator Modes injected automatically
    "sg_calib_mode": "shadow",
    "adv_calib_mode": "disabled",
    "cont_ctx_valid_ms": 120_000,
    # Use generic path or specific bundle path
    "confidence_calibrator_path": os.getenv("CONFIDENCE_CALIBRATOR_PATH", "/app/calibration/confidence_calibration.json"),
    "confidence_calibrator_bundle_path": os.getenv("CONFIDENCE_CALIBRATOR_BUNDLE_PATH", "/app/calibration/confidence_calibration_v2.json"),
    "confidence_calibrator_check_ms": _safe_int(os.getenv("CONFIDENCE_CALIBRATOR_CHECK_MS", "5000")),
    # --- DN-GATE (Delta Notional) ---
    "dn_tier0_usd": _safe_float(os.getenv("DN_TIER0_USD", "30000.0"), 30000.0),
    "dn_tier1_usd": _safe_float(os.getenv("DN_TIER1_USD", "70000.0"), 70000.0),
    "dn_tier2_usd": _safe_float(os.getenv("DN_TIER2_USD", "150000.0"), 150000.0),
}

# Maximum number of symbols per pipeline chunk.  Smaller chunks stay well under
# the asyncio.wait_for timeout even when Redis is temporarily busy.
_CONFIG_PIPE_CHUNK: int = int(os.getenv("ORDERFLOW_CONFIG_PIPE_CHUNK_SIZE", "50"))

# Stale-cache TTL multiplier: if cache entry is younger than
# ttl × stale_mult we still use it as a fallback even after a preload failure.
# Set to 0 to disable (always use default on cache miss).
_CONFIG_STALE_MULT: float = float(os.getenv("ORDERFLOW_CONFIG_STALE_MULT", "4.0"))

class OrderFlowConfigLoader:
    def __init__(self, redis_client):
        self.redis = redis_client
        self._cache: dict[str, tuple[dict[str, Any], float]] = {}
        self._cache_ttl_sec: float = float(os.getenv("ORDERFLOW_CONFIG_CACHE_TTL_S", "60.0"))
        # Timestamp of last successful preload_configs run (for hgetall-skip guard)
        self._last_preload_ts: float = 0.0

    async def preload_configs(self, symbols: list[str]) -> None:
        """
        Предзагружает конфигурации для списка символов с использованием Redis Pipeline.
        Выполняется чанками (_CONFIG_PIPE_CHUNK) чтобы каждый chunk укладывался в
        asyncio.wait_for таймаут даже при временной нагрузке на Redis.
        При таймауте конкретного чанка — чанк пропускается, остальные продолжают.
        После успешной загрузки любого чанка выставляется _last_preload_ts.
        """
        import time
        now = time.time()
        to_fetch = []
        for symbol in symbols:
            cached, ts = self._cache.get(symbol, (None, 0.0))
            if cached is None or (now - ts) >= self._cache_ttl_sec:
                to_fetch.append(symbol)

        if not to_fetch or not self.redis:
            # All symbols are cache-fresh — mark last preload as now so that
            # build_symbol_config skips individual hgetall calls.
            self._last_preload_ts = now
            return

        _timeout = _CONFIG_REDIS_TIMEOUT_S if _CONFIG_REDIS_TIMEOUT_S > 0 else None
        chunk_size = max(1, _CONFIG_PIPE_CHUNK)
        any_ok = False
        total_chunks = (len(to_fetch) + chunk_size - 1) // chunk_size
        failed_chunks = 0

        for i in range(0, len(to_fetch), chunk_size):
            chunk = to_fetch[i : i + chunk_size]
            try:
                pipe = self.redis.pipeline()
                for symbol in chunk:
                    pipe.hgetall(f"config:orderflow:{symbol}")

                if _timeout:
                    results = await asyncio.wait_for(pipe.execute(), timeout=_timeout)
                else:
                    results = await pipe.execute()

                for symbol, overrides in zip(chunk, results):
                    self._cache[symbol] = (overrides or {}, now)
                any_ok = True
            except TimeoutError:
                failed_chunks += 1
                logger.warning(
                    "⚠️ Таймаут preload (pipe) config:orderflow chunk[%d:%d] (>%.1fs) — %d символов пропущено",
                    i, i + len(chunk), _timeout or 0.0, len(chunk),
                )
            except RedisError as exc:
                failed_chunks += 1
                logger.warning(
                    "⚠️ Redis-ошибка preload config:orderflow chunk[%d:%d]: %s", i, i + len(chunk), exc
                )

        if any_ok:
            self._last_preload_ts = now
        elif total_chunks > 0:
            # Loud escalation: no chunk succeeded in this cycle. When this
            # happens, build_symbol_config will fall back to per-symbol
            # hgetall for every symbol on the next access — that saturates
            # the hot-path Redis pool and triggers the cascading timeout
            # storm we observed. Raise the alarm so it's visible without
            # grepping at WARNING level.
            logger.error(
                "❌ preload_configs: ALL %d chunks failed (to_fetch=%d, timeout=%.1fs); "
                "stale-cache fallback will be used until next refresh",
                total_chunks, len(to_fetch), _timeout or 0.0,
            )

    async def build_symbol_config(self, symbol: str) -> dict[str, Any]:
        """
        Берёт базовый OrderFlowConfig и применяет overrides из Redis.
        """
        base_cfg: OrderFlowConfig = get_config(symbol)
        cfg: dict[str, Any] = deepcopy(DEFAULT_CONFIG)
        cfg.update(
            {
                "delta_window": base_cfg.delta_window_ticks,
                "delta_z_threshold": base_cfg.delta_z_threshold,
                "delta_abs_min": base_cfg.delta_abs_min,
                "delta_abs_min_confirm": cfg.get("delta_abs_min_confirm", getattr(base_cfg, "delta_abs_min_confirm", base_cfg.delta_abs_min)),
                "min_confirmations": getattr(base_cfg, "min_confirmations", 1),
                "fp_imb_counts_for_min_confirmations": getattr(base_cfg, "fp_imb_counts_for_min_confirmations", False),
                "weak_progress_atr": base_cfg.weak_progress_atr,
                "iceberg_refresh": base_cfg.iceberg_refresh_count,
                "iceberg_duration": base_cfg.iceberg_min_duration,
                "iceberg_refresh_min_abs": getattr(base_cfg, "iceberg_refresh_min_abs", 0.0),
                "obi_threshold": base_cfg.obi_threshold,
                "obi_hold_secs": getattr(base_cfg, "obi_min_duration", 1.5),
                "signal_cooldown_sec": base_cfg.min_signal_interval_sec,
                # Persistence of strategy params
                "stop_mode": getattr(base_cfg, "stop_mode", "ATR"),
                "stop_atr_mult": getattr(base_cfg, "stop_atr_mult", 1.0),
                "stop_pct": getattr(base_cfg, "stop_pct", 0.2),
                "stop_points": getattr(base_cfg, "stop_points", 100.0),
                "tp_mode": getattr(base_cfg, "tp_mode", "RR"),
                "tp_rr": getattr(base_cfg, "tp_rr", "1.0,1.5,2.5"),
                "tp_atr_mults": getattr(base_cfg, "tp_atr_mults", "0.6,1.0,1.5"),
                "dist_atr_threshold": getattr(base_cfg, "dist_atr_threshold", 0.4),
                "read_count": getattr(base_cfg, "read_count", 200),
                "read_block_ms": getattr(base_cfg, "read_block_ms", 1000),
                "metadata": getattr(base_cfg, "metadata", {}),
                # Calibration & Tiers
                "dn_tier0_usd": getattr(base_cfg, "dn_tier0_usd", 0.0),
                "dn_tier1_usd": getattr(base_cfg, "dn_tier1_usd", 0.0),
                "dn_tier2_usd": getattr(base_cfg, "dn_tier2_usd", 0.0),
                "book_rate_min_hz": getattr(base_cfg, "book_rate_min_hz", 5.0),
                "book_rate_warn_hz": getattr(base_cfg, "book_rate_warn_hz", 3.0),
                "calib_atr_floor_mult": getattr(base_cfg, "calib_atr_floor_mult", 0.5),
                "calib_dn_tier_fallback_usd": getattr(base_cfg, "calib_dn_tier_fallback_usd", 100000.0),
                # Cancellation Spike Gate
                "cancel_spike_enable": getattr(base_cfg, "cancel_spike_enable", 1),
                "cancel_spike_mode": getattr(base_cfg, "cancel_spike_mode", "monitor"),
                "cancel_spike_alpha_slow": getattr(base_cfg, "cancel_spike_alpha_slow", 0.02),
                "cancel_spike_ratio_th": getattr(base_cfg, "cancel_spike_ratio_th", 3.0),
                "cancel_spike_abs_th": getattr(base_cfg, "cancel_spike_abs_th", 0.0),
                "cancel_spike_min_baseline": getattr(base_cfg, "cancel_spike_min_baseline", 0.0),
                "cancel_spike_use_robust_z": getattr(base_cfg, "cancel_spike_use_robust_z", True),
                "cancel_spike_window": getattr(base_cfg, "cancel_spike_window", 120),
                "cancel_spike_min_samples": getattr(base_cfg, "cancel_spike_min_samples", 30),
                "cancel_spike_z_th": getattr(base_cfg, "cancel_spike_z_th", 3.5),
                "cancel_spike_min_taker_rate": getattr(base_cfg, "cancel_spike_min_taker_rate", 0.0),
                # Round 7 V4 & Exec Risk
                "exec_risk_ref_bps": getattr(base_cfg, "exec_risk_ref_bps", 12.0),
                "scenario_v4_enable": getattr(base_cfg, "scenario_v4_enable", False),
                "of_score_min_range": getattr(base_cfg, "of_score_min_range", None),
                "ofc_ctx_enable": getattr(base_cfg, "ofc_ctx_enable", False),
                "ofc_ctx_mode": getattr(base_cfg, "ofc_ctx_mode", "off"),
                "ofc_ctx_fail_mode": getattr(base_cfg, "ofc_ctx_fail_mode", "open"),
                "ofc_ctx_bundle_path": getattr(base_cfg, "ofc_ctx_bundle_path", ""),
                "ofc_ctx_reload_sec": getattr(base_cfg, "ofc_ctx_reload_sec", 30),
                "ofc_ctx_p_min_default": getattr(base_cfg, "ofc_ctx_p_min_default", 0.55),
                "ofc_ctx_edge_floor_p50_bps": getattr(base_cfg, "ofc_ctx_edge_floor_p50_bps", 0.0),
                "ofc_ctx_edge_floor_p90_bps": getattr(base_cfg, "ofc_ctx_edge_floor_p90_bps", -2.0),
                "ofc_ctx_reasons_replace": getattr(base_cfg, "ofc_ctx_reasons_replace", "score_veto,vol_shock_score_veto,saw_chop_score_veto"),
                "strong_need_range": getattr(base_cfg, "strong_need_range", 3),
                "strong_need_escalated": getattr(base_cfg, "strong_need_escalated", 3),
                "strong_need_reversal": getattr(base_cfg, "strong_need_reversal", 2),
                "strong_need_continuation": getattr(base_cfg, "strong_need_continuation", 2),
                "strong_need_extreme_enable": getattr(base_cfg, "strong_need_extreme_enable", 1),
                "strong_need_extreme": getattr(base_cfg, "strong_need_extreme", 4),
                "atr_floor_t0_bps": getattr(base_cfg, "atr_floor_t0_bps", 3.0),
                "atr_floor_t1_bps": getattr(base_cfg, "atr_floor_t1_bps", 5.0),
                "atr_floor_t2_bps": getattr(base_cfg, "atr_floor_t2_bps", 8.0),
                "of_score_agg": getattr(base_cfg, "of_score_agg", "weighted_mean"),
            }
        )

        try:
            from services.pnl_math import get_symbol_info_async
            # Use self.redis (which is async here) to avoid blocking the event loop
            sym_info = await get_symbol_info_async(symbol, self.redis)
        except Exception as exc:
            logger.warning("get_symbol_info_async failed for %s, falling back to sync: %s", symbol, exc)
            sym_info = get_symbol_info(symbol)

        if sym_info:
            # get_symbol_info returns dict, not object
            if isinstance(sym_info, dict):
                cfg["tick_size"] = float(sym_info.get("tick_size", 0.01))
            else:
                cfg["tick_size"] = float(getattr(sym_info, "tick_size", 0.01))

        # Backwards compat if not set in base_cfg (should be handled above but double check)
        cfg.setdefault("delta_abs_min_confirm", cfg["delta_abs_min"])

        overrides: dict[str, Any] = {}
        import time
        now = time.time()
        cached, ts = self._cache.get(symbol, (None, 0.0))
        cache_age = now - ts
        stale_limit = self._cache_ttl_sec * _CONFIG_STALE_MULT

        if cached is not None and cache_age < self._cache_ttl_sec:
            # Hot cache hit — no Redis call needed.
            overrides = cached
        elif self._last_preload_ts > 0 and (now - self._last_preload_ts) < self._cache_ttl_sec:
            # preload_configs was executed recently (within one TTL window).
            # Trust that it already fetched or tried to fetch this symbol;
            # avoid a redundant individual hgetall that would contend with the hot-path pool.
            if cached is not None:
                # Use stale data — better than default.
                overrides = cached
            # else: no cache at all → use DEFAULT_CONFIG (overrides stays {})
        elif cached is not None and cache_age < stale_limit:
            # Preload_ts is stale (preloader has been failing / hasn't refreshed),
            # but we have cached data within the stale-tolerance window
            # (stale_limit = ttl × ORDERFLOW_CONFIG_STALE_MULT, default 4× = 240s).
            # Prefer stale cache over a per-symbol hgetall: the hot-path pool on
            # redis-worker-1 can saturate when every symbol independently
            # decides to do its own round-trip, triggering a cascading timeout
            # storm. Stale overrides are nearly always better than reaching
            # into a saturated pool.
            overrides = cached
            if not getattr(self, "_stale_cache_warned", False):
                logger.warning(
                    "⚠️ Using stale cache for %s (age=%.1fs, preload_ts_age=%.1fs) — "
                    "preloader has not refreshed within TTL; check redis-worker-1 load",
                    symbol, cache_age, now - self._last_preload_ts,
                )
                self._stale_cache_warned = True
        elif self.redis:
            try:
                _timeout = _CONFIG_REDIS_TIMEOUT_S if _CONFIG_REDIS_TIMEOUT_S > 0 else None
                if _timeout:
                    overrides = await asyncio.wait_for(
                        self.redis.hgetall(f"config:orderflow:{symbol}"),
                        timeout=_timeout,
                    )
                else:
                    overrides = await self.redis.hgetall(f"config:orderflow:{symbol}")
                self._cache[symbol] = (overrides or {}, now)
            except TimeoutError:
                logger.warning(
                    "⚠️ (%s) Таймаут загрузки config:orderflow:%s (>%.1fs) — используется %s",
                    symbol, symbol, _CONFIG_REDIS_TIMEOUT_S,
                    "устаревший кэш" if cached is not None else "дефолт",
                )
                if cached is not None:
                    overrides = cached
                    if cache_age < stale_limit:
                        logger.warning("⚠️ (%s) Используем устаревший кэш из-за таймаута (age=%.1fs)", symbol, cache_age)
            except RedisError as exc:
                logger.warning("⚠️ (%s) Не удалось загрузить config:orderflow:%s: %s", symbol, symbol, exc)
                if cached is not None:
                    overrides = cached
                    if cache_age < stale_limit:
                        logger.warning("⚠️ (%s) Используем устаревший кэш из-за ошибки Redis (age=%.1fs)", symbol, cache_age)

        self._apply_overrides(cfg, overrides)
        return cfg

    def _apply_overrides(self, cfg: dict[str, Any], overrides: dict[str, Any]) -> None:
        """
        Применяет overrides из Redis hash, если они присутствуют.
        """
        mapping = {
            "delta_window": ("delta_window", _safe_int),
            "delta_window_ticks": ("delta_window", _safe_int),
            "delta_z_threshold": ("delta_z_threshold", _safe_float),
            "delta_abs_min": ("delta_abs_min", _safe_float),
            "delta_abs_min_confirm": ("delta_abs_min_confirm", _safe_float),
            "min_confirmations": ("min_confirmations", _safe_int),
            "fp_imb_counts_for_min_confirmations": ("fp_imb_counts_for_min_confirmations", _to_bool),
            "obi_depth": ("obi_depth", _safe_int),
            "obi_threshold": ("obi_threshold", _safe_float),
            "obi_hold_secs": ("obi_hold_secs", _safe_float),
            "absorption_min_volume": ("absorption_min_volume", _safe_float),
            "absorption_price_tolerance": ("absorption_price_tolerance", _safe_float),
            "absorption_window_sec": ("absorption_window_sec", _safe_float),
            "iceberg_refresh": ("iceberg_refresh", _safe_int),
            "iceberg_duration": ("iceberg_duration", _safe_float),
            "signal_cooldown_sec": ("signal_cooldown_sec", _safe_int),
            "tick_buffer": ("tick_buffer", _safe_int),
            "orders_queue_enabled": ("orders_queue_enabled", _to_bool),
            "orders_queue_type": ("orders_queue_type", str),
            "orders_queue_profile": ("orders_queue_profile", str),
            "confidence_floor": ("confidence_floor", _safe_float),
            "confidence_cap": ("confidence_cap", _safe_float),
            "confidence_speed_scale": ("confidence_speed_scale", _safe_float),
            "require_strong_confirmation": ("require_strong_confirmation", _to_bool),
            "strong_gate_shadow": ("strong_gate_shadow", _to_bool),
            "publish_of_inputs": ("publish_of_inputs", _to_bool),
            "of_inputs_emit_v2": ("of_inputs_emit_v2", _safe_int),  # 1=v2 (default), 0=v1
            "of_inputs_stream": ("of_inputs_stream", str),
            "of_inputs_stream_maxlen": ("of_inputs_stream_maxlen", _safe_int),
            "publish_of_confirm": ("publish_of_confirm", _to_bool),
            "of_confirm_stream": ("of_confirm_stream", str),
            "of_confirm_stream_maxlen": ("of_confirm_stream_maxlen", _safe_int),
            "cancel_spike_enable": ("cancel_spike_enable", _to_bool),
            "cancel_spike_mode": ("cancel_spike_mode", str),
            "cancel_spike_alpha_slow": ("cancel_spike_alpha_slow", _safe_float),
            "cancel_spike_ratio_th": ("cancel_spike_ratio_th", _safe_float),
            "cancel_spike_abs_th": ("cancel_spike_abs_th", _safe_float),
            "cancel_spike_min_baseline": ("cancel_spike_min_baseline", _safe_float),
            "cancel_spike_use_robust_z": ("cancel_spike_use_robust_z", _to_bool),
            "cancel_spike_window": ("cancel_spike_window", _safe_int),
            "cancel_spike_min_samples": ("cancel_spike_min_samples", _safe_int),
            "cancel_spike_z_th": ("cancel_spike_z_th", _safe_float),
            "cancel_spike_min_taker_rate": ("cancel_spike_min_taker_rate", _safe_float),
            "adverse_check_enable": ("adverse_check_enable", _to_bool),
            "dn_tier0_usd": ("dn_tier0_usd", _safe_float),
            "dn_tier1_usd": ("dn_tier1_usd", _safe_float),
            "dn_tier2_usd": ("dn_tier2_usd", _safe_float),
            "disable_confidence_filter": ("disable_confidence_filter", _to_bool),
            # Round 7 overrides
            "exec_risk_ref_bps": ("exec_risk_ref_bps", _safe_float),
            "scenario_v4_enable": ("scenario_v4_enable", _to_bool),
            "of_score_min_range": ("of_score_min_range", _safe_float),
            "ofc_ctx_enable": ("ofc_ctx_enable", _to_bool),
            "ofc_ctx_mode": ("ofc_ctx_mode", str),
            "ofc_ctx_fail_mode": ("ofc_ctx_fail_mode", str),
            "ofc_ctx_bundle_path": ("ofc_ctx_bundle_path", str),
            "ofc_ctx_reload_sec": ("ofc_ctx_reload_sec", _safe_int),
            "ofc_ctx_p_min_default": ("ofc_ctx_p_min_default", _safe_float),
            "ofc_ctx_edge_floor_p50_bps": ("ofc_ctx_edge_floor_p50_bps", _safe_float),
            "ofc_ctx_edge_floor_p90_bps": ("ofc_ctx_edge_floor_p90_bps", _safe_float),
            "ofc_ctx_reasons_replace": ("ofc_ctx_reasons_replace", str),
            "strong_need_range": ("strong_need_range", _safe_int),
            "strong_need_escalated": ("strong_need_escalated", _safe_int),
            "of_score_agg": ("of_score_agg", str),
            # Confidence Calibration (Runtime Loader V2)
            "confidence_calibrator_enable": ("confidence_calibrator_enable", _safe_int),
            "confidence_calibrator_bundle_enable": ("confidence_calibrator_bundle_enable", _safe_int),
            "confidence_calibrator_path": ("confidence_calibrator_path", str),
            "confidence_calibrator_bundle_path": ("confidence_calibrator_bundle_path", str),
            "confidence_calibrator_reload_sec": ("confidence_calibrator_reload_sec", _safe_int),

            # ML Gate Calibrator Modes
            "sg_calib_mode": ("sg_calib_mode", str),
            "adv_calib_mode": ("adv_calib_mode", str),
            "cont_ctx_valid_ms": ("cont_ctx_valid_ms", _safe_int),
            "confidence_calibrator_check_ms": ("confidence_calibrator_check_ms", _safe_int),
            "confidence_use_calibrated": ("confidence_use_calibrated", _safe_int),
            # OBI Stability (Redis hot-reload enabled)
            # ENV key: OBI_STABLE_SCORE_MIN — canonical key used by book_processor.py + signal_confidence.py
            "obi_stable_score_min": ("obi_stable_score_min", _safe_float),
            "obi_stable_min_secs": ("obi_stable_min_secs", _safe_float),
            "obi_event_ttl_ms": ("obi_event_ttl_ms", _safe_int),
            "book_stale_ms": ("book_stale_ms", _safe_int),
            "obi_stable_window_ms": ("obi_stable_window_ms", _safe_int),
            "obi_deadband": ("obi_deadband", _safe_float),
            "obi_grace_ms": ("obi_grace_ms", _safe_int),
            "dw_obi_stable_score_min": ("dw_obi_stable_score_min", _safe_float),
            # Scenario delta_z bypass (hot-configurable per symbol)
            "scenario_dz_bypass_threshold": ("scenario_dz_bypass_threshold", _safe_float),
            # Bias cascade (RSI/Regime trend direction fallback)
            "bias_rsi_enable": ("bias_rsi_enable", _safe_int),
            "bias_rsi_hi": ("bias_rsi_hi", _safe_float),
            "bias_rsi_lo": ("bias_rsi_lo", _safe_float),
            "bias_regime_enable": ("bias_regime_enable", _safe_int),
            "bias_regime_ttl_ms": ("bias_regime_ttl_ms", _safe_int),
            # Exec-risk hot-config (FIX: these were missing from allowlist → Redis overrides silently ignored)
            "dist_bp_threshold": ("dist_bp_threshold", _safe_float),
            "exec_risk_ref_mult": ("exec_risk_ref_mult", _safe_float),
            "w_exec_risk": ("w_exec_risk", _safe_float),
            "spread_bps_missing_default": ("spread_bps_missing_default", _safe_float),
            "expected_slippage_bps_missing_default": ("expected_slippage_bps_missing_default", _safe_float),
            "of_score_min": ("of_score_min", _safe_float),
            "strong_need_reversal": ("strong_need_reversal", _safe_int),
            "strong_need_continuation": ("strong_need_continuation", _safe_int),
            "strong_need_extreme_enable": ("strong_need_extreme_enable", _to_bool),
            "strong_need_extreme": ("strong_need_extreme", _safe_int),
            "atr_floor_t0_bps": ("atr_floor_t0_bps", _safe_float),
            "atr_floor_t1_bps": ("atr_floor_t1_bps", _safe_float),
            "atr_floor_t2_bps": ("atr_floor_t2_bps", _safe_float),
        }

        for key, (dest, caster) in mapping.items():
            if key in overrides:
                try:
                    cfg[dest] = caster(overrides[key])
                except Exception as exc:  # noqa: BLE001
                    logger.warning("⚠️ Некорректное значение %s=%s (%s)", key, overrides[key], exc)

        for key, value in overrides.items():
            if key.startswith("confidence_weight_"):
                part = key.replace("confidence_weight_", "", 1).strip()
                if part:
                    cfg.setdefault("confidence_weights", deepcopy(DEFAULT_CONFIG["confidence_weights"]))
                    try:
                        cfg["confidence_weights"][part] = float(value)
                    except Exception as exc:  # noqa: BLE001
                        logger.warning("⚠️ Некорректный вес confidence_weight_%s=%s (%s)", part, value, exc)
            elif key.startswith("confidence_bonus_"):
                part = key.replace("confidence_bonus_", "", 1).strip()
                if part:
                    cfg.setdefault("confidence_confirm_bonus", deepcopy(DEFAULT_CONFIG["confidence_confirm_bonus"]))
                    try:
                        cfg["confidence_confirm_bonus"][part] = max(0.0, float(value))
                    except Exception as exc:  # noqa: BLE001
                        logger.warning("⚠️ Некорректный бонус confidence_bonus_%s=%s (%s)", part, value, exc)

        cfg.setdefault("delta_abs_min_confirm", cfg["delta_abs_min"])
