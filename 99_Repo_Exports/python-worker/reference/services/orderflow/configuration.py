
from typing import Any, Dict, List
import os
import json
import logging
from copy import deepcopy
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

def _ensure_list_levels(raw: Any) -> List[List[float]]:
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

    result: List[List[float]] = []
    for item in raw:
        if isinstance(item, (list, tuple)) and len(item) >= 2:
            price = _safe_float(item[0])
            qty = _safe_float(item[1])
            result.append([price, qty])
    return result

_FALLBACK_SYMBOLS: List[str] = [
    "BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT",
    "PEPEUSDT", "DOGEUSDT", "SHIBUSDT", "FLOKIUSDT", "BONKUSDT",
    "WIFUSDT", "SUIUSDT", "APTUSDT", "ARBUSDT", "XAUUSDT"
]

# If SYMBOLS env is set — use it exclusively (full replacement, not extension).
# This ensures that crypto-orderflow-service-2 (meme-coins only) does not
# accidentally spin up readers for SOLUSDT/BTCUSDT/ETHUSDT.
_env_symbols_raw = os.getenv("SYMBOLS", "")
if _env_symbols_raw:
    DEFAULT_SYMBOLS: List[str] = [s.strip().upper() for s in _env_symbols_raw.split(",") if s.strip()]
else:
    DEFAULT_SYMBOLS = list(_FALLBACK_SYMBOLS)

DEFAULT_CONFIG: Dict[str, Any] = {
    "delta_window": 120,
    # "delta_z_threshold" purposely omitted to allow fallback to SymbolSpecs
    "delta_abs_min": 0.75,
    "delta_abs_min_confirm": 1.0,
    "min_confirmations": 1,
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
    "dq_gate_mode": str(os.getenv("DQ_GATE_MODE", "penalty")).lower(),  # off|penalty|enforce|both
    "dq_mode": str(os.getenv("DQ_MODE", "safe")).lower(),
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
    "stop_atr_mult": 0.6,
    "stop_pct": 0.2,
    "stop_points": 1.0,
    "tp_rr": "1.3,2.0,2.7",
    "publish_of_inputs": 1,
    "of_inputs_emit_v2": 1,  # Deterministic version selection: 1=v2 (default), 0=v1
    "of_inputs_stream": "signals:of:inputs",
    "of_inputs_stream_maxlen": 200000,
    "publish_of_confirm": 1,
    "of_confirm_stream": "signals:of:confirm",
    "of_confirm_stream_maxlen": 50000,
    "confidence_weights": {
        "delta": 0.5,
        "speed": 0.2,
        "cluster": 0.2,
        "confirm": 0.1,
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
    "ml_confirm_rollout": str(os.getenv("ML_CONFIRM_ROLLOUT_MODE", "shadow")).lower(),
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
    "obi_event_ttl_ms": 15_000,
    "obi_stable_min_secs": 1.5,
    # OBI Stability Tracker (quality, not only duration)
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
    "adverse_check_enable": _to_bool(os.getenv("ADVERSE_CHECK_ENABLE", "0")),
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
    "of_score_min": 0.60,  # Lowered from 0.65 to allow more trades to pass gate
    "publish_of_confirm": _to_bool(os.getenv("CRYPTO_OF_PUBLISH_CONFIRM", "false")),
    "of_confirm_stream": os.getenv("CRYPTO_OF_CONFIRM_STREAM", "signals:of:confirm"),
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
    # Use generic path or specific bundle path
    "confidence_calibrator_path": os.getenv("CONFIDENCE_CALIBRATOR_PATH", "/app/calibration/confidence_calibration.json"),
    "confidence_calibrator_bundle_path": os.getenv("CONFIDENCE_CALIBRATOR_BUNDLE_PATH", "/app/calibration/confidence_calibration_v2.json"),
    "confidence_calibrator_check_ms": _safe_int(os.getenv("CONFIDENCE_CALIBRATOR_CHECK_MS", "5000")),
    "confidence_use_calibrated": _safe_int(os.getenv("CONFIDENCE_USE_CALIBRATED", "0")),
}

class OrderFlowConfigLoader:
    def __init__(self, redis_client):
        self.redis = redis_client

    async def build_symbol_config(self, symbol: str) -> Dict[str, Any]:
        """
        Берёт базовый OrderFlowConfig и применяет overrides из Redis.
        """
        base_cfg: OrderFlowConfig = get_config(symbol)
        cfg: Dict[str, Any] = deepcopy(DEFAULT_CONFIG)
        cfg.update(
            {
                "delta_window": base_cfg.delta_window_ticks,
                "delta_z_threshold": base_cfg.delta_z_threshold,
                "delta_abs_min": base_cfg.delta_abs_min,
                "delta_abs_min_confirm": cfg.get("delta_abs_min_confirm", getattr(base_cfg, "delta_abs_min_confirm", base_cfg.delta_abs_min)),
                "weak_progress_atr": base_cfg.weak_progress_atr,
                "iceberg_refresh": base_cfg.iceberg_refresh_count,
                "iceberg_duration": base_cfg.iceberg_min_duration,
                "iceberg_refresh_min_abs": getattr(base_cfg, "iceberg_refresh_min_abs", 0.0),
                "obi_threshold": base_cfg.obi_threshold,
                "obi_hold_secs": getattr(base_cfg, "obi_min_duration", 1.5),
                "signal_cooldown_sec": base_cfg.min_signal_interval_sec,
                # Persistence of strategy params
                "stop_mode": getattr(base_cfg, "stop_mode", "ATR"),
                "stop_atr_mult": getattr(base_cfg, "stop_atr_mult", 0.6),
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
                "strong_need_range": getattr(base_cfg, "strong_need_range", 3),
                "strong_need_escalated": getattr(base_cfg, "strong_need_escalated", 3),
                "of_score_agg": getattr(base_cfg, "of_score_agg", "weighted_mean"),
            }
        )

        sym_info = get_symbol_info(symbol)
        if sym_info:
            # get_symbol_info returns dict, not object
            if isinstance(sym_info, dict):
                cfg["tick_size"] = float(sym_info.get("tick_size", 0.01))
            else:
                cfg["tick_size"] = float(getattr(sym_info, "tick_size", 0.01))

        # Backwards compat if not set in base_cfg (should be handled above but double check)
        cfg.setdefault("delta_abs_min_confirm", cfg["delta_abs_min"])

        overrides = {}
        if self.redis:
            try:
                overrides = await self.redis.hgetall(f"config:orderflow:{symbol}")
            except RedisError as exc:
                logger.warning("⚠️ (%s) Не удалось загрузить config:orderflow:%s: %s", symbol, symbol, exc)

        self._apply_overrides(cfg, overrides)
        return cfg

    def _apply_overrides(self, cfg: Dict[str, Any], overrides: Dict[str, Any]) -> None:
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
            "strong_need_range": ("strong_need_range", _safe_int),
            "strong_need_escalated": ("strong_need_escalated", _safe_int),
            "of_score_agg": ("of_score_agg", str),
            # Confidence Calibration (Runtime Loader V2)
            "confidence_calibrator_enable": ("confidence_calibrator_enable", _safe_int),
            "confidence_calibrator_bundle_enable": ("confidence_calibrator_bundle_enable", _safe_int),
            "confidence_calibrator_path": ("confidence_calibrator_path", str),
            "confidence_calibrator_bundle_path": ("confidence_calibrator_bundle_path", str),
            "confidence_calibrator_check_ms": ("confidence_calibrator_check_ms", _safe_int),
            "confidence_use_calibrated": ("confidence_use_calibrated", _safe_int),
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
