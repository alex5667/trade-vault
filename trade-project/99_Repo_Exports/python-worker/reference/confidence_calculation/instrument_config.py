"""
Конфигурация для различных инструментов (XAUUSD, Crypto и т.д.)

Централизованное управление параметрами анализа для каждого типа инструмента.
Поддерживает загрузку из environment variables и пресетов.
"""

import os
import re
from collections.abc import Callable
from dataclasses import dataclass, field, replace
from typing import Any, Optional, TypeVar

_T = TypeVar("_T")

_QUOTE_SUFFIX_RE = re.compile(r"(USDT|USD|USDC|BUSD)(?:PERP)?$", re.IGNORECASE)
_TV_PERP_SUFFIX_RE = re.compile(r"\.(P|PERP)$", re.IGNORECASE)


def normalize_symbol(symbol: str) -> str:
    """
    Нормализует символ:
    - приводит к верхнему регистру и убирает пробелы
    - поддерживает TradingView-формат perpetual: XAUUSDT.P -> XAUUSDT
    - (опционально) поддерживает префикс биржи: BINANCE:XAUUSDT.P -> XAUUSDT
    """
    s = (symbol or "").strip().upper()
    # Support "BINANCE:XXX" style
    if ":" in s:
        s = s.split(":", 1)[-1].strip()
    # Support ".P" / ".PERP" suffix used by TradingView perpetual tickers
    s = _TV_PERP_SUFFIX_RE.sub("", s)
    return s


def symbol_prefix(symbol: str) -> str:
    """
    Извлекает префикс символа для env override.
    
    Примеры:
        BTCUSDT -> BTC
        XAUUSD -> XAU
        ETHUSD -> ETH
    """
    s = normalize_symbol(symbol)
    s = _QUOTE_SUFFIX_RE.sub("", s)
    return s


ENV_PREFIX_OVERRIDE = {
    "1000PEPEUSDT": "PEPE",
    "1000SHIBUSDT": "SHIB",
    "1000FLOKIUSDT": "FLOKI",
    "1000BONKUSDT": "BONK",
    "1000LUNCUSDT": "LUNC",
    "1000XECUSDT": "XEC",
    "1000RATSUSDT": "RATS",
}

# Defaults for USD-based filtering (Delta Notional)
def get_default_usd_threshold(symbol: str) -> float:
    """
    Returns default minimum USD volume for delta based on symbol family.
    Ranges (approx):
      BTC: 10k-30k
      ETH: 3k-10k
      SOL/BNB: 1k-5k
      XRP: 300-1500
      Memes/Others: 200 (safety quality gate)
    """
    s = normalize_symbol(symbol)
    if "BTC" in s:
        return 15000.0  # Conservative mid
    if "ETH" in s:
        return 5000.0
    if "SOL" in s or "BNB" in s:
        return 2000.0
    if "XRP" in s:
        return 500.0
    # Metals (TradFi perps) bootstrap gates:
    # XAU ~ $2k-3k per oz, so a meaningful min notional should be >= a few oz.
    if s.startswith("XAU"):
        return 5_000.0
    if s.startswith("XAG"):
        return 1_000.0
    # For others (including memes), keep a basic gate
    return 200.0

def get_default_obi_settings(symbol: str) -> dict:
    """
    Returns default OBI configuration (threshold, min_duration) based on symbol family.
    
    Recommendations:
      BTC/ETH: 0.22-0.30, 1.2-1.8s
      SOL/BNB: 0.25-0.33, 1.2-1.6s
      XRP:     0.28-0.35, 1.2-2.0s
      Memes:   Same threshold (or 0.5 default), but stricter duration 2.0-3.0s
    """
    s = normalize_symbol(symbol)

    # Defaults
    th = 0.5   # Conservative default
    dur = 2.0  # Conservative default

    if "BTC" in s:
        th, dur = 0.25, 1.5
    elif "ETH" in s:
        th, dur = 0.28, 1.5
    elif "SOL" in s or "BNB" in s:
        th, dur = 0.30, 1.4
    elif "XRP" in s:
        th, dur = 0.32, 1.5
    elif symbol_env_prefix(s) in ("PEPE", "SHIB", "DOGE", "BONK", "FLOKI", "WIF"):
        # Memes: stricter duration, standard threshold
        th, dur = 0.35, 2.5

    return {"obi_threshold": th, "obi_min_duration": dur}

def get_default_dist_bp_threshold(symbol: str) -> float:
    """
    Returns default distance-to-level in BASIS POINTS.
    
    Recommendations:
      Majors (BTC/ETH): 8..15 bp
      Memes: 12..25 bp (due to noise)
    """
    s = normalize_symbol(symbol)
    if "BTC" in s or "ETH" in s:
        return 12.0  # Mid of 8-15
    if "SOL" in s or "BNB" in s:
        return 15.0
    # Metals: keep tighter proximity than generic "others"
    if s.startswith("XAU"):
        return 12.0
    if s.startswith("XAG"):
        return 15.0

    return 20.0  # Memes/Others: 20 bp default


def get_default_delta_tiers(symbol: str) -> dict:
    """
    Returns default delta_notional_usd tiers [Tier0, Tier1, Tier2] based on symbol.
    
    EXPERT CALIBRATION (2026-01-19):
    Based on real spike analysis, previous defaults were 10-25x too high.
    Real observed spikes: BTC ~260k, ETH ~132k, SOL ~27k.
    
    Tiers:
    - Tier 0 (Trend): Lenient, ~p75 of real spikes
    - Tier 1 (Range/Mixed): Standard, ~p90 of real spikes  
    - Tier 2 (Thin/News): Strict, ~p97 of real spikes
    
    These are bootstrap values; runtime calibrator will refine them based on live data.
    """
    s = normalize_symbol(symbol)

    # 1. BTCUSDT - Real spikes ~260k USD
    if "BTC" in s:
        t0, t1, t2 = 250_000.0, 400_000.0, 750_000.0

    # 2. ETHUSDT - Real spikes ~132k USD
    elif "ETH" in s:
        t0, t1, t2 = 100_000.0, 150_000.0, 300_000.0

    # 3. SOLUSDT - Real spikes ~27k USD
    elif "SOL" in s:
        t0, t1, t2 = 20_000.0, 35_000.0, 80_000.0

    # 4. BNB - Similar to SOL
    elif "BNB" in s:
        t0, t1, t2 = 25_000.0, 45_000.0, 100_000.0

    # 5. XRP - Lower liquidity
    elif "XRP" in s:
        t0, t1, t2 = 8_000.0, 15_000.0, 35_000.0

    # 6. DOGE - Meme tier
    elif "DOGE" in s:
        t0, t1, t2 = 5_000.0, 10_000.0, 25_000.0

    # 7. SUI / APT / ARB - Mid-caps
    elif any(x in s for x in ["SUI", "APT", "ARB"]):
        t0, t1, t2 = 3_000.0, 6_000.0, 15_000.0

    # 8. WIF - Meme
    elif "WIF" in s:
        t0, t1, t2 = 2_000.0, 4_000.0, 10_000.0

    # 9. Memes (1000* or PEPE/SHIB/FLOKI/BONK)
    elif "1000" in s or any(x in s for x in ["PEPE", "SHIB", "FLOKI", "BONK"]):
        t0, t1, t2 = 1_500.0, 3_000.0, 8_000.0

    # 10. Default / Fallback - Conservative
    else:
        t0, t1, t2 = 5_000.0, 10_000.0, 25_000.0

    # Metals (bootstrap): start mid-range, calibrator will refine
    if s.startswith("XAU"):
        t0, t1, t2 = 25_000.0, 45_000.0, 90_000.0
    if s.startswith("XAG"):
        t0, t1, t2 = 10_000.0, 20_000.0, 50_000.0

    return {"tier0": t0, "tier1": t1, "tier2": t2}


def get_default_book_rate_settings(symbol: str) -> dict:
    s = normalize_symbol(symbol)

    # Base defaults (Small caps / Unknown)
    min_hz = 5.0
    warn_hz = 3.0

    # 1. Majors
    if "BTC" in s:
        return {"book_rate_min_hz": 50.0, "book_rate_warn_hz": 30.0}
    if "ETH" in s:
        return {"book_rate_min_hz": 20.0, "book_rate_warn_hz": 10.0}

    # 2. Large-cap (BNB, SOL)
    if any(x in s for x in ["BNB", "SOL"]):
        return {"book_rate_min_hz": 15.0, "book_rate_warn_hz": 10.0}

    # 3. Mid-cap / Others (XRP, DOGE, SUI, APT, ARB, WIF)
    if any(x in s for x in ["XRP", "DOGE", "SUI", "APT", "ARB", "WIF"]):
        return {"book_rate_min_hz": 10.0, "book_rate_warn_hz": 5.0}

    # 4. Metals (TradFi perps) - bootstrap settings
    if s.startswith("XAU"):
        return {"book_rate_min_hz": 15.0, "book_rate_warn_hz": 10.0}
    if s.startswith("XAG"):
        return {"book_rate_min_hz": 10.0, "book_rate_warn_hz": 5.0}

    # 5. Memes / 1000*
    if "1000" in s or any(x in s for x in ["PEPE", "SHIB", "FLOKI", "BONK"]):
        return {"book_rate_min_hz": 8.0, "book_rate_warn_hz": 4.0}

    return {"book_rate_min_hz": min_hz, "book_rate_warn_hz": warn_hz}


def get_liquidity_class(symbol: str) -> str:
    """
    Canonical liquidity class for risk overlays (liq_regime / thresholds).

    Single source of truth:
      uses the SAME family logic as get_default_book_rate_settings().
    Families inside get_default_book_rate_settings():
      - BTC/ETH -> majors
      - SOL/BNB -> large
      - XRP/DOGE/SUI/APT/ARB/WIF -> mid
      - 1000* or PEPE/SHIB/FLOKI/BONK -> memes
      - fallback -> mid
    """
    s = normalize_symbol(symbol)
    br = get_default_book_rate_settings(s)
    min_hz = float(br.get("book_rate_min_hz", 5.0) or 5.0)
    # The mapping matches function families above.
    if min_hz >= 20.0:
        return "majors"
    if min_hz >= 15.0:
        return "large"
    if min_hz >= 10.0:
        return "mid"
    if min_hz >= 8.0:
        return "memes"
    return "mid"


def get_default_cancel_spike_settings(symbol: str) -> dict:
    """
    Returns default cancellation spike gate settings based on symbol.
    
    MAJOR SYMBOLS (BTC/ETH/SOL/BNB): Veto mode, pull_without_aggr enabled.
    MEMES: Veto mode, pull_without_aggr disabled (min_taker_rate=0), stricter spike.
    """
    s = normalize_symbol(symbol)

    # Global Defaults (from request)
    base = {
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
    }

    # BTCUSDT
    if "BTC" in s:
        base.update({
            "cancel_spike_mode": "veto",
            "cancel_spike_min_taker_rate": 0.15,
            "cancel_spike_min_baseline": 0.05,
            "cancel_spike_abs_th": 0.10
        })
    # ETHUSDT
    elif "ETH" in s:
        base.update({
            "cancel_spike_mode": "veto",
            "cancel_spike_min_taker_rate": 1.5,
            "cancel_spike_min_baseline": 0.5,
            "cancel_spike_abs_th": 1.0
        })
    # SOLUSDT
    elif "SOL" in s:
        base.update({
            "cancel_spike_mode": "veto",
            "cancel_spike_min_taker_rate": 5.0,
            "cancel_spike_min_baseline": 2.0,
            "cancel_spike_abs_th": 4.0
        })
    # BNBUSDT
    elif "BNB" in s:
        base.update({
            "cancel_spike_mode": "veto",
            "cancel_spike_min_taker_rate": 0.25,
            "cancel_spike_min_baseline": 0.10,
            "cancel_spike_abs_th": 0.20
        })
    # XRPUSDT
    elif "XRP" in s:
        base.update({
            "cancel_spike_mode": "veto",
            "cancel_spike_min_taker_rate": 150.0,
            "cancel_spike_min_baseline": 50.0,
            "cancel_spike_abs_th": 100.0
        })
    # DOGEUSDT
    elif "DOGE" in s:
        base.update({
            "cancel_spike_mode": "veto",
            "cancel_spike_min_taker_rate": 500.0,
            "cancel_spike_min_baseline": 200.0,
            "cancel_spike_abs_th": 400.0
        })
    # ARBUSDT
    elif "ARB" in s:
        base.update({
            "cancel_spike_mode": "veto",
            "cancel_spike_min_taker_rate": 80.0,
            "cancel_spike_min_baseline": 30.0,
            "cancel_spike_abs_th": 60.0
        })
    # APTUSDT
    elif "APT" in s:
        base.update({
            "cancel_spike_mode": "veto",
            "cancel_spike_min_taker_rate": 2.0,
            "cancel_spike_min_baseline": 0.7,
            "cancel_spike_abs_th": 1.5
        })
    # SUIUSDT
    elif "SUI" in s:
        base.update({
            "cancel_spike_mode": "veto",
            "cancel_spike_min_taker_rate": 120.0,
            "cancel_spike_min_baseline": 40.0,
            "cancel_spike_abs_th": 80.0
        })
    # WIFUSDT
    elif "WIF" in s:
        base.update({
            "cancel_spike_mode": "veto",
            "cancel_spike_min_taker_rate": 8.0,
            "cancel_spike_min_baseline": 2.0,
            "cancel_spike_abs_th": 4.0
        })
    # PEPE / SHIB / FLOKI / BONK (Memecoins)
    elif any(x in s for x in ["PEPE", "SHIB", "FLOKI", "BONK"]):
        base.update({
            "cancel_spike_mode": "veto",
            "cancel_spike_min_taker_rate": 0.0,
            "cancel_spike_ratio_th": 4.0,
            "cancel_spike_z_th": 4.0,
            "cancel_spike_min_samples": 60,
            "cancel_spike_abs_th": 0.0,
            "cancel_spike_min_baseline": 0.0
        })
    # XAUUSDT (Metals perp) - start with monitor mode, bootstrap settings
    elif s.startswith("XAU") or s.startswith("XAG"):
        base.update({
            "cancel_spike_mode": "monitor",
            "cancel_spike_min_taker_rate": 0.0,
            "cancel_spike_ratio_th": 3.0,
            "cancel_spike_z_th": 3.5,
            "cancel_spike_min_samples": 30,
            "cancel_spike_abs_th": 0.0,
            "cancel_spike_min_baseline": 0.0
        })

    return base


def symbol_env_prefix(symbol: str) -> str:
    """
    Returns the environment variable prefix for a given symbol.
    Handles overrides for "1000*" symbols (e.g. 1000-PEPE -> PEPE).
    Fallback logic strips leading digits if no override exists.
    """
    sym = normalize_symbol(symbol)
    if sym in ENV_PREFIX_OVERRIDE:
        return ENV_PREFIX_OVERRIDE[sym]

    p = symbol_prefix(sym)
    # Safety fallback: if prefix starts with digit (e.g. 1000PEPE), strip digits
    if p and p[0].isdigit():
        p2 = re.sub(r"^[0-9]+", "", p)
        if p2:
            return p2
    return p

def _env_first(keys: list[str], cast: Callable[[str], _T], default: _T) -> _T:
    """
    Пытается получить значение из env переменных по списку ключей.
    Возвращает первое найденное значение или default.
    """
    for k in keys:
        raw = os.getenv(k)
        if raw is None or raw == "":
            continue
        try:
            return cast(raw)
        except Exception:
            return default
    return default


def _env_one(key: str, cast: Callable[[str], _T], default: _T) -> _T:
    """Получает значение из одной env переменной."""
    return _env_first([key], cast, default)


def _to_bool(value: Any) -> bool:
    """Converts value to boolean (True/False)."""
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


@dataclass
class SymbolSpecs:
    """
    Спецификация торгового инструмента.

    Содержит параметры, специфичные для каждого символа (размер контракта,
    минимальный/максимальный лот, стоимость пипса и т.д.)
    """
    symbol: str
    contract_size: float = 100.0      # Размер контракта
    pip_value: float = 0.01           # Стоимость пипса
    lot_step: float = 0.01            # Шаг лота
    min_lot: float = 0.01             # Минимальный лот
    max_lot: float = 100.0            # Максимальный лот
    tick_value: float = 0.01          # Минимальное изменение цены
    point_value: float = 0.01         # Значение пойнта
    price_decimals: int = 2           # Количество знаков после запятой для цены
    volume_decimals: int = 2          # Количество знаков после запятой для объема
    delta_z: float = 3.0              # Default delta_z threshold for this symbol

    def __post_init__(self) -> None:
        """Нормализует символ при создании."""
        self.symbol = normalize_symbol(self.symbol)


@dataclass
class LiquidityConfig:
    """Конфигурация для анализа ликвидности"""
    # минимальный (аггрегированный объём у стены) / (остальной стакан)
    min_aggr_to_rest_ratio: float = 0.35

    # насколько одна сторона должна доминировать над другой
    min_side_domination_ratio: float = 1.3


@dataclass
class OrderFlowConfig:
    """
    Конфигурация обработчика Order Flow для конкретного инструмента.

    Содержит пороги и параметры для анализа ордер-флоу (Delta, Z-score,
    OBI, Iceberg detection и т.д.)
    """
    symbol: str = ""

    # === Метаданные сигнала ===
    family: str = "crypto_orderflow"      # Семейство сигналов
    venue: str = "binance_futures"        # Биржа/поставщик
    timeframe_s: int = 60                 # Базовый таймфрейм агрегации

    # === Пороги для формирования/оценки сигнала ===
    min_bucket_trades: int = 10           # Минимальное количество сделок в бакете
    min_bucket_notional_usd: float = 50_000.0  # Минимальный оборот
    min_delta_z: float = 2.0              # Минимальный |delta_z|
    min_obi_z: float = 1.5                # Минимальный |obi_z|

    # === Конфигурация ликвидности ===
    liquidity: LiquidityConfig = field(default_factory=LiquidityConfig)

    # === Параметры Delta и Z-score ===
    delta_window_ticks: int = 120          # Размер окна для Delta (количество тиков)
    delta_z_threshold: float | None = None  # Порог Z-score для delta spike (None => take from specs)

    # NEW: Absolute delta thresholds (prevents filtering on low volatility)
    delta_abs_min: float = 0.5             # Минимальная абсолютная дельта для сигнала
    # B1: Normalize delta threshold to USD
    delta_abs_min_usd: float | None = None # Если задано, приоритет выше чем delta_abs_min (coin volume)

    delta_abs_min_confirm: float = 0.5     # Минимальная дельта для подтверждения

    # === Staleness & TTL ===
    sweep_valid_ms: int = 120_000
    reclaim_signal_valid_ms: int = 120_000
    reclaim_hold_bars: int = 2
    obi_event_ttl_ms: int = 15_000
    obi_stable_min_secs: float = 1.5
    iceberg_event_ttl_ms: int = 15_000
    iceberg_strict_refresh_min: int = 1
    iceberg_strict_duration_min: float = 1.0
    iceberg_strict_dist_bp: float = 5.0

    # === Scoring Weights ===
    score_z_ref: float = 3.0
    w_z: float = 0.30
    w_wp: float = 0.15
    w_reclaim: float = 0.20
    w_obi: float = 0.15
    w_ice: float = 0.15
    w_abs: float = 0.05
    of_score_min: float = 0.65

    # === Publishing ===
    publish_of_confirm: bool = False
    of_confirm_stream: str = "signals:of:confirm"

    # === Параметры Weak Progress (Absorption) ===
    # Legacy generic threshold (kept for backward compatibility or simplistic views)
    weak_progress_atr: float = 0.10

    # NEW: Dual thresholds (Range/ATR and Body/ATR)
    # Range is usually larger (wicks included). Body is tighter.
    weak_progress_range_atr: float = 0.35  # Max Range/ATR for weak progress
    weak_progress_body_atr: float = 0.25   # Max Body/ATR for weak progress

    absorption_min_volume: float = 15.0    # Минимальный объем (в лотах/к-ве) для absorption
    absorption_price_tolerance: float = 5.0 # Толерантность цены (пункты/тиков)
    absorption_window_sec: float = 8.0     # Окно времени для накопления
    abs_lvl_enable: bool = False
    abs_lvl_counts_as: str = "absorption"

    # === Параметры OBI (Order Book Imbalance) ===
    obi_threshold: float = 0.5             # Порог OBI для sustained condition
    obi_min_duration: float = 2.0          # Минимальная длительность OBI (секунды)
    obi_depth: int = 5                     # Глубина стакана (уровни)
    obi_hold_secs: float = 1.5             # Сколько держать сигнал OBI

    # === Cooldown & Burst Logic ===
    cooldown_reversal_sec: int = 0
    cooldown_continuation_sec: int = 0
    cooldown_min_ms: int = 1000
    cooldown_max_ms: int = 5000
    cooldown_mul_thin: float = 1.0
    cooldown_spread_hi_bp: float = 10.0
    cooldown_mul_wide_spread: float = 1.0
    cooldown_mul_pressure_hi: float = 1.0

    pressure_window_ms: int = 60000
    burst_window_ms: int = 2500
    burst_max_age_ms: int = 8000

    spread_stats_window: int = 300
    book_rate_stats_window: int = 300
    book_rate_min_hz: float = 5.0
    book_rate_warn_hz: float = 3.0

    # === CVD & Microstructure ===
    cvd_reset_mode: str = "day"
    cvd_ema_period_delta: int = 10
    cvd_ema_period_cvd: int = 20
    cvd_robust_w: int = 500

    microbar_mode: str = "time"
    microbar_tf_ms: int = 1000
    microbar_volume_target: float = 0.0

    delta_bucket_ms: int = 1000

    # === Structure Detectors (Swing, Div) ===
    swing_left: int = 3
    swing_right: int = 3
    swing_min_bp: float = 5.0
    swing_min_range_bp: float = 1.0

    div_strength_min: float = 2.5
    div_min_price_bp: float = 5.0
    div_require_bias_hidden: bool = True

    # === Strong Gate Configuration ===
    strong_z_min: float = 3.0
    strong_use_iceberg: bool = False
    strong_need_reversal: int = 0
    strong_need_continuation: int = 0

    # === Calibration Settings ===
    calib_key_prefix: str = "calib:usps:v2"
    calib_regimes_set_prefix: str = "regimes:usps:v2"
    calib_ttl_sec: int = 3600
    calib_audit_enable: bool = False
    calib_audit_stream: str = "audit:calibration"
    calib_audit_stream_maxlen: int = 10000

    # Expert Calib Settings
    calib_atr_floor_mult: float = 0.5
    calib_dn_tier_fallback_usd: float = 100_000.0

    dn_tier0_usd: float = 0.0
    dn_tier1_usd: float = 0.0
    dn_tier2_usd: float = 0.0

    # NEW Round 7: Veto & Scenario V4 control
    exec_risk_ref_bps: float = 12.0        # Reference BPS for normalization (12-15 crypto)
    scenario_v4_enable: bool = False       # Enable Range/Trend V4 logic
    of_score_min_range: float | None = None
    strong_need_range: int = 3             # Required legs for range scenarios
    strong_need_escalated: int = 3         # Escalate to this many legs when thin/unstables
    of_score_agg: str = "weighted_mean"    # weighted_mean | sum

    # === Helper Fields ===
    tick_buffer: int = 500
    fallback_atr: float = 1.0

    # === Параметры Iceberg Detection ===
    iceberg_refresh_count: int = 2         # Количество refresh-ей для iceberg
    iceberg_min_duration: float = 1.5      # Минимальная длительность (секунды)
    iceberg_refresh_min_abs: float = 1.0   # Минимальный абсолютный объем refresh
    # B2: Iceberg refresh in USD
    iceberg_refresh_min_notional_usd: float | None = None

    # === Параметры уровней (Pivot Points) ===
    dist_atr_threshold: float = 0.5        # Расстояние до уровня (в ATR)

    # NEW: proximity in basis points (disabled by default -> no prod behavior change)
    dist_bp_threshold: float | None = None

    # NEW: how to combine ATR and BPS proximity if dist_bp_threshold is enabled:
    # - "or"  -> pass if near_atr OR near_bps (recommended default)
    # - "and" -> pass only if both (stricter)
    dist_mode: str = "or"

    # === Параметры генерации сигналов ===
    min_signal_interval_sec: int = 60      # Минимальный интервал между сигналами
    read_count: int = 100                  # Количество сообщений для чтения из stream
    read_block_ms: int = 1000              # Таймаут блокировки при чтении (мс)

    # === Risk Management ===
    stop_mode: str = "ATR"                 # Режим Stop Loss: ATR | PCT | POINTS
    stop_atr_mult: float = 0.6             # Множитель ATR для SL
    stop_pct: float = 0.2                  # Процент для SL (если mode=PCT)
    stop_points: float = 1.0               # Количество пунктов для SL (если mode=POINTS)

    tp_mode: str = "RR"                    # Режим Take Profit: RR | ATR | PCT
    tp_rr: str = "1,2,3"                   # Risk/Reward ratios для TP
    tp_atr_mults: str = "0.6,1.0,1.5"      # Множители ATR для TP (если mode=ATR)


    # === Orders Queue ===
    orders_queue_enabled: bool = False
    orders_queue_type: str = "market"
    orders_queue_profile: str = ""

    # === Confidence Scoring ===
    confidence_weights: dict[str, float] = field(default_factory=lambda: {
        "delta": 0.5, "speed": 0.2, "cluster": 0.2, "confirm": 0.1
    })
    confidence_floor: float = 0.15
    confidence_cap: float = 0.95
    confidence_speed_scale: float = 2.0
    confidence_confirm_bonus: dict[str, float] = field(default_factory=lambda: {
        "obi": 0.35, "absorption": 0.3, "iceberg_refresh": 0.35, "generic": 0.2
    })

    # === Global/Expert Flags ===
    require_strong_confirmation: bool = False
    strong_gate_shadow: bool = False

    # ATR Gate
    atr_bps_min_static: float = 0.0
    atr_gate_audit_only: bool = False

    # === ATR Sanity Calibrator ===
    atr_sanity_enable: bool = True
    atr_sanity_lo_bps: float = 0.50
    atr_sanity_hi_bps: float = 200.0
    atr_sanity_min_samples: int = 500
    atr_sanity_max_age_ms: int = 180_000
    atr_sanity_persist_min_bars: int = 30
    atr_sanity_persist_min_interval_ms: int = 60_000
    atr_sanity_max_bps_abs: float = 500.0
    atr_sanity_fallback_pct: float = 0.0003

    # === Calibration Persistence ===
    calib_persist_enable: bool = True
    calib_persist_min_bars: int = 120
    calib_persist_min_interval_ms: int = 60_000

    # === Strong Dynamic Need ===
    strong_dynamic_need_enable: bool = False

    # === ATR Floor / Delta Notional Tiers / Abs Levels (Dynamic) ===
    # ATR Floor Tiers (bps)
    atr_floor_t0_bps: float = 3.0
    atr_floor_t1_bps: float = 5.0
    atr_floor_t2_bps: float = 8.0

    # Default Tiers (0=Trend, 1=Range, 2=Thin)
    atr_floor_tier_default: int = 1
    atr_floor_tier_trend: int = 0
    atr_floor_tier_thin: int = 2
    atr_floor_tier_range: int = 1

    # Delta Notional Tiers (USD) - Dynamic overrides
    dn_tier_default: int = 1
    dn_tier_trend: int = 0
    dn_tier_range: int = 1
    dn_tier_thin: int = 2
    dn_persist_min_interval_ms: int = 60_000

    # Abs Level Tiers (Dynamic)
    abs_lvl_tier_default: int = 1
    abs_lvl_tier_range: int = 1
    abs_lvl_tier_trend: int = 0
    abs_lvl_tier_thin: int = 2

    abs_lvl_eff_quote_th: float = 0.0020
    abs_lvl_min_quote_delta: float = 0.0
    abs_lvl_th_drift_max: float = 0.35
    abs_lvl_th_range_max: float = 1.20
    abs_lvl_calib_min_samples: int = 300
    abs_lvl_th_unstable: int = 0

    # === SMT / Correlation ===
    smt_snapshot_every_ms: int = 1000
    smt_of_strong_valid_ms: int = 120_000
    smt_reclaim_valid_ms: int = 120_000
    smt_sweep_valid_ms: int = 120_000
    smt_retrace_atr: float = 0.0
    smt_near_zone_bp: float = 15.0
    smt_zone_max_bp: float = 15.0
    smt_snapshot_ttl_sec: int = 30

    # === ATR TF Calibration ===
    atr_tf_calib_refresh_ms: int = 60_000
    atr_tf_calib_persist_gap_ms: int = 300_000
    eq_atr_refresh_ms: int = 15_000

    # === Microbars ===
    micro_tf: str = "1s"

    # === Book Rate ===
    book_rate_crit_hz: float = 2.0

    # === Cancellation Spike Gate (L3-lite) ===
    cancel_spike_enable: bool = True
    cancel_spike_mode: str = "monitor"
    cancel_spike_alpha_slow: float = 0.02
    cancel_spike_ratio_th: float = 3.0
    cancel_spike_abs_th: float = 0.0
    cancel_spike_min_baseline: float = 0.0
    cancel_spike_use_robust_z: bool = True
    cancel_spike_window: int = 120
    cancel_spike_min_samples: int = 30
    cancel_spike_z_th: float = 3.5
    cancel_spike_min_taker_rate: float = 0.0

    # === GPU Acceleration ===
    gpu_offload_enabled: bool = False

    # === Специфичные параметры для разных типов инструментов ===
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Нормализует символ и выполняет базовые проверки."""
        self.symbol = normalize_symbol(self.symbol)
        self.validate()

    def validate(self) -> None:
        """
        Validates configuration consistency and thresholds.
        Raises ValueError if configuration is invalid.
        """
        # Logic modes
        if self.dist_mode not in ("or", "and"):
            raise ValueError(f"Invalid dist_mode='{self.dist_mode}'. Must be 'or' or 'and'.")

        valid_stop_modes = {"ATR", "PCT", "POINTS"}
        if self.stop_mode not in valid_stop_modes:
            raise ValueError(f"Invalid stop_mode='{self.stop_mode}'. Must be one of {valid_stop_modes}")

        valid_tp_modes = {"RR", "ATR", "PCT"}
        if self.tp_mode not in valid_tp_modes:
            raise ValueError(f"Invalid tp_mode='{self.tp_mode}'. Must be one of {valid_tp_modes}")

        # Numeric thresholds sanity checks
        if not (0.0 < self.obi_threshold <= 1.0):
            # 1.0 is technically possible but unlikely, >1 is definitely wrong for probability-like score
            if self.obi_threshold > 1.0:
                raise ValueError(f"obi_threshold={self.obi_threshold} is too high (max 1.0)")

        if self.weak_progress_atr > 2.0:
            raise ValueError(f"weak_progress_atr={self.weak_progress_atr} seems extremely large (>2.0)")

        if self.read_count < 1:
            raise ValueError(f"read_count={self.read_count} must be >= 1")

        if self.min_bucket_trades < 1:
            raise ValueError(f"min_bucket_trades={self.min_bucket_trades} must be >= 1")

    @classmethod
    def from_env(cls, symbol: str, base: Optional["OrderFlowConfig"] = None) -> "OrderFlowConfig":
        """
        Загружает конфигурацию из переменных окружения.
        
        Накладывает значения из env поверх `base` (если base не передан - 
        поверх дефолтов dataclass).
        
        Использует префикс из символа (XAU, BTC, ETH и т.д.) для поиска env переменных.
        Например, для XAUUSD будет искать XAU_DELTA_WINDOW, XAU_DELTA_Z_THRESHOLD и т.д.
        
        Для risk-параметров сначала ищет per-instrument переменные (BTC_STOP_MODE)
        затем глобальные (STOP_MODE), затем использует base.
        
        Args:
            symbol: Символ инструмента (XAUUSD, BTCUSD, etc)
            base: Базовая конфигурация для overlay (опционально)
            
        Returns:
            Экземпляр OrderFlowConfig с параметрами из env или defaults
        """
        sym = normalize_symbol(symbol)
        base_cfg = base or cls(symbol=sym)

        prefix = symbol_env_prefix(sym)

        obi_defaults = get_default_obi_settings(sym)
        dn_tiers = get_default_delta_tiers(sym)

        # V4 / Logic
        is_meme = prefix in ("PEPE", "SHIB", "DOGE", "BONK", "FLOKI", "WIF") or "1000" in sym

        def_exec_ref = 25.0 if is_meme else 12.0
        def_v4_en = True if is_meme else False
        def_need_range = 3 if is_meme else 3
        def_need_escalated = 3 if is_meme else 3
        def_score_range = 0.45 if is_meme else 0.65
        def_agg = "weighted_mean" if is_meme else "weighted_mean"

        cfg = cls(
            symbol=sym,

            exec_risk_ref_bps=_env_one(f"{prefix}_EXEC_RISK_REF_BPS", float,
                                      _env_one("EXEC_RISK_REF_BPS", float, def_exec_ref)),
            scenario_v4_enable=_to_bool(_env_one(f"{prefix}_SCENARIO_V4_ENABLE", str,
                                      _env_one("SCENARIO_V4_ENABLE", str, str(def_v4_en)))),
            strong_need_range=_env_one(f"{prefix}_STRONG_NEED_RANGE", int,
                                      _env_one("STRONG_NEED_RANGE", int, def_need_range)),
            strong_need_escalated=_env_one(f"{prefix}_STRONG_NEED_ESCALATED", int,
                                      _env_one("STRONG_NEED_ESCALATED", int, def_need_escalated)),
            of_score_min_range=_env_one(f"{prefix}_OF_SCORE_MIN_RANGE", float,
                                      _env_one("OF_SCORE_MIN_RANGE", float, def_score_range)),
            of_score_agg=_env_one(f"{prefix}_OF_SCORE_AGG", str,
                                 _env_one("OF_SCORE_AGG", str, def_agg)),

            delta_window_ticks=_env_one(f"{prefix}_DELTA_WINDOW", int, base_cfg.delta_window_ticks),
            delta_z_threshold=_env_one(f"{prefix}_DELTA_Z_THRESHOLD", float, base_cfg.delta_z_threshold),

            delta_abs_min=_env_one(f"{prefix}_DELTA_ABS_MIN", float, base_cfg.delta_abs_min),
            delta_abs_min_usd=_env_one(f"{prefix}_DELTA_ABS_MIN_USD", float, base_cfg.delta_abs_min_usd if base_cfg.delta_abs_min_usd is not None else get_default_usd_threshold(sym)),
            delta_abs_min_confirm=_env_one(f"{prefix}_DELTA_ABS_MIN_CONFIRM", float, base_cfg.delta_abs_min_confirm),

            weak_progress_atr=_env_one(f"{prefix}_WEAK_PROGRESS_ATR", float, base_cfg.weak_progress_atr),
            weak_progress_range_atr=_env_one(f"{prefix}_WEAK_PROGRESS_RANGE_ATR", float, base_cfg.weak_progress_range_atr),
            weak_progress_body_atr=_env_one(f"{prefix}_WEAK_PROGRESS_BODY_ATR", float, base_cfg.weak_progress_body_atr),

            absorption_min_volume=_env_one(f"{prefix}_ABSORPTION_MIN_VOLUME", float, base_cfg.absorption_min_volume),
            absorption_price_tolerance=_env_one(f"{prefix}_ABSORPTION_PRICE_TOLERANCE", float, base_cfg.absorption_price_tolerance),
            absorption_window_sec=_env_one(f"{prefix}_ABSORPTION_WINDOW_SEC", float, base_cfg.absorption_window_sec),
            abs_lvl_enable=_env_one(f"{prefix}_ABS_LVL_ENABLE", _to_bool, base_cfg.abs_lvl_enable),
            abs_lvl_counts_as=_env_one(f"{prefix}_ABS_LVL_COUNTS_AS", str, base_cfg.abs_lvl_counts_as),

            obi_threshold=_env_one(f"{prefix}_OBI_THRESHOLD", float, base_cfg.obi_threshold if base_cfg.obi_threshold != 0.5 else obi_defaults["obi_threshold"]),
            obi_min_duration=_env_one(f"{prefix}_OBI_MIN_DURATION", float, base_cfg.obi_min_duration if base_cfg.obi_min_duration != 2.0 else obi_defaults["obi_min_duration"]),
            obi_depth=_env_one(f"{prefix}_OBI_DEPTH", int, base_cfg.obi_depth),
            obi_hold_secs=_env_one(f"{prefix}_OBI_HOLD_SECS", float, base_cfg.obi_hold_secs),

            iceberg_refresh_count=_env_one(f"{prefix}_ICEBERG_REFRESH", int, base_cfg.iceberg_refresh_count),
            iceberg_min_duration=_env_one(f"{prefix}_ICEBERG_DURATION", float, base_cfg.iceberg_min_duration),
            iceberg_refresh_min_abs=_env_one(
                f"{prefix}_ICEBERG_REFRESH_MIN_ABS",
                float,
                base_cfg.iceberg_refresh_min_abs
            ),
            iceberg_refresh_min_notional_usd=_env_one(
                f"{prefix}_ICEBERG_REFRESH_MIN_NOTIONAL_USD",
                float,
                base_cfg.iceberg_refresh_min_notional_usd
            ),

            cooldown_reversal_sec=_env_one(f"{prefix}_COOLDOWN_REVERSAL_SEC", int, base_cfg.cooldown_reversal_sec),
            cooldown_continuation_sec=_env_one(f"{prefix}_COOLDOWN_CONTINUATION_SEC", int, base_cfg.cooldown_continuation_sec),
            cooldown_min_ms=_env_one(f"{prefix}_COOLDOWN_MIN_MS", int, base_cfg.cooldown_min_ms),
            cooldown_max_ms=_env_one(f"{prefix}_COOLDOWN_MAX_MS", int, base_cfg.cooldown_max_ms),
            cooldown_mul_thin=_env_one(f"{prefix}_COOLDOWN_MUL_THIN", float, base_cfg.cooldown_mul_thin),
            cooldown_spread_hi_bp=_env_one(f"{prefix}_COOLDOWN_SPREAD_HI_BP", float, base_cfg.cooldown_spread_hi_bp),
            cooldown_mul_wide_spread=_env_one(f"{prefix}_COOLDOWN_MUL_WIDE_SPREAD", float, base_cfg.cooldown_mul_wide_spread),
            cooldown_mul_pressure_hi=_env_one(f"{prefix}_COOLDOWN_MUL_PRESSURE_HI", float, base_cfg.cooldown_mul_pressure_hi),
            pressure_window_ms=_env_one(f"{prefix}_PRESSURE_WINDOW_MS", int, base_cfg.pressure_window_ms),
            burst_window_ms=_env_one(f"{prefix}_BURST_WINDOW_MS", int, base_cfg.burst_window_ms),
            burst_max_age_ms=_env_one(f"{prefix}_BURST_MAX_AGE_MS", int, base_cfg.burst_max_age_ms),
            spread_stats_window=_env_one(f"{prefix}_SPREAD_STATS_WINDOW", int, base_cfg.spread_stats_window),
            book_rate_stats_window=_env_one(f"{prefix}_BOOK_RATE_STATS_WINDOW", int, base_cfg.book_rate_stats_window),

            book_rate_min_hz=_env_one(
                f"{prefix}_BOOK_RATE_MIN_HZ",
                float,
                base_cfg.book_rate_min_hz if base_cfg.book_rate_min_hz != 5.0 else get_default_book_rate_settings(sym).get("book_rate_min_hz", 5.0),
            ),
            book_rate_warn_hz=_env_one(
                f"{prefix}_BOOK_RATE_WARN_HZ",
                float,
                base_cfg.book_rate_warn_hz if base_cfg.book_rate_warn_hz != 3.0 else get_default_book_rate_settings(sym).get("book_rate_warn_hz", 3.0),
            ),

            cvd_reset_mode=_env_one(f"{prefix}_CVD_RESET_MODE", str, base_cfg.cvd_reset_mode),
            cvd_ema_period_delta=_env_one(f"{prefix}_CVD_EMA_PERIOD_DELTA", int, base_cfg.cvd_ema_period_delta),
            cvd_ema_period_cvd=_env_one(f"{prefix}_CVD_EMA_PERIOD_CVD", int, base_cfg.cvd_ema_period_cvd),
            cvd_robust_w=_env_one(f"{prefix}_CVD_ROBUST_W", int, base_cfg.cvd_robust_w),
            microbar_mode=_env_one(f"{prefix}_MICROBAR_MODE", str, base_cfg.microbar_mode),
            microbar_tf_ms=_env_one(f"{prefix}_MICROBAR_TF_MS", int, base_cfg.microbar_tf_ms),
            microbar_volume_target=_env_one(f"{prefix}_MICROBAR_VOLUME_TARGET", float, base_cfg.microbar_volume_target),

            swing_left=_env_one(f"{prefix}_SWING_LEFT", int, base_cfg.swing_left),
            swing_right=_env_one(f"{prefix}_SWING_RIGHT", int, base_cfg.swing_right),
            swing_min_bp=_env_one(f"{prefix}_SWING_MIN_BP", float, base_cfg.swing_min_bp),
            swing_min_range_bp=_env_one(f"{prefix}_SWING_MIN_RANGE_BP", float, base_cfg.swing_min_range_bp),
            div_strength_min=_env_one(f"{prefix}_DIV_STRENGTH_MIN", float, base_cfg.div_strength_min),
            div_min_price_bp=_env_one(f"{prefix}_DIV_MIN_PRICE_BP", float, base_cfg.div_min_price_bp),
            div_require_bias_hidden=_env_one(f"{prefix}_DIV_REQUIRE_BIAS_HIDDEN", _to_bool, base_cfg.div_require_bias_hidden),

            strong_z_min=_env_one(f"{prefix}_STRONG_Z_MIN", float, base_cfg.strong_z_min),
            strong_use_iceberg=_env_one(f"{prefix}_STRONG_USE_ICEBERG", _to_bool, base_cfg.strong_use_iceberg),
            strong_need_reversal=_env_one(f"{prefix}_STRONG_NEED_REVERSAL", int, base_cfg.strong_need_reversal),
            strong_need_continuation=_env_one(f"{prefix}_STRONG_NEED_CONTINUATION", int, base_cfg.strong_need_continuation),

            calib_key_prefix=_env_one(f"{prefix}_CALIB_KEY_PREFIX", str, base_cfg.calib_key_prefix),
            calib_regimes_set_prefix=_env_one(f"{prefix}_CALIB_REGIMES_SET_PREFIX", str, base_cfg.calib_regimes_set_prefix),
            calib_ttl_sec=_env_one(f"{prefix}_CALIB_TTL_SEC", int, base_cfg.calib_ttl_sec),
            calib_audit_enable=_env_one(f"{prefix}_CALIB_AUDIT_ENABLE", _to_bool, base_cfg.calib_audit_enable),
            calib_audit_stream=_env_one(f"{prefix}_CALIB_AUDIT_STREAM", str, base_cfg.calib_audit_stream),
            calib_audit_stream_maxlen=_env_one(f"{prefix}_CALIB_AUDIT_STREAM_MAXLEN", int, base_cfg.calib_audit_stream_maxlen),

            dn_tier0_usd=_env_one(f"{prefix}_DN_TIER0_USD", float, base_cfg.dn_tier0_usd if base_cfg.dn_tier0_usd != 0.0 else dn_tiers["tier0"]),
            dn_tier1_usd=_env_one(f"{prefix}_DN_TIER1_USD", float, base_cfg.dn_tier1_usd if base_cfg.dn_tier1_usd != 0.0 else dn_tiers["tier1"]),
            dn_tier2_usd=_env_one(f"{prefix}_DN_TIER2_USD", float, base_cfg.dn_tier2_usd if base_cfg.dn_tier2_usd != 0.0 else dn_tiers["tier2"]),

            delta_bucket_ms=_env_one(f"{prefix}_DELTA_BUCKET_MS", int, base_cfg.delta_bucket_ms),
            # === Параметры уровней (Pivot Points) ===
            dist_atr_threshold=_env_one(f"{prefix}_DIST_ATR_THRESHOLD", float, base_cfg.dist_atr_threshold),

            # Use symbol-specific default for dist_bp if not explicitly overridden
            dist_bp_threshold=_env_one(
                f"{prefix}_DIST_BP_THRESHOLD",
                float,
                base_cfg.dist_bp_threshold if base_cfg.dist_bp_threshold is not None else get_default_dist_bp_threshold(sym),
            ),

            dist_mode=_env_one(f"{prefix}_DIST_MODE", str, base_cfg.dist_mode),

            min_signal_interval_sec=_env_one(f"{prefix}_MIN_SIGNAL_INTERVAL", int, base_cfg.min_signal_interval_sec),
            tick_buffer=_env_one(f"{prefix}_TICK_BUFFER", int, base_cfg.tick_buffer),
            read_count=_env_one(f"{prefix}_READ_COUNT", int, base_cfg.read_count),
            read_block_ms=_env_one(f"{prefix}_READ_BLOCK_MS", int, base_cfg.read_block_ms),
            fallback_atr=_env_one(f"{prefix}_FALLBACK_ATR", float, base_cfg.fallback_atr),

            orders_queue_enabled=_env_one(f"{prefix}_ORDERS_QUEUE_ENABLED", _to_bool, base_cfg.orders_queue_enabled),
            orders_queue_type=_env_one(f"{prefix}_ORDERS_QUEUE_TYPE", str, base_cfg.orders_queue_type),
            orders_queue_profile=_env_one(f"{prefix}_ORDERS_QUEUE_PROFILE", str, base_cfg.orders_queue_profile),

            confidence_floor=_env_one(f"{prefix}_CONFIDENCE_FLOOR", float, base_cfg.confidence_floor),
            confidence_cap=_env_one(f"{prefix}_CONFIDENCE_CAP", float, base_cfg.confidence_cap),
            confidence_speed_scale=_env_one(f"{prefix}_CONFIDENCE_SPEED_SCALE", float, base_cfg.confidence_speed_scale),

            require_strong_confirmation=_env_one(f"{prefix}_REQUIRE_STRONG_CONFIRMATION", _to_bool, base_cfg.require_strong_confirmation),
            strong_gate_shadow=_env_one(f"{prefix}_STRONG_GATE_SHADOW", _to_bool, base_cfg.strong_gate_shadow),

            atr_bps_min_static=_env_one(f"{prefix}_ATR_BPS_MIN_STATIC", float, base_cfg.atr_bps_min_static),
            atr_gate_audit_only=_env_one(f"{prefix}_ATR_GATE_AUDIT_ONLY", _to_bool, base_cfg.atr_gate_audit_only),

            sweep_valid_ms=_env_one(f"{prefix}_SWEEP_VALID_MS", int, base_cfg.sweep_valid_ms),
            reclaim_signal_valid_ms=_env_one(f"{prefix}_RECLAIM_SIGNAL_VALID_MS", int, base_cfg.reclaim_signal_valid_ms),
            reclaim_hold_bars=_env_one(f"{prefix}_RECLAIM_HOLD_BARS", int, base_cfg.reclaim_hold_bars),
            obi_event_ttl_ms=_env_one(f"{prefix}_OBI_EVENT_TTL_MS", int, base_cfg.obi_event_ttl_ms),
            obi_stable_min_secs=_env_one(f"{prefix}_OBI_STABLE_MIN_SECS", float, base_cfg.obi_stable_min_secs),
            iceberg_event_ttl_ms=_env_one(f"{prefix}_ICEBERG_EVENT_TTL_MS", int, base_cfg.iceberg_event_ttl_ms),
            iceberg_strict_refresh_min=_env_one(f"{prefix}_ICEBERG_STRICT_REFRESH_MIN", int, base_cfg.iceberg_strict_refresh_min),
            iceberg_strict_duration_min=_env_one(f"{prefix}_ICEBERG_STRICT_DURATION_MIN", float, base_cfg.iceberg_strict_duration_min),
            iceberg_strict_dist_bp=_env_one(f"{prefix}_ICEBERG_STRICT_DIST_BP", float, base_cfg.iceberg_strict_dist_bp),

            score_z_ref=_env_one(f"{prefix}_SCORE_Z_REF", float, base_cfg.score_z_ref),
            w_z=_env_one(f"{prefix}_W_Z", float, base_cfg.w_z),
            w_wp=_env_one(f"{prefix}_W_WP", float, base_cfg.w_wp),
            w_reclaim=_env_one(f"{prefix}_W_RECLAIM", float, base_cfg.w_reclaim),
            w_obi=_env_one(f"{prefix}_W_OBI", float, base_cfg.w_obi),
            w_ice=_env_one(f"{prefix}_W_ICE", float, base_cfg.w_ice),
            w_abs=_env_one(f"{prefix}_W_ABS", float, base_cfg.w_abs),
            of_score_min=_env_one(f"{prefix}_OF_SCORE_MIN", float, base_cfg.of_score_min),

            publish_of_confirm=_env_one(f"{prefix}_PUBLISH_OF_CONFIRM", _to_bool, base_cfg.publish_of_confirm),
            of_confirm_stream=_env_one(f"{prefix}_OF_CONFIRM_STREAM", str, base_cfg.of_confirm_stream),

            # Risk: сначала per-instrument, затем глобальные, затем base
            stop_mode=_env_first([f"{prefix}_STOP_MODE", "STOP_MODE"], str, base_cfg.stop_mode),
            stop_atr_mult=_env_first([f"{prefix}_STOP_ATR_MULT", "STOP_ATR_MULT"], float, base_cfg.stop_atr_mult),
            stop_pct=_env_first([f"{prefix}_STOP_PCT", "STOP_PCT"], float, base_cfg.stop_pct),
            stop_points=_env_first([f"{prefix}_STOP_POINTS", "STOP_POINTS"], float, base_cfg.stop_points),

            tp_mode=_env_first([f"{prefix}_TP_MODE", "TP_MODE"], str, base_cfg.tp_mode),
            tp_rr=_env_first([f"{prefix}_TP_RR", "TP_RR"], str, base_cfg.tp_rr),
            tp_atr_mults=_env_first([f"{prefix}_TP_ATR_MULTS", "TP_ATR_MULTS"], str, base_cfg.tp_atr_mults),



            # ATR Sanity
            atr_sanity_enable=_env_one(f"{prefix}_ATR_SANITY_ENABLE", _to_bool, base_cfg.atr_sanity_enable),
            atr_sanity_lo_bps=_env_one(f"{prefix}_ATR_SANITY_LO_BPS", float, base_cfg.atr_sanity_lo_bps),
            atr_sanity_hi_bps=_env_one(f"{prefix}_ATR_SANITY_HI_BPS", float, base_cfg.atr_sanity_hi_bps),

            # === Cancellation Spike Gate ===
            cancel_spike_enable=_env_one(f"{prefix}_CANCEL_SPIKE_ENABLE", _to_bool, get_default_cancel_spike_settings(sym)["cancel_spike_enable"]),
            cancel_spike_mode=_env_one(f"{prefix}_CANCEL_SPIKE_MODE", str, get_default_cancel_spike_settings(sym)["cancel_spike_mode"]),
            cancel_spike_alpha_slow=_env_one(f"{prefix}_CANCEL_SPIKE_ALPHA_SLOW", float, get_default_cancel_spike_settings(sym)["cancel_spike_alpha_slow"]),
            cancel_spike_ratio_th=_env_one(f"{prefix}_CANCEL_SPIKE_RATIO_TH", float, get_default_cancel_spike_settings(sym)["cancel_spike_ratio_th"]),
            cancel_spike_abs_th=_env_one(f"{prefix}_CANCEL_SPIKE_ABS_TH", float, get_default_cancel_spike_settings(sym)["cancel_spike_abs_th"]),
            cancel_spike_min_baseline=_env_one(f"{prefix}_CANCEL_SPIKE_MIN_BASELINE", float, get_default_cancel_spike_settings(sym)["cancel_spike_min_baseline"]),
            cancel_spike_use_robust_z=_env_one(f"{prefix}_CANCEL_SPIKE_USE_ROBUST_Z", _to_bool, get_default_cancel_spike_settings(sym)["cancel_spike_use_robust_z"]),
            cancel_spike_window=_env_one(f"{prefix}_CANCEL_SPIKE_WINDOW", int, get_default_cancel_spike_settings(sym)["cancel_spike_window"]),
            cancel_spike_min_samples=_env_one(f"{prefix}_CANCEL_SPIKE_MIN_SAMPLES", int, get_default_cancel_spike_settings(sym)["cancel_spike_min_samples"]),
            cancel_spike_z_th=_env_one(f"{prefix}_CANCEL_SPIKE_Z_TH", float, get_default_cancel_spike_settings(sym)["cancel_spike_z_th"]),
            cancel_spike_min_taker_rate=_env_one(f"{prefix}_CANCEL_SPIKE_MIN_TAKER_RATE", float, get_default_cancel_spike_settings(sym)["cancel_spike_min_taker_rate"]),
            atr_sanity_min_samples=_env_one(f"{prefix}_ATR_SANITY_MIN_SAMPLES", int, base_cfg.atr_sanity_min_samples),
            atr_sanity_max_age_ms=_env_one(f"{prefix}_ATR_SANITY_MAX_AGE_MS", int, base_cfg.atr_sanity_max_age_ms),
            atr_sanity_persist_min_bars=_env_one(f"{prefix}_ATR_SANITY_PERSIST_MIN_BARS", int, base_cfg.atr_sanity_persist_min_bars),
            atr_sanity_persist_min_interval_ms=_env_one(f"{prefix}_ATR_SANITY_PERSIST_MIN_INTERVAL_MS", int, base_cfg.atr_sanity_persist_min_interval_ms),
            atr_sanity_max_bps_abs=_env_one(f"{prefix}_ATR_SANITY_MAX_BPS_ABS", float, base_cfg.atr_sanity_max_bps_abs),
            atr_sanity_fallback_pct=_env_one(f"{prefix}_ATR_SANITY_FALLBACK_PCT", float, base_cfg.atr_sanity_fallback_pct),

            # Calibration Persistence
            calib_persist_enable=_env_one(f"{prefix}_CALIB_PERSIST_ENABLE", _to_bool, base_cfg.calib_persist_enable),
            calib_persist_min_bars=_env_one(f"{prefix}_CALIB_PERSIST_MIN_BARS", int, base_cfg.calib_persist_min_bars),
            calib_persist_min_interval_ms=_env_one(f"{prefix}_CALIB_PERSIST_MIN_INTERVAL_MS", int, base_cfg.calib_persist_min_interval_ms),

            # Strong Dynamic Need
            strong_dynamic_need_enable=_env_one(f"{prefix}_STRONG_DYNAMIC_NEED_ENABLE", _to_bool, base_cfg.strong_dynamic_need_enable),

            # ATR Floor Tiers
            atr_floor_t0_bps=_env_one(f"{prefix}_ATR_FLOOR_T0_BPS", float, base_cfg.atr_floor_t0_bps),
            atr_floor_t1_bps=_env_one(f"{prefix}_ATR_FLOOR_T1_BPS", float, base_cfg.atr_floor_t1_bps),
            atr_floor_t2_bps=_env_one(f"{prefix}_ATR_FLOOR_T2_BPS", float, base_cfg.atr_floor_t2_bps),

            atr_floor_tier_default=_env_one(f"{prefix}_ATR_FLOOR_TIER_DEFAULT", int, base_cfg.atr_floor_tier_default),
            atr_floor_tier_trend=_env_one(f"{prefix}_ATR_FLOOR_TIER_TREND", int, base_cfg.atr_floor_tier_trend),
            atr_floor_tier_thin=_env_one(f"{prefix}_ATR_FLOOR_TIER_THIN", int, base_cfg.atr_floor_tier_thin),
            atr_floor_tier_range=_env_one(f"{prefix}_ATR_FLOOR_TIER_RANGE", int, base_cfg.atr_floor_tier_range),

            # Delta Notional Tiers
            dn_tier_default=_env_one(f"{prefix}_DN_TIER_DEFAULT", int, base_cfg.dn_tier_default),
            dn_tier_trend=_env_one(f"{prefix}_DN_TIER_TREND", int, base_cfg.dn_tier_trend),
            dn_tier_range=_env_one(f"{prefix}_DN_TIER_RANGE", int, base_cfg.dn_tier_range),
            dn_tier_thin=_env_one(f"{prefix}_DN_TIER_THIN", int, base_cfg.dn_tier_thin),
            dn_persist_min_interval_ms=_env_one(f"{prefix}_DN_PERSIST_MIN_INTERVAL_MS", int, base_cfg.dn_persist_min_interval_ms),

            # Abs Level Tiers
            abs_lvl_tier_default=_env_one(f"{prefix}_ABS_LVL_TIER_DEFAULT", int, base_cfg.abs_lvl_tier_default),
            abs_lvl_tier_range=_env_one(f"{prefix}_ABS_LVL_TIER_RANGE", int, base_cfg.abs_lvl_tier_range),
            abs_lvl_tier_trend=_env_one(f"{prefix}_ABS_LVL_TIER_TREND", int, base_cfg.abs_lvl_tier_trend),
            abs_lvl_tier_thin=_env_one(f"{prefix}_ABS_LVL_TIER_THIN", int, base_cfg.abs_lvl_tier_thin),

            abs_lvl_eff_quote_th=_env_one(f"{prefix}_ABS_LVL_EFF_QUOTE_TH", float, base_cfg.abs_lvl_eff_quote_th),
            abs_lvl_min_quote_delta=_env_one(f"{prefix}_ABS_LVL_MIN_QUOTE_DELTA", float, base_cfg.abs_lvl_min_quote_delta),
            abs_lvl_th_drift_max=_env_one(f"{prefix}_ABS_LVL_TH_DRIFT_MAX", float, base_cfg.abs_lvl_th_drift_max),
            abs_lvl_th_range_max=_env_one(f"{prefix}_ABS_LVL_TH_RANGE_MAX", float, base_cfg.abs_lvl_th_range_max),
            abs_lvl_calib_min_samples=_env_one(f"{prefix}_ABS_LVL_CALIB_MIN_SAMPLES", int, base_cfg.abs_lvl_calib_min_samples),
            abs_lvl_th_unstable=_env_one(f"{prefix}_ABS_LVL_TH_UNSTABLE", int, base_cfg.abs_lvl_th_unstable),

            # SMT / Correlation
            smt_snapshot_every_ms=_env_one(f"{prefix}_SMT_SNAPSHOT_EVERY_MS", int, base_cfg.smt_snapshot_every_ms),
            smt_of_strong_valid_ms=_env_one(f"{prefix}_SMT_OF_STRONG_VALID_MS", int, base_cfg.smt_of_strong_valid_ms),
            smt_reclaim_valid_ms=_env_one(f"{prefix}_SMT_RECLAIM_VALID_MS", int, base_cfg.smt_reclaim_valid_ms),
            smt_sweep_valid_ms=_env_one(f"{prefix}_SMT_SWEEP_VALID_MS", int, base_cfg.smt_sweep_valid_ms),
            smt_retrace_atr=_env_one(f"{prefix}_SMT_RETRACE_ATR", float, base_cfg.smt_retrace_atr),
            smt_near_zone_bp=_env_one(f"{prefix}_SMT_NEAR_ZONE_BP", float, base_cfg.smt_near_zone_bp),
            smt_zone_max_bp=_env_one(f"{prefix}_SMT_ZONE_MAX_BP", float, base_cfg.smt_zone_max_bp),
            smt_snapshot_ttl_sec=_env_one(f"{prefix}_SMT_SNAPSHOT_TTL_SEC", int, base_cfg.smt_snapshot_ttl_sec),

            # ATR TF Calibration
            atr_tf_calib_refresh_ms=_env_one(f"{prefix}_ATR_TF_CALIB_REFRESH_MS", int, base_cfg.atr_tf_calib_refresh_ms),
            atr_tf_calib_persist_gap_ms=_env_one(f"{prefix}_ATR_TF_CALIB_PERSIST_GAP_MS", int, base_cfg.atr_tf_calib_persist_gap_ms),
            eq_atr_refresh_ms=_env_one(f"{prefix}_EQ_ATR_REFRESH_MS", int, base_cfg.eq_atr_refresh_ms),

            # Microbars
            micro_tf=_env_one(f"{prefix}_MICRO_TF", str, base_cfg.micro_tf),

            # Book Rate
            book_rate_crit_hz=_env_one(f"{prefix}_BOOK_RATE_CRIT_HZ", float, base_cfg.book_rate_crit_hz),

            gpu_offload_enabled=_to_bool(_env_first([f"{prefix}_GPU_OFFLOAD_ENABLED", "GPU_OFFLOAD_ENABLED", "GPU_ENABLED"], str, base_cfg.gpu_offload_enabled)),

            metadata=dict(base_cfg.metadata or {}),
        )

        return cfg


# ═════════════════════════════════════════════════════════════════════
# ПРЕСЕТЫ ДЛЯ ПОПУЛЯРНЫХ ИНСТРУМЕНТОВ
# ═════════════════════════════════════════════════════════════════════

# Forex: Gold (XAUUSD)
XAUUSD_CONFIG = OrderFlowConfig(
    symbol="XAUUSD",
    delta_window_ticks=120,
    delta_z_threshold=3.0,
    weak_progress_atr=0.10,
    obi_threshold=0.5,
    obi_min_duration=2.0,
    iceberg_refresh_count=2,
    iceberg_min_duration=1.5,
    iceberg_refresh_min_abs=1.0,
    dist_atr_threshold=0.5,
    min_signal_interval_sec=60,  # 1 минута между сигналами
    metadata={
        "asset_class": "forex",
        "base_currency": "XAU",
        "quote_currency": "USD",
    }
)

XAUUSD_SPECS = SymbolSpecs(
    symbol="XAUUSD",
    contract_size=100.0,
    pip_value=0.01,
    lot_step=0.01,
    min_lot=0.01,
    max_lot=100.0,
    tick_value=0.01,
    point_value=0.01,
    price_decimals=2,
    volume_decimals=2
)

# TradFi Perp (Binance): Gold (XAUUSDT)
# TradingView: XAUUSDT.P is the perpetual symbol.
# Binance Futures page exists for XAUUSDT perpetual.
XAUUSDT_CONFIG = OrderFlowConfig(
    symbol="XAUUSDT",
    delta_window_ticks=120,
    delta_z_threshold=3.0,
    # Prefer USD-normalized gate (see from_env): bootstrap to avoid noise
    delta_abs_min_usd=5_000.0,
    weak_progress_atr=0.10,
    weak_progress_range_atr=0.35,
    weak_progress_body_atr=0.25,
    # Use "major-like" OBI defaults rather than conservative 0.5/2.0
    obi_threshold=0.30,
    obi_min_duration=1.8,
    iceberg_refresh_count=3,
    iceberg_min_duration=1.0,
    iceberg_refresh_min_abs=0.5,
    dist_atr_threshold=0.45,
    dist_bp_threshold=12.0,
    min_signal_interval_sec=30,
    stop_mode="ATR",
    stop_atr_mult=1.2,
    tp_mode="RR",
    tp_rr="1,1.5,2.5",
    # Start with non-veto to avoid overblocking until L3-lite is validated
    cancel_spike_enable=True,
    cancel_spike_mode="monitor",
    metadata={
        "asset_class": "tradfi_perp",
        "base_currency": "XAU",
        "quote_currency": "USDT",
        "underlying_unit": "troy_ounce",
        "venue_hint": "binance_usds_m",
    }
)

XAUUSDT_SPECS = SymbolSpecs(
    symbol="XAUUSDT",
    # On Binance metals perps the contract is typically quoted per 1 troy ounce (bootstrap assumption).
    # Use exchangeInfo in prod to confirm exact filters.
    contract_size=1.0,
    pip_value=0.01,
    lot_step=0.001,
    min_lot=0.001,
    max_lot=1_000_000.0,
    tick_value=0.01,
    point_value=0.01,
    price_decimals=2,
    volume_decimals=3,
    delta_z=3.0,
)

# Crypto: Bitcoin (BTCUSD / BTCUSDT)
BTCUSD_CONFIG = OrderFlowConfig(
    symbol="BTCUSD",
    delta_window_ticks=120,
    delta_z_threshold=2.7,          # BTC_DELTA_Z_THRESHOLD=2.7
    delta_abs_min=0.5,
    delta_abs_min_confirm=0.5,
    weak_progress_atr=0.15,
    obi_threshold=0.35,             # BTC_OBI_THRESHOLD=0.35
    obi_min_duration=1.5,
    iceberg_refresh_count=3,
    iceberg_min_duration=1.0,
    iceberg_refresh_min_abs=1.0,
    dist_atr_threshold=0.4,
    min_signal_interval_sec=20,     # BTC_MIN_SIGNAL_INTERVAL=20
    stop_mode="ATR",                # BTC_STOP_MODE=ATR
    stop_atr_mult=1.2,              # BTC_STOP_ATR_MULT=0.8
    tp_mode="RR",
    tp_rr="1,1.5,2.5",              # BTC_TP_RR=1,1.5,2.5
    tp_atr_mults="0.6,1.0,1.5",
    metadata={
        "asset_class": "crypto",
        "base_currency": "BTC",
        "quote_currency": "USD",
    }
)

BTCUSD_SPECS = SymbolSpecs(
    symbol="BTCUSD",
    contract_size=1.0,              # Для крипты обычно 1:1
    pip_value=0.01,
    lot_step=0.001,                 # Меньший шаг лота
    min_lot=0.001,
    max_lot=1000.0,
    tick_value=0.01,
    point_value=0.01,
    price_decimals=2,               # BTC обычно 2 знака ($50000.00)
    volume_decimals=3               # Объем до 3 знаков (0.001 BTC)
)

# BTCUSDT - отдельный config/specs (алиас с обновленным metadata)
BTCUSDT_CONFIG = replace(
    BTCUSD_CONFIG,
    symbol="BTCUSDT",
    metadata={**BTCUSD_CONFIG.metadata, "quote_currency": "USDT"}
)
BTCUSDT_SPECS = replace(BTCUSD_SPECS, symbol="BTCUSDT")

# Crypto: Ethereum (ETHUSD / ETHUSDT)
ETHUSD_CONFIG = OrderFlowConfig(
    symbol="ETHUSD",
    delta_window_ticks=120,
    delta_z_threshold=2.5,
    delta_abs_min=0.5,
    delta_abs_min_confirm=0.5,
    weak_progress_atr=0.15,
    obi_threshold=0.4,
    obi_min_duration=1.5,
    iceberg_refresh_count=3,
    iceberg_min_duration=1.0,
    iceberg_refresh_min_abs=1.0,
    dist_atr_threshold=0.4,
    min_signal_interval_sec=30,
    metadata={
        "asset_class": "crypto",
        "base_currency": "ETH",
        "quote_currency": "USD",
    }
)

ETHUSD_SPECS = SymbolSpecs(
    symbol="ETHUSD",
    contract_size=1.0,
    pip_value=0.01,
    lot_step=0.01,
    min_lot=0.01,
    max_lot=1000.0,
    tick_value=0.01,
    point_value=0.01,
    price_decimals=2,               # ETH обычно 2 знака ($3000.00)
    volume_decimals=2
)

# ETHUSDT - отдельный config/specs (алиас с обновленным metadata)
ETHUSDT_CONFIG = replace(
    ETHUSD_CONFIG,
    symbol="ETHUSDT",
    metadata={**ETHUSD_CONFIG.metadata, "quote_currency": "USDT"}
)
ETHUSDT_SPECS = replace(ETHUSD_SPECS, symbol="ETHUSDT")

# Crypto: Binance Coin (BNBUSD / BNBUSDT)
BNBUSD_CONFIG = OrderFlowConfig(
    symbol="BNBUSD",
    delta_window_ticks=120,
    delta_z_threshold=2.9,
    delta_abs_min=0.5,
    delta_abs_min_confirm=0.5,
    weak_progress_atr=0.17,
    obi_threshold=0.34,
    obi_min_duration=1.3,
    iceberg_refresh_count=3,
    iceberg_min_duration=1.0,
    iceberg_refresh_min_abs=1.5,
    dist_atr_threshold=0.42,
    min_signal_interval_sec=18,
    stop_mode="ATR",
    stop_atr_mult=1.2,
    tp_mode="RR",
    tp_rr="1,1.5,2.5",
    tp_atr_mults="0.65,1.05,1.6",
    metadata={"asset_class": "crypto", "base_currency": "BNB", "quote_currency": "USD"},
)

BNBUSD_SPECS = SymbolSpecs(
    symbol="BNBUSD",
    contract_size=1.0,
    pip_value=0.01,
    lot_step=0.01,
    min_lot=0.01,
    max_lot=1_000_000.0,
    tick_value=0.01,
    point_value=0.01,
    price_decimals=2,
    volume_decimals=2
)

# BNBUSDT - отдельный config/specs (алиас с обновленным metadata)
BNBUSDT_CONFIG = replace(
    BNBUSD_CONFIG,
    symbol="BNBUSDT",
    metadata={**BNBUSD_CONFIG.metadata, "quote_currency": "USDT"}
)
BNBUSDT_SPECS = replace(BNBUSD_SPECS, symbol="BNBUSDT")

# Crypto: Solana (SOLUSD / SOLUSDT)
SOLUSD_CONFIG = OrderFlowConfig(
    symbol="SOLUSD",
    delta_window_ticks=110,
    delta_z_threshold=3.0,          # чуть выше BTC: SOL шумнее
    delta_abs_min=0.5,
    delta_abs_min_confirm=0.5,
    weak_progress_atr=0.18,
    obi_threshold=0.33,
    obi_min_duration=1.2,
    iceberg_refresh_count=3,
    iceberg_min_duration=0.9,
    iceberg_refresh_min_abs=2.0,
    dist_atr_threshold=0.45,
    min_signal_interval_sec=15,
    stop_mode="ATR",
    stop_atr_mult=1.2,              # SOL волатильнее -> стоп чуть шире
    tp_mode="RR",
    tp_rr="1,1.6,2.6",
    tp_atr_mults="0.7,1.1,1.7",
    metadata={"asset_class": "crypto", "base_currency": "SOL", "quote_currency": "USD"},
)

SOLUSD_SPECS = SymbolSpecs(
    symbol="SOLUSD",
    contract_size=1.0,
    pip_value=0.01,
    lot_step=0.01,
    min_lot=0.01,
    max_lot=1_000_000.0,
    tick_value=0.01,
    point_value=0.01,
    price_decimals=2,
    volume_decimals=2
)

# SOLUSDT - отдельный config/specs (алиас с обновленным metadata)
SOLUSDT_CONFIG = replace(
    SOLUSD_CONFIG,
    symbol="SOLUSDT",
    metadata={**SOLUSD_CONFIG.metadata, "quote_currency": "USDT"},
)
SOLUSDT_SPECS = replace(SOLUSD_SPECS, symbol="SOLUSDT")

# Crypto: Ripple (XRPUSD / XRPUSDT)
XRPUSD_CONFIG = OrderFlowConfig(
    symbol="XRPUSD",
    delta_window_ticks=150,         # больше окно: много мелких тиков
    delta_z_threshold=3.2,          # XRP часто "пилит" -> выше порог
    delta_abs_min=0.5,
    delta_abs_min_confirm=0.5,
    weak_progress_atr=0.20,
    obi_threshold=0.30,
    obi_min_duration=1.2,
    iceberg_refresh_count=4,
    iceberg_min_duration=1.0,
    iceberg_refresh_min_abs=20.0,   # XRP дешёвый, объёмы в штуках больше
    dist_atr_threshold=0.50,
    min_signal_interval_sec=12,
    stop_mode="ATR",
    stop_atr_mult=1.2,
    tp_mode="RR",
    tp_rr="1,1.5,2.3",
    tp_atr_mults="0.8,1.2,1.8",
    metadata={"asset_class": "crypto", "base_currency": "XRP", "quote_currency": "USD"},
)

XRPUSD_SPECS = SymbolSpecs(
    symbol="XRPUSD",
    contract_size=1.0,
    pip_value=0.0001,
    lot_step=1.0,
    min_lot=1.0,
    max_lot=1_000_000_000.0,
    tick_value=0.0001,
    point_value=0.0001,
    price_decimals=4,
    volume_decimals=0,  # "штуки"
)

# XRPUSDT - отдельный config/specs (алиас с обновленным metadata)
XRPUSDT_CONFIG = replace(
    XRPUSD_CONFIG,
    symbol="XRPUSDT",
    metadata={**XRPUSD_CONFIG.metadata, "quote_currency": "USDT"},
)
XRPUSDT_SPECS = replace(XRPUSD_SPECS, symbol="XRPUSDT")

# New crypto symbols configurations
PEPEUSDT_CONFIG = OrderFlowConfig(
    symbol="1000PEPEUSDT",
    delta_window_ticks=240,
    delta_z_threshold=3.1,
    weak_progress_atr=0.22,
    obi_threshold=0.42,
    obi_min_duration=2.0,
    iceberg_refresh_count=4,
    iceberg_min_duration=1.2,
    iceberg_refresh_min_abs=1.0,
    dist_atr_threshold=0.55,
    min_signal_interval_sec=45,
    read_count=150,
    read_block_ms=1000,
    stop_mode="ATR",
    stop_atr_mult=1.2,
    tp_mode="RR",
    tp_rr="1,1.5,2.5",
    tp_atr_mults="0.6,1.0,1.5",
    metadata={"asset_class": "crypto", "base_currency": "PEPE", "quote_currency": "USDT"},
)

PEPEUSDT_SPECS = SymbolSpecs(
    symbol="1000PEPEUSDT",
    contract_size=1.0,              # TODO: exchangeInfo
    min_lot=1.0,                    # TODO: exchangeInfo
    price_decimals=8,               # TODO: exchangeInfo (PEPE has many decimals)
    volume_decimals=0,              # TODO: exchangeInfo
)

DOGEUSDT_CONFIG = OrderFlowConfig(
    symbol="DOGEUSDT",
    delta_window_ticks=150,
    delta_z_threshold=2.8,
    delta_abs_min=0.5,
    delta_abs_min_confirm=0.5,
    weak_progress_atr=0.16,
    obi_threshold=0.34,
    obi_min_duration=1.6,
    iceberg_refresh_count=3,
    iceberg_min_duration=1.0,
    iceberg_refresh_min_abs=1.0,
    dist_atr_threshold=0.42,
    min_signal_interval_sec=20,
    read_count=120,
    read_block_ms=1000,
    stop_mode="ATR",
    stop_atr_mult=1.2,
    tp_mode="RR",
    tp_rr="1,1.5,2.5",
    tp_atr_mults="0.6,1.0,1.5",
    metadata={"asset_class": "crypto", "base_currency": "DOGE", "quote_currency": "USDT"},
)

DOGEUSDT_SPECS = SymbolSpecs(
    symbol="DOGEUSDT",
    contract_size=1.0,              # TODO: exchangeInfo
    min_lot=1.0,                    # TODO: exchangeInfo
    price_decimals=5,               # TODO: exchangeInfo
    volume_decimals=0,              # TODO: exchangeInfo
)

SHIBUSDT_CONFIG = OrderFlowConfig(
    symbol="1000SHIBUSDT",
    delta_window_ticks=240,
    delta_z_threshold=3.1,
    weak_progress_atr=0.22,
    obi_threshold=0.42,
    obi_min_duration=2.0,
    iceberg_refresh_count=4,
    iceberg_min_duration=1.2,
    iceberg_refresh_min_abs=1.0,
    dist_atr_threshold=0.55,
    min_signal_interval_sec=45,
    read_count=150,
    read_block_ms=1000,
    stop_mode="ATR",
    stop_atr_mult=1.2,
    tp_mode="RR",
    tp_rr="1,1.5,2.5",
    tp_atr_mults="0.6,1.0,1.5",
    metadata={"asset_class": "crypto", "base_currency": "SHIB", "quote_currency": "USDT"},
)

SHIBUSDT_SPECS = SymbolSpecs(
    symbol="1000SHIBUSDT",
    contract_size=1.0,              # TODO: exchangeInfo
    min_lot=1.0,                    # TODO: exchangeInfo
    price_decimals=8,               # TODO: exchangeInfo
    volume_decimals=0,              # TODO: exchangeInfo
)

FLOKIUSDT_CONFIG = OrderFlowConfig(
    symbol="1000FLOKIUSDT",
    delta_window_ticks=240,
    delta_z_threshold=3.1,
    delta_abs_min=0.5,
    delta_abs_min_confirm=0.5,
    weak_progress_atr=0.24,
    obi_threshold=0.43,
    obi_min_duration=2.1,
    iceberg_refresh_count=4,
    iceberg_min_duration=1.2,
    iceberg_refresh_min_abs=1.0,
    dist_atr_threshold=0.58,
    min_signal_interval_sec=50,
    read_count=150,
    read_block_ms=1000,
    stop_mode="ATR",
    stop_atr_mult=1.2,
    tp_mode="RR",
    tp_rr="1,1.5,2.5",
    tp_atr_mults="0.6,1.0,1.5",
    metadata={"asset_class": "crypto", "base_currency": "FLOKI", "quote_currency": "USDT"},
)

FLOKIUSDT_SPECS = SymbolSpecs(
    symbol="1000FLOKIUSDT",
    contract_size=1.0,              # TODO: exchangeInfo
    min_lot=1.0,                    # TODO: exchangeInfo
    price_decimals=6,               # TODO: exchangeInfo
    volume_decimals=0,              # TODO: exchangeInfo
)

BONKUSDT_CONFIG = OrderFlowConfig(
    symbol="1000BONKUSDT",
    delta_window_ticks=240,
    delta_z_threshold=3.2,
    delta_abs_min=0.5,
    delta_abs_min_confirm=0.5,
    weak_progress_atr=0.25,
    obi_threshold=0.44,
    obi_min_duration=2.1,
    iceberg_refresh_count=4,
    iceberg_min_duration=1.2,
    iceberg_refresh_min_abs=1.0,
    dist_atr_threshold=0.60,
    min_signal_interval_sec=55,
    read_count=150,
    read_block_ms=1000,
    stop_mode="ATR",
    stop_atr_mult=1.2,
    tp_mode="RR",
    tp_rr="1,1.5,2.5",
    tp_atr_mults="0.6,1.0,1.5",
    metadata={"asset_class": "crypto", "base_currency": "BONK", "quote_currency": "USDT"},
)

BONKUSDT_SPECS = SymbolSpecs(
    symbol="1000BONKUSDT",
    contract_size=1.0,              # TODO: exchangeInfo
    min_lot=1.0,                    # TODO: exchangeInfo
    price_decimals=8,               # TODO: exchangeInfo
    volume_decimals=0,              # TODO: exchangeInfo
)

WIFUSDT_CONFIG = OrderFlowConfig(
    symbol="WIFUSDT",
    delta_window_ticks=220,
    delta_z_threshold=3.0,
    delta_abs_min=0.5,
    delta_abs_min_confirm=0.5,
    weak_progress_atr=0.21,
    obi_threshold=0.40,
    obi_min_duration=1.9,
    iceberg_refresh_count=4,
    iceberg_min_duration=1.1,
    iceberg_refresh_min_abs=1.0,
    dist_atr_threshold=0.55,
    min_signal_interval_sec=40,
    read_count=140,
    read_block_ms=1000,
    stop_mode="ATR",
    stop_atr_mult=1.2,
    tp_mode="RR",
    tp_rr="1,1.5,2.5",
    tp_atr_mults="0.6,1.0,1.5",
    metadata={"asset_class": "crypto", "base_currency": "WIF", "quote_currency": "USDT"},
)

WIFUSDT_SPECS = SymbolSpecs(
    symbol="WIFUSDT",
    contract_size=1.0,              # TODO: exchangeInfo
    min_lot=1.0,                    # TODO: exchangeInfo
    price_decimals=6,               # TODO: exchangeInfo
    volume_decimals=0,              # TODO: exchangeInfo
)

SUIUSDT_CONFIG = OrderFlowConfig(
    symbol="SUIUSDT",
    delta_window_ticks=140,
    delta_z_threshold=2.8,
    delta_abs_min=0.5,
    delta_abs_min_confirm=0.5,
    weak_progress_atr=0.16,
    obi_threshold=0.34,
    obi_min_duration=1.6,
    iceberg_refresh_count=3,
    iceberg_min_duration=1.0,
    iceberg_refresh_min_abs=1.0,
    dist_atr_threshold=0.42,
    min_signal_interval_sec=20,
    read_count=120,
    read_block_ms=1000,
    stop_mode="ATR",
    stop_atr_mult=1.2,
    tp_mode="RR",
    tp_rr="1,1.5,2.5",
    tp_atr_mults="0.6,1.0,1.5",
    metadata={"asset_class": "crypto", "base_currency": "SUI", "quote_currency": "USDT"},
)

SUIUSDT_SPECS = SymbolSpecs(
    symbol="SUIUSDT",
    contract_size=1.0,              # TODO: exchangeInfo
    min_lot=1.0,                    # TODO: exchangeInfo
    price_decimals=4,               # TODO: exchangeInfo
    volume_decimals=0,              # TODO: exchangeInfo
)

APTUSDT_CONFIG = OrderFlowConfig(
    symbol="APTUSDT",
    delta_window_ticks=140,
    delta_z_threshold=2.8,
    delta_abs_min=0.5,
    delta_abs_min_confirm=0.5,
    weak_progress_atr=0.16,
    obi_threshold=0.34,
    obi_min_duration=1.6,
    iceberg_refresh_count=3,
    iceberg_min_duration=1.0,
    iceberg_refresh_min_abs=1.0,
    dist_atr_threshold=0.42,
    min_signal_interval_sec=20,
    read_count=120,
    read_block_ms=1000,
    stop_mode="ATR",
    stop_atr_mult=1.2,
    tp_mode="RR",
    tp_rr="1,1.5,2.5",
    tp_atr_mults="0.6,1.0,1.5",
    metadata={"asset_class": "crypto", "base_currency": "APT", "quote_currency": "USDT"},
)

APTUSDT_SPECS = SymbolSpecs(
    symbol="APTUSDT",
    contract_size=1.0,              # TODO: exchangeInfo
    min_lot=1.0,                    # TODO: exchangeInfo
    price_decimals=4,               # TODO: exchangeInfo
    volume_decimals=0,              # TODO: exchangeInfo
)

ARBUSDT_CONFIG = OrderFlowConfig(
    symbol="ARBUSDT",
    delta_window_ticks=140,
    delta_z_threshold=2.8,
    delta_abs_min=0.5,
    delta_abs_min_confirm=0.5,
    weak_progress_atr=0.16,
    obi_threshold=0.34,
    obi_min_duration=1.6,
    iceberg_refresh_count=3,
    iceberg_min_duration=1.0,
    iceberg_refresh_min_abs=1.0,
    dist_atr_threshold=0.42,
    min_signal_interval_sec=20,
    read_count=120,
    read_block_ms=1000,
    stop_mode="ATR",
    stop_atr_mult=1.2,
    tp_mode="RR",
    tp_rr="1,1.5,2.5",
    tp_atr_mults="0.6,1.0,1.5",
    metadata={"asset_class": "crypto", "base_currency": "ARB", "quote_currency": "USDT"},
)

ARBUSDT_SPECS = SymbolSpecs(
    symbol="ARBUSDT",
    contract_size=1.0,              # TODO: exchangeInfo
    min_lot=1.0,                    # TODO: exchangeInfo
    price_decimals=5,               # TODO: exchangeInfo
    volume_decimals=0,              # TODO: exchangeInfo
)

# Forex: Silver (XAGUSD)
XAGUSD_CONFIG = OrderFlowConfig(
    symbol="XAGUSD",
    delta_window_ticks=120,
    delta_z_threshold=3.0,
    weak_progress_atr=0.10,
    obi_threshold=0.5,
    obi_min_duration=2.0,
    iceberg_refresh_count=2,
    iceberg_min_duration=1.5,
    iceberg_refresh_min_abs=1.0,
    dist_atr_threshold=0.5,
    min_signal_interval_sec=60,
    metadata={
        "asset_class": "forex",
        "base_currency": "XAG",
        "quote_currency": "USD",
    }
)

XAGUSD_SPECS = SymbolSpecs(
    symbol="XAGUSD",
    contract_size=5000.0,           # 5000 унций серебра
    pip_value=0.001,                # Меньший pip для серебра
    lot_step=0.01,
    min_lot=0.01,
    max_lot=100.0,
    tick_value=0.001,
    point_value=0.001,
    price_decimals=3,               # Серебро: $25.123
    volume_decimals=2
)


# ═════════════════════════════════════════════════════════════════════
# REGISTRY - Централизованный реестр конфигураций
# ═════════════════════════════════════════════════════════════════════




ONDOUSDT_CONFIG = OrderFlowConfig(
    symbol="ONDOUSDT",
    delta_window_ticks=150,
    delta_z_threshold=2.95,
    delta_abs_min=0.5,
    delta_abs_min_confirm=0.5,
    weak_progress_atr=0.17,
    obi_threshold=0.35,
    obi_min_duration=1.6,
    iceberg_refresh_count=3,
    iceberg_min_duration=1.0,
    iceberg_refresh_min_abs=1.0,
    dist_atr_threshold=0.45,
    min_signal_interval_sec=24,
    read_count=125,
    read_block_ms=1000,
    stop_mode="ATR",
    stop_atr_mult=1.2,
    tp_mode="RR",
    tp_rr="1,1.5,2.5",
    tp_atr_mults="0.6,1.0,1.5",
    metadata={"asset_class": "crypto", "base_currency": "ONDO", "quote_currency": "USDT"},
)


ONDOUSDT_SPECS = SymbolSpecs(
    symbol="ONDOUSDT",
    contract_size=1.0,              # TODO: exchangeInfo
    min_lot=1.0,                    # TODO: exchangeInfo
    price_decimals=4,               # TODO: exchangeInfo
    volume_decimals=0,              # TODO: exchangeInfo
)


OPUSDT_CONFIG = OrderFlowConfig(
    symbol="OPUSDT",
    delta_window_ticks=145,
    delta_z_threshold=2.9,
    delta_abs_min=0.5,
    delta_abs_min_confirm=0.5,
    weak_progress_atr=0.17,
    obi_threshold=0.34,
    obi_min_duration=1.6,
    iceberg_refresh_count=3,
    iceberg_min_duration=1.0,
    iceberg_refresh_min_abs=1.0,
    dist_atr_threshold=0.44,
    min_signal_interval_sec=22,
    read_count=120,
    read_block_ms=1000,
    stop_mode="ATR",
    stop_atr_mult=1.2,
    tp_mode="RR",
    tp_rr="1,1.5,2.5",
    tp_atr_mults="0.6,1.0,1.5",
    metadata={"asset_class": "crypto", "base_currency": "OP", "quote_currency": "USDT"},
)


OPUSDT_SPECS = SymbolSpecs(
    symbol="OPUSDT",
    contract_size=1.0,              # TODO: exchangeInfo
    min_lot=1.0,                    # TODO: exchangeInfo
    price_decimals=4,               # TODO: exchangeInfo
    volume_decimals=0,              # TODO: exchangeInfo
)


HBARUSDT_CONFIG = OrderFlowConfig(
    symbol="HBARUSDT",
    delta_window_ticks=140,
    delta_z_threshold=2.85,
    delta_abs_min=0.5,
    delta_abs_min_confirm=0.5,
    weak_progress_atr=0.16,
    obi_threshold=0.34,
    obi_min_duration=1.6,
    iceberg_refresh_count=3,
    iceberg_min_duration=1.0,
    iceberg_refresh_min_abs=1.0,
    dist_atr_threshold=0.43,
    min_signal_interval_sec=24,
    read_count=120,
    read_block_ms=1000,
    stop_mode="ATR",
    stop_atr_mult=1.2,
    tp_mode="RR",
    tp_rr="1,1.5,2.5",
    tp_atr_mults="0.6,1.0,1.5",
    metadata={"asset_class": "crypto", "base_currency": "HBAR", "quote_currency": "USDT"},
)


HBARUSDT_SPECS = SymbolSpecs(
    symbol="HBARUSDT",
    contract_size=1.0,              # TODO: exchangeInfo
    min_lot=1.0,                    # TODO: exchangeInfo
    price_decimals=5,               # TODO: exchangeInfo
    volume_decimals=0,              # TODO: exchangeInfo
)


SEIUSDT_CONFIG = OrderFlowConfig(
    symbol="SEIUSDT",
    delta_window_ticks=160,
    delta_z_threshold=3.0,
    delta_abs_min=0.5,
    delta_abs_min_confirm=0.5,
    weak_progress_atr=0.18,
    obi_threshold=0.36,
    obi_min_duration=1.7,
    iceberg_refresh_count=3,
    iceberg_min_duration=1.0,
    iceberg_refresh_min_abs=1.0,
    dist_atr_threshold=0.48,
    min_signal_interval_sec=26,
    read_count=130,
    read_block_ms=1000,
    stop_mode="ATR",
    stop_atr_mult=1.2,
    tp_mode="RR",
    tp_rr="1,1.5,2.5",
    tp_atr_mults="0.6,1.0,1.5",
    metadata={"asset_class": "crypto", "base_currency": "SEI", "quote_currency": "USDT"},
)


SEIUSDT_SPECS = SymbolSpecs(
    symbol="SEIUSDT",
    contract_size=1.0,              # TODO: exchangeInfo
    min_lot=1.0,                    # TODO: exchangeInfo
    price_decimals=5,               # TODO: exchangeInfo
    volume_decimals=0,              # TODO: exchangeInfo
)


RENDERUSDT_CONFIG = OrderFlowConfig(
    symbol="RENDERUSDT",
    delta_window_ticks=160,
    delta_z_threshold=3.0,
    delta_abs_min=0.5,
    delta_abs_min_confirm=0.5,
    weak_progress_atr=0.18,
    obi_threshold=0.36,
    obi_min_duration=1.7,
    iceberg_refresh_count=3,
    iceberg_min_duration=1.0,
    iceberg_refresh_min_abs=1.0,
    dist_atr_threshold=0.48,
    min_signal_interval_sec=26,
    read_count=130,
    read_block_ms=1000,
    stop_mode="ATR",
    stop_atr_mult=1.2,
    tp_mode="RR",
    tp_rr="1,1.5,2.5",
    tp_atr_mults="0.6,1.0,1.5",
    metadata={"asset_class": "crypto", "base_currency": "RENDER", "quote_currency": "USDT"},
)


RENDERUSDT_SPECS = SymbolSpecs(
    symbol="RENDERUSDT",
    contract_size=1.0,              # TODO: exchangeInfo
    min_lot=1.0,                    # TODO: exchangeInfo
    price_decimals=3,               # TODO: exchangeInfo
    volume_decimals=0,              # TODO: exchangeInfo
)


AAVEUSDT_CONFIG = OrderFlowConfig(
    symbol="AAVEUSDT",
    delta_window_ticks=150,
    delta_z_threshold=2.95,
    delta_abs_min=0.5,
    delta_abs_min_confirm=0.5,
    weak_progress_atr=0.17,
    obi_threshold=0.35,
    obi_min_duration=1.6,
    iceberg_refresh_count=3,
    iceberg_min_duration=1.0,
    iceberg_refresh_min_abs=1.0,
    dist_atr_threshold=0.46,
    min_signal_interval_sec=24,
    read_count=125,
    read_block_ms=1000,
    stop_mode="ATR",
    stop_atr_mult=1.2,
    tp_mode="RR",
    tp_rr="1,1.5,2.6",
    tp_atr_mults="0.6,1.0,1.6",
    metadata={"asset_class": "crypto", "base_currency": "AAVE", "quote_currency": "USDT"},
)


AAVEUSDT_SPECS = SymbolSpecs(
    symbol="AAVEUSDT",
    contract_size=1.0,              # TODO: exchangeInfo
    min_lot=1.0,                    # TODO: exchangeInfo
    price_decimals=2,               # TODO: exchangeInfo
    volume_decimals=0,              # TODO: exchangeInfo
)


TRBUSDT_CONFIG = OrderFlowConfig(
    symbol="TRBUSDT",
    delta_window_ticks=190,
    delta_z_threshold=3.15,
    delta_abs_min=0.5,
    delta_abs_min_confirm=0.5,
    weak_progress_atr=0.21,
    obi_threshold=0.39,
    obi_min_duration=1.9,
    iceberg_refresh_count=4,
    iceberg_min_duration=1.1,
    iceberg_refresh_min_abs=1.0,
    dist_atr_threshold=0.56,
    min_signal_interval_sec=35,
    read_count=140,
    read_block_ms=1000,
    stop_mode="ATR",
    stop_atr_mult=1.2,
    tp_mode="RR",
    tp_rr="1,1.5,2.7",
    tp_atr_mults="0.7,1.1,1.8",
    metadata={"asset_class": "crypto", "base_currency": "TRB", "quote_currency": "USDT"},
)


TRBUSDT_SPECS = SymbolSpecs(
    symbol="TRBUSDT",
    contract_size=1.0,              # TODO: exchangeInfo
    min_lot=1.0,                    # TODO: exchangeInfo
    price_decimals=2,               # TODO: exchangeInfo
    volume_decimals=0,              # TODO: exchangeInfo
)


NEARUSDT_CONFIG = OrderFlowConfig(
    symbol="NEARUSDT",
    delta_window_ticks=140,
    delta_z_threshold=2.9,
    delta_abs_min=0.5,
    delta_abs_min_confirm=0.5,
    weak_progress_atr=0.17,
    obi_threshold=0.34,
    obi_min_duration=1.6,
    iceberg_refresh_count=3,
    iceberg_min_duration=1.0,
    iceberg_refresh_min_abs=1.0,
    dist_atr_threshold=0.44,
    min_signal_interval_sec=22,
    read_count=120,
    read_block_ms=1000,
    stop_mode="ATR",
    stop_atr_mult=1.2,
    tp_mode="RR",
    tp_rr="1,1.5,2.5",
    tp_atr_mults="0.6,1.0,1.5",
    metadata={"asset_class": "crypto", "base_currency": "NEAR", "quote_currency": "USDT"},
)


NEARUSDT_SPECS = SymbolSpecs(
    symbol="NEARUSDT",
    contract_size=1.0,              # TODO: exchangeInfo
    min_lot=1.0,                    # TODO: exchangeInfo
    price_decimals=4,               # TODO: exchangeInfo
    volume_decimals=0,              # TODO: exchangeInfo
)






INSTRUMENT_CONFIGS: dict[str, OrderFlowConfig] = {
    "XAUUSD": XAUUSD_CONFIG,
    "XAUUSDT": XAUUSDT_CONFIG,
    "XAGUSD": XAGUSD_CONFIG,
    "BTCUSD": BTCUSD_CONFIG,
    "BTCUSDT": BTCUSDT_CONFIG,  # Отдельный config для USDT
    "ETHUSD": ETHUSD_CONFIG,
    "ETHUSDT": ETHUSDT_CONFIG,  # Отдельный config для USDT
    "BNBUSD": BNBUSD_CONFIG,
    "BNBUSDT": BNBUSDT_CONFIG,  # Отдельный config для USDT
    "SOLUSD": SOLUSD_CONFIG,
    "SOLUSDT": SOLUSDT_CONFIG,  # Отдельный config для USDT
    "XRPUSD": XRPUSD_CONFIG,
    "XRPUSDT": XRPUSDT_CONFIG,  # Отдельный config для USDT
    # New crypto symbols
    "1000PEPEUSDT": PEPEUSDT_CONFIG,
    "DOGEUSDT": DOGEUSDT_CONFIG,
    "1000SHIBUSDT": SHIBUSDT_CONFIG,
    "1000FLOKIUSDT": FLOKIUSDT_CONFIG,
    "1000BONKUSDT": BONKUSDT_CONFIG,
    "WIFUSDT": WIFUSDT_CONFIG,
    "SUIUSDT": SUIUSDT_CONFIG,
    "APTUSDT": APTUSDT_CONFIG,
    "ARBUSDT": ARBUSDT_CONFIG,
}

INSTRUMENT_SPECS: dict[str, SymbolSpecs] = {
    "XAUUSD": XAUUSD_SPECS,
    "XAUUSDT": XAUUSDT_SPECS,
    "XAGUSD": XAGUSD_SPECS,
    "BTCUSD": BTCUSD_SPECS,
    "BTCUSDT": BTCUSDT_SPECS,  # Отдельный specs для USDT
    "ETHUSD": ETHUSD_SPECS,
    "ETHUSDT": ETHUSDT_SPECS,  # Отдельный specs для USDT
    "BNBUSD": BNBUSD_SPECS,
    "BNBUSDT": BNBUSDT_SPECS,  # Отдельный specs для USDT
    "SOLUSD": SOLUSD_SPECS,
    "SOLUSDT": SOLUSDT_SPECS,  # Отдельный specs для USDT
    "XRPUSD": XRPUSD_SPECS,
    "XRPUSDT": XRPUSDT_SPECS,  # Отдельный specs для USDT
    # New crypto symbols
    "1000PEPEUSDT": PEPEUSDT_SPECS,
    "DOGEUSDT": DOGEUSDT_SPECS,
    "1000SHIBUSDT": SHIBUSDT_SPECS,
    "1000FLOKIUSDT": FLOKIUSDT_SPECS,
    "1000BONKUSDT": BONKUSDT_SPECS,
    "WIFUSDT": WIFUSDT_SPECS,
    "SUIUSDT": SUIUSDT_SPECS,
    "APTUSDT": APTUSDT_SPECS,
    "ARBUSDT": ARBUSDT_SPECS,
}


def get_config(symbol: str, use_env: bool = True) -> OrderFlowConfig:
    """
    Получает конфигурацию для указанного символа.

    Args:
        symbol: Символ инструмента
        use_env: Использовать environment variables (True) или пресет (False)
                 Если True, накладывает env поверх пресета (если пресет существует)

    Returns:
        Конфигурация OrderFlowConfig

    Raises:
        ValueError: Если символ не найден в реестре и use_env=False
    """
    sym = normalize_symbol(symbol)

    if sym in INSTRUMENT_CONFIGS:
        preset = INSTRUMENT_CONFIGS[sym]
        # Apply env overlay on top of preset (docstring обещал именно так)
        if use_env:
            return OrderFlowConfig.from_env(sym, base=preset)
        return preset

    if use_env:
        # Fallback to Env/Defaults (Priority 3)
        return OrderFlowConfig.from_env(sym)

    raise ValueError(f"Unknown symbol: {sym}. Add to INSTRUMENT_CONFIGS or use use_env=True")




ONDOUSDT_CONFIG = OrderFlowConfig(
    symbol="ONDOUSDT",
    delta_window_ticks=150,
    delta_z_threshold=2.95,
    delta_abs_min=0.5,
    delta_abs_min_confirm=0.5,
    weak_progress_atr=0.17,
    obi_threshold=0.35,
    obi_min_duration=1.6,
    iceberg_refresh_count=3,
    iceberg_min_duration=1.0,
    iceberg_refresh_min_abs=1.0,
    dist_atr_threshold=0.45,
    min_signal_interval_sec=24,
    read_count=125,
    read_block_ms=1000,
    stop_mode="ATR",
    stop_atr_mult=1.2,
    tp_mode="RR",
    tp_rr="1,1.5,2.5",
    tp_atr_mults="0.6,1.0,1.5",
    metadata={"asset_class": "crypto", "base_currency": "ONDO", "quote_currency": "USDT"},
)


ONDOUSDT_SPECS = SymbolSpecs(
    symbol="ONDOUSDT",
    contract_size=1.0,              # TODO: exchangeInfo
    min_lot=1.0,                    # TODO: exchangeInfo
    price_decimals=4,               # TODO: exchangeInfo
    volume_decimals=0,              # TODO: exchangeInfo
)


OPUSDT_CONFIG = OrderFlowConfig(
    symbol="OPUSDT",
    delta_window_ticks=145,
    delta_z_threshold=2.9,
    delta_abs_min=0.5,
    delta_abs_min_confirm=0.5,
    weak_progress_atr=0.17,
    obi_threshold=0.34,
    obi_min_duration=1.6,
    iceberg_refresh_count=3,
    iceberg_min_duration=1.0,
    iceberg_refresh_min_abs=1.0,
    dist_atr_threshold=0.44,
    min_signal_interval_sec=22,
    read_count=120,
    read_block_ms=1000,
    stop_mode="ATR",
    stop_atr_mult=1.2,
    tp_mode="RR",
    tp_rr="1,1.5,2.5",
    tp_atr_mults="0.6,1.0,1.5",
    metadata={"asset_class": "crypto", "base_currency": "OP", "quote_currency": "USDT"},
)


OPUSDT_SPECS = SymbolSpecs(
    symbol="OPUSDT",
    contract_size=1.0,              # TODO: exchangeInfo
    min_lot=1.0,                    # TODO: exchangeInfo
    price_decimals=4,               # TODO: exchangeInfo
    volume_decimals=0,              # TODO: exchangeInfo
)


HBARUSDT_CONFIG = OrderFlowConfig(
    symbol="HBARUSDT",
    delta_window_ticks=140,
    delta_z_threshold=2.85,
    delta_abs_min=0.5,
    delta_abs_min_confirm=0.5,
    weak_progress_atr=0.16,
    obi_threshold=0.34,
    obi_min_duration=1.6,
    iceberg_refresh_count=3,
    iceberg_min_duration=1.0,
    iceberg_refresh_min_abs=1.0,
    dist_atr_threshold=0.43,
    min_signal_interval_sec=24,
    read_count=120,
    read_block_ms=1000,
    stop_mode="ATR",
    stop_atr_mult=1.2,
    tp_mode="RR",
    tp_rr="1,1.5,2.5",
    tp_atr_mults="0.6,1.0,1.5",
    metadata={"asset_class": "crypto", "base_currency": "HBAR", "quote_currency": "USDT"},
)


HBARUSDT_SPECS = SymbolSpecs(
    symbol="HBARUSDT",
    contract_size=1.0,              # TODO: exchangeInfo
    min_lot=1.0,                    # TODO: exchangeInfo
    price_decimals=5,               # TODO: exchangeInfo
    volume_decimals=0,              # TODO: exchangeInfo
)


SEIUSDT_CONFIG = OrderFlowConfig(
    symbol="SEIUSDT",
    delta_window_ticks=160,
    delta_z_threshold=3.0,
    delta_abs_min=0.5,
    delta_abs_min_confirm=0.5,
    weak_progress_atr=0.18,
    obi_threshold=0.36,
    obi_min_duration=1.7,
    iceberg_refresh_count=3,
    iceberg_min_duration=1.0,
    iceberg_refresh_min_abs=1.0,
    dist_atr_threshold=0.48,
    min_signal_interval_sec=26,
    read_count=130,
    read_block_ms=1000,
    stop_mode="ATR",
    stop_atr_mult=1.2,
    tp_mode="RR",
    tp_rr="1,1.5,2.5",
    tp_atr_mults="0.6,1.0,1.5",
    metadata={"asset_class": "crypto", "base_currency": "SEI", "quote_currency": "USDT"},
)


SEIUSDT_SPECS = SymbolSpecs(
    symbol="SEIUSDT",
    contract_size=1.0,              # TODO: exchangeInfo
    min_lot=1.0,                    # TODO: exchangeInfo
    price_decimals=5,               # TODO: exchangeInfo
    volume_decimals=0,              # TODO: exchangeInfo
)


RENDERUSDT_CONFIG = OrderFlowConfig(
    symbol="RENDERUSDT",
    delta_window_ticks=160,
    delta_z_threshold=3.0,
    delta_abs_min=0.5,
    delta_abs_min_confirm=0.5,
    weak_progress_atr=0.18,
    obi_threshold=0.36,
    obi_min_duration=1.7,
    iceberg_refresh_count=3,
    iceberg_min_duration=1.0,
    iceberg_refresh_min_abs=1.0,
    dist_atr_threshold=0.48,
    min_signal_interval_sec=26,
    read_count=130,
    read_block_ms=1000,
    stop_mode="ATR",
    stop_atr_mult=1.2,
    tp_mode="RR",
    tp_rr="1,1.5,2.5",
    tp_atr_mults="0.6,1.0,1.5",
    metadata={"asset_class": "crypto", "base_currency": "RENDER", "quote_currency": "USDT"},
)


RENDERUSDT_SPECS = SymbolSpecs(
    symbol="RENDERUSDT",
    contract_size=1.0,              # TODO: exchangeInfo
    min_lot=1.0,                    # TODO: exchangeInfo
    price_decimals=3,               # TODO: exchangeInfo
    volume_decimals=0,              # TODO: exchangeInfo
)


AAVEUSDT_CONFIG = OrderFlowConfig(
    symbol="AAVEUSDT",
    delta_window_ticks=150,
    delta_z_threshold=2.95,
    delta_abs_min=0.5,
    delta_abs_min_confirm=0.5,
    weak_progress_atr=0.17,
    obi_threshold=0.35,
    obi_min_duration=1.6,
    iceberg_refresh_count=3,
    iceberg_min_duration=1.0,
    iceberg_refresh_min_abs=1.0,
    dist_atr_threshold=0.46,
    min_signal_interval_sec=24,
    read_count=125,
    read_block_ms=1000,
    stop_mode="ATR",
    stop_atr_mult=1.2,
    tp_mode="RR",
    tp_rr="1,1.5,2.6",
    tp_atr_mults="0.6,1.0,1.6",
    metadata={"asset_class": "crypto", "base_currency": "AAVE", "quote_currency": "USDT"},
)


AAVEUSDT_SPECS = SymbolSpecs(
    symbol="AAVEUSDT",
    contract_size=1.0,              # TODO: exchangeInfo
    min_lot=1.0,                    # TODO: exchangeInfo
    price_decimals=2,               # TODO: exchangeInfo
    volume_decimals=0,              # TODO: exchangeInfo
)


TRBUSDT_CONFIG = OrderFlowConfig(
    symbol="TRBUSDT",
    delta_window_ticks=190,
    delta_z_threshold=3.15,
    delta_abs_min=0.5,
    delta_abs_min_confirm=0.5,
    weak_progress_atr=0.21,
    obi_threshold=0.39,
    obi_min_duration=1.9,
    iceberg_refresh_count=4,
    iceberg_min_duration=1.1,
    iceberg_refresh_min_abs=1.0,
    dist_atr_threshold=0.56,
    min_signal_interval_sec=35,
    read_count=140,
    read_block_ms=1000,
    stop_mode="ATR",
    stop_atr_mult=1.2,
    tp_mode="RR",
    tp_rr="1,1.5,2.7",
    tp_atr_mults="0.7,1.1,1.8",
    metadata={"asset_class": "crypto", "base_currency": "TRB", "quote_currency": "USDT"},
)


TRBUSDT_SPECS = SymbolSpecs(
    symbol="TRBUSDT",
    contract_size=1.0,              # TODO: exchangeInfo
    min_lot=1.0,                    # TODO: exchangeInfo
    price_decimals=2,               # TODO: exchangeInfo
    volume_decimals=0,              # TODO: exchangeInfo
)


def get_specs(symbol: str) -> SymbolSpecs:
    """
    Получает спецификацию для указанного символа.
    
    Args:
        symbol: Символ инструмента
        
    Returns:
        Спецификация SymbolSpecs
        
    Raises:
        ValueError: Если символ не найден в реестре
    """
    sym = normalize_symbol(symbol)

    if sym in INSTRUMENT_SPECS:
        return INSTRUMENT_SPECS[sym]

    raise ValueError(f"Unknown symbol: {sym}. Add to INSTRUMENT_SPECS")


def register_instrument(symbol: str, config: OrderFlowConfig, specs: SymbolSpecs) -> None:
    """
    Регистрирует новый инструмент в реестре.
    
    Args:
        symbol: Символ инструмента
        config: Конфигурация OrderFlowConfig
        specs: Спецификация SymbolSpecs
    """
    sym = normalize_symbol(symbol)
    INSTRUMENT_CONFIGS[sym] = replace(config, symbol=sym)
    INSTRUMENT_SPECS[sym] = replace(specs, symbol=sym)
