"""
ServiceConfig — единое место для всех ENV-переменных CryptoOrderflowService.

Вместо ~60 os.getenv-вызовов разбросанных по __init__, все параметры
собраны здесь. Это позволяет:
  - тестировать без monkeypatch (ServiceConfig(pools=RedisPoolCfg(main_max=4)))
  - иметь документацию по ENV прямо рядом с default-значениями
  - единожды менять дефолты без grep по 2700-строчному файлу
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Optional


def _env_bool(name: str, default: str = "0") -> bool:
    return os.getenv(name, default).strip().lower() in ("1", "true", "yes", "on")


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except (ValueError, TypeError):
        return default


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except (ValueError, TypeError):
        return default


# ── Redis pool sizing ─────────────────────────────────────────────────────────

@dataclass
class RedisPoolCfg:
    """Размеры пулов и таймауты для всех Redis-клиентов.

    Каждый символ держит 2 блокирующих соединения (ticks + books).
    Формула: N_symbols × 2 + overhead(50) = нужный ticks_max.
    При 8 инстансах: 8 × (128 main + 128 ticks) = 2048 итого.
    """
    main_max: int = field(default_factory=lambda: _env_int("REDIS_MAIN_MAX_CONNECTIONS", 128))
    ticks_max: int = field(default_factory=lambda: _env_int("REDIS_TICKS_MAX_CONNECTIONS", 128))
    notify_max: int = field(default_factory=lambda: _env_int("REDIS_NOTIFY_MAX_CONNECTIONS", 32))
    config_max: int = field(default_factory=lambda: _env_int("REDIS_CONFIG_MAX_CONNECTIONS", 10))
    health_max: int = field(default_factory=lambda: _env_int("REDIS_HEALTH_MAX_CONNECTIONS", 10))
    health_contract_max: int = field(default_factory=lambda: _env_int("REDIS_HEALTH_CONTRACT_MAX_CONNECTIONS", 5))
    ml_gate_max: int = field(default_factory=lambda: _env_int("REDIS_ML_GATE_MAX_CONNECTIONS", 5))

    # 1.0s на hot-path: event loop заблокируется максимум на 1s при Redis stall.
    # Было 30.0 — любой Redis stall замораживал весь pipeline на 30s.
    sock_to: float = field(default_factory=lambda: _env_float("REDIS_SOCKET_TIMEOUT", 1.0))
    conn_to: float = field(default_factory=lambda: _env_float("REDIS_SOCKET_CONNECT_TIMEOUT", 5.0))
    hc_interval: int = field(default_factory=lambda: _env_int("REDIS_HEALTHCHECK_INTERVAL", 30))

    # Отдельный таймаут для config-клиента (не hot-path, может быть выше)
    config_sock_to: Optional[float] = None  # None → берёт ORDERFLOW_CONFIG_SOCKET_TIMEOUT или 5.0

    def resolved_config_sock_to(self) -> float:
        if self.config_sock_to is not None:
            return self.config_sock_to
        raw = os.getenv("ORDERFLOW_CONFIG_SOCKET_TIMEOUT")
        # Config client не hot-path: 5.0s по умолчанию (выше sock_to).
        return float(raw) if raw else 5.0


# ── Calibration ───────────────────────────────────────────────────────────────

@dataclass
class CalibCfg:
    window: int = field(default_factory=lambda: _env_int("CONF_CALIB_WINDOW", 2000))
    min_history: int = field(default_factory=lambda: _env_int("CONF_CALIB_MIN_HISTORY", 30))
    fallback_k: float = field(default_factory=lambda: _env_float("CONF_CALIB_FALLBACK_K", 8.0))


# ── Risk / DQ / Quarantine gates ──────────────────────────────────────────────

@dataclass
class RiskGateCfg:
    dq_hard_veto_enable: bool = field(
        default_factory=lambda: _env_bool("TRADE_DQ_HARD_VETO_ENABLE", "1"))

    risk_engine_v2_enable: bool = field(
        default_factory=lambda: _env_bool(
            "TRADE_RISK_ENGINE_V2_ENABLE",
            os.getenv("RISK_ENGINE_V2_ENABLE", "1"),
        ))

    risk_hard_veto: bool = field(
        default_factory=lambda: _env_bool("PORTFOLIO_RISK_HARD_VETO", "1"))

    risk_sql_audit_enable: bool = field(
        default_factory=lambda: _env_bool("TRADE_RISK_SQL_AUDIT_ENABLE", "1"))

    quarantine_enable: bool = field(
        default_factory=lambda: _env_bool("EXEC_QUARANTINE_DENYLIST_ENABLE", "1"))

    quarantine_sids_key: str = field(
        default_factory=lambda: (
            os.getenv("ORDERS_QUARANTINE_SIDS_KEY", "orders:quarantine:state:sids").strip()
            or "orders:quarantine:state:sids"
        ))

    quarantine_cache_ms: int = field(
        default_factory=lambda: _env_int("QUARANTINE_DENYLIST_CACHE_MS", 1000))


# ── Tick consumer ─────────────────────────────────────────────────────────────

@dataclass
class TickCfg:
    backoff_base: float = field(default_factory=lambda: _env_float("REDIS_BACKOFF_BASE", 0.5))
    backoff_cap: float = field(default_factory=lambda: _env_float("REDIS_BACKOFF_CAP", 15.0))
    backoff_jitter: bool = field(
        default_factory=lambda: bool(_env_int("REDIS_BACKOFF_JITTER_ENABLED", 1)))
    idle_sleep_sec: float = field(default_factory=lambda: _env_float("REDIS_IDLE_SLEEP_SEC", 0.05))
    sample_rate: float = field(default_factory=lambda: _env_float("TICK_SAMPLE_RATE", 1.0))
    # 50ms: снижает H2 floor с 1000ms (instrument_config default) до 50ms.
    # При пустом стриме Python делает XREADGROUP не чаще 1 раза в 50ms — приемлемо.
    read_block_ms: str = field(default_factory=lambda: os.getenv("CRYPTO_OF_READ_BLOCK_MS", "50"))
    lag_tracker_max_ms: int = field(
        default_factory=lambda: _env_int("WORKER_LAG_TRACKER_MAX_MS", 60_000))

    # Unknown-side policy: "ignore_delta" matches normalize_unknown_side_policy(None) behaviour
    unknown_side_policy: str = field(
        default_factory=lambda: os.getenv("CRYPTO_OF_UNKNOWN_SIDE_POLICY", "ignore_delta"))
    unknown_side_quarantine_stream: str = field(
        default_factory=lambda: os.getenv(
            "TICK_SIDE_QUARANTINE_STREAM", "stream:tick_side:quarantine"))
    unknown_side_quarantine_sample: float = field(
        default_factory=lambda: _env_float("TICK_SIDE_QUARANTINE_SAMPLE", 0.01))
    unknown_side_quarantine_maxlen: int = field(
        default_factory=lambda: _env_int("TICK_SIDE_QUARANTINE_MAXLEN", 20_000))

    ack_batch: int = field(default_factory=lambda: _env_int("CRYPTO_OF_ACK_BATCH", 200))
    max_lag_ms: int = field(default_factory=lambda: _env_int("CRYPTO_OF_MAX_LAG_MS", 500))
    drop_on_lag: bool = field(default_factory=lambda: _env_bool("CRYPTO_OF_DROP_ON_LAG", "0"))
    max_ts_skew_ms: int = field(
        default_factory=lambda: _env_int("CRYPTO_OF_MAX_TS_SKEW_MS", 6 * 3_600_000))


# ── Signal streams / publish ──────────────────────────────────────────────────

@dataclass
class StreamCfg:
    """Имена Redis-ключей и stream-темплейтов для публикации сигналов."""

    # Импортируем RS здесь чтобы не создавать circular-import на уровне модуля
    @classmethod
    def from_env(cls) -> "StreamCfg":
        from core.redis_keys import RedisStreams as RS
        return cls(
            notify_stream=os.getenv("NOTIFY_STREAM", RS.NOTIFY_TELEGRAM),
            raw_signal_stream=os.getenv("CRYPTO_RAW_STREAM", RS.CRYPTO_RAW),
            orders_queue=(
                os.getenv("ORDERS_QUEUE_MT5")
                or os.getenv("ORDERS_QUEUE")
                or RS.ORDERS_QUEUE_MT5
            ),
            signal_stream_template=os.getenv(
                "CRYPTO_ORDERFLOW_SIGNAL_STREAM", "signals:cryptoorderflow:{symbol}"),
            burst_audit_stream=os.getenv("BURST_AUDIT_STREAM", "stream:of:burst_audit"),
            quarantine_stream=os.getenv("SIGNAL_QUARANTINE_STREAM", "stream:of:quarantine"),
        )

    notify_stream: str = ""
    raw_signal_stream: str = ""
    orders_queue: str = ""
    signal_stream_template: str = "signals:cryptoorderflow:{symbol}"
    burst_audit_stream: str = "stream:of:burst_audit"
    quarantine_stream: str = "stream:of:quarantine"


# ── ML / Engine ───────────────────────────────────────────────────────────────

@dataclass
class EngineCfg:
    of_confirm_version: int = field(
        default_factory=lambda: _env_int("OF_CONFIRM_VERSION", 2))
    force_trail_after_tp1: bool = field(
        default_factory=lambda: _env_bool("FORCE_TRAIL_AFTER_TP1", "0"))


# ── Top-level ServiceConfig ───────────────────────────────────────────────────

@dataclass
class ServiceConfig:
    """Единый источник истины для всех ENV-параметров CryptoOrderflowService.

    Использование:
        cfg = ServiceConfig.from_env()          # prod: читает os.environ
        cfg = ServiceConfig()                   # тесты: дефолты из dataclass
        cfg = ServiceConfig(pools=RedisPoolCfg(main_max=4))  # переопределить часть
    """
    pools: RedisPoolCfg = field(default_factory=RedisPoolCfg)
    calib: CalibCfg = field(default_factory=CalibCfg)
    risk: RiskGateCfg = field(default_factory=RiskGateCfg)
    tick: TickCfg = field(default_factory=TickCfg)
    engine: EngineCfg = field(default_factory=EngineCfg)

    # StreamCfg строится через from_env() из-за отложенного импорта RedisStreams
    streams: Optional[StreamCfg] = None

    # Lifecycle
    refresh_interval_sec: int = field(
        default_factory=lambda: _env_int("CRYPTO_OF_REFRESH_SEC", 30))
    bootstrap_max_conc: int = field(
        default_factory=lambda: _env_int("CRYPTO_OF_BOOTSTRAP_MAX_CONC", 10))

    @classmethod
    def from_env(cls) -> "ServiceConfig":
        cfg = cls()
        cfg.streams = StreamCfg.from_env()
        return cfg

    def resolved_streams(self) -> StreamCfg:
        if self.streams is None:
            self.streams = StreamCfg.from_env()
        return self.streams
