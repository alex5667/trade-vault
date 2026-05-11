from __future__ import annotations

# infra/redis_repo.py
import json
import logging
import os
from collections.abc import Mapping
from dataclasses import asdict
from typing import Any, Protocol

from core.redis_keys import STREAM_RETENTION
from core.redis_keys import RedisStreams as RS
from domain.models import PositionState, SignalNorm, TradeEvent
from domain.normalizers import canon_source, canon_strategy, canon_symbol, canon_tf, tf_variants
import contextlib

logger = logging.getLogger(__name__)

def _env_bool(name: str, default: bool = False) -> bool:  # type: ignore
    v = os.getenv(name)  # type: ignore
    if v is None:
        return default
    return v.strip().lower() in ("1", "true", "yes", "y", "on")

def _normalize_side(v: Any) -> str:
    """
    direction в разных местах может быть:
      - Literal "LONG"/"SHORT" (ваш domain/models.py)
      - Enum с value "long"/"short" (в signal_exec)
      - строка "long"/"short"
    В Redis хотим единообразно: "LONG" / "SHORT".
    """
    if v is None:
        return "LONG"
    s = str(v)
    # Enum может печататься как "Side.LONG" — вытаскиваем хвост
    if "." in s:
        s = s.split(".")[-1]
    sl = s.strip().lower()
    if sl in ("long", "buy"):
        return "LONG"
    if sl in ("short", "sell"):
        return "SHORT"
    # уже "LONG"/"SHORT" или что-то близкое
    su = s.strip().upper()
    return su if su in ("LONG", "SHORT") else "LONG"


def _canon_regime(v: Any) -> str:
    try:
        s = (v or "").strip().lower()
    except Exception:
        return "na"
    return s or "na"


def _extract_entry_regime_from_obj(obj: Any) -> str:
    """
    Extract entry regime from position/closed-trade object.
    Priority: entry_regime -> regime -> market_regime -> regime_label.
    """
    for k in ("entry_regime", "regime", "market_regime", "regime_label"):
        try:
            v = getattr(obj, k, None)
        except Exception:
            v = None
        if v:
            return _canon_regime(v)
    return "na"


def _mk_crypto_sid(symbol: str, ts_ms: int) -> str:
    """Create canonical SID: crypto-of:{symbol}:{ts_ms}"""
    return f"crypto-of:{symbol}:{ts_ms}"


def _normalize_crypto_sid(raw: object, *, symbol: str, ts_ms: int) -> str:
    """
    Normalize SID to canonical format: crypto-of:{symbol}:{ts_ms}
    
    Supports legacy formats:
      - crypto-of:{symbol}:{ts_ms} (already canonical)
      - {symbol}|{ts}|{dir} (legacy format)
      - {symbol}:{ts} (legacy without prefix)
      - empty -> generate from symbol+ts_ms
    """
    s = str(raw or "").strip()
    if s.startswith("crypto-of:"):
        return s
    if "|" in s:
        parts = s.split("|")
        if len(parts) >= 2:
            sym = (parts[0].strip() or symbol).strip()
            try:
                t = int(parts[1])
            except Exception:
                t = ts_ms
            if sym and t > 0:
                return _mk_crypto_sid(sym, t)
    # Accept legacy "SYMBOL:TS" without prefix (not "crypto-of:SYMBOL:TS")
    if s and (":" in s) and (not s.startswith("crypto-of:")) and ("|" not in s):
        p = s.split(":")
        if len(p) >= 2 and p[1].strip().isdigit():
            sym = (p[0].strip() or symbol).strip()
            t = int(p[1].strip())
            if sym and t > 0:
                return _mk_crypto_sid(sym, t)
    if (not s) and symbol and ts_ms > 0:
        return _mk_crypto_sid(symbol, ts_ms)
    return s


def _b01(v: Any) -> str:  # type: ignore
    """Единый формат bool в Redis: '1'/'0' (а не 'True'/'False')."""
    try:
        return "1" if bool(v) else "0"
    except Exception:
        return "0"


def _to_int(v: Any, default: int = 0) -> int:
    try:
        if v is None:
            return default
        return int(float(str(v).strip()))
    except Exception:
        return default

# NOTE:
#  - Этот репозиторий обязан быть устойчивым к тому, что redis-py может возвращать bytes,
#    если клиент создан без decode_responses=True.
#  - Поэтому все чтения/записи делаем через нормализацию типов.

logger = logging.getLogger("RedisTradeRepository")

# -----------------------------------------------------------------------------
# Compatibility: profile field name mismatch
#
# Facts in your codebase:
# - PositionState has:   trail_profile
# - TradeClosed has:     trailing_profile
# - save_open() used:    trail_profile
# - save_closed() used:  trailing_profile
#
# If a consumer expects only one key, the other path becomes "empty".
# We store BOTH keys everywhere (hash + stream) with identical values.
#
# Canon key:  trailing_profile  (matches TradeClosed dataclass)
# Alias key:  trail_profile     (matches PositionState field)
# -----------------------------------------------------------------------------
PROFILE_CANON_KEY = "trailing_profile"
PROFILE_ALIAS_KEY = "trail_profile"

# --- Feature flags (все по умолчанию OFF, чтобы не менять поведение внезапно) ---
_ENV_ENABLE_CLOSED_ZSET_INDEX = "ENABLE_CLOSED_ZSET_INDEX"
_ENV_CLOSED_ZSET_RETENTION_DAYS = "CLOSED_ZSET_RETENTION_DAYS"
_ENV_TRADES_CLOSED_STREAM_COMPACT = "TRADES_CLOSED_STREAM_COMPACT"

# Если включено, repo будет:
#  - публиковать в stream RS.TRADES_CLOSED минимальный payload (compact),
#  - предполагая, что consumer/reporter умеет hydrate детали из order:{id}.
TRADES_CLOSED_STREAM_COMPACT = os.getenv("TRADES_CLOSED_STREAM_COMPACT", "0").strip().lower() in ("1", "true", "yes", "on")

# Если включено, repo будет индексировать закрытые сделки в ZSET:
#  - score = exit_ts_ms (точное время закрытия),
#  - member = order_id.
# Это даёт быстрые/точные выборки окна по времени, без сканирования stream.
ENABLE_CLOSED_ZSET_INDEX = (
    os.getenv("ENABLE_CLOSED_ZSET_INDEX", "0").strip().lower() in ("1", "true", "yes", "on") or
    os.getenv("TRADES_CLOSED_ZSET_INDEX", "0").strip().lower() in ("1", "true", "yes", "on") or
    os.getenv("REDIS_CLOSED_ZSETS_ENABLED", "0").strip().lower() in ("1", "true", "yes", "on")
)


def _env_bool(name: str, default: bool) -> bool:
    """
    Read boolean feature flags from env.
    Accepts: 1/0, true/false, yes/no, on/off (case-insensitive).
    """
    v = os.getenv(name)
    if v is None:
        return default
    s = v.strip().lower()
    if s in ("1", "true", "yes", "y", "on"):
        return True
    if s in ("0", "false", "no", "n", "off"):
        return False
    return default


def _mirror_profile_keys(mapping: dict[str, Any]) -> None:
    """
    Ensure both profile keys exist if at least one is present.
    This prevents partial updates from breaking downstream consumers.
    """
    if not mapping:
        return
    if PROFILE_CANON_KEY in mapping and PROFILE_ALIAS_KEY not in mapping:
        mapping[PROFILE_ALIAS_KEY] = mapping[PROFILE_CANON_KEY]
    elif PROFILE_ALIAS_KEY in mapping and PROFILE_CANON_KEY not in mapping:
        mapping[PROFILE_CANON_KEY] = mapping[PROFILE_ALIAS_KEY]


def _set_profile_fields(mapping: dict[str, Any], *, profile_value: str) -> None:
    """
    Пишем оба поля:
      - trailing_profile (канон)
      - trail_profile   (алиас для обратной совместимости)
    """
    v = profile_value or ""
    mapping[PROFILE_CANON_KEY] = v
    mapping[PROFILE_ALIAS_KEY] = v


def _normalize_health_snapshot(d: Mapping[str, Any]) -> dict[str, Any]:
    """
    Нормализует health snapshot к ключам с префиксом "health_".
    Если ключ уже начинается с "health_" — не дублируем.
    """
    out: dict[str, Any] = {}
    for k, v in (d or {}).items():
        if v is None:
            continue
        ks = k
        if ks.startswith("health_"):
            out[ks] = v
        else:
            out[f"health_{ks}"] = v
    return out

TRADES_CLOSED_STREAM_NAME = os.getenv("TRADES_CLOSED_STREAM_NAME", RS.TRADES_CLOSED)
TRADES_CLOSED_STREAM_MAXLEN = int(os.getenv("TRADES_CLOSED_STREAM_MAXLEN", "50000"))

#
# Compact stream:
#   TRADES_CLOSED_STREAM_COMPACT=1
#
# Идея:
#   - trades:closed stream хранит только минимальный payload (для "шины событий")
#   - ВСЕ детали сделки лежат в order:{order_id} hash
#   - consumer'ы при необходимости "hydrated" из order:{id}
#
TRADES_CLOSED_STREAM_COMPACT_ENV = "TRADES_CLOSED_STREAM_COMPACT"

# -----------------------
# Optional ZSET time index
# -----------------------
# Если включено, то при каждом save_closed() мы поддерживаем индекс закрытий:
#   closed_z:{strategy}:{symbol}:{tf}               (без source)
#   closed_z:{strategy}:{symbol}:{tf}:{source}      (с source)
#
# score = exit_ts_ms, member = order_id.
#
# Зачем:
#   - быстро выбрать окно по времени (ZRANGEBYSCORE [from..to])
#   - не сканировать trades:closed stream по min_id
#   - упрощает будущий "compact stream" (stream хранит минимум, детали берём из order:{id}).
TRADES_CLOSED_ZSET_INDEX_ENV = "TRADES_CLOSED_ZSET_INDEX"
TRADES_CLOSED_ZSET_MAXLEN_ENV = "TRADES_CLOSED_ZSET_MAXLEN"


# -----------------------------
# Health snapshot provider (инъекция)
# -----------------------------
class HealthSnapshotProvider(Protocol):
    """
    Интерфейс источника health-метрик.
    ВАЖНО: репозиторий НЕ должен сам создавать HealthMetrics/redis-подключения на каждый close.
    Это делается выше (tick loop / monitor), либо через singleton provider.
    """
    def get_snapshot(self, symbol: str) -> dict[str, Any]:
        ...


# -----------------------------
# Helpers: normalize types
# -----------------------------
def _to_str(x: Any) -> str:
    """bytes/bytearray -> utf-8 string; everything else -> str(x)."""
    if isinstance(x, (bytes, bytearray)):
        return x.decode("utf-8", "replace")
    return str(x)


def _decode_map(m: dict[Any, Any]) -> dict[str, str]:
    """
    Redis может вернуть dict[bytes, bytes] если decode_responses=False.
    Мы приводим к dict[str, str] без артефактов вида "b'open'".
    """
    out: dict[str, str] = {}
    for k, v in (m or {}).items():
        out[_to_str(k)] = _to_str(v)
    return out


def _b01(x: Any) -> str:
    """Нормализация булевых в '0'/'1' для стабильного парсинга."""
    return "1" if bool(x) else "0"


def _json(v: Any) -> str:
    """Стабильная JSON-сериализация для dict/list (без ASCII-эскейпа кириллицы)."""
    return json.dumps(v, ensure_ascii=False, separators=(",", ":"))


def _side_to_str(side: Any) -> str:
    """
    Нормализация Side/Enum/строк.
    Цель: в Redis хранить единый формат, чтобы recovery и аналитика не зависели от __str__ Enum.
    """
    if side is None:
        return ""
    # Enum -> value/name
    val = getattr(side, "value", None)
    if isinstance(val, str):
        s = val
    else:
        s = _to_str(side)
    s2 = s.strip().lower()
    # допускаем разные варианты
    if "long" in s2:
        return "long"
    if "short" in s2:
        return "short"
    return s2


def _canon_side(v: Any) -> str:
    """
    В проекте side бывает:
      - Literal["LONG","SHORT"] (uppercase)
      - Enum (lowercase long/short)
    В Redis храним канонически "LONG"/"SHORT".
    """
    s = ("" if v is None else str(v)).strip()
    if not s:
        return "LONG"
    sl = s.lower()
    if sl in ("long", "buy"):
        return "LONG"
    if sl in ("short", "sell"):
        return "SHORT"
    su = s.upper()
    return "LONG" if su not in ("LONG", "SHORT") else su


def _closed_zset_key(strategy: str, symbol: str, tf: str, source: str | None = None) -> str:
    st = canon_strategy(strategy)
    sy = canon_symbol(symbol)
    t = canon_tf(tf)
    if source is None:
        return f"closed_z:{st}:{sy}:{t}"
    so = canon_source(source)
    return f"closed_z:{st}:{sy}:{t}:{so}"


def _stringify(d: dict[str, Any]) -> dict[str, str]:
    """
    Конвертирует Dict[str, Any] в Dict[str, str] для Redis stream.
    - None → skip
    - dict/list → json.dumps()
    - всё остальное → str()
    """
    out: dict[str, str] = {}
    for k, v in (d or {}).items():
        if v is None:
            continue
        kk = _to_str(k)
        if isinstance(v, (dict, list)):
            out[kk] = _json(v)
        else:
            out[kk] = _to_str(v)
    return out

def _health_prefix_snapshot(hs: dict[str, Any]) -> dict[str, str]:
    """
    Health snapshot в python-worker сейчас отдаётся как mapping вида:
      { "l2_stale_ratio_tick": "...", "avg_l2_age_ms": "...", ... }
    Чтобы:
      - не конфликтовать с полями сделки,
      - одинаково хранить в order hash и (опционально) в stream,
    мы префиксуем health_.
    """
    out: dict[str, str] = {}
    for k, v in (hs or {}).items():
        if v is None:
            continue
        kk = k
        if not kk.startswith("health_"):
            kk = "health_" + kk
        out[kk] = str(v)
    return out

def _compact_trade_payload(closed, *, prof: str) -> dict[str, Any]:
    """
    Минимальный payload для trades:closed stream в compact режиме.

    Задачи минимального payload:
      1) Быстро отфильтровать "свой" source/symbol без HGETALL на КАЖДУЮ запись.
      2) Иметь время закрытия (exit_ts_ms) для окна отчёта.
      3) Иметь order_id для дальнейшего hydrate.

    Важно:
      - ВСЕ остальные поля считаем "деталями" и берём из order:{id} hash.
      - health_* в stream в compact режиме НЕ кладём (они и так лежат в order hash).
    """
    return {
        "schema_version": str(getattr(closed, "schema_version", 1)),
        "order_id": str(getattr(closed, "order_id", "") or ""),
        "sid": str(getattr(closed, "sid", "") or ""),
        "strategy": str(getattr(closed, "strategy", "") or ""),
        "source": str(getattr(closed, "source", "") or ""),
        "symbol": str(getattr(closed, "symbol", "") or ""),
        "tf": canon_tf(getattr(closed, "tf", None)),
        "direction": _normalize_side(getattr(closed, "direction", None)),
        "exit_ts_ms": str(int(getattr(closed, "exit_ts_ms", 0) or 0)),
        "closed_time": str(int(getattr(closed, "exit_ts_ms", 0) or 0)),  # alias
        # несколько полей, которые часто используются без hydrate:
        "pnl_net": str(getattr(closed, "pnl_net", 0.0)),
        "pnl_if_fixed_exit": str(getattr(closed, "pnl_if_fixed_exit", 0.0)),
        "one_r_money": str(getattr(closed, "one_r_money", 0.0)),
        "close_reason": str(getattr(closed, "close_reason", "") or ""),
        "close_reason_raw": str(getattr(closed, "close_reason_raw", "") or ""),
        "close_reason_detail": str(getattr(closed, "close_reason_detail", "") or ""),
        "trailing_started": str(int(bool(getattr(closed, "trailing_started", False)))),
        "trailing_active": str(int(bool(getattr(closed, "trailing_active", False)))),
        "trailing_profile": prof,
        "trail_profile": prof,
        "is_final_close": str(int(bool(getattr(closed, "is_final_close", True)))),
        "status": "closed",
        "atr": str(getattr(closed, "atr", 0.0)),
        "sl_price": str(getattr(closed, "sl", 0.0)),
        "tp1_price": str(getattr(closed, "tp1_price", 0.0)),
        "fees_usd": str(getattr(closed, "fees_usd", 0.0) or getattr(closed, "fees", 0.0) or 0.0),
        "turnover_roundtrip": str(getattr(closed, "turnover_roundtrip", 0.0) or 0.0),
        "risk_usd": str(getattr(closed, "risk_usd", 0.0) or getattr(closed, "one_r_money", 0.0) or 0.0),
        "r_mult": str(getattr(closed, "r_mult", 0.0) or getattr(closed, "r_multiple", 0.0) or 0.0),
        "p0_slippage_bps_est": str(getattr(closed, "p0_slippage_bps_est", 0.0) or getattr(closed, "slippage_bps_est", 0.0) or 0.0),
        "meta_enforce_cov_bucket": str(getattr(closed, "meta_enforce_cov_bucket", "") or ""),
        "meta_enforce_applied": str(int(getattr(closed, "meta_enforce_applied", -1))),
    }


def _compact_closed_stream_payload(full: dict[str, Any]) -> dict[str, Any]:
    """
    Compact stream payload to reduce Redis memory/IO.
    IMPORTANT: this is behind TRADES_CLOSED_STREAM_COMPACT=1 because
    some downstream consumers may expect full asdict(TradeClosed).
    Full snapshot still goes to order:{id} hash.
    """
    keep = {
        "schema_version",
        "order_id", "sid",
        "strategy", "source", "symbol", "tf",
        "direction",
        "entry_ts_ms", "exit_ts_ms",
        "entry_price", "exit_price",
        "lot", "notional_usd",
        "pnl_net", "fees", "fees_usd", "pnl_pct",
        "tp1_hit", "tp2_hit", "tp3_hit", "tp_hits", "tp_before_sl",
        "trailing_started", "trailing_active", "trailing_moves",
        "close_reason", "close_reason_raw", "close_reason_detail",
        "baseline_exit_reason", "baseline_exit_ts_ms", "baseline_exit_price",
        "duration_ms", "duration",
        "r_multiple", "r_mult", "risk_usd",
        "entry_tag",
        "max_favorable_price", "max_favorable_ts",
        "turnover_entry", "turnover_roundtrip",
        "mfe_bps", "mae_bps", "time_to_mfe_ms",
        "spread_bps_at_entry", "slippage_bps_est", "p0_slippage_bps_est", "book_age_ms",
        "meta_enforce_cov_bucket", "meta_enforce_applied",
        PROFILE_CANON_KEY, PROFILE_ALIAS_KEY,
        # Phase 0.3: horizon scalar first-class fields for analytics consumers
        "sc_contract_ver", "sc_risk_horizon_bucket",
        "sc_hold_target_ms", "sc_alpha_half_life_ms", "sc_max_signal_age_ms",
        "sc_atr_age_ms", "sc_atr_source", "sc_atr_pct",
        "sc_vol_ratio_fast_slow", "sc_vol_ratio_z",
        # Phase 0.3: raw event scalars (non-prefixed, for event consumers)
        "risk_horizon_bucket", "hold_target_ms", "atr_tf_ms",
        "atr_age_ms", "atr_source", "atr_pct",
        "vol_ratio_fast_slow", "vol_ratio_z",
    }
    # keep health fields if present
    out: dict[str, Any] = {}
    for k, v in (full or {}).items():
        if k in keep or k.startswith("health_"):
            out[k] = v
    return out


# -----------------------------------------------------------------------------
# Atomic save_closed via Lua (EVAL is allowed in your environment)
#
# Guarantees:
#  - HSET order:{id} always happens (final snapshot)
#  - SREM orders:open always happens (idempotent cleanup)
#  - stream+legacy indexes happen once (dedupe key)
#  - optional ZSET indexes added once
#
# This eliminates partial states on retries/crashes and prevents duplicate events.
# -----------------------------------------------------------------------------
_SAVE_CLOSED_LUA = r"""
-- KEYS:
--  1 = order_key           (order:{id})
--  2 = open_set_key        (orders:open)
--  3 = closed_stream_key   (trades:closed)
--  4 = legacy_list_1       (closed:{strategy}:{symbol}:{tf})
--  - legacy_list_2       (closed:{strategy}:{symbol}:{tf}:{source})
--  6 = dedupe_key          (dedupe:trades:closed:{order_id})
--  7 = zset_1              (closed_z:{strategy}:{symbol}:{tf})
--  8 = zset_2              (closed_z:{strategy}:{symbol}:{tf}:{source})
--
-- ARGV:
--  1 = stream_maxlen
--  2 = dedupe_ttl_sec
--  3 = legacy_lists_enabled (0/1)
--  4 = zsets_enabled        (0/1)
--  5 = zset_score           (exit_ts_ms)
--  6 = hpair_count
--  7.. = hpair_count*2 values (HSET pairs)
--  next = order_id_for_srem
--  next = spair_count
--  next = spair_count*2 values (XADD payload)
--  next = order_id_for_list1
--  next = order_id_for_list2

local stream_maxlen = tonumber(ARGV[1]) or 50000
local dedupe_ttl = tonumber(ARGV[2]) or 604800
local legacy_enabled = tonumber(ARGV[3]) or 1
local zsets_enabled = tonumber(ARGV[4]) or 1
local zscore = tonumber(ARGV[5]) or 0

local hpair_count = tonumber(ARGV[6]) or 0
local idx = 7

-- 1) Update final snapshot hash (always)
for i = 1, hpair_count do
  redis.call('HSET', KEYS[1], ARGV[idx], ARGV[idx+1])
  idx = idx + 2
end

-- 2) Always cleanup open index (idempotent)
local oid_srem = ARGV[idx]
idx = idx + 1
if oid_srem and oid_srem ~= '' then
  redis.call('SREM', KEYS[2], oid_srem)
end

-- 3) Deduplicate stream + indexes
local first = redis.call('SET', KEYS[6], '1', 'NX', 'EX', dedupe_ttl)
if not first then
  return 0
end

-- 4) XADD once
local spair_count = tonumber(ARGV[idx]) or 0
idx = idx + 1
local payload = {}
for i = 1, spair_count do
  payload[#payload+1] = ARGV[idx]
  payload[#payload+1] = ARGV[idx+1]
  idx = idx + 2
end
redis.call('XADD', KEYS[3], 'MAXLEN', '~', stream_maxlen, '*', unpack(payload))

-- 5) Optional legacy lists once
local oid1 = ARGV[idx]
idx = idx + 1
local oid2 = ARGV[idx]
idx = idx + 1
if legacy_enabled == 1 then
  if oid1 and oid1 ~= '' then redis.call('RPUSH', KEYS[4], oid1) end
  if oid2 and oid2 ~= '' then redis.call('RPUSH', KEYS[5], oid2) end
end

-- 6) Optional ZSET indexes once
if zsets_enabled == 1 then
  if oid1 and oid1 ~= '' then redis.call('ZADD', KEYS[7], zscore, oid1) end
  if oid2 and oid2 ~= '' then redis.call('ZADD', KEYS[8], zscore, oid2) end
end

return 1
"""


class RedisTradeRepository:
    """
    Хранилище в Redis:
      - order:{id} hash (single source of truth)
      - orders:open set
      - trades:closed stream (для trade_back и reporter)
      - events:trades stream (все события)
      - closed:{strategy}:{symbol}:{tf} list (legacy fallback)
      - closed:{strategy}:{symbol}:{tf}:{source} list
      - signals:{sid} string(json) (аудит)
    """

    def __init__(
        self,
        redis_client,
        *,
        health_provider=None,
        metrics=None,
        close_done_ttl_sec=3600,
        close_lock_ttl_sec=60,
    ):
        """
        redis_client: sync redis.Redis (decode_responses=True в вашем проекте).

        health_provider: опциональный callable(symbol)->dict, который возвращает
        уже подготовленный health snapshot (без создания новых коннектов).

        metrics: опциональный metrics sink (fail-open). Expected interface: incr(name: str, value: int = 1, **tags)

        ВАЖНО:
          Раньше save_closed() создавал HealthMetrics(...) на КАЖДОЕ закрытие,
          что могло добавлять латентность/лишние подключения.
          Теперь repo сам НЕ создаёт HealthMetrics; максимум — принимает snapshot извне.
        """
        self.r = redis_client
        self._health_provider = health_provider
        # Optional metrics sink (fail-open). Expected interface: incr(name: str, value: int = 1, **tags)
        self.metrics = metrics

        # Для дешёвой фоновой чистки ZSET (retention) без перетяжеления hot-path:
        self._last_zset_trim_ms: int = 0

        # register_script() caches SHA in redis-py for efficiency
        self._save_closed_script = None
        # Идемпотентность close:
        #   - closed_done:{oid}  -> выставляется ПОСЛЕ успешной публикации в stream/lists
        #   - close_lock:{oid}   -> короткий lock на время финализации (чтобы не было гонок)
        self._close_done_ttl_sec = int(close_done_ttl_sec)
        self._close_lock_ttl_sec = int(close_lock_ttl_sec)

    # ---------------------------------------------------------------------
    # Sid-level guard helpers
    # ---------------------------------------------------------------------
    @staticmethod
    def _sid_done_key(sid: str) -> str:
        return f"closed_sid_done:{sid}"

    def _sid_guard_enabled(self) -> bool:
        # Rollout-friendly: можно включить/выключить без деплоя кода.
        return os.getenv("CLOSED_SID_GUARD_ENABLED", "1") == "1"

    def _sid_done_ttl_sec(self) -> int:
        days = int(os.getenv("CLOSED_SID_DONE_TTL_DAYS", "7"))
        return max(1, days) * 24 * 3600

    def _metrics_incr(self, name: str, value: int = 1, **tags) -> None:
        try:
            m = getattr(self, "metrics", None)
            if m is None:
                return
            incr = getattr(m, "incr", None)
            if callable(incr):
                incr(name, value=value, **tags)
        except Exception:
            pass

    # ---------------------------------------------------------------------
    # FAST helpers (lock-free friendly)
    #
    # Problem:
    #   TradeMonitorService wants to release the global lock before doing Redis I/O.
    #   Existing save_tp_hit/save_trailing_* methods accept PositionState and may read
    #   live mutable fields.
    #
    # Solution:
    #   Provide *fast* wrappers that take primitives and construct a minimal stub.
    #   This keeps key-derivation and schema in ONE place (repo), and lets caller
    #   do I/O outside locks safely.
    # ---------------------------------------------------------------------

    def _pos_stub(
        self,
        *,
        order_id: str,
        sid: str,
        strategy: str,
        source: str,
        symbol: str,
        tf: str,
        direction: str,
    ) -> Any:
        from types import SimpleNamespace
        return SimpleNamespace(
            id=order_id,
            sid=(sid or ""),
            strategy=(strategy or "unknown"),
            source=(source or "Unknown"),
            symbol=(symbol or "UNKNOWN"),
            tf=(tf or "tick"),
            direction=(direction or "LONG"),
        )

    def save_tp_hit_fast(
        self,
        *,
        order_id: str,
        sid: str,
        strategy: str,
        source: str,
        symbol: str,
        tf: str,
        direction: str,
        tp_level: int,
        fill_price: float,
        closed_qty: float,
        pnl_part: float,
        ts_ms: int,
    ) -> None:
        pos = self._pos_stub(
            order_id=order_id,
            sid=sid,
            strategy=strategy,
            source=source,
            symbol=symbol,
            tf=tf,
            direction=direction,
        )
        self.save_tp_hit(
            pos,
            tp_level=tp_level,
            fill_price=fill_price,
            closed_qty=closed_qty,
            pnl_part=pnl_part,
            ts_ms=ts_ms,
        )

    def save_trailing_move_fast(
        self,
        *,
        order_id: str,
        sid: str,
        strategy: str,
        source: str,
        symbol: str,
        tf: str,
        direction: str,
        previous_sl: float,
        new_sl: float,
        ts_ms: int,
    ) -> None:
        pos = self._pos_stub(
            order_id=order_id,
            sid=sid,
            strategy=strategy,
            source=source,
            symbol=symbol,
            tf=tf,
            direction=direction,
        )
        self.save_trailing_move(pos, previous_sl, new_sl, ts_ms)

    def save_trailing_sync_fast(
        self,
        *,
        order_id: str,
        sid: str,
        strategy: str,
        source: str,
        symbol: str,
        tf: str,
        direction: str,
        ts_ms: int,
    ) -> None:
        pos = self._pos_stub(
            order_id=order_id,
            sid=sid,
            strategy=strategy,
            source=source,
            symbol=symbol,
            tf=tf,
            direction=direction,
        )
        self.save_trailing_sync(pos, ts_ms)

    # --------------------
    # Signals
    # --------------------
    def persist_signal(self, signal: SignalNorm, ttl_sec: int | None = None) -> None:
        if not signal.sid:
            return
        key = f"signals:{signal.sid}"
        # Default: 48 h — enough for active monitoring/replay without OOM pressure.
        # Previously 14 days caused ~1.2 GB accumulation (9 900 keys × 130 KB) on noeviction shards.
        # Override with SIGNAL_PERSIST_TTL_SEC env-var (e.g. 259200 = 3 days for ML dataset needs).
        if ttl_sec is None:
            ttl_sec = int(os.getenv("SIGNAL_PERSIST_TTL_SEC", str(86400 * 2)))
        try:
            self.r.set(key, json.dumps(signal.payload, ensure_ascii=False))
            self.r.expire(key, ttl_sec)
        except Exception:
            pass

    def persist_volume_data(self, volume_data: list[dict[str, Any]], ttl_sec: int = 3600) -> None:
        """
        Сохраняет данные volume в Redis с TTL.

        Args:
            volume_data: Список словарей с данными volume
            ttl_sec: Время жизни в секундах (по умолчанию 1 час)
        """
        try:
            key = "volume:top_symbols"
            self.r.set(key, json.dumps(volume_data, ensure_ascii=False))
            self.r.expire(key, ttl_sec)
        except Exception:
            pass

    # --------------------
    # Open position
    # --------------------
    def save_open(self, pos) -> None:
        """
        Сохраняет позицию как Redis Hash + добавляет id в глобальный индекс orders:open.

        ВАЖНО:
          - используем pipeline(transaction=True), чтобы HSET + SADD были атомарны относительно crash.
          - пишем канон-поле trail_profile и алиас trailing_profile (совместимость).
          - пишем и entry_ts_ms, и entry_time (совместимость).
          - direction нормализуем до 'long'/'short'.
        """
        key = f"order:{pos.id}"

        tp_levels = list(getattr(pos, "tp_levels", []) or [])
        trail_profile = getattr(pos, "trail_profile", "") or ""
        # ВАЖНО: TF в open тоже лучше писать в каноне (и/или сохранять оба варианта в ключах закрытий).
        # В hash мы храним одно значение: каноническое.
        tf_canon = canon_tf(getattr(pos, "tf", None))

        # Persist regime-at-entry to Redis so that:
        #   - later loaders reconstruct pos.entry_regime correctly
        #   - close pipeline can attach entry_regime to TradeClosed deterministically
        entry_regime = _extract_entry_regime_from_obj(pos)

        mapping: dict[str, Any] = {
            "schema_version": "1",
            "id": pos.id,
            "sid": pos.sid,
            "strategy": pos.strategy,
            "source": pos.source,
            "symbol": pos.symbol,
            "tf": tf_canon,
            "direction": _side_to_str(getattr(pos, "direction", "")),

            # Conditional trailing (auditable in Redis)
            # 1 -> allowed to start trailing at TP1, 0 -> vetoed by publisher/policy.
            "trail_after_tp1": _to_str(1 if bool(getattr(pos, "trail_after_tp1", True)) else 0),
            "trail_after_tp1_reason": _to_str(getattr(pos, "trail_after_tp1_reason", "") or ""),
            "trailing_skip_reason": _to_str(getattr(pos, "trailing_skip_reason", "") or ""),

            # regime metadata (EV / analytics)
            "entry_regime": entry_regime if entry_regime != "na" else "",
            "regime": entry_regime if entry_regime != "na" else "",  # alias


            # timestamps (ms)
            "entry_ts_ms": _to_str(getattr(pos, "entry_ts_ms", getattr(pos, "entry_time", 0))),
            "entry_time": _to_str(getattr(pos, "entry_ts_ms", getattr(pos, "entry_time", 0))),  # alias

            "entry_price": _to_str(getattr(pos, "entry_price", 0.0)),
            "lot": _to_str(getattr(pos, "lot", 0.0)),
            "remaining_qty": _to_str(getattr(pos, "remaining_qty", 0.0)),
            "sl": _to_str(getattr(pos, "sl", 0.0)),

            # Signal & Shadow Analytics
            "is_virtual": _b01(getattr(pos, "is_virtual", False)),
            "v_gate_status": str(getattr(pos, "v_gate_status", "na")),
            "v_gate_reason": str(getattr(pos, "v_gate_reason", "")),

            # legacy tp1/tp2/tp3
            "tp1": _to_str(tp_levels[0] if len(tp_levels) > 0 else 0),
            "tp2": _to_str(tp_levels[1] if len(tp_levels) > 1 else 0),
            "tp3": _to_str(tp_levels[2] if len(tp_levels) > 2 else 0),

            # канон: tp_levels JSON
            "tp_levels": _json(tp_levels),

            "status": "open",

            # tracking
            "tp_hits": _to_str(getattr(pos, "tp_hits", 0)),
            "tp1_hit": _b01(getattr(pos, "tp1_hit", False)),
            "tp2_hit": _b01(getattr(pos, "tp2_hit", False)),
            "tp3_hit": _b01(getattr(pos, "tp3_hit", False)),

            "trailing_started": _b01(getattr(pos, "trailing_started", False)),
            "trailing_active": _b01(getattr(pos, "trailing_active", False)),
            "trailing_moves": _to_str(getattr(pos, "trailing_moves_count", getattr(pos, "trailing_moves", 0))),
            "trailing_distance": _to_str(getattr(pos, "trailing_distance", 0.0)),
            "trailing_point": _to_str(getattr(pos, "trailing_point", 0.0)),

            # P41 compliance (native meta)
            "meta_enforce_cov_bucket": str(getattr(pos, "meta_enforce_cov_bucket", "") or ""),
            "meta_enforce_applied": _to_str(int(getattr(pos, "meta_enforce_applied", -1))),

            # MFE/MAE
            "max_favorable_price": _to_str(getattr(pos, "max_favorable_price", 0.0)),
            "max_favorable_ts": _to_str(getattr(pos, "max_favorable_ts", 0)),
            "mfe_pnl": _to_str(getattr(pos, "mfe_pnl", 0.0)),
            "mae_pnl": _to_str(getattr(pos, "mae_pnl", 0.0)),
            "one_r_money": _to_str(getattr(pos, "one_r_money", 0.0)),

            # audit
            "entry_tag": _to_str(getattr(pos, "entry_tag", "")),

            # канон trail_profile + алиас trailing_profile (ВАЖНО для совместимости аналитики/репортера)
            "trail_profile": _to_str(trail_profile),
            "trailing_profile": _to_str(trail_profile),

            "trailing_min_lock_r": _to_str(getattr(pos, "trailing_min_lock_r", 0.0)),
            "min_lock_price": _to_str(getattr(pos, "min_lock_price", 0.0)),

            # baseline (хранится, т.к. signal_payload не сохраняется -- UPD: теперь сохраняется!)
            "signal_payload": _json(getattr(pos, "signal_payload", {}) or {}),
            "baseline_mode": _to_str(getattr(pos, "baseline_mode", "tp_sl")),
            "baseline_horizon_ms": _to_str(getattr(pos, "baseline_horizon_ms", 0)),
            "baseline_sl": _to_str(getattr(pos, "baseline_sl", 0.0)),
            "baseline_tp1": _to_str(getattr(pos, "baseline_tp1", 0.0)),
            "baseline_tp2": _to_str(getattr(pos, "baseline_tp2", 0.0)),
            "baseline_tp3": _to_str(getattr(pos, "baseline_tp3", 0.0)),
            "atr": _to_str(getattr(pos, "atr", 0.0)),

            # Conditional trailing flags (debuggable in Redis order:{id}).
            # Stored as "0/1" strings for consistent reading in recovery.
            "trail_after_tp1": _to_str(1 if bool(getattr(pos, "trail_after_tp1", True)) else 0),
            "trail_after_tp1_reason": _to_str(getattr(pos, "trail_after_tp1_reason", "") or ""),
        }

        # Phase 0.3: write horizon scalar fields directly to Redis hash
        # so recovery works even when signal_payload JSON is absent or truncated.
        try:
            from services.horizon_contract import extract_position_horizon_scalars
            _ph03 = extract_position_horizon_scalars(pos)
            for _k, _v in _ph03.items():
                mapping[_k] = _to_str(_v)
        except Exception:
            pass

        # Атомарность HSET + SADD
        pipe = self.r.pipeline(transaction=True)
        pipe.hset(key, mapping=_stringify(mapping))
        pipe.sadd("orders:open", pos.id)
        pipe.execute()

    def _get_save_closed_script(self):
        """
        Lazily register Lua script. Works with real redis-py clients and can be
        easily emulated in tests via FakeRedis.register_script().
        """
        if self._save_closed_script is None:
            self._save_closed_script = self.r.register_script(_SAVE_CLOSED_LUA)
        return self._save_closed_script

    def _closed_zkey(self, closed, *, with_source: bool) -> str:
        """
        ZSET key для индексирования закрытий:
          closed_z:{strategy}:{symbol}:{tf}[:{source}]
        """
        st = canon_strategy(getattr(closed, "strategy", "unknown"))
        sy = canon_symbol(getattr(closed, "symbol", "UNKNOWN"))
        tf = canon_tf(getattr(closed, "tf", "tick"))
        if not with_source:
            return f"closed_z:{st}:{sy}:{tf}"
        so = canon_source(getattr(closed, "source", "Unknown"))
        return f"closed_z:{st}:{sy}:{tf}:{so}"

    def _index_closed_trade(self, closed) -> None:
        """
        Best-effort индексирование закрытия в ZSET.
        Никаких исключений наружу: индексация не должна ломать close-path.
        """
        if not ENABLE_CLOSED_ZSET_INDEX:
            return
        try:
            oid = str(getattr(closed, "order_id", "") or "")
            score = float(getattr(closed, "exit_ts_ms", 0) or 0)
            if not oid or score <= 0:
                return
            k1 = self._closed_zkey(closed, with_source=False)
            k2 = self._closed_zkey(closed, with_source=True)
            # redis-py: zadd(name, mapping={member: score})
            self.r.zadd(k1, {oid: score})
            self.r.zadd(k2, {oid: score})
        except Exception as e:
            logger.debug(f"ZSET index failed for closed trade: {e}")

    def _apply_health_snapshot_to_hash(self, order_key: str, health_snapshot: dict[str, Any]) -> None:
        """
        Сохраняем health-* поля в order:{id} hash.
        Это важно при compact-stream: consumer hydrate'ит детали из hash.
        """
        if not health_snapshot:
            return
        try:
            self.r.hset(order_key, mapping=_stringify(health_snapshot))
        except Exception as e:
            logger.debug(f"Could not store health snapshot in {order_key}: {e}")

    def update_fields(self, order_id: str, mapping: dict[str, Any]) -> None:
        """
        Обновление произвольных полей Hash.
        ВАЖНО:
          - dict/list -> JSON
          - bool -> 0/1 (стабильный парсинг)
          - bytes -> декодим
        """
        key = f"order:{order_id}"
        m2: dict[str, str] = {}
        for k, v in (mapping or {}).items():
            if v is None:
                continue
            kk = _to_str(k)
            if isinstance(v, (dict, list)):
                m2[kk] = _json(v)
            elif isinstance(v, bool):
                m2[kk] = _b01(v)
            else:
                m2[kk] = _to_str(v)
        if m2:
            self.r.hset(key, mapping=m2)

    def save_tp_hit(self, pos: PositionState, tp_level: int, fill_price: float, closed_qty: float, pnl_part: float, ts_ms: int) -> None:
        """Сохранение TP hit с нормализацией булевых через _b01."""
        self.update_fields(pos.id, {
            f"tp{tp_level}_hit": _b01(True),
            "tp_hits": pos.tp_hits,
            "remaining_qty": pos.remaining_qty,
            "pnl_gross_running": pos.realized_pnl_gross,
            "max_favorable_price": pos.max_favorable_price,
            "max_favorable_ts": pos.max_favorable_ts,
            "last_tp_level": tp_level,
            "last_tp_fill_price": fill_price,
            "last_tp_closed_qty": closed_qty,
            "last_tp_pnl_gross": pnl_part,
            "last_update_ts": ts_ms,
        })

    def save_trailing_move(self, pos: PositionState, prev_sl: float, new_sl: float, ts_ms: int) -> None:
        """Сохранение trailing move с нормализацией булевых через _b01."""
        self.update_fields(pos.id, {
            "sl": new_sl,
            "trailing_active": _b01(True),
            "trailing_started": _b01(pos.trailing_started),
            "trailing_moves": pos.trailing_moves_count,
            "max_favorable_price": pos.max_favorable_price,
            "max_favorable_ts": pos.max_favorable_ts,
            "last_update_ts": ts_ms,
        })

    def save_trailing_sync(self, pos: PositionState, ts_ms: int) -> None:
        """Сохранение trailing sync с нормализацией булевых через _b01."""
        self.update_fields(pos.id, {
            "sl": pos.sl,
            "trailing_active": _b01(pos.trailing_active),
            "trailing_started": _b01(pos.trailing_started),
            "trailing_distance": pos.trailing_distance,
            "trailing_point": pos.trailing_point,
            "tp_levels": pos.tp_levels,
            "last_update_ts": ts_ms,
        })

    def save_closed(self, closed, *, health_snapshot=None) -> None:
        oid = closed.order_id
        key = f"order:{oid}"

        # Идемпотентность close:
        #   - closed_done:{oid}      -> выставляется ПОСЛЕ успешной публикации в stream/lists
        #   - close_lock:{oid}       -> короткий lock на время финализации (чтобы не было гонок)
        #   - closed_sid_done:{sid}  -> защита от повторных external-close после рестартов/cleanup (ещё выше)
        done_key = f"closed_done:{oid}"
        lock_key = f"close_lock:{oid}"
        sid = getattr(closed, "sid", None) or getattr(closed, "signal_id", None)
        sid_done_key = self._sid_done_key(str(sid)) if sid else None

        # NEW: sid-level guard (optional)
        if sid_done_key and self._sid_guard_enabled():
            try:
                if self.r.get(sid_done_key):
                    logger.debug("Trade sid=%s already closed (sid-guard)", sid)
                    self._metrics_incr("repo.save_closed.sid_guard_hit", sid=str(sid))
                    return
            except Exception:
                # fail-open: не ломаем close из-за ошибок Redis
                pass

        # Проверяем, не закрыта ли уже эта сделка
        if self.r.get(done_key):
            logger.debug(f"Trade {oid} already closed (idempotent)")
            if sid_done_key:
                self._metrics_incr("repo.save_closed.oid_guard_hit", sid=str(sid))
            return

        # Блокируем повторные вызовы на время финализации
        if not self.r.set(lock_key, "1", ex=self._close_lock_ttl_sec, nx=True):
            logger.debug(f"Trade {oid} close already in progress")
            if sid_done_key:
                self._metrics_incr("repo.save_closed.lock_hit", sid=str(sid))
            return

        # Feature toggles
        legacy_lists_enabled = _env_bool("REDIS_LEGACY_CLOSED_LISTS_ENABLED", True)
        zsets_enabled = ENABLE_CLOSED_ZSET_INDEX
        compact_stream = _env_bool(TRADES_CLOSED_STREAM_COMPACT_ENV, False)
        dedupe_ttl = int(os.getenv("TRADES_CLOSED_DEDUPE_TTL_SEC", "604800"))  # 7 days

        #
        # 1) Обновляем order:{id} hash ПОЛНЫМ снапшотом сделки
        #    Именно этот hash теперь является "источником истины" для деталей.
        #
        #    trail_profile vs trailing_profile:
        #      - канон в TradeClosed: trailing_profile
        #      - легаси в PositionState: trail_profile
        #    В order hash пишем оба ключа одинаковым значением.
        #
        prof = str(getattr(closed, "trailing_profile", "") or getattr(closed, "trail_profile", "") or "").strip()

        # Persist entry-regime on close as well (best-effort).
        # This ensures StatsAggregator (EV EMA) can segment by regime even if
        # position object wasn't available to that consumer.
        entry_regime = _extract_entry_regime_from_obj(closed)

        mapping = {
            "status": "closed",
            "closed_time": str(closed.exit_ts_ms),
            "exit_price": f"{closed.exit_price}",
            "entry_price": f"{closed.entry_price}",
            "lot": f"{closed.lot}",
            "notional_usd": f"{closed.notional_usd}",
            "pnl": f"{closed.pnl_net}",  # legacy
            "pnl_net": f"{closed.pnl_net}",
            "pnl_gross": f"{closed.pnl_gross}",
            "fees": f"{closed.fees}",
            "pnl_pct": f"{closed.pnl_pct}",
            "pnl_if_fixed_exit": f"{closed.pnl_if_fixed_exit}",
            "tp_hits": str(closed.tp_hits),
            "tp1_hit": str(closed.tp1_hit),
            "tp2_hit": str(closed.tp2_hit),
            "tp3_hit": str(closed.tp3_hit),
            "tp_before_sl": str(closed.tp_before_sl),
            "close_reason": closed.close_reason_raw or closed.close_reason,
            "close_reason_norm": closed.close_reason,
            "close_reason_detail": closed.close_reason_detail,
            "baseline_exit_reason": closed.baseline_exit_reason,
            "baseline_exit_ts_ms": str(closed.baseline_exit_ts_ms),
            "baseline_exit_price": f"{closed.baseline_exit_price}",
            "entry_tag": closed.entry_tag,
            "trailing_profile": prof,
            "trail_profile": prof,
            "trailing_min_lock_r": f"{closed.trailing_min_lock_r}",
            "trailing_active": str(closed.trailing_active),
            "trailing_started": str(closed.trailing_started),
            "trailing_moves": str(closed.trailing_moves),
            "duration_ms": str(closed.duration_ms),
            "mfe_pnl": f"{closed.mfe_pnl}",
            "mae_pnl": f"{closed.mae_pnl}",
            # -----------------------------------------------------------------
            # NEW: TP1 timestamp and excursion snapshots (for empirical MFE/MAE@TP1).
            #
            # These fields are optional and may be absent on older positions.
            # We only serialize them if present to preserve backward compatibility.
            # -----------------------------------------------------------------
            "tp1_hit_ts_ms": f"{getattr(closed, 'tp1_hit_ts_ms', 0) or 0}",
            "mfe_pnl_at_tp1": f"{getattr(closed, 'mfe_pnl_at_tp1', 0.0) or 0.0}",
            "mae_pnl_before_tp1": f"{getattr(closed, 'mae_pnl_before_tp1', 0.0) or 0.0}",
            "mfe_price_at_tp1": f"{getattr(closed, 'mfe_price_at_tp1', 0.0) or 0.0}",
            "mae_price_before_tp1": f"{getattr(closed, 'mae_price_before_tp1', 0.0) or 0.0}",
            "mfe_ts_at_tp1": f"{getattr(closed, 'mfe_ts_at_tp1', 0) or 0}",
            "mae_ts_before_tp1": f"{getattr(closed, 'mae_ts_before_tp1', 0) or 0}",
            # Entry regime is the correct segmentation key for calibration.
            "entry_regime": str(getattr(closed, "entry_regime", "") or ""),

            # P41 compliance (native meta)
            "meta_enforce_cov_bucket": str(getattr(closed, "meta_enforce_cov_bucket", "") or ""),
            "meta_enforce_applied": _to_str(int(getattr(closed, "meta_enforce_applied", -1))),

            "giveback": f"{closed.giveback}",
            "missed_profit": f"{closed.missed_profit}",
            "one_r_money": f"{closed.one_r_money}",
            "r_multiple": f"{closed.r_multiple}",
            "max_favorable_price": f"{closed.max_favorable_price}",
            "max_favorable_ts": str(closed.max_favorable_ts),
            "schema_version": str(closed.schema_version),
            "atr": f"{getattr(closed, 'atr', 0.0)}",
            "sl_price": f"{getattr(closed, 'sl', 0.0)}",
            "tp1_price": f"{getattr(closed, 'tp1_price', 0.0)}",

            # Signal & Shadow Analytics
            "is_virtual": _b01(getattr(closed, "is_virtual", False)),
            "v_gate_status": str(getattr(closed, "v_gate_status", "na")),
            "v_gate_reason": str(getattr(closed, "v_gate_reason", "")),
            "tp_levels": json.dumps(getattr(closed, 'tp_levels', [])),
            "is_final_close": "1",
            "remaining_qty": "0",
            # dims (важно хранить в order hash, чтобы hydrate всегда имел источник/символ/TF)
            "strategy": canon_strategy(getattr(closed, "strategy", None)),
            "source": canon_source(getattr(closed, "source", None)),
            "symbol": canon_symbol(getattr(closed, "symbol", None)),
            "tf": canon_tf(getattr(closed, "tf", None)),
            "direction": _normalize_side(getattr(closed, "direction", None)),
            "signal_payload": json.dumps(getattr(closed, "signal_payload", {}) or {}, ensure_ascii=False, separators=(",", ":")),

            # regime metadata (EV / analytics)
            "entry_regime": entry_regime if entry_regime != "na" else "",
            "regime": entry_regime if entry_regime != "na" else "",  # alias

            "entry_ts_ms": str(getattr(closed, "entry_ts_ms", 0) or 0),  # canonical field
            "entry_time": str(getattr(closed, "entry_ts_ms", 0) or 0),   # legacy alias
            "exit_ts_ms": str(getattr(closed, "exit_ts_ms", 0) or 0),
            "atr": f"{getattr(closed, 'atr', 0.0)}",
            "sl_price": f"{getattr(closed, 'sl', 0.0)}",
            "tp_levels": _json(getattr(closed, "tp_levels", [])),
            "tp1_price": f"{getattr(closed, 'tp1_price', 0.0)}",
        }

        # -----------------------------------------------------------------
        # NEW: conditional trailing audit fields (optional).
        # Safe to add: extra hash fields are backward-compatible.
        # -----------------------------------------------------------------
        try:
            if getattr(closed, "trail_after_tp1", None) is not None:
                mapping["trail_after_tp1"] = "1" if bool(closed.trail_after_tp1) else "0"
            if getattr(closed, "trail_after_tp1_reason", ""):
                mapping["trail_after_tp1_reason"] = str(closed.trail_after_tp1_reason)[:256]
            if getattr(closed, "trailing_skipped_after_tp1", None) is not None:
                mapping["trailing_skipped_after_tp1"] = "1" if bool(closed.trailing_skipped_after_tp1) else "0"
            if getattr(closed, "trailing_skipped_reason", ""):
                mapping["trailing_skipped_reason"] = str(closed.trailing_skipped_reason)[:256]
            if getattr(closed, "trailing_armed_ts_ms", 0):
                mapping["trailing_armed_ts_ms"] = str(int(closed.trailing_armed_ts_ms or 0))
            if getattr(closed, "trailing_start_reason", ""):
                mapping["trailing_start_reason"] = str(closed.trailing_start_reason)[:256]
        except Exception:
            pass

        # Phase 0.3: persist horizon scalars into closed order hash.
        # Allows cold-start recovery to read sc_* fields without signal_payload.
        try:
            from services.horizon_contract import extract_position_horizon_scalars
            _ph03 = extract_position_horizon_scalars(closed)
            for _k, _v in _ph03.items():
                mapping[_k] = _to_str(_v)
        except Exception:
            pass

        # -----------------------------------------------------------------
        # FIX: Save TP touched flags (calculated from MFE)
        # -----------------------------------------------------------------
        try:
            if getattr(closed, "tp1_touched", None) is not None:
                mapping["tp1_touched"] = "1" if bool(closed.tp1_touched) else "0"
            if getattr(closed, "tp2_touched", None) is not None:
                mapping["tp2_touched"] = "1" if bool(closed.tp2_touched) else "0"
            if getattr(closed, "tp3_touched", None) is not None:
                mapping["tp3_touched"] = "1" if bool(closed.tp3_touched) else "0"
        except Exception:
            pass

        # -----------------------------------------------------------------------------
        # NEW: time-bucket snapshots written by domain/handlers.py::finalize_trade
        # into TradeClosed.{mfe_pnl_t, mae_pnl_t} as JSON.
        #
        # This is consumed by services/stats_aggregator.py::_write_timebucket_buffers().
        # Fail-open: if not present, nothing changes.
        # -----------------------------------------------------------------------------
        try:
            v = getattr(closed, "mfe_pnl_t", None)
            if v is not None:
                if isinstance(v, dict):
                    mapping["mfe_pnl_t"] = json.dumps(v, ensure_ascii=False, separators=(",", ":"))
                else:
                    mapping["mfe_pnl_t"] = str(v)
        except Exception:
            pass
        try:
            v = getattr(closed, "mae_pnl_t", None)
            if v is not None:
                if isinstance(v, dict):
                    mapping["mae_pnl_t"] = json.dumps(v, ensure_ascii=False, separators=(",", ":"))
                else:
                    mapping["mae_pnl_t"] = str(v)
        except Exception:
            pass

        # health snapshot: без дополнительных коннектов/чтения Redis.
        # Если caller положил closed._health_snapshot, сохраняем его в order hash.
        hs = getattr(closed, "_health_snapshot", None)
        if isinstance(hs, dict) and hs:
            mapping.update(_health_prefix_snapshot(hs))

        self.r.hset(key, mapping=mapping)

        #
        # 2) trades:closed stream
        #
        # В compact режиме пишем минимальный payload (см. _compact_trade_payload()).
        # В non-compact режиме — полный asdict(closed) (как было).
        #
        compact = _env_bool(TRADES_CLOSED_STREAM_COMPACT_ENV, default=False)
        if compact:
            d = _compact_trade_payload(closed, prof=prof)
        else:
            d = asdict(closed)
            d["duration"] = closed.duration_ms  # alias
            if prof:
                d.setdefault("trailing_profile", prof)
                d.setdefault("trail_profile", prof)

        # Normalize sid to canonical format: crypto-of:{symbol}:{ts_ms}
        # This is critical for join: metrics:ml_confirm ↔ trades:closed
        raw_sid = getattr(closed, "sid", None) or getattr(closed, "signal_id", None) or ""
        symbol_str = canon_symbol(getattr(closed, "symbol", ""))
        exit_ts_ms = int(getattr(closed, "exit_ts_ms", 0) or 0)
        normalized_sid = _normalize_crypto_sid(raw_sid, symbol=symbol_str, ts_ms=exit_ts_ms)
        if normalized_sid:
            d["sid"] = normalized_sid

        # Добавляем health snapshot в данные стрима
        if hs:
            try:
                d.update(hs)
            except Exception as e:
                logger.debug(f"Could not merge health snapshot for trade {oid}: {e}")

        stream_data = _stringify(d)
        # алиасы совместимости
        d["duration"] = getattr(closed, "duration_ms", 0)
        d["trail_profile"] = _to_str(prof)
        d["trailing_profile"] = _to_str(prof)

        # FIX #9: health metrics приходят сверху (уже собранный снапшот)
        # Ключи оставляем как в текущем downstream: health_*
        hs = health_snapshot
        if not hs:
            # Check if attached to closed object (preferred way from TradeMonitorService)
            hs = getattr(closed, "_health_snapshot", None)

        # Обратная совместимость: если передали provider, используем его (deprecated)
        elif self._health_provider is not None:
            try:
                snap = self._health_provider.get_snapshot(_to_str(getattr(closed, "symbol", "")))
                if snap:
                    d.update(snap)
            except Exception as e:
                logger.debug(f"Health provider failed for trade {oid}: {e}")

        stream_data = _stringify(d)

        logger.debug(
            f"💾 save_closed -> {TRADES_CLOSED_STREAM_NAME}: "
            f"order_id={oid}, source={getattr(closed, 'source', '')}, strategy={getattr(closed, 'strategy', '')}, "
            f"symbol={getattr(closed, 'symbol', '')}, exit_ts_ms={getattr(closed, 'exit_ts_ms', 0)}"
        )

        # Основные записи публикуем pipeline'ом (меньше roundtrip + меньше шанс частичного состояния)
        pipe = self.r.pipeline(transaction=False)
        pipe.xadd(
            TRADES_CLOSED_STREAM_NAME,
            stream_data,
            maxlen=TRADES_CLOSED_STREAM_MAXLEN,
            approximate=True,
        )

        strategy = canon_strategy(getattr(closed, "strategy", ""))
        symbol = canon_symbol(getattr(closed, "symbol", ""))
        tf_keys = tf_variants(getattr(closed, "tf", None))
        source = canon_source(getattr(closed, "source", ""))
        # Пишем в несколько TF ключей (канон + легаси), чтобы не было "дыр" в чтении.
        for tf in tf_keys:
            pipe.rpush(f"closed:{strategy}:{symbol}:{tf}", oid)
            pipe.rpush(f"closed:{strategy}:{symbol}:{tf}:{source}", oid)

        # 3.1) ZSET индекс (опционально)
        if zsets_enabled:
            try:
                self._index_closed_zset(closed)
            except Exception:
                # Индекс не должен ломать финализацию сделки.
                pass

        pipe.srem("orders:open", oid)
        # done_key ставим ПОСЛЕ publish (уменьшает риск "пометили, но не опубликовали")
        pipe.set(done_key, "1", ex=self._close_done_ttl_sec)

        # ---- NEW: mark sid done AFTER successful close (best-effort) ----
        if sid_done_key:
            try:
                pipe.set(sid_done_key, "1", ex=self._sid_done_ttl_sec())
            except Exception as e:
                logger.debug("Failed to set sid done key %s: %s", sid_done_key, e)

        # 30-day retention for closed order hashes in Redis (Postgres is the long-term archive)
        retention_days = int(os.getenv("TRADES_CLOSED_RETENTION_DAYS", "30"))
        pipe.expire(key, retention_days * 24 * 3600)

        pipe.execute()

        # lock cleanup (best-effort)
        with contextlib.suppress(Exception):
            self.r.delete(lock_key)

    def append_event(self, ev: TradeEvent) -> None:
        """Добавление события с нормализацией direction через _side_to_str."""
        payload = _stringify({
            "event_type": ev.event_type,
            "event": ev.event_type,
            "order_id": ev.order_id,
            "sid": ev.sid,
            "strategy": ev.strategy,
            "source": ev.source,
            "symbol": ev.symbol,
            "tf": ev.tf,
            "direction": _side_to_str(ev.direction),
            "ts": ev.ts_ms,
            **(ev.payload or {}),
        })
        self.r.xadd(RS.EVENTS_TRADES, payload, maxlen=STREAM_RETENTION[RS.EVENTS_TRADES], approximate=True)

    def _index_closed_zset(self, closed) -> None:
        """
        Индексация закрытий в ZSET:
          score  = exit_ts_ms
          member = order_id

        Ключи:
          closed_z:{strategy}:{symbol}:{tf}
          closed_z:{strategy}:{symbol}:{tf}:{source}
        """
        ts = _to_int(getattr(closed, "exit_ts_ms", 0), 0)
        if ts <= 0:
            return
        strategy = canon_strategy(getattr(closed, "strategy", ""))
        symbol = canon_symbol(getattr(closed, "symbol", ""))
        # TF variants: чтобы ZSET индекс был совместим со старыми ключами (m1/m5) и новым каноном (1m/5m)
        tf_keys = tf_variants(getattr(closed, "tf", None))
        source = canon_source(getattr(closed, "source", ""))
        oid = str(getattr(closed, "order_id", "")).strip()
        if not oid:
            return
        for tf in tf_keys:
            self.r.zadd(self._zkey_closed(strategy=strategy, symbol=symbol, tf=tf, source=source), {oid: ts})
            self.r.zadd(self._zkey_closed(strategy=strategy, symbol=symbol, tf=tf, source=None), {oid: ts})

    def _zkey_closed(self, *, strategy: str, symbol: str, tf: str, source: str | None) -> str:
        if source:
            return f"closed_z:{strategy}:{symbol}:{tf}:{source}"
        return f"closed_z:{strategy}:{symbol}:{tf}"

    # --- Read utilities for ZSET (optional migration helpers) ---
    def get_closed_by_time(  # type: ignore
        self,  # type: ignore
        *,
        strategy: str,
        symbol: str,
        tf: str,
        source: str | None = None,
        from_ts_ms: int,
        to_ts_ms: int,
        limit: int = 5000,
        desc: bool = True,
    ) -> list[str]:
        """
        Быстрая выборка order_id по времени (ZSET).
        Используйте для reporter/trade_back, чтобы не сканировать stream.
        """
        st = canon_strategy(strategy)
        sy = canon_symbol(symbol)
        tff = canon_tf(tf)
        key = f"closed_z:{st}:{sy}:{tff}" if not source else f"closed_z:{st}:{sy}:{tff}:{canon_source(source)}"
        min_s = from_ts_ms
        max_s = to_ts_ms
        # redis-py: zrevrangebyscore / zrangebyscore
        if desc:
            return list(self.r.zrevrangebyscore(key, max_s, min_s, start=0, num=limit) or [])
        return list(self.r.zrangebyscore(key, min_s, max_s, start=0, num=limit) or [])

    def get_closed_last_n(  # type: ignore
        self,  # type: ignore
        *,
        strategy: str,
        symbol: str,
        tf: str,
        source: str | None = None,
        n: int = 500,
    ) -> list[str]:
        """Последние N закрытых order_id (ZSET)."""
        st = canon_strategy(strategy)
        sy = canon_symbol(symbol)
        tff = canon_tf(tf)
        key = f"closed_z:{st}:{sy}:{tff}" if not source else f"closed_z:{st}:{sy}:{tff}:{canon_source(source)}"
        return list(self.r.zrevrange(key, 0, max(0, n - 1)) or [])

    # ---------------------------------------------------------------------
    # New: ZSET-based read utilities
    # ---------------------------------------------------------------------
    def get_closed_by_time(  # type: ignore
        self,  # type: ignore
        *,
        strategy: str,
        symbol: str,
        tf: str,
        from_ts_ms: int,
        to_ts_ms: int,
        source: str | None = None,
        limit: int | None = None,
        with_hash: bool = True,
        reverse: bool = False,
    ) -> list[dict[str, str]]:
        """
        Быстро выбирает закрытые сделки за диапазон времени по ZSET.

        Требует:
          ENABLE_CLOSED_ZSET_INDEX=1
        Иначе вернет [].

        Возвращает:
          - если with_hash=True: список HGETALL(order:{id})
          - иначе: список dict {"order_id": "..."} (минимум)
        """
        if not _env_bool(_ENV_ENABLE_CLOSED_ZSET_INDEX, default=False):
            return []

        st = canon_strategy(strategy)
        sy = canon_symbol(symbol)
        t = canon_tf(tf)
        zkey = _closed_zset_key(st, sy, t, source)

        start = 0
        num = limit if (limit is not None and limit > 0) else None

        # redis-py: zrangebyscore(name, min, max, start=None, num=None)
        if reverse:
            ids = self.r.zrevrangebyscore(zkey, to_ts_ms, from_ts_ms, start=start, num=num) or []
        else:
            ids = self.r.zrangebyscore(zkey, from_ts_ms, to_ts_ms, start=start, num=num) or []

        out: list[dict[str, str]] = []
        if not ids:
            return out

        if not with_hash:
            return [{"order_id": str(x)} for x in ids]

        # Пакетное чтение (если есть pipeline)
        pipe = getattr(self.r, "pipeline", None)
        if callable(pipe):
            p = self.r.pipeline(transaction=False)
            for oid in ids:
                p.hgetall(f"order:{oid}")
            rows = p.execute() or []
            for row in rows:
                if row:
                    out.append({str(k): str(v) for k, v in row.items()})
            return out

        # Fallback: без pipeline
        for oid in ids:
            row = self.r.hgetall(f"order:{oid}") or {}
            if row:
                out.append({str(k): str(v) for k, v in row.items()})
        return out

    def get_closed_last_n(  # type: ignore
        self,  # type: ignore
        *,
        strategy: str,
        symbol: str,
        tf: str,
        n: int = 200,
        source: str | None = None,
        with_hash: bool = True,
    ) -> list[dict[str, str]]:
        """
        Быстро берет последние N закрытых сделок по ZSET.
        """
        if not _env_bool(_ENV_ENABLE_CLOSED_ZSET_INDEX, default=False):
            return []
        n = n if n > 0 else 200

        st = canon_strategy(strategy)
        sy = canon_symbol(symbol)
        t = canon_tf(tf)
        zkey = _closed_zset_key(st, sy, t, source)

        ids = self.r.zrevrange(zkey, 0, n - 1) or []
        if not ids:
            return []
        if not with_hash:
            return [{"order_id": str(x)} for x in ids]

        pipe = getattr(self.r, "pipeline", None)
        out: list[dict[str, str]] = []
        if callable(pipe):
            p = self.r.pipeline(transaction=False)
            for oid in ids:
                p.hgetall(f"order:{oid}")
            rows = p.execute() or []
            for row in rows:
                if row:
                    out.append({str(k): str(v) for k, v in row.items()})
            return out

        for oid in ids:
            row = self.r.hgetall(f"order:{oid}") or {}
            if row:
                out.append({str(k): str(v) for k, v in row.items()})
        return out

    # -----------------
    # ZSET read helpers
    # -----------------
    def get_closed_by_time(
        self,
        *,
        strategy: str,
        symbol: str,
        tf: str,
        source: str | None,
        from_ts_ms: int,
        to_ts_ms: int,
        limit: int = 2000,
        desc: bool = True,
    ) -> list[str]:
        """
        Быстрая выборка order_id закрытых сделок по времени через ZSET.
        Требует TRADES_CLOSED_ZSET_INDEX=1.

        Почему ZSET:
          - точное окно времени: ZRANGEBYSCORE [from..to]
          - без сканирования stream по min_id
          - компактно и быстро (O(logN + K)).
        """
        st = canon_strategy(strategy)
        sym = canon_symbol(symbol)
        # tf приводим в канон — но если индексация была и в легаси, caller может передавать "M1"
        tf_c = canon_tf(tf)
        src = canon_source(source) if source else None
        zkey = self._zkey_closed(strategy=st, symbol=sym, tf=tf_c, source=src)
        try:
            if desc:
                # zrevrangebyscore(max, min, start, num)
                out = self.r.zrevrangebyscore(zkey, to_ts_ms, from_ts_ms, start=0, num=limit) or []
            else:
                out = self.r.zrangebyscore(zkey, from_ts_ms, to_ts_ms, start=0, num=limit) or []
        except Exception:
            out = []
        return [str(x) for x in out]

    def get_closed_last_n(
        self,
        *,
        strategy: str,
        symbol: str,
        tf: str,
        source: str | None,
        limit: int = 200,
        desc: bool = True,
    ) -> list[str]:
        """
        Выборка последних N order_id по времени закрытия через ZSET.
        Удобно для trailing_edge_analyzer и небольших тулов.
        """
        st = canon_strategy(strategy)
        sym = canon_symbol(symbol)
        tf_c = canon_tf(tf)
        src = canon_source(source) if source else None
        zkey = self._zkey_closed(strategy=st, symbol=sym, tf=tf_c, source=src)
        try:
            if desc:
                out = self.r.zrevrange(zkey, 0, max(0, limit - 1)) or []
            else:
                out = self.r.zrange(zkey, 0, max(0, limit - 1)) or []
        except Exception:
            out = []
        return [str(x) for x in out]

    # --------------------
    # Recovery
    # --------------------
    def load_open_positions(self, limit: int = 5000) -> list[dict[str, str]]:
        """
        Recovery при старте.

        Фиксы:
          - используем SSCAN вместо SMEMBERS, чтобы не выгружать весь set в память.
          - корректно обрабатываем bytes (decode_responses=False).
          - гарантируем, что out содержит dict[str, str] без артефактов b'...'.
        """
        out: list[dict[str, str]] = []
        cursor = 0
        fetched = 0

        while True:
            cursor, batch = self.r.sscan("orders:open", cursor=cursor, count=10000)
            for oid in (batch or []):
                soid = _to_str(oid)
                h_raw = self.r.hgetall(f"order:{soid}") or {}
                h = _decode_map(h_raw)
                if h and (h.get("status") == "open"):
                    out.append(h)
                    fetched += 1
                    if fetched >= limit:
                        return out
            if cursor == 0:
                break
        return out
