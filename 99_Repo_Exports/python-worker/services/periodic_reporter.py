# services/periodic_reporter.py
import importlib.util
import json
import math
import os
import time
from datetime import UTC, datetime
from pathlib import Path

from common.log import setup_logger
from core.redis_client import get_redis
from core.redis_keys import RedisStreams as RS
from handlers.crypto_orderflow.utils.log_sampler import sampled_info, sampled_warning
from services.pnl_math import safe_div
from services.reporting_service import ReportingService
from services.trade_metrics_service import TradeMetricsService
from services.trailing_edge_analyzer import TrailingEdgeAnalyzer
from utils.helpers import _norm_map, _si
from utils.time_utils import get_ny_time_millis

# Setup logger before imports that might fail
logger = setup_logger("PeriodicReporter")

try:
    from services.edge_gate_reporter import EdgeGateReportConfig, EdgeGateReporter
except ImportError:
    # Fail safe if file doesn't exist yet (though we just created it)
    logger.warning("Could not import EdgeGateReporter")
    EdgeGateReporter = None


# Import trailing vs baseline analyzer
try:
    scripts_dir = Path(__file__).resolve().parent.parent / "scripts"
    module_path = scripts_dir / "analyze_trailing_vs_baseline_postgres.py"
    if not module_path.exists():
        raise ImportError(f"missing {module_path}")
    spec = importlib.util.spec_from_file_location("analyze_trailing_vs_baseline_postgres", module_path)
    if not spec or not spec.loader:
        raise ImportError("unable to load analyze_trailing_vs_baseline_postgres")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    analyze_global = getattr(module, "analyze_global", None)
    analyze_by_tag = getattr(module, "analyze_by_tag", None)
    analyze_by_strong_gate = getattr(module, "analyze_by_strong_gate", None)
    load_trades_from_postgres = getattr(module, "load_trades_from_postgres", None)
    if not all([analyze_global, analyze_by_tag, analyze_by_strong_gate, load_trades_from_postgres]):
        raise ImportError("trailing analyzer missing expected functions")
except Exception as e:
    logger.warning(f"⚠️ Could not import trailing analyzer: {e}")
    analyze_global = None
    analyze_by_tag = None
    analyze_by_strong_gate = None
    load_trades_from_postgres = None

# Import trailing size recommender
try:
    from services.trailing_size_recommender import ClosedTradeSnapshot, recommend_trailing_size
except ImportError as e:
    logger.warning(f"⚠️ Could not import trailing size recommender: {e}")
    recommend_trailing_size = None
    ClosedTradeSnapshot = None

from domain.normalizers import (
    bucket_close_reason,
    canon_source,
    canon_strategy,
    canon_symbol,
    canon_tf,
    strategy_from_source,
    tf_variants,
)
from infra.redis_repo import RedisTradeRepository
from services.trade_closed_hydrator import hydrate_trade_closed, hydrate_trade_closed_batch
import contextlib

REDIS_URL = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")

# Если ZSET индекс включён, можно НЕ сканировать stream для окна по времени.
# Это особенно полезно в compact режиме (stream маленький, но всё равно нужен hydrate).
PERIODIC_REPORT_USE_ZSET = os.getenv("PERIODIC_REPORT_USE_ZSET", "1").strip().lower() in ("1","true","yes","on")

# Полный список TF в проде (по вашим данным). Можно переопределить env'ом.
PERIODIC_REPORT_TFS_ENV = os.getenv(
    "PERIODIC_REPORT_TFS",
    "tick,1m,5m,15m,1h,4h,1d,1w,1month,M1,M5"
)

# Отправлять отчеты, даже если есть только виртуальные сделки (real=0)
PERIODIC_REPORT_SEND_VIRTUAL_ONLY = os.getenv("PERIODIC_REPORT_SEND_VIRTUAL_ONLY", "true").strip().lower() in ("1","true","yes","on")


def _env_bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return str(v).strip().lower() in ("1", "true", "yes", "y", "on")


def _closed_zkey(strategy: str, symbol: str, tf: str, source: str | None = None) -> str:
    from domain.normalizers import canon_tf
    st = canon_strategy(strategy)
    sy = canon_symbol(symbol)
    t = canon_tf(tf)
    if source is None:
        return f"closed_z:{st}:{sy}:{t}"
    so = canon_source(source)
    return f"closed_z:{st}:{sy}:{t}:{so}"

RECENT_WINDOW_SECONDS = int(os.getenv("PERIODIC_REPORT_WINDOW_SECONDS", "3600"))
RECENT_LIMIT = int(os.getenv("PERIODIC_REPORT_RECENT_LIMIT", "2000"))
# Отчеты теперь триггерятся каждые N сделок (по умолчанию 100)
REPORT_TRIGGER_COUNT = int(os.getenv("REPORT_TRIGGER_COUNT", "100"))
# ИЛИ каждые N секунд (по умолчанию 3600 = 1 час)
REPORT_TRIGGER_INTERVAL_SECONDS = int(os.getenv("REPORT_TRIGGER_INTERVAL_SECONDS", "3600"))

REPORT_COUNTER_TTL_SECONDS = int(os.getenv("REPORT_COUNTER_TTL_SECONDS", "86400"))
REPORT_DEDUP_TTL_SECONDS = int(os.getenv("REPORT_DEDUP_TTL_SECONDS", "172800"))
REPORT_LOCK_TTL_SECONDS = int(os.getenv("REPORT_LOCK_TTL_SECONDS", "90"))

MIN_TRADES_FOR_WR_WARN = int(os.getenv("PERIODIC_REPORT_MIN_TRADES_FOR_WR_WARN", "10"))
EPS = float(os.getenv("PERIODIC_REPORT_EPS", "1e-9"))
# Опция: если нет явного трейлинга, но был TP1 — считать трейлинг стартовавшим (rocket_v1)
INFER_TRAILING_FROM_TP1 = os.getenv("PERIODIC_INFER_TRAILING_FROM_TP1", "true").lower() == "true"
# Управление показом метрик трейлинга в отчетах (автоопределение по уже существующим флагам)
def _detect_trailing_enabled() -> bool:
    """
    Возвращает, включен ли трейлинг:
    - ENV FORCE_TRAIL_AFTER_TP1 (глобальный флаг из docker-compose) — если "false", считаем трейлинг выключенным.
    - иначе пытаемся прочитать config/trailing_config.json → поле enabled
    - по умолчанию считаем включенным
    """
    force_trail = os.getenv("FORCE_TRAIL_AFTER_TP1")
    if force_trail is not None:
        return force_trail.lower() == "true"

    try:
        cfg_path_env = os.getenv("TRAILING_CONFIG_PATH")
        cfg_path = Path(cfg_path_env) if cfg_path_env else Path(__file__).resolve().parent.parent / "config" / "trailing_config.json"
        if cfg_path.exists():
            with cfg_path.open("r", encoding="utf-8") as f:
                data = json.load(f)
            enabled = data.get("enabled")
            if isinstance(enabled, bool):
                return enabled
    except Exception:
        pass

    return True

TRAILING_SECTION_ENABLED = _detect_trailing_enabled()
# Окно в сделках (по умолчанию совпадает с триггером REPORT_TRIGGER_COUNT). 0 или меньше — использовать окно по времени.
TRADE_WINDOW_COUNT = int(os.getenv("PERIODIC_REPORT_TRADE_WINDOW_COUNT", str(REPORT_TRIGGER_COUNT)))

# Контроль включения анализа trailing vs baseline в отчеты
TRAILING_VS_BASELINE_ENABLED = os.getenv("PERIODIC_REPORT_TRAILING_VS_BASELINE_ENABLED", "true").lower() == "true"

_reporter_instance = None


def get_reporter_instance():
    global _reporter_instance
    if _reporter_instance is None:
        _reporter_instance = PeriodicReporter()
    return _reporter_instance


def check_and_trigger_report(source: str, symbol: str, counter_type: str = "trades", order_id: str | None = None, demo_only: bool = False):
    try:
        # demo_only is deprecated — unified report now includes virtual/real breakdown
        get_reporter_instance()._check_and_trigger_report(source, symbol, counter_type, order_id=order_id)
    except Exception as e:
        logger.debug("trigger check failed: %s", e)


class PeriodicReporter:
    def __init__(self):
        self.reporting = ReportingService(redis_url=REDIS_URL)
        # Используем decode_responses=True для автоматической конвертации bytes→str
        import redis as redis_lib
        if REDIS_URL:
            _pool = redis_lib.ConnectionPool.from_url(
                REDIS_URL,
                decode_responses=True,
                socket_timeout=120,
                socket_connect_timeout=30,
                socket_keepalive=True,
                health_check_interval=30,
                retry_on_timeout=True,
                retry_on_error=[
                    redis_lib.exceptions.ConnectionError,
                    redis_lib.exceptions.TimeoutError,
                ],
                max_connections=int(os.getenv("REDIS_MAX_CONNECTIONS", "20")),
            )
            self.redis = redis_lib.Redis(connection_pool=_pool)
        else:
            self.redis = get_redis()
        # repo нужен только для удобных batch-read (pipeline) и единых ключей;
        # write-path здесь отсутствует.
        self.repo = RedisTradeRepository(self.redis)
        self.tm = TradeMetricsService(eps=EPS)
        self.trailing_analyzer = TrailingEdgeAnalyzer(self.redis)

        # Trailing edge analysis state
        # Анализ trailing edge делается каждые N отчетов, а не каждые N секунд
        # Это обеспечивает согласованность с REPORT_TRIGGER_COUNT
        self.trailing_analysis_reports_interval = int(os.getenv("TRAILING_ANALYSIS_REPORTS_INTERVAL", "5"))  # every 5 reports (150 сделок)

        # Trailing vs baseline analysis state
        # Анализ trailing vs baseline выполняется каждый отчет для принятия решений каждые 100 сделок
        self.trailing_vs_baseline_reports_interval = int(os.getenv("TRAILING_VS_BASELINE_REPORTS_INTERVAL", "1"))  # every 1 report (100 сделок)

        self.report_counter = {}  # {f"{source}:{symbol}": count}

    def _candidate_tfs(self) -> list[str]:
        """
        Возвращаем список TF в КАНОНИЧЕСКОМ виде.
        При этом запись закрытий уже идет в несколько tf_variants(), но для чтения
        лучше начинать с канона, а легаси покроется через tf_variants(tf).
        """
        raw = [x.strip() for x in (PERIODIC_REPORT_TFS_ENV or "").split(",") if x.strip()]
        out: list[str] = []
        for tf in raw:
            c = canon_tf(tf)
            if c and c not in out:
                out.append(c)
        return out or ["tick", "1m", "5m"]


    def _symbol_trailing_enabled(self, symbol: str) -> bool | None:
        """
        Пытаемся считать из Redis спецификацию символа: symbol_specs:{symbol}
        Ожидаем поле trailing_enabled (bool). Если нет/ошибка — возвращаем None.
        """
        try:
            raw = self.redis.get(f"symbol_specs:{symbol}")
            if not raw:
                return None
            if isinstance(raw, bytes):
                raw = raw.decode("utf-8")
            data = json.loads(raw) if isinstance(raw, str) else raw
            if isinstance(data, dict) and "trailing_enabled" in data:
                val = data.get("trailing_enabled")
                if isinstance(val, bool):
                    return val
                if isinstance(val, (int, float)):
                    return bool(val)
                if isinstance(val, str):
                    return val.lower() in ("1", "true", "yes", "on")
            return None
        except Exception:
            return None

    def _iter_recent_trades_window(
        self,
        *,
        strategy: str,
        symbol: str,
        tf: str,
        source: str,
        window_seconds: int | None = None,
    ) -> list[dict[str, str]]:
        """
        Возвращает список сделок для окна отчёта.

        Источники (приоритет):
          1) ZSET (если PERIODIC_REPORT_USE_ZSET=1 и ENABLE_CLOSED_ZSET_INDEX=1)
          2) trades:closed stream (как сейчас)
          3) legacy lists / order:{id} (как сейчас в вашем коде, если stream пуст)

        ВАЖНО про compact-stream:
          Если TRADES_CLOSED_STREAM_COMPACT=1, то stream содержит минимум полей.
          Тогда hydrate_trade_closed() подтянет полные поля из order:{id}.
        """
        now_ms = get_ny_time_millis()
        target_window = window_seconds if window_seconds is not None else RECENT_WINDOW_SECONDS
        cutoff_ms = now_ms - int(target_window) * 1000

        # (A) режим "последние N сделок" имеет приоритет над "окно по времени"
        # НО только если window_seconds НЕ передан явно (для ежедневных отчетов всегда берем по времени)
        trade_window_count: int | None = (TRADE_WINDOW_COUNT if TRADE_WINDOW_COUNT > 0 else None) if window_seconds is None else None

        zset_enabled = (
            _env_bool("ENABLE_CLOSED_ZSET_INDEX", default=False) or
            _env_bool("TRADES_CLOSED_ZSET_INDEX", default=False) or
            _env_bool("REDIS_CLOSED_ZSETS_ENABLED", default=False)
        )

        if PERIODIC_REPORT_USE_ZSET and zset_enabled and symbol != "ALL":
            all_ids = []
            for candidate_tf in self._candidate_tfs():
                zkey = _closed_zkey(strategy, symbol, candidate_tf, source)
                try:
                    if trade_window_count:
                        ids = self.redis.zrevrange(zkey, 0, min(trade_window_count - 1, RECENT_LIMIT - 1)) or []
                    else:
                        # score = exit_ts_ms, поэтому окно времени точное
                        ids = self.redis.zrevrangebyscore(zkey, now_ms, cutoff_ms, start=0, num=RECENT_LIMIT) or []
                    if ids:
                        all_ids.extend(ids)
                except Exception:
                    pass

            if all_ids:
                u_ids = []
                seen_ids = set()
                for i in all_ids:
                    if i not in seen_ids:
                        seen_ids.add(i)
                        u_ids.append(i)

                # Быстро hydrate из order hash (пакетно pipeline)
                p = self.redis.pipeline(transaction=False)
                for oid in u_ids:
                    p.hgetall(f"order:{oid}")
                rows = p.execute() or []
                out: list[dict[str, str]] = []
                for row in rows:
                    if not row:
                        continue
                    t = _norm_map(row)
                    # фильтр по времени (на случай если tp window_count)
                    # в order hash у вас closed_time=exit_ts_ms
                    try:
                        ct = int(t.get("closed_time") or t.get("exit_ts_ms") or 0)
                    except Exception:
                        ct = 0
                    if trade_window_count is None and ct and ct < cutoff_ms:
                        continue
                    if (t.get("status") or "").lower() != "closed":
                        continue
                    out.append(t)
                return out

        # (B) STREAM path (fallback or primary if ZSET disabled)
        # ----------------------------------------------------------------------
        # SPECIAL CASE FOR "ALL" with ZSET enabled:
        # Since RS.TRADES_CLOSED stream might be empty/deprecated, we must manually
        # aggregate ZSETs from all active symbols if we want a reliable "ALL" report.
        # ----------------------------------------------------------------------
        if symbol == "ALL" and PERIODIC_REPORT_USE_ZSET and zset_enabled:
            # 1. Get all known symbols
            # We can use a predefined list or fetch from redis "crypto:symbols"
            # For reported consistency, let's try getting dynamic symbols first, fallback to DEFAULT
            sys_symbols_raw = self.redis.smembers("crypto:symbols")
            all_syms = set()
            if sys_symbols_raw:
                for s in sys_symbols_raw:
                    s_str = s.decode("utf-8") if isinstance(s, bytes) else str(s)
                    all_syms.add(canon_symbol(s_str))

            # Add defaults just in case
            from services.orderflow.configuration import DEFAULT_SYMBOLS
            for s in DEFAULT_SYMBOLS:
                all_syms.add(canon_symbol(s))

            # Explicitly include  and XAU if needed

            all_syms.add("XAUUSDT")

            # 2. Collect trades from ALL symbols
            # We'll limit per symbol to avoid exploding memory, then sort and limit global
            # For "ALL" report we usually want the global recent list.

            # This is heavy but necessary without a global stream.
            # We treat target_window as authoritative.

            all_trades: list[dict[str, str]] = []

            # We need to look at specific TFs or just "tick"?
            # closed_z index exists per TF. Usually "tick" or "1m" covers everything if implemented correctly.
            # safe assumption: iterate candidate TFs.
            tfs = self._candidate_tfs()

            for sym in all_syms:
                # FIX: Do not skip XAU / GOLD if we want them in ALLSYMBOLS
                # if "XAU" in sym or "GOLD" in sym:
                #    continue

                # Re-use ZSET logic per symbol
                # We can't easily call self._iter_recent_trades_window recursively because it might recurse infinitely or fail checks
                # So we duplicate the ZSET fetch logic for single symbol here (simplified)
                zkey_ids = []
                for tf in tfs:
                    zkey = _closed_zkey(strategy, sym, tf, source)
                    try:
                        # fetch somewhat more than needed to ensure we have enough after global merge
                        # score = exit_ts_ms
                        ids = self.redis.zrevrangebyscore(zkey, now_ms, cutoff_ms, start=0, num=RECENT_LIMIT) or []
                        zkey_ids.extend(ids)
                    except Exception:
                        pass

                if not zkey_ids:
                    continue

                # uniq
                u_ids = list(set(zkey_ids))
                if not u_ids:
                    continue

                # hydrate logic
                p = self.redis.pipeline(transaction=False)
                valid_ids = []
                for oid in u_ids:
                    oid_str = str(oid) if not isinstance(oid, bytes) else oid.decode('utf-8')
                    if oid_str:
                        valid_ids.append(oid_str)
                        p.hgetall(f"order:{oid_str}")

                if not valid_ids:
                    continue

                rows = p.execute() or []
                for r in rows:
                    if not r:
                        continue
                    t = _norm_map(r)
                    if (t.get("status") or "").lower() != "closed":
                        continue
                    # Double check source (though zkey included it)
                    raw_source = t.get("source") or ""
                    raw_strategy = t.get("strategy") or ""
                    t_source = self._source_from_strategy(raw_strategy or raw_source or "", sym)
                    req_source = self._source_from_strategy(source, sym)
                    if t_source != req_source and t_source not in ("binance", "bybit", "mt5", "binance_real", "binance_paper"):
                        continue

                    all_trades.append(t)

            if all_trades:
                # 3. Sort by exit_ts_ms desc
                all_trades.sort(key=lambda x: float(x.get("exit_ts_ms") or x.get("closed_time") or 0), reverse=True)

                # 4. Limit
                limit = trade_window_count if trade_window_count else RECENT_LIMIT
                return all_trades[:limit]

            # If all_trades is still empty (e.g. ZSETs are actually lists leading to WRONGTYPE),
            # fall through to the Standard Stream Path below.

        # Standard Stream Path (for non-ALL or if ZSET disabled)
        # For symbol="ALL", we rely on the stream (global source) and filter in loop.
        min_id = f"{cutoff_ms}-0"
        try:
            _stream_limit = trade_window_count if trade_window_count else max(RECENT_LIMIT, 50000)
            entries = self.redis.xrevrange(RS.TRADES_CLOSED, max="+", min=min_id, count=_stream_limit) or []
        except Exception:
            entries = []

        out: list[dict[str, str]] = []
        if entries:
            for _id, fields in entries:
                f = _norm_map(fields or {})
                # hydrate (учтёт compact-stream и/или недостающие поля)
                merged = hydrate_trade_closed(self.redis, f, require_closed=False, merge_precedence="hash")
                t = _norm_map(merged)
                # фильтры
                raw_source = t.get("source") or ""
                raw_strategy = t.get("strategy") or ""
                t_symbol = canon_symbol(t.get("symbol"))

                t_source = self._source_from_strategy(raw_strategy or raw_source or "", t_symbol)
                req_source = self._source_from_strategy(source, t_symbol)

                if t_source != req_source and t_source not in ("binance", "bybit", "mt5", "binance_real", "binance_paper"):
                    continue

                # Handling for "ALL" aggregation
                if symbol == "ALL":
                     # FIX: Do not exclude non-crypto (e.g. XAU/GOLD) in ALL report
                     # if "XAU" in t_symbol or "GOLD" in t_symbol:
                     #     continue
                     pass
                else:
                    # Standard symbol filter
                    if t_symbol != canon_symbol(symbol):
                         continue
                out.append(t)
            if trade_window_count:
                return out[:trade_window_count]
            return out

        # (C) Если stream пуст — ваш существующий fallback через lists/order hashes остаётся
        # But fallback using lists is symbol-specific. If ALL, we can't easily fallback unless we iterate all known keys?
        # Usually stream shouldn't be empty if there are trades. skipping fallback for ALL.
        if symbol == "ALL":
             return []

        return []

    # Env-flag: если DISABLE_SCHEDULED_REPORTS=true, event-driven path пропускает hourly/daily
    # (расписание владеет periodic_reporter_timer сервис). Только count-based trigger остаётся.
    _DISABLE_SCHEDULED = os.getenv("DISABLE_SCHEDULED_REPORTS", "false").lower() in ("1", "true", "yes")

    def _check_and_trigger_report(self, source: str, symbol: str, counter_type: str = "trades", order_id: str | None = None):
        """
        Проверяет условия и триггерит отчеты (Hourly/Daily/Count-based).
        Вызывается при каждой закрытой сделке.
        Если DISABLE_SCHEDULED_REPORTS=true — hourly/daily пропускаются (делает periodic-reporter-timer).
        """
        instance_id = os.getenv("HOSTNAME", "local-worker")
        logger.debug(f"[_check_and_trigger_report] Caller: {instance_id}, Pair: {source}/{symbol}, Type: {counter_type}")

        src = canon_source(source)
        sym = canon_symbol(symbol)
        now_ts = time.time()
        now_dt = datetime.fromtimestamp(now_ts, tz=UTC)

        pfx = ""

        # ---------------------------------------------------------
        # 1. Hourly Report (Window = 60 minutes)
        # Пропускаем если расписание отдано periodic-reporter-timer (DISABLE_SCHEDULED_REPORTS=true)
        # ---------------------------------------------------------
        if not PeriodicReporter._DISABLE_SCHEDULED:
            hourly_key = f"{pfx}report_last_hourly_hour:{src}:{sym}"
            current_hour_str = now_dt.strftime("%Y-%m-%d-%H")
            try:
                last_hourly_hour = self.redis.get(hourly_key)
            except Exception:
                last_hourly_hour = None

            if last_hourly_hour != current_hour_str:
                lock_key = f"{pfx}report_lock:{src}:{sym}"
                if self._acquire_lock(lock_key, ttl=REPORT_LOCK_TTL_SECONDS):
                    try:
                        if self.redis.get(hourly_key) == current_hour_str:
                            return
                        last_ts_key = f"{pfx}report_last_ts:{src}:{sym}"
                        try:
                            last_ts = float(self.redis.get(last_ts_key) or 0)
                        except Exception:
                            last_ts = 0.0
                        if (now_ts - last_ts) < 1800 and last_hourly_hour is not None:
                            return
                        sampled_info(logger, "PERIODIC_REPORTER_HOURLY", f"⏰ HOURLY Report trigger for {src}/{sym} at {now_dt.strftime('%H:%M')} UTC")
                        self.send_report_for_pair(src, sym, window_seconds=3600, silent_locked=True)
                        self.redis.set(hourly_key, current_hour_str, ex=172800)
                        self.redis.set(last_ts_key, str(now_ts), ex=172800)
                        # FIX: Reset count-based trigger counter after hourly fires
                        # to prevent count-based trigger from firing duplicate report
                        # in same window (especially relevant for symbol=ALL)
                        _cnt_key = f"{pfx}report_trade_count:{src}:{sym}"
                        with contextlib.suppress(Exception):
                            self.redis.set(_cnt_key, 0)
                    finally:
                        self._release_lock(lock_key)

        # ---------------------------------------------------------
        # 2. Daily Report (17:00 UTC)
        # Пропускаем если расписание отдано periodic-reporter-timer (DISABLE_SCHEDULED_REPORTS=true)
        # ---------------------------------------------------------
        if not PeriodicReporter._DISABLE_SCHEDULED and (now_dt.hour > 17 or (now_dt.hour == 17 and now_dt.minute >= 5)):
            daily_key = f"{pfx}report_last_daily_date:{src}:{sym}"
            today_str = now_dt.strftime("%Y-%m-%d")

            try:
                last_daily_date = self.redis.get(daily_key)
            except Exception:
                last_daily_date = None

            if last_daily_date != today_str:
                lock_key = f"{pfx}report_lock:{src}:{sym}"
                if self._acquire_lock(lock_key, ttl=REPORT_LOCK_TTL_SECONDS):
                    try:
                        if self.redis.get(daily_key) == today_str:
                            return

                        sampled_info(logger, "PERIODIC_REPORTER_DAILY", f"📅 DAILY Report trigger (17:00 UTC) for {src}/{sym}")
                        # Используем send_report_for_pair для совместимости с тестами и единообразия
                        self.send_report_for_pair(src, sym, window_seconds=86400, silent_locked=True)
                        self.redis.set(daily_key, today_str, ex=259200) # 3 days TTL
                    finally:
                        self._release_lock(lock_key)

        # ---------------------------------------------------------
        # 3. Count-based Trigger (DISABLED)
        # Отчеты теперь отправляются только раз в час/сутки для всех символов (включая ALL).
        # ---------------------------------------------------------
        counter_key = f"{pfx}report_trade_count:{src}:{sym}"
        with contextlib.suppress(Exception):
            self.redis.incr(counter_key)

        # ---------------------------------------------------------
        # 4. Global "ALL" Report Trigger (Recursive)
        # ---------------------------------------------------------
        if sym != "ALL":
             # Делаем паузу перед триггером ALL, чтобы накопить сделки если они идут пачкой в одну секунду
             # Но поскольку это async/worker, мы просто вызываем. Дедуп по ключу сработает.
             self._check_and_trigger_report(src, "ALL", counter_type, order_id)


    def send_report_for_pair(self, source: str, symbol: str, window_seconds: int | None = None, silent_locked: bool = False, demo_only: bool = False):
        """
        Public wrapper to send a report manually or via legacy calls.
        Acquires lock to ensure single execution if called concurrently.
        NOTE: demo_only is deprecated and ignored. Unified report always includes virtual/real breakdown.
        """
        sym = canon_symbol(symbol)
        src = self._source_from_strategy(source, sym)

        lock_key = f"report_lock:{src}:{sym}"
        if not self._acquire_lock(lock_key, ttl=REPORT_LOCK_TTL_SECONDS):
            if not silent_locked:
                logger.warning(f"⚠️ Не удалось получить lock для {src}/{sym}")
            else:
                logger.debug(f"⏭️ Пропуск {src}/{sym} (lock занят)")
            return

        try:
            self._generate_and_send_report_internal(src, sym, window_seconds=window_seconds)
        finally:
            self._release_lock(lock_key)

    def _is_trade_virtual(self, t: dict) -> bool:
        """Helper to determine if a trade is virtual (demo, shadow, or paper)."""
        is_v_flag = (t.get("is_virtual") or "0") in ("1", "True", "true", "TRUE")
        if not is_v_flag:
            _temp_inds = t.get("indicators") or {}
            if isinstance(_temp_inds, str):
                try:
                    import json
                    _temp_inds = json.loads(_temp_inds)
                except Exception:
                    _temp_inds = {}
            if not _temp_inds and t.get("signal_payload"):
                _sp_raw = t.get("signal_payload")
                if isinstance(_sp_raw, str):
                    try:
                        import json
                        _sp = json.loads(_sp_raw)
                        _temp_inds = _sp.get("indicators") or {}
                    except Exception:
                        pass
                elif isinstance(_sp_raw, dict):
                    _temp_inds = _sp_raw.get("indicators") or {}
            if isinstance(_temp_inds, dict):
                gate_mode = (_temp_inds.get("of_gate_mode") or "").upper()
                if gate_mode in ("SHADOW", "PAPER") or (_temp_inds.get("is_virtual") or "0") in ("1", "True", "true", "TRUE"):
                    is_v_flag = True
        return is_v_flag

    def _generate_and_send_report_internal(self, source: str, symbol: str, window_seconds: int | None = None):
        """
        Internal method to generate and send report.
        ASSUMES LOCK IS ALREADY ACQUIRED by caller.
        """
        src = canon_source(source)
        sym = canon_symbol(symbol)

        # --- ATOMIC DEDUPLICATION ---
        # Prevents duplicates if multiple parallel workers/timers (e.g., SignalPerformanceTracker and
        # _reporter_timer_loop) attempt to generate the same periodic report.
        if window_seconds is not None and window_seconds > 0:
            dedup_val = str(int(time.time()) // window_seconds)
            dedup_key = f"report_dedup_sent:{src}:{sym}:{window_seconds}"

            existing_val = self.redis.get(dedup_key)
            if existing_val == dedup_val:
                logger.debug(f"⏭️ Пропуск {src}/{sym} {window_seconds}s (report already generated for dedup window {dedup_val})")
                return

            # Cross-check legacy timer loop keys just and update them to prevent it running again
            if window_seconds == 3600:
                legacy_hour_key = f"report_last_hourly_hour:{src}:{sym}"
                current_hr_str = datetime.now(UTC).strftime("%Y-%m-%d-%H")
                if self.redis.get(legacy_hour_key) == current_hr_str:
                    logger.debug(f"⏭️ Пропуск {src}/{sym} {window_seconds}s (legacy hour key)")
                    return
                self.redis.set(legacy_hour_key, current_hr_str, ex=86400)
            elif window_seconds == 86400:
                legacy_day_key = f"report_last_daily_date:{src}:{sym}"
                current_day_str = datetime.now(UTC).strftime("%Y-%m-%d")
                if self.redis.get(legacy_day_key) == current_day_str:
                    logger.debug(f"⏭️ Пропуск {src}/{sym} {window_seconds}s (legacy day key)")
                    return
                self.redis.set(legacy_day_key, current_day_str, ex=86400 * 2)

            # Mark dedup key immediately to prevent other callers acquiring the lock from generating it
            self.redis.set(dedup_key, dedup_val, ex=window_seconds * 2 + 300)

        sampled_info(
            logger,
            "PERIODIC_REPORTER_FORMATION",
            f"📤 Формирование отчета для {src}/{sym}"
        )

        try:
            # NEW: ZSET-окно + compact-stream (если включено)
            from domain.normalizers import strategy_from_source
            strategy = strategy_from_source(src)
            tf = "tick"  # основной tf для данного source

            # Попытка использовать новый метод (если ZSET доступен)
            trades = self._iter_recent_trades_window(
                strategy=strategy,
                symbol=sym,
                tf=tf,
                source=src,
                window_seconds=window_seconds,
            )

            if trades:
                # -------------------------------------------------------------
                # Enhanced Metrics: buckets for Real / Shadow-Passed / All Signals
                # -------------------------------------------------------------
                m_real = self.tm.new_metrics()
                m_passed = self.tm.new_metrics()
                m_all = self.tm.new_metrics()
                m_smt_passed = self.tm.new_metrics()  # Hypothetical: SMT VETO mode enabled
                m_all_gates = self.tm.new_metrics()   # Hypothetical: ML Gate + SMT VETO enabled

                # New metrics for Virtual Trades
                m_v_all = self.tm.new_metrics()

                # Setup symbol breakdown accumulator for ALL report
                symbol_breakdown = {}  # {symbol: {"pnl": 0.0, "trades": 0}}

                _min_conf = float(os.getenv("CRYPTO_SIGNAL_MIN_CONF", "70"))
                for t in trades:
                    is_v_flag = self._is_trade_virtual(t)

                    v_gate = (t.get("v_gate_status") or "na").lower()

                    _conf_raw = t.get("conf") or t.get("confidence")
                    if _conf_raw is None:
                        _inds = t.get("indicators") or {}
                        if isinstance(_inds, dict):
                            _conf_raw = _inds.get("confidence") or _inds.get("conf") or _inds.get("score")
                    try:
                        _conf_val = float(_conf_raw) * 100.0 if float(_conf_raw) <= 1.0 else float(_conf_raw)
                    except (ValueError, TypeError):
                        _conf_val = 100.0

                    # Guard: В отчет должны попадать сделки ТОЛЬКО если confidence >= CRYPTO_SIGNAL_MIN_CONF
                    # Игнорируем и виртуальные, и реальные сделки с низким скором.
                    if _conf_val < _min_conf:
                        continue

                    is_real = (not is_v_flag) and (v_gate in ("passed", "na"))

                    # Accumulate ALL valid signals (real + virtual)
                    is_valid = self._accumulate_trade_metrics(m_all, t)
                    if not is_valid:
                        # Skip symbol breakdown, SMT sim, etc. for quarantined trades
                        continue

                    if is_real:
                        self._accumulate_trade_metrics(m_real, t)

                    if is_v_flag:
                        # Accumulate virtual metrics separately
                        self._accumulate_trade_metrics(m_v_all, t)

                    if v_gate in ("passed", "na"):
                        # Would have passed gates
                        self._accumulate_trade_metrics(m_passed, t)

                    # Accumulate symbol specific PnL if this is an aggregated report
                    # Include both real and virtual trades for breakdown
                    if sym == "ALL":
                         t_sym = canon_symbol(t.get("symbol") or "")
                         if t_sym:
                             if t_sym not in symbol_breakdown:
                                 symbol_breakdown[t_sym] = {"pnl": 0.0, "trades": 0}

                             pnl_val = float(t.get("pnl_net") or t.get("pnl") or 0.0)
                             symbol_breakdown[t_sym]["pnl"] += pnl_val
                             symbol_breakdown[t_sym]["trades"] += 1

                    # --- SMT Veto Simulation ---
                    # Logic from SmtCoherenceGate: veto if countertrend + confirmed leader + high coherence
                    # We track missing-fields trades separately so the report shows accurate numbers.

                    _inds = t.get("indicators")
                    if isinstance(_inds, str):
                        try:
                            import json
                            _inds = json.loads(_inds)
                        except Exception:
                            _inds = {}
                    elif not isinstance(_inds, dict):
                        _inds = {}

                    if not _inds and t.get("signal_payload"):
                        _sp_raw = t.get("signal_payload")
                        if isinstance(_sp_raw, str):
                            try:
                                import json
                                _inds = json.loads(_sp_raw).get("indicators", {})
                            except Exception:
                                pass
                        elif isinstance(_sp_raw, dict):
                            _inds = _sp_raw.get("indicators", {})

                    t_smt_conf = t.get("smt_leader_confirm") if t.get("smt_leader_confirm") is not None else _inds.get("smt_leader_confirm")
                    t_smt_coh = t.get("smt_coh") if t.get("smt_coh") is not None else _inds.get("smt_coh")
                    t_smt_ld = t.get("smt_leader_dir") if t.get("smt_leader_dir") is not None else _inds.get("smt_leader_dir")

                    _smt_has_fields = (
                        t_smt_conf is not None
                        or t_smt_coh is not None
                        or t_smt_ld is not None
                    )

                    if _smt_has_fields:
                        try:
                            stm_conf = int(float(t_smt_conf or 0))
                            stm_coh = float(t_smt_coh or 0.0)
                            stm_ld = (t_smt_ld or "").upper().strip()
                            side = (t.get("side") or "").upper().strip()

                            ld_norm = "LONG" if stm_ld == "UP" else ("SHORT" if stm_ld == "DOWN" else "NA")
                            countertrend = (side in ("LONG", "SHORT")) and (ld_norm in ("LONG", "SHORT")) and (side != ld_norm)

                            # Default SMT thresholds: confirm=1, coh>=0.65
                            is_vetoed = countertrend and stm_conf == 1 and stm_coh >= 0.65

                            if not is_vetoed:
                                self._accumulate_trade_metrics(m_smt_passed, t)
                                # All Gates (Sim): Passed ML Gate AND SMT Gate
                                if v_gate == "passed" or (not is_v_flag and v_gate == "na"):
                                    self._accumulate_trade_metrics(m_all_gates, t)
                            # vetoed trades are NOT added to m_smt_passed (counts as hard veto)
                        except Exception:
                            # Genuine parse error on present fields — skip, do not fail-open
                            pass
                    else:
                        # SMT fields not stored in order hash for this trade → sim is N/A
                        # Track separately; do NOT add to m_smt_passed to avoid inflating pass rate
                        m_smt_passed["_missing_fields"] = int(m_smt_passed.get("_missing_fields") or 0) + 1

                # Attach breakdown to metrics (both m_real and m_all for virtual-only reports)
                if sym == "ALL" and symbol_breakdown:
                    m_real["symbol_breakdown"] = symbol_breakdown
                    m_all["symbol_breakdown"] = symbol_breakdown

                # Finalize all buckets
                self.tm.finalize(m_real)
                self.tm.finalize(m_passed)
                self.tm.finalize(m_all)
                self.tm.finalize(m_smt_passed)
                self.tm.finalize(m_all_gates)
                self.tm.finalize(m_v_all)

                # Inject shadow buckets into main metrics
                m_real["shadow_passed"] = m_passed
                m_real["shadow_all"] = m_all
                m_real["smt_passed"] = m_smt_passed
                m_real["shadow_all_gates"] = m_all_gates
                m_real["virtual_all"] = m_v_all

                self._add_health_metrics(m_all, src, sym)
                real_total = int(m_real.get("total_trades", 0))
                virtual_total = int(m_v_all.get("total_trades", 0))
                combined_total = real_total + virtual_total

                # User requested separate reports for REAL and VIRTUAL trades.
                m_real["shadow_passed"] = m_passed
                m_real["shadow_all"] = m_all
                m_real["smt_passed"] = m_smt_passed
                m_real["shadow_all_gates"] = m_all_gates

                m_v_all["shadow_passed"] = m_passed
                m_v_all["shadow_all"] = m_all
                m_v_all["smt_passed"] = m_smt_passed
                m_v_all["shadow_all_gates"] = m_all_gates

                # Count trades OPENED within the report window (by entry_ts_ms / open_time)
                _now_ms = get_ny_time_millis()
                _target_w = (window_seconds if window_seconds is not None else RECENT_WINDOW_SECONDS)
                _cutoff_open_ms = _now_ms - int(_target_w) * 1000
                _opened_real = 0
                _opened_virtual = 0
                for _t in trades:
                    _is_v = self._is_trade_virtual(_t)
                    try:
                        _ets = int(float(_t.get("entry_ts_ms") or _t.get("open_time") or 0))
                        if _ets > 0 and _ets >= _cutoff_open_ms:
                            if _is_v:
                                _opened_virtual += 1
                            else:
                                _opened_real += 1
                    except Exception:
                        pass
                m_real["opened_in_window"] = _opened_real
                m_v_all["opened_in_window"] = _opened_virtual

                sampled_info(
                    logger,
                    "PERIODIC_REPORTER_METRICS_ZSET",
                    f"📊 Собрано метрик для {src}/{sym} через ZSET: сделок={len(trades)}"
                )

                send_empty = os.getenv("PERIODIC_REPORT_SEND_EMPTY", "false").lower() == "true"

                if real_total > 0 or send_empty:
                    self._send_report(src, sym, m_real, window_seconds=window_seconds, report_type="REAL")

                if virtual_total > 0 or (send_empty and PERIODIC_REPORT_SEND_VIRTUAL_ONLY):
                    self._send_report(src, sym, m_v_all, window_seconds=window_seconds, report_type="VIRTUAL")

                return
            else:
                # Fallback на старый метод
                metrics = self._gather_window_metrics_stream(src, sym, window_seconds=window_seconds)
                total_trades = int(metrics.get("total_trades", 0))
                sampled_info(
                    logger,
                    "PERIODIC_REPORTER_METRICS_STREAM",
                    f"📊 Собрано метрик для {src}/{sym} через stream: сделок={total_trades}"
                )
                # FIX: Inject shadow buckets into fallback metrics.
                # _gather_window_metrics_stream() collects only real trades (no virtual split),
                # so shadow_all == shadow_passed == m_real for this path.
                # Without this, "Все сигналы" section shows 0 in the report.
                if "shadow_all" not in metrics and total_trades > 0:
                    # Build a minimal stub with the scalar fields needed for _send_report
                    _scalar_stub = {
                        k: v for k, v in metrics.items()
                        if isinstance(v, (int, float)) and k not in (
                            "shadow_all", "shadow_passed", "smt_passed",
                            "shadow_all_gates", "virtual_all", "symbol_breakdown", "health_metrics",
                        )
                    }
                    metrics["shadow_all"] = _scalar_stub
                    metrics["shadow_passed"] = _scalar_stub
                    metrics["smt_passed"] = _scalar_stub
                    metrics["shadow_all_gates"] = _scalar_stub
                    metrics["virtual_all"] = _scalar_stub

            total_trades = int(metrics.get("total_trades", 0))
            self._send_report(src, sym, metrics, window_seconds=window_seconds, report_type="REAL")
        except Exception as e:
            logger.error(f"❌ Ошибка при формировании/отправке отчета для {src}/{sym}: {e}", exc_info=True)

    # ----------------------------
    # WINDOW metrics from multiple sources
    # ----------------------------
    def _gather_window_metrics_stream(self, source: str, symbol: str, window_seconds: int | None = None) -> dict[str, any]:
        """
        Собирает метрики из нескольких источников:
        1. trades:closed stream
        2. closed:{strategy}:{symbol}:{tf}:{source} lists
        3. order:{id} hashes (fallback)
        """
        from domain.normalizers import strategy_from_source

        target_window = window_seconds if window_seconds is not None else RECENT_WINDOW_SECONDS
        # Если window_seconds задан, игнорируем trade_window_count
        trade_window_count = (TRADE_WINDOW_COUNT if TRADE_WINDOW_COUNT > 0 else None) if window_seconds is None else None

        cutoff_ms = 0 if trade_window_count else get_ny_time_millis() - target_window * 1000
        min_id = f"{cutoff_ms}-0"

        m = self.tm.new_metrics()

        def reached_limit() -> bool:
            return bool(trade_window_count) and m["total_trades"] >= trade_window_count

        processed_order_ids = set()  # дедупликация

        # -------------------------
        # 0) Fast path via ZSET index
        # -------------------------
        symbol_breakdown = {}
        if symbol == "ALL":
             # Initialize if needed
             pass

        def _acc_sym(t_data: dict):
             if symbol == "ALL":
                 s = canon_symbol(t_data.get("symbol") or "")
                 if s:
                     if s not in symbol_breakdown:
                         symbol_breakdown[s] = {"pnl": 0.0, "trades": 0}
                     pv = float(t_data.get("pnl_net") or t_data.get("pnl") or 0.0)
                     symbol_breakdown[s]["pnl"] += pv
                     symbol_breakdown[s]["trades"] += 1

        # Если:
        #   - есть окно по времени (trade_window_count выключен)
        #   - включён ZSET индекс (TRADES_CLOSED_ZSET_INDEX=1)
        #   - разрешили use_zset
        # Тогда берём order_id из ZSET и читаем order hash.
        #
        # Это:
        #   - стабильнее, чем скан XREVRANGE по min_id,
        #   - быстрее при больших stream'ах,
        #   - идеально сочетается с compact-stream.
        if (
            (not trade_window_count)
            and PERIODIC_REPORT_USE_ZSET
            and (os.getenv("TRADES_CLOSED_ZSET_INDEX","0").strip().lower() in ("1","true","yes","on"))
        ):
            try:
                strategy = strategy_from_source(source)
                now_ms = get_ny_time_millis()
                oids: list[str] = []
                for tf in self._candidate_tfs():
                    oids.extend(self.repo.get_closed_by_time(strategy, symbol, tf, source, from_ts_ms=cutoff_ms, to_ts_ms=now_ms, limit=RECENT_LIMIT, desc=True))
                    if len(oids) >= RECENT_LIMIT:
                        break
                # uniq preserve order
                seen = set()
                uniq = []
                for oid in oids:
                    if oid and oid not in seen:
                        seen.add(oid)
                        uniq.append(oid)
                # Hydrate: просто HGETALL(order:{id}), batch через pipeline.
                # hydrate_trade_closed_batch ожидает rows "как из stream", но умеет работать и с dict с order_id.
                rows = [{"order_id": oid} for oid in uniq[:RECENT_LIMIT]]
                hydrated = hydrate_trade_closed_batch(self.redis, rows, require_closed=True, merge_precedence="hash")
                for t in hydrated:
                    oid = t.get("order_id") or ""
                    if not oid or oid in processed_order_ids:
                        continue
                    # фильтры source/symbol
                    t_source = canon_source(t.get("strategy") or t.get("source") or "")
                    t_strategy = canon_strategy(t.get("strategy") or "")
                    req_strategy = canon_strategy(strategy_from_source(source))
                    req_source = canon_source(source)
                    t_symbol = canon_symbol(t.get("symbol") or "")

                    if t_strategy != req_strategy:
                        continue
                    if t_source != req_source and t_source not in ("binance", "bybit", "mt5", "binance_real", "binance_paper"):
                        continue
                    if symbol == "ALL":
                        if "XAU" in t_symbol or "GOLD" in t_symbol:
                            continue
                    elif t_symbol != symbol:
                        continue
                    self._accumulate_trade_metrics(m, t)
                    _acc_sym(t)
                    processed_order_ids.add(oid)
                    if reached_limit():
                        break
                # если что-то собрали — возвращаем, не трогаем stream/lists
                if m["total_trades"] > 0:
                    self.tm.finalize(m)
                    self._add_health_metrics(m, source, symbol)
                    if symbol == "ALL" and symbol_breakdown:
                        m["symbol_breakdown"] = symbol_breakdown
                    return m
            except Exception as e:
                logger.debug(f"⚠️ ZSET fast-path failed: {e}")

        # 0) ZSET path (самый быстрый и точный по времени)
        strategy = strategy_from_source(source)
        # ВАЖНО: сделки могут быть в разных TF (хоть tick доминирует).
        # Поэтому собираем кандидатов из ZSET по каждому TF и мерджим.
        # Дедуп идёт по order_id.
        tfs = self._candidate_tfs()

        if PERIODIC_REPORT_USE_ZSET and _env_bool("ENABLE_CLOSED_ZSET_INDEX", default=False) and not reached_limit():
            z_total = 0
            z_used = 0
            for tf in tfs:
                zkey = _closed_zkey(strategy, symbol, tf, source)
                try:
                    if trade_window_count:
                        ids = self.redis.zrevrange(zkey, 0, min(trade_window_count - 1, RECENT_LIMIT - 1)) or []
                    else:
                        now_ms = get_ny_time_millis()
                        ids = self.redis.zrevrangebyscore(zkey, now_ms, cutoff_ms, start=0, num=RECENT_LIMIT) or []
                except Exception:
                    ids = []

                if not ids:
                    continue
                z_total += len(ids)

                # pipeline HGETALL для скорости
                p = self.redis.pipeline(transaction=False)
                oids: list[str] = []
                for oid_raw in ids:
                    oid = str(oid_raw) if not isinstance(oid_raw, bytes) else oid_raw.decode('utf-8')
                    if not oid or oid in processed_order_ids:
                        continue
                    oids.append(oid)
                    p.hgetall(f"order:{oid}")
                rows = p.execute() or []

                for oid, row in zip(oids, rows):
                    if not row:
                        continue
                    order_data = _norm_map(row)
                    # статус/время
                    status = (order_data.get("status") or "").lower()
                    if status != "closed":
                        continue
                    closed_ts_raw = _si(order_data.get("closed_time") or order_data.get("exit_ts_ms") or order_data.get("close_time") or 0)
                    closed_ts = _normalize_ts_ms(closed_ts_raw)
                    if closed_ts > 0 and closed_ts < cutoff_ms:
                        continue

                    # source/symbol фильтр (с fallback на strategy)
                    o_source = canon_source(order_data.get("strategy") or order_data.get("source") or "")
                    o_symbol = canon_symbol(order_data.get("symbol") or "")
                    if o_source != source:
                        continue
                    if symbol == "ALL":
                        if "XAU" in o_symbol or "GOLD" in o_symbol:
                            continue
                    elif o_symbol != symbol:
                        continue

                    if not self._is_trade_virtual(order_data):
                        self._accumulate_trade_metrics(m, order_data)
                        _acc_sym(order_data)
                    processed_order_ids.add(oid)
                    z_used += 1
                    if reached_limit():
                        break

                if reached_limit():
                    break

            logger.debug(f"📊 ZSET window: total_ids={z_total}, used={z_used} for {source}/{symbol}")

        # 1) trades:closed stream
        entries = []
        try:
            _stream_limit = max(RECENT_LIMIT, 50000)
            entries = self.redis.xrevrange(RS.TRADES_CLOSED, max="+", min=min_id, count=_stream_limit) or []
            logger.debug(f"📊 trades:closed stream: найдено {len(entries)} записей (окно: {RECENT_WINDOW_SECONDS}s)")
        except Exception as e:
            logger.debug(f"⚠️ Ошибка чтения trades:closed: {e}")
            entries = []

        # Важно: hydrate делаем batch'ем, чтобы:
        #   - в compact режиме выполнить HGETALL пачкой через pipeline,
        #   - в non-compact режиме почти всегда обойтись без hash.
        rows = []
        for _, fields in entries:
            rows.append(fields or {})
        hydrated = hydrate_trade_closed_batch(self.redis, rows, require_closed=False, merge_precedence="hash")

        matched_count = 0
        for t in hydrated:
            order_id = t.get("order_id") or t.get("id") or ""

            if order_id in processed_order_ids:
                continue

            raw_source = t.get("source") or ""
            raw_strategy = t.get("strategy") or ""
            t_source = canon_source(raw_strategy or raw_source or "")
            t_symbol = canon_symbol(t.get("symbol") or "")

            if t_source != source:
                continue
            if symbol == "ALL":
                if "XAU" in t_symbol or "GOLD" in t_symbol:
                    continue
            elif t_symbol != symbol:
                continue

            matched_count += 1

            closed_ts_raw = _si(t.get("closed_time") or t.get("exit_ts_ms") or t.get("close_time") or 0)
            closed_ts = _normalize_ts_ms(closed_ts_raw)
            if closed_ts > 0 and closed_ts < cutoff_ms:
                continue

            # final-close only (опционально, если поле есть)
            is_final = (t.get("is_final_close") or "1")
            if is_final not in ("1", "true", "True"):
                continue

            # Use the proper accumulation method that handles TRAILING_PROFIT as TP
            if not self._is_trade_virtual(t):
                self._accumulate_trade_metrics(m, t)
                _acc_sym(t)

            if order_id:
                processed_order_ids.add(order_id)
            if reached_limit():
                break

        # 2) closed:{strategy}:{symbol}:{tf}:{source} lists
        strategy = strategy_from_source(source)
        # Читаем по списку TF (канон), но каждый TF расширяем через tf_variants()
        # чтобы покрыть старые ключи ("m1"/"m5") и новые ("1m"/"5m").

        if not reached_limit():
            for tf in self._candidate_tfs():
                # читаем из нескольких tf-ключей (канон + легаси)
                for tfk in tf_variants(tf):
                    list_key = f"closed:{strategy}:{symbol}:{tfk}:{source}"
                    try:
                        order_ids = self.redis.lrange(list_key, -RECENT_LIMIT, -1) or []
                        logger.debug(f"📊 closed list {list_key}: найдено {len(order_ids)} order IDs")

                        for oid_raw in order_ids:
                            oid = str(oid_raw) if not isinstance(oid_raw, bytes) else oid_raw.decode('utf-8')
                            if not oid or oid in processed_order_ids:
                                continue

                            order_key = f"order:{oid}"
                            order_data_raw = self.redis.hgetall(order_key) or {}
                            order_data = _norm_map(order_data_raw)

                            if not order_data:
                                continue

                            # Проверка статуса и времени (нормализуем timestamp)
                            status = (order_data.get("status") or "").lower()
                            if status != "closed":
                                continue

                            closed_ts_raw = _si(order_data.get("closed_time") or order_data.get("close_time") or 0)
                            closed_ts = _normalize_ts_ms(closed_ts_raw)
                            if closed_ts > 0 and closed_ts < cutoff_ms:
                                continue

                            # Проверка source и symbol (с fallback на strategy)
                            o_source = canon_source(order_data.get("strategy") or order_data.get("source") or "")
                            o_symbol = canon_symbol(order_data.get("symbol") or "")

                            if o_source != source:
                                continue
                            if symbol == "ALL":
                                if "XAU" in o_symbol or "GOLD" in o_symbol:
                                    continue
                            elif o_symbol != symbol:
                                continue

                            # Use the proper accumulation method that handles TRAILING_PROFIT as TP
                            if not self._is_trade_virtual(order_data):
                                self._accumulate_trade_metrics(m, order_data)
                                _acc_sym(order_data)

                            processed_order_ids.add(oid)
                            if reached_limit():
                                break

                        if reached_limit():
                            break
                    except Exception as e:
                        logger.debug(f"⚠️ Ошибка чтения closed list {list_key}: {e}")
                if reached_limit():
                    break

        # 3) Fallback: closed:{strategy}:{symbol}:{tf} (без source)
        if m["total_trades"] == 0 and not reached_limit():
            for tf in self._candidate_tfs():
                for tfk in tf_variants(tf):
                    list_key = f"closed:{strategy}:{symbol}:{tfk}"
                    try:
                        order_ids = self.redis.lrange(list_key, -RECENT_LIMIT, -1) or []

                        for oid_raw in order_ids:
                            oid = str(oid_raw) if not isinstance(oid_raw, bytes) else oid_raw.decode('utf-8')
                            if not oid or oid in processed_order_ids:
                                continue

                            order_key = f"order:{oid}"
                            order_data_raw = self.redis.hgetall(order_key) or {}
                            order_data = _norm_map(order_data_raw)

                            if not order_data:
                                continue

                            status = (order_data.get("status") or "").lower()
                            if status != "closed":
                                continue

                            closed_ts_raw = _si(order_data.get("closed_time") or order_data.get("close_time") or 0)
                            closed_ts = _normalize_ts_ms(closed_ts_raw)
                            if closed_ts > 0 and closed_ts < cutoff_ms:
                                continue

                            o_source = canon_source(order_data.get("strategy") or order_data.get("source") or "")
                            o_symbol = canon_symbol(order_data.get("symbol") or "")

                            if o_source != source:
                                continue
                            if symbol == "ALL":
                                if "XAU" in o_symbol or "GOLD" in o_symbol:
                                    continue
                            elif o_symbol != symbol:
                                continue

                            # Use the proper accumulation method that handles TRAILING_PROFIT as TP
                            if not self._is_trade_virtual(order_data):
                                self._accumulate_trade_metrics(m, order_data)
                                _acc_sym(order_data)

                            processed_order_ids.add(str(oid))
                            if reached_limit():
                                break

                        if reached_limit():
                            break
                    except Exception:
                        continue
                if reached_limit():
                    break

        if symbol == "ALL" and symbol_breakdown:
            m["symbol_breakdown"] = symbol_breakdown

        window_label = f"{trade_window_count} trades" if trade_window_count else f"{target_window}s"
        sampled_info(
            logger,
            "PERIODIC_REPORTER_SUMMARY",
            f"📊 Итого собрано {m['total_trades']} сделок для {source}/{symbol} "
            f"(окно {window_label}, matched={matched_count} из {len(entries)}, processed_order_ids={len(processed_order_ids)})"
        )

        # Диагностика: если сделок нет, логируем больше информации
        if m['total_trades'] == 0:
            sampled_warning(
                logger,
                "PERIODIC_REPORTER_NO_TRADES",
                f"⚠️ Нет сделок для {source}/{symbol} в окне {window_label}. "
                f"Проверьте: trades:closed stream (найдено {len(entries)} записей, matched={matched_count}), "
                f"closed lists, order:* hashes. "
                f"Убедитесь, что source={source} и symbol={symbol} корректны."
            )

        self.tm.finalize(m)

        # Add health metrics for the symbol
        self._add_health_metrics(m, source, symbol)

        return m

    def _get_gate_diagnostics(self, source: str, symbol: str) -> list[str]:
        """
        Анализирует последний replay файл для диагностики gate.
        Возвращает список строк для добавления в отчет.
        """
        try:
            # Ищем последний replay файл для данного символа
            out_dir = os.getenv("OUT_DIR", "/var/lib/trade/of_reports/out")
            if not os.path.exists(out_dir):
                return []

            # Ищем последний nightly run (не meta)
            import glob
            pattern = os.path.join(out_dir, "nightly_*")
            runs = sorted([d for d in glob.glob(pattern) if os.path.isdir(d) and "meta" not in d], reverse=True)

            if not runs:
                return []

            latest_run = runs[0]
            replay_file = os.path.join(latest_run, "of_replay_engine.ndjson")
            inputs_file = os.path.join(latest_run, "of_inputs_canary.ndjson")

            if not os.path.exists(replay_file):
                return []

            # Анализируем replay
            from collections import Counter

            rows = []
            with open(replay_file, encoding="utf-8") as f:
                for line in f:
                    if line.strip():
                        try:
                            rows.append(json.loads(line))
                        except Exception:
                            continue

            if not rows:
                return []

            # Фильтруем по символу, если указан
            if symbol and symbol != "ALL":
                rows = [r for r in rows if (r.get("symbol", "")).upper() == symbol.upper()]

            if not rows:
                return []

            # Статистика
            total = len(rows)
            ok1 = sum(1 for r in rows if r.get("ok") == 1)
            ok0 = total - ok1

            scores = [r.get("score", 0.0) for r in rows if "score" in r]
            have_need = [(r.get("have", 0), r.get("need", 0)) for r in rows]
            have_need_ok = sum(1 for h, n in have_need if h >= n and n > 0)

            # Причины блокировки
            ok0_rows = [r for r in rows if r.get("ok") == 0]
            reasons = Counter([r.get("reason", "unknown")[:50] for r in ok0_rows[:200]])
            top_reasons = reasons.most_common(5)

            # Конфигурация из inputs
            score_min = 0.65  # default
            if os.path.exists(inputs_file):
                try:
                    with open(inputs_file, encoding="utf-8") as f:
                        sample = json.loads(f.readline())
                        cfg = sample.get("cfg", {})
                        if isinstance(cfg, dict):
                            score_min = float(cfg.get("of_score_min", 0.65))
                except Exception:
                    pass

            # Score distribution
            scores_above_threshold = sum(1 for s in scores if s >= score_min) if scores else 0

            # Формируем строки отчета
            lines = [
                "",
                "<b>🔍 Gate Diagnostics (Replay Analysis)</b>",
                f"Replay: <b>{os.path.basename(latest_run)}</b>",
                f"Total rows: <b>{total}</b> | ok=1: <b>{ok1}</b> ({ok1/total*100:.1f}%) | ok=0: <b>{ok0}</b> ({ok0/total*100:.1f}%)",
            ]

            if scores:
                lines.append(
                    f"Score >= {score_min:.2f}: <b>{scores_above_threshold}</b>/{len(scores)} "
                    f"({scores_above_threshold/len(scores)*100:.1f}%) | "
                    f"Range: <b>{min(scores):.3f}</b> - <b>{max(scores):.3f}</b> | "
                    f"Mean: <b>{sum(scores)/len(scores):.3f}</b>"
                )

            lines.append(
                f"Have >= Need: <b>{have_need_ok}</b>/{len(have_need)} "
                f"({have_need_ok/len(have_need)*100:.1f}%)"
            )

            lines.append(f"Config of_score_min: <b>{score_min:.3f}</b>")

            if top_reasons:
                reasons_str = ", ".join([f"{reason}({count})" for reason, count in top_reasons])
                lines.append(f"Top ok=0 reasons: <b>{reasons_str}</b>")

            return lines

        except Exception as e:
            logger.debug(f"⚠️ Gate diagnostics error: {e}")
            return []

    def _accumulate_trade_metrics(self, m: dict[str, any], t: dict[str, str]) -> bool:
        # --- bucket for strict stats (with TRAILING_PROFIT special case) ---
        raw_reason = (
            t.get("close_reason_raw")
            or t.get("close_reason")
            or t.get("close_reason_norm")
            or ""
        )

        rr = str(raw_reason).strip().upper().replace(" ", "_")
        if rr == "TRAILING_PROFIT":
            bucket = "TRAIL_SL"
            t_normalized = dict(t)
            t_normalized["close_reason_raw"] = "TRAILING_PROFIT"
        else:
            bucket = bucket_close_reason(raw_reason)
            t_normalized = dict(t)

        if bucket == "UNKNOWN":
            sampled_warning(logger, "UNKNOWN_CLOSE_REASON", f"⚠️ Trade {t.get('order_id')} has UNKNOWN close_reason (raw={raw_reason}). Check PositionState preservation.")

        # Ensure the normalized bucket is passed to TradeMetricsService
        t_normalized["close_reason"] = bucket
        # Ensure raw reason is preserved if not already present
        if "close_reason_raw" not in t_normalized:
            t_normalized["close_reason_raw"] = raw_reason

        # --- TradeMetricsService handles all basic aggregation ---
        if not self.tm.accumulate_trade(m, t_normalized):
            return False

        # --- STRICT W/L/BE по bucket + sign(pnl) ---
        pnl_val = float(t.get("pnl_net") or t.get("pnl") or 0.0)
        eps = 1e-9

        if bucket == "TP_LIMIT" or bucket == "TP":  # Added TP for consistency
            if pnl_val > eps:
                m["wins_strict"] += 1
            elif pnl_val < -eps:
                m["losses_strict"] += 1
            else:
                m["breakeven_strict"] += 1
        elif bucket == "TRAIL_SL":
            if pnl_val > eps:
                m["wins_strict"] += 1
            else:
                m["losses_strict"] += 1
        elif bucket == "INITIAL_SL":
            m["losses_strict"] += 1
        else:
            # TIMEOUT, MANUAL, UNKNOWN -> use PnL sign instead of blindly BE
            # This aligns "Strict" stats better with Reality while preserving Reason buckets
            if pnl_val > eps:
                m["wins_strict"] += 1
            elif pnl_val < -eps:
                m["losses_strict"] += 1
            else:
                m["breakeven_strict"] += 1

        if os.getenv("PERIODIC_REPORT_DEBUG_STRICT", "false").lower() == "true":
            logger.debug(
                f"DEBUG_STRICT: id={t.get('order_id')} bucket={bucket} pnl={pnl_val:.4f} "
                f"strict={m['wins_strict']}/{m['losses_strict']}/{m['breakeven_strict']}"
            )

        return True

    def _calculate_session_pnl_breakdown(self, trades: list[dict[str, str]], window_hours: int = 24) -> dict[str, dict[str, any]]:
        """
        Рассчитывает разбивку PnL по торговым сессиям за последние N часов.

        Args:
            trades: Список сделок с полями exit_ts_ms, pnl_net
            window_hours: Окно анализа в часах (по умолчанию 24)

        Returns:
            Dict с сессиями: "asia", "london", "nyc"
            Каждая сессия содержит: {"profit": float, "loss": float, "net": float, "trades": int}
        """
        from datetime import datetime

        if not trades:
            return {}

        now_ms = get_ny_time_millis()
        cutoff_ms = now_ms - (window_hours * 3600 * 1000)

        # Инициализация сессий
        sessions = {
            "asia": {"profit": 0.0, "loss": 0.0, "net": 0.0, "trades": 0},    # 00:00-08:00 UTC
            "london": {"profit": 0.0, "loss": 0.0, "net": 0.0, "trades": 0},  # 08:00-16:00 UTC
            "nyc": {"profit": 0.0, "loss": 0.0, "net": 0.0, "trades": 0}       # 16:00-00:00 UTC
        }

        def get_session_for_timestamp(ts_ms: int) -> str:
            """Определяет торговую сессию для временной метки"""
            dt = datetime.fromtimestamp(ts_ms / 1000, tz=UTC)
            hour = dt.hour

            if 0 <= hour < 8:
                return "asia"
            elif 8 <= hour < 16:
                return "london"
            else:  # 16 <= hour < 24
                return "nyc"

        # Группировка сделок по сессиям
        for t in trades:
            try:
                exit_ts_raw = t.get("exit_ts_ms") or t.get("closed_time") or t.get("close_time")
                if not exit_ts_raw:
                    continue

                exit_ts = _normalize_ts_ms(_si(exit_ts_raw))
                if exit_ts < cutoff_ms:
                    continue

                # Определяем сессию по UTC времени выхода из сделки
                session = get_session_for_timestamp(exit_ts)

                # Получаем PnL
                pnl = float(t.get("pnl_net") or t.get("pnl") or 0.0)

                sessions[session]["trades"] += 1
                sessions[session]["net"] += pnl

                if pnl > 0:
                    sessions[session]["profit"] += pnl
                else:
                    sessions[session]["loss"] += abs(pnl)

            except Exception as e:
                logger.debug(f"⚠️ Ошибка обработки сделки в session breakdown: {e}")
                continue

        return sessions

    def _add_health_metrics(self, m: dict[str, any], source: str, symbol: str) -> None:
        """
        Добавляет health metrics для symbol в отчет.
        """
        try:
            if symbol == "ALL":
                # For aggregated report, specific L2 health is not applicable/aggregated yet.
                m["health_metrics"] = {}
                return

            # Получаем health snapshot для символа
            health_snapshot_key = f"orderflow:{symbol}:health_snapshot"
            health_snapshot = self.redis.hgetall(health_snapshot_key) or {}

            # Получаем текущие rate метрики
            l2_stale_tick_key = f"orderflow:{symbol}:l2_stale_ratio_tick"
            l2_stale_now_key = f"orderflow:{symbol}:l2_stale_ratio_now"
            signal_rate_key = f"orderflow:{symbol}:signal_emit_rate"
            dlq_rate_key = f"orderflow:{symbol}:dlq_rate"

            l2_stale_ratio_tick = self.redis.get(l2_stale_tick_key)
            l2_stale_ratio_now = self.redis.get(l2_stale_now_key)
            signal_emit_rate = self.redis.get(signal_rate_key)
            dlq_rate = self.redis.get(dlq_rate_key)

            # Добавляем в метрики
            health_metrics = {
                "health_snapshot": health_snapshot,
                "l2_stale_ratio_tick": float(l2_stale_ratio_tick or 0.0),
                "l2_stale_ratio_now": float(l2_stale_ratio_now or 0.0),
                "signal_emit_rate": float(signal_emit_rate or 0.0),
                "dlq_rate": float(dlq_rate or 0.0),
            }

            # Извлекаем ключевые метрики из snapshot для удобства
            if health_snapshot:
                health_metrics.update({
                    "avg_l2_age_ms": float(health_snapshot.get("avg_l2_age_ms", 0.0)),
                    "avg_l2_age_tick_ms": float(health_snapshot.get("avg_l2_age_tick_ms", 0.0)),
                    "ticks_total": int(health_snapshot.get("ticks_total", 0)),
                    "ticks_with_l2": int(health_snapshot.get("ticks_with_l2", 0)),
                })

            m["health_metrics"] = health_metrics

            logger.debug(f"✅ Добавлены health metrics для {source}/{symbol}: {len(health_snapshot)} полей в snapshot")

        except Exception as e:
            logger.warning(f"⚠️ Ошибка получения health metrics для {source}/{symbol}: {e}")
            m["health_metrics"] = {}

    # ----------------------------
    # Telegram report
    # ----------------------------
    def _send_report(self, source: str, symbol: str, m: dict[str, any], window_seconds: int | None = None, report_type: str = "REAL") -> None:
        try:
            total = int(m.get("total_trades", 0))
            send_empty = os.getenv("PERIODIC_REPORT_SEND_EMPTY", "false").lower() == "true"

            sampled_info(
                logger,
                "PERIODIC_REPORTER_SEND_REPORT",
                f"📨 _send_report вызван для {source}/{symbol} ({report_type}): total={total}, send_empty={send_empty}"
            )

            # Guard: не отправляем пустые отчеты (по просьбе пользователя)
            # Если это не плановый отчет (window_seconds=3600 или 86400),
            # то проверяем минимальное количество сделок.
            is_scheduled = window_seconds in (3600, 86400)
            if total <= 0 and not send_empty:
                logger.debug(f"⏭️ Пропуск пустого отчета для {source}/{symbol} (total=0)")
                return

            if not is_scheduled and total < REPORT_TRIGGER_COUNT and REPORT_TRIGGER_COUNT > 0:
                logger.debug(f"⏭️ Пропуск отчета для {source}/{symbol} (всего {total} сделок, нужно {REPORT_TRIGGER_COUNT})")
                return

            # Guard: не отправляем отчеты с недостаточным количеством сделок
            # Если это scheduled report (window_seconds задан), то отправляем даже малое кол-во сделок (если > 0)
            # Если это count-based trigger, то уважаем лимит.
            if window_seconds is not None:
                min_trades_for_report = 0 if send_empty else 1
            else:
                min_trades_for_report = int(os.getenv("PERIODIC_REPORT_MIN_TRADES", str(REPORT_TRIGGER_COUNT)))

            if total < min_trades_for_report:
                sampled_info(
                    logger,
                    "PERIODIC_REPORTER_SKIP_INSUFFICIENT",
                    f"⏭️ Пропуск отчета для {source}/{symbol}: недостаточно сделок ({total} < {min_trades_for_report})"
                )
                return

            trade_window_count = TRADE_WINDOW_COUNT if TRADE_WINDOW_COUNT > 0 and window_seconds is None else None
            effective_window_sec = window_seconds if window_seconds is not None else RECENT_WINDOW_SECONDS
            window_minutes = max(1, int(effective_window_sec // 60))

            # Показываем ожидаемое окно сделок (поскольку отчеты отправляются только при достаточном количестве)
            window_label = (
                f"последние <b>{trade_window_count}</b> сделок"
                if trade_window_count else
                f"последние <b>{window_minutes}</b> мин"
            )
            now_utc = datetime.now(UTC).strftime("%Y-%m-%d %H:%M UTC")

            # Анализ trailing vs baseline (если включен и доступны функции, и пришло время)
            trailing_vs_baseline_results = None
            pair_key = f"{source}:{symbol}"
            self.report_counter[pair_key] = self.report_counter.get(pair_key, 0) + 1

            should_analyze_trailing_vs_baseline = (self.report_counter[pair_key] % self.trailing_vs_baseline_reports_interval) == 0

            if (TRAILING_VS_BASELINE_ENABLED and should_analyze_trailing_vs_baseline and
                analyze_global and analyze_by_tag and analyze_by_strong_gate and load_trades_from_postgres and
                total >= int(os.getenv("PERIODIC_REPORT_MIN_TRADES_FOR_TRAILING_ANALYSIS", "1"))):

                try:
                    logger.debug(f"🎯 Выполнение анализа trailing vs baseline для {source}/{symbol}")

                    # Загружаем сделки из PostgreSQL (если доступно соединение)
                    # Используем те же параметры, что и в основном отчете
                    psycopg2_conn = None
                    try:
                        import psycopg2
                        dsn = (os.getenv("ANALYTICS_DB_DSN") or os.getenv("TRADES_DB_DSN")) or (os.getenv("ANALYTICS_DB_DSN") or os.getenv("DATABASE_URL")) or (os.getenv("ANALYTICS_DB_DSN") or os.getenv("POSTGRES_DSN"))
                        if dsn:
                            psycopg2_conn = psycopg2.connect(dsn)

                            trades = load_trades_from_postgres(
                                conn=psycopg2_conn,
                                source=source,
                                symbol=symbol,
                                limit=min(total, 500),  # ограничиваем для производительности
                                since_days=None
                            )

                            if len(trades) >= 1:  # минимум 1 сделок для анализа
                                global_analysis = analyze_global(trades)
                                tag_analysis = analyze_by_tag(trades, min_trades=1)
                                strong_gate_analysis = analyze_by_strong_gate(trades, min_trades=5)  # NEW

                                trailing_vs_baseline_results = {
                                    "global": global_analysis,
                                    "by_tag": tag_analysis[:3],  # топ 3 тега
                                    "by_strong_gate": strong_gate_analysis,  # NEW: Strong/Weak разбивка
                                    "total_trades_analyzed": len(trades)
                                }

                                logger.info(f"✅ Анализ trailing vs baseline выполнен для {source}/{symbol}: {len(trades)} сделок")
                            else:
                                logger.debug(f"⏭️ Недостаточно сделок для анализа trailing vs baseline: {len(trades)} < 20")
                        else:
                            logger.debug("⏭️ DSN для PostgreSQL не настроен, пропуск анализа trailing vs baseline")
                    except ImportError:
                        logger.debug("⏭️ psycopg2 не доступен, пропуск анализа trailing vs baseline")
                    except Exception as e:
                        logger.warning(f"⚠️ Ошибка при анализе trailing vs baseline для {source}/{symbol}: {e}")
                    finally:
                        if psycopg2_conn:
                            psycopg2_conn.close()

                except Exception as e:
                    logger.warning(f"⚠️ Критическая ошибка при анализе trailing vs baseline для {source}/{symbol}: {e}")

            wins = int(m["wins"]); losses = int(m["losses"]); be = int(m["breakeven"])
            ws = int(m["wins_strict"]); ls = int(m["losses_strict"]); bs = int(m["breakeven_strict"])

            total_pnl = float(m["total_pnl"])
            if total > 0:
                avg_pnl = total_pnl / total
                avg_dur_s = (float(m["sum_duration_ms"]) / total) / 1000.0
            else:
                avg_pnl = 0.0
                avg_dur_s = 0.0

            # Use Net Return (mean_ret) for Avg P/L % to align with Net PnL basis
            # mean_ret is ratio (e.g. 0.01), so multiply by 100 for %
            avg_pct = float(m.get("mean_ret", 0.0)) * 100.0
            fees = float(m["total_fees"])
            total_pnl_gross = float(m.get("total_pnl_gross", 0.0))
            total_notional = float(m.get("total_notional_usd", 0.0))

            gross_profit = float(m["gross_profit"])
            gross_loss = float(m["gross_loss"])

            # Диагностика: проверяем соотношение P/L net и fees
            # User Req 3: "pnl_net artificially lowered?" check
            # We trust TradeMetricsService accumulation which usually does pnl_gross = (exit-entry)*lot.

            pnl_gross_calc = total_pnl + fees  # Expected 'gross' if 'total_pnl' is truly net

            # If total_pnl_gross (from source) is approx total_pnl, it means likely source 'pnl_gross' was polluted with net value.
            # But normally we accumulate distinct fields.

            fees_ratio = abs(fees / total_pnl) if abs(total_pnl) > EPS else 0.0

            # Invariant check for audit log (invisible to user unless warn)
            if abs(total_pnl_gross - pnl_gross_calc) > 1.0 and abs(total_pnl_gross) > 10.0:
                 # Discrepancy: Collected Gross != Calc Gross
                 # This implies either:
                 # 1. pnl_gross in DB is same as pnl_net (already net)
                 # 2. significant math drift
                 pass

            gross_profit = float(m["gross_profit"])
            if gross_loss > EPS:
                pf = gross_profit / gross_loss
                pf_str = f"{pf:.2f}"
            elif gross_profit > EPS:
                pf_str = "inf"
            else:
                pf_str = "0.00"

            wr = safe_div(wins, total) * 100.0
            wrs = safe_div(ws, total) * 100.0

            reasons = m.get("reasons") or {}
            top = sorted(reasons.items(), key=lambda kv: kv[1], reverse=True)[:4]
            # Экранируем HTML специальные символы в top_str
            import html
            top_str = ", ".join(f"{html.escape(str(k))}:{v}" for k, v in top)

            min_pnl = m.get("min_pnl")
            max_pnl = m.get("max_pnl")
            min_pnl_str = f"{min_pnl:.2f}" if min_pnl != float("inf") else "N/A"
            max_pnl_str = f"{max_pnl:.2f}" if max_pnl != float("-inf") else "N/A"

            # Trailing метрики: объединяем legacy и TradeMetricsService
            trailing_started_legacy = int(m.get("trailing_started", 0))
            trailing_started_count = int(m.get("trailing_started_count", 0))  # из TradeMetricsService
            trailing_started = max(trailing_started_legacy, trailing_started_count)  # берем максимум

            trailing_stop_hits_legacy = int(m.get("trailing_stop_hits", 0))
            trailing_stop_hits_tm = int(m.get("trailing_stop_hits", 0))  # из TradeMetricsService (то же поле)
            trailing_stop_hits = max(trailing_stop_hits_legacy, trailing_stop_hits_tm)

            trailing_profit_hits = int(m.get("trailing_profit_hits", 0))  # только из TradeMetricsService
            closed_by_trail = int(m.get("closed_by_trail", 0))

            symbol_trailing_enabled = self._symbol_trailing_enabled(symbol)
            effective_trailing_enabled = (
                symbol_trailing_enabled
                if symbol_trailing_enabled is not None
                else TRAILING_SECTION_ENABLED
            )
            profiles = m.get("trailing_profiles") or {}
            show_trailing_section = effective_trailing_enabled and (trailing_started > 0 or closed_by_trail > 0 or bool(profiles))

            expectancy_r = float(m.get("expectancy_r", 0.0))
            std_r = float(m.get("std_r", 0.0))
            payoff_r = float(m.get("payoff_r", 0.0))
            kelly = float(m.get("kelly_f_r", 0.0))

            sharpe_like = float(m.get("sharpe_like_trades", 0.0))
            sortino_like = float(m.get("sortino_like_trades", 0.0))
            mdd = float(m.get("max_drawdown_usd", 0.0))
            max_w_streak = int(m.get("max_consecutive_wins", 0))
            max_l_streak = int(m.get("max_consecutive_losses", 0))

            exit_eff = float(m.get("exit_eff_avg_win", 0.0))
            giveback_avg = float(m.get("giveback_ratio_avg_win", 0.0))
            missed_avg = float(m.get("missed_profit_ratio_avg", 0.0))

            pf_net = m.get("profit_factor_net", 0.0)
            pf_net_num = float(pf_net) if isinstance(pf_net, (int, float)) else 0.0
            pf_net_str = (
                f"{pf_net_num:.2f}" if math.isfinite(pf_net_num)
                else ("inf" if pf_net == float("inf") else "0.00")
            )
            exp_r = float(m.get("expectancy_r", 0.0))
            med_r = float(m.get("median_r", 0.0))
            trim_r = float(m.get("trimmed_mean_r", 0.0))

            sh = float(m.get("sharpe_like_trades", 0.0))
            so = float(m.get("sortino_like_trades", 0.0))
            mdd_net = float(m.get("max_drawdown_usd", 0.0))
            calmar = m.get("calmar_like_ret", 0.0)
            calmar_num = float(calmar) if isinstance(calmar, (int, float)) else 0.0
            calmar_str = f"{calmar_num:.2f}" if math.isfinite(calmar_num) else ("inf" if calmar == float("inf") else "0.00")

            iqr_ret = float(m.get("iqr_ret", 0.0))
            mad_ret_s = float(m.get("mad_ret_scaled", 0.0))

            min_es = int(os.getenv("PERIODIC_REPORT_MIN_TRADES_FOR_ES", "20"))

            bad_ts_sec = int(m.get("bad_ts_sec", 0))
            bad_ts_us = int(m.get("bad_ts_us", 0))
            neg_dur = int(m.get("negative_duration_count", 0))
            bad_time = int(m.get("bad_time", 0))
            tp_zero = int(m.get("tp_hit_but_zero_pnl", 0))
            cr_incon = int(m.get("close_reason_inconsistent_with_pnl_sign", 0))
            fees_huge = int(m.get("fees_huge_count", 0))

            def rate(x: int) -> float:
                return safe_div(x, total) * 100.0

            # Извлекаем health metrics
            health_metrics = m.get("health_metrics", {})
            l2_stale_ratio_tick = float(health_metrics.get("l2_stale_ratio_tick", 0.0))
            l2_stale_ratio_now = float(health_metrics.get("l2_stale_ratio_now", 0.0))
            signal_emit_rate = float(health_metrics.get("signal_emit_rate", 0.0))
            dlq_rate = float(health_metrics.get("dlq_rate", 0.0))
            avg_l2_age_ms = float(health_metrics.get("avg_l2_age_ms", 0.0))
            avg_l2_age_tick_ms = float(health_metrics.get("avg_l2_age_tick_ms", 0.0))
            ticks_total = int(health_metrics.get("ticks_total", 0))
            ticks_with_l2 = int(health_metrics.get("ticks_with_l2", 0))

            # v_pass_rate (float), total_signals_val (int), missing_legs_stats (Dict[str, float]), passed_val (int), bypassed_val (int)
            v_pass_rate, total_signals_val, missing_legs_stats, v_passed_count, v_bypassed_count, v_ok_fail_breakdown, v_score_by_threshold = self._get_validation_stats(source, symbol, window_seconds)
            v_failed_count = total_signals_val - v_passed_count - v_bypassed_count

            trade_label = "Сделок"
            header_note = ""
            shadow_all = m.get("shadow_all", {}) or {}
            shadow_passed = m.get("shadow_passed", {}) or {}
            smt_passed = m.get("smt_passed", {}) or {}
            all_gates = m.get("shadow_all_gates", {}) or {}

            # Helper for metrics
            def _get_metrics(dm):
                c = int(dm.get('total_trades', 0))
                w = float(dm.get('wins', 0))
                p = float(dm.get('total_pnl', 0))
                wr_ = (w / c * 100.0) if c > 0 else 0.0
                return c, wr_, p

            s_all_c, s_all_wr, s_all_pnl = _get_metrics(shadow_all)
            s_pass_c, s_pass_wr, s_pass_pnl = _get_metrics(shadow_passed)
            smt_c, smt_wr, smt_pnl = _get_metrics(smt_passed)
            ag_c, ag_wr, ag_pnl = _get_metrics(all_gates)

            # Virtual Trade Metrics
            virtual_all = m.get("virtual_all", {}) or {}

            v_all_c, v_all_wr, v_all_pnl = _get_metrics(virtual_all)

            # --- SMT VETO: accurate pass-rate & missing-fields label ---
            # FIX: smt_c = trades NOT vetoed (SMT fields present + passed sim)
            #      smt_missing = trades where SMT fields absent → sim was N/A
            #      smt_evaluated = total trades where sim actually ran
            #      smt_vetoed = smt_evaluated - smt_c
            #      smt_pass_rate = smt_c / smt_evaluated (not / all trades!)
            smt_missing_fields = int(smt_passed.get("_missing_fields") or 0)
            smt_evaluated = s_all_c - smt_missing_fields  # trades where sim ran
            smt_vetoed = max(0, smt_evaluated - smt_c)
            smt_pass_rate = (smt_c / smt_evaluated * 100.0) if smt_evaluated > 0 else 0.0

            # SMT VETO line: show vetoed count + missing note if relevant
            if smt_missing_fields > 0 and smt_evaluated == 0:
                # All trades lack SMT fields → sim fully N/A
                _smt_line_virt = f"SMT VETO (Sim): <b>N/A</b> <i>(поля отсутствуют у {smt_missing_fields} из {s_all_c} сделок)</i>"
                _smt_line_real = _smt_line_virt
            elif smt_missing_fields > 0:
                _smt_line_virt = (
                    f"SMT VETO (Sim): прошло <b>{smt_c}</b> / оценено <b>{smt_evaluated}</b> ({smt_pass_rate:.1f}%) "
                    f"| vetoed: <b>{smt_vetoed}</b> | N/A (нет полей): <b>{smt_missing_fields}</b> "
                    f"| WR: <b>{smt_wr:.1f}%</b> | PnL: <b>{smt_pnl:+.2f}</b>"
                )
                _smt_line_real = _smt_line_virt
            else:
                _smt_line_virt = (
                    f"SMT VETO (Sim): прошло <b>{smt_c}</b> / оценено <b>{smt_evaluated}</b> ({smt_pass_rate:.1f}%) "
                    f"| vetoed: <b>{smt_vetoed}</b> "
                    f"| WR: <b>{smt_wr:.1f}%</b> | PnL: <b>{smt_pnl:+.2f}</b>"
                )
                _smt_line_real = _smt_line_virt

            # Unified: always show both Virtual and Real lines (removed)

            signal_shadow_lines = [
                "<b>📊 Signal & Shadow Analytics</b>",
                f"Все сигналы (>conf): <b>{int(shadow_all.get('total_trades',0))}</b> | WR: <b>{float(shadow_all.get('wins',0))/max(1,int(shadow_all.get('total_trades',0)))*100.0:.1f}%</b> | PnL: <b>{float(shadow_all.get('total_pnl',0)):+.2f}</b>",
                f"Прошедшие гейты: <b>{int(shadow_passed.get('total_trades',0))}</b> | WR: <b>{float(shadow_passed.get('wins',0))/max(1,int(shadow_passed.get('total_trades',0)))*100.0:.1f}%</b> | PnL: <b>{float(shadow_passed.get('total_pnl',0)):+.2f}</b>",
                f"All Gates (Sim): <b>{ag_c}</b> | WR: <b>{ag_wr:.1f}%</b> | PnL: <b>{ag_pnl:+.2f}</b>",
                _smt_line_real,
            ]

            # --- NEW: Strong (High Conf) Lines ---
            shc_stats = m.get("strong_high_conf_stats") or {}
            # Traverse sorted keys 70->100
            for thr in sorted([int(k) for k in shc_stats.keys()]):
                st = shc_stats[str(thr)]
                cnt = int(st.get("count", 0))
                if cnt > 0:
                    s_wins = int(st.get("wins", 0))
                    s_pnl = float(st.get("pnl", 0.0))
                    s_wr = (s_wins / cnt * 100.0)
                    line = f"Strong (High Conf) {thr}: <b>{cnt}</b> | WR: <b>{s_wr:.1f}%</b> | PnL: <b>{s_pnl:+.2f}</b>"
                    signal_shadow_lines.append(line)
            display_symbol = "ALLSYMBOLS" if symbol == "ALL" else symbol
            report_type_label = "VIRTUAL / SHADOW" if report_type == "VIRTUAL" else "REAL"
            title_text = f"Отчет: {html.escape(str(source))} ({report_type_label})"
            sections: list[str] = [
                f"📊 <b>{title_text} / {html.escape(display_symbol)}</b>",
                f"🕐 {now_utc}",
                f"🪟 Окно: {window_label}",
                *( [header_note] if header_note else [] ),
                "========================================",
                "",
                "<b>📈 ОСНОВНОЕ (net по PnL)</b>",
                f"{trade_label}: <b>{total}</b>",
                f"W/L/BE (net): <b>{wins}({rate(wins):.1f}%)</b>/<b>{losses}({rate(losses):.1f}%)</b>/<b>{be}({rate(be):.1f}%)</b> | WR: <b>{wr:.1f}%</b>",
                f"Hits: W(TP): <b>{int(m.get('cnt_tp_hit', 0))}</b>({rate(int(m.get('cnt_tp_hit', 0))):.1f}%) | L(SL): <b>{int(m.get('cnt_sl_hit', 0))}</b>({rate(int(m.get('cnt_sl_hit', 0))):.1f}%)",
                f"P/L net: <b>{total_pnl:+.2f}</b> | Avg: <b>{avg_pnl:+.2f}</b>",
                f"Avg P/L %: <b>{avg_pct:+.3f}</b>",
                f"Fees: <b>{fees:+.2f}</b>",
                f"Turnover (entry): <b>{total_notional:.2f}</b> USDT",
                f"PF(gross): <b>{pf_str}</b> | PF(net): <b>{pf_net_str}</b>",
                f"Avg duration: <b>{avg_dur_s:.1f}s</b>",
                f"Opened in window: <b>{int(m.get('opened_in_window', 0))}</b> | Closed in window: <b>{total}</b> | Δ: <b>{int(m.get('opened_in_window', 0)) - total:+d}</b>"
                + (" ℹ️ <i>(avg_dur &gt; window)</i>" if int(m.get('opened_in_window', 0)) == 0 and avg_dur_s > effective_window_sec * 0.8 else ""),
                "",
                "<b>📐 Edge / Risk (must-have)</b>",
                f"Expectancy: <b>{avg_pnl:+.2f}$</b> | <b>{expectancy_r:+.3f}R</b>" if int(m.get("cnt_r", 0)) > 0 else f"Expectancy: <b>{avg_pnl:+.2f}$</b> | R: N/A",
                (f"Median R: <b>{med_r:+.3f}</b> | TrimMean R(10%): <b>{trim_r:+.3f}</b>" if int(m.get("cnt_r", 0)) > 0 else "Median R: N/A | TrimMean R: N/A"),
                (f"StdDev: <b>{std_r:.3f}R</b> | Sharpe-like: <b>{sharpe_like:.2f}</b> | Sortino-like: <b>{sortino_like:.2f}</b>" if int(m.get("cnt_r", 0)) > 0 else f"Sharpe-like: <b>{sharpe_like:.2f}</b> | Sortino-like: <b>{sortino_like:.2f}</b>"),
                (f"Payoff(R): <b>{payoff_r:.2f}</b> | Kelly(f): <b>{kelly:.2f}</b>" if int(m.get("cnt_r", 0)) > 0 else ""),
                *([f"⚠️ <i>R-metrics based on {int(m.get('cnt_r', 0))}/{total} trades (risk data missing for {int(m.get('count_missing_risk', 0))})</i>"] if int(m.get("count_missing_risk", 0)) > 0 else []),
                f"Max DD: <b>{mdd:.2f}$</b> | Streaks W/L: <b>{max_w_streak}</b>/<b>{max_l_streak}</b>",
                "",
                *signal_shadow_lines,
                "",
                "<b>👮 Validation Stats</b>",
                # Show breakdown: X passed, Y failed, Z bypassed
                # Pass rate is only over decided (passed+failed), bypassed = gate not evaluated (Shadow)
                *(
                    [f"Validation Pass Rate: <b>{v_pass_rate:.1f}%</b> ({v_passed_count}/{v_passed_count + v_failed_count} decided) | Bypassed: <b>{v_bypassed_count}</b>/<b>{total_signals_val}</b>"]
                    if total_signals_val > 0 else
                    ["Validation Pass Rate: <b>N/A</b> (no signals in window)"]
                ),
                *(
                    [f"Missing Legs: {', '.join(f'{k}: <b>{v:.1f}%</b>' for k, v in sorted(missing_legs_stats.items()))}"]
                    if missing_legs_stats and total_signals_val > 0 else []
                ),
                "",
                "<b>🛡️ Filtration & Signal Quality (Pool)</b>",
                *(
                    [
                        f"Vetoed (Gate): <b>{m.get('cnt_veto_gate', 0)}</b> ({safe_div(m.get('cnt_veto_gate', 0), total) * 100.0:.1f}%)",
                        f"Rejected (Low TP): <b>{m.get('cnt_rejected_low_tp', 0)}</b> ({safe_div(m.get('cnt_rejected_low_tp', 0), total) * 100.0:.1f}%)",
                    ] if total > 0 else ["No signals to analyze quality."]
                ),
                # ── ok=0 breakdown ───────────────────────────────────────────────────
                # FIX: explicitly label data source — this breakdown is from signals stream
                # (signals:of:inputs / signals:cryptoorderflow), NOT from closed trades.
                # "230 (100%)" means 100% of REJECTED signals had score<0.65 — different pool from trades.
                *(
                    [
                        "",
                        "<b>❌ Почему ok=0? (breakdown сигналов в сигнал-стриме)</b>",
                        "<i>Источник: signals:of:inputs за окно отчёта — пул сигналов (не сделок)</i>",
                        f"Отклонено сигналов (ok=0): <b>{v_failed_count}</b> из <b>{total_signals_val}</b> total | pass_rate: <b>{v_pass_rate:.1f}%</b>",
                        "Условия, заблокировавшие ok (% от отклонённых):",
                        *[
                            f"  • <code>{html.escape(c)}</code>: {n_} ({n_ / v_failed_count * 100:.1f}%)"
                            for c, n_ in sorted(v_ok_fail_breakdown.items(), key=lambda kv: -kv[1])[:10]
                        ],
                        "<i>Один сигнал может нарушать несколько условий. Пул сигналов ≠ пул сделок.</i>",
                    ]
                    if v_ok_fail_breakdown and v_pass_rate < 25.0 and v_failed_count > 0 else []
                ),
                # ── score threshold breakdown (only when score_veto is dominant) ────────────
                *(
                    [
                        "",
                        "<b>🎯 Score Threshold Breakdown (сигналы с score ≥ порога):</b>",
                        *[
                            f"  score ≥ {thr}: <b>{st['count']}</b> ({st['count'] / max(total_signals_val, 1) * 100:.1f}%) "
                            f"| pass: <b>{st['passed']}</b> "
                            f"({st['passed'] / max(st['count'], 1) * 100:.1f}%)"
                            for thr, st in sorted(v_score_by_threshold.items(), key=lambda x: float(x[0]))
                            if st["count"] > 0
                        ],
                    ]
                    if v_score_by_threshold
                    and any(st["count"] > 0 for st in v_score_by_threshold.values())
                    and any(k.startswith("score_veto") and n > 0 for k, n in v_ok_fail_breakdown.items())
                    else []
                ),
                # ───────────────────────────────────────────────────
                "",
                "<b>🎛️ Exit Quality</b>",
                f"ExitEff(win): <b>{exit_eff:.2f}</b> | Giveback(win): <b>{giveback_avg:.2f}</b> | Missed(SL_AFTER_TP): <b>{missed_avg:.2f}</b>",
                "",
                "<b>📐 Setup Stats (ATR)</b>",
                f"Avg SL: <b>{float(m.get('avg_sl_atr', 0.0)):.2f} ATR</b> | Avg TP1: <b>{float(m.get('avg_tp_atr', 0.0)):.2f} ATR</b>"
                + (f" | RR(SL/TP1): <b>{float(m.get('avg_sl_atr', 0.0)) / max(float(m.get('avg_tp_atr', 0.0)), 1e-9):.2f}</b> ⚠️" if float(m.get('avg_sl_atr', 0.0)) > float(m.get('avg_tp_atr', 0.0)) > 0 else (f" | RR(SL/TP1): <b>{float(m.get('avg_sl_atr', 0.0)) / max(float(m.get('avg_tp_atr', 0.0)), 1e-9):.2f}</b>" if float(m.get('avg_tp_atr', 0.0)) > 0 else "")),
                *(
                    [f"Avg TP_final (furthest hit): <b>{float(m.get('avg_tp_final_atr', 0.0)):.2f} ATR</b>"
                    + (f" | RR(SL/TP_final): <b>{float(m.get('avg_sl_atr', 0.0)) / max(float(m.get('avg_tp_final_atr', 0.0)), 1e-9):.2f}</b>"
                       if float(m.get('avg_tp_final_atr', 0.0)) > 0 else "")]
                    if float(m.get('avg_tp_final_atr', 0.0)) > 0 else []
                ),
                "",
                "<b>🧷 Data Quality</b>",
                f"bad_ts(sec/us): <b>{bad_ts_sec}</b>/<b>{bad_ts_us}</b> | bad_time: <b>{bad_time}</b> | neg_dur: <b>{neg_dur}</b>",
                f"tp_hit_but_zero_pnl: <b>{tp_zero}</b> | close_reason_inconsistent: <b>{cr_incon}</b> | fees_huge: <b>{fees_huge}</b>",
                "",
                "<b>🏥 Health Metrics</b>",
                f"L2 stale tick/now: <b>{l2_stale_ratio_tick:.1%}</b>/<b>{l2_stale_ratio_now:.1%}</b>",
                f"Avg L2 age: <b>{avg_l2_age_ms:.0f}ms</b> | Tick age: <b>{avg_l2_age_tick_ms:.0f}ms</b>",
                f"Ticks total/with L2: <b>{ticks_total}</b>/<b>{ticks_with_l2}</b>",
                f"Signal/DLQ rate: <b>{signal_emit_rate:.2f}</b>/<b>{dlq_rate:.2f}</b> per sec",
                "",
                "<b>🎯 TP Targets (Price Action)</b>",
                f"TP1/TP2/TP3 hits: <b>{int(m['tp1_hits'])}</b>/<b>{int(m['tp2_hits'])}</b>/<b>{int(m['tp3_hits'])}</b>",
                f"TP→SL: TP1 <b>{int(m['tp1_then_sl'])}</b> ({safe_div(int(m['tp1_then_sl']), int(m['tp1_hits'])) * 100.0:.1f}%) | TP2 <b>{int(m['tp2_then_sl'])}</b> ({safe_div(int(m['tp2_then_sl']), int(m['tp2_hits'])) * 100.0:.1f}%) | TP3 <b>{int(m['tp3_then_sl'])}</b> ({safe_div(int(m['tp3_then_sl']), int(m['tp3_hits'])) * 100.0:.1f}%)",
                "",
                "<b>🧬 PnL Sign x Reason</b>",
            ]

            def _fmt_reason_map(d: dict[str, int], total_count: int) -> str:
                if not d: return "none"
                top_d = sorted(d.items(), key=lambda kv: kv[1], reverse=True)[:3]
                parts = []
                for k, v in top_d:
                    pct = (v / total_count * 100.0) if total_count > 0 else 0.0
                    parts.append(f"{html.escape(str(k))}:{v} ({pct:.1f}%)")
                return ", ".join(parts)

            sections.extend([
                f"Wins: {_fmt_reason_map(m.get('wins_by_reason', {}), total)}",
                f"Losses: {_fmt_reason_map(m.get('losses_by_reason', {}), total)}",
            ])
            if m.get('breakeven_by_reason'):
                sections.append(f"BE: {_fmt_reason_map(m.get('breakeven_by_reason', {}), total)}")

            # Trailing section separated
            trailing_lines = ["", "<b>🔄 Trailing Statistics</b>"]
            has_trailing_info = False

            if show_trailing_section:
                trailing_info_parts = []
                if trailing_started > 0 or effective_trailing_enabled:
                    trailing_info_parts.append(f"Started: <b>{trailing_started}</b>")

                if closed_by_trail > 0:
                    trailing_info_parts.append(f"Closed by trail: <b>{closed_by_trail}</b>")
                    twr = safe_div(closed_by_trail, trailing_started) * 100.0
                    trailing_info_parts.append(f"Trail WR: <b>{twr:.1f}%</b>")

                if trailing_info_parts:
                    trailing_lines.append(" | ".join(trailing_info_parts))
                    has_trailing_info = True

            profiles = m.get("trailing_profiles") or {}
            prof_stats = m.get("trailing_profile_stats") or {}
            if profiles:
                prof_top = sorted(profiles.items(), key=lambda kv: kv[1], reverse=True)
                prof_str_parts = []
                for k, v in prof_top:
                    st = prof_stats.get(k)
                    if st and st["count"] > 0:
                        wr_p = (st["wins"] / st["count"]) * 100.0
                        pnl_p = st["pnl"]
                        prof_str_parts.append(f"{html.escape(str(k))}:{v} | WR: {wr_p:.1f}% | PnL: {pnl_p:+.2f}")
                    else:
                        prof_str_parts.append(f"{html.escape(str(k))}:{v}")

                prof_str = ", ".join(prof_str_parts)
                trailing_lines.append(f"Profiles: <b>{prof_str}</b>")
                has_trailing_info = True

            reg_stats = m.get("regime_stats") or {}
            if reg_stats:
                reg_top = sorted(reg_stats.items(), key=lambda kv: kv[1]["count"], reverse=True)
                reg_str_parts = []
                for k, st in reg_top:
                    if st and st["count"] > 0:
                        wr_p = (st["wins"] / st["count"]) * 100.0
                        pnl_p = st["pnl"]
                        reg_str_parts.append(f"{html.escape(str(k))}:{st['count']} | WR: {wr_p:.1f}% | PnL: {pnl_p:+.2f}")
                if reg_str_parts:
                    reg_str = "\n".join([f"  • {part}" for part in reg_str_parts])
                    trailing_lines.append(f"Regimes:\n<b>{reg_str}</b>")
                    has_trailing_info = True

            if has_trailing_info:
                sections.extend(trailing_lines)

            # --- Removed Duplicate Edge/Risk Section ---
            # Original code here added a second "Edge / Risk" block with broken keys (sh_new, mdd_new=0).
            # We have merged the valuable info (PF(net), Median R) into the main sections above.

            sections.append("") # Spacer

            if total >= min_es:
                var_ret = float(m.get("var_ret", 0.0))
                cvar_ret = float(m.get("cvar_ret", 0.0))
                var_r = float(m.get("var_r", 0.0))
                cvar_r = float(m.get("cvar_r", 0.0))
                sections.append(
                    f"Tail(5%): VaR(ret): <b>{var_ret:.6f}</b> | ES(ret): <b>{cvar_ret:.6f}</b> | VaR(R): <b>{var_r:+.3f}</b> | ES(R): <b>{cvar_r:+.3f}</b>"
                )

            exp_entry = float(m.get("expectancy_entry_usd", 0.0))
            exp_mgmt = float(m.get("expectancy_mgmt_usd", 0.0))
            sections.extend([
                "",
                "<b>⚖️ Разделение edge</b>",
                f"Entry-edge (baseline P/L): <b>{exp_entry:+.2f}</b> USD/сделка",
                f"Mgmt-edge (pnl_net - baseline): <b>{exp_mgmt:+.2f}</b> USD/сделка",
            ])

            # === Baseline vs Managed ===
            n_fixed = int(m.get("n_fixed", 0))
            if n_fixed > 0:
                wr_fixed = float(m.get("wr_fixed", 0.0)) * 100.0  # конвертируем в проценты
                exp_fixed_r = float(m.get("expectancy_fixed_r", 0.0))
                payoff_fixed_r = float(m.get("payoff_fixed_r", 0.0))
                payoff_fixed_usd = float(m.get("payoff_fixed_usd", 0.0))

                # WR(managed) — используем strict если есть, иначе обычный
                ws_strict = int(m.get("wins_strict", 0))
                ls_strict = int(m.get("losses_strict", 0))
                total_strict = ws_strict + ls_strict
                wr_managed = safe_div(ws_strict, total_strict) * 100.0 if total_strict > 0 else safe_div(wins, total) * 100.0

                exp_managed_r = exp_r
                delta_exp_r = exp_managed_r - exp_fixed_r

                baseline_lines = [
                    "",
                    "<b>📊 Baseline vs Managed</b>",
                    f"WR(baseline): <b>{wr_fixed:.1f}%</b> | Exp_R(baseline): <b>{exp_fixed_r:+.3f}</b>",
                    f"WR(managed):  <b>{wr_managed:.1f}%</b> | Exp_R(managed): <b>{exp_managed_r:+.3f}</b>",
                    f"ΔExp_R (managed - baseline): <b>{delta_exp_r:+.3f}</b>",
                ]

                # Добавляем Payoff baseline если есть
                if payoff_fixed_r > 0 or payoff_fixed_usd > 0:
                    baseline_lines.append(
                        f"Payoff_baseline: <b>{payoff_fixed_usd:.2f}</b> USD | <b>{payoff_fixed_r:.2f}</b> R"
                    )

                sections.extend(baseline_lines)

            # Removed redundant Data Quality section that used incorrect keys

            # === Gate Diagnostics (Replay Analysis) ===
            gate_diag_lines = self._get_gate_diagnostics(source, symbol)
            if gate_diag_lines:
                sections.extend(gate_diag_lines)

            m_shadow = m.get("shadow_all", m)

            # === Gate & Scenarios (NEW) ===
            cnt_rev = int(m_shadow.get("cnt_scenario_reversal", 0))
            cnt_cont = int(m_shadow.get("cnt_scenario_continuation", 0))

            cnt_s_ok = int(m_shadow.get("cnt_strong_ok", 0))
            pnl_s_ok = float(m_shadow.get("sum_pnl_strong_ok", 0.0))
            cnt_s_fail = int(m_shadow.get("cnt_strong_fail", 0))
            pnl_s_fail = float(m_shadow.get("sum_pnl_strong_fail", 0.0))
            total_s = cnt_s_ok + cnt_s_fail

            if cnt_rev > 0 or cnt_cont > 0 or total_s > 0:
                pnl_rev = float(m_shadow.get("sum_pnl_scenario_reversal", 0.0))
                pnl_cont = float(m_shadow.get("sum_pnl_scenario_continuation", 0.0))

                cnt_enforce = int(m_shadow.get("cnt_gate_enforce", 0))
                cnt_shadow = int(m_shadow.get("cnt_gate_shadow", 0))
                cnt_veto = int(m_shadow.get("cnt_gate_shadow_veto", 0))
                pnl_veto = float(m_shadow.get("sum_pnl_shadow_veto", 0.0))

                gate_lines = [
                    "",
                    "<b>⛩️ Gate & Scenarios</b>",
                    f"Reversal: <b>{cnt_rev}</b> ({pnl_rev:+.2f}$) | Continuation: <b>{cnt_cont}</b> ({pnl_cont:+.2f}$)",
                    f"Gate Mode: Enforce(<b>{cnt_enforce}</b>) vs Shadow(<b>{cnt_shadow}</b>)",
                ]
                if cnt_veto > 0:
                    gate_lines.append(f"Shadow Veto: <b>{cnt_veto}</b> rejected (Impact: <b>{pnl_veto:+.2f}$</b>)")

                # Strong vs Weak Breakdown
                cnt_s_ok = int(m_shadow.get("cnt_strong_ok", 0))
                pnl_s_ok = float(m_shadow.get("sum_pnl_strong_ok", 0.0))
                cnt_s_fail = int(m_shadow.get("cnt_strong_fail", 0))
                pnl_s_fail = float(m_shadow.get("sum_pnl_strong_fail", 0.0))

                if total_s > 0:
                    pct_ok = (cnt_s_ok / total_s) * 100.0
                    pct_fail = (cnt_s_fail / total_s) * 100.0
                    gate_lines.append(f"Strong(Pass): <b>{cnt_s_ok}</b> ({pct_ok:.1f}%) PnL: <b>{pnl_s_ok:+.2f}$</b>")
                    gate_lines.append(f"Weak(Fail): <b>{cnt_s_fail}</b> ({pct_fail:.1f}%) PnL: <b>{pnl_s_fail:+.2f}$</b>")

                sections.extend(gate_lines)


            # === 4-Hour PnL Breakdown (для дневных отчетов) ===
            # Показываем только если окно >= 12 часов
            if effective_window_sec >= 43200:  # 12 часов
                try:
                    # Собираем сделки для разбивки
                    trades_for_breakdown = self._iter_recent_trades_window(
                        strategy=strategy_from_source(source),
                        symbol=symbol,
                        tf="tick",
                        source=source,
                        window_seconds=effective_window_sec
                    )

                    if trades_for_breakdown:
                        session_breakdown = self._calculate_session_pnl_breakdown(
                            trades_for_breakdown,
                            window_hours=min(24, effective_window_sec // 3600)
                        )

                        if session_breakdown:
                            breakdown_lines = [
                                "",
                                "<b>📊 Разбивка PnL по торговым сессиям (последние 24ч)</b>",
                            ]

                            # Определяем порядок сессий (от более свежих к старым)
                            # NYC (16:00-00:00) -> London (08:00-16:00) -> Asia (00:00-08:00)
                            session_order = ["nyc", "london", "asia"]
                            session_names = {
                                "nyc": "Нью-Йорк (16:00-00:00 UTC)",
                                "london": "Лондон (08:00-16:00 UTC)",
                                "asia": "Азия (00:00-08:00 UTC)"
                            }

                            for session_key in session_order:
                                if session_key not in session_breakdown:
                                    continue

                                session_data = session_breakdown[session_key]

                                if session_data["trades"] == 0:
                                    continue

                                profit = session_data["profit"]
                                loss = session_data["loss"]
                                net = session_data["net"]
                                trades_count = session_data["trades"]
                                session_name = session_names[session_key]

                                breakdown_lines.append(
                                    f"{session_name}: <b>{net:+.2f}$</b> "
                                    f"(↑{profit:.2f} / ↓{loss:.2f}, {trades_count} сделок)"
                                )

                            sections.extend(breakdown_lines)

                except Exception as e:
                    logger.warning(f"⚠️ Ошибка расчета hourly PnL breakdown: {e}")

            sections.extend([
                "",
                "<b>🧪 Диагностика (strict по close_reason)</b>",
                f"W/L/BE(strict): <b>{ws}</b>/<b>{ls}</b>/<b>{bs}</b> | WR(strict): <b>{wrs:.1f}%</b>",
                *(
                    ["⚠️ <i>WR(net) &gt; 95% при N&gt;=10: проверьте корректность данных (pnl/close_reason).</i>"]
                    if total >= MIN_TRADES_FOR_WR_WARN and wr > 95.0 else []
                ),
                *(
                    ["⚠️ <i>WR(strict) &gt; 95% при N&gt;=10: проверьте нормализацию close_reason.</i>"]
                    if total >= MIN_TRADES_FOR_WR_WARN and wrs > 95.0 else []
                ),
                f"🧾 <i>Top close_reason: {top_str}</i>",
                "",
                "<b>🔍 Диагностика данных</b>",
                f"Убыточных сделок (pnl &lt; 0): <b>{int(m['neg_pnl_count'])}</b>",
                f"Min PnL: <b>{min_pnl_str}</b> | Max PnL: <b>{max_pnl_str}</b>",
                f"Пропущено fees: <b>{int(m['missing_fees_count'])}</b> | Пропущено duration: <b>{int(m['missing_duration_count'])}</b>",
                *(
                    [f"💰 P/L gross: <b>{total_pnl_gross:+.2f}</b> | Fees ratio: <b>{fees_ratio:.2f}x</b>"]
                    if total_pnl_gross != 0.0 or fees_ratio > 0.5 else []
                ),
                *(
                    ["⚠️ <i>Внимание: Fees сопоставимы с P/L net. Проверьте расчет комиссий относительно размера позиции.</i>"]
                    if fees_ratio > 0.8 and abs(total_pnl) > EPS else []
                ),
            ])

            # === OF Confirm Stats (NEW) ===
            of_confirm_stats = m_shadow.get("of_confirm_stats", m.get("of_confirm_stats", {}))
            of_lines = [
                "",
                "<b>✅ OF Confirm Stats</b>"
            ]

            if not of_confirm_stats:
                # Add default if completely empty
                of_confirm_stats = {"none_gate(0/0)": {"count": 0, "wins": 0, "pnl": 0.0}}

            # Calculate total signals for percentage
            total_of_signals = sum(s["count"] for s in of_confirm_stats.values())

            # Sort by count descending
            sorted_stats = sorted(
                of_confirm_stats.items(),
                key=lambda x: x[1]["count"],
                reverse=True
            )

            for key, stats in sorted_stats:
                cnt = stats["count"]
                s_wins = stats["wins"]
                s_pnl = stats["pnl"]

                s_wr = (s_wins / cnt * 100.0) if cnt > 0 else 0.0
                share = (cnt / total_of_signals * 100.0) if total_of_signals > 0 else 0.0

                of_lines.append(
                    f"{key}: WR: <b>{s_wr:.1f}%</b> | PnL: <b>{s_pnl:+.2f}$</b> | Share: <b>{share:.1f}%</b>"
                )

            sections.extend(of_lines)

            # === ML Performance Stats (ENHANCED) ===
            ml_stats = m_shadow.get("ml_stats", m.get("ml_stats", {}))
            ml_cond_stats = m_shadow.get("ml_condition_stats", m.get("ml_condition_stats", {}))

            ml_lines = [
                "",
                "<b>🤖 ML Performance (Shadow Mode)</b>"
            ]

            if not ml_stats:
                ml_stats = {
                    "pass": {"count": 0, "wins": 0, "pnl": 0.0},
                    "veto": {"count": 0, "wins": 0, "pnl": 0.0}
                }

            # Overall stats
            total_ml_signals = sum(s["count"] for s in ml_stats.values())
            for key in ["pass", "veto"]:
                stats = ml_stats.get(key, {"count": 0, "wins": 0, "pnl": 0.0})
                cnt = stats["count"]
                s_wins = stats["wins"]
                s_pnl = stats["pnl"]
                s_wr = (s_wins / cnt * 100.0) if cnt > 0 else 0.0
                share = (cnt / total_ml_signals * 100.0) if total_ml_signals > 0 else 0.0
                label = "ALLOW" if key == "pass" else "VETO "
                ml_lines.append(
                    f"{label}: WR: <b>{s_wr:.1f}%</b> | PnL: <b>{s_pnl:+.2f}$</b> | Share: <b>{share:.1f}%</b> ({cnt})"
                )

            # NEW: Detailed condition breakdown
            if not ml_cond_stats:
                ml_cond_stats = {"total_evaluated": 0, "avg_p_edge": 0.0, "median_p_edge": 0.0}

            total_eval = ml_cond_stats.get("total_evaluated", 0)
            avg_p = ml_cond_stats.get("avg_p_edge", 0.0)
            med_p = ml_cond_stats.get("median_p_edge", 0.0)

            ml_lines.extend([
                "",
                f"<b>📊 ML Condition Analysis ({total_eval} signals evaluated)</b>",
                f"Avg p_edge: <b>{avg_p:.3f}</b> | Median: <b>{med_p:.3f}</b>",
            ])
            # ⚠️ Warn if ML model returns constant 0.5 for all signals (default / untrained)
            if total_eval >= 50 and abs(avg_p - 0.5) < 0.001 and abs(med_p - 0.5) < 0.001:
                ml_lines.append(
                    "⚠️ <i>ML model returning constant p_edge=0.50 for all signals — "
                    "model may not be trained or predictions not loaded. "
                    "Check ml_models/ directory and ML service logs.</i>"
                )
            # Per-threshold breakdown
            by_thr = ml_cond_stats.get("by_threshold", {})
            if by_thr:
                ml_lines.append("")
                ml_lines.append("<b>🎯 Signals passing different thresholds (if enforce enabled):</b>")

                # Sort thresholds ascending
                sorted_thrs = sorted(by_thr.items(), key=lambda x: float(x[0]))
                for thr_key, stats in sorted_thrs:
                    cnt = stats["count"]
                    s_wins = stats["wins"]
                    s_pnl = stats["pnl"]
                    s_wr = (s_wins / cnt * 100.0) if cnt > 0 else 0.0
                    if total_eval:
                        share = (cnt / total_eval * 100.0)
                    else:
                        share = 0.0

                    ml_lines.append(
                        f"  p_edge ≥ {thr_key}: <b>{cnt}</b> ({share:.1f}%) | "
                        f"WR: <b>{s_wr:.1f}%</b> | PnL: <b>{s_pnl:+.2f}$</b>"
                    )

            # Per-scenario breakdown
            by_scn = ml_cond_stats.get("by_scenario", {})
            if by_scn:
                ml_lines.append("")
                ml_lines.append("<b>🎭 By Scenario:</b>")

                for scn_key, stats in sorted(by_scn.items(), key=lambda x: x[1]["count"], reverse=True):
                    cnt = stats["count"]
                    s_wins = stats["wins"]
                    s_pnl = stats["pnl"]
                    avg_p = stats.get("avg_p_edge", 0.0)
                    s_wr = (s_wins / cnt * 100.0) if cnt > 0 else 0.0

                    ml_lines.append(
                        f"  {html.escape(scn_key)}: n=<b>{cnt}</b> | "
                        f"WR: <b>{s_wr:.1f}%</b> | PnL: <b>{s_pnl:+.2f}$</b> | "
                        f"avg_p: <b>{avg_p:.3f}</b>"
                    )

            # P_edge distribution
            dist = ml_cond_stats.get("p_edge_distribution", {})
            if dist:
                ml_lines.append("")
                ml_lines.append("<b>📈 P_edge Distribution:</b>")

                # Sort buckets
                bucket_order = ["0.0-0.3", "0.3-0.4", "0.4-0.5", "0.5-0.6", "0.6-0.7", "0.7-1.0"]
                for bucket in bucket_order:
                    if bucket in dist:
                        # Handle both old format (int) and new format (dict)
                        if isinstance(dist[bucket], dict):
                            stats = dist[bucket]
                            cnt = stats.get("count", 0)
                            s_wins = stats.get("wins", 0)
                            s_pnl = stats.get("pnl", 0.0)
                            s_wr = (s_wins / cnt * 100.0) if cnt > 0 else 0.0
                            if total_eval:
                                share = (cnt / total_eval * 100.0)
                            else:
                                share = 0.0
                            ml_lines.append(
                                f"  {bucket}: <b>{cnt}</b> ({share:.1f}%) | "
                                f"WR: <b>{s_wr:.1f}%</b> | PnL: <b>{s_pnl:+.2f}$</b>"
                            )
                        else:
                            # Backward compatibility: old format was just count
                            cnt = dist[bucket]
                            if total_eval:
                                share = (cnt / total_eval * 100.0)
                            else:
                                share = 0.0
                            ml_lines.append(f"  {bucket}: <b>{cnt}</b> ({share:.1f}%)")

            sections.extend(ml_lines)

            # === OK-SOFT Stats (Requested) ===
            ok_soft = m.get("ok_soft_stats", {})
            cnt = ok_soft.get("count", 0)
            s_wins = ok_soft.get("wins", 0)
            s_pnl = ok_soft.get("pnl", 0.0)
            s_wr = (s_wins / cnt * 100.0) if cnt > 0 else 0.0

            # Use final pre-calculated share if available, else calc
            share = (ok_soft.get("share", 0.0) * 100.0) if "share" in ok_soft else (cnt / total * 100.0 if total > 0 else 0.0)

            ok_soft_lines = [
                "",
                f"<b>🆗 ok-soft: WR: <b>{s_wr:.1f}%</b> | PnL: <b>{s_pnl:+.2f}$</b> | Share: <b>{share:.1f}%</b> ({cnt})</b>"
            ]

            # 1. Show "Soft" reasons for these specific trades if any
            soft_reasons = m.get("ok_soft_reasons", {})
            if soft_reasons and cnt > 0:
                sorted_soft = sorted(soft_reasons.items(), key=lambda x: x[1], reverse=True)
                ok_soft_lines.append("Условия, не позволившие стать ok (soft):")
                for r, n_ in sorted_soft:
                    pct = (n_ / cnt * 100.0)
                    ok_soft_lines.append(f"  • <code>{html.escape(r)}</code>: {n_} ({pct:.1f}%)")

            # 2. Show GLOBAL breakdown of unmet conditions (why ok=0)
            unmet = m.get("unmet_ok_reasons", {})
            if unmet:
                # Total signals that failed strong gate
                fail_cnt = m.get("cnt_strong_fail", 0)
                if fail_cnt > 0:
                    sorted_unmet = sorted(unmet.items(), key=lambda x: x[1], reverse=True)
                    ok_soft_lines.append("<i>Процент условий, не удовлетворивших чтобы стать ok (всего):</i>")
                    for r, n_ in sorted_unmet:
                        pct = (n_ / fail_cnt * 100.0)
                        ok_soft_lines.append(f"  • <code>{html.escape(r)}</code>: {pct:.1f}% ({n_})")

            sections.extend(ok_soft_lines)

            # === Symbol Breakdown (PnL per symbol) ===
            symbol_breakdown = m.get("symbol_breakdown")
            if symbol_breakdown:
                try:
                    # Sort symbols by PnL descending
                    sorted_syms = sorted(
                        symbol_breakdown.items(),
                        key=lambda x: x[1]['pnl'],
                        reverse=True
                    )

                    sb_lines = [
                        "",
                        "<b>🪙 PnL по символам</b>"
                    ]

                    for sym_key, stats in sorted_syms:
                        p = stats['pnl']
                        tr = stats['trades']
                        # Format: SYMBOL: +123.45$ (5 сделок)
                        sb_lines.append(f"{html.escape(str(sym_key))}: <b>{p:+.2f}$</b> ({tr} сделок)")

                    sections.extend(sb_lines)
                except Exception as e:
                    logger.warning(f"⚠️ Error formatting symbol breakdown: {e}")

            # === Trailing vs Baseline Analysis ===
            if trailing_vs_baseline_results:
                global_analysis = trailing_vs_baseline_results["global"]
                by_tag_analysis = trailing_vs_baseline_results["by_tag"]
                trades_analyzed = trailing_vs_baseline_results["total_trades_analyzed"]

                trailing_sections = [
                    "",
                    f"<b>🎯 Trailing vs Baseline (анализ {trades_analyzed} сделок)</b>",
                    f"WR(managed): <b>{global_analysis.get('wr', 0)*100:.1f}%</b> | WR(baseline): <b>{global_analysis.get('wr_fixed', 0)*100:.1f}%</b>",
                    f"Exp_R(managed): <b>{global_analysis.get('expectancy_managed_r', 0):+.3f}</b> | Exp_R(baseline): <b>{global_analysis.get('expectancy_baseline_r', 0):+.3f}</b>",
                    f"ΔExp_R: <b>{global_analysis.get('delta_expectancy_r', 0):+.3f}</b>",
                    f"Sharpe(R): <b>{global_analysis.get('sharpe_r', 0):+.2f}</b> | Sortino(R): <b>{global_analysis.get('sortino_r', 0):+.2f}</b>",
                    f"MDD(managed): <b>{global_analysis.get('mdd_net_usd', 0):.2f}$</b> | MDD(baseline): <b>{global_analysis.get('mdd_baseline_usd', 0):.2f}$</b>",
                ]

                # Добавляем информацию о trailing trades
                trailing_share = global_analysis.get('trailing_share', 0)
                trailing_close_share = global_analysis.get('trailing_close_share', 0)
                if trailing_share > 0:
                    trailing_sections.extend([
                        f"Trailing запущен: <b>{trailing_share*100:.1f}%</b> | Закрыт по трейлу: <b>{trailing_close_share*100:.1f}%</b>",
                        f"Trailing WR: <b>{global_analysis.get('trailing_wr', 0)*100:.1f}%</b>",
                        f"Trailing Exp_R: <b>{global_analysis.get('trailing_expectancy_r', 0):+.3f}</b> (Δ: <b>{global_analysis.get('trailing_delta_expectancy_r', 0):+.3f}</b>)",
                    ])

                # Добавляем giveback/missed stats
                giveback_avg_r = global_analysis.get('giveback_avg_r', 0)
                missed_avg_r = global_analysis.get('missed_avg_r', 0)
                if giveback_avg_r != 0 or missed_avg_r != 0:
                    trailing_sections.extend([
                        f"Giveback: <b>{giveback_avg_r:+.3f}R</b> ({global_analysis.get('giveback_share', 0)*100:.1f}%) | Missed: <b>{missed_avg_r:+.3f}R</b> ({global_analysis.get('missed_share', 0)*100:.1f}%)",
                        f"MFE/MAE: <b>{global_analysis.get('mfe_avg_r', 0):+.3f}R</b> / <b>{global_analysis.get('mae_avg_r', 0):+.3f}R</b>",
                    ])

                # Добавляем топ тегов (если есть)
                if by_tag_analysis:
                    trailing_sections.append("")
                    trailing_sections.append("<b>📊 По entry_tag (топ):</b>")
                    for tag_stats in by_tag_analysis[:2]:  # показываем топ 2
                        tag_name = html.escape(tag_stats.get('tag', 'unknown')[:15])
                        tag_n = tag_stats.get('n', 0)
                        tag_delta = tag_stats.get('delta_expectancy_r', 0)
                        tag_trailing = tag_stats.get('trailing_share', 0) * 100
                        trailing_sections.append(
                            f"• {tag_name}: n={tag_n}, ΔExp_R=<b>{tag_delta:+.3f}</b>, trailing=<b>{tag_trailing:.1f}%</b>"
                        )

                # NEW: Добавляем разбивку по силе сигнала (Strong/Weak)
                by_strong_gate_analysis = trailing_vs_baseline_results.get("by_strong_gate", [])
                if by_strong_gate_analysis:
                    trailing_sections.append("")
                    trailing_sections.append("<b>💪 По силе сигнала (Strong/Weak):</b>")
                    for gate_stats in by_strong_gate_analysis:
                        gate_label = html.escape(gate_stats.get('tag', 'unknown'))
                        gate_n = gate_stats.get('n', 0)
                        gate_wr = gate_stats.get('wr', 0) * 100
                        gate_exp_r = gate_stats.get('expectancy_r', 0)
                        gate_delta = gate_stats.get('delta_expectancy_r', 0)
                        gate_pnl_avg = gate_stats.get('pnl_net_avg', 0)
                        trailing_sections.append(
                            f"• <b>{gate_label}</b>: n={gate_n:.0f}, WR=<b>{gate_wr:.1f}%</b>, Exp_R=<b>{gate_exp_r:+.3f}</b>, ΔExp_R=<b>{gate_delta:+.3f}</b>, Avg P/L=<b>{gate_pnl_avg:+.2f}$</b>"
                        )

                # Добавляем логику принятия решений на основе анализа (внутри trailing_sections)
                decision_sections = self._generate_decision_recommendations(global_analysis, by_tag_analysis)
                if decision_sections:
                    trailing_sections.extend(decision_sections)

                sections.extend(trailing_sections)
                logger.debug(f"✅ Добавлена секция trailing vs baseline в отчет для {source}/{symbol}")

            # Добавляем рекомендации по размеру трейлинга (если доступны функции)
            if recommend_trailing_size and ClosedTradeSnapshot:
                trailing_size_sections = self._generate_trailing_size_recommendations(source, symbol, total)
                if trailing_size_sections:
                    sections.extend(trailing_size_sections)
                    logger.debug(f"✅ Добавлена секция trailing size рекомендаций для {source}/{symbol}")

            msg = "\n".join(sections)

            # Проверяем длину сообщения и разбиваем на части если необходимо
            # Telegram лимит: 4096 символов, но с HTML тегами лучше использовать 3500-3800
            MAX_MESSAGE_LENGTH = 3500

            if len(msg) > MAX_MESSAGE_LENGTH:
                logger.info(f"⚠️ Отчет слишком длинный ({len(msg)} символов), разбиваем на части...")

                # Ищем логическое место для разбиения — предпочитаем разбить ПОСЛЕ блока
                # "🔄 Trailing Statistics", чтобы он целиком попал в Часть 1.
                # Вторичные маркеры — перед тяжёлыми аналитическими секциями Часть 2.
                msg_lines = msg.split('\n')

                # Приоритетные маркеры: точки разбиения (split происходит ПЕРЕД строкой)
                split_markers_primary = [
                    "<b>🎯 Trailing vs Baseline",    # после trailing block
                    "<b>🧠 РЕКОМЕНДАЦИИ ПО НАСТРОЙКАМ:",
                    "<b>💪 По силе сигнала",
                ]
                # Запасные маркеры — если основных нет (короткие отчёты без baseline анализа)
                split_markers_fallback = [
                    "<b>✅ OF Confirm Stats",         # OF confirm — начало тяжёлой аналитики
                    "<b>🤖 ML Performance",
                    "<b>🆗 ok-soft:",
                    "<b>⛩️ Gate &amp; Scenarios",
                ]

                split_index = None
                # Ищем первичные маркеры
                for i, line in enumerate(msg_lines):
                    for marker in split_markers_primary:
                        if marker in line:
                            split_index = i
                            break
                    if split_index is not None:
                        break

                # Если не нашли — используем запасные, но только если часть 1 > 60% MAX
                if split_index is None:
                    target = int(len(msg_lines) * 0.60)
                    for i, line in enumerate(msg_lines):
                        if i < target:
                            continue
                        for marker in split_markers_fallback:
                            if marker in line:
                                split_index = i
                                break
                        if split_index is not None:
                            break

                # Если совсем не нашли маркер — разбиваем пополам
                if split_index is None:
                    split_index = len(msg_lines) // 2

                # Формируем первую часть
                part1_lines = msg_lines[:split_index]
                part1 = "\n".join(part1_lines)

                # Формируем вторую часть
                part2_lines = msg_lines[split_index:]
                part2 = "\n".join(part2_lines)

                # Проверяем, что части не слишком длинные (если да, telegram worker разобьет дальше)
                if len(part1) > MAX_MESSAGE_LENGTH:
                    logger.warning(f"⚠️ Часть 1 все еще слишком длинная ({len(part1)} символов), telegram worker разобьет дальше")
                if len(part2) > MAX_MESSAGE_LENGTH:
                    logger.warning(f"⚠️ Часть 2 все еще слишком длинная ({len(part2)} символов), telegram worker разобьет дальше")

                # Добавляем индикаторы частей
                part1 += "\n\n📄 Часть 1/2"
                # Добавляем контекст во вторую часть, чтобы при вклинивании других сообщений было понятно к чему она
                part2 = f"📄 Часть 2/2 | <b>{html.escape(str(source))} ({report_type_label}) / {html.escape(display_symbol)}</b>\n\n" + part2

                logger.info(f"📤 Публикация отчета в Redis stream для {source}/{symbol} (часть 1/2, {len(part1)} символов)...")
                success1 = self.reporting.send_telegram_message(part1)

                # Небольшая задержка между частями для избежания rate limit
                import time
                time.sleep(0.3)

                logger.info(f"📤 Публикация отчета в Redis stream для {source}/{symbol} (часть 2/2, {len(part2)} символов)...")
                success2 = self.reporting.send_telegram_message(part2)

                success = success1 and success2
            else:
                # Отправка основного отчета (если не превышает лимит)
                logger.info(f"📤 Публикация отчета в Redis stream для {source}/{symbol}...")
                success = self.reporting.send_telegram_message(msg)

            # Анализ trailing edge (не при каждом отчете, а каждые N отчетов)
            should_analyze_trailing = (self.report_counter[pair_key] % self.trailing_analysis_reports_interval) == 0

            if should_analyze_trailing:
                logger.info(f"🎯 Выполнение анализа trailing edge для {source}/{symbol} (отчет #{self.report_counter[pair_key]})")

                trailing_edge_result = self.trailing_analyzer.analyze_last_trades(
                    source=source,
                    symbol=symbol,
                    limit=200,  # Анализируем последние 200 сделок (согласуется с REPORT_TRIGGER_COUNT * trailing_analysis_reports_interval)
                    since_hours=None  # Без ограничения по времени
                )

                # Отправка анализа trailing edge (если есть данные)
                if trailing_edge_result:
                    trailing_msg = trailing_edge_result.to_telegram_message()

                    # Получаем рекомендации по настройке трейлинга
                    recommendations = trailing_edge_result.generate_trailing_recommendation()
                    if recommendations:
                        trailing_msg += "\n\n🎛️ <b>Рекомендации по настройке:</b>"
                        for action in recommendations["actions"]:
                            trailing_msg += f"\n• {action['reason']}"

                            # Сохраняем рекомендации в Redis для применения
                            rec_key = f"trailing_recommendations:{source}:{symbol}"
                            rec_data = {
                                "timestamp": recommendations["analysis_timestamp"],
                                "confidence": recommendations["confidence_level"],
                                "actions": recommendations["actions"],
                                "analysis": {
                                    "delta_exp_r": trailing_edge_result.delta_exp_r,
                                    "share_better": trailing_edge_result.share_better,
                                    "share_worse": trailing_edge_result.share_worse,
                                    "total_trades": trailing_edge_result.total_trades,
                                    "trailing_trades": trailing_edge_result.trailing_trades
                                }
                            }

                            try:
                                self.redis.set(rec_key, json.dumps(rec_data), ex=86400*7)  # 7 дней
                                logger.info(f"💾 Рекомендации сохранены в Redis: {rec_key}")
                            except Exception as e:
                                logger.warning(f"⚠️ Не удалось сохранить рекомендации: {e}")

                    logger.info(f"🎯 Отправка анализа trailing edge для {source}/{symbol}")
                    trailing_success = self.reporting.send_telegram_message(trailing_msg)
                    if trailing_success:
                        logger.info(f"✅ Анализ trailing edge отправлен для {source}/{symbol}")
                    else:
                        logger.warning(f"⚠️ Не удалось отправить анализ trailing edge для {source}/{symbol}")
                else:
                    logger.debug(f"⏭️ Недостаточно данных для анализа trailing edge {source}/{symbol}")
            else:
                logger.debug(f"⏭️ Пропуск анализа trailing edge для {source}/{symbol} (отчет #{self.report_counter[pair_key]}, интервал {self.trailing_analysis_reports_interval})")
            if success:
                logger.info(f"✅ Отчет опубликован в Redis stream для {source}/{symbol}: trades={total} WR(net)={wr:.1f}% WR(strict)={wrs:.1f}%")
                # Проверяем, что сообщение действительно попало в stream
                try:
                    notify_stream = os.getenv("NOTIFY_STREAM", RS.NOTIFY_TELEGRAM)
                    stream_len = self.redis.xlen(notify_stream)
                    logger.debug(f"📊 Длина stream {notify_stream}: {stream_len} сообщений")
                except Exception as e:
                    logger.warning(f"⚠️ Не удалось проверить длину stream: {e}")
            else:
                logger.error(f"❌ Не удалось опубликовать отчет в Redis stream для {source}/{symbol}")
        except Exception as e:
            logger.error(f"❌ Ошибка при формировании/отправке отчета для {source}/{symbol}: {e}", exc_info=True)

    def _gather_trades_for_trailing_analysis(self, source: str, symbol: str, limit: int = 500) -> list[ClosedTradeSnapshot]:
        """
        Собирает сделки для анализа trailing size рекомендаций.
        Возвращает список ClosedTradeSnapshot из trades:closed stream.
        """
        if not ClosedTradeSnapshot:
            logger.debug("⚠️ ClosedTradeSnapshot не доступен, пропуск анализа trailing size")
            return []

        trade_window_count = TRADE_WINDOW_COUNT if TRADE_WINDOW_COUNT > 0 else None
        cutoff_ms = 0 if trade_window_count else get_ny_time_millis() - RECENT_WINDOW_SECONDS * 1000
        min_id = f"{cutoff_ms}-0"

        trades = []
        processed_order_ids = set()

        # Собираем из trades:closed stream (как в основном отчете)
        entries = []
        try:
            entries = self.redis.xrevrange(RS.TRADES_CLOSED, max="+", min=min_id, count=min(limit, RECENT_LIMIT)) or []
            logger.debug(f"📊 trailing analysis: найдено {len(entries)} записей из trades:closed")
        except Exception as e:
            logger.debug(f"⚠️ Ошибка чтения trades:closed для trailing analysis: {e}")
            return []

        for _, fields in entries:
            t = _norm_map(fields or {})
            order_id = t.get("order_id") or t.get("id") or ""

            if order_id in processed_order_ids:
                continue

            # Проверяем source и symbol
            raw_source = t.get("source") or ""
            raw_strategy = t.get("strategy") or ""
            t_source = canon_source(raw_strategy or raw_source or "")
            t_symbol = canon_symbol(t.get("symbol") or "")

            if t_source != source or t_symbol != symbol:
                continue

            processed_order_ids.add(order_id)

            # Конвертируем в ClosedTradeSnapshot
            try:
                snapshot = ClosedTradeSnapshot.from_trade_closed_dict(t)
                trades.append(snapshot)
            except Exception as e:
                logger.debug(f"⚠️ Ошибка конвертации сделки {order_id} в ClosedTradeSnapshot: {e}")
                continue

            if len(trades) >= limit:
                break

        logger.debug(f"📊 trailing analysis: собрано {len(trades)} сделок для {source}/{symbol}")
        return trades


    def _generate_trailing_size_recommendations(self, source: str, symbol: str, total_trades: int) -> list[str]:
        """
        Генерирует секции отчета с рекомендациями по размеру трейлинга.
        Возвращает список строк для добавления в отчет.
        """
        if not recommend_trailing_size or not ClosedTradeSnapshot:
            return []

        sections = []

        try:
            # Собираем сделки для анализа
            trades = self._gather_trades_for_trailing_analysis(source, symbol, limit=500)

            if len(trades) < 50:  # минимум 50 сделок для анализа
                logger.debug(f"⏭️ Недостаточно сделок для trailing size анализа: {len(trades)} < 50")
                return []

            # Получаем stop_atr_mult для символа
            stop_atr_mult = self._get_stop_atr_mult_for_symbol(symbol)

            # Генерируем рекомендации по всем выигрышным сделкам
            rec_all = recommend_trailing_size(
                trades,
                stop_atr_mult=stop_atr_mult,
                min_trades=50,
                winners_only=True,
                mfe_quantile=0.33,
                trailing_only=False,
            )

            # Генерируем рекомендации только по trailing-сделкам
            rec_trailing = recommend_trailing_size(
                trades,
                stop_atr_mult=stop_atr_mult,
                min_trades=25,  # меньше требований для trailing-сделок
                winners_only=True,
                mfe_quantile=0.33,
                trailing_only=True,
            )

            if not rec_all and not rec_trailing:
                return []

            sections.append("")
            sections.append("<b>🎯 Trailing Size Recommendations</b>")

            def format_rec(rec, label: str) -> str:
                if not rec:
                    return f"• {label}: недостаточно данных"
                return (
                    f"• {label}: n={rec.sample_size_total}, wins={rec.sample_size_win}, "
                    f"lock_r≈{rec.lock_r:.2f}R → TP1_OFFSET_ATR≈{rec.trailing_tp1_offset_atr:.2f}, "
                    f"giveback≈{rec.avg_giveback_ratio_win:.2f}, confidence≈{rec.confidence:.2f}"
                )

            if rec_all:
                sections.append(format_rec(rec_all, "Все win-сделки"))
            if rec_trailing:
                sections.append(format_rec(rec_trailing, "Только trailing win-сделки"))

                # Добавляем интерпретацию различий
                if rec_all and rec_trailing:
                    lock_r_diff = rec_trailing.lock_r - rec_all.lock_r
                    if abs(lock_r_diff) > 0.05:  # значимое различие
                        if lock_r_diff < 0:
                            sections.append("  💡 <i>Trailing-сделки показывают меньший оптимальный lock_r - возможно трейлинг поджимается слишком рано</i>")
                        else:
                            sections.append("  💡 <i>Trailing-сделки позволяют больший lock_r - текущие настройки консервативны</i>")

            logger.debug(f"✅ Сгенерированы trailing size рекомендации для {source}/{symbol}")

        except Exception as e:
            logger.warning(f"⚠️ Ошибка при генерации trailing size рекомендаций для {source}/{symbol}: {e}")
            return []

        return sections

    def _get_stop_atr_mult_for_symbol(self, symbol: str) -> float:
        """
        Получает stop_atr_mult для символа из Redis или возвращает дефолтное значение.
        """
        try:
            # Пытаемся прочитать из Redis (symbol_specs:{symbol})
            specs_key = f"symbol_specs:{symbol}"
            data = self.redis.hgetall(specs_key)
            if data and b"stop_atr_mult" in data:
                val = float(data[b"stop_atr_mult"])
                return val
        except Exception as e:
            logger.debug(f"⚠️ Ошибка чтения stop_atr_mult для {symbol}: {e}")

        # Дефолтные значения по символу
        if "BTC" in symbol:
            # FIX: Do not hardcode 0.5 for BTC, allow default (1.0) or Redis value.
            # return 0.5
            pass
        elif "ETH" in symbol:
            # return 0.6
            pass
        else:
            return 1.0

    # ----------------------------
    # Locks
    # ----------------------------
    def _acquire_lock(self, key: str, ttl: int) -> bool:
        try:
            return bool(self.redis.set(key, "1", nx=True, ex=max(1, int(ttl))))
        except Exception:
            return True

    def _release_lock(self, key: str) -> None:
        with contextlib.suppress(Exception):
            self.redis.delete(key)

    def _source_from_strategy(self, strategy: str, symbol: str | None = None) -> str:
        """Преобразует strategy в source для корректного маппинга."""
        s = (strategy or "").strip().lower()

        # Special handling for Gold/Forex generic orderflow
        if s == "orderflow" and symbol:
            sym_upper = symbol.upper()
            if "XAU" in sym_upper:
                return "OrderFlow"

        mapping = {
            "ta": "TechnicalAnalysis",
            "orderflow": "CryptoOrderFlow",  # default for crypto-heavy context
            "cryptoorderflow": "CryptoOrderFlow",
            "aggregated": "AggregatedHub-V2",
        }
        source_raw = mapping.get(s, strategy)
        return canon_source(source_raw)

    def send_periodic_report(self, window_seconds: int | None = None) -> None:
        """
        Отправляет периодические отчеты для всех найденных пар source/symbol.
        Используется для вызова из других сервисов (например, SignalPerformanceTracker).
        """
        try:
            logger.info("🔍 Начало поиска пар для периодических отчетов...")
            pairs = self._discover_pairs()

            if not pairs:
                logger.warning("⚠️ Пар для отчетов не найдено. Проверьте наличие данных в Redis")
                return

            logger.info(f"📊 Найдено {len(pairs)} пар для отчетов: {pairs[:5]}{'...' if len(pairs) > 5 else ''}")

            sent_count = 0
            skipped_count = 0
            for source, symbol in pairs:
               # Prepare title
                display_symbol = "ALLSYMBOLS" if symbol == "ALL" else symbol
                title = f"<b>📊 Отчет: {source} / {display_symbol}</b>"
                try:
                    lock_key = f"report_lock:{source}:{symbol}"
                    if not self._acquire_lock(lock_key, ttl=REPORT_LOCK_TTL_SECONDS):
                        logger.debug(f"⏭️ Пропуск {source}/{symbol} (lock занят)")
                        continue

                    try:
                        metrics = self._gather_window_metrics_stream(source, symbol, window_seconds=window_seconds)
                        total_trades = int(metrics.get("total_trades", 0))

                        # Пропускаем пары без сделок, чтобы не засорять Telegram пустыми отчетами
                        send_empty = os.getenv("PERIODIC_REPORT_SEND_EMPTY", "false").lower() == "true"
                        if total_trades <= 0 and not send_empty:
                            logger.debug(f"⏭️ Пропуск {source}/{symbol} (нет сделок в окне, send_empty={send_empty})")
                            skipped_count += 1
                            continue

                        logger.info(f"📤 Формирование отчета для {source}/{symbol} (сделок: {total_trades})")
                        self._send_report(source, symbol, metrics, window_seconds=window_seconds)
                        sent_count += 1
                    finally:
                        self._release_lock(lock_key)

                except Exception as e:
                    logger.error(f"❌ Ошибка при отправке отчета для {source}/{symbol}: {e}", exc_info=True)

            if sent_count > 0:
                logger.info(f"✅ Отправлено отчетов: {sent_count}/{len(pairs)} (пропущено: {skipped_count})")
            else:
                logger.warning(
                    f"⚠️ Не удалось отправить ни одного отчета из {len(pairs)} пар "
                    f"(пропущено: {skipped_count}). "
                    f"Возможные причины: нет сделок в окне {RECENT_WINDOW_SECONDS}s, "
                    f"или PERIODIC_REPORT_SEND_EMPTY=false"
                )

        except Exception as e:
            logger.error(f"❌ Ошибка в send_periodic_report: {e}", exc_info=True)

    def _get_validation_stats(self, source: str, symbol: str, window_seconds: int | None = None) -> tuple[float, int, dict[str, float], int, int]:
        """
        Вычисляет статистику валидации сигналов за период.
        Возвращает:
          - процент прохождения (float): passed / (passed + failed) * 100, не считая bypassed
          - общее количество сигналов (int)
          - статистику по отсутствующим ногам (Dict[str, float] - процент отсутствия для каждой ноги)
          - passed_count (int)
          - bypassed_count (int)
        """
        try:
            # Получаем сигналы из Redis streams
            window_sec = window_seconds or RECENT_WINDOW_SECONDS
            since_ms = get_ny_time_millis() - (window_sec * 1000)

            # Проверяем сигналы в streams
            of_inputs_stream = os.getenv("OF_INPUTS_STREAM", RS.OF_INPUTS)
            streams_to_check = [
                of_inputs_stream,
                f"signals:cryptoorderflow:{symbol}",
                RS.CRYPTO_RAW
            ]

            total_signals = 0
            passed_validation = 0
            failed_validation = 0
            bypassed_validation = 0
            missing_legs_counts: dict[str, int] = {}
            ok_fail_conds: dict[str, int] = {}
            ok_fail_reasons: dict[str, int] = {}
            # Score threshold for veto label — reads same ENV as engine
            _score_veto_min: float = float(os.getenv("OF_SCORE_MIN", "0.60"))
            _score_veto_label: str = f"score_veto (score<{_score_veto_min:.2f})"
            # Score threshold breakdown: {threshold_str: {"count": int, "passed": int, "failed": int}}
            _SCORE_THRESHOLDS = [0.40, 0.45, 0.50, 0.55, 0.60, 0.65, 0.70, 0.75]
            score_by_threshold: dict[str, dict[str, int]] = {
                f"{t:.2f}": {"count": 0, "passed": 0, "failed": 0} for t in _SCORE_THRESHOLDS
            }

            for stream in streams_to_check:
                try:
                    # Get messages within the window directly from Redis
                    min_id = f"{since_ms}-0"
                    messages = self.redis.xrevrange(stream, max="+", min=min_id, count=5000)
                    for msg_id, fields in messages:
                        try:
                            # Парсим timestamp из ID сообщения
                            ts_str = msg_id.split('-')[0]
                            msg_ts = int(ts_str)

                            if msg_ts < since_ms:
                                continue

                            # Check both 'payload' (raw stream) and 'data' (symbol stream) fields
                            payload_str = fields.get('payload') or fields.get('data') or '{}'
                            try:
                                payload = json.loads(payload_str)
                            except json.JSONDecodeError:
                                payload = {}

                            # Filter by symbol
                            sig_symbol = (payload.get('symbol') or '')
                            if symbol != 'ALL' and canon_symbol(sig_symbol) != symbol:
                                continue

                            # Filter by source/strategy
                            sig_source = payload.get('source') or payload.get('strategy') or ''
                            if source != 'ALL' and self._source_from_strategy(sig_source, sig_symbol) != self._source_from_strategy(source, sig_symbol):
                                continue

                            total_signals += 1

                            # Проверяем статус валидации
                            # Check explicitly for validation status first
                            v_status = (payload.get('validation_status') or "").lower()

                            # Fallback to indicators if validation_status not set (older signals)
                            indicators = payload.get('indicators') or {}
                            if not v_status:
                                of_ok = indicators.get("strong_gate_ok")
                                if of_ok is None:
                                    of_ok = indicators.get("of_confirm_ok")

                                if of_ok is None:
                                    rule = payload.get("rule") or {}
                                    if isinstance(rule, dict):
                                        of_ok = rule.get("ok")

                                if of_ok is None:
                                    oc = indicators.get("of_confirm") or {}
                                    if isinstance(oc, str):
                                        try:
                                            oc = json.loads(oc)
                                        except (ValueError, json.JSONDecodeError):
                                            oc = {}
                                    if isinstance(oc, dict):
                                        of_ok = oc.get("ok")

                                if of_ok is True or str(of_ok) == '1' or str(of_ok).lower() == 'true':
                                    v_status = 'passed'
                                elif of_ok is False or str(of_ok) == '0' or str(of_ok).lower() == 'false':
                                    v_status = 'failed'

                            if v_status == 'passed':
                                passed_validation += 1
                            elif v_status == 'failed':
                                failed_validation += 1
                                # —— ok=0 condition breakdown ——
                                try:
                                    gate = {}
                                    oc = indicators.get("of_confirm") or {}
                                    if isinstance(oc, str):
                                        try:
                                            oc = json.loads(oc)
                                        except Exception:
                                            oc = {}
                                    gate = oc if isinstance(oc, dict) and oc else (payload.get("rule") or {})
                                    if not isinstance(gate, dict):
                                        gate = {}
                                    def _ff(v):
                                        try: return float(v)
                                        except Exception: return 0.0
                                    # 1. score veto
                                    sc = gate.get("score") or indicators.get("of_score") or indicators.get("score")
                                    hv = gate.get("have")
                                    nd = gate.get("need")
                                    has_legs = False
                                    if hv is not None and nd is not None:
                                        with contextlib.suppress(Exception):
                                            has_legs = float(hv) >= float(nd)
                                    rsn = str(gate.get("reason") or payload.get("reason") or "")
                                    if sc is not None and _ff(sc) < _score_veto_min and (has_legs or "score_veto" in rsn):
                                        ok_fail_conds[_score_veto_label] = ok_fail_conds.get(_score_veto_label, 0) + 1
                                    # Populate score threshold breakdown (includes this failed signal)
                                    if sc is not None:
                                        sc_val = _ff(sc)
                                        for thr in _SCORE_THRESHOLDS:
                                            if sc_val >= thr:
                                                key = f"{thr:.2f}"
                                                score_by_threshold[key]["count"] += 1
                                                score_by_threshold[key]["failed"] += 1
                                    # 2. exec_risk cap
                                    ev2 = gate.get("evidence") or {}
                                    if not isinstance(ev2, dict): ev2 = {}
                                    ern = ev2.get("exec_risk_norm") or indicators.get("exec_risk_norm")
                                    if ern is not None and _ff(ern) >= 1.0:
                                        ok_fail_conds["exec_risk_cap (norm>=1.0)"] = ok_fail_conds.get("exec_risk_cap (norm>=1.0)", 0) + 1
                                    # 3. have < need
                                    hv = gate.get("have"); nd = gate.get("need")
                                    if hv is not None and nd is not None:
                                        try:
                                            if float(hv) < float(nd):
                                                ok_fail_conds["have<need (legs not met)"] = ok_fail_conds.get("have<need (legs not met)", 0) + 1
                                        except Exception: pass
                                    # 4. missing_legs
                                    mlg = gate.get("missing_legs") or payload.get("missing_legs")
                                    if isinstance(mlg, list) and mlg:
                                        k4 = f"missing_legs (top: {mlg[0]})"
                                        ok_fail_conds[k4] = ok_fail_conds.get(k4, 0) + 1
                                    # 5. scenario veto
                                    sc5 = str(gate.get("scenario_v4") or gate.get("scenario") or "")
                                    if sc5 in ("dn_veto", "meta_veto"):
                                        ok_fail_conds[f"scenario={sc5}"] = ok_fail_conds.get(f"scenario={sc5}", 0) + 1
                                    # 6. raw reason first token
                                    rsn = str(gate.get("reason") or payload.get("reason") or "")
                                    if rsn:
                                        rt = rsn.split(",")[0].split(";")[0].strip()[:80]
                                        if rt: ok_fail_reasons[rt] = ok_fail_reasons.get(rt, 0) + 1
                                except Exception:
                                    pass
                            elif v_status == 'bypassed' or not v_status:
                                bypassed_validation += 1

                            # Score threshold breakdown for passed signals (score was OK)
                            if v_status == 'passed':
                                try:
                                    indicators_p = payload.get('indicators') or {}
                                    gate_p: dict = {}
                                    oc_p = indicators_p.get("of_confirm") or {}
                                    if isinstance(oc_p, str):
                                        try:
                                            oc_p = json.loads(oc_p)
                                        except Exception:
                                            oc_p = {}
                                    gate_p = oc_p if isinstance(oc_p, dict) and oc_p else (payload.get("rule") or {})
                                    if not isinstance(gate_p, dict):
                                        gate_p = {}
                                    sc_p = gate_p.get("score") or indicators_p.get("of_score") or indicators_p.get("score")
                                    if sc_p is not None:
                                        try:
                                            sc_pv = float(sc_p)
                                        except (TypeError, ValueError):
                                            sc_pv = None
                                        if sc_pv is not None:
                                            for thr in _SCORE_THRESHOLDS:
                                                if sc_pv >= thr:
                                                    key = f"{thr:.2f}"
                                                    score_by_threshold[key]["count"] += 1
                                                    score_by_threshold[key]["passed"] += 1
                                except Exception:
                                    pass

                            # Collect missing legs stats
                            # We look at 'strong_gate_legs' in indicators.
                            # It is expected to be a dict like {"A": 1, "B": 0, "C": 1} where 0 means missing.
                            gate_legs = indicators.get("strong_gate_legs")
                            if isinstance(gate_legs, dict):
                                for leg, val in gate_legs.items():
                                    try:
                                        if int(val) == 0:
                                            missing_legs_counts[leg] = missing_legs_counts.get(leg, 0) + 1
                                    except (ValueError, TypeError):
                                        pass

                        except (ValueError, json.JSONDecodeError):
                            continue

                except Exception as e:
                    logger.debug(f"Error checking stream {stream}: {e}")
                    continue

            if total_signals <= 0:
                return 0.0, 0, {}, 0, 0, {}, {}

            # Calculate pass rate only over decided signals (ignoring bypassed)
            # bypassed = gate not evaluated (Shadow mode / no OFConfirm run)
            decided = passed_validation + failed_validation
            pass_rate = (float(passed_validation) / float(decided)) * 100.0 if decided > 0 else 0.0

            missing_stats_pct = {}
            for leg, count in missing_legs_counts.items():
                missing_stats_pct[leg] = (float(count) / float(total_signals)) * 100.0

            # Build ok_fail_breakdown
            ok_fail_breakdown: dict[str, int] = dict(ok_fail_conds)
            for rs, cnt in sorted(ok_fail_reasons.items(), key=lambda kv: -kv[1])[:8]:
                ok_fail_breakdown[f"reason: {rs}"] = cnt

            return pass_rate, total_signals, missing_stats_pct, passed_validation, bypassed_validation, ok_fail_breakdown, score_by_threshold

        except Exception as e:
            logger.warning(f"Error calculating validation stats for {source}/{symbol}: {e}")
            return 0.0, 0, {}, 0, 0, {}, {}

    def send_daily_report(self) -> None:
        """
        Отправляет ежедневный отчет (за последние 24 часа).
        """
        logger.info("📅 Запуск ежедневной рассылки (окно 24ч)...")
        self.send_periodic_report(window_seconds=86400)

    def _discover_pairs(self) -> list[tuple[str, str]]:
        """Обнаружить пары source/symbol из разных источников."""
        try:
            pairs = []
            seen_pairs = set()

            # 1) Получаем все стратегии из stats:strategies
            try:
                strategies = self.redis.smembers("stats:strategies") or set()
                for strategy in strategies:
                    if not strategy:
                        continue
                    str_strategy = strategy if isinstance(strategy, bytes) else strategy

                    # Получаем символы для стратегии
                    symbols_key = f"stats:symbols:{str_strategy}"
                    symbols = self.redis.smembers(symbols_key) or set()

                    for symbol in symbols:
                        if not symbol:
                            continue
                        str_symbol = symbol if isinstance(symbol, bytes) else symbol
                        # Преобразуем strategy в source для корректного маппинга
                        source = self._source_from_strategy(str_strategy, str_symbol)
                        symbol_norm = canon_symbol(str_symbol)
                        pair = (source, symbol_norm)
                        if pair not in seen_pairs:
                            seen_pairs.add(pair)
                            pairs.append(pair)
            except Exception as e:
                logger.debug(f"⚠️ Ошибка при чтении stats:strategies: {e}")

            # 2) Пробуем из stream trades:closed
            if len(pairs) < 500:  # Если мало пар, дополняем из stream (Limit increased to 500)
                try:
                    entries = self.redis.xrevrange(RS.TRADES_CLOSED, max="+", count=2000) or []
                    logger.debug(f"🔍 Проверяю trades:closed stream, найдено {len(entries)} записей")

                    for _, fields in entries:
                        if not fields:
                            continue
                        t = {str(k): str(v) for k, v in fields.items()}
                        source_raw = t.get("strategy") or t.get("source") or "unknown"
                        symbol_raw = t.get("symbol") or "UNKNOWN"

                        if not source_raw or source_raw == "unknown" or symbol_raw == "UNKNOWN":
                            continue

                        symbol = canon_symbol(symbol_raw)
                        source = self._source_from_strategy(source_raw, symbol)

                        pair = (source, symbol)
                        if pair not in seen_pairs:
                            seen_pairs.add(pair)
                            pairs.append(pair)
                            if len(pairs) >= 500:  # Limit increased
                                break
                except Exception as e:
                    logger.warning(f"⚠️ Ошибка при поиске пар в trades:closed stream: {e}")

            # 3) Пробуем из orders:open (открытые позиции)
            if len(pairs) < 500:  # Limit increased
                try:
                    order_ids = list(self.redis.smembers("orders:open") or set())[:50]
                    logger.debug(f"🔍 Проверяю orders:open, найдено {len(order_ids)} открытых позиций")

                    for oid_raw in order_ids:
                        oid = str(oid_raw) if not isinstance(oid_raw, bytes) else oid_raw.decode('utf-8')
                        if not oid:
                            continue
                        order_key = f"order:{oid}"
                        order_data_raw = self.redis.hgetall(order_key) or {}
                        order_data = _norm_map(order_data_raw)

                        if not order_data:
                            continue

                        source_raw = order_data.get("strategy") or order_data.get("source") or "unknown"
                        symbol_raw = order_data.get("symbol") or "UNKNOWN"

                        if source_raw == "unknown" or symbol_raw == "UNKNOWN":
                            continue

                        symbol = canon_symbol(symbol_raw)
                        source = self._source_from_strategy(source_raw, symbol)

                        pair = (source, symbol)
                        if pair not in seen_pairs:
                            seen_pairs.add(pair)
                            pairs.append(pair)
                            if len(pairs) >= 500:
                                break
                except Exception as e:
                    logger.debug(f"⚠️ Ошибка при чтении orders:open: {e}")

            # 4) Пробуем из signals streams (последние сигналы)
            if len(pairs) < 500:  # Limit increased
                try:
                    signal_keys = []
                    for pattern in [
                        "signals:orderflow:*", "signals:OrderFlow:*",
                        "signals:cryptoorderflow:*", "signals:CryptoOrderFlow:*",
                        "signals:ta:*", "signals:TechnicalAnalysis:*",
                        "signals:aggregated:*", "signals:AggregatedHub-V2:*"
                    ]:
                        # Increased scan count and limit to avoid missing keys
                        keys = list(self.redis.scan_iter(match=pattern, count=10000))[:500]
                        signal_keys.extend(keys)

                    logger.debug(f"🔍 Проверяю signals streams, найдено {len(signal_keys)} ключей")

                    for key in signal_keys[:1000]:  # Increased limit
                        try:
                            key_str = str(key) if isinstance(key, bytes) else key
                            # Извлекаем symbol из ключа: signals:orderflow:
                            parts = key_str.split(":")
                            if len(parts) >= 3:
                                strategy_part = parts[1].lower()
                                symbol_raw = parts[2]

                                # Используем единый метод для маппинга
                                source = self._source_from_strategy(strategy_part, symbol_raw)
                                symbol = canon_symbol(symbol_raw)

                                pair = (source, symbol)
                                if pair not in seen_pairs:
                                    seen_pairs.add(pair)
                                    pairs.append(pair)
                                    if len(pairs) >= 500:
                                        break
                        except Exception:
                            continue
                except Exception as e:
                    logger.debug(f"⚠️ Ошибка при чтении signals streams: {e}")

            if pairs:
                logger.info(f"✅ Найдено {len(pairs)} пар для отчетов: {pairs[:5]}{'...' if len(pairs) > 5 else ''}")
            else:
                logger.warning(
                    "⚠️ Не найдено ни одной пары source/symbol для отчетов. "
                    "Проверьте наличие данных в Redis: "
                    "stats:strategies, trades:closed stream, orders:open, signals:* streams"
                )

            # 5) Добавляем "ALL" для каждого найденного источника
            sources = set(p[0] for p in pairs)
            for src in sources:
                if (src, "ALL") not in seen_pairs:
                    seen_pairs.add((src, "ALL"))
                    pairs.append((src, "ALL"))

            return pairs
        except Exception as e:
            logger.error(f"❌ Ошибка обнаружения пар: {e}", exc_info=True)
            return []

    def _generate_decision_recommendations(self, global_analysis: dict[str, any], by_tag_analysis: list[dict[str, any]]) -> list[str]:
        """
        Генерирует рекомендации по настройкам трейлинга на основе анализа baseline vs managed.
        """
        import html
        sections = []

        if not global_analysis:
            return sections

        # Анализ глобальных метрик
        delta_exp_r = global_analysis.get("delta_expectancy_r", 0)
        share_better = global_analysis.get("share_better", 0)
        share_worse = global_analysis.get("share_worse", 0)
        trailing_share = global_analysis.get("trailing_share", 0)

        sections.append("")
        sections.append("<b>🧠 РЕКОМЕНДАЦИИ ПО НАСТРОЙКАМ:</b>")

        # Глобальные рекомендации
        if delta_exp_r > 0.05 and share_better > 0.55:
            sections.append(f"✅ <b>Трейлинг полезен</b> (ΔExp_R={delta_exp_r:+.3f}, better={share_better:.1%})")
            sections.append("   → Оставить текущие настройки или усилить")
        elif delta_exp_r < -0.05 and share_worse > 0.55:
            sections.append(f"⚠️  <b>Трейлинг вреден</b> (ΔExp_R={delta_exp_r:+.3f}, worse={share_worse:.1%})")
            sections.append("   → Ослабить или отключить трейлинг")
        else:
            sections.append(f"🤔 <b>Нейтральный эффект</b> (ΔExp_R={delta_exp_r:+.3f})")
            sections.append("   → Продолжить мониторинг")

        # Рекомендации по trailing share
        if trailing_share > 0.8:
            sections.append(f"⚡ <b>Высокий trailing share</b> ({trailing_share:.1%}) - рассмотреть снижение")
        elif trailing_share < 0.3:
            sections.append(f"🐌 <b>Низкий trailing share</b> ({trailing_share:.1%}) - можно увеличить")

        # Анализ по топ тегам
        if by_tag_analysis:
            tag_recommendations = []
            for tag_stats in by_tag_analysis[:3]:  # топ 3 тега
                tag_name = html.escape(tag_stats.get('tag', 'unknown')[:15])
                tag_delta = tag_stats.get('delta_expectancy_r', 0)
                tag_better = tag_stats.get('share_better', 0)
                tag_worse = tag_stats.get('share_worse', 0)
                tag_n = tag_stats.get('n', 0)

                if tag_n >= 10:  # минимум 10 сделок для значимости
                    if tag_delta > 0.1 and tag_better > 0.6:
                        tag_recommendations.append(f"✅ <b>{tag_name}</b>: усилить трейлинг (Δ={tag_delta:+.3f})")
                    elif tag_delta < -0.05 and tag_worse > 0.5:
                        tag_recommendations.append(f"⚠️  <b>{tag_name}</b>: ослабить трейлинг (Δ={tag_delta:+.3f})")

            if tag_recommendations:
                sections.append("")
                sections.append("<b>🏷️ По паттернам:</b>")
                sections.extend(tag_recommendations[:2])  # показываем топ 2 рекомендации

        return sections


def _normalize_ts_ms(ts: int) -> int:
    """
    Нормализует timestamp в миллисекунды.
    - секунды  (1e9 .. 1e10)  → × 1000
    - миллисекунды (1e10..1e14) → как есть
    - микросекунды (> 1e14)   → // 1000
    Паритет с trade_metrics_service._normalize_ts_ms.
    """
    if ts <= 0:
        return ts
    if 0 < ts < 10_000_000_000:          # секунды
        return ts * 1000
    if ts > 100_000_000_000_000:         # микросекунды → ms
        return ts // 1000
    return ts                             # уже ms


def main():
    """Главная функция для запуска периодического репортера."""
    import signal

    logger.info("=" * 70)
    logger.info("📊 Periodic Reporter Service")
    logger.info("=" * 70)
    logger.info(f"Redis URL: {REDIS_URL}")
    logger.info(f"Window: {RECENT_WINDOW_SECONDS}s ({RECENT_WINDOW_SECONDS // 60} мин)")
    logger.info(f"Trigger count: {REPORT_TRIGGER_COUNT} (Interval: {os.getenv('PERIODIC_REPORT_CHECK_INTERVAL_SEC', '0')})")
    logger.info("=" * 70)

    reporter = PeriodicReporter()

    # Graceful shutdown
    stop_flag = {"running": True}

    def signal_handler(signum, frame):
        logger.info(f"⚠️ Получен сигнал {signum}, завершение работы...")
        stop_flag["running"] = False

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)


    # Периодическая отправка отчетов
    last_check_time = time.time()
    check_interval = int(os.getenv("PERIODIC_REPORT_CHECK_INTERVAL_SEC", "0"))  # 0 = таймер отключен, работаем от триггера сделок

    if check_interval <= 0:
        logger.info("⏸️ Таймер периодических проверок отключен (PERIODIC_REPORT_CHECK_INTERVAL_SEC<=0). "
                    "Отчеты отправляются кратно количеству сделок (REPORT_TRIGGER_COUNT).")
    else:
        logger.info(f"🔄 Начало работы, проверка каждые {check_interval}с")

    # Init Edge Gate Reporter
    edge_gate_dsn = (os.getenv("ANALYTICS_DB_DSN") or os.getenv("TRADES_DB_DSN"))
    edge_gate_interval = int(os.getenv("EDGE_GATE_REPORT_INTERVAL_SEC", "3600"))
    edge_gate_reporter = None
    last_edge_gate_report_time = 0

    if edge_gate_dsn and EdgeGateReporter:
        try:
            logger.info(f"🛡️ Configured Edge Gate Reporter (interval={edge_gate_interval}s)")
            # lookback defaults to 6h in class, or we can maximize it. 6h is fine.
            eg_cfg = EdgeGateReportConfig(db_dsn=edge_gate_dsn)
            edge_gate_reporter = EdgeGateReporter(eg_cfg)
        except Exception as e:
            logger.error(f"❌ Failed to init Edge Gate Reporter: {e}")

    # Init Daily Report Scheduler
    daily_report_enabled = os.getenv("DAILY_REPORT_ENABLED", "true").lower() == "true"
    daily_report_hour = int(os.getenv("DAILY_REPORT_UTC_HOUR", "17"))
    daily_report_minute = int(os.getenv("DAILY_REPORT_UTC_MINUTE", "5"))

    # "ALL" in daily_report_symbols means we discover pairs dynamically.
    daily_report_symbols_env = os.getenv("DAILY_REPORT_SYMBOLS", "ALL").strip()
    if daily_report_symbols_env.upper() == "ALL":
        daily_report_symbols = "ALL"
    else:
        daily_report_symbols = [s.strip() for s in daily_report_symbols_env.split(",") if s.strip()]

    daily_report_source = os.getenv("DAILY_REPORT_SOURCE", "CryptoOrderFlow")
    # Initialize to today so we DON'T do catch-up on restart.
    # Daily report will fire only at next scheduled time (e.g. 17:00 UTC tomorrow if started after 17:00).
    # Set to None only if DAILY_REPORT_CATCHUP=true.
    if os.getenv("DAILY_REPORT_CATCHUP", "false").lower() in ("1", "true", "yes"):
        last_daily_report_date = None  # catch-up mode: send immediately if time has passed
    else:
        from datetime import date as _date
        last_daily_report_date = _date.today()  # no catch-up: wait for next scheduled send

    # Init Hourly Report State
    # Initialize to -1: fires at next XX:00 (correct behavior).
    # Per-pair Redis dedup prevents actual duplicate sends even after restart.
    last_hourly_report_hour = -1

    if daily_report_enabled:
        logger.info(f"📅 Daily Report Scheduler enabled: {daily_report_hour:02d}:{daily_report_minute:02d} UTC")
        logger.info(f"   Symbols: {daily_report_symbols}")
        logger.info(f"   Source: {daily_report_source}")
    else:
        logger.info("📅 Daily Report Scheduler disabled")

    while stop_flag["running"]:
        try:
            now = time.time()

            # --- Edge Gate Reporting ---
            if edge_gate_reporter and (now - last_edge_gate_report_time >= edge_gate_interval):
                logger.info("🛡️ Running periodic Edge Gate report...")
                try:
                    edge_gate_reporter.generate_and_send()
                except Exception as e:
                    logger.error(f"❌ Error in Edge Gate Reporter loop: {e}")

                last_edge_gate_report_time = now

            # --- Daily Report Scheduling ---
            if daily_report_enabled:
                try:
                    current_utc = datetime.now(UTC)
                    current_date = current_utc.date()
                    current_time = current_utc.time()

                    # Create scheduled time for today
                    from datetime import time as dt_time
                    scheduled_time = dt_time(daily_report_hour, daily_report_minute)

                    # Check if we should send daily report:
                    # 1. Current time has passed the scheduled time
                    # 2. We haven't sent a report today yet
                    should_send = (
                        current_time >= scheduled_time and
                        last_daily_report_date != current_date
                    )

                    if should_send:
                        logger.info(f"📅 Daily report trigger activated at {current_utc.strftime('%Y-%m-%d %H:%M:%S')} UTC")

                        target_pairs = []
                        if daily_report_symbols == "ALL":
                             # Discover all pairs + filter by source if possible or just use discovered source
                            discovered = reporter._discover_pairs()
                            target_pairs = discovered
                        else:
                            # Use configured symbols with fixed source
                            for s in daily_report_symbols:
                                target_pairs.append((daily_report_source, s))

                        count_sent = 0
                        for src, sym in target_pairs:
                            try:
                                # Dedup check specifically for this pair/date to be safe against restarts mid-process
                                # (though last_daily_report_date handles the main loop, individual checks help if process crashed)
                                day_key = f"report_last_daily_date:{src}:{sym}"
                                last_date_val = reporter.redis.get(day_key)
                                today_str = current_date.strftime("%Y-%m-%d")

                                if last_date_val == today_str:
                                    continue

                                logger.info(f"📤 Sending daily report for {src}/{sym} (24h window)")
                                reporter.send_report_for_pair(src, sym, window_seconds=86400)
                                reporter.redis.set(day_key, today_str)
                                count_sent += 1
                            except Exception as e:
                                logger.error(f"❌ Error sending daily report for {src}/{sym}: {e}", exc_info=True)

                        # Mark today as reported globally (loop finished)
                        last_daily_report_date = current_date
                        logger.info(f"✅ Daily reports completed ({count_sent} sent). Next scheduled: {current_date + __import__('datetime').timedelta(days=1)} {scheduled_time}")

                except Exception as e:
                    logger.error(f"❌ Error in daily report scheduler: {e}", exc_info=True)

            # --- Hourly Report Scheduling ---
            # Strict hourly report at XX:00 for ALL pairs
            hourly_report_enabled = os.getenv("HOURLY_REPORT_ENABLED", "true").lower() == "true"
            if hourly_report_enabled:
                try:
                    current_utc = datetime.now(UTC)

                    if current_utc.minute == 0 and current_utc.hour != last_hourly_report_hour:
                        logger.info(f"🕐 Hourly report trigger activated at {current_utc.strftime('%H:%M')} UTC")
                        last_hourly_report_hour = current_utc.hour

                        hourly_pairs = reporter._discover_pairs()

                        count_sent_hourly = 0
                        for src, sym in hourly_pairs:
                            try:
                                # Унифицированные ключи такие же как в _check_and_trigger_report
                                hour_key = f"report_last_hourly_hour:{src}:{sym}"
                                current_hour_str = current_utc.strftime("%Y-%m-%d-%H")
                                last_hour_val = reporter.redis.get(hour_key)

                                if last_hour_val == current_hour_str:
                                    logger.debug(f"⏭️ Skipping hourly report for {src}/{sym} (already sent for {current_hour_str})")
                                    continue

                                # Также проверяем timestamp для полной уверенности
                                last_ts_key = f"report_last_ts:{src}:{sym}"
                                try:
                                    last_ts = float(reporter.redis.get(last_ts_key) or 0)
                                except Exception:
                                    last_ts = 0.0

                                if (time.time() - last_ts) < 1800 and last_hour_val is not None:
                                    continue

                                logger.info(f"📤 Sending hourly report for {src}/{sym} (60m window via main loop)")
                                reporter.send_report_for_pair(src, sym, window_seconds=3600, silent_locked=True)
                                # send_report_for_pair вызывает _generate_and_send_report_internal,
                                # но НЕ проставляет ключи hour_key/last_ts_key. Проставим их здесь.
                                reporter.redis.set(hour_key, current_hour_str, ex=172800)
                                reporter.redis.set(last_ts_key, str(time.time()), ex=172800)
                                count_sent_hourly += 1
                            except Exception as e:
                                logger.error(f"❌ Error sending hourly report for {src}/{sym}: {e}")

                        logger.info(f"✅ Hourly reports completed ({count_sent_hourly} sent).")

                except Exception as e:
                    logger.error(f"❌ Error in hourly report scheduler: {e}", exc_info=True)


            if check_interval <= 0:

                # Таймер отключен, ждем внешних триггеров (check_and_trigger_report)
                time.sleep(10)
                continue

            now = time.time()

            # Периодическая проверка и отправка отчетов
            if now - last_check_time >= check_interval:
                logger.info("🔍 Проверка пар для отправки отчетов...")
                pairs = reporter._discover_pairs()

                if pairs:
                    logger.info(f"📊 Найдено {len(pairs)} пар: {pairs[:5]}{'...' if len(pairs) > 5 else ''}")

                    sent_count = 0
                    for source, symbol in pairs:
                        try:
                            # Проверяем, есть ли новые сделки для отчета
                            lock_key = f"report_lock:{source}:{symbol}"
                            if not reporter._acquire_lock(lock_key, ttl=REPORT_LOCK_TTL_SECONDS):
                                logger.debug(f"⏭️ Пропуск {source}/{symbol} (lock занят)")
                                continue

                            try:
                                metrics = reporter._gather_window_metrics_stream(source, symbol)
                                total_trades = int(metrics.get("total_trades", 0))

                                # Пропускаем пустые отчеты (как в send_periodic_report())
                                send_empty = os.getenv("PERIODIC_REPORT_SEND_EMPTY", "false").lower() == "true"
                                if total_trades <= 0 and not send_empty:
                                    logger.debug(f"⏭️ Пропуск {source}/{symbol} (нет сделок в окне)")
                                    continue

                                logger.info(f"📤 Формирование отчета для {source}/{symbol} (сделок: {total_trades})")
                                reporter._send_report(source, symbol, metrics, window_seconds=RECENT_WINDOW_SECONDS)
                                sent_count += 1
                            finally:
                                reporter._release_lock(lock_key)

                        except Exception as e:
                            logger.error(f"❌ Ошибка при отправке отчета для {source}/{symbol}: {e}", exc_info=True)

                    if sent_count > 0:
                        logger.info(f"✅ Отправлено отчетов: {sent_count}/{len(pairs)}")
                    else:
                        logger.warning(f"⚠️ Не удалось отправить ни одного отчета из {len(pairs)} пар")
                else:
                    logger.warning("⚠️ Пар для отчетов не найдено. Проверьте наличие данных в Redis (stats:strategies, trades:closed, orders:open, signals:* streams)")

                last_check_time = now
            else:
                # Короткий сон между проверками
                time.sleep(10)

        except KeyboardInterrupt:
            logger.info("⚠️ KeyboardInterrupt, завершение...")
            break
        except Exception as e:
            logger.error(f"❌ Критическая ошибка в main loop: {e}", exc_info=True)
            time.sleep(60)  # Долгая пауза после ошибки

    logger.info("✅ Periodic Reporter завершен")


if __name__ == "__main__":
    main()
