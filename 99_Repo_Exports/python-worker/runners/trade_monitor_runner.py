# runners/trade_monitor_runner.py
from __future__ import annotations
from utils.time_utils import get_epoch_ms

import json
import os
import signal as signal_mod
import sys
import time
import threading
from typing import Dict, List, Tuple

import redis
from prometheus_client import start_http_server, Gauge, Counter

from common.log import setup_logger, set_trace_id, clear_trace_id
from core.redis_client import get_redis

from services.redis_streams_runtime import (
    ensure_group,
    discover_streams,
    xreadgroup_multi,
    autoclaim_stale,
    StreamMsg,
)
from domain.time_utils import normalize_ts_ms

from services.trade_monitor import TradeMonitorService
from services.auto_calibration_service import init_auto_calibration

# Wrap in a registry check to avoid "Duplicated timeseries" during test collection 
from prometheus_client import REGISTRY
def _get_or_create_metric(collector_type, name, documentation, labelnames=()):
    # Check for name or name_total (Prometheus appends _total for Counters)
    for n in [name, name + "_total"]:
        if n in REGISTRY._names_to_collectors:
            return REGISTRY._names_to_collectors[n]
    return collector_type(name, documentation, labelnames=labelnames)

# Prometheus metrics for observability
trade_monitor_loop_age_seconds = _get_or_create_metric(Gauge, 'trade_monitor_loop_age_seconds', 'Epoch timestamp of the last TradeMonitor loop cycle start')
# NOTE: 'trade_monitor_open_positions_total' renamed to 'trade_monitor_open_positions' as gauges shouldn't have _total suffix.
trade_monitor_open_positions = _get_or_create_metric(Gauge, 'trade_monitor_open_positions', 'Current number of open positions in trade-monitor memory')
trade_monitor_signals_skipped = _get_or_create_metric(Counter, 'trade_monitor_signals_skipped_total', 'Total signals skipped due to cap/age/timeout')

XACK_FAIL_REASON_TOTAL = _get_or_create_metric(
    Counter,
    "xack_fail_reason_total",
    "Total xack failures",
    ["stream", "reason"]
)
DLQ_WRITE_FAIL_TOTAL = _get_or_create_metric(
    Counter,
    "tm_dlq_write_fail_total",
    "Total failures writing messages to the trade-monitor DLQ",
    []
)

log = setup_logger("trade-monitor-runner")

REDIS_URL = os.getenv("REDIS_URL", "")
GROUP = os.getenv("TM_GROUP", "trade-monitor")
CONSUMER = os.getenv("TM_CONSUMER", f"tm-{os.getpid()}")

SIGNAL_PATTERNS = [p.strip() for p in os.getenv("TM_SIGNAL_STREAM_PATTERNS", "signals:*,stream:trade:entry_audit").split(",") if p.strip()]
TICK_PATTERNS = [p.strip() for p in os.getenv("TM_TICK_STREAM_PATTERNS", "stream:tick_*").split(",") if p.strip()]

READ_COUNT = int(os.getenv("TM_READ_COUNT", "200"))
READ_COUNT_TICKS = int(os.getenv("TM_READ_COUNT_TICKS", "2000"))
BLOCK_MS = int(os.getenv("TM_BLOCK_MS", "1000"))

RESCAN_EVERY_SEC = int(os.getenv("TM_RESCAN_EVERY_SEC", "15"))
CLAIM_EVERY_SEC = int(os.getenv("TM_CLAIM_EVERY_SEC", "10"))
MIN_IDLE_MS = int(os.getenv("TM_MIN_IDLE_MS", "60000"))

MAX_RETRIES = int(os.getenv("TM_MAX_RETRIES", "20"))
RETRY_BACKOFF_MS = int(os.getenv("TM_RETRY_BACKOFF_MS", "150"))
DLQ_STREAM = os.getenv("TM_DLQ_STREAM", "dlq:trade-monitor")

RETRY_HASH_PREFIX = os.getenv("TM_RETRY_HASH_PREFIX", "retries:tm")  # retries:tm:{stream}:{id}

# ── Anti-hang protection ──
# Max open positions in memory before skipping new signals
MAX_OPEN_POSITIONS = int(os.getenv("TM_MAX_OPEN_POSITIONS", "500"))
# Max age of signal (ms) to accept at startup (skip stale backlog)
MAX_SIGNAL_AGE_MS = int(os.getenv("TM_MAX_SIGNAL_AGE_MS", "300000"))  # 5 min
# Per-signal processing timeout (sec)
SIGNAL_TIMEOUT_SEC = int(os.getenv("TM_SIGNAL_TIMEOUT_SEC", "10"))
# Heartbeat interval
HEARTBEAT_EVERY_SEC = int(os.getenv("TM_HEARTBEAT_EVERY_SEC", "30"))


def _retry_key(stream: str, msg_id: str) -> str:
    return f"{RETRY_HASH_PREFIX}:{stream}:{msg_id}"


def _parse_signal(fields: Dict[str, str]) -> Dict:
    """
    Outbox protocol (fixed by Lua script):
      XADD ... 'data' <envelope_json>

    Поэтому в outbox stream всегда ожидаем:
      fields['data'] = JSON string (полный envelope)

    Backward/defensive:
      - если пришли "плоские" поля (редко) -> собираем dict как есть
      - если fields bytes -> декодируем
      - если JSON вложен в fields['data'] как dict/строка -> нормализуем

    Fail-open:
      - на любой ошибке возвращаем максимально безопасный dict,
        чтобы монитор не падал и не терял поток.
    """
    try:
        # Redis может вернуть bytes
        f: Dict[str, str] = {}
        for k, v in dict(fields or {}).items():
            kk = k.decode("utf-8", errors="ignore") if isinstance(k, (bytes, bytearray)) else str(k)
            vv = v.decode("utf-8", errors="ignore") if isinstance(v, (bytes, bytearray)) else str(v)
            f[kk] = vv

        out_obj = f.copy()

        # Parse 'data' or 'envelope_json'
        for env_key in ("data", "envelope_json"):
            if env_key in f and f[env_key]:
                try:
                    obj = json.loads(f[env_key])
                    if isinstance(obj, dict):
                        out_obj.update(obj)
                except Exception:
                    pass

        # Flatten nested 'payload' or parse 'payload_json'
        if "payload" in out_obj:
            p = out_obj["payload"]
            if isinstance(p, str):
                try:
                    p = json.loads(p)
                except Exception:
                    p = {}
            if isinstance(p, dict):
                out_obj.update(p)

        if "payload_json" in f and f["payload_json"]:
            try:
                p = json.loads(f["payload_json"])
                if isinstance(p, dict):
                    out_obj.update(p)
            except Exception:
                pass

        return out_obj
    except Exception:
        return {}


def _parse_tick(fields: Dict[str, str]) -> Dict:
    # ожидаем хотя бы symbol и price/bid/ask/last + ts
    out = dict(fields)
    # нормализуем ts
    if "ts" in out:
        try:
            out["ts"] = int(float(out["ts"]))
        except Exception:
            pass
    return out


def _xack(r: redis.Redis, stream: str, msg_id: str) -> None:
    try:
        r.xack(stream, GROUP, msg_id)
    except Exception as e:
        try:
            XACK_FAIL_REASON_TOTAL.labels(stream=stream, reason=type(e).__name__).inc()
        except Exception:
            pass


def _to_dlq(r: redis.Redis, msg: Dict) -> None:
    try:
        r.xadd(DLQ_STREAM, {k: json.dumps(v, ensure_ascii=False) if isinstance(v, (dict, list)) else str(v) for k, v in msg.items()}, maxlen=200000)
    except Exception as e:  # Fix #6: was silent
        log.exception("DLQ write failed stream=%s id=%s: %r", msg.get("stream"), msg.get("id"), e)
        try:
            DLQ_WRITE_FAIL_TOTAL.inc()
        except Exception:
            pass


# ── Platform capability check: signal.alarm() is POSIX-only ─────────────────────
# On Windows (and other non-POSIX) signal.SIGALRM does not exist.
# We detect this at import-time and warn once so the operator knows
# per-signal timeout protection is inactive on this platform.
_HAS_SIGALRM: bool = hasattr(signal_mod, "SIGALRM")
if not _HAS_SIGALRM and SIGNAL_TIMEOUT_SEC > 0:
    import warnings
    warnings.warn(
        "trade-monitor-runner: signal.alarm() (SIGALRM) is unavailable on this platform "
        f"(platform={sys.platform}). Per-signal timeout protection is DISABLED even though "
        f"TM_SIGNAL_TIMEOUT_SEC={SIGNAL_TIMEOUT_SEC}. "
        "Set TM_SIGNAL_TIMEOUT_SEC=0 to suppress this warning.",
        RuntimeWarning,
        stacklevel=1,
    )
    log.warning(
        "trade-monitor-runner: SIGALRM unavailable on %s — per-signal timeout inactive. "
        "Set TM_SIGNAL_TIMEOUT_SEC=0 to suppress.",
        sys.platform,
    )

class _SignalTimeout(Exception):
    """Raised when signal processing exceeds timeout."""
    pass


def _timeout_handler(signum, frame):
    raise _SignalTimeout("Signal processing timed out")


# Global stats for heartbeat
_stats = {
    "signals_processed": 0,
    "signals_skipped_cap": 0,
    "signals_skipped_age": 0,
    "ticks_processed": 0,
    "last_heartbeat": 0.0,
}


def _should_skip_signal(monitor: TradeMonitorService, raw_sig: Dict) -> str:
    """
    Returns skip reason or empty string if signal should be processed.
    Protects against: (1) too many open positions, (2) stale signals at startup.
    """
    # Cap on open positions to prevent memory/CPU overload
    try:
        n_open = len(monitor.open_positions)
        if n_open >= MAX_OPEN_POSITIONS:
            return f"cap_exceeded({n_open}/{MAX_OPEN_POSITIONS})"
    except Exception:
        pass

    # Skip stale signals (older than MAX_SIGNAL_AGE_MS)
    try:
        ts_ms = int(float(raw_sig.get("ts_ms") or raw_sig.get("ts") or 0))
        if ts_ms > 0:
            now_ms = get_epoch_ms()
            age_ms = now_ms - ts_ms
            if age_ms > MAX_SIGNAL_AGE_MS:
                return f"stale({age_ms}ms>{MAX_SIGNAL_AGE_MS}ms)"
    except Exception:
        pass

    return ""


def _process_one(r: redis.Redis, monitor: TradeMonitorService, m: StreamMsg, is_tick: bool) -> None:
    rk = _retry_key(m.stream, m.msg_id)

    try:
        if is_tick:
            raw_tick = _parse_tick(m.fields)
            trace_id = str(raw_tick.get("trace_id", ""))
            if trace_id:
                set_trace_id(trace_id)
            monitor.on_tick(raw_tick)
            _stats["ticks_processed"] += 1
        else:
            raw_sig = _parse_signal(m.fields)
            trace_id = str(raw_sig.get("trace_id", ""))
            if trace_id:
                set_trace_id(trace_id)
            if "entry_audit" in m.stream:
                monitor.on_audit(raw_sig)
            else:
                # ── Anti-hang: skip signals that would overload the monitor ──
                skip_reason = _should_skip_signal(monitor, raw_sig)
                if skip_reason:
                    _stats["signals_skipped_cap" if "cap" in skip_reason else "signals_skipped_age"] += 1
                    trade_monitor_signals_skipped.inc()
                    # ACK so we don't re-process stale signals forever
                    _xack(r, m.stream, m.msg_id)
                    return

                # ── Per-signal timeout protection ────────────────────────────────────────────────────
                # POSIX only: signal.alarm() only works in the main thread
                # and only on platforms that have SIGALRM (i.e. not Windows).
                # _HAS_SIGALRM is evaluated once at module load; on non-POSIX
                # the branch is simply skipped (warning already emitted at startup).
                use_timeout = (
                    _HAS_SIGALRM
                    and SIGNAL_TIMEOUT_SEC > 0
                    and (threading.current_thread() is threading.main_thread())
                )
                prev_handler = None
                if use_timeout:
                    prev_handler = signal_mod.signal(signal_mod.SIGALRM, _timeout_handler)  # type: ignore[attr-defined]
                    signal_mod.alarm(SIGNAL_TIMEOUT_SEC)  # type: ignore[attr-defined]
                try:
                    monitor.on_signal(raw_sig)
                finally:
                    if use_timeout:
                        signal_mod.alarm(0)  # type: ignore[attr-defined]
                        if prev_handler is not None:
                            signal_mod.signal(signal_mod.SIGALRM, prev_handler)  # type: ignore[attr-defined]

            _stats["signals_processed"] += 1

        # ✅ ACK только после успешной обработки
        _xack(r, m.stream, m.msg_id)
        try:
            r.delete(rk)
        except Exception:
            pass

    except _SignalTimeout:
        log.error("⏰ Signal processing timed out (%ds) for stream=%s id=%s", SIGNAL_TIMEOUT_SEC, m.stream, m.msg_id)
        # ACK timed out signals to avoid infinite retry
        _xack(r, m.stream, m.msg_id)

    except Exception as e:
        # ❌ НЕ ACK: пусть будет retry через pending/autoclaim.
        import traceback
        log.error(f"Error in _process_one: {e}\n{traceback.format_exc()}")
        tries = 0
        try:
            tries = int(r.incr(rk))
            r.expire(rk, 3600 * 24 * 7)
        except Exception as incr_err:  # Fix #7: was silent
            log.warning("retry incr failed stream=%s id=%s: %r — no DLQ escalation until key restored", m.stream, m.msg_id, incr_err)
            tries = tries + 1

        if tries >= MAX_RETRIES:
            # чтобы не застревало навечно: отправляем в DLQ и ACK
            _to_dlq(r, {
                "stream": m.stream,
                "id": m.msg_id,
                "tries": tries,
                "is_tick": int(is_tick),
                "error": str(e),
                "fields": m.fields,
            })
            try:
                # Извлекаем trace_id из полей, если он там есть
                trace_id_str = m.fields.get(b"trace_id") or m.fields.get("trace_id") or b""
                trace_id = trace_id_str.decode("utf-8", errors="ignore") if isinstance(trace_id_str, (bytes, bytearray)) else str(trace_id_str)
                trace_info = f"\nTraceID: <code>{trace_id}</code>" if trace_id else ""
                
                r.xadd("notify:telegram", {
                    "payload": json.dumps({
                        "message": f"🚨 <b>Trade Monitor DLQ Escalation</b>\nStream: <code>{m.stream}</code>\nMsgID: <code>{m.msg_id}</code>{trace_info}\nError: <code>{e}</code>\n<i>Check dlq:trade-monitor for details</i>"
                    })
                }, maxlen=2000)
            except Exception as notify_err:
                log.warning("Failed to escalate DLQ alert to telegram: %r", notify_err)
            _xack(r, m.stream, m.msg_id)
            try:
                r.delete(rk)
            except Exception:
                pass
        else:
            # мягкий backoff, чтобы не жечь CPU при "ядовитых" сообщениях
            time.sleep(RETRY_BACKOFF_MS / 1000.0)
    finally:
        clear_trace_id()


def main():
    import urllib.parse
    redis_clients = []
    r1 = redis.from_url(REDIS_URL, decode_responses=True, socket_timeout=5.0, socket_connect_timeout=5.0) if REDIS_URL else get_redis()
    redis_clients.append(r1)

    try:
        if REDIS_URL:
            # Парсим URL, чтобы получить пользователя и пароль, и просто меняем хост на redis-worker-2
            parsed = urllib.parse.urlparse(REDIS_URL)
            if parsed.hostname == 'redis-worker-1':
                r2_url = parsed._replace(netloc=parsed.netloc.replace('redis-worker-1', 'redis-worker-2')).geturl()
            else:
                r2_url = REDIS_URL.replace("redis-worker-1", "redis-worker-2")
                
            r2 = redis.from_url(r2_url, decode_responses=True, socket_timeout=5.0, socket_connect_timeout=5.0)
        else:
            redis_host_2 = os.getenv("REDIS_SIGNALS_HOST_2", "redis-worker-2")
            redis_port_2 = int(os.getenv("REDIS_SIGNALS_PORT_2", "6379"))
            r2 = redis.Redis(host=redis_host_2, port=redis_port_2, db=0, decode_responses=True, socket_timeout=5.0, socket_connect_timeout=5.0)

        if r2.ping():
            redis_clients.append(r2)
            log.info(f"Connected to secondary redis {r2.connection_pool.connection_kwargs.get('host')}")
    except Exception as e:
        log.warning(f"Secondary redis not available: {e}")

    try:
        r_ticks_url = os.environ.get("REDIS_TICKS_URL")
        log.info(f"TRACING: init ticks url={r_ticks_url}")
        if not r_ticks_url:
            # Fallback for local/container deployment: clean redis-ticks
            # We don't inherit credentials from REDIS_URL because infrastructure Redis (ticks)
            # might have a different ACL set (usually default with no pass).
            r_ticks_url = "redis://redis-ticks:6379/0"
            log.info(f"TRACING: derived clean fallback ticks url={r_ticks_url}")
        
        if r_ticks_url:
            r_ticks = redis.from_url(r_ticks_url, decode_responses=True, socket_connect_timeout=2, socket_timeout=2)
            log.info("TRACING: attempting ping to ticks redis...")
            res = r_ticks.ping()
            log.info(f"TRACING: ping result={res}")
            if res:
                redis_clients.insert(0, r_ticks)  # Prioritize ticks node in the list
                log.info(f"Connected to ticks redis {r_ticks.connection_pool.connection_kwargs.get('host')}")
    except Exception as e:
        log.warning(f"Ticks redis not available (market data updates will stop): {repr(e)}")

    # Инициализация автокалибровки параметров торговли
    trades_threshold = int(os.getenv("AUTO_CALIBRATION_THRESHOLD", "100"))
    enabled_symbols = [s.strip() for s in os.getenv("AUTO_CALIBRATION_SYMBOLS", "BTCUSDT,ETHUSDT").split(",") if s.strip()]
    calibration_source = os.getenv("AUTO_CALIBRATION_SOURCE", "CryptoOrderFlow")

    init_auto_calibration(
        trades_threshold=trades_threshold,
        enabled_symbols=enabled_symbols,
        source=calibration_source
    )

    monitor = TradeMonitorService(redis_url=REDIS_URL if REDIS_URL else None)

    last_rescan = 0.0
    last_claim = 0.0

    client_states = {rc: {"signals": [], "ticks": []} for rc in redis_clients}
    
    from concurrent.futures import ThreadPoolExecutor
    bg_executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix="tm_bg")
    discover_future = None
    claim_future = None

    # Снижаем блокировку до 50 мс, чтобы не залипать на одном узле
    multi_block_ms = min(BLOCK_MS, 50) if len(redis_clients) > 1 else BLOCK_MS

    prom_port = int(os.getenv("TM_PROMETHEUS_PORT", "9844"))
    log.info("Starting Prometheus metrics server on port %d", prom_port)
    try:
        start_http_server(prom_port)
    except Exception as e:
        log.error(f"Failed to start prometheus server: {e}")

    last_heartbeat = time.time()
    _shutdown_requested = False

    # Fix #9: graceful SIGTERM/SIGINT with drain window
    _DRAIN_TIMEOUT_SEC = int(os.getenv("TM_DRAIN_TIMEOUT_SEC", "5"))

    def _graceful_shutdown(signum, frame):
        nonlocal _shutdown_requested
        log.info("trade-monitor-runner: received signal %d, draining for %ds then exiting", signum, _DRAIN_TIMEOUT_SEC)
        _shutdown_requested = True

    signal_mod.signal(signal_mod.SIGTERM, _graceful_shutdown)
    signal_mod.signal(signal_mod.SIGINT, _graceful_shutdown)

    _drain_start: float | None = None

    while True:
        # Fix #9: honour shutdown request — drain in-flight then exit cleanly
        if _shutdown_requested:
            if _drain_start is None:
                _drain_start = time.time()
                log.info("trade-monitor-runner: drain started (timeout=%ds)", _DRAIN_TIMEOUT_SEC)
            elif time.time() - _drain_start >= _DRAIN_TIMEOUT_SEC:
                log.info("trade-monitor-runner: drain complete, exiting cleanly")
                sys.exit(0)

        now = time.time()
        trade_monitor_loop_age_seconds.set(now)

        # ── Heartbeat: periodic status log to detect hangs ──
        if now - last_heartbeat >= HEARTBEAT_EVERY_SEC:
            try:
                n_open = len(monitor.open_positions)
                trade_monitor_open_positions.set(n_open)
                log.info(
                    "💓 HEARTBEAT: open_positions=%d signals=%d ticks=%d skipped_cap=%d skipped_age=%d",
                    n_open,
                    _stats["signals_processed"],
                    _stats["ticks_processed"],
                    _stats["signals_skipped_cap"],
                    _stats["signals_skipped_age"],
                )
            except Exception:
                log.info("💓 HEARTBEAT: alive")
            last_heartbeat = now
        
        # 1) discover streams + ensure groups
        if now - last_rescan >= RESCAN_EVERY_SEC and discover_future is None:
            def _bg_discover():
                res = {}
                for rc in redis_clients:
                    s = discover_streams(rc, SIGNAL_PATTERNS)
                    t = discover_streams(rc, TICK_PATTERNS)
                    if s or t:
                        for st in set((s or []) + (t or [])):
                            ensure_group(rc, st, GROUP, start_id="$")
                    res[rc] = (s, t)
                return res

            discover_future = bg_executor.submit(_bg_discover)
            last_rescan = now
            
        if discover_future is not None and discover_future.done():
            try:
                disc_res = discover_future.result()
                total_s = total_t = 0
                for rc, (s, t) in disc_res.items():
                    if s: client_states[rc]["signals"] = s
                    if t: client_states[rc]["ticks"] = t
                    total_s += len(client_states[rc]["signals"])
                    total_t += len(client_states[rc]["ticks"])
                log.info(f"streams across {len(redis_clients)} nodes: signals={total_s} ticks={total_t}")
            except Exception as e:
                log.warning("background discover_streams failed: %s", e)
            discover_future = None

        # 2) periodically reclaim stale pendings
        if now - last_claim >= CLAIM_EVERY_SEC and claim_future is None:
            def _bg_claim():
                res_msgs = []
                for rc in redis_clients:
                    stale_streams = client_states[rc]["ticks"] + client_states[rc]["signals"]
                    for s in stale_streams:
                        stale = autoclaim_stale(rc, s, GROUP, CONSUMER, min_idle_ms=MIN_IDLE_MS, start_id="0-0", count=50)
                        if stale:
                            res_msgs.append((rc, stale))
                return res_msgs

            claim_future = bg_executor.submit(_bg_claim)
            last_claim = now

        if claim_future is not None and claim_future.done():
            try:
                stale_msgs = claim_future.result()
                for rc, msgs in stale_msgs:
                    tick_set = set(client_states[rc]["ticks"])
                    for m in msgs:
                        _process_one(rc, monitor, m, is_tick=(m.stream in tick_set))
            except Exception as e:
                log.warning("background autoclaim failed: %s", e)
            claim_future = None

        # 3) read new messages (>, per group)
        processed_any = False
        # P41 Optimization: prioritize nodes carrying market data (ticks) to minimize monitor lag
        # We process clients with ticks first to ensure _max_tick_ts_ms advances before signal processing.
        sorted_clients = sorted(redis_clients, key=lambda c: 0 if client_states[c]["ticks"] else 1)

        for rc in sorted_clients:
            t_streams = client_states[rc]["ticks"]
            s_streams = client_states[rc]["signals"]
            streams_all = t_streams + s_streams
            if not streams_all:
                continue
            
            try:
                # Use higher READ_COUNT for nodes with ticks to allow faster catch-up during backlog
                eff_count = READ_COUNT_TICKS if t_streams else READ_COUNT
                msgs = xreadgroup_multi(rc, GROUP, CONSUMER, streams_all, count=eff_count, block_ms=multi_block_ms)
                if not msgs:
                    continue
                
                processed_any = True
                tick_set = set(client_states[rc]["ticks"])
                # Record loop age frequently to avoid AIOps marking TM as hung during massive tick processing
                trade_monitor_loop_age_seconds.set(time.time())
                for m in msgs:
                    _process_one(rc, monitor, m, is_tick=(m.stream in tick_set))
            except (redis.ConnectionError, redis.TimeoutError, ConnectionError, OSError) as conn_err:
                log.warning(
                    "Redis connection error on node %s: %s — skipping this node for current cycle",
                    rc.connection_pool.connection_kwargs.get("host", "unknown"), conn_err,
                )
                continue
                
        if not processed_any and multi_block_ms == 0:
            time.sleep(0.01)

if __name__ == "__main__":
    main()

