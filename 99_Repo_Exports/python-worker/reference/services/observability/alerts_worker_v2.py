from __future__ import annotations

import json
import os
import time

import redis

from core.telegram_notify import send_telegram


def _decode(x) -> str:
    if x is None:
        return ""
    if isinstance(x, bytes):
        return x.decode("utf-8", "ignore")
    return str(x)


def _sscan_all(r: redis.Redis, key: str, limit: int = 2000) -> list[str]:
    out: list[str] = []
    cur = 0
    while True:
        cur, batch = r.sscan(key, cursor=cur, count=10000)
        for b in batch or []:
            s = _decode(b)
            if s:
                out.append(s)
                if len(out) >= limit:
                    return sorted(set(out))
        if int(cur) == 0:
            break
    return sorted(set(out))


def _cooldown_ok(r: redis.Redis, key: str, cooldown_sec: int) -> bool:
    try:
        if r.get(key):
            return False
        r.set(key, "1", ex=cooldown_sec)
        return True
    except Exception:
        return True


def _connect_redis_with_retry(redis_url: str, max_retries: int = 10, retry_delay: float = 2.0) -> redis.Redis:
    """Connect to Redis with retry logic and exponential backoff."""
    for attempt in range(max_retries):
        try:
            # ✅ FIX: Add max_connections and disable health_check_interval to prevent recursion
            # Health check can cause recursion errors in some Redis versions
            r = redis.Redis.from_url(
                redis_url,
                decode_responses=False,
                socket_connect_timeout=10,
                socket_timeout=30,
                retry_on_timeout=True,
                max_connections=5,  # Alerts worker needs minimal connections
                health_check_interval=0,  # Disable to prevent recursion errors
                socket_keepalive=True,
            )
            # Test connection
            r.ping()
            return r
        except (redis.ConnectionError, redis.TimeoutError, OSError):
            if attempt < max_retries - 1:
                delay = retry_delay * (2 ** attempt)  # Exponential backoff
                time.sleep(min(delay, 30))  # Cap at 30 seconds
            else:
                raise


def main():
    if os.getenv("ALERTS_ENABLE", "0") not in {"1", "true", "yes"}:
        raise SystemExit("ALERTS_ENABLE=0")

    redis_url = os.getenv("METRICS_REDIS_URL") or os.getenv("REPORTS_REDIS_URL") or os.getenv("REDIS_URL") or "redis://localhost:6379/0"
    max_retries = int(os.getenv("REDIS_CONNECT_MAX_RETRIES", "10"))
    retry_delay = float(os.getenv("REDIS_CONNECT_RETRY_DELAY", "2.0"))
    r = _connect_redis_with_retry(redis_url, max_retries=max_retries, retry_delay=retry_delay)

    interval = int(os.getenv("ALERTS_INTERVAL_SEC", "60"))
    cooldown = int(os.getenv("ALERTS_COOLDOWN_SEC", "600"))

    thr_atr_bad = float(os.getenv("ALERT_ATR_BAD_PCT", "30"))
    thr_cvd_q = float(os.getenv("ALERT_CVD_QUAR_PCT", "30"))
    thr_min_xlen = int(os.getenv("ALERT_STREAM_MIN_XLEN", "500"))
    thr_atr_sw = int(os.getenv("ALERT_ATR_SWITCH_COUNT", "10"))
    thr_cvd_jump = int(os.getenv("ALERT_CVD_JUMP_COUNT", "10"))
    thr_lcb_changes = int(os.getenv("ALERT_LCB_WINNER_CHANGES", "10"))

    symbols_set = os.getenv("MICROBAR_SYMBOLS_SET", "events:microbar_closed:symbols")
    tpl = os.getenv("MICROBAR_PER_SYMBOL_STREAM_TEMPLATE", "events:microbar_closed:{sym}")
    legacy_key = os.getenv("MICROBAR_LEGACY_STREAM", "events:microbar_closed")

    # Check if split streams are enabled
    split_streams = os.getenv("MICROBAR_SPLIT_STREAMS_ENABLE", "0").strip().lower() in {"1", "true", "yes"}
    dual_write = os.getenv("MICROBAR_SPLIT_DUAL_WRITE", "0").strip().lower() in {"1", "true", "yes"}

    while True:
        try:
            syms = _sscan_all(r, symbols_set, limit=int(os.getenv("REPORT_MAX_SYMBOLS", "200")))
            n = len(syms) or 1
            syms_set = set(syms)

            # ATR bad %
            bad_syms_raw = _sscan_all(r, "cfg:atr_bad:symbols", limit=2000)
            # Filter: only symbols whose per-key still exists (avoids TTL-stale membership)
            bad_syms = []
            stale_syms = []
            for _s in bad_syms_raw:
                try:
                    _exists = r.exists(f"cfg:atr_bad:{_s}")
                except Exception:
                    _exists = 1
                if _exists:
                    bad_syms.append(_s)
                else:
                    stale_syms.append(_s)
            # Clean stale members from set (best-effort, non-blocking)
            if stale_syms:
                try:
                    r.srem("cfg:atr_bad:symbols", *stale_syms)
                except Exception:
                    pass
            bad_pct = 100.0 * (len(set(bad_syms) & syms_set) / float(n))
            if bad_pct >= thr_atr_bad and _cooldown_ok(r, "alerts:cooldown:atr_bad", cooldown):
                # Collect detailed info: symbols with reasons and reason distribution
                bad_active = list(set(bad_syms) & syms_set)[:20]  # Top 20
                reason_stats: dict[str, int] = {}
                symbol_details: list[tuple[str, str, int]] = []  # (symbol, reason, count)

                for s in bad_active:
                    try:
                        # Get current reason from cfg:atr_bad:{symbol}
                        bad_info_raw = _decode(r.get(f"cfg:atr_bad:{s}"))
                        current_reason = "unknown"
                        if bad_info_raw:
                            try:
                                bad_info = json.loads(bad_info_raw) if bad_info_raw.startswith("{") else {}
                                current_reason = (bad_info.get("reason", "1" if bad_info_raw == "1" else "unknown"))
                            except Exception:
                                current_reason = "unknown" if bad_info_raw != "1" else "unknown"

                        # Get reason distribution from metrics:atr_bad_total:{symbol}
                        reason_counts = {}
                        try:
                            reason_hash = r.hgetall(f"metrics:atr_bad_total:{s}")
                            if reason_hash:
                                for reason_key, count_val in (reason_hash or {}).items():
                                    rk = _decode(reason_key)
                                    cv = int(_decode(count_val) or "0")
                                    if cv > 0:
                                        reason_counts[rk] = cv
                                        reason_stats[rk] = reason_stats.get(rk, 0) + cv
                        except Exception:
                            pass

                        # Use current reason or most common from metrics
                        if reason_counts:
                            top_reason = max(reason_counts.items(), key=lambda x: x[1])[0]
                            total_count = sum(reason_counts.values())
                        else:
                            top_reason = current_reason
                            total_count = 1

                        symbol_details.append((s, top_reason, total_count))
                    except Exception:
                        symbol_details.append((s, "unknown", 0))

                # Sort by count desc
                symbol_details.sort(key=lambda x: x[2], reverse=True)

                # Build alert message
                msg_parts = [f"[ALERT] ATR bad pct={bad_pct:.1f}% (thr={thr_atr_bad}%)"]

                # Reason distribution
                if reason_stats:
                    reason_items = sorted(reason_stats.items(), key=lambda x: x[1], reverse=True)
                    reason_str = ", ".join([f"{r}:{c}" for r, c in reason_items[:5]])
                    msg_parts.append(f"Reasons: {reason_str}")

                    # NEW: Timeframe breakdown
                    try:
                        import re
                        from collections import defaultdict
                        tf_counts = defaultdict(int)
                        for reason, count in reason_items:
                            # Extract TF from reason like "stale>120000:tf=1m"
                            match = re.search(r'tf=([a-z0-9]+)', reason.lower())
                            if match:
                                tf_counts[match.group(1)] += count

                        if tf_counts:
                            tf_str = ", ".join([f"{tf}:{cnt}" for tf, cnt in sorted(tf_counts.items(), key=lambda x: -x[1])[:5]])
                            msg_parts.append(f"📈 Timeframes: {tf_str}")
                    except Exception:
                        pass

                    # Highlight stale issues if significant
                    stale_total = sum(c for r, c in reason_items if "stale" in r.lower())
                    if stale_total > 0:
                        stale_pct = 100.0 * stale_total / sum(reason_stats.values())
                        if stale_pct >= 20.0:  # If stale > 20% of issues
                            msg_parts.append(f"⚠️ STALE: {stale_total} events ({stale_pct:.1f}%) - check data pipeline delays")

                    # Highlight jump issues if significant
                    jump_total = sum(c for r, c in reason_items if "jump" in r.lower())
                    if jump_total > 0:
                        jump_pct = 100.0 * jump_total / sum(reason_stats.values())
                        if jump_pct >= 30.0:  # If jumps > 30% of issues
                            msg_parts.append(f"⚠️ JUMPS: {jump_total} events ({jump_pct:.1f}%) - possible market volatility spike")

                # Top symbols
                if symbol_details:
                    top_symbols = [f"{s}:{r}({c})" for s, r, c in symbol_details[:10]]
                    msg_parts.append(f"Top: {', '.join(top_symbols)}")

                send_telegram("\n".join(msg_parts))

            # CVD quarantine %
            q_syms = _sscan_all(r, "cfg:cvd_quarantine:symbols", limit=2000)
            q_pct = 100.0 * (len(set(q_syms) & syms_set) / float(n))
            if q_pct >= thr_cvd_q and _cooldown_ok(r, "alerts:cooldown:cvd_quar", cooldown):
                send_telegram(f"[ALERT] CVD quarantine pct={q_pct:.1f}% (thr={thr_cvd_q}%)")

            # ATR switchers (top)
            sw_syms = _sscan_all(r, "cfg:atr_switch:symbols", limit=500)
            offenders = []
            for s in sw_syms:
                try:
                    c = int(_decode(r.get(f"cfg:atr_switch_count:{s}")) or "0")
                except Exception:
                    c = 0
                if c >= thr_atr_sw:
                    offenders.append((s, c))
            offenders.sort(key=lambda x: x[1], reverse=True)
            if offenders and _cooldown_ok(r, "alerts:cooldown:atr_switch", cooldown):
                top = ", ".join([f"{s}:{c}" for s, c in offenders[:10]])
                send_telegram(f"[ALERT] ATR switches >= {thr_atr_sw}: {top}")

            # Streams min XLEN (risk of blindness)
            if "{sym}" in tpl and syms:
                small = []
                for s in syms[: int(os.getenv("METRICS_MAX_SYMBOLS", "200"))]:
                    try:
                        ln = int(r.xlen(tpl.format(sym=s)))
                    except Exception:
                        ln = 0
                    if ln < thr_min_xlen:
                        small.append((s, ln))
                small.sort(key=lambda x: x[1])
                if small and _cooldown_ok(r, "alerts:cooldown:streams_min_xlen", cooldown):
                    top = ", ".join([f"{s}:{ln}" for s, ln in small[:10]])
                    send_telegram(f"[ALERT] microbar xlen < {thr_min_xlen}: {top}")
            elif not split_streams or dual_write:
                # Only check legacy stream if split streams are disabled OR dual write is enabled
                try:
                    ln = int(r.xlen(legacy_key))
                    if ln < thr_min_xlen and _cooldown_ok(r, "alerts:cooldown:legacy_xlen", cooldown):
                        send_telegram(f"[ALERT] legacy microbar xlen {ln} < {thr_min_xlen}")
                except Exception:
                    pass

            # CVD jumps (best-effort totals)
            offenders = []
            for s in syms[: int(os.getenv("METRICS_MAX_SYMBOLS", "200"))]:
                try:
                    c = int(_decode(r.get(f"metrics:cvd_jump_total:{s}")) or "0")
                except Exception:
                    c = 0
                if c >= thr_cvd_jump:
                    offenders.append((s, c))
            offenders.sort(key=lambda x: x[1], reverse=True)
            if offenders and _cooldown_ok(r, "alerts:cooldown:cvd_jump", cooldown):
                top = ", ".join([f"{s}:{c}" for s, c in offenders[:10]])
                send_telegram(f"[ALERT] CVD jumps >= {thr_cvd_jump}: {top}")

            # LCB winner changes (per key)
            try:
                lcb_keys = _sscan_all(r, "metrics:lcb:keys", limit=2000)
            except Exception:
                lcb_keys = []
            offenders = []
            for k in lcb_keys[:2000]:
                try:
                    c = int(_decode(r.get(f"metrics:lcb_winner_changes_total:{k}")) or "0")
                except Exception:
                    c = 0
                if c >= thr_lcb_changes:
                    offenders.append((k, c))
            offenders.sort(key=lambda x: x[1], reverse=True)
            if offenders and _cooldown_ok(r, "alerts:cooldown:lcb_changes", cooldown):
                top = ", ".join([f"{k}:{c}" for k, c in offenders[:10]])
                send_telegram(f"[ALERT] LCB winner changes >= {thr_lcb_changes}: {top}")

        except (redis.ConnectionError, redis.TimeoutError, OSError) as e:
            # Connection errors - try to reconnect
            try:
                if _cooldown_ok(r, "alerts:cooldown:worker_err", cooldown):
                    send_telegram(f"[ALERT] alerts worker Redis connection error: {e}")
            except Exception:
                pass
            # Attempt to reconnect
            try:
                redis_url = os.getenv("METRICS_REDIS_URL") or os.getenv("REPORTS_REDIS_URL") or os.getenv("REDIS_URL") or "redis://localhost:6379/0"
                max_retries = int(os.getenv("REDIS_CONNECT_MAX_RETRIES", "10"))
                retry_delay = float(os.getenv("REDIS_CONNECT_RETRY_DELAY", "2.0"))
                r = _connect_redis_with_retry(redis_url, max_retries=3, retry_delay=retry_delay)
            except Exception:
                # If reconnection fails, wait longer before retrying
                time.sleep(max(5, interval))
                continue
        except Exception as e:
            # Other errors
            try:
                if _cooldown_ok(r, "alerts:cooldown:worker_err", cooldown):
                    send_telegram(f"[ALERT] alerts worker error: {e}")
            except Exception:
                pass

        time.sleep(max(1, interval))


if __name__ == "__main__":
    main()

