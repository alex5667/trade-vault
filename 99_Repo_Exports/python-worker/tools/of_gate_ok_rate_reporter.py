import argparse
import ast
import html
import json
import logging
import os
import socket
import time
from collections import Counter
from typing import Any

import redis

from domain.evidence_keys import MetaKeys
from utils.time_utils import get_ny_time_millis
from core.redis_keys import RedisStreams as RS

# Use common project utilities if available (like in of_gate_sre_monitor.py)
try:
    from common.redis_errors import retry_redis_operation
    from core.ok_fields import get_ts_ms, parse_ok_fields
    from tools.of_gate_metrics_contract import is_gate_row, scenario_key
except ImportError:
    # Minimal fallback implementations for standalone test execution
    def retry_redis_operation(operation, **kwargs):
        try:
            return operation()
        except Exception as e:
            if 'on_final_failure' in kwargs:
                return kwargs['on_final_failure'](e)
            raise e

    def get_ts_ms(row: dict) -> int:
        try:
            return int(row.get("ts_ms") or row.get("ts") or 0)
        except Exception:
            return 0

    def is_gate_row(r: dict) -> bool:
        # Row from metrics:of_gate always has 'ok' field; also check schema_name
        return "ok" in r or r.get("schema_name") in ("of_gate_metrics", "of_gate_metrics_v1")

    def scenario_key(r: dict) -> str:
        return str(r.get("scenario_v4") or r.get("scenario") or "na")


def _is_valid_row(r: dict[str, Any]) -> bool:
    """Lenient validity check: only requires ts_ms and ok to be present and parseable.
    Unlike validate_of_gate_row, does NOT reject rows missing ok_soft or with
    Python-literal missing_legs (which would silently filter all rows -> n=0).
    """
    try:
        ts = int(r.get("ts_ms") or r.get("ts") or 0)
        if ts <= 0:
            return False
    except Exception:
        return False
    ok_raw = r.get("ok")
    if ok_raw is None:
        return False
    try:
        int(float(ok_raw))
    except Exception:
        return False
    return True

logger = logging.getLogger("ok_rate_reporter")
logger.setLevel(logging.INFO)
stream_handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s %(levelname)s [%(name)s] %(message)s')
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)


def _now_ms() -> int:
    return get_ny_time_millis()


def _parse_missing_legs(r: dict[str, Any]) -> list[str]:
    """Parse missing_legs which can be JSON or eval-able string representation of list"""
    x = r.get("missing_legs", "")
    if not x:
        return []
    try:
        if isinstance(x, (bytes, bytearray)):
            x = x.decode("utf-8", "ignore")
        v = json.loads(str(x))
        if isinstance(v, list):
            return [str(z) for z in v]
    except Exception:
        pass

    try:
         # Fallback for Python literal arrays like "['vol_bump', 'need_met']"
         v = ast.literal_eval(str(x))
         if isinstance(v, list):
             return [str(z) for z in v]
    except Exception:
        pass

    return []


def read_stream(r: redis.Redis, stream: str, window_ms: int) -> list[dict[str, Any]]:
    """Reads events from a Redis stream for the past window_ms."""
    start_ms = _now_ms() - window_ms
    end_ms = _now_ms()
    rows: list[dict[str, Any]] = []
    last_id = "+"
    scanned = 0
    max_scan = 500000

    while scanned < max_scan:
        try:
            batch = retry_redis_operation(
                operation=lambda: r.xrevrange(stream, max=last_id, min="-", count=2000),
                operation_name="xrevrange",
                max_retries=10,
                base_delay=1.0,
                max_delay=30.0,
                on_final_failure=lambda e: [],
            )
        except Exception as e:
            logger.error(f"Error reading stream: {e}")
            batch = []

        if not batch:
            break

        for msg_id, fields in batch:
            scanned += 1
            if msg_id == last_id:
                continue
            last_id = msg_id

            d = dict(fields or {})
            ts = get_ts_ms(d)
            if ts <= 0:
                continue
            if ts < start_ms:
                scanned = max_scan  # Force break outer loop
                break
            if ts <= end_ms:
                d["_ts_ms"] = ts
                rows.append(d)

        if len(batch) < 2000:
            break

    rows.sort(key=lambda x: int(x.get("_ts_ms", 0)))
    return rows


def analyze_metrics(rows: list[dict[str, Any]]) -> dict[str, Any]:
    """Analyzes the stream rows, calculates ok_rate, and counts reasons why it's failing."""
    gate_rows = [r for r in rows if is_gate_row(r)]

    valid_rows = []
    dn_vetos = 0
    meta_vetos = 0
    missing_counter = Counter()
    book_bad = 0
    src_bad = 0
    dh_bad = 0

    ok_count = 0

    for r0 in gate_rows:
        r = r0
        if isinstance(r0, dict) and "payload" in r0 and ("ok" not in r0 or "scenario_v4" not in r0):
            try:
                inner = json.loads(r0.get("payload") or "{}")
                if isinstance(inner, dict):
                    r = {**inner, **{k: v for k, v in r0.items() if k != "payload"}}
            except Exception:
                r = r0

        if not _is_valid_row(r):
            continue

        # Ignore dn_veto for basic denominator
        if scenario_key(r) == "dn_veto":
             dn_vetos += 1
             continue

        valid_rows.append(r)

        ok = int(r.get("ok", 0))
        if ok == 1:
            ok_count += 1
        else:
             # ONLY count reasons for rejection on rows where it was rejected!
             mv = int(r.get(MetaKeys.VETO, 0) or 0)
             if mv == 1:
                 meta_vetos += 1

             bho = int(r.get("book_health_ok", 1) or 1)
             if bho == 0:
                 book_bad += 1

             sco = int(r.get("source_consistency_ok", 1) or 1)
             if sco == 0:
                 src_bad += 1

             dh = float(r.get("data_health", 1.0) or 1.0)
             if dh < 0.70:
                 dh_bad += 1

             for m in _parse_missing_legs(r):
                 missing_counter[m] += 1

    n = len(valid_rows)
    ok_rate = ok_count / n if n > 0 else 0.0

    return {
        "n": n,
        "ok_count": ok_count,
        "dn_vetos": dn_vetos,
        "ok_rate": ok_rate,
        "meta_vetos": meta_vetos,
        "book_bad": book_bad,
        "src_bad": src_bad,
        "dh_bad": dh_bad,
        "missing_counter": missing_counter,
    }

def format_telegram_report(stats: dict[str, Any], window_hours: float) -> str:
    lines = []

    hostname = socket.gethostname()

    n = stats.get('n', 0)
    ok_rate = stats.get('ok_rate', 0.0)

    lines.append(f"📊 <b>Отчет о доле успешных сигналов (OK Rate) за последние {window_hours:g} ч.</b>  [<code>{html.escape(hostname)}</code>]")
    lines.append(f"Всего оценено сигналов (исключая dn_veto): <code>{n}</code>")

    if n == 0:
        lines.append("<i>В этот период не было подходящих сигналов для оценки.</i>")
        return "\n".join(lines)

    lines.append(f"Текущий ok_rate: <b>{ok_rate:.3f}</b> (<code>{stats.get('ok_count', 0)}/{n}</code>)")

    if ok_rate > 0.10:
         lines.append("✅ <i>OK rate находится в пределах нормы.</i>")

    # Generate failure breakdown
    fail_count = n - stats.get('ok_count', 0)
    if fail_count > 0:
        lines.append("")
        lines.append(f"<b>Топ причин отклонения сигналов (Всего отклонено: {fail_count}):</b>")

        reasons = []
        if stats.get('meta_vetos', 0) > 0:
             reasons.append(("Блокировка Мета-моделью (meta_veto)", stats['meta_vetos']))
        if stats.get('book_bad', 0) > 0:
             reasons.append(("Плохое состояние стакана (book_health_ok=0)", stats['book_bad']))
        if stats.get('src_bad', 0) > 0:
             reasons.append(("Неконсистентность источников", stats['src_bad']))
        if stats.get('dh_bad', 0) > 0:
             reasons.append(("Низкое качество данных (data_health<0.7)", stats['dh_bad']))

        # Add missing legs dynamically
        for leg, count in stats.get('missing_counter', {}).most_common(10): # Top 10 missing legs
             reasons.append((f"Не выполнено условие: [{leg}]", count))


        # Sort reasons by count descending
        reasons.sort(key=lambda x: x[1], reverse=True)

        # Output top 15 reasons overall
        for reason, count in reasons[:15]:
            percent = (count / fail_count) * 100.0
            lines.append(f"• <code>{html.escape(reason)}</code>: {count} ({percent:.1f}%)")

    lines.append("")
    lines.append("<i>Чтобы ok_rate вырос, необходимо ослабить эти конкретные условия (например, через переобучение ML моделей, изменение порогов ликвидности).</i>")

    return "\n".join(lines)


def _notify(r: redis.Redis, stream: str, text: str) -> None:
    try:
        retry_redis_operation(
            operation=lambda: r.xadd(stream, {
                "type": "report",
                "subtype": "ok_rate_analysis",
                "ts_ms": str(_now_ms()),
                "text": text,
                "format": "html" # Tell telegram notifier to try HTML parse if it supports it
            }, maxlen=50000, approximate=True),
            operation_name="xadd_notify",
            max_retries=1,
            base_delay=1.0,
            max_delay=10.0,
            on_final_failure=lambda e: None,
        )
        logger.info("Successfully pushed report to Telegram notify stream.")
    except Exception as e:
        logger.error(f"Failed to push report to redis stream: {e}")

def main():
    parser = argparse.ArgumentParser(description="Hourly OK Rate Telegram Reporter")
    parser.add_argument("--redis-url", default=os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0"))
    parser.add_argument("--metrics-stream", default=os.getenv("OF_GATE_METRICS_STREAM", RS.OF_GATE_METRICS))
    parser.add_argument("--notify-stream", default=os.getenv("NOTIFY_TELEGRAM_STREAM", RS.NOTIFY_TELEGRAM))
    parser.add_argument("--window-hours", type=float, default=float(os.getenv("OK_RATE_REPORT_WINDOW_HOURS", "1.0")))
    parser.add_argument("--interval-sec", type=int, default=int(os.getenv("OK_RATE_REPORT_INTERVAL_SEC", "3600")))
    parser.add_argument("--run-once", action="store_true", help="Run once and exit")

    args = parser.parse_args()

    logger.info(f"Starting OK Rate Reporter connecting to {args.redis_url}")
    r = redis.Redis.from_url(args.redis_url, decode_responses=True)

    window_ms = int(args.window_hours * 3600 * 1000)

    def job():
        logger.info(f"Running analysis for the past {args.window_hours} hours...")
        rows = read_stream(r, args.metrics_stream, window_ms)
        logger.info(f"Read {len(rows)} events from stream.")
        stats = analyze_metrics(rows)
        logger.info(f"Analyzed {stats.get('n', 0)} eligible signals. OK Rate: {stats.get('ok_rate', 0.0):.3f}")

        report_text = format_telegram_report(stats, args.window_hours)
        _notify(r, args.notify_stream, report_text)

    if args.run_once:
        job()
    else:
        # Initial sleep slightly to stagger execution at startup
        time.sleep(30)
        while True:
            try:
                job()
            except Exception as e:
                logger.error(f"Job failed: {e}")
            logger.info(f"Sleeping for {args.interval_sec} seconds...")
            time.sleep(args.interval_sec)

if __name__ == "__main__":
    main()
