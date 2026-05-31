from __future__ import annotations

import json
import os
from typing import Any

import psycopg2
import psycopg2.extras
import redis
from core.redis_keys import RedisStreams as RS


def _redis():
    return redis.Redis.from_url(os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0"), decode_responses=True)


def _dsn() -> str:
    return (
        os.getenv("ANALYTICS_DB_DSN")
        or os.getenv("TRADES_DB_DSN")
        or "postgresql://postgres:12345@postgres:5432/scanner_analytics"
    )


def _chat_id() -> str:
    return (os.getenv("ATR_POLICY_TELEGRAM_CHAT_ID", "") or "")


def _top_n() -> int:
    try:
        return int(os.getenv("ATR_POLICY_TELEGRAM_SUMMARY_TOP_N", "5") or 5)
    except Exception:
        return 5


def _fmt(x: Any) -> str:
    try:
        return f"{float(x):.2f}"
    except Exception:
        return "n/a"


def _notify(text: str, buttons: list[list[dict[str, str]]] | None = None) -> bool:
    payload: dict[str, Any] = {"text": text}
    if buttons:
        payload["buttons"] = json.dumps(buttons, ensure_ascii=False)
    cid = _chat_id()
    if cid:
        payload["chat_id"] = cid
    try:
        _redis().xadd(RS.NOTIFY_TELEGRAM, payload, maxlen=10000, approximate=True)
        return True
    except Exception:
        return False


def summary_menu_buttons() -> list[list[dict[str, str]]]:
    return [
        [
            {"text": "📥 Pending", "callback": "atrsum:pending"},
            {"text": "🟢 Active", "callback": "atrsum:active"},
        ],
        [
            {"text": "♻️ Revoked today", "callback": "atrsum:revoked"},
            {"text": "🏆 Best cohorts", "callback": "atrsum:best"},
        ],
        [
            {"text": "⚠️ Worst cohorts", "callback": "atrsum:worst"},
        ],
    ]


def publish_summary_menu() -> bool:
    return _notify(
        "ATR Policy Ops\nВыберите отчёт:",
        buttons=summary_menu_buttons(),
    )


def _scan_active_keys() -> list[str]:
    r = _redis()
    cur = 0
    out: list[str] = []
    while True:
        cur, keys = r.scan(cur, match="cfg:atr_policy:active:*", count=10000)
        out.extend(keys)
        if cur == 0:
            break
    return sorted(out)


def report_pending() -> str:
    r = _redis()
    ids = sorted(list(r.smembers("queue:atr_policy:pending") or []))
    if not ids:
        return "ATR Policy Pending\nНет pending proposals."
    lines = ["ATR Policy Pending"]
    for pid in ids[:_top_n()]:
        raw = r.get(f"cfg:proposals:atr_policy:{pid}")
        if not raw:
            continue
        obj = json.loads(raw)
        if (obj.get("status") or "") != "SUBMITTED":
            continue
        lines.append(
            f"- {obj.get('symbol','')} | {obj.get('scenario','')} | "
            f"{obj.get('regime','')} | {obj.get('risk_horizon_bucket','')} | "
            f"stop={obj.get('stop_ttl_mode','')} trail={obj.get('trailing_mode','')} | "
            f"id={pid[:8]}"
        )
    return "\n".join(lines)


def report_active() -> str:
    r = _redis()
    keys = _scan_active_keys()
    if not keys:
        return "ATR Policy Active\nНет active policies."
    lines = ["ATR Policy Active"]
    for key in keys[:_top_n()]:
        raw = r.get(key)
        if not raw:
            continue
        obj = json.loads(raw)
        lines.append(
            f"- {obj.get('symbol','')} | {obj.get('scenario','')} | {obj.get('regime','')} | "
            f"{obj.get('risk_horizon_bucket','')} | stop={obj.get('stop_ttl_mode','')} "
            f"trail={obj.get('trailing_mode','')}"
        )
    return "\n".join(lines)


def report_revoked_today() -> str:
    conn = psycopg2.connect(_dsn(), connect_timeout=5, application_name="atr_policy_tg_summary")
    try:
        with conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT symbol, scenario, regime, risk_horizon_bucket, reason_code, created_at  # type: ignore
                FROM atr_promotion_policy_audit
                WHERE created_at::date = current_date
                  AND (decision_json->>'action') = 'REVOKE'
                ORDER BY created_at DESC
                LIMIT %s
                """,
                (_top_n(),),
            )
            rows = cur.fetchall()
        if not rows:
            return "ATR Policy Revoked Today\nСегодня revoke не было."
        lines = ["ATR Policy Revoked Today"]
        for r in rows:
            lines.append(
                f"- {r['symbol']} | {r['scenario']} | {r['regime']} | "
                f"{r['risk_horizon_bucket']} | {r['reason_code']}"
            )
        return "\n".join(lines)
    finally:
        conn.close()


def _cohort_report(best: bool) -> str:
    conn = psycopg2.connect(_dsn(), connect_timeout=5, application_name="atr_policy_tg_summary")
    order = "DESC" if best else "ASC"
    title = "Best cohorts" if best else "Worst cohorts"
    try:
        with conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                f"""
                WITH paired AS (  # type: ignore
                  SELECT
                    source, symbol, scenario, regime, risk_horizon_bucket,
                    max(CASE WHEN live_surface_applied THEN avg_pnl_bps END) AS pnl_canary,
                    max(CASE WHEN NOT live_surface_applied THEN avg_pnl_bps END) AS pnl_control,
                    max(CASE WHEN live_surface_applied THEN tp1_rate END) AS tp1_canary,
                    max(CASE WHEN NOT live_surface_applied THEN tp1_rate END) AS tp1_control,
                    max(CASE WHEN live_surface_applied THEN n END) AS n_canary,
                    max(CASE WHEN NOT live_surface_applied THEN n END) AS n_control
                  FROM horizon_live_surface_ab_daily
                  WHERE day >= current_date - %s::int
                  GROUP BY 1,2,3,4,5
                )
                SELECT
                  symbol, scenario, regime, risk_horizon_bucket,
                  n_canary, n_control,
                  (coalesce(pnl_canary,0) - coalesce(pnl_control,0)) AS pnl_delta,
                  (coalesce(tp1_canary,0) - coalesce(tp1_control,0)) AS tp1_delta
                FROM paired
                WHERE coalesce(n_canary,0) > 0 AND coalesce(n_control,0) > 0
                ORDER BY pnl_delta {order}, tp1_delta {order}
                LIMIT %s
                """,
                (int(os.getenv("ATR_POLICY_TELEGRAM_SUMMARY_WINDOW_DAYS", "21")), _top_n()),
            )
            rows = cur.fetchall()
        if not rows:
            return f"ATR Policy {title}\nНет данных."
        lines = [f"ATR Policy {title}"]
        for r in rows:
            lines.append(
                f"- {r['symbol']} | {r['scenario']} | {r['regime']} | {r['risk_horizon_bucket']} | "
                f"ΔPnL={_fmt(r['pnl_delta'])} | ΔTP1={_fmt(r['tp1_delta'])} | "
                f"n={r['n_canary']}/{r['n_control']}"
            )
        return "\n".join(lines)
    finally:
        conn.close()


def report_best() -> str:
    return _cohort_report(best=True)


def report_worst() -> str:
    return _cohort_report(best=False)


def publish_nightly_digest() -> int:
    n = 0
    for text in (
        report_pending(),
        report_active(),
        report_revoked_today(),
        report_best(),
        report_worst(),
    ):
        if _notify(text):
            n += 1
    return n


if __name__ == "__main__":
    print(publish_nightly_digest())
