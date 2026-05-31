"""plan2_autopilot_telegram_reporter.py — focused Plan 2 rollout narrative.

aiops_agent.py runs a generic 30-min SRE sweep over 80+ metrics. For Plan 2
rollout we want a different cadence and a different story: "what stage are
we on, what's blocking advance, and was anything just promoted?". This
service runs as its own periodic task and posts a focused report to Telegram.

Send policy (anti-spam):
  * **Stage transition** (S1→S2 / S2→S3 / per-kind activate) → send immediately.
  * **Expectancy threshold ratchet** (autopilot lowered it) → send immediately.
  * Otherwise: send at most every `PLAN2_TG_KEEPALIVE_HOURS` (default 24h) as a
    heartbeat so the operator confirms the autopilot is alive.
  * Sticky `cfg:autopilot:plan2:tg_state` HSET tracks last-sent fingerprint so
    restarts don't double-post.

Sources of truth (no Prometheus dependency — direct Redis + PG):
  * Stage flags + activation timestamps from `cfg:autopilot:plan2:state`.
  * Persister 24h row count + 1h row count from `signal_gated_out_outcomes`.
  * Tracker liveness from Redis `XLEN stream:signals:gated_out_outcomes`.
  * Critical drift warns from Redis `drift:state:*` HASH severity field.

ENV:
  PLAN2_TG_REPORTER_ENABLED      = 0    master switch (0 = print only, no send)
  PLAN2_TG_REPORTER_REDIS_URL    = redis://redis-worker-1:6379/0
  PLAN2_TG_REPORTER_DB_DSN       = (TRADES_DB_DSN)
  PLAN2_TG_REPORTER_PORT         = 9943
  PLAN2_TG_REPORTER_INTERVAL_SEC = 3600    poll cadence
  PLAN2_TG_KEEPALIVE_HOURS       = 24      always send at least this often
  PLAN2_TG_BOT_TOKEN             = (fallback TELEGRAM_BOT_TOKEN)
  PLAN2_TG_CHAT_ID               = (fallback TELEGRAM_CHAT_ID)
  PLAN2_S3_KIND_ALLOWLIST        = meta_lr_blend,v14_of   # for narrative
"""
from __future__ import annotations

import html
import json
import logging
import os
import time
import urllib.error
import urllib.request
from typing import Any

from core.plan2_autopilot_flags import (
    AUTOPILOT_KEY,
    FIELD_EXPECTANCY_THRESHOLD,
    FLAG_DRIFT_PH_ENABLED,
    FLAG_PERSISTER_ENABLED,
    activated_at_field,
    kind_demote_flag,
)

log = logging.getLogger("plan2_tg_reporter")

# Sticky last-send state lives next to the autopilot state for easy ops audit.
LAST_SEND_KEY = "cfg:autopilot:plan2:tg_state"


# ─── ENV helpers ─────────────────────────────────────────────────────────────

def _env(k: str, d: str = "") -> str:
    return os.environ.get(k, d)


def _env_int(k: str, d: int) -> int:
    try:
        return int(_env(k, str(d)))
    except Exception:
        return d


def _env_bool(k: str, d: bool) -> bool:
    raw = _env(k, "")
    if not raw:
        return d
    return raw.strip().lower() in ("1", "true", "yes", "on")


def _env_csv(k: str, d: str = "") -> list[str]:
    raw = _env(k, d)
    return [s.strip().lower() for s in raw.split(",") if s.strip()]


# ─── State collection ───────────────────────────────────────────────────────

def collect_autopilot_state(
    rc: Any, *, allowlist: list[str], now_ms: int,
) -> dict:
    """Read everything the report needs from Redis."""
    state: dict[str, Any] = {
        "now_ms": now_ms,
        "persister_enabled": False,
        "ph_enabled": False,
        "per_kind_demote": {},
        "persister_age_h": 0.0,
        "ph_age_h": 0.0,
        "expectancy_threshold": 0.0,
        "tracker_xlen": 0,
        "warn_per_kind": {},
    }
    try:
        full = rc.hgetall(AUTOPILOT_KEY) or {}
    except Exception as e:
        log.warning("hgetall %s error: %s", AUTOPILOT_KEY, e)
        full = {}

    state["persister_enabled"] = str(
        full.get(FLAG_PERSISTER_ENABLED, "")
    ).strip() == "1"
    state["ph_enabled"] = str(
        full.get(FLAG_DRIFT_PH_ENABLED, "")
    ).strip() == "1"

    if state["persister_enabled"]:
        ts = full.get(activated_at_field(FLAG_PERSISTER_ENABLED))
        try:
            state["persister_age_h"] = max(0.0, (now_ms - int(ts or 0)) / 3_600_000.0)
        except (TypeError, ValueError):
            pass
    if state["ph_enabled"]:
        ts = full.get(activated_at_field(FLAG_DRIFT_PH_ENABLED))
        try:
            state["ph_age_h"] = max(0.0, (now_ms - int(ts or 0)) / 3_600_000.0)
        except (TypeError, ValueError):
            pass

    try:
        thr = full.get(FIELD_EXPECTANCY_THRESHOLD)
        if thr is not None:
            state["expectancy_threshold"] = float(thr)
    except (TypeError, ValueError):
        pass

    for kind in allowlist:
        flag = kind_demote_flag(kind)
        state["per_kind_demote"][kind] = (
            str(full.get(flag, "")).strip() == "1"
        )

    try:
        state["tracker_xlen"] = int(
            rc.xlen("stream:signals:gated_out_outcomes")
        )
    except Exception:
        pass

    for kind in allowlist:
        try:
            matched = 0
            for key in rc.scan_iter(match=f"drift:state:{kind}:*", count=200):
                sev = rc.hget(key, "severity")
                if str(sev or "").strip() == "critical":
                    matched += 1
            state["warn_per_kind"][kind] = matched
        except Exception:
            state["warn_per_kind"][kind] = 0

    return state


def collect_persister_stats(conn: Any) -> dict:
    """Row counts that surface persister liveness."""
    out = {"rows_1h": 0, "rows_24h": 0, "table_exists": False}
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM information_schema.tables "
                "WHERE table_name = 'signal_gated_out_outcomes' LIMIT 1"
            )
            out["table_exists"] = cur.fetchone() is not None
            if not out["table_exists"]:
                return out
            cur.execute(
                "SELECT "
                "  COUNT(*) FILTER (WHERE ingest_time_ms > (now_ms() - 3600000)),"
                "  COUNT(*) FILTER (WHERE ingest_time_ms > (now_ms() - 86400000))"
                " FROM signal_gated_out_outcomes"
            )
            row = cur.fetchone()
            if row:
                out["rows_1h"] = int(row[0] or 0)
                out["rows_24h"] = int(row[1] or 0)
    except Exception as e:
        log.warning("persister stats query failed: %s", e)
    return out


# ─── Pure narrative formatting ──────────────────────────────────────────────

def current_stage(state: dict) -> int:
    """Highest active stage (0/1/2/3)."""
    if any(state["per_kind_demote"].values()):
        return 3
    if state["ph_enabled"]:
        return 2
    if state["persister_enabled"]:
        return 1
    return 0


def fingerprint(state: dict, stats: dict) -> str:
    """Deterministic compact signature of stage state.

    Used to decide "did something change worth sending?" — bumps on stage
    transitions, per-kind activations, and expectancy threshold changes
    (rounded to 0.001 to avoid jitter notifications).
    """
    sig = {
        "stage": current_stage(state),
        "s1": int(state["persister_enabled"]),
        "s2": int(state["ph_enabled"]),
        "kinds": sorted(
            k for k, v in state["per_kind_demote"].items() if v
        ),
        "thr": round(state["expectancy_threshold"], 3),
        "table": int(stats["table_exists"]),
    }
    return json.dumps(sig, sort_keys=True)


def format_report(state: dict, stats: dict) -> str:
    """Build a Telegram HTML message describing the current rollout state.

    Pure function — no side effects, fully testable.
    """
    stage = current_stage(state)
    e = html.escape

    lines: list[str] = []
    lines.append(f"🚦 <b>Plan 2 Rollout — Stage {stage}/3</b>")
    lines.append("")

    # Stage 1
    if state["persister_enabled"]:
        lines.append(
            f"✅ <b>S1</b> persister active "
            f"({state['persister_age_h']:.1f}h)"
        )
    else:
        lines.append("⏳ <b>S1</b> persister SHADOW (not yet activated)")

    # Stage 2
    if state["ph_enabled"]:
        lines.append(
            f"✅ <b>S2</b> page_hinkley active "
            f"({state['ph_age_h']:.1f}h)"
        )
    elif state["persister_enabled"]:
        lines.append(
            f"⏳ <b>S2</b> page_hinkley pending "
            f"(need 48h S1, now {state['persister_age_h']:.1f}h)"
        )
    else:
        lines.append("⏸ <b>S2</b> blocked — S1 not active")

    # Stage 3 per-kind
    s3_active_kinds = [k for k, v in state["per_kind_demote"].items() if v]
    if s3_active_kinds:
        lines.append(
            f"✅ <b>S3</b> auto-demote: {', '.join(e(k) for k in s3_active_kinds)}"
        )
    elif state["ph_enabled"]:
        lines.append(
            f"⏳ <b>S3</b> per-kind pending "
            f"(need 168h S2, now {state['ph_age_h']:.1f}h)"
        )
    else:
        lines.append("⏸ <b>S3</b> blocked — S2 not active")

    lines.append("")
    lines.append("<b>Telemetry</b>")
    lines.append(f"persister rows 1h:  {stats['rows_1h']}")
    lines.append(f"persister rows 24h: {stats['rows_24h']}")
    lines.append(f"tracker xlen:       {state['tracker_xlen']}")
    if state["expectancy_threshold"] != 0.0:
        lines.append(
            f"expectancy thr:     {state['expectancy_threshold']:.4f} "
            f"(autotuned)"
        )
    else:
        lines.append("expectancy thr:     0.0000 (default)")
    if state["warn_per_kind"]:
        warn_str = ", ".join(
            f"{e(k)}={n}" for k, n in state["warn_per_kind"].items()
        )
        lines.append(f"critical warns:     {warn_str}")

    return "\n".join(lines)


def should_send(
    *, current_fingerprint: str, last_fingerprint: str | None,
    last_sent_ts_ms: int | None, now_ms: int, keepalive_hours: float,
) -> tuple[bool, str]:
    """Decide whether to send a report this cycle.

    Triggers:
      1. fingerprint changed → state advanced (always send)
      2. no prior send → first run after enable (send)
      3. last send was longer than keepalive_hours ago → heartbeat

    Returns (decision, reason). Reason goes to logs.
    """
    if last_sent_ts_ms is None:
        return True, "first_run"
    if current_fingerprint != (last_fingerprint or ""):
        return True, "fingerprint_changed"
    age_h = max(0.0, (now_ms - last_sent_ts_ms) / 3_600_000.0)
    if age_h >= keepalive_hours:
        return True, f"keepalive_{age_h:.1f}h"
    return False, f"no_change_age_{age_h:.1f}h"


# ─── Telegram + Redis side effects ───────────────────────────────────────────

def send_telegram(*, bot_token: str, chat_id: str, text: str, timeout: int = 10) -> bool:
    """POST to Telegram Bot API. Returns True on 2xx."""
    if not (bot_token and chat_id):
        return False
    try:
        data = json.dumps({
            "chat_id": chat_id,
            "text": text,
            "parse_mode": "HTML",
        }).encode()
        req = urllib.request.Request(
            f"https://api.telegram.org/bot{bot_token}/sendMessage",
            data=data,
            headers={"Content-Type": "application/json"},
        )
        urllib.request.urlopen(req, timeout=timeout)
        return True
    except urllib.error.HTTPError as he:
        body = he.read().decode("utf-8", errors="replace")[:300]
        log.warning("telegram HTTPError %d: %s", he.code, body)
        return False
    except Exception as e:
        log.warning("telegram send error: %s", e)
        return False


def update_last_send(rc: Any, *, fingerprint_value: str, now_ms: int) -> None:
    try:
        rc.hset(
            LAST_SEND_KEY,
            mapping={
                "fingerprint": fingerprint_value,
                "last_sent_ts_ms": str(now_ms),
            },
        )
    except Exception as e:
        log.warning("update_last_send hset error: %s", e)


def read_last_send(rc: Any) -> tuple[str | None, int | None]:
    try:
        d = rc.hgetall(LAST_SEND_KEY) or {}
        fp = d.get("fingerprint")
        ts = d.get("last_sent_ts_ms")
        return fp, int(ts) if ts else None
    except Exception:
        return None, None


# ─── Main loop ──────────────────────────────────────────────────────────────

def main() -> None:
    import redis  # type: ignore
    from prometheus_client import Counter, Gauge, start_http_server

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )

    enabled = _env_bool("PLAN2_TG_REPORTER_ENABLED", False)
    redis_url = _env(
        "PLAN2_TG_REPORTER_REDIS_URL",
        _env("REDIS_URL", "redis://redis-worker-1:6379/0"),
    )
    db_dsn = _env("PLAN2_TG_REPORTER_DB_DSN", _env("TRADES_DB_DSN", ""))
    port = _env_int("PLAN2_TG_REPORTER_PORT", 9943)
    interval_sec = _env_int("PLAN2_TG_REPORTER_INTERVAL_SEC", 3600)
    keepalive_h = float(_env_int("PLAN2_TG_KEEPALIVE_HOURS", 24))
    bot_token = _env("PLAN2_TG_BOT_TOKEN", _env("TELEGRAM_BOT_TOKEN", ""))
    chat_id = _env("PLAN2_TG_CHAT_ID", _env("TELEGRAM_CHAT_ID", ""))
    allowlist = _env_csv("PLAN2_S3_KIND_ALLOWLIST", "meta_lr_blend,v14_of")

    log.info(
        "plan2_tg_reporter starting | enabled=%s port=%d interval=%ds keepalive=%.0fh kinds=%s",
        enabled, port, interval_sec, keepalive_h, allowlist,
    )

    rc = redis.from_url(redis_url, decode_responses=True)

    start_http_server(port)
    c_sent = Counter("plan2_tg_reporter_sent_total", "Reports sent", ["reason"])
    c_skipped = Counter("plan2_tg_reporter_skipped_total", "Reports skipped", ["reason"])
    c_err = Counter("plan2_tg_reporter_error_total", "Cycle errors", [])
    g_last_send_age = Gauge(
        "plan2_tg_reporter_last_send_age_seconds",
        "Seconds since last Telegram send",
    )

    conn = None

    def _get_conn():
        nonlocal conn
        if conn is None or conn.closed:
            import psycopg2
            conn = psycopg2.connect(db_dsn)
        return conn

    while True:
        try:
            time.sleep(interval_sec)
            now_ms = int(time.time() * 1000)

            stats = {"rows_1h": 0, "rows_24h": 0, "table_exists": False}
            if db_dsn:
                try:
                    stats = collect_persister_stats(_get_conn())
                except Exception as e:
                    c_err.inc()
                    log.warning("collect_persister_stats error: %s", e)
                    conn = None

            state = collect_autopilot_state(
                rc, allowlist=allowlist, now_ms=now_ms,
            )

            fp = fingerprint(state, stats)
            last_fp, last_ts = read_last_send(rc)
            if last_ts:
                g_last_send_age.set(max(0.0, (now_ms - last_ts) / 1000.0))

            decision, reason = should_send(
                current_fingerprint=fp,
                last_fingerprint=last_fp,
                last_sent_ts_ms=last_ts,
                now_ms=now_ms,
                keepalive_hours=keepalive_h,
            )

            report_text = format_report(state, stats)

            if not decision:
                c_skipped.labels(reason=reason).inc()
                log.debug("plan2_tg skip: %s", reason)
                continue

            if not enabled:
                # Shadow mode: print to stdout, don't send.
                c_skipped.labels(reason=f"shadow:{reason}").inc()
                log.info("plan2_tg SHADOW would send (%s):\n%s", reason, report_text)
                # Still update fingerprint so we don't keep printing the same shadow report.
                update_last_send(rc, fingerprint_value=fp, now_ms=now_ms)
                continue

            ok = send_telegram(
                bot_token=bot_token, chat_id=chat_id, text=report_text,
            )
            if ok:
                c_sent.labels(reason=reason).inc()
                update_last_send(rc, fingerprint_value=fp, now_ms=now_ms)
                log.info("plan2_tg sent (%s)", reason)
            else:
                c_err.inc()
                log.warning("plan2_tg send failed (will retry next cycle)")

        except Exception as e:
            c_err.inc()
            log.warning("plan2_tg main loop error: %s", e)


if __name__ == "__main__":
    main()
