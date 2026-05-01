from __future__ import annotations
"""policy_effectiveness_telegram_report_p73_v1.py

P73: Nightly/periodic Telegram summary for policy effectiveness (P71).

Reads P71 outputs from Redis hash `settings:dynamic_cfg` (or DYN_CFG_KEY)
and emits a short HTML report to Telegram via Redis stream `notify:telegram`
(or direct Telegram API).

Key goals:
- deterministic formatting (stable order, stable rounding)
- cooldown + dedup (do not spam the same payload)
- fail-open (reporting must not break trading path)

Env:
  REDIS_URL=redis://...
  DYN_CFG_KEY=settings:dynamic_cfg

  TELEGRAM_MODE=redis|direct
  TELEGRAM_REDIS_URL=redis://... (optional, defaults to REDIS_URL)
  TELEGRAM_NOTIFY_STREAM=notify:telegram
  TELEGRAM_BOT_TOKEN=... (direct)
  TELEGRAM_CHAT_ID=... (direct)

  ENABLE_POLICY_EFFECTIVENESS_TG_REPORT=1
  POLICY_EFF_TG_COOLDOWN_SEC=21600          # 6h
  POLICY_EFF_TG_FORCE_ON_CRITICAL=1         # bypass cooldown if critical
  POLICY_EFF_TG_STALE_WARN_SEC=1800         # 30m
  POLICY_EFF_TG_STALE_CRIT_SEC=7200         # 2h
  POLICY_EFF_TG_MIN_TOTAL_N=200             # skip if too few samples (unless critical)
  POLICY_EFF_TG_STATE_KEY=ops:policy_eff:p73:tg_state
"""

from utils.time_utils import get_ny_time_millis

import argparse
import hashlib
import html
import os
import time
from typing import Any, Dict, Tuple


def now_ms() -> int:
    return get_ny_time_millis()


def _i(v: Any, default: int = 0) -> int:
    try:
        if v is None:
            return default
        if isinstance(v, bool):
            return int(v)
        return int(float(str(v)))
    except Exception:
        return default


def _f(v: Any, default: float = 0.0) -> float:
    try:
        if v is None:
            return default
        return float(str(v))
    except Exception:
        return default


def _fmt_pct(x: float, digits: int = 2) -> str:
    return f"{100.0 * x:.{digits}f}%"


def _sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()


def _telegram_send_redis(redis_url: str, stream: str, text_html: str) -> None:
    import redis  # redis-py

    r = redis.Redis.from_url(redis_url)
    r.xadd(
        stream,
        {
            "type": "report",
            "text": text_html,
            "parse_mode": "HTML",
            "disable_web_page_preview": "1",
            "ts": str(now_ms()),
        },
        maxlen=200000,
        approximate=True,
    )


def _telegram_send_direct(token: str, chat_id: str, text_html: str) -> None:
    import requests

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    requests.post(
        url,
        json={
            "chat_id": chat_id,
            "text": text_html,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        },
        timeout=15,
    ).raise_for_status()


def _classify_severity(
    report_age_sec: int,
    baseline_ok_present: int,
    total_n_24h: int,
    stale_warn_sec: int,
    stale_crit_sec: int,
) -> str:
    if report_age_sec >= stale_crit_sec:
        return "critical"
    if baseline_ok_present == 0 and total_n_24h >= 50:
        return "critical"
    if report_age_sec >= stale_warn_sec:
        return "warning"
    return "info"


def build_message(cfg: Dict[str, str]) -> Tuple[str, Dict[str, Any]]:
    """Build stable HTML message + extracted stats."""
    ts_ms = _i(cfg.get("policy_effectiveness_last_ts_ms"), 0)
    in_ts_ms = _i(cfg.get("policy_effectiveness_input_last_ts_ms"), 0)
    baseline_ok = _i(cfg.get("policy_effectiveness_baseline_ok_present"), 0)
    total_n = _i(cfg.get("policy_effectiveness_total_n_24h"), 0)

    age_sec = int((now_ms() - ts_ms) / 1000) if ts_ms > 0 else 10**9
    in_age_sec = int((now_ms() - in_ts_ms) / 1000) if in_ts_ms > 0 else 10**9

    modes = ["ok", "warn", "block", "unknown"]
    shares = {m: _f(cfg.get(f"policy_effectiveness_share_24h_{m}"), 0.0) for m in modes}

    deltas = {}
    for m in ["warn", "block", "unknown"]:
        deltas[m] = {
            "exp_r": _f(cfg.get(f"policy_effectiveness_expectancy_r_delta_24h_{m}"), 0.0),
            "prec_top5p": _f(cfg.get(f"policy_effectiveness_precision_top5p_delta_24h_{m}"), 0.0),
            "ece": _f(cfg.get(f"policy_effectiveness_ece_delta_24h_{m}"), 0.0),
        },

    def _code(x: str) -> str:
        return f"<code>{html.escape(x)}</code>"

    lines = ["<b>Policy effectiveness (24h)</b>"]
    if ts_ms <= 0:
        lines.append("status=" + _code("no_data"))
    else:
        lines.append(
            "ts_ms="
            + _code(str(ts_ms))
            + " age_s="
            + _code(str(age_sec))
            + " in_age_s="
            + _code(str(in_age_sec))
            + " total_n="
            + _code(str(total_n))
            + " baseline_ok="
            + _code(str(baseline_ok))
        )
        lines.append(
            "share: ok="
            + _code(_fmt_pct(shares["ok"]))
            + " warn="
            + _code(_fmt_pct(shares["warn"]))
            + " block="
            + _code(_fmt_pct(shares["block"]))
            + " unk="
            + _code(_fmt_pct(shares["unknown"]))
        )
        for m in ["warn", "block"]:
            d = deltas[m]
            lines.append(
                f"Δ vs ok ({m}): exp_R="
                + _code(f"{d['exp_r']:+.3f}")
                + " prec_top5p="
                + _code(f"{d['prec_top5p']:+.3f}")
                + " ece="
                + _code(f"{d['ece']:+.3f}")
            )

    meta = {
        "ts_ms": ts_ms,
        "age_sec": age_sec,
        "in_age_sec": in_age_sec,
        "baseline_ok": baseline_ok,
        "total_n": total_n,
        "shares": shares,
        "deltas": deltas,
    },
    return "\n".join(lines), meta


def should_send(
    r,
    state_key: str,
    msg_html: str,
    severity: str,
    cooldown_sec: int,
    force_on_critical: int,
) -> Tuple[bool, str]:
    """Returns (send?, reason)."""
    try:
        st = r.hgetall(state_key) or {}
    except Exception:
        st = {}

    last_ts = _i(st.get("last_sent_ts_ms"), 0)
    last_hash = (st.get("last_hash") or "").strip()
    last_sev = (st.get("last_severity") or "").strip()

    h = _sha1(msg_html)
    age_ms = now_ms() - last_ts if last_ts > 0 else 10**12

    if severity == "critical" and force_on_critical == 1:
        if last_sev != "critical" or age_ms >= int(0.2 * cooldown_sec * 1000):
            return True, "critical_bypass"

    if last_hash == h and age_ms < cooldown_sec * 1000:
        return False, "dedup_cooldown"
    if age_ms < cooldown_sec * 1000:
        return False, "cooldown"
    return True, "ok"


def run_once() -> int:
    enable = int(os.getenv("ENABLE_POLICY_EFFECTIVENESS_TG_REPORT", "0") or 0)
    if enable != 1:
        return 0

    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    dyn_key = os.getenv("DYN_CFG_KEY", "settings:dynamic_cfg")
    state_key = os.getenv("POLICY_EFF_TG_STATE_KEY", "ops:policy_eff:p73:tg_state")

    cooldown_sec = int(os.getenv("POLICY_EFF_TG_COOLDOWN_SEC", "21600") or 21600)
    stale_warn_sec = int(os.getenv("POLICY_EFF_TG_STALE_WARN_SEC", "1800") or 1800)
    stale_crit_sec = int(os.getenv("POLICY_EFF_TG_STALE_CRIT_SEC", "7200") or 7200)
    min_total_n = int(os.getenv("POLICY_EFF_TG_MIN_TOTAL_N", "200") or 200)
    force_on_critical = int(os.getenv("POLICY_EFF_TG_FORCE_ON_CRITICAL", "1") or 1)

    import redis  # redis-py

    r = redis.Redis.from_url(redis_url, decode_responses=True)
    try:
        cfg = r.hgetall(dyn_key) or {}
    except Exception:
        cfg = {}

    msg, meta = build_message(cfg)
    sev = _classify_severity(
        report_age_sec=int(meta.get("age_sec", 10**9)),
        baseline_ok_present=int(meta.get("baseline_ok", 0)),
        total_n_24h=int(meta.get("total_n", 0)),
        stale_warn_sec=stale_warn_sec,
        stale_crit_sec=stale_crit_sec,
    )

    if int(meta.get("total_n", 0)) < min_total_n and sev != "critical":
        return 0

    send, reason = should_send(
        r=r,
        state_key=state_key,
        msg_html=msg,
        severity=sev,
        cooldown_sec=cooldown_sec,
        force_on_critical=force_on_critical,
    )
    if not send:
        return 0

    tg_mode = os.getenv("TELEGRAM_MODE", "redis").strip().lower()
    if tg_mode == "direct":
        token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
        chat_id = os.getenv("TELEGRAM_CHAT_ID", "").strip()
        if token and chat_id:
            _telegram_send_direct(token, chat_id, msg)
        else:
            tg_mode = "redis"

    if tg_mode != "direct":
        nredis = os.getenv("TELEGRAM_REDIS_URL", redis_url)
        stream_out = os.getenv("TELEGRAM_NOTIFY_STREAM", os.getenv("CRYPTO_NOTIFY_STREAM", "notify:telegram"))
        _telegram_send_redis(nredis, stream_out, msg)

    try:
        r.hset(
            state_key,
            mapping={
                "last_sent_ts_ms": str(now_ms()),
                "last_hash": _sha1(msg),
                "last_severity": sev,
                "last_reason": reason,
            },
        )
        r.expire(state_key, int(os.getenv("POLICY_EFF_TG_STATE_TTL_SEC", "2592000") or 2592000))
    except Exception:
        pass

    return 0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--once", action="store_true", help="Run one iteration and exit")
    args = ap.parse_args()
    if args.once:
        return run_once()
    return run_once()


if __name__ == "__main__":
    raise SystemExit(main())
