from utils.time_utils import get_ny_time_millis
import os
print("DEBUG: Top level start")
import json
import time
import html
from typing import Any, Dict, List

import psycopg2
import psycopg2.extras

import redis  # pip install redis


ANALYTICS_DB_DSN = os.getenv("ANALYTICS_DB_DSN", "")
REPORT_SQL_FILE = os.getenv(
    "REPORT_SQL_FILE",
    "python-worker/tools/trade_diagnostics/sql/trades_window_join_p0.sql",
)

REPORT_HOURS = int(os.getenv("REPORT_HOURS", "24"))
REPORT_TO_MS = int(os.getenv("REPORT_TO_MS", "0"))
REPORT_FROM_MS = int(os.getenv("REPORT_FROM_MS", "0"))
REPORT_MIN_TRADES = int(os.getenv("REPORT_MIN_TRADES", "50"))

# thresholds
RESP_MIN_BPS = float(os.getenv("REPORT_RESP_MIN_BPS", "2.0"))
ADVERSE_200_BPS = float(os.getenv("REPORT_ADVERSE_200_BPS", "3.0"))
COST_DOM_BPS = float(os.getenv("REPORT_COST_DOM_BPS", "10.0"))
TIME_TO_MFE_MAX_MS = int(os.getenv("REPORT_TIME_TO_MFE_MAX_MS", "1200"))
L2_AGE_MS = float(os.getenv("REPORT_L2_AGE_MS", "250.0"))
L2_STALE_RATIO = float(os.getenv("REPORT_L2_STALE_RATIO", "0.2"))
GIVEBACK_FRAC = float(os.getenv("REPORT_GIVEBACK_FRAC", "0.5"))

REPORT_MAX_CHARS = int(os.getenv("REPORT_MAX_CHARS", "3800"))

# Telegram publish via redis stream to notify_worker
REPORT_SEND_TELEGRAM = os.getenv("REPORT_SEND_TELEGRAM", "0") == "1"
REPORT_REDIS_URL = os.getenv("REPORT_REDIS_URL", "redis://localhost:6379/0")
REPORT_STREAM_KEY = os.getenv("REPORT_STREAM_KEY", "stream:notify")

# IMPORTANT:
# - fields: XADD key type=report text=<html> ts_ms=...
# - payload_json: XADD key payload='{"type":"report","text":"..."}'
REPORT_STREAM_FORMAT = os.getenv("REPORT_STREAM_FORMAT", "fields")  # fields|payload_json


def now_ms() -> int:
    return get_ny_time_millis()


def load_sql(path: str) -> str:
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


def safe_float(x: Any, default: float = 0.0) -> float:
    try:
        if x is None:
            return default
        return float(x)
    except Exception:
        return default


def safe_int(x: Any, default: int = 0) -> int:
    try:
        if x is None:
            return default
        return int(x)
    except Exception:
        return default


def quantile(xs: List[float], q: float) -> float:
    if not xs:
        return 0.0
    xs2 = sorted(xs)
    idx = int(round((len(xs2) - 1) * q))
    idx = max(0, min(idx, len(xs2) - 1))
    return float(xs2[idx])


def parse_features_json(v: Any) -> Dict[str, Any]:
    if v is None:
        return {}
    if isinstance(v, dict):
        return v
    if isinstance(v, str) and v.strip():
        try:
            return json.loads(v)
        except Exception:
            return {}
    return {}


def get_adverse_200_bps(features: Dict[str, Any]) -> float:
    try:
        d = features.get("adverse_bps_t") or {}
        return safe_float(d.get("200", 0.0), 0.0)
    except Exception:
        return 0.0


def fees_bps(fees_usd: float, notional_usd: float) -> float:
    if notional_usd <= 0:
        return 0.0
    return 10000.0 * fees_usd / notional_usd


def pnl_bps(pnl_usd: float, notional_usd: float) -> float:
    if notional_usd <= 0:
        return 0.0
    return 10000.0 * pnl_usd / notional_usd


def classify_loss(
    pnl_net: float,
    cost_bps_val: float,
    mfe_bps_val: float,
    close_reason: str,
    adverse_200_bps: float,
    time_to_mfe_ms: int,
    giveback: float,
    mfe_pnl: float,
    l2_age_ms: float,
    l2_stale_now: float,
) -> str:
    if pnl_net >= 0:
        return "WIN_OR_BE"

    if cost_bps_val >= COST_DOM_BPS or cost_bps_val >= max(3.0, 0.7 * max(mfe_bps_val, 0.0)):
        return "COST_DOMINATES"

    if mfe_bps_val < RESP_MIN_BPS and adverse_200_bps >= ADVERSE_200_BPS:
        return "NO_FOLLOW_THROUGH"

    if (l2_age_ms >= L2_AGE_MS) or (l2_stale_now >= L2_STALE_RATIO):
        return "L2_STALE_EXEC"

    if mfe_pnl > 0 and giveback >= GIVEBACK_FRAC * mfe_pnl:
        return "GIVEBACK"

    cr = (close_reason or "").upper()
    if "SL" in cr or "STOP" in cr:
        return "STOP_HIT"

    if time_to_mfe_ms > TIME_TO_MFE_MAX_MS and mfe_bps_val < RESP_MIN_BPS:
        return "LATE_ENTRY"

    return "OTHER"


def fetch_rows(sql: str, from_ms: int, to_ms: int) -> List[Dict[str, Any]]:
    with psycopg2.connect(ANALYTICS_DB_DSN) as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(sql, {"from_ms": from_ms, "to_ms": to_ms})
        return list(cur.fetchall())


def build_report(from_ms: int, to_ms: int) -> str:
    if not ANALYTICS_DB_DSN:
        raise RuntimeError("ANALYTICS_DB_DSN is required")

    sql = load_sql(REPORT_SQL_FILE)
    rows = fetch_rows(sql, from_ms, to_ms)
    n = len(rows)
    if n < REPORT_MIN_TRADES:
        return f"Trade Quality Report v2\nwindow_ms: {from_ms}..{to_ms}\nToo few trades: {n} < {REPORT_MIN_TRADES}"

    total_pnl = 0.0
    wins = 0
    losers = 0
    missing_p0 = 0

    bucket = {}
    slice_agg = {}

    for r in rows:
        pnl_net = safe_float(r.get("pnl_net"), 0.0)
        total_pnl += pnl_net
        if pnl_net > 0:
            wins += 1
        elif pnl_net < 0:
            losers += 1

        if r.get("scenario") is None and r.get("regime") is None and r.get("session") is None and r.get("features_json") is None:
            missing_p0 += 1

        notional = safe_float(r.get("notional_usd"), 0.0)
        fee = safe_float(r.get("fees"), 0.0)
        f_bps = fees_bps(fee, notional)

        spread = safe_float(r.get("spread_bps_at_entry"), 0.0)
        slip = safe_float(r.get("slippage_bps_est"), 0.0)
        cost = f_bps + spread + slip

        mfe_bps_val = r.get("mfe_bps")
        mae_bps_val = r.get("mae_bps")
        if mfe_bps_val is None:
            mfe_bps_val = pnl_bps(safe_float(r.get("mfe_pnl"), 0.0), notional)
        else:
            mfe_bps_val = safe_float(mfe_bps_val, 0.0)
        if mae_bps_val is None:
            mae_bps_val = abs(pnl_bps(safe_float(r.get("mae_pnl"), 0.0), notional))
        else:
            mae_bps_val = safe_float(mae_bps_val, 0.0)

        time_to_mfe = safe_int(r.get("time_to_mfe_ms"), 0)
        close_reason = str(r.get("close_reason") or "")
        l2_age = safe_float(r.get("health_avg_l2_age_ms"), 0.0)
        l2_stale = safe_float(r.get("health_l2_stale_ratio_now"), 0.0)

        features = parse_features_json(r.get("features_json"))
        adverse_200 = get_adverse_200_bps(features)

        giveback = safe_float(r.get("giveback"), 0.0)
        mfe_pnl = safe_float(r.get("mfe_pnl"), 0.0)

        b = classify_loss(
            pnl_net=pnl_net,
            cost_bps_val=cost,
            mfe_bps_val=mfe_bps_val,
            close_reason=close_reason,
            adverse_200_bps=adverse_200,
            time_to_mfe_ms=time_to_mfe,
            giveback=giveback,
            mfe_pnl=mfe_pnl,
            l2_age_ms=l2_age,
            l2_stale_now=l2_stale,
        )

        if pnl_net < 0:
            a = bucket.setdefault(b, {"trades": 0, "pnl_sum": 0.0, "cost": [], "mfe": [], "mae": [], "adv200": [], "l2age": []})
            a["trades"] += 1
            a["pnl_sum"] += pnl_net
            a["cost"].append(cost)
            a["mfe"].append(mfe_bps_val)
            a["mae"].append(mae_bps_val)
            a["adv200"].append(adverse_200)
            a["l2age"].append(l2_age)

        sym = str(r.get("symbol") or "")
        scn = str(r.get("scenario") or "")
        ses = str(r.get("session") or "")
        key = (sym, scn, ses)
        s = slice_agg.setdefault(key, {"trades": 0, "wins": 0, "pnl_sum": 0.0, "cost": []})
        s["trades"] += 1
        s["wins"] += 1 if pnl_net > 0 else 0
        s["pnl_sum"] += pnl_net
        s["cost"].append(cost)

    bucket_items = []
    for name, a in bucket.items():
        bucket_items.append((
            name,
            a["trades"],
            a["pnl_sum"],
            quantile(a["cost"], 0.5),
            quantile(a["mfe"], 0.5),
            quantile(a["mae"], 0.5),
            quantile(a["adv200"], 0.9),
            quantile(a["l2age"], 0.9),
        ))
    bucket_items.sort(key=lambda x: x[2])

    slice_items = []
    for (sym, scn, ses), a in slice_agg.items():
        wr = (a["wins"] / a["trades"]) if a["trades"] else 0.0
        slice_items.append((sym, scn, ses, a["trades"], wr, a["pnl_sum"], quantile(a["cost"], 0.5)))
    slice_items.sort(key=lambda x: x[5])

    winrate = (wins / n) if n else 0.0
    missing_pct = (missing_p0 / n) * 100.0 if n else 0.0

    lines: List[str] = []
    lines.append("Trade Quality Report v2")
    lines.append(f"window_ms: {from_ms}..{to_ms}")
    lines.append(f"trades: {n} | winrate: {winrate*100:.1f}% | pnl_sum: {total_pnl:.2f}")
    lines.append(f"losers: {losers} | missing_p0: {missing_p0} ({missing_pct:.1f}%)")
    lines.append("")
    lines.append("TOP-3 LOSS BUCKETS (by pnl_sum):")
    lines.append("bucket | trades | pnl_sum | cost_med_bps | mfe_med_bps | mae_med_bps | adv200_p90 | l2age_p90_ms")
    for it in bucket_items[:3]:
        lines.append(f"{it[0]} | {it[1]} | {it[2]:.2f} | {it[3]:.2f} | {it[4]:.2f} | {it[5]:.2f} | {it[6]:.2f} | {it[7]:.1f}")
    lines.append("")
    lines.append("TOP-5 TOXIC SLICES (symbol×scenario×session by pnl_sum):")
    lines.append("symbol | scenario | session | trades | winrate | pnl_sum | cost_med_bps")
    for it in slice_items[:5]:
        lines.append(f"{it[0]} | {it[1]} | {it[2]} | {it[3]} | {it[4]*100:.1f}% | {it[5]:.2f} | {it[6]:.2f}")

    text = "\n".join(lines)
    if len(text) > REPORT_MAX_CHARS:
        text = text[:REPORT_MAX_CHARS] + "\n...trimmed..."
    return text


def publish_to_telegram(report_text: str, ts_ms: int) -> None:
    if not REPORT_SEND_TELEGRAM:
        return

    text_html = "<pre>" + html.escape(report_text) + "</pre>"
    r = redis.Redis.from_url(REPORT_REDIS_URL, decode_responses=True)

    if REPORT_STREAM_FORMAT == "payload_json":
        payload = {"type": "report", "text": text_html, "ts_ms": ts_ms}
        r.xadd(REPORT_STREAM_KEY, {"payload": json.dumps(payload, ensure_ascii=False)}, maxlen=50000)
    else:
        r.xadd(REPORT_STREAM_KEY, {"type": "report", "text": text_html, "ts_ms": str(ts_ms)}, maxlen=50000)


def main() -> None:
    print("DEBUG: Script started")
    to_ms = REPORT_TO_MS or now_ms()
    from_ms = REPORT_FROM_MS or (to_ms - REPORT_HOURS * 3600 * 1000)
    print(f"DEBUG: Querying {from_ms} to {to_ms}")
    report = build_report(from_ms, to_ms)
    print(f"DEBUG: Report built, length {len(report)}")
    print(report)
    publish_to_telegram(report, ts_ms=to_ms)


if __name__ == "__main__":
    main()
