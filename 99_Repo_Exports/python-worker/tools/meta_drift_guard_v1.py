from __future__ import annotations

import argparse
import json
import os
from typing import Any

import redis

from domain.evidence_keys import MetaKeys
from utils.time_utils import get_ny_time_millis
from core.redis_keys import RedisStreams as RS


def now_ms() -> int:
    return get_ny_time_millis()


def safe_float(x: Any, d: float = 0.0) -> float:
    try:
        v = float(x)
        return v if v == v and abs(v) != float("inf") else d
    except Exception:
        return d


def safe_int(x: Any, d: int = 0) -> int:
    try:
        return int(float(x))
    except Exception:
        return d


def _loads_maybe_json(x: Any) -> dict[str, Any]:
    if x is None:
        return {}
    if isinstance(x, dict):
        return x
    if isinstance(x, (bytes, bytearray)):
        try:
            x = x.decode("utf-8", "ignore")
        except Exception:
            return {}
    if isinstance(x, str):
        s = x.strip()
        if not s:
            return {}
        try:
            v = json.loads(s)
            return v if isinstance(v, dict) else {}
        except Exception:
            return {}
    return {}


def _extract_meta(fields: dict[str, Any]) -> dict[str, Any]:
    meta = _loads_maybe_json(fields.get("meta") or fields.get("metadata"))
    if meta:
        return meta
    payload = _loads_maybe_json(fields.get("payload"))
    if isinstance(payload, dict):
        m2 = _loads_maybe_json(payload.get("meta") or payload.get("metadata"))
        if m2:
            return m2
    return {}


def _extract_evidence(meta: dict[str, Any]) -> dict[str, Any]:
    if isinstance(meta.get("of_confirm"), dict):
        oc = meta.get("of_confirm") or {}
        if isinstance(oc.get("evidence"), dict):
            return oc.get("evidence") or {}
    if isinstance(meta.get("evidence"), dict):
        return meta.get("evidence") or {}
    return {}


def read_trades_closed(r: redis.Redis, stream: str, since_ms: int, max_scan: int):
    last_id = "+"
    scanned = 0
    while scanned < max_scan:
        batch = r.xrevrange(stream, max=last_id, min="-", count=2000)
        if not batch:
            break
        if len(batch) == 1 and batch[0][0] == last_id:
            break
        for msg_id, fields in batch:
            scanned += 1
            if msg_id == last_id:
                continue
            last_id = msg_id
            ts = safe_int(fields.get("ts_ms", fields.get("ts", fields.get("timestamp", 0))), 0)
            if ts and ts < since_ms:
                scanned = max_scan
                break
            row = dict(fields)
            row["_ts_ms"] = ts
            yield row


def notify(r: redis.Redis, text: str) -> None:
    stream = os.getenv("NOTIFY_TELEGRAM_STREAM", RS.NOTIFY_TELEGRAM)
    r.xadd(stream, {"type": "report", "text": text, "ts": str(now_ms())}, maxlen=200000, approximate=True)


def _read_freeze_state(r: redis.Redis, cfg_prefix: str, symbols: list[str]) -> Tuple[bool, dict[str, int]]:
    states: dict[str, int] = {}
    any_frozen = False
    for sym in symbols:
        hk = f"{cfg_prefix}{sym}"
        v = r.hget(hk, "meta_model_freeze")
        try:
            x = int(float(v)) if v is not None else 0
        except Exception:
            x = 0
        x = 1 if x != 0 else 0
        states[sym] = x
        any_frozen = any_frozen or (x == 1)
    return any_frozen, states


def _cooldown_ok(r: redis.Redis, key: str, cooldown_ms: int) -> bool:
    try:
        last = int(r.get(key) or "0")
    except Exception:
        last = 0
    if last <= 0:
        return True
    return (now_ms() - last) >= cooldown_ms


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--redis-url", default=os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0"))
    ap.add_argument("--stream", default=os.getenv("TRADES_CLOSED_STREAM", "trades:closed"))
    ap.add_argument("--since-min", type=int, default=180)
    ap.add_argument("--max-scan", type=int, default=300000)
    ap.add_argument("--p50-min", type=float, default=float(os.getenv("META_DRIFT_PEDGE_P50_MIN", "0.20")))
    ap.add_argument("--pzero-rate-max", type=float, default=float(os.getenv("META_DRIFT_PEDGE_ZERO_RATE_MAX", "0.05")))
    ap.add_argument("--missing-rate-max", type=float, default=float(os.getenv("META_DRIFT_MISSING_META_RATE_MAX", "0.05")))
    ap.add_argument("--cov-p50-min", type=float, default=float(os.getenv("META_DRIFT_COV_P50_MIN", "0.85")))
    ap.add_argument("--cov-bad-rate-max", type=float, default=float(os.getenv("META_DRIFT_COV_BAD_RATE_MAX", "0.10")))

    # Legacy (P6): direct freeze write (discouraged)
    ap.add_argument("--freeze-key", default=os.getenv("META_MODEL_FREEZE_KEY", "cfg:meta_model_freeze"))
    ap.add_argument("--freeze-write-mode", default=os.getenv("META_MODEL_FREEZE_WRITE_MODE", "SET"))
    ap.add_argument("--freeze-field", default=os.getenv("META_MODEL_FREEZE_FIELD", ""))
    ap.add_argument("--write-freeze", action="store_true")

    # P6.1: proposal into cfg:suggestions contour (sid/meta/approvals)
    ap.add_argument("--emit-suggestion", action="store_true")
    ap.add_argument("--unfreeze-on-ok", action="store_true")
    ap.add_argument("--suggestions-prefix", default=os.getenv("META_DRIFT_SUGGESTIONS_PREFIX", "cfg:suggestions:entry_policy"))
    ap.add_argument("--suggestions-scope", default=os.getenv("META_DRIFT_SUGGESTIONS_SCOPE", "ALL"))
    ap.add_argument("--suggestions-ttl-sec", type=int, default=int(os.getenv("META_DRIFT_SUGGESTIONS_TTL_SEC", "86400") or 86400))
    ap.add_argument("--suggestions-cooldown-min", type=int, default=int(os.getenv("META_DRIFT_SUGGESTIONS_COOLDOWN_MIN", "60") or 60))

    ap.add_argument("--symbols", default=os.getenv("CANARY_SYMBOLS", "BTCUSDT,ETHUSDT"))
    ap.add_argument("--cfg-prefix", default=os.getenv("CFG_HASH_PREFIX", "config:orderflow:"))
    ap.add_argument("--freeze-mode", default=os.getenv("META_FREEZE_MODE", "OPEN"))

    ap.add_argument("--notify", action="store_true")
    args = ap.parse_args()

    r = redis.Redis.from_url(args.redis_url, decode_responses=True)
    since_ms = now_ms() - args.since_min * 60_000

    p: list[float] = []
    covs: list[float] = []
    cov_bad = 0
    miss = 0
    n_total = 0
    for x in read_trades_closed(r, args.stream, since_ms, args.max_scan):
        n_total += 1
        meta = _extract_meta(x)
        ev = _extract_evidence(meta)
        pv = ev.get(MetaKeys.P, meta.get(MetaKeys.P, None))

        cv = ev.get("meta_model_feature_coverage", ev.get(MetaKeys.FEATURE_COVERAGE, meta.get("meta_model_feature_coverage", None)))
        if cv is not None:
            c = safe_float(cv, -1.0)
            if c >= 0.0:
                covs.append(c)
                if c < float(args.cov_p50_min):
                    cov_bad += 1

        if pv is None:
            miss += 1
            continue
        p.append(safe_float(pv, 0.0))

    if n_total == 0:
        return

    n = n_total
    miss_rate = miss / max(1, n)
    p_sorted = sorted(p) if p else []
    p50 = p_sorted[len(p_sorted) // 2] if p_sorted else 0.0
    pzero_rate = (sum(1 for v in p if abs(v) < 1e-12) / max(1, len(p))) if p else 1.0

    cov_sorted = sorted(covs) if covs else []
    cov_p50 = cov_sorted[len(cov_sorted)//2] if cov_sorted else 0.0
    cov_bad_rate = float(cov_bad) / float(max(1, len(covs))) if covs else 1.0

    alerts = []
    if miss_rate > args.missing_rate_max:
        alerts.append(f"missing_meta_rate>{args.missing_rate_max}")
    if p50 < args.p50_min:
        alerts.append(f"meta_p50<{args.p50_min}")
    if pzero_rate > args.pzero_rate_max:
        alerts.append(f"meta_p_zero_rate>{args.pzero_rate_max}")

    if cov_p50 < args.cov_p50_min:
        alerts.append(f"cov_p50<{args.cov_p50_min}")
    if cov_bad_rate > args.cov_bad_rate_max:
        alerts.append(f"cov_bad_rate>{args.cov_bad_rate_max}")

    freeze = 1 if alerts else 0

    report = {
        "ts_ms": now_ms(),
        "since_min": int(args.since_min),
        "n_total": int(n),
        "n_with_p": int(len(p)),
        "miss_rate": float(miss_rate),
        "p50": float(p50),
        "pzero_rate": float(pzero_rate),
        "cov_p50": float(cov_p50),
        "cov_bad_rate": float(cov_bad_rate),
        "alerts": alerts,
        "freeze": int(freeze),
    }
    r.set("meta_drift:last_report", json.dumps(report, separators=(",", ":")))
    r.set("meta_drift:last_ts_ms", str(report["ts_ms"]))

    # ---- P6.1: emit proposal into approval/apply contour ----
    if args.emit_suggestion:
        syms = [s.strip().upper() for s in str(args.symbols or "").split(",") if s.strip()]
        scope = str(args.suggestions_scope or "ALL").strip().upper()
        any_frozen, _states = _read_freeze_state(r, str(args.cfg_prefix), syms)

        want_freeze = (freeze == 1 and not any_frozen)
        want_unfreeze = (freeze == 0 and bool(args.unfreeze_on_ok) and any_frozen)

        cd_ms = int(max(1, args.suggestions_cooldown_min) * 60_000)
        if (want_freeze or want_unfreeze) and _cooldown_ok(r, "meta_drift:suggestions:last_ms", cd_ms):
            try:
                from tools.propose_meta_freeze_suggestion_v1 import emit_meta_freeze_suggestion
            except Exception:
                from propose_meta_freeze_suggestion_v1 import emit_meta_freeze_suggestion  # type: ignore

            sid = emit_meta_freeze_suggestion(
                r,
                prefix=str(args.suggestions_prefix),
                scope=scope,
                symbols=syms,
                cfg_prefix=str(args.cfg_prefix),
                freeze=1 if want_freeze else 0,
                freeze_mode=str(args.freeze_mode),
                report=report,
                ttl_sec=int(args.suggestions_ttl_sec),
            )
            r.set("meta_drift:suggestions:last_ms", str(now_ms()), ex=int(args.suggestions_ttl_sec))
            r.set("meta_drift:suggestions:last_sid", sid, ex=int(args.suggestions_ttl_sec))
        elif (want_freeze or want_unfreeze) and args.notify:
            # notify(r, "<b>META_DRIFT</b> skip=<code>cooldown</code>")
            pass
        return

    # ---- Legacy direct freeze write (discouraged) ----
    if args.write_freeze:
        try:
            mode = str(args.freeze_write_mode or "SET").upper()
            field = str(args.freeze_field or "").strip()
            if mode == "HSET" and field:
                r.hset(args.freeze_key, field, str(freeze))
            else:
                r.set(args.freeze_key, str(freeze))
        except Exception:
            pass

    if alerts and args.notify:
        import html
        if isinstance(alerts, list):
            alerts_str = html.escape(json.dumps(alerts, ensure_ascii=False), quote=True)
        else:
            alerts_str = html.escape(str(alerts), quote=True)
        txt = (
            "<b>META_MODEL DRIFT ALERT</b>\n"
            f"since_min=<code>{report['since_min']}</code> n=<code>{report['n_total']}</code> with_p=<code>{report['n_with_p']}</code>\n"
            f"p50=<code>{report['p50']:.3f}</code> miss_rate=<code>{report['miss_rate']:.3f}</code> pzero_rate=<code>{report['pzero_rate']:.3f}</code>\n"
            f"alerts=<code>{alerts_str}</code> freeze=<code>{freeze}</code>"
        )
        notify(r, txt)


if __name__ == "__main__":
    main()
