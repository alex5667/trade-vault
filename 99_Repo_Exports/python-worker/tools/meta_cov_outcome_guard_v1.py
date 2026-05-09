#!/usr/bin/env python3
from __future__ import annotations

from domain.evidence_keys import MetaKeys
from core.redis_keys import RedisStreams as RS

"""
meta_cov_outcome_guard_v1.py

P32: Outcome guardrails for meta ENFORCE by feature-coverage buckets.

Goal
----
When meta-model enforcement is enabled per coverage bucket (P30), provide a conservative
safety loop that can *downgrade* per-bucket enforce shares if recent outcomes deteriorate.

This tool:
- Reads recent POSITION_CLOSED trade events from TRADE_EVENTS_STREAM (default: events:trades)
- Groups them by meta_enforce_cov_bucket (a/b/c/d)
- Simulates policy expectancy under current per-bucket shares (cfg2 meta_enforce_share_cov_*)
  and a baseline share=0 (no enforce) for delta comparison
- On severe deterioration, emits:
    - alert (exit code 2)
    - optional cfg:suggestions proposal to downgrade share(s)
    - optional direct apply to cfg2

Exit codes
----------
0 : OK / no severe deterioration
2 : Guardrail triggered (alert)
1 : Error

Assumptions
-----------
- Closed trades have event == POSITION_CLOSED and include r_mult (R units)
- Meta enforce fields are expanded at root level by TradeEventsLogger, including:
  meta_enforce_key, meta_veto, meta_enforce_bucket_type, meta_enforce_cov_bucket
- cfg2 hash exists (DYN_CFG_KEY, default settings:dynamic_cfg)

cfg2 fields read
---------------
- meta_enforce_per_cov (0/1)
- meta_enforce_share_cov_a / _b / _c / _d (0..1 floats)
- meta_enforce_salt (string)

cfg2 fields optionally written (audit / anti-flap)
-------------------------------------------------
- meta_cov_outcome_guard_last_ts_ms
- meta_cov_outcome_guard_last_report (json)
- meta_cov_outcome_guard_last_change_ms

Proposal path (optional)
------------------------
Emits a cfg:suggestions proposal (default prefix cfg:suggestions:entry_policy for compatibility
with the existing ApplyRunner). Override via META_COV_OUTCOME_SUGGESTIONS_PREFIX.
"""

import argparse
import hashlib
import html
import json
import os
import secrets
from typing import Any

from utils.time_utils import get_ny_time_millis
import contextlib

try:
    import redis  # type: ignore
except Exception:  # pragma: no cover
    redis = None  # type: ignore


def now_ms() -> int:
    return get_ny_time_millis()


def _b2s(x: Any) -> str:
    if x is None:
        return ""
    if isinstance(x, (bytes, bytearray)):
        return x.decode("utf-8", "replace")
    return str(x)


def _loads_maybe_json(v: Any) -> Any:
    if v is None:
        return None
    if isinstance(v, (bytes, bytearray)):
        try:
            v = v.decode("utf-8", "replace")
        except Exception:
            return None
    if isinstance(v, str):
        s = v.strip()
        if not s:
            return None
        if (s.startswith("{") and s.endswith("}")) or (s.startswith("[") and s.endswith("]")):
            try:
                return json.loads(s)
            except Exception:
                return v
        return v
    return v


def _parse_entry(fields: dict[Any, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    payload_obj: dict[str, Any] | None = None
    for k, v in fields.items():
        ks = _b2s(k)
        out[ks] = _loads_maybe_json(v)

    # Some writers nest under 'payload' or 'json'
    if isinstance(out.get("payload"), dict):
        payload_obj = out.get("payload")  # type: ignore[assignment]
    elif isinstance(out.get("json"), dict):
        payload_obj = out.get("json")  # type: ignore[assignment]

    if payload_obj:
        merged = dict(out)
        merged.update(payload_obj)
        out = merged

    # ── Fallback: extract meta_enforce fields from signal_payload ──────────
    # Confirmed path (via Redis inspection):
    #   signal_payload → indicators → of_confirm → evidence → meta_enforce_bucket
    # and also: indicators → of_confirm_v3 → evidence → meta_enforce_bucket
    try:
        if not str(out.get("meta_enforce_bucket") or out.get(MetaKeys.ENFORCE_COV_BUCKET) or "").strip():
            sp = out.get("signal_payload") or {}
            if isinstance(sp, str):
                try:
                    import json as _j
                    sp = _j.loads(sp)
                except Exception:
                    sp = {}
            if isinstance(sp, dict):
                # Navigate: sp.indicators.{of_confirm|of_confirm_v3}.evidence
                ind = sp.get("indicators") or {}
                if isinstance(ind, str):
                    try:
                        import json as _j2
                        ind = _j2.loads(ind)
                    except Exception:
                        ind = {}

                evidence: dict[str, Any] = {}
                for _oc_key in ("of_confirm", "of_confirm_v3", "of_confirm_v2", "of"):
                    _oc = ind.get(_oc_key) if isinstance(ind, dict) else None
                    if isinstance(_oc, dict):
                        _ev = _oc.get("evidence") or {}
                        if isinstance(_ev, dict) and _ev:
                            evidence = _ev
                            break

                bkt = str(
                    evidence.get("meta_enforce_bucket")
                    or evidence.get(MetaKeys.ENFORCE_COV_BUCKET)
                    or ""
                ).strip().lower()
                if bkt:
                    out[MetaKeys.ENFORCE_COV_BUCKET] = bkt
                    if not (out.get("meta_enforce_bucket") or "").strip():
                        out["meta_enforce_bucket"] = bkt

                for _fld in ("meta_enforce_key", "meta_enforce_salt", "meta_veto", "meta_enforce_applied"):
                    if not (out.get(_fld) or "").strip():
                        _val = evidence.get(_fld)
                        if _val is not None:
                            out[_fld] = _val
    except Exception:
        pass  # fail-open: guard degradation better than crash

    return out


def _i(x: Any, default: int = 0) -> int:
    try:
        return int(float(x))
    except Exception:
        return default


def _f(x: Any, default: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return default


def _event_ts_ms(d: dict[str, Any]) -> int:
    for k in ("exit_ts_ms", "ts_ms", "event_ts_ms", "ts"):
        v = d.get(k)
        if v is None:
            continue
        t = _i(v, 0)
        if t > 0:
            return t
    return 0


def _is_position_closed(d: dict[str, Any]) -> bool:
    ev = str(d.get("event") or d.get("type") or "").upper()
    if ev == "POSITION_CLOSED":
        return True
    st = (d.get("status") or "").upper()
    # trades:closed stream uses status="closed" (maps to CLOSED after .upper())
    # events:trades uses status="POSITION_CLOSED"
    return st in ("POSITION_CLOSED", "CLOSED")


def _sha_to_unit_interval(salt: str, key: str) -> float:
    h = hashlib.sha256(f"{salt}:{key}".encode("utf-8", "ignore")).digest()
    x = int.from_bytes(h[:8], byteorder="big", signed=False)
    return x / float(2**64)


def _pctl(xs: list[float], q: float) -> float:
    if not xs:
        return 0.0
    if q <= 0:
        return min(xs)
    if q >= 1:
        return max(xs)
    xs2 = sorted(xs)
    n = len(xs2)
    pos = (n - 1) * q
    lo = int(pos)
    hi = min(n - 1, lo + 1)
    frac = pos - lo
    return xs2[lo] * (1.0 - frac) + xs2[hi] * frac


def _summary_stats(xs: list[float]) -> dict[str, float]:
    if not xs:
        return {
            "n": 0.0,
            "meanR": 0.0,
            "p05": 0.0,
            "p50": 0.0,
            "p95": 0.0,
            "win_rate": 0.0,
            "tail_rate_le_neg1R": 0.0,
        }
    n = len(xs)
    meanR = sum(xs) / float(n)
    win_rate = sum(1 for v in xs if v > 0) / float(n)
    tail_rate = sum(1 for v in xs if v <= -1.0) / float(n)
    return {
        "n": float(n),
        "meanR": float(meanR),
        "p05": float(_pctl(xs, 0.05)),
        "p50": float(_pctl(xs, 0.50)),
        "p95": float(_pctl(xs, 0.95)),
        "win_rate": float(win_rate),
        "tail_rate_le_neg1R": float(tail_rate),
    }


def _simulate_share(rows: list[dict[str, Any]], share: float, salt: str) -> dict[str, Any]:
    """Simulate applying ENFORCE at a given share.

    For each opportunity (row):
      apply = U(salt:key) < share
      if apply and meta_veto==1 -> 'blocked' => outcome=0 (no trade)
      else outcome=r_mult

    'opp' stats include zeros for blocked opportunities (expected mean under policy).
    'exec' stats include only executed trades (diagnostics).
    """
    share = max(0.0, min(1.0, float(share)))
    opp: list[float] = []
    execs: list[float] = []
    used = 0
    blocked = 0

    for d in rows:
        key = (d.get(MetaKeys.ENFORCE_KEY) or "")
        if not key:
            continue
        used += 1
        u = _sha_to_unit_interval(salt, key)
        apply = u < share
        veto = 1 if _i(d.get(MetaKeys.VETO), 0) != 0 else 0
        r_mult = _f(d.get("r_mult"), 0.0)

        if apply and veto == 1:
            blocked += 1
            opp.append(0.0)
            continue

        opp.append(float(r_mult))
        execs.append(float(r_mult))

    return {
        "used": int(used),
        "blocked": int(blocked),
        "exec_rate": float((used - blocked) / float(used)) if used > 0 else 0.0,
        "opp": _summary_stats(opp),
        "exec": _summary_stats(execs),
    }


def _read_recent_closed_trades(
    r: Any,
    *,
    stream: str,
    since_ms: int,
    max_scan: int,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
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

            d = _parse_entry(fields)
            if not _is_position_closed(d):
                continue

            ts = _event_ts_ms(d)
            if ts and ts < since_ms:
                return rows

            rows.append(d)
            if scanned >= max_scan:
                break

    return rows


def _load_cfg2(r: Any, key: str) -> dict[str, Any]:
    raw = r.hgetall(key) or {}
    out: dict[str, Any] = {}
    for k, v in raw.items():
        out[_b2s(k)] = _loads_maybe_json(v)
    return out


def _write_cfg2(r: Any, key: str, patch: dict[str, Any]) -> None:
    m: dict[str, str] = {}
    for k, v in patch.items():
        if isinstance(v, (dict, list)):
            m[k] = json.dumps(v, ensure_ascii=False, separators=(",", ":"))
        else:
            m[k] = str(v)
    r.hset(key, mapping=m)


def _notify(r: Any, text: str, sid: str | None = None) -> None:
    stream = os.getenv("NOTIFY_TELEGRAM_STREAM", RS.NOTIFY_TELEGRAM)
    payload: dict[str, str] = {"type": "report", "text": text, "ts": str(now_ms())}
    if sid:
        payload["sid"] = sid
    with contextlib.suppress(Exception):
        r.xadd(stream, payload, maxlen=200000, approximate=True)


def _emit_cfg_suggestion(
    r: Any,
    *,
    prefix: str,
    kind: str,
    scope: str,
    cfg2_key: str,
    patch: dict[str, Any],
    report: dict[str, Any],
    ttl_sec: int,
    min_approvals: int,
    auto_approve: bool,
) -> str:
    ttl_sec = int(ttl_sec or 86400)
    min_approvals = int(min_approvals or 1)

    sid = f"{kind}:{now_ms()}:{secrets.token_hex(4)}"
    meta_key = f"{prefix}:meta:{sid}"
    appr_key = f"{prefix}:approvals:{sid}"
    latest_key = f"{prefix}:latest:{kind}:{scope}"

    ops: list[dict[str, str]] = []
    for k, v in patch.items():
        if isinstance(v, (dict, list)):
            vv = json.dumps(v, ensure_ascii=False, separators=(",", ":"))
        else:
            vv = str(v)
        ops.append({"op": "HSET", "key": cfg2_key, "field": str(k), "value": vv})

    meta = {
        "sid": sid,
        "created_ms": now_ms(),
        "ttl_sec": ttl_sec,
        "who": "meta_cov_outcome_guard_v1",
        "kind": kind,
        "scope": scope,
        "min_approvals": min_approvals,
        "ops": ops,
        "report": report,
        "status": "approved" if auto_approve else "pending",
    }

    r.set(meta_key, json.dumps(meta, ensure_ascii=False, separators=(",", ":")), ex=ttl_sec)

    try:
        base_appr = {
            "created_ms": str(meta["created_ms"]),
            "kind": kind,
            "status": "approved" if auto_approve else "pending",
        }
        if auto_approve:
            base_appr["auto"] = "1"
        r.hset(appr_key, mapping=base_appr)
        r.expire(appr_key, ttl_sec)
    except Exception:
        pass

    r.set(latest_key, sid, ex=ttl_sec)

    alerts = report.get("alerts") or []
    _notify(
        r,
        "<b>META_COV_OUTCOME proposal</b>\n"
        f"kind=<code>{html.escape(kind, quote=True)}</code> scope=<code>{html.escape(scope, quote=True)}</code>\n"
        f"sid=<code>{html.escape(sid, quote=True)}</code>\n"
        f"alerts=<code>{html.escape(json.dumps(alerts, ensure_ascii=False), quote=True)}</code>",
        sid=sid,
    )
    return sid


def main() -> int:
    ap = argparse.ArgumentParser()

    ap.add_argument("--redis-url", default=os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0"))
    ap.add_argument("--cfg2-key", default=os.getenv("DYN_CFG_KEY", "settings:dynamic_cfg"))
    ap.add_argument("--trade-stream", default=os.getenv("TRADE_EVENTS_STREAM", RS.EVENTS_TRADES))

    ap.add_argument("--lookback-hours", type=float, default=float(os.getenv("META_COV_OUTCOME_LOOKBACK_H", "24") or 24))
    ap.add_argument("--max-scan", type=int, default=int(os.getenv("META_COV_OUTCOME_MAX_SCAN", "200000") or 200000))
    ap.add_argument("--min-n", type=int, default=int(os.getenv("META_COV_OUTCOME_MIN_N", "80") or 80))

    # Guard thresholds (severe-by-default)
    ap.add_argument("--meanr-min", type=float, default=float(os.getenv("META_COV_OUTCOME_MEANR_MIN", "-0.05") or -0.05))
    ap.add_argument("--delta-meanr-min", type=float, default=float(os.getenv("META_COV_OUTCOME_DELTA_MIN", "-0.05") or -0.05))
    ap.add_argument("--p05-min", type=float, default=float(os.getenv("META_COV_OUTCOME_P05_MIN", "-1.00") or -1.00))
    ap.add_argument("--tail-max", type=float, default=float(os.getenv("META_COV_OUTCOME_TAIL_MAX", "0.35") or 0.35))

    # Actions / anti-flap
    ap.add_argument("--step-down", type=float, default=float(os.getenv("META_COV_OUTCOME_STEP_DOWN", "0.25") or 0.25))
    ap.add_argument("--min-hold-sec", type=int, default=int(os.getenv("META_COV_OUTCOME_MIN_HOLD_SEC", "1800") or 1800))
    ap.add_argument("--force", type=int, default=0)

    # Proposal / apply knobs
    ap.add_argument("--emit-suggestion", type=int, default=int(os.getenv("META_COV_OUTCOME_EMIT_SUGGESTION", "0") or 0))
    ap.add_argument("--direct-apply", type=int, default=int(os.getenv("META_COV_OUTCOME_DIRECT_APPLY", "0") or 0))
    ap.add_argument("--write-audit", type=int, default=int(os.getenv("META_COV_OUTCOME_WRITE_AUDIT", "1") or 1))

    ap.add_argument("--suggestion-prefix", default=os.getenv("META_COV_OUTCOME_SUGGESTIONS_PREFIX", "cfg:suggestions:entry_policy"))
    ap.add_argument("--suggestion-kind", default=os.getenv("META_COV_OUTCOME_SUGGESTIONS_KIND", "meta_cov_outcome_guard"))
    ap.add_argument("--suggestion-scope", default=os.getenv("META_COV_OUTCOME_SUGGESTIONS_SCOPE", "ALL"))
    ap.add_argument("--ttl-sec", type=int, default=int(os.getenv("META_COV_OUTCOME_SUGGESTIONS_TTL_SEC", "86400") or 86400))
    ap.add_argument("--min-approvals", type=int, default=int(os.getenv("META_COV_OUTCOME_MIN_APPROVALS", "1") or 1))
    ap.add_argument("--auto-approve", type=int, default=int(os.getenv("META_COV_OUTCOME_AUTO_APPROVE", "0") or 0))

    args = ap.parse_args()

    if redis is None:
        print(json.dumps({"ok": False, "reason": "redis_python_not_installed"}, ensure_ascii=False))
        return 1

    r = redis.Redis.from_url(args.redis_url, decode_responses=False)

    cfg2 = _load_cfg2(r, args.cfg2_key)
    per_cov = 1 if _i(cfg2.get("meta_enforce_per_cov"), 0) != 0 else 0
    salt = (cfg2.get(MetaKeys.ENFORCE_SALT) or "")

    global_share = _f(cfg2.get(MetaKeys.ENFORCE_SHARE), 1.0)
    shares = {
        "trend": max(0.0, min(1.0, _f(cfg2.get("meta_enforce_share_trend"), global_share))),
        "range": max(0.0, min(1.0, _f(cfg2.get("meta_enforce_share_range"), global_share))),
        "other": max(0.0, min(1.0, _f(cfg2.get("meta_enforce_share_other"), global_share))),
    }

    since_ms = now_ms() - int(float(args.lookback_hours) * 3600.0 * 1000.0)
    rows = _read_recent_closed_trades(r, stream=args.trade_stream, since_ms=since_ms, max_scan=int(args.max_scan))

    # Group by coverage bucket (only bucket_type=cov when present)
    by_bucket: dict[str, list[dict[str, Any]]] = {"trend": [], "range": [], "other": []}
    for d in rows:
        b = str(d.get("meta_enforce_bucket") or d.get(MetaKeys.ENFORCE_COV_BUCKET) or d.get("meta_cov_bucket") or "").strip().lower()
        if b in by_bucket:
            by_bucket[b].append(d)

    bucket_reports: dict[str, Any] = {}
    alerts: list[str] = []
    patch: dict[str, Any] = {}

    sim_salt = salt if salt else "nosalt"

    for b in ("trend", "range", "other"):
        rows_b = by_bucket.get(b) or []
        rep_cur = _simulate_share(rows_b, shares[b], sim_salt)
        rep_0 = _simulate_share(rows_b, 0.0, sim_salt)

        used = int(rep_cur["used"])
        mean_cur = float(rep_cur["opp"]["meanR"])
        mean_0 = float(rep_0["opp"]["meanR"])
        delta = mean_cur - mean_0
        p05 = float(rep_cur["opp"]["p05"])
        tail = float(rep_cur["opp"]["tail_rate_le_neg1R"])

        bucket_reports[b] = {
            "share": float(shares[b]),
            "n_rows": int(len(rows_b)),
            "used": used,
            "rep_cur": rep_cur,
            "rep_share0": rep_0,
            "delta_meanR": float(delta),
        }

        if shares[b] <= 0.0 or used < int(args.min_n):
            continue

        severe = (
            (mean_cur < float(args.meanr_min))
            or (delta < float(args.delta_meanr_min))
            or (p05 < float(args.p05_min))
            or (tail > float(args.tail_max))
        )

        if severe:
            new_share = max(0.0, min(shares[b], float(shares[b]) - float(args.step_down)))
            field = f"meta_enforce_share_{b}"
            if new_share < float(shares[b]) - 1e-9:
                patch[field] = float(new_share)
                alerts.append(
                    f"meta_cov_outcome:{b}:severe mean={mean_cur:.3f} delta={delta:.3f} "
                    f"p05={p05:.3f} tail={tail:.2f} used={used} share={shares[b]:.2f}->{new_share:.2f}"
                )

    last_change_ms = _i(cfg2.get("meta_cov_outcome_guard_last_change_ms"), 0)
    too_soon = (now_ms() - last_change_ms) < int(args.min_hold_sec) * 1000

    report: dict[str, Any] = {
        "ok": True,
        "ts_ms": now_ms(),
        "per_cov": int(per_cov),
        "trade_stream": str(args.trade_stream),
        "lookback_hours": float(args.lookback_hours),
        "since_ms": int(since_ms),
        "n_closed_trades": int(len(rows)),
        "shares": shares,
        "thresholds": {
            "min_n": int(args.min_n),
            "meanr_min": float(args.meanr_min),
            "delta_meanr_min": float(args.delta_meanr_min),
            "p05_min": float(args.p05_min),
            "tail_max": float(args.tail_max),
            "step_down": float(args.step_down),
            "min_hold_sec": int(args.min_hold_sec),
        },
        "alerts": alerts,
        "bucket_reports": bucket_reports,
        "patch": patch,
        "too_soon": bool(too_soon),
        "last_change_ms": int(last_change_ms),
        "emit_suggestion": bool(int(args.emit_suggestion) == 1),
        "direct_apply": bool(int(args.direct_apply) == 1),
    }

    print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=False))

    triggered = len(alerts) > 0 and len(patch) > 0

    if int(args.write_audit) == 1:
        with contextlib.suppress(Exception):
            _write_cfg2(
                r,
                args.cfg2_key,
                {
                    "meta_cov_outcome_guard_last_ts_ms": now_ms(),
                    "meta_cov_outcome_guard_last_report": report,
                },
            )

    if not triggered:
        return 0

    if per_cov == 0:
        return 2

    if too_soon and int(args.force) != 1:
        return 2

    patch_with_meta = dict(patch)
    patch_with_meta["meta_cov_outcome_guard_last_change_ms"] = now_ms()

    sid = ""
    if int(args.direct_apply) == 1:
        try:
            _write_cfg2(r, args.cfg2_key, patch_with_meta)
        except Exception:
            return 1

    if int(args.emit_suggestion) == 1:
        try:
            sid = _emit_cfg_suggestion(
                r,
                prefix=str(args.suggestion_prefix),
                kind=str(args.suggestion_kind),
                scope=str(args.suggestion_scope).strip().upper(),
                cfg2_key=str(args.cfg2_key),
                patch=patch_with_meta,
                report=report,
                ttl_sec=int(args.ttl_sec),
                min_approvals=int(args.min_approvals),
                auto_approve=bool(int(args.auto_approve) == 1),
            )
        except Exception:
            sid = ""

    note = f"sid={sid}" if sid else "sid=none"
    _notify(
        r,
        "<b>META_COV_OUTCOME alert</b>\n"
        + f"alerts=<code>{html.escape(json.dumps(alerts, ensure_ascii=False), quote=True)}</code>\n"
        + f"{html.escape(note, quote=True)}",
        sid=sid or None,
    )
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
