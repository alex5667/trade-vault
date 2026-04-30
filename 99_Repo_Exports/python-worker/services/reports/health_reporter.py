from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import json
import os
import time
from typing import Any, Dict, List, Tuple

import redis

from core.telegram_notify import send_telegram
from common.redis_errors import retry_redis_operation


def _now() -> int:
    return int(time.time())


def _now_ms() -> int:
    return get_ny_time_millis()


def _i(x: Any, d: int = 0) -> int:
    try:
        if x is None:
            return d
        return int(float(x))
    except Exception:
        return d


def _f(x: Any, d: float = 0.0) -> float:
    try:
        if x is None:
            return d
        return float(x)
    except Exception:
        return d


def _decode(x: Any) -> str:
    if x is None:
        return ""
    if isinstance(x, bytes):
        return x.decode("utf-8", "ignore")
    return str(x)


def _topn_pairs(pairs: List[Tuple[str, float]], n: int) -> List[Tuple[str, float]]:
    pairs.sort(key=lambda x: x[1], reverse=True)
    return pairs[:n]


def _sscan_all(r: redis.Redis, key: str, limit: int = 2000) -> List[str]:
    out: List[str] = []
    cursor = 0
    while True:
        cursor, batch = retry_redis_operation(
            lambda: r.sscan(key, cursor=cursor, count=10000)
            operation_name=f"sscan {key}"
        )
        for s in batch or []:
            sym = _decode(s)
            if sym:
                out.append(sym)
                if len(out) >= limit:
                    return sorted(set(out))
        if int(cursor) == 0:
            break
    return sorted(set(out))


def _redis() -> redis.Redis:
    url = os.getenv("REPORTS_REDIS_URL", "").strip() or os.getenv("REDIS_URL", "").strip()
    if not url:
        url = "redis://localhost:6379/0"
    return redis.Redis.from_url(url, decode_responses=False)


def _read_json(r: redis.Redis, key: str) -> Dict[str, Any]:
    raw = r.get(key)
    if not raw:
        return {}
    try:
        return json.loads(_decode(raw))
    except Exception:
        return {"raw": _decode(raw)}


def _report_atr(r: redis.Redis, top_n: int) -> Tuple[str, int]:
    top_n = int(os.getenv("ATR_REPORT_TOP_N", "15"))
    
    # ATR: stale (top by age_ms) from selector meta if available
    active_syms = _sscan_all(r, os.getenv("MICROBAR_SYMBOLS_SET", "events:microbar_closed:symbols"), limit=int(os.getenv("REPORT_MAX_SYMBOLS", "200")))
    stale_pairs: List[Tuple[str, float]] = []
    if active_syms:
        pipe = r.pipeline()
        for s in active_syms:
            pipe.get(f"cfg:atr_sel_meta:{s}")
        metas = retry_redis_operation(
            lambda: pipe.execute()
            operation_name="pipeline get atr_sel_meta"
        )
        for s, raw in zip(active_syms, metas):
            if not raw:
                continue
            try:
                d = json.loads(_decode(raw))
                age = float(d.get("age_ms", 0) or 0)
                if age > 0:
                    stale_pairs.append((s, age))
            except Exception:
                continue
    stale_pairs = _topn_pairs(stale_pairs, top_n)

    # ATR bad symbols (explicit bad key)
    atr_bad_syms = _sscan_all(r, "cfg:atr_bad:symbols", limit=500)

    # ATR jumpers (rolling window counter)
    jump_syms = _sscan_all(r, "cfg:atr_jump:symbols", limit=500)
    jump_pairs: List[Tuple[str, float]] = []
    if jump_syms:
        pipe = r.pipeline()
        for s in jump_syms:
            pipe.get(f"cfg:atr_jump_count:{s}")
        vals = retry_redis_operation(
            lambda: pipe.execute()
            operation_name="pipeline get atr_jump_count"
        )
        for s, v in zip(jump_syms, vals):
            try:
                jump_pairs.append((s, float(v or 0)))
            except Exception:
                continue
    jump_pairs = _topn_pairs(jump_pairs, top_n)

    # ATR switchers (source/tf switches in rolling window)
    sw_syms = _sscan_all(r, "cfg:atr_switch:symbols", limit=500)
    sw_pairs: List[Tuple[str, float]] = []
    if sw_syms:
        pipe = r.pipeline()
        for s in sw_syms:
            pipe.get(f"cfg:atr_switch_count:{s}")
        vals = retry_redis_operation(
            lambda: pipe.execute()
            operation_name="pipeline get atr_switch_count"
        )
        for s, v in zip(sw_syms, vals):
            try:
                sw_pairs.append((s, float(v or 0)))
            except Exception:
                continue
    sw_pairs = _topn_pairs(sw_pairs, top_n)

    # build message
    lines = []
    lines.append("ATR report")
    
    if stale_pairs:
        lines.append("ATR stale (age_ms top):")
        for s, age in stale_pairs:
            lines.append(f"- {s}: {int(age)}ms")

    if atr_bad_syms:
        lines.append("ATR bad:")
        lines.extend([f"- {s}" for s in atr_bad_syms[:top_n]])

    if jump_pairs:
        lines.append("ATR jumps (last window):")
        for s, c in jump_pairs:
            lines.append(f"- {s}: {int(c)}")

    if sw_pairs:
        lines.append("ATR switches (last window):")
        for s, c in sw_pairs:
            lines.append(f"- {s}: {int(c)}")

    if not (stale_pairs or atr_bad_syms or jump_pairs or sw_pairs):
        return "ATR: ok (no issues)", 0
    
    return "\n".join(lines), len(atr_bad_syms) + len(jump_pairs) + len(sw_pairs)


def _report_cvd(r: redis.Redis, top_n: int) -> Tuple[str, int]:
    cvd_top_n = int(os.getenv("CVD_REPORT_TOP_N", "15"))
    cvd_syms = _sscan_all(r, "cfg:cvd_quarantine:symbols", limit=500)
    cvd_items: List[Tuple[str, int, str, str]] = []  # (sym, ttl_sec, reason, mode)
    if cvd_syms:
        pipe = r.pipeline()
        for s in cvd_syms:
            pipe.get(f"cfg:cvd_quarantine_meta:{s}")
        metas = retry_redis_operation(
            lambda: pipe.execute()
            operation_name="pipeline get cvd_quarantine_meta"
        )
        now_ms = _now_ms()
        for s, raw in zip(cvd_syms, metas):
            if not raw:
                continue
            try:
                d = json.loads(_decode(raw)) if isinstance(raw, str) else json.loads(_decode(raw))
                until_ms = int(d.get("until_ms", 0) or 0)
                ttl_sec = 0
                if until_ms > now_ms:
                    ttl_sec = int((until_ms - now_ms) / 1000)
                reason = str(d.get("reason", "") or "")
                mode = str(d.get("mode", "") or "")
                cvd_items.append((s, ttl_sec, reason, mode))
            except Exception:
                continue
        cvd_items.sort(key=lambda x: x[1], reverse=True)  # longest remaining first
        cvd_items = cvd_items[:cvd_top_n]

    if not cvd_items:
        return "CVD: ok (no quarantine)", 0

    lines = ["CVD quarantine (top):"]
    for s, ttl, reason, mode in cvd_items:
        rs = reason if reason else "na"
        lines.append(f"- {s}: ttl={ttl}s mode={mode} reason={rs[:120]}")
    return "\n".join(lines), len(cvd_items)


def _report_streams(r: redis.Redis, top_n: int) -> Tuple[str, int]:
    streams_top_n = int(os.getenv("STREAMS_REPORT_TOP_N", "15"))
    legacy_key = os.getenv("MICROBAR_LEGACY_STREAM", "events:microbar_closed")
    majors_key = os.getenv("MICROBAR_MAJORS_STREAM", "events:microbar_closed:majors")
    tpl = os.getenv("MICROBAR_PER_SYMBOL_STREAM_TEMPLATE", "events:microbar_closed:{sym}")

    try:
        legacy_len = retry_redis_operation(
            lambda: r.xlen(legacy_key)
            operation_name=f"xlen {legacy_key}"
        )
    except Exception:
        legacy_len = None
    try:
        majors_len = retry_redis_operation(
            lambda: r.xlen(majors_key)
            operation_name=f"xlen {majors_key}"
        )
    except Exception:
        majors_len = None
    try:
        sym_count = retry_redis_operation(
            lambda: r.scard(os.getenv("MICROBAR_SYMBOLS_SET", "events:microbar_closed:symbols"))
            operation_name="scard symbols_set"
        )
    except Exception:
        sym_count = None

    # sample per-symbol xlen
    active_syms = _sscan_all(r, os.getenv("MICROBAR_SYMBOLS_SET", "events:microbar_closed:symbols"), limit=int(os.getenv("REPORT_MAX_SYMBOLS", "200")))
    per_pairs: List[Tuple[str, int]] = []
    if active_syms and "{sym}" in tpl:
        pipe = r.pipeline()
        keys = []
        for s in active_syms[: int(os.getenv("REPORT_MAX_SYMBOLS", "200"))]:
            k = tpl.format(sym=s)
            keys.append((s, k))
            pipe.xlen(k)
        try:
            lens = retry_redis_operation(
                lambda: pipe.execute()
                operation_name="pipeline xlen per_symbol"
            )
            for (s, _k), ln in zip(keys, lens):
                try:
                    per_pairs.append((s, int(ln or 0)))
                except Exception:
                    continue
        except Exception:
            pass

    if (legacy_len is None) and (majors_len is None) and (sym_count is None) and not per_pairs:
        return "Streams: no data", 0

    lines = ["Streams health:"]
    if sym_count is not None:
        lines.append(f"- symbols_active: {int(sym_count)}")
    if legacy_len is not None:
        lines.append(f"- legacy_xlen: {int(legacy_len)} ({legacy_key})")
    if majors_len is not None:
        lines.append(f"- majors_xlen: {int(majors_len)} ({majors_key})")
    if per_pairs:
        # show smallest (at risk) and largest (hot symbols) by XLEN
        per_pairs_sorted = sorted(per_pairs, key=lambda x: x[1])
        small = per_pairs_sorted[:streams_top_n]
        big = list(reversed(per_pairs_sorted))[:streams_top_n]
        lines.append("  per-symbol xlen (smallest):")
        for s, ln in small:
            lines.append(f"  - {s}: {ln}")
        lines.append("  per-symbol xlen (largest):")
        for s, ln in big:
            lines.append(f"  - {s}: {ln}")

    return "\n".join(lines), len(per_pairs) if per_pairs else 0


def main() -> None:
    if os.getenv("REPORTS_ENABLE", "0").strip().lower() not in {"1", "true", "yes"}:
        return

    period = int(os.getenv("REPORTS_PERIOD_SEC", "3600"))
    top_n = int(os.getenv("REPORTS_TOP_N", "10"))
    cooldown = int(os.getenv("REPORTS_SPIKE_COOLDOWN_SEC", "600"))

    r = _redis()
    last_spike_ts = 0

    while True:
        parts: List[str] = []
        atr_txt, atr_n = ("", 0)
        cvd_txt, cvd_n = ("", 0)

        if os.getenv("REPORT_ATR_ENABLE", "1").strip().lower() in {"1", "true", "yes"}:
            atr_txt, atr_n = _report_atr(r, top_n)
            if atr_txt:
                parts.append(atr_txt)

        if os.getenv("REPORT_CVD_ENABLE", "1").strip().lower() in {"1", "true", "yes"}:
            cvd_txt, cvd_n = _report_cvd(r, top_n)
            parts.append(cvd_txt)

        streams_txt, streams_n = _report_streams(r, top_n)
        if streams_txt:
            parts.append(streams_txt)

        msg = "\n\n".join([p for p in parts if p])
        if msg:
            send_telegram(msg)

        # spike alert: if many quarantined or many atr_bad, send again with cooldown
        spike = (atr_n >= int(os.getenv("REPORT_SPIKE_ATR_BAD", "20"))) or (cvd_n >= int(os.getenv("REPORT_SPIKE_CVD_Q", "20")))
        now = _now()
        if spike and (now - last_spike_ts) >= cooldown:
            send_telegram("SPIKE ALERT\n\n" + msg)
            last_spike_ts = now

        time.sleep(max(5, period))


if __name__ == "__main__":
    main()

