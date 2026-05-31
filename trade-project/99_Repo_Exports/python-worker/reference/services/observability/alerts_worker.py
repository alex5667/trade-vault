from __future__ import annotations

import os
import time

import redis

try:
    from core.telegram_notify import send_telegram
except Exception:
    send_telegram = None  # type: ignore


def _b2s(x) -> str:
    return x.decode("utf-8") if isinstance(x, (bytes, bytearray)) else str(x)


def _read_set(r: redis.Redis, key: str, max_n: int = 50) -> list[str]:
    xs = list(r.smembers(key) or [])
    out: list[str] = []
    for x in xs[:max_n]:
        out.append(_b2s(x))
    return out


def _cooldown_ok(r: redis.Redis, key: str, cooldown_sec: int) -> bool:
    if r.get(key):
        return False
    r.set(key, "1", ex=cooldown_sec)
    return True


def main() -> None:
    if os.getenv("ALERTS_ENABLE", "0") != "1":
        return

    redis_url = os.getenv("REPORTS_REDIS_URL") or os.getenv("REDIS_URL") or "redis://localhost:6379/0"
    r = redis.Redis.from_url(redis_url, decode_responses=False)

    period = int(os.getenv("ALERTS_PERIOD_SEC", "60"))
    cooldown = int(os.getenv("ALERTS_COOLDOWN_SEC", "900"))

    atr_bad_pct = float(os.getenv("ALERT_ATR_BAD_PCT", "0.30"))
    cvd_q_pct = float(os.getenv("ALERT_CVD_QUARANTINE_PCT", "0.20"))
    xlen_legacy_max = int(os.getenv("ALERT_MICROBAR_XLEN_LEGACY_MAX", "200000"))
    xlen_sym_max = int(os.getenv("ALERT_MICROBAR_XLEN_SYMBOL_MAX", "80000"))
    redis_mem_mb_max = float(os.getenv("ALERT_REDIS_USED_MEMORY_MB_MAX", "6500"))
    lcb_winner_changes_1h_max = int(os.getenv("ALERT_LCB_WINNER_CHANGES_1H_MAX", "6"))
    ml_missing_critical_5m_max = int(os.getenv("ALERT_ML_MISSING_CRITICAL_5M_MAX", "0"))

    while True:
        try:
            universe = int(r.scard("events:microbar_closed:symbols") or 0)
            universe = max(1, universe)

            atr_bad_n = int(r.scard("cfg:atr_bad:symbols") or 0)
            cvd_q_n = int(r.scard("cfg:cvd_quarantine:symbols") or 0)

            info = r.info()
            used_mb = float(info.get("used_memory", 0)) / (1024.0 * 1024.0)

            legacy_key = os.getenv("MICROBAR_LEGACY_STREAM", "events:microbar_closed")
            legacy_xlen = int(r.xlen(legacy_key) or 0)

            sample_syms = _read_set(r, "events:microbar_closed:symbols", max_n=50)
            worst_sym = None
            worst_xlen = 0
            prefix = os.getenv("MICROBAR_PER_SYMBOL_PREFIX", "events:microbar_closed:")
            for sym in sample_syms:
                xlen = int(r.xlen(f"{prefix}{sym}") or 0)
                if xlen > worst_xlen:
                    worst_xlen = xlen
                    worst_sym = sym

            msgs: list[str] = []
            if used_mb > redis_mem_mb_max and _cooldown_ok(r, "alerts:cooldown:redis_mem", cooldown):
                msgs.append(f"REDIS used_memory_mb={used_mb:.0f} > {redis_mem_mb_max:.0f}")

            if legacy_xlen > xlen_legacy_max and _cooldown_ok(r, "alerts:cooldown:legacy_xlen", cooldown):
                msgs.append(f"microbar legacy XLEN={legacy_xlen} > {xlen_legacy_max}")

            if worst_xlen > xlen_sym_max and _cooldown_ok(r, "alerts:cooldown:sym_xlen", cooldown):
                msgs.append(f"microbar {worst_sym} XLEN={worst_xlen} > {xlen_sym_max} (sample)")

            if (atr_bad_n / float(universe)) >= atr_bad_pct and _cooldown_ok(r, "alerts:cooldown:atr_bad", cooldown):
                top = _read_set(r, "cfg:atr_bad:symbols", max_n=20)
                msgs.append(
                    f"ATR bad pct={atr_bad_n}/{universe}={atr_bad_n/universe:.2%} >= {atr_bad_pct:.0%}. Top: {', '.join(top)}"
                )

            if (cvd_q_n / float(universe)) >= cvd_q_pct and _cooldown_ok(r, "alerts:cooldown:cvd_q", cooldown):
                top = _read_set(r, "cfg:cvd_quarantine:symbols", max_n=20)
                msgs.append(
                    f"CVD quarantine pct={cvd_q_n}/{universe}={cvd_q_n/universe:.2%} >= {cvd_q_pct:.0%}. Top: {', '.join(top)}"
                )

            if msgs and send_telegram:
                send_telegram("\n".join(["🚨 SRE Alerts"] + msgs))
        except Exception:
            pass

        time.sleep(float(period))


if __name__ == "__main__":
    main()

