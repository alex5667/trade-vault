from __future__ import annotations

"""liqmap_gate_calibrator_v1.py

Авто-калибратор LiqMap Gate — persistent service.

Проблема: shadow + нет калибратора → нет данных о качестве порогов.
Решение: каждые INTERVAL_SEC читает decisions:final + trades:closed,
считает quality metrics, ведёт state machine, при квалификации
АВТОМАТИЧЕСКИ пишет liqmap_gate_mode=enforce в Redis и шлёт
Telegram-уведомление с кнопкой «↩ Rollback → SHADOW».

State machine:
  WARMUP → DATA_COLLECTED → QUALIFIED → ENFORCE_ACTIVE

Quality criteria (все должны быть выполнены):
  shadow_hours    >= LIQMAP_CAL_MIN_SHADOW_HOURS  (default 48)
  veto_n          >= LIQMAP_CAL_MIN_VETO_N        (default 10)
  veto_precision  >= LIQMAP_CAL_MIN_PRECISION     (default 0.55)
  r_delta         >= LIQMAP_CAL_MIN_R_DELTA       (default 0.25)

ENV:
  LIQMAP_CAL_REDIS_URL          redis://redis-worker-1:6379/0
  LIQMAP_CAL_REDIS_MAIN_URL     redis://redis:6379/0
  LIQMAP_CAL_SINCE_HOURS        168 (7d)
  LIQMAP_CAL_INTERVAL_SEC       3600
  LIQMAP_CAL_MIN_SHADOW_HOURS   48
  LIQMAP_CAL_MIN_VETO_N         10
  LIQMAP_CAL_MIN_PRECISION      0.55
  LIQMAP_CAL_MIN_R_DELTA        0.25
  LIQMAP_CAL_COOLDOWN_SEC       86400
  LIQMAP_CAL_STATE_PATH         /var/lib/trade/of_reports/liqmap_cal_state.json
  LIQMAP_CAL_APPLY              1 (0 = dry-run, не применять)
  LIQMAP_CAL_ENABLE             1
  NOTIFY_STREAM                 notify:telegram
  RECS_HMAC_SECRET              CHANGE_ME  (для rollback bundle)
"""

import collections
import hashlib
import hmac
import json
import logging
import math
import os
import statistics
import sys
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import redis

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [liqmap-cal] %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# ENV helpers
# ---------------------------------------------------------------------------

def _env(k: str, d: str = "") -> str:
    return os.environ.get(k, d)

def _env_int(k: str, d: int) -> int:
    try:
        return int(_env(k, str(d)))
    except Exception:
        return d

def _env_float(k: str, d: float) -> float:
    try:
        return float(_env(k, str(d)))
    except Exception:
        return d


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

@dataclass
class CalConfig:
    redis_url: str       = ""
    redis_main_url: str  = ""
    since_hours: float   = 168.0
    interval_sec: int    = 3600
    min_shadow_hours: float = 48.0
    min_veto_n: int      = 10
    min_precision: float = 0.55
    min_r_delta: float   = 0.25
    cooldown_sec: int    = 86400
    state_path: str      = "/var/lib/trade/of_reports/liqmap_cal_state.json"
    apply: bool          = True
    enable: bool         = True
    notify_stream: str   = "notify:telegram"
    recs_secret: str     = "CHANGE_ME"


def load_config() -> CalConfig:
    return CalConfig(
        redis_url       = _env("LIQMAP_CAL_REDIS_URL", "redis://redis-worker-1:6379/0"),
        redis_main_url  = _env("LIQMAP_CAL_REDIS_MAIN_URL", "redis://redis:6379/0"),
        since_hours     = _env_float("LIQMAP_CAL_SINCE_HOURS", 168.0),
        interval_sec    = _env_int("LIQMAP_CAL_INTERVAL_SEC", 3600),
        min_shadow_hours= _env_float("LIQMAP_CAL_MIN_SHADOW_HOURS", 48.0),
        min_veto_n      = _env_int("LIQMAP_CAL_MIN_VETO_N", 10),
        min_precision   = _env_float("LIQMAP_CAL_MIN_PRECISION", 0.55),
        min_r_delta     = _env_float("LIQMAP_CAL_MIN_R_DELTA", 0.25),
        cooldown_sec    = _env_int("LIQMAP_CAL_COOLDOWN_SEC", 86400),
        state_path      = _env("LIQMAP_CAL_STATE_PATH", "/var/lib/trade/of_reports/liqmap_cal_state.json"),
        apply           = bool(_env_int("LIQMAP_CAL_APPLY", 1)),
        enable          = bool(_env_int("LIQMAP_CAL_ENABLE", 1)),
        notify_stream   = _env("NOTIFY_STREAM", "notify:telegram"),
        recs_secret     = _env("RECS_HMAC_SECRET", "CHANGE_ME"),
    )


# ---------------------------------------------------------------------------
# State file
# ---------------------------------------------------------------------------

STATES = ("WARMUP", "DATA_COLLECTED", "QUALIFIED", "ENFORCE_PROPOSED", "ENFORCE_ACTIVE")


def _load_state(path: str) -> dict[str, Any]:
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def _save_state(path: str, state: dict[str, Any]) -> None:
    try:
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(state, f, indent=2)
        os.replace(tmp, path)
    except Exception as e:
        log.warning(f"state save failed: {e}")


# ---------------------------------------------------------------------------
# Safe helpers
# ---------------------------------------------------------------------------

def _f(v: Any, d: float = 0.0) -> float:
    try:
        x = float(v)
        return x if math.isfinite(x) else d
    except Exception:
        return d

def _i(v: Any, d: int = 0) -> int:
    try:
        return int(v)
    except Exception:
        try:
            return int(float(v))
        except Exception:
            return d

def _now_ms() -> int:
    return int(time.time() * 1000)

def _mean(vals: list[float]) -> float:
    return statistics.mean(vals) if vals else 0.0

def _median(vals: list[float]) -> float:
    return statistics.median(vals) if vals else 0.0


# ---------------------------------------------------------------------------
# Redis readers
# ---------------------------------------------------------------------------

def _read_decisions(
    r: redis.Redis,
    since_hours: float,
    batch: int = 2000,
) -> dict[str, dict[str, Any]]:
    """Read decisions:final → liqmap gate shadow decisions."""
    stream = "decisions:final"
    since_ms = _now_ms() - int(since_hours * 3_600_000)
    cur = f"{since_ms}-0"
    decisions: dict[str, dict[str, Any]] = {}
    first = True

    while True:
        min_id = cur if first else f"({cur}"
        first = False
        try:
            rows: list = r.xrange(stream, min=min_id, max="+", count=batch)  # type: ignore[assignment]
        except Exception as e:
            log.warning(f"xrange {stream}: {e}")
            break
        if not rows:
            break

        for sid_stream, fields in rows:
            cur = str(sid_stream)
            raw = fields.get("payload") or ""
            if not raw:
                continue
            try:
                rec = json.loads(raw)
            except Exception:
                continue
            if not isinstance(rec, dict):
                continue

            lm = rec.get("liqmap")
            if not isinstance(lm, dict):
                continue
            gate = lm.get("gate")
            if not isinstance(gate, dict):
                continue

            mode = (gate.get("mode") or "").lower()
            if mode in ("", "off"):
                continue

            sid = (rec.get("sid") or "").strip()
            if not sid:
                continue

            decisions[sid] = {
                "shadow_veto": _i(gate.get("shadow_veto"), 0),
                "veto":        _i(gate.get("veto"), 0),
                "rr":          _f(gate.get("rr"), 0.0),
                "risk_bps":    _f(gate.get("risk_bps"), 0.0),
                "reward_bps":  _f(gate.get("reward_bps"), 0.0),
                "reason":      (gate.get("reason") or "ok"),
                "mode":        mode,
                "symbol":      (rec.get("symbol") or "").upper(),
                "direction":   (rec.get("direction") or "").upper(),
                "ts_ms":       _i(rec.get("ts_ms"), 0),
            }

    log.info(f"decisions loaded: {len(decisions)} (shadow-active)")
    return decisions


def _read_trades(
    r: redis.Redis,
    since_hours: float,
    batch: int = 2000,
) -> dict[str, dict[str, Any]]:
    """Read trades:closed stream → r_mult by sid."""
    stream = "trades:closed"
    since_ms = _now_ms() - int(since_hours * 3_600_000)
    cur = f"{since_ms}-0"
    trades: dict[str, dict[str, Any]] = {}
    first = True

    while True:
        min_id = cur if first else f"({cur}"
        first = False
        try:
            rows: list = r.xrange(stream, min=min_id, max="+", count=batch)  # type: ignore[assignment]
        except Exception as e:
            log.warning(f"xrange {stream}: {e}")
            break
        if not rows:
            break

        for sid_stream, fields in rows:
            cur = str(sid_stream)
            # trades:closed может иметь payload или flat fields
            raw = fields.get("payload")
            if raw:
                try:
                    rec = json.loads(raw)
                except Exception:
                    rec = None
            else:
                rec = fields

            if not isinstance(rec, dict):
                continue

            sid = (rec.get("sid") or "").strip()
            if not sid:
                continue

            r_mult = _f(rec.get("r_mult"), None)  # type: ignore[arg-type]
            if r_mult is None or not math.isfinite(r_mult):
                continue

            trades[sid] = {
                "r_mult":    r_mult,
                "symbol":    (rec.get("symbol") or "").upper(),
                "direction": str(rec.get("direction") or rec.get("side") or "").upper(),
            }

    log.info(f"trades loaded: {len(trades)}")
    return trades


# ---------------------------------------------------------------------------
# Stats computation
# ---------------------------------------------------------------------------

@dataclass
class CalStats:
    total_decisions: int
    total_joined: int
    veto_n: int
    pass_n: int
    veto_precision: float   # P(r_mult < 0 | shadow_veto=1)
    veto_r_mean: float
    veto_r_median: float
    pass_r_mean: float
    pass_r_median: float
    r_delta: float          # pass_r_mean - veto_r_mean
    veto_reasons: dict[str, int]
    by_symbol: dict[str, Any]


def compute_stats(
    decisions: dict[str, dict[str, Any]],
    trades: dict[str, dict[str, Any]],
) -> CalStats:
    veto_r: list[float] = []
    pass_r: list[float] = []
    veto_reasons: dict[str, int] = collections.Counter()  # type: ignore[assignment]
    by_symbol: dict[str, dict[str, Any]] = {}

    for sid, dec in decisions.items():
        trade = trades.get(sid)
        if trade is None:
            continue
        r = trade["r_mult"]
        sym = dec["symbol"] or trade["symbol"]

        if sym not in by_symbol:
            by_symbol[sym] = {"veto": [], "pass": []}

        if dec["shadow_veto"] == 1:
            veto_r.append(r)
            veto_reasons[dec["reason"]] += 1
            by_symbol[sym]["veto"].append(r)
        else:
            pass_r.append(r)
            by_symbol[sym]["pass"].append(r)

    veto_neg = sum(1 for r in veto_r if r < 0.0)
    veto_precision = veto_neg / len(veto_r) if veto_r else 0.0
    r_delta = _mean(pass_r) - _mean(veto_r)

    flat_sym: dict[str, Any] = {}
    for sym, s in sorted(by_symbol.items()):
        vv, pp = s["veto"], s["pass"]
        neg_frac = sum(1 for x in vv if x < 0.0) / len(vv) if vv else 0.0
        flat_sym[sym] = {
            "veto_n":       len(vv),
            "veto_r_mean":  round(_mean(vv), 3),
            "veto_neg_frac": round(neg_frac, 3),
            "pass_n":       len(pp),
            "pass_r_mean":  round(_mean(pp), 3),
        }

    return CalStats(
        total_decisions = len(decisions),
        total_joined    = len(veto_r) + len(pass_r),
        veto_n          = len(veto_r),
        pass_n          = len(pass_r),
        veto_precision  = round(veto_precision, 4),
        veto_r_mean     = round(_mean(veto_r), 4),
        veto_r_median   = round(_median(veto_r), 4),
        pass_r_mean     = round(_mean(pass_r), 4),
        pass_r_median   = round(_median(pass_r), 4),
        r_delta         = round(r_delta, 4),
        veto_reasons    = dict(veto_reasons),
        by_symbol       = flat_sym,
    )


# ---------------------------------------------------------------------------
# Qualify check
# ---------------------------------------------------------------------------

@dataclass
class QualifyResult:
    qualified: bool
    shadow_hours: float
    checks: dict[str, Any]


def check_qualify(stats: CalStats, cfg: CalConfig, shadow_start_ms: int) -> QualifyResult:
    now_ms = _now_ms()
    shadow_hours = (now_ms - shadow_start_ms) / 3_600_000 if shadow_start_ms > 0 else 0.0

    checks: dict[str, Any] = {
        "shadow_hours":   {"value": round(shadow_hours, 2), "min": cfg.min_shadow_hours,
                           "ok": shadow_hours >= cfg.min_shadow_hours},
        "veto_n":         {"value": stats.veto_n, "min": cfg.min_veto_n,
                           "ok": stats.veto_n >= cfg.min_veto_n},
        "veto_precision": {"value": stats.veto_precision, "min": cfg.min_precision,
                           "ok": stats.veto_precision >= cfg.min_precision},
        "r_delta":        {"value": stats.r_delta, "min": cfg.min_r_delta,
                           "ok": stats.r_delta >= cfg.min_r_delta},
    }
    qualified = all(c["ok"] for c in checks.values())
    return QualifyResult(qualified=qualified, shadow_hours=shadow_hours, checks=checks)


# ---------------------------------------------------------------------------
# Telegram notifications + auto-apply
# ---------------------------------------------------------------------------

def _sign(bundle_id: str, secret: str) -> str:
    return hmac.new(secret.encode(), bundle_id.encode(), hashlib.sha256).hexdigest()[:8]


def _fmt_reasons(reasons: dict[str, int]) -> str:
    if not reasons:
        return "—"
    return " | ".join(f"<code>{k}</code>:{v}" for k, v in reasons.items())


def _fmt_sym(by_sym: dict[str, Any]) -> str:
    if not by_sym:
        return "—"
    lines = []
    for sym, s in list(by_sym.items())[:8]:
        lines.append(
            f"  <code>{sym:<12}</code> "
            f"veto:{s['veto_n']} prec:{s['veto_neg_frac']:.0%} R̄={s['veto_r_mean']:+.2f}  "
            f"pass:{s['pass_n']} R̄={s['pass_r_mean']:+.2f}"
        )
    return "\n".join(lines)


def apply_enforce(
    r: redis.Redis,
    r_main: redis.Redis,
    stats: CalStats,
    qual: QualifyResult,
    cfg: CalConfig,
) -> str:
    """Автоматически включает ENFORCE и шлёт Telegram-уведомление с кнопкой отката."""
    now_ms = _now_ms()

    # 1. Применяем немедленно
    r.hset("config:orderflow:GLOBAL", "liqmap_gate_mode", "enforce")
    log.info("liqmap_gate_mode=enforce applied to config:orderflow:GLOBAL")

    # 2. Готовим rollback-bundle (один клик → shadow)
    rb_id = f"liqmap_rollback_{int(time.time())}"
    sig = _sign(rb_id, cfg.recs_secret)
    rollback_bundle = {
        "id": rb_id,
        "created_ms": now_ms,
        "ops": [{"op": "HSET", "key": "config:orderflow:GLOBAL",
                 "field": "liqmap_gate_mode", "value": "shadow"}],
        "meta": {"title": "LiqMap Gate rollback → SHADOW"},
    }
    r_main.set(f"recs:bundle:{rb_id}", json.dumps(rollback_bundle))
    r_main.set(f"recs:status:{rb_id}", "PENDING", ex=86400 * 7)

    checks_txt = "\n".join(
        f"  {'✅' if c['ok'] else '❌'} {k}: {c['value']} (min {c['min']})"
        for k, c in qual.checks.items()
    )
    text = (
        f"<b>✅ LiqMap Gate → ENFORCE включён автоматически</b>\n\n"
        f"Shadow: <b>{qual.shadow_hours:.1f}ч</b>  "
        f"Joined: <b>{stats.total_joined}</b> трейдов\n\n"
        f"<b>Критерии:</b>\n{checks_txt}\n\n"
        f"<b>shadow_veto=1</b>: {stats.veto_n}  R̄={stats.veto_r_mean:+.3f}  "
        f"prec={stats.veto_precision:.0%}\n"
        f"<b>shadow_veto=0</b>: {stats.pass_n}  R̄={stats.pass_r_mean:+.3f}\n"
        f"ΔR (pass−veto) = <b>{stats.r_delta:+.3f}R</b>\n\n"
        f"Причины: {_fmt_reasons(stats.veto_reasons)}\n\n"
        f"<b>По символам:</b>\n{_fmt_sym(stats.by_symbol)}"
    )

    buttons = [[
        {"text": "↩ Rollback → SHADOW", "callback_data": f"recs:confirm:{rb_id}:{sig}"},
    ]]

    r_main.xadd(cfg.notify_stream, {
        "type": "report", "subtype": "liqmap_calibrator_enforce",
        "ts": str(now_ms),
        "text": text,
        "parse_mode": "HTML",
        "buttons": json.dumps(buttons),
    })
    log.info(f"enforce notification sent rollback_bundle={rb_id}")
    return rb_id


def send_status_report(
    r_main: redis.Redis,
    stats: CalStats,
    qual: QualifyResult,
    state_name: str,
    cfg: CalConfig,
) -> None:
    checks_txt = "\n".join(
        f"  {'✅' if c['ok'] else '❌'} {k}: {c['value']} (min {c['min']})"
        for k, c in qual.checks.items()
    )
    text = (
        f"<b>📈 LiqMap Gate Calibrator — статус</b>\n\n"
        f"State: <code>{state_name}</code>  "
        f"Shadow: {qual.shadow_hours:.1f}ч\n\n"
        f"<b>Критерии:</b>\n{checks_txt}\n\n"
        f"veto_n={stats.veto_n}  prec={stats.veto_precision:.0%}  "
        f"ΔR={stats.r_delta:+.3f}\n"
        f"Причины: {_fmt_reasons(stats.veto_reasons)}"
    )
    r_main.xadd(cfg.notify_stream, {
        "type": "report", "subtype": "liqmap_calibrator_status",
        "ts": str(_now_ms()),
        "text": text,
        "parse_mode": "HTML",
    })


# ---------------------------------------------------------------------------
# Prometheus metrics (stdout → scrape)
# ---------------------------------------------------------------------------

def emit_metrics(stats: CalStats, qual: QualifyResult, state_name: str) -> None:
    lines = [
        f'liqmap_cal_veto_n {stats.veto_n}',
        f'liqmap_cal_pass_n {stats.pass_n}',
        f'liqmap_cal_veto_precision {stats.veto_precision}',
        f'liqmap_cal_r_delta {stats.r_delta}',
        f'liqmap_cal_veto_r_mean {stats.veto_r_mean}',
        f'liqmap_cal_pass_r_mean {stats.pass_r_mean}',
        f'liqmap_cal_shadow_hours {qual.shadow_hours:.2f}',
        f'liqmap_cal_qualified {int(qual.qualified)}',
        f'liqmap_cal_state_warmup {int(state_name == "WARMUP")}',
        f'liqmap_cal_state_qualified {int(state_name == "QUALIFIED")}',
        f'liqmap_cal_state_enforce_proposed {int(state_name == "ENFORCE_PROPOSED")}',
    ]
    for reason, cnt in stats.veto_reasons.items():
        lines.append(f'liqmap_cal_veto_reason_total{{reason="{reason}"}} {cnt}')
    log.info("METRICS\n" + "\n".join(lines))


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def _is_already_enforce(r: redis.Redis) -> bool:
    try:
        val = r.hget("config:orderflow:GLOBAL", "liqmap_gate_mode") or ""
        return str(val).lower() == "enforce"
    except Exception:
        return False


def run_once(
    r: redis.Redis,
    r_main: redis.Redis,
    cfg: CalConfig,
    state: dict[str, Any],
) -> dict[str, Any]:
    now_ms = _now_ms()
    state_name: str = state.get("state", "WARMUP")

    # If already ENFORCE in Redis, just track passively
    if _is_already_enforce(r):
        state_name = "ENFORCE_ACTIVE"
        state["state"] = state_name
        log.info("liqmap_gate_mode=enforce already active — passive monitoring")

    decisions = _read_decisions(r, cfg.since_hours)
    if not decisions:
        log.warning("no shadow decisions found — staying in WARMUP")
        state["state"] = "WARMUP"
        state["last_run_ms"] = now_ms
        return state

    trades = _read_trades(r, cfg.since_hours)
    stats = compute_stats(decisions, trades)

    # Track shadow start from first seen decision ts
    if not state.get("shadow_start_ms"):
        earliest = min((d["ts_ms"] for d in decisions.values() if d["ts_ms"] > 0), default=0)
        state["shadow_start_ms"] = earliest if earliest > 0 else now_ms
        log.info(f"shadow_start_ms set: {state['shadow_start_ms']}")

    qual = check_qualify(stats, cfg, state["shadow_start_ms"])
    emit_metrics(stats, qual, state_name)

    log.info(
        f"state={state_name} shadow={qual.shadow_hours:.1f}h "
        f"veto_n={stats.veto_n} precision={stats.veto_precision:.2f} "
        f"r_delta={stats.r_delta:+.3f} qualified={qual.qualified}"
    )

    # State transitions
    if state_name == "ENFORCE_ACTIVE":
        pass  # passive — metrics only

    elif qual.qualified:
        # Cooldown: не применять чаще одного раза в cooldown_sec
        last_applied_ms = state.get("last_enforce_applied_ms") or 0
        cooldown_ok = (now_ms - last_applied_ms) >= cfg.cooldown_sec * 1000

        if not cooldown_ok:
            wait_sec = (cfg.cooldown_sec * 1000 - (now_ms - last_applied_ms)) // 1000
            log.info(f"qualified but cooldown active ({wait_sec}s left)")
            state["state"] = "QUALIFIED"
        elif cfg.apply:
            rb_id = apply_enforce(r, r_main, stats, qual, cfg)
            state["state"] = "ENFORCE_ACTIVE"
            state["last_enforce_applied_ms"] = now_ms
            state["rollback_bundle_id"] = rb_id
        else:
            log.info("DRY-RUN: criteria met, would apply enforce (LIQMAP_CAL_APPLY=0)")
            state["state"] = "QUALIFIED"

    else:
        enough_data = stats.total_joined >= 2
        state["state"] = "DATA_COLLECTED" if enough_data else "WARMUP"

    state["last_run_ms"] = now_ms
    state["last_stats"] = asdict(stats)
    state["total_runs"] = state.get("total_runs", 0) + 1

    log.info(f"state → {state['state']}")
    return state


def main() -> None:
    cfg = load_config()
    if not cfg.enable:
        log.info("LIQMAP_CAL_ENABLE=0 — exiting")
        return

    log.info(
        f"liqmap-gate-calibrator start "
        f"since_hours={cfg.since_hours} interval={cfg.interval_sec}s "
        f"apply={cfg.apply}"
    )

    try:
        r = redis.Redis.from_url(cfg.redis_url, decode_responses=True, socket_timeout=10)
        r_main = redis.Redis.from_url(cfg.redis_main_url, decode_responses=True, socket_timeout=10)
        r.ping()
        r_main.ping()
    except Exception as e:
        log.error(f"Redis connect failed: {e}")
        sys.exit(1)

    state = _load_state(cfg.state_path)
    log.info(f"loaded state: {state.get('state', 'WARMUP')} runs={state.get('total_runs', 0)}")

    state = run_once(r, r_main, cfg, state)
    _save_state(cfg.state_path, state)
    log.info("run complete")


if __name__ == "__main__":
    main()
