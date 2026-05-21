from __future__ import annotations

"""cost_edge_slippage_calibrator_v1.py

Batch calibrator для slippage_bps в CostEdgeGate.

Зачем
  CostEdgeGate.slippage_bps = 4.0 HARDCODED — не учитывает реальный слиппедж.
  SOL/PEPE реально исполняются с 8–12 bps adverse, BTC с 2–3 bps.
  Калибратор вычисляет q75(adverse_bps_t) per (symbol × session) из trades_closed
  и публикует в Redis — gate читает через SlippageCalReader.

Метод (rolling_q75_v1)
  Source: trades_closed.adverse_bps_t (realized market impact/adverse)
  Grouping: (symbol, session)  — session из trades_closed.session
  Weight: w = exp(-ln2 * age_days / HALF_LIFE_DAYS)  (half-life 7d)
  q75(weighted) → blend EWMA с прошлым значением → clamp [1.0, 30.0] bps
  Aggregate fallbacks: (sym, "*") + ("*", "*") для иерархии fallback в reader

Redis output
  enforce-mode: slippage_bps_cal:v1
  shadow-mode:  slippage_bps_cal:v1:shadow
  Payload: {"schema_version":1, "calibrated_ms":..., "groups":{"BTCUSDT:us_main":2.1, ...}}

Run
  python -m orderflow_services.cost_edge_slippage_calibrator_v1
  python -m orderflow_services.cost_edge_slippage_calibrator_v1 --apply 1
  python -m orderflow_services.cost_edge_slippage_calibrator_v1 --apply 1 --shadow-enforce 1

ENV
  SLIP_CAL_REDIS_URL       (default REDIS_URL или redis://redis-worker-1:6379/0)
  SLIP_CAL_REDIS_MAIN_URL  (default REDIS_URL)
  SLIP_CAL_DB_URL          (default DATABASE_URL)
  SLIP_CAL_APPLY           0=dry-run, 1=write
  SLIP_CAL_SHADOW_ENFORCE  0=shadow, 1=enforce
  SLIP_CAL_LOOKBACK_DAYS   default 30
  SLIP_CAL_HALF_LIFE_DAYS  default 7
  SLIP_CAL_LOWER           default 1.0  bps
  SLIP_CAL_UPPER           default 30.0 bps
  SLIP_CAL_ALPHA           default 0.095 (EWMA blend weight)
  SLIP_CAL_MIN_N           default 20 (min trades per group)
  SLIP_CAL_COOLDOWN_SEC    default 21600 (6h)
  SLIP_CAL_STATE_PATH      default /var/lib/trade/of_reports/slippage_cal_state.json
  SLIP_CAL_OUT_DIR         default /var/lib/trade/of_reports/slippage_calibration
"""

import argparse
import asyncio
import json
import math
import os
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any


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


def _log(msg: str) -> None:
    ts = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
    print(f"[{ts}] [slip_cal] {msg}", flush=True)


# ---------------------------------------------------------------------------
# Константы
# ---------------------------------------------------------------------------

HALF_LIFE_DAYS: float = _env_float("SLIP_CAL_HALF_LIFE_DAYS", 7.0)
SLIP_LOWER: float = _env_float("SLIP_CAL_LOWER", 1.0)
SLIP_UPPER: float = _env_float("SLIP_CAL_UPPER", 30.0)
DEFAULT_SLIP_BPS: float = _env_float("EDGE_SLIPPAGE_BPS_DEFAULT", 4.0)

_LN2 = math.log(2.0)
_ALPHA_DEFAULT: float = 1.0 - math.pow(2.0, -1.0 / HALF_LIFE_DAYS)  # ~0.095 at 7d

CAL_KEY = _env("SLIP_CAL_KEY", "slippage_bps_cal:v1")
CAL_SHADOW_KEY = CAL_KEY + ":shadow"

MIN_N: int = _env_int("SLIP_CAL_MIN_N", 20)
COOLDOWN_SEC: int = _env_int("SLIP_CAL_COOLDOWN_SEC", 21600)  # 6h

STATE_PATH: str = _env(
    "SLIP_CAL_STATE_PATH",
    "/var/lib/trade/of_reports/slippage_cal_state.json",
)
OUT_DIR: str = _env(
    "SLIP_CAL_OUT_DIR",
    "/var/lib/trade/of_reports/slippage_calibration",
)


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------

@dataclass
class SlipRow:
    symbol: str
    session: str      # us_main / european / asian / overnight / na
    adverse_bps: float
    exit_ts_ms: int

    @property
    def age_days(self) -> float:
        now_ms = int(time.time() * 1000)
        return max(0.0, (now_ms - self.exit_ts_ms)) / (1000.0 * 86400.0)

    @property
    def weight(self) -> float:
        return math.exp(-_LN2 * self.age_days / HALF_LIFE_DAYS)


@dataclass
class SlipFitResult:
    group_key: tuple[str, str]  # (symbol, session)
    n: int
    q25: float
    q50: float
    q75: float                  # основной результат
    w_total: float


# ---------------------------------------------------------------------------
# Weighted quantile (без numpy)
# ---------------------------------------------------------------------------

def _weighted_quantile(values: list[float], weights: list[float], q: float) -> float:
    """Взвешенный квантиль без внешних зависимостей."""
    pairs = [
        (v, w) for v, w in zip(values, weights)
        if math.isfinite(v) and math.isfinite(w) and w > 0 and v > 0
    ]
    if not pairs:
        return DEFAULT_SLIP_BPS
    q = max(0.0, min(1.0, q))
    pairs.sort(key=lambda x: x[0])
    vals = [p[0] for p in pairs]
    wts = [p[1] for p in pairs]
    total_w = sum(wts)
    if total_w <= 0:
        return DEFAULT_SLIP_BPS
    cum_w = 0.0
    cum: list[float] = []
    for w in wts:
        cum_w += w / total_w
        cum.append(cum_w)
    if q <= cum[0]:
        return float(vals[0])
    if q >= cum[-1]:
        return float(vals[-1])
    for i in range(1, len(cum)):
        if cum[i] >= q:
            w_lo, w_hi = cum[i - 1], cum[i]
            if abs(w_hi - w_lo) < 1e-12:
                return float(vals[i])
            frac = (q - w_lo) / (w_hi - w_lo)
            return float(vals[i - 1] * (1.0 - frac) + vals[i] * frac)
    return float(vals[-1])


# ---------------------------------------------------------------------------
# DB load (asyncpg)
# ---------------------------------------------------------------------------

async def load_trades(db_url: str, lookback_days: int = 30) -> list[SlipRow]:
    """Загружает (symbol, session, adverse_bps_t, exit_ts_ms) из trades_closed."""
    try:
        import asyncpg  # type: ignore[import-untyped]
    except ImportError:
        _log("asyncpg not available")
        return []

    sql = f"""
        SELECT
            symbol,
            COALESCE(NULLIF(session, ''), 'na')  AS session,
            adverse_bps_t,
            exit_ts_ms
        FROM trades_closed
        WHERE exit_ts_ms >= EXTRACT(EPOCH FROM (now() - INTERVAL '{lookback_days} days')) * 1000
          AND is_virtual IS NOT TRUE
          AND adverse_bps_t > 0
          AND adverse_bps_t < 500
          AND symbol IS NOT NULL
        ORDER BY exit_ts_ms ASC
    """
    try:
        conn = await asyncpg.connect(db_url)
        try:
            rows = await conn.fetch(sql)
        finally:
            await conn.close()
    except Exception as exc:
        _log(f"DB load error: {exc}")
        return []

    result: list[SlipRow] = []
    for r in rows:
        try:
            sym = str(r["symbol"] or "").upper().strip()
            if not sym:
                continue
            sess = str(r["session"] or "na").lower().strip() or "na"
            bps = float(r["adverse_bps_t"] or 0.0)
            if bps <= 0:
                continue
            ts = int(r["exit_ts_ms"] or 0)
            result.append(SlipRow(symbol=sym, session=sess, adverse_bps=bps, exit_ts_ms=ts))
        except Exception:
            continue

    _log(f"Loaded {len(result)} rows from DB (lookback={lookback_days}d)")
    return result


# ---------------------------------------------------------------------------
# Calibration core
# ---------------------------------------------------------------------------

def compute_q75_fit(rows: list[SlipRow]) -> dict[tuple[str, str], SlipFitResult]:
    """Вычисляет q75(adverse_bps) per (symbol, session) + aggregate fallbacks."""
    groups: dict[tuple[str, str], list[SlipRow]] = {}
    for r in rows:
        key = (r.symbol, r.session)
        groups.setdefault(key, []).append(r)
        # aggregate by symbol across all sessions
        groups.setdefault((r.symbol, "*"), []).append(r)
        # global aggregate
        groups.setdefault(("*", "*"), []).append(r)

    results: dict[tuple[str, str], SlipFitResult] = {}
    for gk, grows in groups.items():
        bps_vals = [r.adverse_bps for r in grows]
        w_vals = [r.weight for r in grows]
        w_total = sum(w_vals)
        q25 = _weighted_quantile(bps_vals, w_vals, 0.25)
        q50 = _weighted_quantile(bps_vals, w_vals, 0.50)
        q75 = _weighted_quantile(bps_vals, w_vals, 0.75)
        results[gk] = SlipFitResult(
            group_key=gk,
            n=len(grows),
            q25=q25,
            q50=q50,
            q75=q75,
            w_total=w_total,
        )
    return results


def blend_and_clamp(q75: float, old_bps: float, alpha: float) -> float:
    """EWMA blend + hard clamp [SLIP_LOWER, SLIP_UPPER]."""
    if not math.isfinite(old_bps) or old_bps <= 0:
        old_bps = DEFAULT_SLIP_BPS
    if not math.isfinite(q75) or q75 <= 0:
        q75 = DEFAULT_SLIP_BPS
    blended = (1.0 - alpha) * old_bps + alpha * q75
    return max(SLIP_LOWER, min(SLIP_UPPER, blended))


# ---------------------------------------------------------------------------
# Redis helpers
# ---------------------------------------------------------------------------

def load_current_calibration(redis_client: Any, key: str) -> dict[str, float]:
    """Загружает предыдущую калибровку из Redis → {"SYM:sess" → bps}."""
    try:
        raw = redis_client.get(key)
        if not raw:
            return {}
        obj = json.loads(str(raw))
        groups = obj.get("groups")
        if not isinstance(groups, dict):
            return {}
        out: dict[str, float] = {}
        for k, v in groups.items():
            try:
                f = float(v)
                if f > 0:
                    out[str(k).upper()] = f
            except Exception:
                pass
        return out
    except Exception as exc:
        _log(f"load_current_calibration error: {exc}")
        return {}


def build_payload(
    results: dict[tuple[str, str], SlipFitResult],
    current: dict[str, float],
    alpha: float,
    run_id: str,
    n_rows: int,
    min_n: int,
    apply: bool,
    shadow_enforce: int,
) -> dict[str, Any]:
    now_ms = int(time.time() * 1000)
    groups_out: dict[str, Any] = {}
    for (sym, sess), fit in results.items():
        if fit.n < min_n and "*" not in sym and "*" not in sess:
            continue  # пропускаем маломощные реальные группы
        key_str = f"{sym}:{sess}".upper()
        old_bps = current.get(key_str, DEFAULT_SLIP_BPS)
        new_bps = blend_and_clamp(fit.q75, old_bps, alpha)
        groups_out[key_str] = {
            "symbol": sym,
            "session": sess,
            "n": fit.n,
            "q25": round(fit.q25, 3),
            "q50": round(fit.q50, 3),
            "q75": round(fit.q75, 3),
            "old_bps": round(old_bps, 3),
            "new_bps": round(new_bps, 3),
            "w_total": round(fit.w_total, 3),
        }
    return {
        "schema_version": 1,
        "calibrated_ms": now_ms,
        "run_id": run_id,
        "method": "rolling_q75_v1",
        "n_rows": n_rows,
        "n_groups": len(groups_out),
        "alpha": round(alpha, 6),
        "slip_lower": SLIP_LOWER,
        "slip_upper": SLIP_UPPER,
        "half_life_days": HALF_LIFE_DAYS,
        "apply": apply,
        "shadow_enforce": shadow_enforce,
        "groups": groups_out,
    }


# ---------------------------------------------------------------------------
# File I/O
# ---------------------------------------------------------------------------

def _atomic_write(path: str, obj: dict[str, Any]) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2, default=str)
    os.replace(tmp, path)


def _load_json_safe(path: str) -> dict[str, Any]:
    try:
        if path and os.path.exists(path):
            with open(path, encoding="utf-8") as f:
                obj = json.load(f)
            return obj if isinstance(obj, dict) else {}
    except Exception:
        pass
    return {}


def write_redis(redis_client: Any, key: str, payload: dict[str, Any]) -> bool:
    try:
        redis_client.set(key, json.dumps(payload, ensure_ascii=False, separators=(",", ":")))
        return True
    except Exception as exc:
        _log(f"write_redis({key}) error: {exc}")
        return False


# ---------------------------------------------------------------------------
# Telegram notify
# ---------------------------------------------------------------------------

def notify_telegram(redis_url: str, text: str, severity: str = "info",
                    dedup_key: str | None = None,
                    notify_stream: str = "notify:telegram") -> None:
    try:
        import redis as redis_lib
        r = redis_lib.Redis.from_url(redis_url, decode_responses=True)
        if isinstance(r.xlen(notify_stream), int) and r.xlen(notify_stream) > 10_000:
            return
        if dedup_key:
            if not r.set(f"dedup:reporting:{dedup_key}", "1", nx=True, ex=6 * 3600):
                return
        r.xadd(notify_stream, {  # type: ignore[arg-type]
            "type": "report",
            "text": text,
            "parse_mode": "HTML",
            "source": "cost_edge_slippage_calibrator_v1",
            "severity": severity,
            "timestamp": str(int(time.time() * 1000)),
            **({"dedup_key": dedup_key} if dedup_key else {}),
        }, maxlen=5000)
    except Exception as exc:
        _log(f"Telegram notify error: {exc}")


def _fmt_msg(payload: dict[str, Any], phase: str) -> str:
    heads = {
        "promoted": "✅ <b>Slippage Calibrator — применено</b>",
        "shadow":   "🔍 <b>Slippage Calibrator — shadow write</b>",
        "blocked":  "🚫 <b>Slippage Calibrator — заблокировано</b>",
        "dry_run":  "ℹ️ <b>Slippage Calibrator — dry-run</b>",
    }
    lines = [heads.get(phase, "ℹ️ <b>Slippage Calibrator</b>"), ""]
    lines.append(f"<b>Run:</b> <code>{payload.get('run_id', '')}</code>")
    lines.append(
        f"<b>Строк:</b> {payload.get('n_rows', 0)}  "
        f"<b>Групп:</b> {payload.get('n_groups', 0)}  "
        f"<b>Alpha:</b> {payload.get('alpha', 0):.4f}"
    )
    if payload.get("blockers"):
        lines.append("")
        lines.append("<b>Блокеры:</b>")
        for b in payload["blockers"]:
            lines.append(f"  ❌ {b}")
    groups = payload.get("groups") or {}
    real = {k: v for k, v in groups.items() if "*" not in k}
    if real:
        lines.append("")
        lines.append("<b>Примеры (old→new bps):</b>")
        for i, (k, v) in enumerate(sorted(real.items())):
            if i >= 8:
                lines.append(f"  ... и ещё {len(real) - 8}")
                break
            lines.append(
                f"  <code>{k}</code>: "
                f"{v.get('old_bps', 0):.2f}→{v.get('new_bps', 0):.2f} bps "
                f"(n={v.get('n', 0)}, q75={v.get('q75', 0):.2f})"
            )
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# main()
# ---------------------------------------------------------------------------

def main() -> int:
    ap = argparse.ArgumentParser(description="Cost-Edge slippage calibrator")
    ap.add_argument("--redis-url",
                    default=_env("SLIP_CAL_REDIS_URL", _env("REDIS_URL", "redis://redis-worker-1:6379/0")))
    ap.add_argument("--redis-main-url",
                    default=_env("SLIP_CAL_REDIS_MAIN_URL", _env("REDIS_URL", "redis://redis:6379/0")))
    ap.add_argument("--db-url",
                    default=_env("SLIP_CAL_DB_URL", _env("DATABASE_URL",
                        "postgresql://trading:trading@scanner-postgres:5432/scanner_analytics")))
    ap.add_argument("--apply", type=int, default=_env_int("SLIP_CAL_APPLY", 0))
    ap.add_argument("--shadow-enforce", type=int, default=_env_int("SLIP_CAL_SHADOW_ENFORCE", 0))
    ap.add_argument("--lookback-days", type=int, default=_env_int("SLIP_CAL_LOOKBACK_DAYS", 30))
    ap.add_argument("--min-n", type=int, default=_env_int("SLIP_CAL_MIN_N", MIN_N))
    ap.add_argument("--alpha", type=float, default=_env_float("SLIP_CAL_ALPHA", _ALPHA_DEFAULT))
    ap.add_argument("--cooldown-sec", type=int, default=_env_int("SLIP_CAL_COOLDOWN_SEC", COOLDOWN_SEC))
    ap.add_argument("--state-path", default=_env("SLIP_CAL_STATE_PATH", STATE_PATH))
    ap.add_argument("--out-dir", default=_env("SLIP_CAL_OUT_DIR", OUT_DIR))
    ap.add_argument("--notify-stream", default=_env("NOTIFY_STREAM", "notify:telegram"))
    args = ap.parse_args()

    # --- State / cooldown ---
    state = _load_json_safe(args.state_path)
    now_ms = int(time.time() * 1000)
    last_run_ms = int(state.get("last_run_ms") or 0)
    cooldown_ms = args.cooldown_sec * 1000

    if last_run_ms > 0 and (now_ms - last_run_ms) < cooldown_ms:
        remaining_s = (cooldown_ms - (now_ms - last_run_ms)) / 1000
        _log(f"Cooldown active — {remaining_s:.0f}s remaining, skipping")
        return 0

    state["last_run_ms"] = now_ms
    state["pid"] = os.getpid()
    state.setdefault("history", [])

    ts_str = time.strftime("%Y%m%d_%H%M%S", time.gmtime())
    run_id = f"slip_cal_v1_{ts_str}_{uuid.uuid4().hex[:6]}"
    _log(f"Starting run_id={run_id} apply={args.apply} shadow_enforce={args.shadow_enforce}")

    # --- Load data ---
    rows = asyncio.run(load_trades(db_url=args.db_url, lookback_days=args.lookback_days))

    if not rows:
        msg = f"Нет данных adverse_bps_t в trades_closed (lookback={args.lookback_days}d)"
        _log(msg)
        state["phase"] = "blocked"
        state["block_reason"] = "no_data"
        _atomic_write(args.state_path, state)
        notify_telegram(args.redis_main_url,
                        f"⚠️ <b>Slippage Calibrator</b>\n\n{msg}",
                        severity="warn",
                        dedup_key="slip_cal_no_data",
                        notify_stream=args.notify_stream)
        return 1

    # --- Calibrate ---
    results = compute_q75_fit(rows)

    # Реальные группы (без агрегатов)
    real_groups = {k: v for k, v in results.items() if "*" not in k[0] and "*" not in k[1]}
    qualified = {k: v for k, v in real_groups.items() if v.n >= args.min_n}

    blockers: list[str] = []
    if not qualified:
        blockers.append(
            f"0 qualified groups (min_n={args.min_n}, real_groups={len(real_groups)})"
        )

    # --- Load previous calibration ---
    redis_client = None
    cal_key = CAL_KEY if args.shadow_enforce == 1 else CAL_SHADOW_KEY
    current: dict[str, float] = {}
    try:
        import redis as redis_lib
        redis_client = redis_lib.Redis.from_url(args.redis_url, decode_responses=True)
        current = load_current_calibration(redis_client, cal_key)
    except Exception as exc:
        _log(f"Redis connect error: {exc}")

    # --- Build payload ---
    payload = build_payload(
        results=results,
        current=current,
        alpha=args.alpha,
        run_id=run_id,
        n_rows=len(rows),
        min_n=args.min_n,
        apply=bool(args.apply),
        shadow_enforce=args.shadow_enforce,
    )
    payload["blockers"] = blockers

    event = {
        "ts_ms": now_ms,
        "run_id": run_id,
        "n_rows": len(rows),
        "n_real": len(real_groups),
        "n_qualified": len(qualified),
        "gates_passed": not blockers,
        "apply": args.apply,
        "shadow_enforce": args.shadow_enforce,
    }
    state["history"] = (state.get("history") or [])[-49:] + [event]

    if blockers:
        _log(f"Gates FAILED: {blockers}")
        state["phase"] = "blocked"
        state["last_blockers"] = blockers
        _atomic_write(args.state_path, state)
        notify_telegram(args.redis_main_url, _fmt_msg(payload, "blocked"),
                        severity="warn",
                        dedup_key=f"slip_cal_blocked_{ts_str[:8]}",
                        notify_stream=args.notify_stream)
        return 0

    out_path = ""
    if args.apply:
        # --- Write file ---
        ts_label = time.strftime("%Y%m%d_%H%M%S", time.gmtime())
        out_path = str(Path(args.out_dir) / f"slippage_calibration_{ts_label}.json")
        try:
            _atomic_write(out_path, payload)
            _log(f"Written file: {out_path}")
        except Exception as exc:
            _log(f"File write error: {exc}")
            state["phase"] = "blocked"
            _atomic_write(args.state_path, state)
            return 1

        # --- Write Redis ---
        write_key = CAL_KEY if args.shadow_enforce == 1 else CAL_SHADOW_KEY
        if redis_client is not None:
            ok = write_redis(redis_client, write_key, payload)
            _log(f"Redis write {'OK' if ok else 'FAILED'}: {write_key}")
        phase_label = "promoted" if args.shadow_enforce == 1 else "shadow"
        state["phase"] = phase_label
        state["last_applied_ms"] = now_ms
        state["last_cal_path"] = out_path
        state["last_run_id"] = run_id
        notify_telegram(args.redis_main_url, _fmt_msg(payload, phase_label),
                        severity="info",
                        dedup_key=f"slip_cal_applied_{ts_str}",
                        notify_stream=args.notify_stream)
    else:
        _log("Dry-run (--apply=0): no file/Redis write")
        state["phase"] = "dry_run"
        state["last_run_id"] = run_id
        notify_telegram(args.redis_main_url, _fmt_msg(payload, "dry_run"),
                        severity="info",
                        dedup_key=f"slip_cal_dryrun_{ts_str[:8]}",
                        notify_stream=args.notify_stream)

    _atomic_write(args.state_path, state)
    _log(f"Done. phase={state.get('phase')} run_id={run_id}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
