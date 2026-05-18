from __future__ import annotations

"""cost_k_calibrator_v1.py

Авто-калибратор Cost-K мультипликатора для CostEdgeGate.

Зачем
  CostEdgeGate использует K-множитель:
    expected_edge_bps > total_costs_bps * K
  Статичный K=4.0 не учитывает реальное соотношение edge/costs по символам
  и режимам. Этот калибратор вычисляет K из реальных trades_closed.

Метод (ewma_realized_k_v1)
  K_observed_i = pnl_gross_i / fees_i  (отношение gross edge к cost per trade)
  Time-weighted median: w_i = exp(-ln(2) * age_days / 7.0), half-life 7 дней
  Blend: K_new = (1 - alpha) * K_old + alpha * K_fit, alpha ≈ 0.095 (half-life 7d)
  Hard clamp: [K_LOWER=2.0, K_UPPER=8.0]
  Иерархический fallback: (sym, regime) → (sym, "*") → ("*", "*") → 4.0

Output
  - File: COST_K_CAL_OUT_DIR/cost_k_calibration_<ts>.json
  - Redis:
      shadow-mode: cfg:cost_edge_gate:v1:calibration:shadow
      enforce-mode: cfg:cost_edge_gate:v1:calibration
  - notify:telegram (success/blocked/dry-run)
  - State: COST_K_CAL_STATE_PATH (cooldown + history[last 50])

Run
  python -m orderflow_services.cost_k_calibrator_v1
  python -m orderflow_services.cost_k_calibrator_v1 --apply 1 --shadow-enforce 1
  python -m orderflow_services.cost_k_calibrator_v1 --lookback-days 90 --apply 1
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
    print(f"[{ts}] [cost_k_calibrator] {msg}", flush=True)


# ---------------------------------------------------------------------------
# Константы
# ---------------------------------------------------------------------------

DEFAULT_K: float = _env_float("COST_K_DEFAULT", 4.0)
K_LOWER: float = _env_float("COST_K_LOWER", 2.0)
K_UPPER: float = _env_float("COST_K_UPPER", 8.0)
HALF_LIFE_DAYS: float = _env_float("COST_K_HALF_LIFE_DAYS", 7.0)

# alpha = 1 - 2^(-1/half_life): при HALF_LIFE_DAYS=7 → ≈0.0953
_ALPHA_DEFAULT: float = 1.0 - math.pow(2.0, -1.0 / HALF_LIFE_DAYS)

# Redis keys
CAL_KEY = _env("COST_K_CAL_KEY", "cfg:cost_edge_gate:v1:calibration")
CAL_SHADOW_KEY = CAL_KEY + ":shadow"

MIN_TRADES_PER_GROUP: int = _env_int("COST_K_MIN_TRADES_PER_GROUP", 20)
MIN_GROUPS: int = _env_int("COST_K_MIN_GROUPS", 1)
COOLDOWN_SEC: int = _env_int("COST_K_COOLDOWN_SEC", 43200)  # 12h

STATE_PATH: str = _env(
    "COST_K_CAL_STATE_PATH",
    "/var/lib/trade/of_reports/cost_k_calibrator_state.json"
)
OUT_DIR: str = _env(
    "COST_K_CAL_OUT_DIR",
    "/var/lib/trade/of_reports/cost_k_calibration"
)


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------

@dataclass
class TradeRow:
    symbol: str
    regime: str           # "NORMAL" если NULL
    pnl_gross: float
    fees: float
    notional_usd: float
    exit_ts_ms: int

    @property
    def age_days(self) -> float:
        now_ms = int(time.time() * 1000)
        delta_ms = max(0, now_ms - self.exit_ts_ms)
        return delta_ms / (1000.0 * 86400.0)

    @property
    def weight(self) -> float:
        """Экспоненциальный вес: w = exp(-ln(2) * age_days / half_life)."""
        ln2 = math.log(2.0)
        return math.exp(-ln2 * self.age_days / HALF_LIFE_DAYS)

    @property
    def k_observed(self) -> float:
        """K_i = pnl_gross / fees (наблюдаемое соотношение edge/cost)."""
        if self.fees <= 0:
            return float("nan")
        return self.pnl_gross / self.fees


@dataclass
class KFitResult:
    group_key: tuple[str, str]   # (symbol, regime)
    n: int
    K_p25: float
    K_p50: float                 # основной результат
    K_p75: float
    n_positive: int              # число сделок с pnl_gross > 0
    w_total: float               # сумма весов


# ---------------------------------------------------------------------------
# Weighted quantile (без numpy)
# ---------------------------------------------------------------------------

def _weighted_quantile(values: list[float], weights: list[float], q: float) -> float:
    """Взвешенный квантиль через sort + накопление весов.

    Реализация аналогична numpy.percentile с интерполяцией.
    """
    if not values:
        return DEFAULT_K

    q = max(0.0, min(1.0, q))

    # Фильтруем NaN и отрицательные веса
    pairs = [
        (v, w) for v, w in zip(values, weights)
        if math.isfinite(v) and math.isfinite(w) and w > 0
    ]
    if not pairs:
        return DEFAULT_K

    # Сортируем по значению
    pairs.sort(key=lambda x: x[0])
    vals = [p[0] for p in pairs]
    wts = [p[1] for p in pairs]

    total_w = sum(wts)
    if total_w <= 0:
        return DEFAULT_K

    # Нормируем
    cum_w = 0.0
    cum_weights: list[float] = []
    for w in wts:
        cum_w += w / total_w
        cum_weights.append(cum_w)

    # Находим позицию квантиля через линейную интерполяцию
    target = q
    if target <= cum_weights[0]:
        return float(vals[0])
    if target >= cum_weights[-1]:
        return float(vals[-1])

    for i in range(1, len(cum_weights)):
        if cum_weights[i] >= target:
            # Линейная интерполяция между i-1 и i
            w_lo = cum_weights[i - 1]
            w_hi = cum_weights[i]
            if abs(w_hi - w_lo) < 1e-12:
                return float(vals[i])
            frac = (target - w_lo) / (w_hi - w_lo)
            return float(vals[i - 1] * (1.0 - frac) + vals[i] * frac)

    return float(vals[-1])


# ---------------------------------------------------------------------------
# Data loading (asyncpg)
# ---------------------------------------------------------------------------

async def load_trades(db_url: str, lookback_days: int = 60) -> list[TradeRow]:
    """Загружает trades_closed из PostgreSQL."""
    try:
        import asyncpg  # type: ignore[import-untyped]
    except ImportError:
        _log("asyncpg not available — install it to use DB loading")
        return []

    sql = f"""
        SELECT
            symbol,
            COALESCE(atr_policy_regime, 'NORMAL') AS regime,
            COALESCE(pnl_gross, 0.0)             AS pnl_gross,
            COALESCE(fees, 0.0)                  AS fees,
            COALESCE(notional_usd, 0.0)          AS notional_usd,
            exit_ts_ms
        FROM trades_closed
        WHERE exit_ts_ms >= EXTRACT(EPOCH FROM (now() - INTERVAL '{lookback_days} days')) * 1000
          AND is_virtual IS NOT TRUE
          AND notional_usd > 0
          AND fees > 0
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

    result: list[TradeRow] = []
    for r in rows:
        try:
            symbol = str(r["symbol"] or "").upper().strip()
            if not symbol:
                continue
            regime = str(r["regime"] or "NORMAL").upper().strip() or "NORMAL"
            pnl_gross = float(r["pnl_gross"] or 0.0)
            fees = float(r["fees"] or 0.0)
            notional_usd = float(r["notional_usd"] or 0.0)
            exit_ts_ms = int(r["exit_ts_ms"] or 0)
            if fees <= 0 or notional_usd <= 0:
                continue
            result.append(TradeRow(
                symbol=symbol,
                regime=regime,
                pnl_gross=pnl_gross,
                fees=fees,
                notional_usd=notional_usd,
                exit_ts_ms=exit_ts_ms,
            ))
        except Exception:
            continue

    _log(f"Loaded {len(result)} trades from DB (lookback={lookback_days}d)")
    return result


# ---------------------------------------------------------------------------
# Calibration core
# ---------------------------------------------------------------------------

def compute_k_fit(trades: list[TradeRow]) -> dict[tuple[str, str], KFitResult]:
    """Вычисляет K_fit для каждой (symbol, regime) группы.

    Также заполняет агрегированные группы:
      (symbol, "*") — агрегат по всем режимам
      ("*", "*")    — глобальный агрегат
    """
    # Группируем по (symbol, regime)
    groups: dict[tuple[str, str], list[TradeRow]] = {}
    for t in trades:
        key = (t.symbol, t.regime)
        groups.setdefault(key, []).append(t)
        # Агрегат по символу
        key_sym = (t.symbol, "*")
        groups.setdefault(key_sym, []).append(t)
        # Глобальный агрегат
        key_global = ("*", "*")
        groups.setdefault(key_global, []).append(t)

    results: dict[tuple[str, str], KFitResult] = {}

    for group_key, group_trades in groups.items():
        valid = [t for t in group_trades if t.fees > 0 and math.isfinite(t.k_observed)]
        if not valid:
            continue

        k_vals = [t.k_observed for t in valid]
        w_vals = [t.weight for t in valid]
        w_total = sum(w_vals)

        k_p25 = _weighted_quantile(k_vals, w_vals, 0.25)
        k_p50 = _weighted_quantile(k_vals, w_vals, 0.50)
        k_p75 = _weighted_quantile(k_vals, w_vals, 0.75)
        n_positive = sum(1 for t in valid if t.pnl_gross > 0)

        results[group_key] = KFitResult(
            group_key=group_key,
            n=len(valid),
            K_p25=k_p25,
            K_p50=k_p50,
            K_p75=k_p75,
            n_positive=n_positive,
            w_total=w_total,
        )

    return results


# ---------------------------------------------------------------------------
# Redis helpers
# ---------------------------------------------------------------------------

def load_current_calibration(redis_client: Any) -> dict[str, float]:
    """Загружает предыдущую калибровку из Redis.

    Returns: dict["sym:regime" → K_old]
    """
    try:
        raw = redis_client.get(CAL_KEY)
        if not raw:
            return {}
        obj = json.loads(str(raw))
        groups = obj.get("groups")
        if not isinstance(groups, dict):
            return {}
        out: dict[str, float] = {}
        for key_str, entry in groups.items():
            if isinstance(entry, dict):
                v = entry.get("K_new") or entry.get("K_fit")
            else:
                v = entry
            try:
                if v is not None:
                    out[key_str] = float(v)
            except Exception:
                pass
        return out
    except Exception as exc:
        _log(f"load_current_calibration error: {exc}")
        return {}


def blend_and_clamp(K_fit: float, K_old: float, alpha: float) -> float:
    """EWMA blend с hard clamp."""
    if not math.isfinite(K_old) or K_old <= 0:
        K_old = DEFAULT_K
    if not math.isfinite(K_fit):
        K_fit = DEFAULT_K

    K_new = (1.0 - alpha) * K_old + alpha * K_fit
    return max(K_LOWER, min(K_UPPER, K_new))


# ---------------------------------------------------------------------------
# Gates
# ---------------------------------------------------------------------------

def check_gates(
    n_trades: int,
    n_groups: int,
    blockers: list[str],
    min_trades_per_group: int = MIN_TRADES_PER_GROUP,
    min_groups: int = MIN_GROUPS,
) -> tuple[bool, list[str]]:
    """Проверяет условия применения калибровки.

    Добавляет в blockers причины блокировки.
    Returns: (passed, updated_blockers)
    """
    result_blockers = list(blockers)

    if n_trades < min_trades_per_group:
        result_blockers.append(
            f"total_trades={n_trades} < min_trades_per_group={min_trades_per_group}"
        )

    # Исключаем агрегированные группы из подсчёта "реальных" групп
    if n_groups < min_groups:
        result_blockers.append(
            f"n_groups={n_groups} < min_groups={min_groups}"
        )

    return len(result_blockers) == 0, result_blockers


# ---------------------------------------------------------------------------
# Payload builder
# ---------------------------------------------------------------------------

def build_payload(
    results: dict[tuple[str, str], KFitResult],
    current_k: dict[str, float],
    alpha: float,
    blockers: list[str],
    gates_passed: bool,
    run_id: str,
    n_trades: int,
    apply: bool,
    shadow_enforce: int,
) -> dict[str, Any]:
    now_ms = int(time.time() * 1000)

    groups_out: dict[str, Any] = {}
    for (sym, regime), fit in results.items():
        key_str = f"{sym}:{regime}"
        K_old = current_k.get(key_str, DEFAULT_K)
        K_new = blend_and_clamp(fit.K_p50, K_old, alpha)
        groups_out[key_str] = {
            "symbol": sym,
            "regime": regime,
            "n": fit.n,
            "K_p25": round(fit.K_p25, 4),
            "K_p50": round(fit.K_p50, 4),
            "K_p75": round(fit.K_p75, 4),
            "K_old": round(K_old, 4),
            "K_new": round(K_new, 4),
            "K_fit": round(fit.K_p50, 4),
            "n_positive": fit.n_positive,
            "w_total": round(fit.w_total, 4),
        }

    return {
        "schema_version": 1,
        "calibrated_ms": now_ms,
        "run_id": run_id,
        "method": "ewma_realized_k_v1",
        "n_trades": n_trades,
        "n_groups": len(results),
        "alpha": round(alpha, 6),
        "K_lower": K_LOWER,
        "K_upper": K_UPPER,
        "half_life_days": HALF_LIFE_DAYS,
        "gates_passed": gates_passed,
        "blockers": blockers,
        "apply": apply,
        "shadow_enforce": shadow_enforce,
        "groups": groups_out,
    }


# ---------------------------------------------------------------------------
# File I/O
# ---------------------------------------------------------------------------

def _atomic_write_json(path: str, obj: dict[str, Any]) -> None:
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
        _log(f"write_redis error (key={key}): {exc}")
        return False


def write_file(out_dir: str, payload: dict[str, Any]) -> str:
    ts_str = time.strftime("%Y%m%d_%H%M%S", time.gmtime())
    out_path = str(Path(out_dir) / f"cost_k_calibration_{ts_str}.json")
    try:
        _atomic_write_json(out_path, payload)
        _log(f"Calibration written: {out_path}")
        return out_path
    except Exception as exc:
        _log(f"write_file error: {exc}")
        return ""


# ---------------------------------------------------------------------------
# Telegram notify
# ---------------------------------------------------------------------------

def notify_telegram(
    redis_url: str,
    text: str,
    severity: str = "info",
    dedup_key: str | None = None,
    notify_stream: str = "notify:telegram",
) -> None:
    try:
        import redis as redis_lib
        r = redis_lib.Redis.from_url(redis_url, decode_responses=True)
        q_len = r.xlen(notify_stream)
        if isinstance(q_len, int) and q_len > 10_000:
            _log("Telegram stream overloaded, dropping notification")
            return
        if dedup_key:
            d_key = f"dedup:reporting:{dedup_key}"
            if not r.set(d_key, "1", nx=True, ex=6 * 3600):
                return
        msg: dict[str, str] = {
            "type": "report",
            "text": text,
            "parse_mode": "HTML",
            "source": "cost_k_calibrator_v1",
            "severity": severity,
            "timestamp": str(int(time.time() * 1000)),
        }
        if dedup_key:
            msg["dedup_key"] = dedup_key
        r.xadd(notify_stream, msg, maxlen=5000)  # type: ignore[arg-type]
    except Exception as exc:
        _log(f"Telegram notify error: {exc}")


def _fmt_msg(payload: dict[str, Any], phase: str) -> str:
    groups = payload.get("groups") or {}
    n_trades = payload.get("n_trades", 0)
    n_groups = payload.get("n_groups", 0)
    head = {
        "promoted": "✅ <b>Cost-K Calibrator — применено</b>",
        "blocked": "🚫 <b>Cost-K Calibrator — заблокировано</b>",
        "dry_run": "ℹ️ <b>Cost-K Calibrator — dry-run</b>",
        "shadow": "🔍 <b>Cost-K Calibrator — shadow write</b>",
    }.get(phase, "ℹ️ <b>Cost-K Calibrator</b>")

    lines = [head, ""]
    lines.append(f"<b>Run:</b> <code>{payload.get('run_id', '')}</code>")
    lines.append(f"<b>Сделок:</b> {n_trades}  <b>Групп:</b> {n_groups}")
    lines.append(f"<b>Alpha:</b> {payload.get('alpha', 0):.4f}  "
                 f"<b>Clamp:</b> [{payload.get('K_lower')}, {payload.get('K_upper')}]")

    if payload.get("blockers"):
        lines.append("")
        lines.append("<b>Блокеры:</b>")
        for b in payload["blockers"]:
            lines.append(f"  ❌ {b}")

    if groups:
        lines.append("")
        lines.append("<b>Группы (K_old → K_new):</b>")
        # Показываем только реальные группы (без агрегированных "*")
        shown = 0
        for key_str, entry in sorted(groups.items()):
            if "*" in key_str:
                continue
            if shown >= 10:
                lines.append(f"  ... и ещё {len(groups)} групп")
                break
            lines.append(
                f"  <code>{key_str}</code>: "
                f"{entry.get('K_old', 0):.2f} → {entry.get('K_new', 0):.2f} "
                f"(n={entry.get('n', 0)})"
            )
            shown += 1

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# main()
# ---------------------------------------------------------------------------

def main() -> int:
    ap = argparse.ArgumentParser(description="Cost-K auto-calibrator")
    ap.add_argument("--redis-url",
                    default=_env("REDIS_URL", "redis://redis-worker-1:6379/0"))
    ap.add_argument("--redis-main-url",
                    default=_env("REDIS_MAIN_URL", _env("REDIS_URL", "redis://redis:6379/0")))
    ap.add_argument("--db-url",
                    default=_env("DATABASE_URL",
                                 "postgresql://trading:trading@scanner-postgres:5432/scanner_analytics"))
    ap.add_argument("--apply", type=int, default=_env_int("COST_K_CAL_APPLY", 0),
                    help="1 = write to Redis/file; 0 = dry-run")
    ap.add_argument("--shadow-enforce", type=int, default=_env_int("COST_K_CAL_SHADOW_ENFORCE", 0),
                    help="0 = write to shadow key; 1 = write to enforce key")
    ap.add_argument("--lookback-days", type=int, default=_env_int("COST_K_CAL_LOOKBACK_DAYS", 60))
    ap.add_argument("--out-dir", default=_env("COST_K_CAL_OUT_DIR", OUT_DIR))
    ap.add_argument("--state-path", default=_env("COST_K_CAL_STATE_PATH", STATE_PATH))
    ap.add_argument("--notify-stream", default=_env("NOTIFY_STREAM", "notify:telegram"))
    ap.add_argument("--alpha", type=float, default=_env_float("COST_K_CAL_ALPHA", _ALPHA_DEFAULT))
    ap.add_argument("--min-trades-per-group", type=int,
                    default=_env_int("COST_K_MIN_TRADES_PER_GROUP", MIN_TRADES_PER_GROUP))
    ap.add_argument("--cooldown-sec", type=int,
                    default=_env_int("COST_K_COOLDOWN_SEC", COOLDOWN_SEC))
    args = ap.parse_args()

    # --- State / cooldown ---
    state = _load_json_safe(args.state_path)
    now_ms = int(time.time() * 1000)
    last_run_ms = int(state.get("last_run_ms") or 0)
    cooldown_ms = args.cooldown_sec * 1000

    if last_run_ms > 0 and (now_ms - last_run_ms) < cooldown_ms:
        remaining = (cooldown_ms - (now_ms - last_run_ms)) / 1000
        _log(f"Cooldown active — {remaining:.0f}s remaining, skipping")
        return 0

    state["last_run_ms"] = now_ms
    state["pid"] = os.getpid()
    state.setdefault("history", [])

    ts_str = time.strftime("%Y%m%d_%H%M%S", time.gmtime())
    run_id = f"cost_k_cal_v1_{ts_str}_{uuid.uuid4().hex[:6]}"

    _log(f"Starting calibration run_id={run_id} apply={args.apply} shadow_enforce={args.shadow_enforce}")

    # --- Load trades ---
    trades = asyncio.run(load_trades(db_url=args.db_url, lookback_days=args.lookback_days))

    if not trades:
        msg = f"Нет данных trades_closed для Cost-K калибровки (lookback={args.lookback_days}d)"
        _log(msg)
        state["phase"] = "blocked"
        state["block_reason"] = "no_data"
        _atomic_write_json(args.state_path, state)
        notify_telegram(
            args.redis_main_url,
            f"⚠️ <b>Cost-K Calibrator — ошибка</b>\n\n{msg}",
            severity="warn",
            dedup_key="cost_k_cal_no_data",
            notify_stream=args.notify_stream,
        )
        return 1

    # --- Calibration ---
    results = compute_k_fit(trades)

    # Подсчёт "реальных" групп (не агрегированных)
    real_groups = {k: v for k, v in results.items() if "*" not in k[0] and "*" not in k[1]}

    # --- Gates ---
    passed, blockers = check_gates(
        n_trades=len(trades),
        n_groups=len(real_groups),
        blockers=[],
        min_trades_per_group=args.min_trades_per_group,
    )

    # --- Load current calibration ---
    redis_client = None
    current_k: dict[str, float] = {}
    try:
        import redis as redis_lib
        redis_client = redis_lib.Redis.from_url(args.redis_url, decode_responses=True)
        current_k = load_current_calibration(redis_client)
    except Exception as exc:
        _log(f"Redis connect error: {exc}")

    # --- Build payload ---
    payload = build_payload(
        results=results,
        current_k=current_k,
        alpha=args.alpha,
        blockers=blockers,
        gates_passed=passed,
        run_id=run_id,
        n_trades=len(trades),
        apply=bool(args.apply),
        shadow_enforce=args.shadow_enforce,
    )

    # --- History ---
    event = {
        "ts_ms": now_ms,
        "run_id": run_id,
        "n_trades": len(trades),
        "n_real_groups": len(real_groups),
        "gates_passed": passed,
        "blockers": blockers,
        "apply": args.apply,
        "shadow_enforce": args.shadow_enforce,
    }
    state["history"] = (state.get("history") or [])[-49:] + [event]

    if not passed:
        _log(f"Gates FAILED: {blockers}")
        state["phase"] = "blocked"
        state["last_blockers"] = blockers
        _atomic_write_json(args.state_path, state)
        notify_telegram(
            args.redis_main_url,
            _fmt_msg(payload, "blocked"),
            severity="warn",
            dedup_key=f"cost_k_blocked_{ts_str[:8]}",
            notify_stream=args.notify_stream,
        )
        return 0

    out_path = ""
    if args.apply:
        # --- Write file ---
        out_path = write_file(args.out_dir, payload)
        if not out_path:
            state["phase"] = "blocked"
            state["block_reason"] = "file_write_failed"
            _atomic_write_json(args.state_path, state)
            notify_telegram(
                args.redis_main_url,
                "⚠️ <b>Cost-K Calibrator — ошибка записи файла</b>",
                severity="error",
                notify_stream=args.notify_stream,
            )
            return 1

        # --- Write Redis ---
        if redis_client is not None:
            if args.shadow_enforce == 1:
                # enforce mode — пишем в основной ключ
                ok = write_redis(redis_client, CAL_KEY, payload)
                _log(f"Redis enforce write {'OK' if ok else 'FAILED'}: {CAL_KEY}")
                phase_label = "promoted"
            else:
                # shadow mode — пишем в shadow ключ
                ok = write_redis(redis_client, CAL_SHADOW_KEY, payload)
                _log(f"Redis shadow write {'OK' if ok else 'FAILED'}: {CAL_SHADOW_KEY}")
                phase_label = "shadow"
        else:
            _log("Redis client unavailable — skipping Redis write")
            phase_label = "promoted" if args.shadow_enforce == 1 else "shadow"

        state["phase"] = phase_label
        state["last_applied_ms"] = now_ms
        state["last_cal_path"] = out_path
        state["last_run_id"] = run_id
        state["last_blockers"] = []

        notify_telegram(
            args.redis_main_url,
            _fmt_msg(payload, phase_label),
            severity="info",
            dedup_key=f"cost_k_applied_{ts_str}",
            notify_stream=args.notify_stream,
        )
    else:
        _log("Dry-run (--apply=0): no file written, no Redis update")
        state["phase"] = "dry_run"
        state["last_run_id"] = run_id
        state["last_blockers"] = []
        notify_telegram(
            args.redis_main_url,
            _fmt_msg(payload, "dry_run"),
            severity="info",
            dedup_key=f"cost_k_dryrun_{ts_str[:8]}",
            notify_stream=args.notify_stream,
        )

    _atomic_write_json(args.state_path, state)
    _log(f"Done. phase={state.get('phase')} run_id={run_id} out={out_path or 'n/a'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
