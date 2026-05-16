from __future__ import annotations

"""of_layer_b_shadow_gate_v1.py

Layer-B Shadow Gate — soft-veto / size-clamp counterfactual (72h shadow).

Гипотеза:
  Не veto, а clamp lots для bucket'ов с пониженным edge:
    1) slippage_bps_est ∈ [LO_SLIP, HI_SLIP)  → ×SLIP_CLAMP   (default 0.5)
    2) spread_bps_at_entry ∈ [LO_SPR, HI_SPR) → ×SPR_CLAMP    (default 0.5)
    3) LONG без HTF-confirmation (regime ∉ CONFIRM_LONG)
                                              → ×LONG_CLAMP   (default 0.7)

Композиция: мультипликативная, с floor MIN_CLAMP (default 0.2).

Что делает (НИЧЕГО не блокирует, НИЧЕГО не уменьшает реально):
  - Читает trades:closed за SINCE_HOURS.
  - Для каждого трейда считает composite clamp_factor.
  - Counterfactual PnL = pnl_net * clamp_factor.
  - Saved_pnl = sum(pnl_net * (1 - clamp_factor)).
  - Пишет JSON-отчёт + Prometheus.

Запуск одним env-флагом OF_LAYER_B_SHADOW_ENABLE=1.

ENV:
  OF_LAYER_B_SHADOW_ENABLE          0
  OF_LAYER_B_SHADOW_REDIS_URL       redis://redis-worker-1:6379/0
  OF_LAYER_B_SHADOW_STREAM          trades:closed
  OF_LAYER_B_SHADOW_SINCE_HOURS     72.0
  OF_LAYER_B_SHADOW_INTERVAL_SEC    300

  OF_LAYER_B_SLIP_LO                1.0
  OF_LAYER_B_SLIP_HI                2.0
  OF_LAYER_B_SLIP_CLAMP             0.5

  OF_LAYER_B_SPR_LO                 0.8
  OF_LAYER_B_SPR_HI                 1.5
  OF_LAYER_B_SPR_CLAMP              0.5

  OF_LAYER_B_LONG_REGIME_FIELD      regime
  OF_LAYER_B_LONG_CONFIRM_VALUES    uptrend,trend_up
  OF_LAYER_B_LONG_CLAMP             0.7
  OF_LAYER_B_SYMMETRY_SHORT_ENABLE  0
  OF_LAYER_B_SHORT_CONFIRM_VALUES   downtrend,trend_down
  OF_LAYER_B_SHORT_CLAMP            0.7

  OF_LAYER_B_MIN_CLAMP              0.2
  OF_LAYER_B_REPORT_PATH            /var/lib/trade/of_reports/of_layer_b_shadow.json
  OF_LAYER_B_PROM_PORT              9848
  OF_LAYER_B_BATCH                  2000
"""

import json
import logging
import math
import os
import time
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

import redis
from prometheus_client import Counter as PCounter, Gauge, start_http_server  # type: ignore

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [of-layer-b-shadow] %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)


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


def _env_csv(k: str, d: str) -> tuple[str, ...]:
    raw = _env(k, d)
    return tuple(s.strip().lower() for s in raw.split(",") if s.strip())


@dataclass
class Cfg:
    enable: bool
    redis_url: str
    stream: str
    since_hours: float
    interval_sec: int

    slip_lo: float
    slip_hi: float
    slip_clamp: float

    spr_lo: float
    spr_hi: float
    spr_clamp: float

    long_regime_field: str
    long_confirm_values: tuple[str, ...]
    long_clamp: float
    symmetry_short: bool
    short_confirm_values: tuple[str, ...]
    short_clamp: float

    min_clamp: float
    report_path: str
    prom_port: int
    batch: int


def load_cfg() -> Cfg:
    return Cfg(
        enable       = bool(_env_int("OF_LAYER_B_SHADOW_ENABLE", 0)),
        redis_url    = _env("OF_LAYER_B_SHADOW_REDIS_URL", "redis://redis-worker-1:6379/0"),
        stream       = _env("OF_LAYER_B_SHADOW_STREAM", "trades:closed"),
        since_hours  = _env_float("OF_LAYER_B_SHADOW_SINCE_HOURS", 72.0),
        interval_sec = _env_int("OF_LAYER_B_SHADOW_INTERVAL_SEC", 300),

        slip_lo    = _env_float("OF_LAYER_B_SLIP_LO", 1.0),
        slip_hi    = _env_float("OF_LAYER_B_SLIP_HI", 2.0),
        slip_clamp = _env_float("OF_LAYER_B_SLIP_CLAMP", 0.5),

        spr_lo    = _env_float("OF_LAYER_B_SPR_LO", 0.8),
        spr_hi    = _env_float("OF_LAYER_B_SPR_HI", 1.5),
        spr_clamp = _env_float("OF_LAYER_B_SPR_CLAMP", 0.5),

        long_regime_field    = _env("OF_LAYER_B_LONG_REGIME_FIELD", "regime"),
        long_confirm_values  = _env_csv("OF_LAYER_B_LONG_CONFIRM_VALUES", "uptrend,trend_up"),
        long_clamp           = _env_float("OF_LAYER_B_LONG_CLAMP", 0.7),
        symmetry_short       = bool(_env_int("OF_LAYER_B_SYMMETRY_SHORT_ENABLE", 0)),
        short_confirm_values = _env_csv("OF_LAYER_B_SHORT_CONFIRM_VALUES", "downtrend,trend_down"),
        short_clamp          = _env_float("OF_LAYER_B_SHORT_CLAMP", 0.7),

        min_clamp    = _env_float("OF_LAYER_B_MIN_CLAMP", 0.2),
        report_path  = _env("OF_LAYER_B_REPORT_PATH",
                            "/var/lib/trade/of_reports/of_layer_b_shadow.json"),
        prom_port    = _env_int("OF_LAYER_B_PROM_PORT", 9848),
        batch        = _env_int("OF_LAYER_B_BATCH", 2000),
    )


# Prometheus
g_up           = Gauge("of_layer_b_shadow_up", "service loop up")
g_last_run     = Gauge("of_layer_b_shadow_last_run_ts", "last run unix ts")
g_total        = Gauge("of_layer_b_shadow_trades_total", "total trades evaluated")
g_clamped      = Gauge("of_layer_b_shadow_clamped_total", "trades with clamp<1")
g_clamp_rate   = Gauge("of_layer_b_shadow_clamp_rate", "clamped rate [0..1]")
g_clamp_by     = Gauge("of_layer_b_shadow_clamp_by_reason",
                       "clamp count by reason", ["reason"])
g_factor_mean  = Gauge("of_layer_b_shadow_factor_mean", "mean clamp factor over all trades")
g_factor_clamped_mean = Gauge("of_layer_b_shadow_factor_clamped_mean",
                              "mean clamp factor over only clamped trades")
g_pnl_saved    = Gauge("of_layer_b_shadow_pnl_saved_usd",
                       "sum -(pnl_net * (1 - factor)) over all trades")
g_pnl_winners_lost = Gauge("of_layer_b_shadow_winners_pnl_lost_usd",
                           "sum (pnl_net * (1 - factor)) over winners only (>0)")
g_baseline_total_pnl = Gauge("of_layer_b_shadow_baseline_total_pnl_usd",
                             "baseline sum pnl_net")
g_counterfactual_total_pnl = Gauge("of_layer_b_shadow_counterfactual_total_pnl_usd",
                                   "sum pnl_net*factor")
g_baseline_avg = Gauge("of_layer_b_shadow_baseline_avg_pnl", "baseline avg pnl_net")
g_counter_avg  = Gauge("of_layer_b_shadow_counterfactual_avg_pnl",
                       "counterfactual avg pnl_net*factor")
c_errors       = PCounter("of_layer_b_shadow_errors_total", "errors", ["where"])


def _f(v: Any, d: float = 0.0) -> float:
    try:
        x = float(v)
        return x if math.isfinite(x) else d
    except Exception:
        return d


def _now_ms() -> int:
    return int(time.time() * 1000)


@dataclass
class TradeEval:
    sid: str
    symbol: str
    direction: str
    pnl_net: float
    slippage_bps: float
    spread_bps: float
    regime: str
    factor: float
    reasons: tuple[str, ...]


def _compose(factors: list[float], floor: float) -> float:
    f = 1.0
    for x in factors:
        f *= x
    return max(floor, f)


def _evaluate(d: dict[str, Any], cfg: Cfg) -> TradeEval:
    pnl  = _f(d.get("pnl_net") or d.get("pnl"), 0.0)
    slip = _f(d.get("slippage_bps_est"), 0.0)
    spr  = _f(d.get("spread_bps_at_entry"), 0.0)
    direction = str(d.get("direction") or d.get("side") or "").upper()
    regime    = str(d.get(cfg.long_regime_field) or "").lower()

    factors: list[float] = []
    reasons: list[str] = []

    if cfg.slip_lo <= slip < cfg.slip_hi:
        factors.append(cfg.slip_clamp)
        reasons.append("slippage_mid")

    if cfg.spr_lo <= spr < cfg.spr_hi:
        factors.append(cfg.spr_clamp)
        reasons.append("spread_mid")

    if direction == "LONG" and regime not in cfg.long_confirm_values:
        factors.append(cfg.long_clamp)
        reasons.append("long_no_htf_confirm")

    if cfg.symmetry_short and direction == "SHORT" and regime not in cfg.short_confirm_values:
        factors.append(cfg.short_clamp)
        reasons.append("short_no_htf_confirm")

    factor = _compose(factors, cfg.min_clamp) if factors else 1.0

    return TradeEval(
        sid          = str(d.get("sid") or d.get("signal_id") or ""),
        symbol       = str(d.get("symbol") or "?"),
        direction    = direction or "?",
        pnl_net      = pnl,
        slippage_bps = slip,
        spread_bps   = spr,
        regime       = regime or "na",
        factor       = factor,
        reasons      = tuple(reasons),
    )


def _read_trades(r: redis.Redis, cfg: Cfg) -> list[dict[str, Any]]:
    since_ms = _now_ms() - int(cfg.since_hours * 3_600_000)
    cur = f"{since_ms}-0"
    out: list[dict[str, Any]] = []
    first = True
    while True:
        min_id = cur if first else f"({cur}"
        first = False
        try:
            rows_resp = r.xrange(cfg.stream, min=min_id, max="+", count=cfg.batch)
            rows: list[Any] = list(rows_resp)  # type: ignore[arg-type]
        except Exception as e:
            log.warning(f"xrange {cfg.stream}: {e}")
            c_errors.labels(where="xrange").inc()
            break
        if not rows:
            break
        for sid_stream, fields in rows:
            cur = str(sid_stream)
            raw = fields.get("payload") if isinstance(fields, dict) else None
            rec: Any = None
            if raw:
                try:
                    rec = json.loads(raw)
                except Exception:
                    rec = None
            if not isinstance(rec, dict):
                rec = fields
            if isinstance(rec, dict):
                out.append(rec)
        if len(rows) < cfg.batch:
            break
    return out


@dataclass
class Summary:
    ts_ms: int
    since_hours: float
    thresholds: dict[str, float]
    total: int = 0
    clamped_total: int = 0
    clamp_by_reason: dict[str, int] = field(default_factory=dict)
    factor_sum: float = 0.0
    factor_clamped_sum: float = 0.0
    baseline_total_pnl: float = 0.0
    counterfactual_total_pnl: float = 0.0
    pnl_saved: float = 0.0
    winners_pnl_lost: float = 0.0
    per_reason_breakdown: dict[str, dict[str, float]] = field(default_factory=dict)


def _summarise(evals: list[TradeEval], cfg: Cfg) -> Summary:
    s = Summary(
        ts_ms=_now_ms(),
        since_hours=cfg.since_hours,
        thresholds={
            "slip_lo": cfg.slip_lo, "slip_hi": cfg.slip_hi, "slip_clamp": cfg.slip_clamp,
            "spr_lo": cfg.spr_lo, "spr_hi": cfg.spr_hi, "spr_clamp": cfg.spr_clamp,
            "long_clamp": cfg.long_clamp,
            "short_clamp": cfg.short_clamp if cfg.symmetry_short else 1.0,
            "min_clamp": cfg.min_clamp,
        },
    )
    by_reason_n: Counter[str] = Counter()
    by_reason_pnl_base: defaultdict[str, float] = defaultdict(float)
    by_reason_pnl_saved: defaultdict[str, float] = defaultdict(float)
    by_reason_wins: defaultdict[str, int] = defaultdict(int)

    for e in evals:
        s.total += 1
        s.factor_sum += e.factor
        s.baseline_total_pnl += e.pnl_net
        cf_pnl = e.pnl_net * e.factor
        s.counterfactual_total_pnl += cf_pnl
        delta = e.pnl_net - cf_pnl  # >0 если pnl_net>0 и clamp снизил (потеря winner)
                                    # <0 если pnl_net<0 и clamp снизил (saved loss)
        if e.factor < 1.0:
            s.clamped_total += 1
            s.factor_clamped_sum += e.factor
            # pnl_saved определяем как: -(pnl_net*(1-f)) при loss → положительный saving;
            #                            -(pnl_net*(1-f)) при win → отрицательный (мы «потеряли» прибыль)
            s.pnl_saved += -delta if e.pnl_net < 0 else -delta  # = -delta всегда
            if e.pnl_net > 0:
                s.winners_pnl_lost += delta  # положительная сумма «упущенной прибыли winners»
            for rs in e.reasons:
                by_reason_n[rs] += 1
                by_reason_pnl_base[rs] += e.pnl_net
                by_reason_pnl_saved[rs] += -delta
                if e.pnl_net > 0:
                    by_reason_wins[rs] += 1

    s.clamp_by_reason = dict(by_reason_n)
    s.per_reason_breakdown = {
        k: {
            "n": by_reason_n[k],
            "wins": by_reason_wins[k],
            "winrate_pct": (by_reason_wins[k] / by_reason_n[k] * 100.0)
                           if by_reason_n[k] else 0.0,
            "baseline_pnl_sum": by_reason_pnl_base[k],
            "baseline_avg_pnl": (by_reason_pnl_base[k] / by_reason_n[k])
                                 if by_reason_n[k] else 0.0,
            "pnl_saved": by_reason_pnl_saved[k],
        }
        for k in by_reason_n
    }
    return s


def _emit_prom(s: Summary) -> None:
    g_last_run.set(s.ts_ms / 1000.0)
    g_total.set(s.total)
    g_clamped.set(s.clamped_total)
    g_clamp_rate.set((s.clamped_total / s.total) if s.total else 0.0)
    g_factor_mean.set((s.factor_sum / s.total) if s.total else 1.0)
    g_factor_clamped_mean.set((s.factor_clamped_sum / s.clamped_total)
                              if s.clamped_total else 1.0)
    g_pnl_saved.set(s.pnl_saved)
    g_pnl_winners_lost.set(s.winners_pnl_lost)
    g_baseline_total_pnl.set(s.baseline_total_pnl)
    g_counterfactual_total_pnl.set(s.counterfactual_total_pnl)
    g_baseline_avg.set((s.baseline_total_pnl / s.total) if s.total else 0.0)
    g_counter_avg.set((s.counterfactual_total_pnl / s.total) if s.total else 0.0)
    for reason, cnt in s.clamp_by_reason.items():
        g_clamp_by.labels(reason=reason).set(cnt)


def _save_report(path: str, s: Summary) -> None:
    try:
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(asdict(s), f, indent=2, sort_keys=True)
        os.replace(tmp, path)
    except Exception as e:
        log.warning(f"report save failed: {e}")
        c_errors.labels(where="save_report").inc()


def _run_once(r: redis.Redis, cfg: Cfg) -> Summary:
    trades = _read_trades(r, cfg)
    evals = [_evaluate(t, cfg) for t in trades]
    s = _summarise(evals, cfg)
    _emit_prom(s)
    _save_report(cfg.report_path, s)
    log.info(
        "run: n=%d clamped=%d (%.1f%%) factor_mean=%.3f "
        "baseline_pnl=%+.2f → counter_pnl=%+.2f saved=%+.2f winners_lost=%+.2f",
        s.total, s.clamped_total,
        (s.clamped_total / s.total * 100.0) if s.total else 0.0,
        (s.factor_sum / s.total) if s.total else 1.0,
        s.baseline_total_pnl, s.counterfactual_total_pnl,
        s.pnl_saved, s.winners_pnl_lost,
    )
    for reason, br in s.per_reason_breakdown.items():
        log.info("  reason=%-22s n=%d wr=%.1f%% base_avg=%+.4f saved=%+.2f",
                 reason, int(br["n"]), br["winrate_pct"],
                 br["baseline_avg_pnl"], br["pnl_saved"])
    return s


def main() -> int:
    cfg = load_cfg()
    if not cfg.enable:
        log.info("OF_LAYER_B_SHADOW_ENABLE=0 — exit (shadow disabled)")
        return 0

    log.info(
        "starting: redis=%s stream=%s since=%.1fh interval=%ds",
        cfg.redis_url, cfg.stream, cfg.since_hours, cfg.interval_sec,
    )
    log.info(
        "rules: slip[%.2f,%.2f)->×%.2f  spread[%.2f,%.2f)->×%.2f  "
        "LONG_no_htf->×%.2f (confirm via %s ∈ %s)  symmetry_short=%s  min_clamp=%.2f",
        cfg.slip_lo, cfg.slip_hi, cfg.slip_clamp,
        cfg.spr_lo, cfg.spr_hi, cfg.spr_clamp,
        cfg.long_clamp, cfg.long_regime_field, cfg.long_confirm_values,
        cfg.symmetry_short, cfg.min_clamp,
    )
    try:
        start_http_server(cfg.prom_port)
        log.info(f"prometheus on :{cfg.prom_port}")
    except Exception as e:
        log.warning(f"prometheus start failed: {e}")
        c_errors.labels(where="prom_start").inc()

    g_up.set(1)
    r = redis.from_url(cfg.redis_url, decode_responses=True)

    while True:
        t0 = time.time()
        try:
            _run_once(r, cfg)
        except Exception as e:
            log.exception(f"run_once failed: {e}")
            c_errors.labels(where="run_once").inc()
        dt = time.time() - t0
        sleep_s = max(1, cfg.interval_sec - int(dt))
        time.sleep(sleep_s)


if __name__ == "__main__":
    raise SystemExit(main())
