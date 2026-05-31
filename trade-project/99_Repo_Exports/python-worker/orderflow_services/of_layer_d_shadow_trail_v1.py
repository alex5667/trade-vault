from __future__ import annotations

"""of_layer_d_shadow_trail_v1.py

Layer-D Shadow — early-arm trailing simulator (counterfactual exit policy).

Проблема:
  По данным trades:closed:
    - 79.9% сделок не достигают TP1 (tp_hits=0)
    - 38% сделок отдают ≥1.5R giveback от MFE
    - текущий trailing armится только при TP1 → слишком поздно

Гипотеза:
  Если armировать trailing при MFE_R ≥ ARM_THRESHOLD_R (default 0.5),
  и сохранять KEEP_FRACTION от достигнутого MFE — экономим часть giveback'а.

Модель counterfactual (conservative):
  mfe_R = mfe_pnl / one_r_money
  if mfe_R >= ARM_THRESHOLD_R:
      cf_pnl = max(pnl_net, mfe_pnl * KEEP_FRACTION - EXIT_FEE_USD)
  else:
      cf_pnl = pnl_net   # нет изменения, MFE недостаточен для arm

Никогда не ухудшает сделку (max() гарантирует floor=pnl_net).
Это **lower-bound оценка** реального эффекта — реальная trailing-policy
может удержать больше, но без тиковой симуляции даём осторожную оценку.

ENV:
  OF_LAYER_D_SHADOW_ENABLE          0
  OF_LAYER_D_SHADOW_REDIS_URL       redis://redis-worker-1:6379/0
  OF_LAYER_D_SHADOW_STREAM          trades:closed
  OF_LAYER_D_SHADOW_SINCE_HOURS     72.0
  OF_LAYER_D_SHADOW_INTERVAL_SEC    300
  OF_LAYER_D_ARM_THRESHOLD_R        0.25    # армить при MFE >= 0.25R
  OF_LAYER_D_KEEP_FRACTION          0.5     # удерживаем 50% от MFE
  OF_LAYER_D_EXIT_FEE_USD           0.5     # доп.fees на exit (round-trip)
  OF_LAYER_D_REPORT_PATH            /var/lib/trade/of_reports/of_layer_d_shadow.json
  OF_LAYER_D_PROM_PORT              9850
  OF_LAYER_D_BATCH                  2000
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
    format="%(asctime)s [of-layer-d-shadow] %(levelname)s %(message)s",
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


@dataclass
class Cfg:
    enable: bool
    redis_url: str
    stream: str
    since_hours: float
    interval_sec: int
    arm_threshold_r: float
    keep_fraction: float
    exit_fee_usd: float
    report_path: str
    prom_port: int
    batch: int


def load_cfg() -> Cfg:
    return Cfg(
        enable          = bool(_env_int("OF_LAYER_D_SHADOW_ENABLE", 0)),
        redis_url       = _env("OF_LAYER_D_SHADOW_REDIS_URL", "redis://redis-worker-1:6379/0"),
        stream          = _env("OF_LAYER_D_SHADOW_STREAM", "trades:closed"),
        since_hours     = _env_float("OF_LAYER_D_SHADOW_SINCE_HOURS", 72.0),
        interval_sec    = _env_int("OF_LAYER_D_SHADOW_INTERVAL_SEC", 300),
        arm_threshold_r = _env_float("OF_LAYER_D_ARM_THRESHOLD_R", 0.25),
        keep_fraction   = _env_float("OF_LAYER_D_KEEP_FRACTION", 0.5),
        exit_fee_usd    = _env_float("OF_LAYER_D_EXIT_FEE_USD", 0.5),
        report_path     = _env("OF_LAYER_D_REPORT_PATH",
                               "/var/lib/trade/of_reports/of_layer_d_shadow.json"),
        prom_port       = _env_int("OF_LAYER_D_PROM_PORT", 9850),
        batch           = _env_int("OF_LAYER_D_BATCH", 2000),
    )


# Prometheus
g_up           = Gauge("of_layer_d_shadow_up", "service loop up")
g_last_run     = Gauge("of_layer_d_shadow_last_run_ts", "last run unix ts")
g_total        = Gauge("of_layer_d_shadow_trades_total", "trades evaluated")
g_armable      = Gauge("of_layer_d_shadow_armable_total",
                       "trades with mfe_R >= arm_threshold")
g_arm_rate     = Gauge("of_layer_d_shadow_armable_rate", "armable rate [0..1]")
g_improved     = Gauge("of_layer_d_shadow_improved_total",
                       "trades with counterfactual pnl > baseline")
g_baseline_pnl = Gauge("of_layer_d_shadow_baseline_total_pnl_usd", "baseline sum pnl_net")
g_cf_pnl       = Gauge("of_layer_d_shadow_counterfactual_total_pnl_usd",
                       "counterfactual sum pnl_net")
g_uplift       = Gauge("of_layer_d_shadow_pnl_uplift_usd",
                       "counterfactual - baseline (positive = improvement)")
g_baseline_wr  = Gauge("of_layer_d_shadow_baseline_winrate", "baseline winrate")
g_cf_wr        = Gauge("of_layer_d_shadow_counterfactual_winrate", "counterfactual winrate")
g_losers_flip  = Gauge("of_layer_d_shadow_losers_flipped",
                       "losers that became winners")
g_giveback_avg_base   = Gauge("of_layer_d_shadow_giveback_r_avg_baseline",
                              "avg giveback R baseline")
g_giveback_avg_cf     = Gauge("of_layer_d_shadow_giveback_r_avg_counterfactual",
                              "avg giveback R counterfactual")
g_mfe_bucket   = Gauge("of_layer_d_shadow_by_mfe_bucket",
                       "trades by mfe_R bucket", ["bucket", "metric"])
c_errors       = PCounter("of_layer_d_shadow_errors_total", "errors", ["where"])


def _f(v: Any, d: float = 0.0) -> float:
    try:
        x = float(v)
        return x if math.isfinite(x) else d
    except Exception:
        return d


def _now_ms() -> int:
    return int(time.time() * 1000)


def _mfe_bucket(mfe_r: float) -> str:
    if mfe_r < 0.0:    return "neg"
    if mfe_r < 0.25:   return "[0,0.25)"
    if mfe_r < 0.5:    return "[0.25,0.5)"
    if mfe_r < 1.0:    return "[0.5,1.0)"
    if mfe_r < 2.0:    return "[1.0,2.0)"
    return "[2.0+)"


@dataclass
class TradeEval:
    sid: str
    symbol: str
    direction: str
    pnl_net: float
    mfe_pnl: float
    one_r_money: float
    mfe_r: float
    giveback_r: float
    armable: bool
    counterfactual_pnl: float
    improved: bool
    cf_giveback_r: float  # giveback в counterfactual: max(0, mfe_pnl - cf_pnl) / one_r_money


def _evaluate(d: dict[str, Any], cfg: Cfg) -> TradeEval | None:
    pnl_net     = _f(d.get("pnl_net") or d.get("pnl"), 0.0)
    mfe_pnl     = _f(d.get("mfe_pnl"), 0.0)
    one_r       = _f(d.get("one_r_money"), 0.0)
    giveback_r  = _f(d.get("giveback"), 0.0)
    direction   = str(d.get("direction") or d.get("side") or "").upper()
    sid         = str(d.get("sid") or d.get("signal_id") or "")
    symbol      = str(d.get("symbol") or "?")

    if one_r <= 0:
        # без 1R-scale не можем считать R-multiple
        return None

    mfe_r = mfe_pnl / one_r if one_r > 0 else 0.0

    armable = mfe_r >= cfg.arm_threshold_r
    if armable:
        # удержать KEEP_FRACTION от достигнутого MFE минус доп. fees
        cf_candidate = mfe_pnl * cfg.keep_fraction - cfg.exit_fee_usd
        # никогда не ухудшаем сделку
        cf_pnl = max(pnl_net, cf_candidate)
    else:
        cf_pnl = pnl_net

    improved = cf_pnl > pnl_net + 1e-9
    cf_gb_pnl = max(0.0, mfe_pnl - cf_pnl)
    cf_gb_r = cf_gb_pnl / one_r if one_r > 0 else 0.0

    return TradeEval(
        sid=sid, symbol=symbol, direction=direction or "?",
        pnl_net=pnl_net, mfe_pnl=mfe_pnl, one_r_money=one_r,
        mfe_r=mfe_r, giveback_r=giveback_r,
        armable=armable, counterfactual_pnl=cf_pnl,
        improved=improved, cf_giveback_r=cf_gb_r,
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
    params: dict[str, float]
    total: int = 0
    skipped_no_1r: int = 0
    armable: int = 0
    improved: int = 0
    losers_flipped: int = 0
    baseline_wins: int = 0
    baseline_losses: int = 0
    baseline_pnl: float = 0.0
    cf_wins: int = 0
    cf_losses: int = 0
    cf_pnl: float = 0.0
    pnl_uplift: float = 0.0
    giveback_r_sum_base: float = 0.0
    giveback_r_sum_cf: float = 0.0
    by_mfe_bucket: dict[str, dict[str, float]] = field(default_factory=dict)


def _summarise(evals: list[TradeEval], skipped: int, cfg: Cfg) -> Summary:
    s = Summary(
        ts_ms=_now_ms(),
        since_hours=cfg.since_hours,
        params={
            "arm_threshold_r": cfg.arm_threshold_r,
            "keep_fraction":   cfg.keep_fraction,
            "exit_fee_usd":    cfg.exit_fee_usd,
        },
        skipped_no_1r=skipped,
    )
    by_bucket_n:   Counter[str] = Counter()
    by_bucket_arm: Counter[str] = Counter()
    by_bucket_base_pnl: defaultdict[str, float] = defaultdict(float)
    by_bucket_cf_pnl:   defaultdict[str, float] = defaultdict(float)

    for e in evals:
        s.total += 1
        s.baseline_pnl += e.pnl_net
        s.cf_pnl       += e.counterfactual_pnl
        s.giveback_r_sum_base += e.giveback_r
        s.giveback_r_sum_cf   += e.cf_giveback_r
        if e.pnl_net > 0: s.baseline_wins += 1
        elif e.pnl_net < 0: s.baseline_losses += 1
        if e.counterfactual_pnl > 0: s.cf_wins += 1
        elif e.counterfactual_pnl < 0: s.cf_losses += 1
        if e.improved:
            s.improved += 1
            if e.pnl_net < 0 and e.counterfactual_pnl > 0:
                s.losers_flipped += 1
        if e.armable:
            s.armable += 1
        bkt = _mfe_bucket(e.mfe_r)
        by_bucket_n[bkt] += 1
        if e.armable: by_bucket_arm[bkt] += 1
        by_bucket_base_pnl[bkt] += e.pnl_net
        by_bucket_cf_pnl[bkt]   += e.counterfactual_pnl

    s.pnl_uplift = s.cf_pnl - s.baseline_pnl
    s.by_mfe_bucket = {
        b: {
            "n":          by_bucket_n[b],
            "armable":    by_bucket_arm[b],
            "base_pnl":   by_bucket_base_pnl[b],
            "cf_pnl":     by_bucket_cf_pnl[b],
            "uplift":     by_bucket_cf_pnl[b] - by_bucket_base_pnl[b],
        }
        for b in by_bucket_n
    }
    return s


def _emit_prom(s: Summary) -> None:
    g_last_run.set(s.ts_ms / 1000.0)
    g_total.set(s.total)
    g_armable.set(s.armable)
    g_arm_rate.set((s.armable / s.total) if s.total else 0.0)
    g_improved.set(s.improved)
    g_losers_flip.set(s.losers_flipped)
    g_baseline_pnl.set(s.baseline_pnl)
    g_cf_pnl.set(s.cf_pnl)
    g_uplift.set(s.pnl_uplift)
    base_dec = s.baseline_wins + s.baseline_losses
    cf_dec   = s.cf_wins + s.cf_losses
    g_baseline_wr.set((s.baseline_wins / base_dec) if base_dec else 0.0)
    g_cf_wr.set((s.cf_wins / cf_dec) if cf_dec else 0.0)
    g_giveback_avg_base.set((s.giveback_r_sum_base / s.total) if s.total else 0.0)
    g_giveback_avg_cf.set((s.giveback_r_sum_cf / s.total) if s.total else 0.0)
    for bkt, st in s.by_mfe_bucket.items():
        g_mfe_bucket.labels(bucket=bkt, metric="n").set(st["n"])
        g_mfe_bucket.labels(bucket=bkt, metric="armable").set(st["armable"])
        g_mfe_bucket.labels(bucket=bkt, metric="uplift_usd").set(st["uplift"])


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
    evals: list[TradeEval] = []
    skipped = 0
    for t in trades:
        e = _evaluate(t, cfg)
        if e is None:
            skipped += 1
        else:
            evals.append(e)
    s = _summarise(evals, skipped, cfg)
    _emit_prom(s)
    _save_report(cfg.report_path, s)
    base_dec = s.baseline_wins + s.baseline_losses
    cf_dec   = s.cf_wins + s.cf_losses
    log.info(
        "run: n=%d (skip_no_1r=%d) armable=%d (%.1f%%) improved=%d losers_flipped=%d "
        "pnl: base=%+.2f → cf=%+.2f  uplift=%+.2f  "
        "wr: base=%.1f%% → cf=%.1f%%  giveback_R: base=%.3f → cf=%.3f",
        s.total, s.skipped_no_1r, s.armable,
        (s.armable / s.total * 100.0) if s.total else 0.0,
        s.improved, s.losers_flipped,
        s.baseline_pnl, s.cf_pnl, s.pnl_uplift,
        (s.baseline_wins / base_dec * 100.0) if base_dec else 0.0,
        (s.cf_wins / cf_dec * 100.0) if cf_dec else 0.0,
        (s.giveback_r_sum_base / s.total) if s.total else 0.0,
        (s.giveback_r_sum_cf / s.total) if s.total else 0.0,
    )
    for bkt in ("neg","[0,0.25)","[0.25,0.5)","[0.5,1.0)","[1.0,2.0)","[2.0+)"):
        st = s.by_mfe_bucket.get(bkt)
        if not st:
            continue
        log.info("  mfe_R %-12s n=%d arm=%d  uplift=%+.2f",
                 bkt, int(st["n"]), int(st["armable"]), st["uplift"])
    return s


def main() -> int:
    cfg = load_cfg()
    if not cfg.enable:
        log.info("OF_LAYER_D_SHADOW_ENABLE=0 — exit (shadow disabled)")
        return 0
    log.info(
        "starting: redis=%s stream=%s since=%.1fh interval=%ds "
        "arm@%.2fR keep=%.2f exit_fee=%.2f",
        cfg.redis_url, cfg.stream, cfg.since_hours, cfg.interval_sec,
        cfg.arm_threshold_r, cfg.keep_fraction, cfg.exit_fee_usd,
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
