from __future__ import annotations

"""of_layer_a_shadow_gate_v1.py

Layer-A Shadow Gate — counterfactual evaluator (24h shadow).

Гипотеза (см. trades-closed анализ 2026-05-15, n=5000):
  Hard pre-entry veto по трём независимым признакам уберёт сильно
  убыточный хвост, потеряв минимум winners:
    1) adverse_bps@100ms >= ADVERSE_BPS  (default 15.0)
    2) slippage_bps_est  >= SLIPPAGE_BPS (default 2.0)
    3) spread_bps_at_entry >= SPREAD_BPS (default 1.5)

Что делает (НИЧЕГО не блокирует):
  - Каждые INTERVAL_SEC читает trades:closed за SINCE_HOURS.
  - На уже закрытых сделках применяет counterfactual Layer-A.
  - Считает veto rate, residual winrate, PnL saved, winners lost.
  - Пишет JSON-отчёт + Prometheus metrics.

Запускается одним env-флагом OF_LAYER_A_SHADOW_ENABLE=1.

ENV:
  OF_LAYER_A_SHADOW_ENABLE          0 (главный переключатель; 1=run)
  OF_LAYER_A_SHADOW_REDIS_URL       redis://redis-worker-1:6379/0
  OF_LAYER_A_SHADOW_STREAM          trades:closed
  OF_LAYER_A_SHADOW_SINCE_HOURS     24.0
  OF_LAYER_A_SHADOW_INTERVAL_SEC    300
  OF_LAYER_A_SHADOW_ADVERSE_BPS     15.0
  OF_LAYER_A_SHADOW_SLIPPAGE_BPS    2.0
  OF_LAYER_A_SHADOW_SPREAD_BPS      1.5
  OF_LAYER_A_SHADOW_ADVERSE_KEY     100   (ключ в adverse_bps_t)
  OF_LAYER_A_SHADOW_REPORT_PATH     /var/lib/trade/of_reports/of_layer_a_shadow.json
  OF_LAYER_A_SHADOW_PROM_PORT       9847
  OF_LAYER_A_SHADOW_BATCH           2000
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
    format="%(asctime)s [of-layer-a-shadow] %(levelname)s %(message)s",
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
    adverse_bps: float
    slippage_bps: float
    spread_bps: float
    adverse_key: str
    report_path: str
    prom_port: int
    batch: int


def load_cfg() -> Cfg:
    return Cfg(
        enable       = bool(_env_int("OF_LAYER_A_SHADOW_ENABLE", 0)),
        redis_url    = _env("OF_LAYER_A_SHADOW_REDIS_URL", "redis://redis-worker-1:6379/0"),
        stream       = _env("OF_LAYER_A_SHADOW_STREAM", "trades:closed"),
        since_hours  = _env_float("OF_LAYER_A_SHADOW_SINCE_HOURS", 24.0),
        interval_sec = _env_int("OF_LAYER_A_SHADOW_INTERVAL_SEC", 300),
        adverse_bps  = _env_float("OF_LAYER_A_SHADOW_ADVERSE_BPS", 15.0),
        slippage_bps = _env_float("OF_LAYER_A_SHADOW_SLIPPAGE_BPS", 2.0),
        spread_bps   = _env_float("OF_LAYER_A_SHADOW_SPREAD_BPS", 1.5),
        adverse_key  = _env("OF_LAYER_A_SHADOW_ADVERSE_KEY", "100"),
        report_path  = _env("OF_LAYER_A_SHADOW_REPORT_PATH",
                            "/var/lib/trade/of_reports/of_layer_a_shadow.json"),
        prom_port    = _env_int("OF_LAYER_A_SHADOW_PROM_PORT", 9847),
        batch        = _env_int("OF_LAYER_A_SHADOW_BATCH", 2000),
    )


# Prometheus
g_up         = Gauge("of_layer_a_shadow_up", "service loop up")
g_last_run   = Gauge("of_layer_a_shadow_last_run_ts", "last run unix ts")
g_total      = Gauge("of_layer_a_shadow_trades_total", "total trades evaluated")
g_veto       = Gauge("of_layer_a_shadow_veto_total", "trades that would be vetoed")
g_veto_rate  = Gauge("of_layer_a_shadow_veto_rate", "veto rate [0..1]")
g_veto_by    = Gauge("of_layer_a_shadow_veto_by_reason", "veto count by reason", ["reason"])
g_pnl_saved  = Gauge("of_layer_a_shadow_pnl_saved_usd",
                     "sum -pnl_net over vetoed trades (positive = saved)")
g_winners_lost  = Gauge("of_layer_a_shadow_winners_lost",
                        "winners count inside vetoed set")
g_winners_pnl   = Gauge("of_layer_a_shadow_winners_pnl_lost_usd",
                        "sum pnl_net of winners inside vetoed set")
g_resid_wr      = Gauge("of_layer_a_shadow_residual_winrate",
                        "winrate after counterfactual filter")
g_resid_avg     = Gauge("of_layer_a_shadow_residual_avg_pnl",
                        "avg pnl after counterfactual filter")
g_baseline_wr   = Gauge("of_layer_a_shadow_baseline_winrate", "baseline winrate")
g_baseline_avg  = Gauge("of_layer_a_shadow_baseline_avg_pnl", "baseline avg pnl")
c_errors        = PCounter("of_layer_a_shadow_errors_total", "errors", ["where"])


def _f(v: Any, d: float = 0.0) -> float:
    try:
        x = float(v)
        return x if math.isfinite(x) else d
    except Exception:
        return d


def _now_ms() -> int:
    return int(time.time() * 1000)


def _parse_adverse(features_raw: Any, key: str) -> float | None:
    """features.adverse_bps_t[key] из json-строки."""
    if not features_raw:
        return None
    try:
        j = features_raw if isinstance(features_raw, dict) else json.loads(features_raw)
    except Exception:
        return None
    adv = j.get("adverse_bps_t") if isinstance(j, dict) else None
    if not isinstance(adv, dict):
        return None
    if key in adv:
        v = _f(adv.get(key), None)  # type: ignore[arg-type]
        return v if v is not None and math.isfinite(v) else None
    return None


@dataclass
class TradeEval:
    sid: str
    symbol: str
    direction: str
    pnl_net: float
    adverse_100: float | None
    slippage_bps: float
    spread_bps: float
    veto: bool
    reasons: tuple[str, ...]


def _evaluate(d: dict[str, Any], cfg: Cfg) -> TradeEval:
    pnl = _f(d.get("pnl_net") or d.get("pnl"), 0.0)
    slip = _f(d.get("slippage_bps_est"), 0.0)
    spr  = _f(d.get("spread_bps_at_entry"), 0.0)
    adv  = _parse_adverse(d.get("features"), cfg.adverse_key)

    reasons: list[str] = []
    if adv is not None and adv >= cfg.adverse_bps:
        reasons.append("adverse_microspike")
    if slip >= cfg.slippage_bps:
        reasons.append("slippage")
    if spr >= cfg.spread_bps:
        reasons.append("spread")

    return TradeEval(
        sid          = str(d.get("sid") or d.get("signal_id") or ""),
        symbol       = str(d.get("symbol") or "?"),
        direction    = str(d.get("direction") or d.get("side") or "?").upper(),
        pnl_net      = pnl,
        adverse_100  = adv,
        slippage_bps = slip,
        spread_bps   = spr,
        veto         = bool(reasons),
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
    baseline_wins: int = 0
    baseline_losses: int = 0
    baseline_pnl: float = 0.0
    veto_total: int = 0
    veto_by_reason: dict[str, int] = field(default_factory=dict)
    veto_pnl_sum: float = 0.0       # сумма pnl_net у вето-сделок (отрицательная => экономия)
    winners_in_veto: int = 0
    winners_in_veto_pnl: float = 0.0
    residual_total: int = 0
    residual_wins: int = 0
    residual_losses: int = 0
    residual_pnl: float = 0.0
    per_reason_breakdown: dict[str, dict[str, float]] = field(default_factory=dict)


def _summarise(evals: list[TradeEval], cfg: Cfg) -> Summary:
    s = Summary(
        ts_ms=_now_ms(),
        since_hours=cfg.since_hours,
        thresholds={
            "adverse_bps":  cfg.adverse_bps,
            "slippage_bps": cfg.slippage_bps,
            "spread_bps":   cfg.spread_bps,
            "adverse_key_ms": _f(cfg.adverse_key, 100.0),
        },
    )
    by_reason_cnt: Counter[str] = Counter()
    by_reason_pnl: defaultdict[str, float] = defaultdict(float)
    by_reason_wins: defaultdict[str, int] = defaultdict(int)

    for e in evals:
        s.total += 1
        if e.pnl_net > 0:
            s.baseline_wins += 1
        elif e.pnl_net < 0:
            s.baseline_losses += 1
        s.baseline_pnl += e.pnl_net

        if e.veto:
            s.veto_total += 1
            s.veto_pnl_sum += e.pnl_net
            if e.pnl_net > 0:
                s.winners_in_veto += 1
                s.winners_in_veto_pnl += e.pnl_net
            for rs in e.reasons:
                by_reason_cnt[rs] += 1
                by_reason_pnl[rs] += e.pnl_net
                if e.pnl_net > 0:
                    by_reason_wins[rs] += 1
        else:
            s.residual_total += 1
            if e.pnl_net > 0:
                s.residual_wins += 1
            elif e.pnl_net < 0:
                s.residual_losses += 1
            s.residual_pnl += e.pnl_net

    s.veto_by_reason = dict(by_reason_cnt)
    s.per_reason_breakdown = {
        k: {
            "n": by_reason_cnt[k],
            "wins": by_reason_wins[k],
            "winrate_pct": (by_reason_wins[k] / by_reason_cnt[k] * 100.0)
                             if by_reason_cnt[k] else 0.0,
            "pnl_sum": by_reason_pnl[k],
            "avg_pnl": (by_reason_pnl[k] / by_reason_cnt[k])
                        if by_reason_cnt[k] else 0.0,
        }
        for k in by_reason_cnt
    }
    return s


def _emit_prom(s: Summary) -> None:
    g_last_run.set(s.ts_ms / 1000.0)
    g_total.set(s.total)
    g_veto.set(s.veto_total)
    g_veto_rate.set((s.veto_total / s.total) if s.total else 0.0)
    g_pnl_saved.set(-s.veto_pnl_sum)
    g_winners_lost.set(s.winners_in_veto)
    g_winners_pnl.set(s.winners_in_veto_pnl)
    base_decided = s.baseline_wins + s.baseline_losses
    g_baseline_wr.set((s.baseline_wins / base_decided) if base_decided else 0.0)
    g_baseline_avg.set((s.baseline_pnl / s.total) if s.total else 0.0)
    res_decided = s.residual_wins + s.residual_losses
    g_resid_wr.set((s.residual_wins / res_decided) if res_decided else 0.0)
    g_resid_avg.set((s.residual_pnl / s.residual_total) if s.residual_total else 0.0)
    for reason, cnt in s.veto_by_reason.items():
        g_veto_by.labels(reason=reason).set(cnt)


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
    base_decided = s.baseline_wins + s.baseline_losses
    res_decided = s.residual_wins + s.residual_losses
    base_wr = (s.baseline_wins / base_decided * 100.0) if base_decided else 0.0
    res_wr = (s.residual_wins / res_decided * 100.0) if res_decided else 0.0
    log.info(
        "run: n=%d veto=%d (%.1f%%) pnl_saved=%+.2f winners_lost=%d (pnl=%+.2f) "
        "wr: base=%.1f%% -> resid=%.1f%%  avg: base=%+.4f -> resid=%+.4f",
        s.total, s.veto_total,
        (s.veto_total / s.total * 100.0) if s.total else 0.0,
        -s.veto_pnl_sum, s.winners_in_veto, s.winners_in_veto_pnl,
        base_wr, res_wr,
        (s.baseline_pnl / s.total) if s.total else 0.0,
        (s.residual_pnl / s.residual_total) if s.residual_total else 0.0,
    )
    for reason, br in s.per_reason_breakdown.items():
        log.info("  reason=%-22s n=%d wr=%.1f%% avg=%+.4f tot=%+.2f",
                 reason, int(br["n"]), br["winrate_pct"], br["avg_pnl"], br["pnl_sum"])
    return s


def main() -> int:
    cfg = load_cfg()
    if not cfg.enable:
        log.info("OF_LAYER_A_SHADOW_ENABLE=0 — exit (shadow disabled)")
        return 0

    log.info(
        "starting: redis=%s stream=%s since=%.1fh interval=%ds "
        "thresholds: adverse[%sms]>=%.2f slip>=%.2f spread>=%.2f",
        cfg.redis_url, cfg.stream, cfg.since_hours, cfg.interval_sec,
        cfg.adverse_key, cfg.adverse_bps, cfg.slippage_bps, cfg.spread_bps,
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
