from __future__ import annotations

"""of_layer_c_shadow_gate_v1.py

Layer-C Shadow Gate — confluence requirement (≥N independent legs).

Гипотеза:
  Veto сделок, где меньше MIN_LEGS подтверждающих лег из:
    Leg 1: OF imbalance      (numeric, sign-aware)
    Leg 2: orderbook squeeze (numeric, sign-aware)
    Leg 3: liquidation press (numeric, sign-aware)
    Leg 4: regime alignment  (categorical, per-direction)

Numeric leg active:
  if direction=LONG:  value >=  threshold
  if direction=SHORT: value <= -threshold
Categorical leg active:
  payload[regime_field] ∈ CONFIRM_LONG (LONG) или CONFIRM_SHORT (SHORT)

Leg state: active / inactive / missing (поле отсутствует/NaN).
MISSING != INACTIVE — это разный сигнал для shadow-калибровки.

Counterfactual veto: legs_active < MIN_LEGS.

ENV:
  OF_LAYER_C_SHADOW_ENABLE         0
  OF_LAYER_C_SHADOW_REDIS_URL      redis://redis-worker-1:6379/0
  OF_LAYER_C_SHADOW_STREAM         trades:closed
  OF_LAYER_C_SHADOW_SINCE_HOURS    24.0
  OF_LAYER_C_SHADOW_INTERVAL_SEC   300
  OF_LAYER_C_MIN_LEGS              2

  OF_LAYER_C_LEG1_NAME             of_imbalance
  OF_LAYER_C_LEG1_FEATURE_KEY      qimb_wmean
  OF_LAYER_C_LEG1_THRESHOLD        1.0
  OF_LAYER_C_LEG1_ENABLED          1

  OF_LAYER_C_LEG2_NAME             ob_squeeze
  OF_LAYER_C_LEG2_FEATURE_KEY      lob_dw_obi_z
  OF_LAYER_C_LEG2_THRESHOLD        1.5
  OF_LAYER_C_LEG2_ENABLED          1

  OF_LAYER_C_LEG3_NAME             liq_pressure
  OF_LAYER_C_LEG3_FEATURE_KEY      liq_pressure_z
  OF_LAYER_C_LEG3_THRESHOLD        1.0
  OF_LAYER_C_LEG3_ENABLED          1

  OF_LAYER_C_LEG4_NAME             regime_align
  OF_LAYER_C_LEG4_REGIME_FIELD     regime
  OF_LAYER_C_LEG4_CONFIRM_LONG     uptrend,trend_up
  OF_LAYER_C_LEG4_CONFIRM_SHORT    downtrend,trend_down
  OF_LAYER_C_LEG4_ENABLED          1

  OF_LAYER_C_REPORT_PATH           /var/lib/trade/of_reports/of_layer_c_shadow.json
  OF_LAYER_C_PROM_PORT             9849
  OF_LAYER_C_BATCH                 2000
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
    format="%(asctime)s [of-layer-c-shadow] %(levelname)s %(message)s",
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
class NumericLegCfg:
    name: str
    feature_key: str
    threshold: float
    enabled: bool


@dataclass
class CategoricalLegCfg:
    name: str
    regime_field: str
    confirm_long: tuple[str, ...]
    confirm_short: tuple[str, ...]
    enabled: bool


@dataclass
class Cfg:
    enable: bool
    redis_url: str
    stream: str
    since_hours: float
    interval_sec: int
    min_legs: int
    numeric_legs: tuple[NumericLegCfg, ...]
    regime_leg: CategoricalLegCfg
    report_path: str
    prom_port: int
    batch: int


def load_cfg() -> Cfg:
    legs: list[NumericLegCfg] = []
    for n, dname, dkey, dthr in (
        (1, "of_imbalance", "qimb_wmean",    1.0),
        (2, "ob_squeeze",   "lob_dw_obi_z",  1.5),
        (3, "liq_pressure", "liq_pressure_z", 1.0),
    ):
        legs.append(NumericLegCfg(
            name        = _env(f"OF_LAYER_C_LEG{n}_NAME", dname),
            feature_key = _env(f"OF_LAYER_C_LEG{n}_FEATURE_KEY", dkey),
            threshold   = _env_float(f"OF_LAYER_C_LEG{n}_THRESHOLD", dthr),
            enabled     = bool(_env_int(f"OF_LAYER_C_LEG{n}_ENABLED", 1)),
        ))
    regime_leg = CategoricalLegCfg(
        name          = _env("OF_LAYER_C_LEG4_NAME", "regime_align"),
        regime_field  = _env("OF_LAYER_C_LEG4_REGIME_FIELD", "regime"),
        confirm_long  = _env_csv("OF_LAYER_C_LEG4_CONFIRM_LONG", "uptrend,trend_up"),
        confirm_short = _env_csv("OF_LAYER_C_LEG4_CONFIRM_SHORT", "downtrend,trend_down"),
        enabled       = bool(_env_int("OF_LAYER_C_LEG4_ENABLED", 1)),
    )
    return Cfg(
        enable        = bool(_env_int("OF_LAYER_C_SHADOW_ENABLE", 0)),
        redis_url     = _env("OF_LAYER_C_SHADOW_REDIS_URL", "redis://redis-worker-1:6379/0"),
        stream        = _env("OF_LAYER_C_SHADOW_STREAM", "trades:closed"),
        since_hours   = _env_float("OF_LAYER_C_SHADOW_SINCE_HOURS", 24.0),
        interval_sec  = _env_int("OF_LAYER_C_SHADOW_INTERVAL_SEC", 300),
        min_legs      = _env_int("OF_LAYER_C_MIN_LEGS", 2),
        numeric_legs  = tuple(legs),
        regime_leg    = regime_leg,
        report_path   = _env("OF_LAYER_C_REPORT_PATH",
                             "/var/lib/trade/of_reports/of_layer_c_shadow.json"),
        prom_port     = _env_int("OF_LAYER_C_PROM_PORT", 9849),
        batch         = _env_int("OF_LAYER_C_BATCH", 2000),
    )


# Prometheus
g_up           = Gauge("of_layer_c_shadow_up", "service loop up")
g_last_run     = Gauge("of_layer_c_shadow_last_run_ts", "last run unix ts")
g_total        = Gauge("of_layer_c_shadow_trades_total", "total trades evaluated")
g_veto         = Gauge("of_layer_c_shadow_veto_total", "trades that would be vetoed")
g_veto_rate    = Gauge("of_layer_c_shadow_veto_rate", "veto rate [0..1]")
g_legs_dist    = Gauge("of_layer_c_shadow_legs_dist", "trades by legs active", ["n_legs"])
g_leg_active   = Gauge("of_layer_c_shadow_leg_active_count",
                       "per-leg active count", ["leg"])
g_leg_missing  = Gauge("of_layer_c_shadow_leg_missing_count",
                       "per-leg missing-field count", ["leg"])
g_pnl_saved    = Gauge("of_layer_c_shadow_pnl_saved_usd",
                       "sum -pnl_net over vetoed trades")
g_winners_lost = Gauge("of_layer_c_shadow_winners_lost",
                       "winners in vetoed set")
g_winners_pnl  = Gauge("of_layer_c_shadow_winners_pnl_lost_usd",
                       "sum pnl_net of winners in vetoed set")
g_baseline_wr  = Gauge("of_layer_c_shadow_baseline_winrate", "baseline winrate")
g_residual_wr  = Gauge("of_layer_c_shadow_residual_winrate", "winrate after counterfactual")
g_baseline_avg = Gauge("of_layer_c_shadow_baseline_avg_pnl", "baseline avg pnl_net")
g_residual_avg = Gauge("of_layer_c_shadow_residual_avg_pnl", "residual avg pnl_net")
c_errors       = PCounter("of_layer_c_shadow_errors_total", "errors", ["where"])


def _f(v: Any, d: float = 0.0) -> float:
    try:
        x = float(v)
        return x if math.isfinite(x) else d
    except Exception:
        return d


def _now_ms() -> int:
    return int(time.time() * 1000)


def _features_dict(d: dict[str, Any]) -> dict[str, Any]:
    raw = d.get("features")
    if isinstance(raw, dict):
        return raw
    if not raw:
        return {}
    try:
        j = json.loads(raw)
        return j if isinstance(j, dict) else {}
    except Exception:
        return {}


def _lookup_numeric(d: dict[str, Any], feats: dict[str, Any], key: str) -> float | None:
    """Поиск numeric поля сначала в features, потом в payload верхнего уровня."""
    for src in (feats, d):
        if key in src:
            try:
                v = float(src[key])
                if math.isfinite(v):
                    return v
            except Exception:
                pass
    return None


LEG_STATES = ("active", "inactive", "missing")


@dataclass
class LegResult:
    name: str
    state: str    # active / inactive / missing
    value: float | None = None


@dataclass
class TradeEval:
    sid: str
    symbol: str
    direction: str
    pnl_net: float
    legs: tuple[LegResult, ...]
    legs_active: int
    veto: bool


def _eval_numeric_leg(
    cfg: NumericLegCfg, value: float | None, direction: str,
) -> LegResult:
    if value is None:
        return LegResult(name=cfg.name, state="missing", value=None)
    if direction == "LONG":
        active = value >= cfg.threshold
    elif direction == "SHORT":
        active = value <= -cfg.threshold
    else:
        return LegResult(name=cfg.name, state="missing", value=value)
    return LegResult(name=cfg.name,
                     state="active" if active else "inactive",
                     value=value)


def _eval_regime_leg(
    cfg: CategoricalLegCfg, regime_val: str, direction: str,
) -> LegResult:
    rv = (regime_val or "").lower()
    if rv in ("", "na", "none", "null", "?"):
        return LegResult(name=cfg.name, state="missing")
    if direction == "LONG":
        active = rv in cfg.confirm_long
    elif direction == "SHORT":
        active = rv in cfg.confirm_short
    else:
        return LegResult(name=cfg.name, state="missing")
    return LegResult(name=cfg.name,
                     state="active" if active else "inactive")


def _evaluate(d: dict[str, Any], cfg: Cfg) -> TradeEval:
    pnl = _f(d.get("pnl_net") or d.get("pnl"), 0.0)
    direction = str(d.get("direction") or d.get("side") or "").upper()
    feats = _features_dict(d)

    legs: list[LegResult] = []
    for lc in cfg.numeric_legs:
        if not lc.enabled:
            continue
        v = _lookup_numeric(d, feats, lc.feature_key)
        legs.append(_eval_numeric_leg(lc, v, direction))

    if cfg.regime_leg.enabled:
        rv = str(d.get(cfg.regime_leg.regime_field) or "")
        legs.append(_eval_regime_leg(cfg.regime_leg, rv, direction))

    active = sum(1 for l in legs if l.state == "active")
    return TradeEval(
        sid         = str(d.get("sid") or d.get("signal_id") or ""),
        symbol      = str(d.get("symbol") or "?"),
        direction   = direction or "?",
        pnl_net     = pnl,
        legs        = tuple(legs),
        legs_active = active,
        veto        = active < cfg.min_legs,
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
    min_legs: int
    leg_names: list[str] = field(default_factory=list)
    total: int = 0
    baseline_wins: int = 0
    baseline_losses: int = 0
    baseline_pnl: float = 0.0
    veto_total: int = 0
    veto_pnl_sum: float = 0.0
    winners_in_veto: int = 0
    winners_in_veto_pnl: float = 0.0
    residual_total: int = 0
    residual_wins: int = 0
    residual_losses: int = 0
    residual_pnl: float = 0.0
    legs_dist: dict[str, int] = field(default_factory=dict)
    per_leg: dict[str, dict[str, int]] = field(default_factory=dict)


def _summarise(evals: list[TradeEval], cfg: Cfg) -> Summary:
    leg_names = [l.name for l in cfg.numeric_legs if l.enabled]
    if cfg.regime_leg.enabled:
        leg_names.append(cfg.regime_leg.name)

    s = Summary(
        ts_ms=_now_ms(),
        since_hours=cfg.since_hours,
        min_legs=cfg.min_legs,
        leg_names=leg_names,
    )
    legs_dist: Counter[int] = Counter()
    per_leg_state: defaultdict[str, Counter[str]] = defaultdict(Counter)
    per_leg_active_pnl: defaultdict[str, float] = defaultdict(float)
    per_leg_active_wins: defaultdict[str, int] = defaultdict(int)

    for e in evals:
        s.total += 1
        if e.pnl_net > 0:
            s.baseline_wins += 1
        elif e.pnl_net < 0:
            s.baseline_losses += 1
        s.baseline_pnl += e.pnl_net
        legs_dist[e.legs_active] += 1

        for leg in e.legs:
            per_leg_state[leg.name][leg.state] += 1
            if leg.state == "active":
                per_leg_active_pnl[leg.name] += e.pnl_net
                if e.pnl_net > 0:
                    per_leg_active_wins[leg.name] += 1

        if e.veto:
            s.veto_total += 1
            s.veto_pnl_sum += e.pnl_net
            if e.pnl_net > 0:
                s.winners_in_veto += 1
                s.winners_in_veto_pnl += e.pnl_net
        else:
            s.residual_total += 1
            if e.pnl_net > 0:
                s.residual_wins += 1
            elif e.pnl_net < 0:
                s.residual_losses += 1
            s.residual_pnl += e.pnl_net

    s.legs_dist = {str(k): legs_dist[k] for k in sorted(legs_dist)}
    s.per_leg = {
        name: {
            "active":  per_leg_state[name].get("active", 0),
            "inactive": per_leg_state[name].get("inactive", 0),
            "missing": per_leg_state[name].get("missing", 0),
            "active_wins": per_leg_active_wins[name],
            "active_pnl_sum_x100": int(per_leg_active_pnl[name] * 100),
        }
        for name in leg_names
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
    base_dec = s.baseline_wins + s.baseline_losses
    res_dec = s.residual_wins + s.residual_losses
    g_baseline_wr.set((s.baseline_wins / base_dec) if base_dec else 0.0)
    g_baseline_avg.set((s.baseline_pnl / s.total) if s.total else 0.0)
    g_residual_wr.set((s.residual_wins / res_dec) if res_dec else 0.0)
    g_residual_avg.set((s.residual_pnl / s.residual_total) if s.residual_total else 0.0)
    for n_legs, cnt in s.legs_dist.items():
        g_legs_dist.labels(n_legs=n_legs).set(cnt)
    for name, stats in s.per_leg.items():
        g_leg_active.labels(leg=name).set(stats["active"])
        g_leg_missing.labels(leg=name).set(stats["missing"])


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
    base_dec = s.baseline_wins + s.baseline_losses
    res_dec = s.residual_wins + s.residual_losses
    base_wr = (s.baseline_wins / base_dec * 100.0) if base_dec else 0.0
    res_wr = (s.residual_wins / res_dec * 100.0) if res_dec else 0.0
    log.info(
        "run: n=%d veto=%d (%.1f%%) pnl_saved=%+.2f winners_lost=%d (pnl=%+.2f) "
        "wr: base=%.1f%% → resid=%.1f%%  avg: base=%+.4f → resid=%+.4f",
        s.total, s.veto_total,
        (s.veto_total / s.total * 100.0) if s.total else 0.0,
        -s.veto_pnl_sum, s.winners_in_veto, s.winners_in_veto_pnl,
        base_wr, res_wr,
        (s.baseline_pnl / s.total) if s.total else 0.0,
        (s.residual_pnl / s.residual_total) if s.residual_total else 0.0,
    )
    log.info("  legs_dist: %s", s.legs_dist)
    for name, stats in s.per_leg.items():
        a = stats["active"]; m = stats["missing"]; w = stats["active_wins"]
        wr = (w / a * 100.0) if a else 0.0
        log.info("  leg=%-15s active=%d missing=%d active_wr=%.1f%%",
                 name, a, m, wr)
    return s


def main() -> int:
    cfg = load_cfg()
    if not cfg.enable:
        log.info("OF_LAYER_C_SHADOW_ENABLE=0 — exit (shadow disabled)")
        return 0

    log.info(
        "starting: redis=%s stream=%s since=%.1fh interval=%ds min_legs=%d",
        cfg.redis_url, cfg.stream, cfg.since_hours, cfg.interval_sec, cfg.min_legs,
    )
    for lc in cfg.numeric_legs:
        log.info("  numeric leg: name=%s key=%s thr=%.3f enabled=%s",
                 lc.name, lc.feature_key, lc.threshold, lc.enabled)
    log.info("  regime leg: name=%s field=%s long=%s short=%s enabled=%s",
             cfg.regime_leg.name, cfg.regime_leg.regime_field,
             cfg.regime_leg.confirm_long, cfg.regime_leg.confirm_short,
             cfg.regime_leg.enabled)

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
