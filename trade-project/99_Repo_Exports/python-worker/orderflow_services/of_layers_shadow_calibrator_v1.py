from __future__ import annotations

"""of_layers_shadow_calibrator_v1.py

Auto-calibrator for Layer A/B/C/D shadow services.

Что делает:
  - Каждые INTERVAL_SEC читает JSON-отчёты 4 слоёв.
  - Применяет per-layer quality criteria.
  - Per-layer state machine:
      NO_REPORT → WARMUP → DATA_COLLECTED → QUALIFIED
                          ↓
                       NEEDS_TUNING (если есть проблемы)
  - Notify в Telegram при переходах в QUALIFIED / NEEDS_TUNING (с cooldown).
  - Пишет агрегатный state-файл + Prometheus metrics.

НЕ делает enforcement автоматически — только сигнализирует human operator.
"""

import hashlib
import hmac
import json
import logging
import math
import os
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import redis
from prometheus_client import Counter as PCounter, Gauge, start_http_server  # type: ignore

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [layers-cal] %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

STATES = ("NO_REPORT", "WARMUP", "DATA_COLLECTED", "QUALIFIED",
          "NEEDS_TUNING", "CANARY_APPLIED")
STATE_NUM = {s: i for i, s in enumerate(STATES)}


def _env(k: str, d: str = "") -> str:
    return os.environ.get(k, d)

def _env_int(k: str, d: int) -> int:
    try: return int(_env(k, str(d)))
    except Exception: return d

def _env_float(k: str, d: float) -> float:
    try: return float(_env(k, str(d)))
    except Exception: return d


@dataclass
class LayerCriteriaA:
    veto_rate_min: float
    veto_rate_max: float
    wr_uplift_min_pp: float
    winners_lost_ratio_max: float

@dataclass
class LayerCriteriaB:
    pnl_uplift_min_usd: float
    winners_pnl_lost_ratio_max: float
    clamp_rate_max: float

@dataclass
class LayerCriteriaC:
    missing_max_per_leg: float
    wr_uplift_min_pp: float
    winners_lost_ratio_max: float

@dataclass
class LayerCriteriaD:
    pnl_uplift_min_usd: float
    losers_flipped_min: int
    armable_rate_min: float


@dataclass
class Cfg:
    enable: bool
    interval_sec: int
    min_trades: int
    reports_dir: str
    state_path: str
    layer_a_report: str
    layer_b_report: str
    layer_c_report: str
    layer_d_report: str
    crit_a: LayerCriteriaA
    crit_b: LayerCriteriaB
    crit_c: LayerCriteriaC
    crit_d: LayerCriteriaD
    notify_stream: str
    notify_redis_url: str
    cooldown_sec: int
    prom_port: int

    # auto-promotion (DEFAULT DISABLED)
    auto_promote_enable: bool
    auto_promote_layers: tuple[str, ...]   # csv: "A,B,C" (D never auto)
    canary_symbols: tuple[str, ...]
    gates_redis_url: str
    gates_key_prefix: str
    promote_cooldown_sec: int
    recs_hmac_secret: str


def load_cfg() -> Cfg:
    reports = _env("LAYERS_CAL_REPORTS_DIR", "/var/lib/trade/of_reports")
    return Cfg(
        enable       = bool(_env_int("LAYERS_CAL_ENABLE", 0)),
        interval_sec = _env_int("LAYERS_CAL_INTERVAL_SEC", 600),
        min_trades   = _env_int("LAYERS_CAL_MIN_TRADES", 200),
        reports_dir  = reports,
        state_path   = _env("LAYERS_CAL_STATE_PATH", f"{reports}/layers_cal_state.json"),
        layer_a_report = _env("LAYERS_CAL_A_REPORT", f"{reports}/of_layer_a_shadow.json"),
        layer_b_report = _env("LAYERS_CAL_B_REPORT", f"{reports}/of_layer_b_shadow.json"),
        layer_c_report = _env("LAYERS_CAL_C_REPORT", f"{reports}/of_layer_c_shadow.json"),
        layer_d_report = _env("LAYERS_CAL_D_REPORT", f"{reports}/of_layer_d_shadow.json"),
        crit_a = LayerCriteriaA(
            veto_rate_min          = _env_float("LAYERS_CAL_A_VETO_RATE_MIN", 0.05),
            veto_rate_max          = _env_float("LAYERS_CAL_A_VETO_RATE_MAX", 0.15),
            wr_uplift_min_pp       = _env_float("LAYERS_CAL_A_WR_UPLIFT_MIN_PP", 3.0),
            winners_lost_ratio_max = _env_float("LAYERS_CAL_A_WINNERS_LOST_MAX", 0.05),
        ),
        crit_b = LayerCriteriaB(
            pnl_uplift_min_usd         = _env_float("LAYERS_CAL_B_PNL_UPLIFT_MIN_USD", 0.0),
            winners_pnl_lost_ratio_max = _env_float("LAYERS_CAL_B_WINNERS_LOST_RATIO_MAX", 0.30),
            clamp_rate_max             = _env_float("LAYERS_CAL_B_CLAMP_RATE_MAX", 0.70),
        ),
        crit_c = LayerCriteriaC(
            missing_max_per_leg    = _env_float("LAYERS_CAL_C_MISSING_MAX_PER_LEG", 0.30),
            wr_uplift_min_pp       = _env_float("LAYERS_CAL_C_WR_UPLIFT_MIN_PP", 5.0),
            winners_lost_ratio_max = _env_float("LAYERS_CAL_C_WINNERS_LOST_RATIO_MAX", 0.25),
        ),
        crit_d = LayerCriteriaD(
            pnl_uplift_min_usd  = _env_float("LAYERS_CAL_D_PNL_UPLIFT_MIN_USD", 10.0),
            losers_flipped_min  = _env_int("LAYERS_CAL_D_LOSERS_FLIPPED_MIN", 5),
            armable_rate_min    = _env_float("LAYERS_CAL_D_ARMABLE_RATE_MIN", 0.10),
        ),
        notify_stream    = _env("LAYERS_CAL_NOTIFY_STREAM", "notify:telegram"),
        notify_redis_url = _env("LAYERS_CAL_NOTIFY_REDIS_URL", "redis://redis:6379/0"),
        cooldown_sec     = _env_int("LAYERS_CAL_COOLDOWN_SEC", 86400),
        prom_port        = _env_int("LAYERS_CAL_PROM_PORT", 9851),

        auto_promote_enable  = bool(_env_int("LAYERS_CAL_AUTO_PROMOTE_ENABLE", 0)),
        auto_promote_layers  = tuple(
            s.strip().upper() for s in
            _env("LAYERS_CAL_AUTO_PROMOTE_LAYERS", "A,B,C").split(",")
            if s.strip()
        ),
        canary_symbols       = tuple(
            s.strip().upper() for s in
            _env("LAYERS_CAL_CANARY_SYMBOLS", "BTCUSDT,ETHUSDT,SOLUSDT").split(",")
            if s.strip()
        ),
        gates_redis_url      = _env("LAYERS_CAL_GATES_REDIS_URL",
                                    "redis://redis-worker-1:6379/0"),
        gates_key_prefix     = _env("LAYERS_CAL_GATES_KEY_PREFIX", "of_gate"),
        promote_cooldown_sec = _env_int("LAYERS_CAL_PROMOTE_COOLDOWN_SEC", 86400),
        recs_hmac_secret     = _env("LAYERS_CAL_HMAC_SECRET", "CHANGE_ME"),
    )


# Prometheus
g_up         = Gauge("of_layers_cal_up", "calibrator loop up")
g_last_run   = Gauge("of_layers_cal_last_run_ts", "last run unix ts")
g_state      = Gauge("of_layers_cal_state",
                     "per-layer state numeric", ["layer"])
g_criterion  = Gauge("of_layers_cal_criterion_ok",
                     "1=criterion met, 0=fail", ["layer", "criterion"])
g_value      = Gauge("of_layers_cal_metric",
                     "raw metric value used in criterion", ["layer", "metric"])
c_errors     = PCounter("of_layers_cal_errors_total", "errors", ["where"])
c_notifies   = PCounter("of_layers_cal_notifies_total", "notify sends",
                        ["layer", "kind"])
g_promo_mode = Gauge("of_layers_cal_promo_mode",
                     "0=off 1=canary 2=prod (per layer in gates redis)", ["layer"])
c_promos     = PCounter("of_layers_cal_promotions_total",
                        "auto-promotion writes", ["layer", "result"])


def _load_json(path: str) -> dict[str, Any] | None:
    p = Path(path)
    if not p.exists():
        return None
    try:
        with open(p, encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        log.warning(f"read {path}: {e}")
        c_errors.labels(where="read_report").inc()
        return None


def _f(v: Any, d: float = 0.0) -> float:
    try:
        x = float(v)
        return x if math.isfinite(x) else d
    except Exception:
        return d


def _i(v: Any, d: int = 0) -> int:
    try: return int(v)
    except Exception:
        try: return int(float(v))
        except Exception: return d


@dataclass
class LayerEval:
    layer: str
    state: str
    metrics: dict[str, float] = field(default_factory=dict)
    criteria: dict[str, bool] = field(default_factory=dict)
    issues: list[str] = field(default_factory=list)
    suggestions: list[str] = field(default_factory=list)


def _eval_layer_a(rep: dict[str, Any] | None, cfg: Cfg) -> LayerEval:
    e = LayerEval(layer="A", state="NO_REPORT")
    if not rep:
        e.issues.append("отчёт отсутствует или не парсится")
        return e
    total = _i(rep.get("total"))
    if total < cfg.min_trades:
        e.state = "WARMUP"
        e.metrics["total"] = total
        e.issues.append(f"WARMUP: total={total} < min_trades={cfg.min_trades}")
        return e

    veto = _i(rep.get("veto_total"))
    veto_rate = veto / total if total else 0.0
    base_w = _i(rep.get("baseline_wins"))
    base_l = _i(rep.get("baseline_losses"))
    res_w  = _i(rep.get("residual_wins"))
    res_l  = _i(rep.get("residual_losses"))
    base_wr = base_w / (base_w + base_l) * 100.0 if (base_w + base_l) else 0.0
    res_wr  = res_w  / (res_w  + res_l ) * 100.0 if (res_w  + res_l ) else 0.0
    wr_uplift_pp = res_wr - base_wr
    winners_lost = _i(rep.get("winners_in_veto"))
    winners_lost_ratio = (winners_lost / base_w) if base_w else 0.0
    pnl_saved = _f(rep.get("veto_pnl_sum"), 0.0) * -1.0

    e.metrics = {
        "total": total, "veto_rate": veto_rate, "wr_uplift_pp": wr_uplift_pp,
        "winners_lost_ratio": winners_lost_ratio, "pnl_saved_usd": pnl_saved,
        "baseline_winrate_pct": base_wr, "residual_winrate_pct": res_wr,
    }
    c = cfg.crit_a
    e.criteria = {
        "veto_rate_in_band":   c.veto_rate_min <= veto_rate <= c.veto_rate_max,
        "wr_uplift_ok":        wr_uplift_pp >= c.wr_uplift_min_pp,
        "winners_lost_ok":     winners_lost_ratio <= c.winners_lost_ratio_max,
        "pnl_saved_positive":  pnl_saved > 0,
    }
    if not e.criteria["veto_rate_in_band"]:
        if veto_rate < c.veto_rate_min:
            e.suggestions.append(
                f"veto_rate={veto_rate:.1%} ниже {c.veto_rate_min:.1%}: смягчите пороги "
                "(OF_LAYER_A_SHADOW_ADVERSE_BPS↓, _SLIPPAGE_BPS↓, _SPREAD_BPS↓)")
        else:
            e.suggestions.append(
                f"veto_rate={veto_rate:.1%} выше {c.veto_rate_max:.1%}: ужесточите пороги "
                "(OF_LAYER_A_SHADOW_ADVERSE_BPS↑ и т.п.)")
    if not e.criteria["winners_lost_ok"]:
        e.suggestions.append(
            f"winners_lost={winners_lost_ratio:.1%} > {c.winners_lost_ratio_max:.1%}: "
            "слой режет винеров → ужесточите spread/slippage thresholds")
    if all(e.criteria.values()):
        e.state = "QUALIFIED"
    elif any(e.criteria.values()):
        e.state = "NEEDS_TUNING"
    else:
        e.state = "DATA_COLLECTED"
    return e


def _eval_layer_b(rep: dict[str, Any] | None, cfg: Cfg) -> LayerEval:
    e = LayerEval(layer="B", state="NO_REPORT")
    if not rep:
        e.issues.append("отчёт отсутствует или не парсится")
        return e
    total = _i(rep.get("total"))
    if total < cfg.min_trades:
        e.state = "WARMUP"
        e.metrics["total"] = total
        e.issues.append(f"WARMUP: total={total} < {cfg.min_trades}")
        return e

    clamped = _i(rep.get("clamped_total"))
    clamp_rate = clamped / total if total else 0.0
    base_pnl  = _f(rep.get("baseline_total_pnl"))
    cf_pnl    = _f(rep.get("counterfactual_total_pnl"))
    uplift    = cf_pnl - base_pnl
    winners_lost_pnl = _f(rep.get("winners_pnl_lost"))
    winners_lost_ratio = (winners_lost_pnl / abs(base_pnl)) if base_pnl else 0.0

    e.metrics = {
        "total": total, "clamp_rate": clamp_rate,
        "baseline_pnl_usd": base_pnl, "counterfactual_pnl_usd": cf_pnl,
        "pnl_uplift_usd": uplift,
        "winners_pnl_lost_ratio": winners_lost_ratio,
    }
    c = cfg.crit_b
    e.criteria = {
        "pnl_uplift_positive": uplift >= c.pnl_uplift_min_usd,
        "winners_lost_ok":     winners_lost_ratio <= c.winners_pnl_lost_ratio_max,
        "clamp_rate_ok":       clamp_rate <= c.clamp_rate_max,
    }
    if not e.criteria["pnl_uplift_positive"]:
        e.suggestions.append(
            f"uplift={uplift:+.2f} USD < {c.pnl_uplift_min_usd}: clamp не даёт выгоды; "
            "проверьте distribution slippage/spread и порог LONG_CLAMP")
    if not e.criteria["clamp_rate_ok"]:
        e.suggestions.append(
            f"clamp_rate={clamp_rate:.1%} > {c.clamp_rate_max:.1%}: слишком агрессивно; "
            "сузьте SLIP/SPR диапазоны или отключите long_no_htf_confirm")
    if not e.criteria["winners_lost_ok"]:
        e.suggestions.append(
            f"winners_pnl_lost={winners_lost_ratio:.1%}: слой режет прибыль; "
            "увеличьте clamp factors (×0.5 → ×0.7) или сузьте bucket'ы")
    if all(e.criteria.values()):
        e.state = "QUALIFIED"
    elif any(e.criteria.values()):
        e.state = "NEEDS_TUNING"
    else:
        e.state = "DATA_COLLECTED"
    return e


def _eval_layer_c(rep: dict[str, Any] | None, cfg: Cfg) -> LayerEval:
    e = LayerEval(layer="C", state="NO_REPORT")
    if not rep:
        e.issues.append("отчёт отсутствует или не парсится")
        return e
    total = _i(rep.get("total"))
    if total < cfg.min_trades:
        e.state = "WARMUP"
        e.metrics["total"] = total
        e.issues.append(f"WARMUP: total={total} < {cfg.min_trades}")
        return e

    # per-leg missing analysis — критическая фишка для Layer C
    per_leg = rep.get("per_leg") or {}
    legs_missing_high: list[tuple[str, float]] = []
    legs_active_rate: dict[str, float] = {}
    legs_missing_rate: dict[str, float] = {}
    for name, stats in per_leg.items():
        if not isinstance(stats, dict):
            continue
        active  = _i(stats.get("active"))
        missing = _i(stats.get("missing"))
        legs_active_rate[name] = active / total if total else 0.0
        miss_rate = missing / total if total else 0.0
        legs_missing_rate[name] = miss_rate
        if miss_rate > cfg.crit_c.missing_max_per_leg:
            legs_missing_high.append((name, miss_rate))

    base_w = _i(rep.get("baseline_wins"))
    base_l = _i(rep.get("baseline_losses"))
    res_w  = _i(rep.get("residual_wins"))
    res_l  = _i(rep.get("residual_losses"))
    base_wr = base_w / (base_w + base_l) * 100.0 if (base_w + base_l) else 0.0
    res_wr  = res_w  / (res_w  + res_l ) * 100.0 if (res_w  + res_l ) else 0.0
    wr_uplift_pp = res_wr - base_wr
    winners_lost = _i(rep.get("winners_in_veto"))
    winners_lost_ratio = (winners_lost / base_w) if base_w else 0.0

    e.metrics = {
        "total": total,
        "veto_rate": _i(rep.get("veto_total")) / total if total else 0.0,
        "wr_uplift_pp": wr_uplift_pp,
        "winners_lost_ratio": winners_lost_ratio,
        "baseline_winrate_pct": base_wr, "residual_winrate_pct": res_wr,
    }
    for n, r in legs_missing_rate.items():
        e.metrics[f"leg_missing_rate.{n}"] = r
    for n, r in legs_active_rate.items():
        e.metrics[f"leg_active_rate.{n}"] = r

    c = cfg.crit_c
    e.criteria = {
        "all_legs_present":  not legs_missing_high,
        "wr_uplift_ok":      wr_uplift_pp >= c.wr_uplift_min_pp,
        "winners_lost_ok":   winners_lost_ratio <= c.winners_lost_ratio_max,
    }
    for name, rate in legs_missing_high:
        e.suggestions.append(
            f"leg '{name}' missing={rate:.1%}: фича отсутствует в payload. "
            f"Проверьте OF_LAYER_C_LEGN_FEATURE_KEY или отключите ногу "
            f"(OF_LAYER_C_LEGN_ENABLED=0)")
    if not e.criteria["wr_uplift_ok"]:
        e.suggestions.append(
            f"wr_uplift={wr_uplift_pp:+.1f}pp < {c.wr_uplift_min_pp}pp: confluence "
            "слабый — снизьте MIN_LEGS или уменьшите пороги ног")
    if not e.criteria["winners_lost_ok"]:
        e.suggestions.append(
            f"winners_lost={winners_lost_ratio:.1%}: слишком жёсткий confluence; "
            "уменьшите MIN_LEGS=2→1 или снизьте пороги")
    if all(e.criteria.values()) and not legs_missing_high:
        e.state = "QUALIFIED"
    elif legs_missing_high or any(e.criteria.values()):
        e.state = "NEEDS_TUNING"
    else:
        e.state = "DATA_COLLECTED"
    return e


def _eval_layer_d(rep: dict[str, Any] | None, cfg: Cfg) -> LayerEval:
    e = LayerEval(layer="D", state="NO_REPORT")
    if not rep:
        e.issues.append("отчёт отсутствует или не парсится")
        return e
    total = _i(rep.get("total"))
    if total < cfg.min_trades:
        e.state = "WARMUP"
        e.metrics["total"] = total
        e.issues.append(f"WARMUP: total={total} < {cfg.min_trades}")
        return e

    armable     = _i(rep.get("armable"))
    armable_rate= armable / total if total else 0.0
    improved    = _i(rep.get("improved"))
    losers_flip = _i(rep.get("losers_flipped"))
    base_pnl    = _f(rep.get("baseline_pnl"))
    cf_pnl      = _f(rep.get("cf_pnl"))
    uplift      = _f(rep.get("pnl_uplift"), cf_pnl - base_pnl)

    e.metrics = {
        "total": total, "armable_rate": armable_rate,
        "improved": improved, "losers_flipped": losers_flip,
        "baseline_pnl_usd": base_pnl, "cf_pnl_usd": cf_pnl,
        "pnl_uplift_usd": uplift,
    }
    c = cfg.crit_d
    e.criteria = {
        "uplift_significant": uplift >= c.pnl_uplift_min_usd,
        "armable_rate_ok":    armable_rate >= c.armable_rate_min,
        "losers_flipped_ok":  losers_flip >= c.losers_flipped_min,
    }
    if not e.criteria["armable_rate_ok"]:
        e.suggestions.append(
            f"armable_rate={armable_rate:.1%} < {c.armable_rate_min:.1%}: "
            "слишком высокий ARM_THRESHOLD_R — снизьте до 0.3-0.4")
    if not e.criteria["uplift_significant"]:
        e.suggestions.append(
            f"uplift={uplift:+.2f} USD: модель trailing не даёт выигрыша; "
            "попробуйте OF_LAYER_D_KEEP_FRACTION=0.6 или ARM_THRESHOLD_R=0.4")
    if all(e.criteria.values()):
        e.state = "QUALIFIED"
    elif any(e.criteria.values()):
        e.state = "NEEDS_TUNING"
    else:
        e.state = "DATA_COLLECTED"
    return e


def _emit_prom(evals: list[LayerEval]) -> None:
    for e in evals:
        g_state.labels(layer=e.layer).set(STATE_NUM.get(e.state, 0))
        for k, ok in e.criteria.items():
            g_criterion.labels(layer=e.layer, criterion=k).set(1.0 if ok else 0.0)
        for m, v in e.metrics.items():
            try:
                g_value.labels(layer=e.layer, metric=m).set(v if isinstance(v, (int, float)) else 0.0)
            except Exception:
                pass


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
            json.dump(state, f, indent=2, sort_keys=True, default=str)
        os.replace(tmp, path)
    except Exception as ex:
        log.warning(f"state save failed: {ex}")
        c_errors.labels(where="save_state").inc()


def _notify(redis_client: redis.Redis | None, cfg: Cfg,
            layer: str, kind: str, text: str) -> None:
    if redis_client is None:
        log.info(f"[NOTIFY-DRYRUN layer={layer} kind={kind}] {text}")
        return
    try:
        payload = json.dumps({"text": text, "layer": layer, "kind": kind})
        redis_client.xadd(cfg.notify_stream, {"payload": payload}, maxlen=1000)
        c_notifies.labels(layer=layer, kind=kind).inc()
        log.info(f"notify sent layer={layer} kind={kind}")
    except Exception as ex:
        log.warning(f"notify failed: {ex}")
        c_errors.labels(where="notify").inc()


_MODE_NUM = {"off": 0, "canary": 1, "prod": 2}


def _sign_bundle(payload: dict[str, Any], secret: str) -> str:
    """HMAC-SHA256 подпись стабильного JSON-bundle для tamper protection."""
    blob = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    return hmac.new(secret.encode(), blob, hashlib.sha256).hexdigest()


def _read_current_mode(r: redis.Redis | None, prefix: str, layer: str) -> str:
    if r is None:
        return ""
    try:
        v = r.get(f"{prefix}:layer_{layer.lower()}:mode")
        return str(v or "")
    except Exception:
        return ""


def _apply_canary(
    r: redis.Redis | None, cfg: Cfg, layer: str, eval_: LayerEval,
) -> tuple[bool, str]:
    """Записывает canary-флаги в gates-redis. Возвращает (applied, reason)."""
    if r is None:
        return False, "gates_redis_unavailable"

    prefix = cfg.gates_key_prefix
    base = f"{prefix}:layer_{layer.lower()}"
    now_ms = int(time.time() * 1000)
    bundle: dict[str, Any] = {
        "layer":          layer,
        "mode":           "canary",
        "canary_symbols": list(cfg.canary_symbols),
        "promoted_ts_ms": now_ms,
        "promoted_by":    "of_layers_shadow_calibrator_v1",
        "shadow_metrics": {
            k: v for k, v in eval_.metrics.items()
            if isinstance(v, (int, float))
        },
        "criteria":       dict(eval_.criteria),
    }
    sig = _sign_bundle(bundle, cfg.recs_hmac_secret)

    try:
        pipe = r.pipeline()
        pipe.set(f"{base}:mode", "canary")
        pipe.set(f"{base}:canary_symbols", ",".join(cfg.canary_symbols))
        pipe.set(f"{base}:promoted_ts_ms", str(now_ms))
        pipe.set(f"{base}:promoted_by", "of_layers_shadow_calibrator_v1")
        pipe.set(f"{base}:bundle", json.dumps(bundle, sort_keys=True))
        pipe.set(f"{base}:bundle_sig", sig)
        pipe.execute()
        c_promos.labels(layer=layer, result="ok").inc()
        return True, "applied"
    except Exception as ex:
        log.warning(f"apply_canary {layer}: {ex}")
        c_promos.labels(layer=layer, result="error").inc()
        c_errors.labels(where="apply_canary").inc()
        return False, f"error:{ex}"


def _maybe_promote(
    eval_: LayerEval,
    prev_layer_state: dict[str, Any],
    cfg: Cfg,
    gates_redis: redis.Redis | None,
    now: int,
) -> tuple[str, str | None]:
    """Возвращает (new_state, notify_kind|None).

    Auto-promote триггерится только если:
      - cfg.auto_promote_enable
      - layer ∈ cfg.auto_promote_layers
      - eval_.state == QUALIFIED
      - prev_state != CANARY_APPLIED (idempotency)
      - now - last_promote_ts >= promote_cooldown_sec
    Layer D всегда skip — требует production exec-path change.
    """
    if eval_.state != "QUALIFIED":
        return eval_.state, None

    # Layer D: ранее был hardcoded skip. С 2026-05-15 для D добавлена prod-инфра
    # (services/trade_monitor/layer_d_early_arm_hook.py + arm-request consumer).
    # Включение в allowlist делается через LAYERS_CAL_AUTO_PROMOTE_LAYERS=A,B,C,D.
    if not cfg.auto_promote_enable:
        return eval_.state, None

    if eval_.layer.upper() not in cfg.auto_promote_layers:
        return eval_.state, None

    prev_state = prev_layer_state.get("state", "")
    if prev_state == "CANARY_APPLIED":
        # уже promoted, идемпотентность
        eval_.metrics["promo_mode_numeric"] = 1.0
        return "CANARY_APPLIED", None

    last_promote_ts = _i(prev_layer_state.get("last_promote_ts"))
    if last_promote_ts and (now - last_promote_ts) < cfg.promote_cooldown_sec:
        log.info(f"layer={eval_.layer} cooldown promote "
                 f"{now-last_promote_ts}s<{cfg.promote_cooldown_sec}s")
        return "QUALIFIED", None

    applied, reason = _apply_canary(gates_redis, cfg, eval_.layer, eval_)
    if applied:
        eval_.metrics["promo_mode_numeric"] = 1.0
        eval_.suggestions.insert(
            0,
            f"✅ CANARY enforce applied: symbols={','.join(cfg.canary_symbols)}; "
            f"set {cfg.gates_key_prefix}:layer_{eval_.layer.lower()}:mode=canary"
        )
        return "CANARY_APPLIED", "canary_applied"
    else:
        eval_.suggestions.insert(
            0, f"⚠ auto-promote attempt failed: {reason}"
        )
        return "QUALIFIED", "promote_failed"


def _format_notify(e: LayerEval) -> str:
    head = f"🛡 Layer {e.layer} → {e.state}"
    lines = [head]
    if e.metrics:
        parts = []
        for k in ("total", "veto_rate", "clamp_rate", "armable_rate",
                  "wr_uplift_pp", "pnl_uplift_usd", "pnl_saved_usd",
                  "winners_lost_ratio", "winners_pnl_lost_ratio",
                  "losers_flipped"):
            if k not in e.metrics:
                continue
            v = e.metrics[k]
            if not isinstance(v, (int, float)):
                parts.append(f"{k}={v}")
                continue
            if "rate" in k or "ratio" in k:
                parts.append(f"{k}={v:.1%}")
            else:
                parts.append(f"{k}={v:+.2f}")
        if parts:
            lines.append("  " + "  ".join(parts))
    if e.issues:
        lines.append("  issues: " + "; ".join(e.issues))
    if e.suggestions:
        lines.append("  fix:")
        for s in e.suggestions:
            lines.append(f"    • {s}")
    return "\n".join(lines)


def _run_once(cfg: Cfg, redis_client: redis.Redis | None,
              gates_redis: redis.Redis | None,
              prev_state: dict[str, Any]) -> dict[str, Any]:
    evals = [
        _eval_layer_a(_load_json(cfg.layer_a_report), cfg),
        _eval_layer_b(_load_json(cfg.layer_b_report), cfg),
        _eval_layer_c(_load_json(cfg.layer_c_report), cfg),
        _eval_layer_d(_load_json(cfg.layer_d_report), cfg),
    ]

    now = int(time.time())
    new_state: dict[str, Any] = {"ts": now, "layers": {}}
    for e in evals:
        prev = (prev_state.get("layers") or {}).get(e.layer) or {}
        prev_st = prev.get("state", "")
        last_notify_ts = _i(prev.get("last_notify_ts"))
        last_promote_ts = _i(prev.get("last_promote_ts"))

        # auto-promotion (только при QUALIFIED, не для D, через cooldown)
        new_eval_state, promo_kind = _maybe_promote(e, prev, cfg, gates_redis, now)
        if new_eval_state != e.state:
            e.state = new_eval_state
            if promo_kind == "canary_applied":
                last_promote_ts = now

        # текущий mode в gates redis (для метрик/прозрачности)
        cur_mode = _read_current_mode(gates_redis, cfg.gates_key_prefix, e.layer)
        g_promo_mode.labels(layer=e.layer).set(_MODE_NUM.get(cur_mode, 0))

        kind = ""
        if e.state != prev_st:
            if e.state == "QUALIFIED":
                kind = "qualified"
            elif e.state == "NEEDS_TUNING":
                kind = "needs_tuning"
            elif e.state == "CANARY_APPLIED":
                kind = "canary_applied"
        if promo_kind and not kind:
            kind = promo_kind  # promote_failed etc.

        if kind:
            since = now - last_notify_ts
            if last_notify_ts and since < cfg.cooldown_sec and kind != "canary_applied":
                log.info(f"layer={e.layer} state={e.state} cooldown "
                         f"{since}s<{cfg.cooldown_sec}s — skip notify")
            else:
                _notify(redis_client, cfg, e.layer, kind, _format_notify(e))
                last_notify_ts = now

        new_state["layers"][e.layer] = {
            "state": e.state,
            "metrics": e.metrics,
            "criteria": e.criteria,
            "issues": e.issues,
            "suggestions": e.suggestions,
            "last_notify_ts": last_notify_ts,
            "last_promote_ts": last_promote_ts,
            "current_mode_in_gates_redis": cur_mode,
        }
        crit_ok = sum(1 for v in e.criteria.values() if v)
        crit_n  = len(e.criteria)
        log.info(f"layer={e.layer} state={e.state}  crit={crit_ok}/{crit_n}  "
                 f"gate_mode={cur_mode or 'off'}")
        for s in e.suggestions:
            log.info(f"    suggestion: {s}")

    _emit_prom(evals)
    _save_state(cfg.state_path, new_state)
    g_last_run.set(now)
    return new_state


def main() -> int:
    cfg = load_cfg()
    if not cfg.enable:
        log.info("LAYERS_CAL_ENABLE=0 — exit")
        return 0
    log.info(f"starting: interval={cfg.interval_sec}s min_trades={cfg.min_trades} "
             f"reports_dir={cfg.reports_dir}")
    try:
        start_http_server(cfg.prom_port)
        log.info(f"prometheus on :{cfg.prom_port}")
    except Exception as ex:
        log.warning(f"prom start failed: {ex}")

    redis_client: redis.Redis | None = None
    try:
        redis_client = redis.from_url(cfg.notify_redis_url, decode_responses=True)
        redis_client.ping()
        log.info(f"notify connected: {cfg.notify_redis_url}")
    except Exception as ex:
        log.warning(f"notify redis unavailable, dry-run only: {ex}")
        redis_client = None

    gates_redis: redis.Redis | None = None
    if cfg.auto_promote_enable:
        try:
            gates_redis = redis.from_url(cfg.gates_redis_url, decode_responses=True)
            gates_redis.ping()
            log.info(f"gates-redis connected: {cfg.gates_redis_url}  "
                     f"auto_promote={list(cfg.auto_promote_layers)}  "
                     f"canary_symbols={list(cfg.canary_symbols)}")
        except Exception as ex:
            log.warning(f"gates-redis unavailable, auto-promote disabled: {ex}")
            gates_redis = None
    else:
        log.info("LAYERS_CAL_AUTO_PROMOTE_ENABLE=0 — auto-promote disabled "
                 "(notify-only mode)")

    g_up.set(1)
    state = _load_state(cfg.state_path)
    while True:
        t0 = time.time()
        try:
            state = _run_once(cfg, redis_client, gates_redis, state)
        except Exception as ex:
            log.exception(f"run_once failed: {ex}")
            c_errors.labels(where="run_once").inc()
        time.sleep(max(1, cfg.interval_sec - int(time.time() - t0)))


if __name__ == "__main__":
    raise SystemExit(main())
