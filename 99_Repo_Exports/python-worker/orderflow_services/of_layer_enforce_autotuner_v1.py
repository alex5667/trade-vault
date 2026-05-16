from __future__ import annotations

"""of_layer_enforce_autotuner_v1.py

Autotuner для Layer A/B/C enforce.

Цикл:
  1. Scrape Prometheus от scanner-python-worker — counters of_layer_enforce_*.
  2. Compute activity-rate per layer/outcome за окно.
  3. Layer C: если missing-rate высокий (мало active+inactive) → пробует
     alternative feature keys через Redis-overrides (без рестарта).
  4. Сравнивает с counterfactual shadow JSON-отчётами по тренду.
  5. State machine:
       INIT → SHADOW_RUNNING → SHADOW_OK → ENFORCE_APPLIED
                              ↓
                        SHADOW_NEEDS_TUNING (Layer C feature keys)
  6. При SHADOW_OK для всех 3 слоёв (A/B/C) → пишет
     of_gate:enforce_mode_override=enforce → worker подхватит без restart.

Безопасность:
  - Default disabled (LAYER_AUTOTUNE_ENABLE=0).
  - Только если все 3 слоя SHADOW_OK ≥ min_shadow_hours.
  - Cooldown между записями.
  - Notify per state-transition.
"""

import json
import logging
import os
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import redis
import urllib.request
from prometheus_client import Counter as PCounter, Gauge, start_http_server  # type: ignore

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s [layer-autotuner] %(levelname)s %(message)s")
log = logging.getLogger(__name__)


def _env(k: str, d: str = "") -> str: return os.environ.get(k, d)
def _env_int(k: str, d: int) -> int:
    try: return int(_env(k, str(d)))
    except Exception: return d
def _env_float(k: str, d: float) -> float:
    try: return float(_env(k, str(d)))
    except Exception: return d
def _env_csv(k: str, d: str) -> tuple[str, ...]:
    return tuple(s.strip() for s in _env(k, d).split(",") if s.strip())


@dataclass
class Cfg:
    enable: bool
    interval_sec: int
    prom_scrape_url: str
    min_shadow_hours: float
    min_activity_per_hour: float
    layer_c_min_present_rate: float
    layer_c_alt_keys_leg3: tuple[str, ...]
    cooldown_sec: int
    reports_dir: str
    state_path: str
    redis_url: str
    notify_redis_url: str
    notify_stream: str
    key_prefix: str
    prom_port: int


def load_cfg() -> Cfg:
    return Cfg(
        enable                 = bool(_env_int("LAYER_AUTOTUNE_ENABLE", 0)),
        interval_sec           = _env_int("LAYER_AUTOTUNE_INTERVAL_SEC", 600),
        prom_scrape_url        = _env("LAYER_AUTOTUNE_SCRAPE_URL",
                                      "http://scanner-python-worker:8000/metrics"),
        min_shadow_hours       = _env_float("LAYER_AUTOTUNE_MIN_SHADOW_HOURS", 24.0),
        min_activity_per_hour  = _env_float("LAYER_AUTOTUNE_MIN_ACTIVITY_PER_HOUR", 5.0),
        layer_c_min_present_rate = _env_float("LAYER_AUTOTUNE_C_MIN_PRESENT_RATE", 0.5),
        layer_c_alt_keys_leg3  = _env_csv("LAYER_AUTOTUNE_C_ALT_LEG3_KEYS",
                                          "liq_pressure_boost,liq_pressure_pen,liq_pressure_veto"),
        cooldown_sec           = _env_int("LAYER_AUTOTUNE_COOLDOWN_SEC", 3600),
        reports_dir            = _env("LAYER_AUTOTUNE_REPORTS_DIR",
                                      "/var/lib/trade/of_reports"),
        state_path             = _env("LAYER_AUTOTUNE_STATE_PATH",
                                      "/var/lib/trade/of_reports/layer_autotune_state.json"),
        redis_url              = _env("LAYER_AUTOTUNE_REDIS_URL",
                                      "redis://redis-worker-1:6379/0"),
        notify_redis_url       = _env("LAYER_AUTOTUNE_NOTIFY_REDIS_URL",
                                      "redis://redis:6379/0"),
        notify_stream          = _env("LAYER_AUTOTUNE_NOTIFY_STREAM", "notify:telegram"),
        key_prefix             = _env("LAYER_AUTOTUNE_KEY_PREFIX", "of_gate"),
        prom_port              = _env_int("LAYER_AUTOTUNE_PROM_PORT", 9852),
    )


STATES = ("INIT", "SHADOW_RUNNING", "SHADOW_NEEDS_TUNING",
          "SHADOW_OK", "ENFORCE_APPLIED")
STATE_NUM = {s: i for i, s in enumerate(STATES)}

# Prometheus
g_up        = Gauge("layer_autotune_up", "autotuner loop up")
g_last_run  = Gauge("layer_autotune_last_run_ts", "last run ts")
g_state     = Gauge("layer_autotune_state", "per-layer state numeric", ["layer"])
g_activity  = Gauge("layer_autotune_activity_per_hour",
                    "events/hour per layer", ["layer"])
g_present   = Gauge("layer_autotune_present_rate",
                    "Layer C present rate (per leg)", ["leg"])
c_actions   = PCounter("layer_autotune_actions_total",
                       "tuning actions", ["kind"])
c_errors    = PCounter("layer_autotune_errors_total", "errors", ["where"])


def _scrape_prom(url: str, timeout: float = 5.0) -> dict[str, float]:
    """Минимальный parser Prometheus text-format: name{labels} -> value (counters)."""
    out: dict[str, float] = {}
    try:
        with urllib.request.urlopen(url, timeout=timeout) as resp:
            body = resp.read().decode("utf-8", errors="ignore")
    except Exception as ex:
        log.warning(f"scrape {url}: {ex}")
        c_errors.labels(where="scrape").inc()
        return out
    for line in body.splitlines():
        if not line or line.startswith("#"):
            continue
        try:
            sp = line.rfind(" ")
            if sp < 0:
                continue
            key = line[:sp]
            val = float(line[sp + 1:])
            out[key] = val
        except Exception:
            continue
    return out


def _sum_counter(metrics: dict[str, float], name: str,
                 label_filters: dict[str, str]) -> float:
    """Сумма counter-значений где все label_filters совпадают.
    Поддерживает частичный label-match: {layer="a", outcome=*}."""
    total = 0.0
    prefix = name + "{"
    for k, v in metrics.items():
        if not k.startswith(prefix):
            continue
        ok = True
        for lk, lv in label_filters.items():
            needle = f'{lk}="{lv}"'
            if needle not in k:
                ok = False
                break
        if ok:
            total += v
    return total


@dataclass
class LayerEval:
    layer: str
    state: str = "INIT"
    activity_per_hour: float = 0.0
    veto_count: float = 0.0
    pass_count: float = 0.0
    clamp_count: float = 0.0
    notes: list[str] = field(default_factory=list)


def _eval_layer_a(metrics: dict[str, float], window_hours: float,
                  cfg: Cfg) -> LayerEval:
    e = LayerEval(layer="A")
    name = "of_layer_enforce_active_total"
    veto = _sum_counter(metrics, name, {"layer": "a", "outcome": "vetoed"})
    pas  = _sum_counter(metrics, name, {"layer": "a", "outcome": "pass"})
    total = veto + pas
    e.veto_count = veto
    e.pass_count = pas
    e.activity_per_hour = total / max(0.5, window_hours)
    if total < 1:
        e.state = "SHADOW_RUNNING"
        e.notes.append(f"insufficient activity ({total:.0f} events)")
    elif e.activity_per_hour < cfg.min_activity_per_hour:
        e.state = "SHADOW_RUNNING"
        e.notes.append(f"activity={e.activity_per_hour:.1f}/h below threshold")
    else:
        e.state = "SHADOW_OK"
    return e


def _eval_layer_b(metrics: dict[str, float], window_hours: float,
                  cfg: Cfg) -> LayerEval:
    e = LayerEval(layer="B")
    name = "of_layer_enforce_active_total"
    clamp = _sum_counter(metrics, name, {"layer": "b", "outcome": "clamped"})
    pas   = _sum_counter(metrics, name, {"layer": "b", "outcome": "pass"})
    total = clamp + pas
    e.clamp_count = clamp
    e.pass_count  = pas
    e.activity_per_hour = total / max(0.5, window_hours)
    if e.activity_per_hour < cfg.min_activity_per_hour:
        e.state = "SHADOW_RUNNING"
        e.notes.append(f"activity={e.activity_per_hour:.1f}/h below threshold")
    else:
        e.state = "SHADOW_OK"
    return e


def _eval_layer_c(
    metrics: dict[str, float], window_hours: float, cfg: Cfg,
    shadow_report_c: dict[str, Any] | None,
) -> tuple[LayerEval, list[str]]:
    """Возвращает (eval, suggested_leg3_key_alternatives).
    Если Layer C сильно missing — sgggest альтернативные feature keys."""
    e = LayerEval(layer="C")
    name = "of_layer_enforce_active_total"
    veto = _sum_counter(metrics, name, {"layer": "c", "outcome": "vetoed"})
    pas  = _sum_counter(metrics, name, {"layer": "c", "outcome": "pass"})
    total = veto + pas
    e.veto_count = veto
    e.pass_count = pas
    e.activity_per_hour = total / max(0.5, window_hours)

    suggestions: list[str] = []
    # Из shadow report берём per-leg missing-rate
    leg3_missing_rate = 1.0
    if shadow_report_c and isinstance(shadow_report_c.get("per_leg"), dict):
        per_leg = shadow_report_c["per_leg"]
        tot_trades = float(shadow_report_c.get("total", 0) or 0)
        if tot_trades > 0:
            for leg_name, stats in per_leg.items():
                if not isinstance(stats, dict):
                    continue
                miss = float(stats.get("missing", 0) or 0) / tot_trades
                present_rate = 1.0 - miss
                g_present.labels(leg=leg_name).set(present_rate)
                if leg_name.lower().startswith("liq"):
                    leg3_missing_rate = miss

    if leg3_missing_rate > 1.0 - cfg.layer_c_min_present_rate:
        suggestions = list(cfg.layer_c_alt_keys_leg3)
        e.state = "SHADOW_NEEDS_TUNING"
        e.notes.append(f"leg3 missing={leg3_missing_rate:.1%} (high); alt keys to try")
    elif e.activity_per_hour < cfg.min_activity_per_hour:
        e.state = "SHADOW_RUNNING"
        e.notes.append(f"activity={e.activity_per_hour:.1f}/h below threshold")
    else:
        e.state = "SHADOW_OK"
    return e, suggestions


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
        log.warning(f"state save: {ex}")
        c_errors.labels(where="save_state").inc()


def _load_report(path: str) -> dict[str, Any] | None:
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def _notify(r: redis.Redis | None, cfg: Cfg, text: str) -> None:
    if r is None:
        log.info(f"[NOTIFY-DRYRUN] {text}")
        return
    try:
        r.xadd(cfg.notify_stream,
               {"payload": json.dumps({"text": text, "source": "layer-autotuner"})},
               maxlen=1000)
    except Exception as ex:
        log.warning(f"notify: {ex}")
        c_errors.labels(where="notify").inc()


def _apply_leg3_alt_key(gates_redis: redis.Redis | None, cfg: Cfg,
                       alt_key: str) -> bool:
    """Записывает Redis-override of_gate:layer_c:leg3_key_override=<alt_key>."""
    if gates_redis is None:
        log.warning("gates_redis unavailable — cannot apply leg key override")
        return False
    try:
        gates_redis.set(f"{cfg.key_prefix}:layer_c:leg3_key_override", alt_key)
        gates_redis.set(f"{cfg.key_prefix}:layer_c:leg3_key_override_ts_ms",
                        str(int(time.time() * 1000)))
        c_actions.labels(kind="leg3_key_override").inc()
        log.info(f"applied leg3_key_override={alt_key}")
        return True
    except Exception as ex:
        log.warning(f"apply_leg3: {ex}")
        c_errors.labels(where="apply_leg3").inc()
        return False


def _apply_enforce_mode(gates_redis: redis.Redis | None, cfg: Cfg,
                       mode: str) -> bool:
    """Записывает hot-override OF_LAYER_ENFORCE_MODE через Redis."""
    if gates_redis is None:
        return False
    try:
        gates_redis.set(f"{cfg.key_prefix}:enforce_mode_override", mode)
        gates_redis.set(f"{cfg.key_prefix}:enforce_mode_override_ts_ms",
                        str(int(time.time() * 1000)))
        gates_redis.set(f"{cfg.key_prefix}:enforce_mode_override_by",
                        "of_layer_enforce_autotuner_v1")
        c_actions.labels(kind=f"mode_{mode}").inc()
        log.info(f"applied enforce_mode_override={mode}")
        return True
    except Exception as ex:
        log.warning(f"apply_mode: {ex}")
        c_errors.labels(where="apply_mode").inc()
        return False


def _emit_prom(evals: list[LayerEval]) -> None:
    for e in evals:
        g_state.labels(layer=e.layer).set(STATE_NUM.get(e.state, 0))
        g_activity.labels(layer=e.layer).set(e.activity_per_hour)


def _run_once(cfg: Cfg, gates_redis: redis.Redis | None,
              notify_redis: redis.Redis | None,
              prev_state: dict[str, Any]) -> dict[str, Any]:
    metrics = _scrape_prom(cfg.prom_scrape_url)
    window = cfg.min_shadow_hours
    shadow_c = _load_report(f"{cfg.reports_dir}/of_layer_c_shadow.json")

    eval_a = _eval_layer_a(metrics, window, cfg)
    eval_b = _eval_layer_b(metrics, window, cfg)
    eval_c, leg3_alts = _eval_layer_c(metrics, window, cfg, shadow_c)
    evals = [eval_a, eval_b, eval_c]
    _emit_prom(evals)

    now = int(time.time())
    new_state: dict[str, Any] = {
        "ts": now,
        "layers": {
            e.layer: {
                "state": e.state,
                "activity_per_hour": e.activity_per_hour,
                "veto_count": e.veto_count,
                "pass_count": e.pass_count,
                "clamp_count": e.clamp_count,
                "notes": e.notes,
            }
            for e in evals
        },
    }

    prev_layers = prev_state.get("layers") or {}
    last_action_ts = int(prev_state.get("last_action_ts") or 0)
    cooldown_active = last_action_ts and (now - last_action_ts) < cfg.cooldown_sec

    # Action 1: Layer C needs tuning → пробуем альтернативный leg3 key.
    if eval_c.state == "SHADOW_NEEDS_TUNING" and leg3_alts and not cooldown_active:
        prev_c = prev_layers.get("C") or {}
        tried = list(prev_c.get("tried_alt_keys") or [])
        next_key = next((k for k in leg3_alts if k not in tried), None)
        if next_key:
            if _apply_leg3_alt_key(gates_redis, cfg, next_key):
                tried.append(next_key)
                new_state["layers"]["C"]["tried_alt_keys"] = tried
                new_state["last_action_ts"] = now
                _notify(notify_redis, cfg,
                    f"🛡 Layer C autotune: try leg3_key={next_key} "
                    f"(missing detected). Wait {cfg.cooldown_sec}s for re-eval.")
        else:
            new_state["layers"]["C"]["tried_alt_keys"] = tried
            _notify(notify_redis, cfg,
                "⚠ Layer C: все альтернативные ключи испробованы, нет данных. "
                f"Попробуйте OF_LAYER_C_ENFORCE_LEG3_ENABLED=0 вручную. Tried: {tried}")

    # Action 2: все 3 слоя SHADOW_OK + min_shadow_hours прошло → ENFORCE.
    all_ok = all(e.state == "SHADOW_OK" for e in evals)
    prev_all_ok_ts = int(prev_state.get("all_ok_since_ts") or 0)
    if all_ok:
        if prev_all_ok_ts == 0:
            prev_all_ok_ts = now
        new_state["all_ok_since_ts"] = prev_all_ok_ts

        shadow_elapsed_h = (now - prev_all_ok_ts) / 3600.0
        prev_promoted = prev_state.get("promoted_to_enforce", False)
        if shadow_elapsed_h >= cfg.min_shadow_hours and not prev_promoted:
            if _apply_enforce_mode(gates_redis, cfg, "enforce"):
                new_state["promoted_to_enforce"] = True
                new_state["promoted_ts"] = now
                for layer in ("A", "B", "C"):
                    new_state["layers"][layer]["state"] = "ENFORCE_APPLIED"
                _notify(notify_redis, cfg,
                    f"✅ Layer A/B/C autotune → ENFORCE applied.\n"
                    f"  Shadow OK across {shadow_elapsed_h:.1f}h.\n"
                    f"  Hot-config via Redis: {cfg.key_prefix}:enforce_mode_override=enforce")
        elif not prev_promoted:
            _notify(notify_redis, cfg,
                f"🟢 Все слои SHADOW_OK. До enforce: "
                f"{cfg.min_shadow_hours - shadow_elapsed_h:.1f}h.")
    else:
        # сброс счётчика если хоть один слой выпал
        new_state["all_ok_since_ts"] = 0

    _save_state(cfg.state_path, new_state)
    g_last_run.set(now)

    log.info("layers: A=%s B=%s C=%s  activity(/h): A=%.1f B=%.1f C=%.1f  cooldown_active=%s",
             eval_a.state, eval_b.state, eval_c.state,
             eval_a.activity_per_hour, eval_b.activity_per_hour,
             eval_c.activity_per_hour, bool(cooldown_active))
    return new_state


def main() -> int:
    cfg = load_cfg()
    if not cfg.enable:
        log.info("LAYER_AUTOTUNE_ENABLE=0 — exit")
        return 0
    log.info(f"starting: interval={cfg.interval_sec}s min_shadow_h={cfg.min_shadow_hours} "
             f"scrape={cfg.prom_scrape_url}")
    try:
        start_http_server(cfg.prom_port)
        log.info(f"prometheus on :{cfg.prom_port}")
    except Exception as ex:
        log.warning(f"prom: {ex}")

    gates_redis: redis.Redis | None = None
    try:
        gates_redis = redis.from_url(cfg.redis_url, decode_responses=True)
        gates_redis.ping()
    except Exception as ex:
        log.warning(f"gates redis unavailable: {ex}")
        gates_redis = None

    notify_redis: redis.Redis | None = None
    try:
        notify_redis = redis.from_url(cfg.notify_redis_url, decode_responses=True)
        notify_redis.ping()
    except Exception as ex:
        log.warning(f"notify redis unavailable: {ex}")
        notify_redis = None

    g_up.set(1)
    state = _load_state(cfg.state_path)
    while True:
        t0 = time.time()
        try:
            state = _run_once(cfg, gates_redis, notify_redis, state)
        except Exception as ex:
            log.exception(f"run_once: {ex}")
            c_errors.labels(where="run_once").inc()
        time.sleep(max(1, cfg.interval_sec - int(time.time() - t0)))


if __name__ == "__main__":
    raise SystemExit(main())
