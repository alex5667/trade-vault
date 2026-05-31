from __future__ import annotations

"""meta_model_calibrator_v1.py

Периодически оценивает калибровку meta-модели, при необходимости дообучает
калибратор (temp_logit / platt_logit) и авто-переключает режим
META_MODEL_ENABLE → ENFORCE, если все критерии приёмки пройдены.

Criteria (min bars for ENFORCE):
  holdout_auc       >= V14_CAL_MIN_AUC         (default 0.62)
  expectancy_r_top5 >  0.0                     (positive expectancy)
  ece_cal           <= V14_CAL_MAX_ECE          (default 0.10)
  brier_cal         <= V14_CAL_MAX_BRIER        (default 0.22)
  artifact_age_days <= V14_CAL_MAX_ARTIFACT_AGE (default 7)
  n_samples         >= V14_CAL_MIN_N            (default 200)

Shadow window before ENFORCE:
  V14_CAL_SHADOW_MIN_HOURS >= 48 (default 48)

Telegram уведомления: xadd → notify:telegram stream (Redis main).

State file: V14_CAL_STATE_PATH (default /var/lib/trade/of_reports/meta_model_calibrator_state.json)
Output calibrator: V14_CAL_OUT_DIR/<calibrator_v14_of_<ts>.json>

Запуск:
  python -m orderflow_services.meta_model_calibrator_v1
  python -m orderflow_services.meta_model_calibrator_v1 --apply 1
  python -m orderflow_services.meta_model_calibrator_v1 --dataset /tmp/ml_dataset_v14.jsonl --apply 1
"""

import argparse
import json
import math
import os
import time
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


# ---------------------------------------------------------------------------
# Pure-math helpers (no external deps)
# ---------------------------------------------------------------------------

def _clamp01(x: float) -> float:
    if x <= 0.0: return 0.0
    if x >= 1.0: return 1.0
    return x

def _sigmoid(z: float) -> float:
    if z >= 0:
        e = math.exp(-z)
        return 1.0 / (1.0 + e)
    e = math.exp(z)
    return e / (1.0 + e)

def _logit(p: float, eps: float = 1e-7) -> float:
    p = max(eps, min(1.0 - eps, p))
    return math.log(p / (1.0 - p))

def _nll_temp(probs: list[float], labels: list[int], T: float, eps: float = 1e-7) -> float:
    if T <= 1e-9:
        return 1e9
    s = 0.0
    for p, y in zip(probs, labels):
        z = _logit(p, eps) / T
        pc = _sigmoid(z)
        pc = max(eps, min(1.0 - eps, pc))
        s -= (y * math.log(pc) + (1 - y) * math.log(1.0 - pc))
    return s / max(1, len(probs))

def _nll_platt(probs: list[float], labels: list[int], a: float, b: float, eps: float = 1e-7) -> float:
    s = 0.0
    for p, y in zip(probs, labels):
        z = a * _logit(p, eps) + b
        pc = _sigmoid(z)
        pc = max(eps, min(1.0 - eps, pc))
        s -= (y * math.log(pc) + (1 - y) * math.log(1.0 - pc))
    return s / max(1, len(probs))

def _golden_search(f, lo: float, hi: float, tol: float = 1e-5, max_iter: int = 200) -> float:
    phi = (math.sqrt(5) - 1) / 2
    c, d = hi - phi * (hi - lo), lo + phi * (hi - lo)
    for _ in range(max_iter):
        if abs(hi - lo) < tol:
            break
        if f(c) < f(d):
            hi = d
        else:
            lo = c
        c, d = hi - phi * (hi - lo), lo + phi * (hi - lo)
    return (lo + hi) / 2.0

def fit_temp_logit(probs: list[float], labels: list[int]) -> float:
    """Golden-section search for optimal temperature T ∈ [0.05, 20]."""
    return _golden_search(lambda T: _nll_temp(probs, labels, T), 0.05, 20.0)

def fit_platt_logit(probs: list[float], labels: list[int]) -> tuple[float, float]:
    """Grid-descent for Platt params (a, b). Falls back to scipy if available."""
    try:
        from scipy.optimize import minimize  # type: ignore
        res = minimize(
            lambda ab: _nll_platt(probs, labels, ab[0], ab[1]),
            x0=[1.0, 0.0],
            method="Nelder-Mead",
            options={"xatol": 1e-5, "fatol": 1e-6, "maxiter": 2000},
        )
        return float(res.x[0]), float(res.x[1])
    except Exception:
        # Fallback: fix b=0 and search a
        best_a = _golden_search(lambda a: _nll_platt(probs, labels, a, 0.0), 0.1, 5.0)
        return best_a, 0.0

def apply_calibrator(p: float, cal_type: str, t: float = 1.0,
                     a: float = 1.0, b: float = 0.0, eps: float = 1e-7) -> float:
    if cal_type == "identity":
        return p
    z = _logit(p, eps)
    if cal_type == "temp_logit":
        return _sigmoid(z / max(t, 1e-9))
    if cal_type == "platt_logit":
        return _sigmoid(a * z + b)
    return p


# ---------------------------------------------------------------------------
# Metrics (pure Python, no numpy/sklearn required)
# ---------------------------------------------------------------------------

def compute_ece(probs: list[float], labels: list[int], n_bins: int = 10) -> float:
    bins: list[list[tuple[float, int]]] = [[] for _ in range(n_bins)]
    for p, y in zip(probs, labels):
        idx = min(int(p * n_bins), n_bins - 1)
        bins[idx].append((p, y))
    ece = 0.0
    n = len(probs)
    if n == 0:
        return 1.0
    for b in bins:
        if not b:
            continue
        acc = sum(y for _, y in b) / len(b)
        conf = sum(p for p, _ in b) / len(b)
        ece += len(b) / n * abs(acc - conf)
    return ece

def compute_brier(probs: list[float], labels: list[int]) -> float:
    if not probs:
        return 1.0
    return sum((p - y) ** 2 for p, y in zip(probs, labels)) / len(probs)

def compute_auc(probs: list[float], labels: list[int]) -> float:
    """Wilcoxon-Mann-Whitney AUC (O(n log n))."""
    n = len(probs)
    if n < 2:
        return 0.5
    pos = [p for p, y in zip(probs, labels) if y == 1]
    neg = [p for p, y in zip(probs, labels) if y == 0]
    if not pos or not neg:
        return 0.5
    # rank-sum approach
    pairs = [(p, 1) for p in pos] + [(p, 0) for p in neg]
    pairs.sort(key=lambda x: x[0])
    rank_sum = 0.0
    for i, (_, lbl) in enumerate(pairs):
        if lbl == 1:
            rank_sum += i + 1
    n1, n0 = len(pos), len(neg)
    u = rank_sum - n1 * (n1 + 1) / 2
    return u / (n1 * n0)

def compute_expectancy_r_top5pct(probs: list[float], labels: list[int],
                                  r_values: list[float] | None = None) -> float:
    """Mean R (or win-rate based) of top-5% predictions by confidence."""
    if not probs:
        return -999.0
    n = len(probs)
    k = max(1, int(n * 0.05))
    indexed = sorted(enumerate(probs), key=lambda x: -x[1])
    top_idx = [i for i, _ in indexed[:k]]
    if r_values:
        return sum(r_values[i] for i in top_idx) / k
    # fallback: 2*winrate - 1 as expectancy proxy
    hits = sum(labels[i] for i in top_idx)
    return 2.0 * hits / k - 1.0

def compute_metrics(probs: list[float], labels: list[int],
                    r_values: list[float] | None = None) -> dict[str, float]:
    return {
        "ece": compute_ece(probs, labels),
        "brier": compute_brier(probs, labels),
        "auc": compute_auc(probs, labels),
        "expectancy_r_top5pct": compute_expectancy_r_top5pct(probs, labels, r_values),
        "n": float(len(probs)),
        "pos_rate": float(sum(labels) / max(1, len(labels))),
    }


# ---------------------------------------------------------------------------
# Gate check
# ---------------------------------------------------------------------------

@dataclass
class GateConfig:
    min_auc: float = 0.62
    max_ece: float = 0.10
    max_brier: float = 0.22
    min_n: int = 200
    max_artifact_age_days: float = 7.0
    shadow_min_hours: float = 48.0

    @staticmethod
    def from_env() -> "GateConfig":
        return GateConfig(
            min_auc=_env_float("V14_CAL_MIN_AUC", 0.62),
            max_ece=_env_float("V14_CAL_MAX_ECE", 0.10),
            max_brier=_env_float("V14_CAL_MAX_BRIER", 0.22),
            min_n=_env_int("V14_CAL_MIN_N", 200),
            max_artifact_age_days=_env_float("V14_CAL_MAX_ARTIFACT_AGE", 7.0),
            shadow_min_hours=_env_float("V14_CAL_SHADOW_MIN_HOURS", 48.0),
        )

def check_gates(metrics: dict[str, float], cfg: GateConfig,
                artifact_age_days: float | None = None,
                shadow_hours: float | None = None) -> tuple[bool, list[str]]:
    blockers: list[str] = []
    n = int(metrics.get("n", 0))
    if n < cfg.min_n:
        blockers.append(f"n={n} < min_n={cfg.min_n}")
    auc = metrics.get("auc", 0.0)
    if auc < cfg.min_auc:
        blockers.append(f"auc={auc:.4f} < {cfg.min_auc}")
    ece = metrics.get("ece", 1.0)
    if ece > cfg.max_ece:
        blockers.append(f"ece={ece:.4f} > {cfg.max_ece}")
    brier = metrics.get("brier", 1.0)
    if brier > cfg.max_brier:
        blockers.append(f"brier={brier:.4f} > {cfg.max_brier}")
    exp_r = metrics.get("expectancy_r_top5pct", -999.0)
    if exp_r <= 0.0:
        blockers.append(f"expectancy_r_top5pct={exp_r:.4f} <= 0")
    if artifact_age_days is not None and artifact_age_days > cfg.max_artifact_age_days:
        blockers.append(f"artifact_age={artifact_age_days:.1f}d > {cfg.max_artifact_age_days}d")
    if shadow_hours is not None and shadow_hours < cfg.shadow_min_hours:
        blockers.append(f"shadow_hours={shadow_hours:.1f} < {cfg.shadow_min_hours}")
    return (len(blockers) == 0), blockers


# ---------------------------------------------------------------------------
# Data loading — Redis streams join (inputs × labels) OR dataset NDJSON file
# ---------------------------------------------------------------------------

@dataclass
class SampleRow:
    sid: str
    p_hat: float       # raw model prediction (confidence)
    outcome: int       # 1 = edge, 0 = no-edge (cost-aware TB label)
    r_value: float     # realized R (edge_after_cost_bps proxy, optional)


def _norm_sid(s: str) -> str:
    for prefix in ("crypto-of:", "of:"):
        if s.startswith(prefix):
            return s[len(prefix):]
    return s


def load_from_dataset_ndjson(path: str) -> list[SampleRow]:
    """Load from ml_dataset_v14.jsonl produced by nightly_v14_of_train_bundle."""
    rows: list[SampleRow] = []
    try:
        with open(path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                except Exception:
                    continue
                ind = rec.get("indicators") or {}
                p_hat = float(ind.get("confidence") or ind.get("of_score_final") or 0.5)
                y_raw = rec.get("y_edge_cost_aware") or rec.get("y_edge") or 0
                try:
                    y = int(y_raw)
                except Exception:
                    y = 0
                r_val = float(ind.get("edge_after_cost_bps") or 0.0)
                sid = _norm_sid(str(rec.get("sid") or ""))
                if not sid:
                    continue
                rows.append(SampleRow(sid=sid, p_hat=_clamp01(p_hat), outcome=y, r_value=r_val))
    except Exception as exc:
        _log(f"load_from_dataset_ndjson error: {exc}")
    return rows


def load_from_redis_streams(
    redis_url: str,
    inputs_stream: str,
    labels_stream: str,
    max_records: int = 5000,
    since_hours: float = 72.0,
) -> list[SampleRow]:
    """Join signals:of:inputs × labels:tb by sid."""
    try:
        import redis as redis_lib  # type: ignore
        r = redis_lib.Redis.from_url(redis_url, decode_responses=True)
    except Exception as exc:
        _log(f"Redis connect failed: {exc}")
        return []

    now_ms = int(time.time() * 1000)
    since_ms = now_ms - int(since_hours * 3600 * 1000)
    start_id = f"{since_ms}-0"

    def _xread(stream: str) -> list[tuple[str, dict[str, str]]]:
        try:
            from typing import cast as _cast
            res = _cast(list, r.xrange(stream, min=start_id, max="+", count=max_records))  # type: ignore[arg-type]
            out: list[tuple[str, dict[str, str]]] = []
            for item in (res or []):
                msg_id, flds = item[0], item[1]
                out.append((str(msg_id), dict(flds)))
            return out
        except Exception as exc:
            _log(f"xrange {stream} error: {exc}")
            return []

    # Load inputs → p_hat by sid
    p_by_sid: dict[str, float] = {}
    for _, fields in _xread(inputs_stream):
        try:
            payload_raw = str(fields.get("payload") or fields.get("data") or "")
            payload: dict[str, Any]
            if payload_raw:
                payload = json.loads(payload_raw)
            else:
                payload = dict(fields)
            sid = _norm_sid(str(payload.get("sid") or fields.get("sid") or ""))
            if not sid:
                continue
            ind = payload.get("indicators") or {}
            conf = float((ind if isinstance(ind, dict) else {}).get("confidence") or
                         (ind if isinstance(ind, dict) else {}).get("of_score_final") or 0.5)
            p_by_sid[sid] = _clamp01(conf)
        except Exception:
            continue

    if not p_by_sid:
        _log("No inputs found in stream")
        return []

    # Load labels → outcome by sid
    rows: list[SampleRow] = []
    for _, fields in _xread(labels_stream):
        try:
            payload_raw = fields.get("payload") or fields.get("data") or ""
            if payload_raw:
                payload = json.loads(payload_raw)
            else:
                payload = fields
            # skip secondary horizons
            prim = payload.get("primary", 1)
            if isinstance(prim, dict):
                prim = prim.get("flag", 1)
            if not int(prim or 1):
                continue
            sid = _norm_sid(str(payload.get("sid") or fields.get("sid") or ""))
            if sid not in p_by_sid:
                continue
            y_raw = payload.get("y_edge_cost_aware") or payload.get("y_edge") or 0
            y = int(y_raw)
            r_val = float(payload.get("edge_after_cost_bps") or 0.0)
            rows.append(SampleRow(sid=sid, p_hat=p_by_sid[sid], outcome=y, r_value=r_val))
        except Exception:
            continue

    _log(f"Loaded {len(rows)} joined samples (inputs={len(p_by_sid)})")
    return rows


# ---------------------------------------------------------------------------
# Model artifact helpers
# ---------------------------------------------------------------------------

def load_champion_path_from_redis(redis_url: str, champion_key: str) -> tuple[str, float | None]:
    """Returns (model_path, artifact_age_days). age=None if key missing."""
    try:
        import redis as redis_lib  # type: ignore
        r = redis_lib.Redis.from_url(redis_url, decode_responses=True)
        raw = r.get(champion_key)  # type: ignore[assignment]
        if not raw:
            return "", None
        cfg = json.loads(str(raw))
        path = str(cfg.get("model_path") or "")
        created_ms = float(cfg.get("created_ms") or 0)
        age_days = None
        if created_ms > 0:
            age_days = (time.time() * 1000 - created_ms) / (86400 * 1000)
        return path, age_days
    except Exception as exc:
        _log(f"load_champion_path error: {exc}")
        return "", None


def load_shadow_start_ms_from_redis(redis_url: str, shadow_key: str) -> float | None:
    """Read shadow mode start timestamp for computing shadow_hours."""
    try:
        import redis as redis_lib  # type: ignore
        r = redis_lib.Redis.from_url(redis_url, decode_responses=True)
        raw = r.get(shadow_key)  # type: ignore[assignment]
        if not raw:
            return None
        d = json.loads(str(raw))
        return float(d.get("shadow_start_ms") or d.get("created_ms") or 0) or None
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Logging helper (simple, no external dep)
# ---------------------------------------------------------------------------

def _log(msg: str) -> None:
    ts = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
    print(f"[{ts}] [meta_model_calibrator] {msg}", flush=True)


# ---------------------------------------------------------------------------
# Calibrator JSON builder (compatible with ConfidenceCalibrator schema)
# ---------------------------------------------------------------------------

def build_calibrator_json(
    cal_type: str,
    t: float = 1.0,
    a: float = 1.0,
    b: float = 0.0,
    raw_metrics: dict[str, float] | None = None,
    cal_metrics: dict[str, float] | None = None,
    run_id: str = "",
) -> dict[str, Any]:
    now_ms = int(time.time() * 1000)
    payload: dict[str, Any] = {
        "schema_version": 1,
        "type": cal_type,
        "eps": 1e-7,
        "created_ms": now_ms,
        "run_id": run_id or f"meta_cal_{time.strftime('%Y%m%d_%H%M%S', time.gmtime())}",
        "train_report": {
            "raw": raw_metrics or {},
            "cal": cal_metrics or {},
        },
    }
    if cal_type == "temp_logit":
        payload["t"] = t
    elif cal_type == "platt_logit":
        payload["a"] = a
        payload["b"] = b
    return payload


# ---------------------------------------------------------------------------
# Atomic state file helpers
# ---------------------------------------------------------------------------

def _atomic_write_json(path: str, obj: dict[str, Any]) -> None:
    import os as _os
    _os.makedirs(_os.path.dirname(path) or ".", exist_ok=True)
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)
    _os.replace(tmp, path)


def _load_json_safe(path: str) -> dict[str, Any]:
    try:
        if path and os.path.exists(path):
            with open(path, encoding="utf-8") as f:
                obj = json.load(f)
            return obj if isinstance(obj, dict) else {}
    except Exception:
        pass
    return {}


# ---------------------------------------------------------------------------
# Telegram notification (direct xadd — no ReportingService import needed)
# ---------------------------------------------------------------------------

def _notify_telegram(
    redis_url: str,
    text: str,
    severity: str = "info",
    dedup_key: str | None = None,
    notify_stream: str = "notify:telegram",
) -> None:
    try:
        import redis as redis_lib  # type: ignore
        r = redis_lib.Redis.from_url(redis_url, decode_responses=True)
        # circuit breaker
        q_len = r.xlen(notify_stream)
        if isinstance(q_len, int) and q_len > 10_000:
            _log("Telegram stream overloaded, dropping notification")
            return
        now_ms = int(time.time() * 1000)
        if dedup_key:
            d_key = f"dedup:reporting:{dedup_key}"
            if not r.set(d_key, "1", nx=True, ex=6 * 3600):
                return  # dedup hit
        msg: dict[str, str] = {
            "type": "report",
            "text": text,
            "parse_mode": "HTML",
            "source": "meta_model_calibrator_v1",
            "severity": severity,
            "timestamp": str(now_ms),
        }
        if dedup_key:
            msg["dedup_key"] = dedup_key
        r.xadd(notify_stream, msg, maxlen=5000)  # type: ignore[arg-type]
    except Exception as exc:
        _log(f"Telegram notify error: {exc}")


# ---------------------------------------------------------------------------
# Core calibration workflow
# ---------------------------------------------------------------------------

def _train_val_split(
    rows: list[SampleRow], val_frac: float = 0.3, seed: int = 42
) -> tuple[list[SampleRow], list[SampleRow]]:
    import random
    rng = random.Random(seed)
    shuffled = list(rows)
    rng.shuffle(shuffled)
    k = max(1, int(len(shuffled) * (1.0 - val_frac)))
    return shuffled[:k], shuffled[k:]


def run_calibration(
    rows: list[SampleRow],
    prefer_method: str = "temp_logit",
) -> dict[str, Any]:
    """
    Full calibration pipeline on given samples.
    Returns dict with: raw_metrics, cal_metrics, cal_type, t, a, b, cal_payload.
    """
    probs = [r.p_hat for r in rows]
    labels = [r.outcome for r in rows]
    r_vals = [r.r_value for r in rows]

    train_rows, val_rows = _train_val_split(rows, val_frac=0.3)
    tr_p = [r.p_hat for r in train_rows]
    tr_y = [r.outcome for r in train_rows]
    val_p = [r.p_hat for r in val_rows]
    val_y = [r.outcome for r in val_rows]
    val_r = [r.r_value for r in val_rows]

    raw_metrics = compute_metrics(probs, labels, r_vals)
    _log(f"Raw metrics (n={len(probs)}): ECE={raw_metrics['ece']:.4f} "
         f"Brier={raw_metrics['brier']:.4f} AUC={raw_metrics['auc']:.4f} "
         f"ExpR5={raw_metrics['expectancy_r_top5pct']:.4f}")

    # Fit calibrator on train split
    t_val = a_val = b_val = None
    cal_type = "identity"

    if prefer_method == "platt_logit":
        try:
            a_val, b_val = fit_platt_logit(tr_p, tr_y)
            cal_type = "platt_logit"
        except Exception as exc:
            _log(f"platt_logit fit failed ({exc}), falling back to temp_logit")
            prefer_method = "temp_logit"

    if prefer_method == "temp_logit" or cal_type == "identity":
        try:
            t_val = fit_temp_logit(tr_p, tr_y)
            cal_type = "temp_logit"
        except Exception as exc:
            _log(f"temp_logit fit failed ({exc}), using identity")
            cal_type = "identity"

    # Evaluate calibrated on val split
    cal_val_p: list[float]
    if cal_type == "temp_logit" and t_val is not None:
        cal_val_p = [apply_calibrator(p, "temp_logit", t=t_val) for p in val_p]
        _log(f"Fitted temp_logit T={t_val:.4f}")
    elif cal_type == "platt_logit" and a_val is not None and b_val is not None:
        cal_val_p = [apply_calibrator(p, "platt_logit", a=a_val, b=b_val) for p in val_p]
        _log(f"Fitted platt_logit a={a_val:.4f} b={b_val:.4f}")
    else:
        cal_val_p = list(val_p)

    cal_metrics = compute_metrics(cal_val_p, val_y, val_r)
    _log(f"Cal metrics (val n={len(val_p)}): ECE={cal_metrics['ece']:.4f} "
         f"Brier={cal_metrics['brier']:.4f} AUC={cal_metrics['auc']:.4f} "
         f"ExpR5={cal_metrics['expectancy_r_top5pct']:.4f}")

    ts_str = time.strftime("%Y%m%d_%H%M%S", time.gmtime())
    cal_payload = build_calibrator_json(
        cal_type=cal_type,
        t=t_val or 1.0,
        a=a_val or 1.0,
        b=b_val or 0.0,
        raw_metrics={k: float(v) for k, v in raw_metrics.items()},
        cal_metrics={k: float(v) for k, v in cal_metrics.items()},
        run_id=f"meta_cal_v14_of_{ts_str}",
    )

    return {
        "raw_metrics": raw_metrics,
        "cal_metrics": cal_metrics,
        "cal_type": cal_type,
        "t": t_val,
        "a": a_val,
        "b": b_val,
        "cal_payload": cal_payload,
        "n_total": len(rows),
        "n_train": len(train_rows),
        "n_val": len(val_rows),
        "ts_str": ts_str,
    }


# ---------------------------------------------------------------------------
# Redis promotion helpers
# ---------------------------------------------------------------------------

def _write_calibrator_to_redis(
    redis_url: str,
    cal_key: str,
    cal_payload: dict[str, Any],
) -> bool:
    try:
        import redis as redis_lib  # type: ignore
        r = redis_lib.Redis.from_url(redis_url, decode_responses=True)
        r.set(cal_key, json.dumps(cal_payload, ensure_ascii=False, separators=(",", ":")))
        return True
    except Exception as exc:
        _log(f"write calibrator to Redis failed: {exc}")
        return False


def _update_champion_mode(
    redis_url: str,
    champion_key: str,
    mode: str,
) -> bool:
    """Flip mode field in cfg:ml_confirm:champion (SHADOW → ENFORCE)."""
    try:
        import redis as redis_lib  # type: ignore
        r = redis_lib.Redis.from_url(redis_url, decode_responses=True)
        raw = r.get(champion_key)  # type: ignore[assignment]
        if not raw:
            _log(f"champion key {champion_key} missing, cannot update mode")
            return False
        cfg_obj: dict[str, Any] = json.loads(str(raw))
        old_mode = cfg_obj.get("mode", "SHADOW")
        cfg_obj["mode"] = mode
        cfg_obj["mode_updated_ms"] = int(time.time() * 1000)
        r.set(champion_key, json.dumps(cfg_obj, ensure_ascii=False, separators=(",", ":")))
        _log(f"Champion mode updated: {old_mode} → {mode}")
        return True
    except Exception as exc:
        _log(f"update_champion_mode failed: {exc}")
        return False


# ---------------------------------------------------------------------------
# Telegram message builders
# ---------------------------------------------------------------------------

def _fmt_blocked_msg(blockers: list[str], raw: dict[str, float], cal: dict[str, float],
                     n: int, shadow_hours: float | None) -> str:
    lines = [
        "🚫 <b>Meta Model Calibrator — ENFORCE заблокирован</b>",
        "",
        f"<b>Сэмплов:</b> {n}",
    ]
    if shadow_hours is not None:
        lines.append(f"<b>Shadow:</b> {shadow_hours:.1f}ч")
    lines += [
        "",
        "<b>Метрики (raw → cal):</b>",
        f"  ECE:    {raw.get('ece', 0):.4f} → {cal.get('ece', 0):.4f}",
        f"  Brier:  {raw.get('brier', 0):.4f} → {cal.get('brier', 0):.4f}",
        f"  AUC:    {raw.get('auc', 0):.4f} → {cal.get('auc', 0):.4f}",
        f"  ExpR5%: {raw.get('expectancy_r_top5pct', 0):.4f} → "
        f"{cal.get('expectancy_r_top5pct', 0):.4f}",
        "",
        "<b>Блокеры:</b>",
    ]
    for b in blockers:
        lines.append(f"  ❌ {b}")
    return "\n".join(lines)


def _fmt_promoted_msg(cal_type: str, t: float | None, a: float | None, b: float | None,
                      raw: dict[str, float], cal: dict[str, float],
                      n: int, shadow_hours: float | None, run_id: str) -> str:
    lines = [
        "✅ <b>Meta Model Calibrator — ENFORCE активирован</b>",
        "",
        f"<b>Run ID:</b> <code>{run_id}</code>",
        f"<b>Метод:</b> {cal_type}",
    ]
    if cal_type == "temp_logit" and t is not None:
        lines.append(f"<b>T:</b> {t:.4f}")
    elif cal_type == "platt_logit" and a is not None and b is not None:
        lines.append(f"<b>a={a:.4f} b={b:.4f}</b>")
    lines += [
        f"<b>Сэмплов:</b> {n}",
    ]
    if shadow_hours is not None:
        lines.append(f"<b>Shadow:</b> {shadow_hours:.1f}ч")
    lines += [
        "",
        "<b>Метрики (raw → cal):</b>",
        f"  ECE:    {raw.get('ece', 0):.4f} → {cal.get('ece', 0):.4f}",
        f"  Brier:  {raw.get('brier', 0):.4f} → {cal.get('brier', 0):.4f}",
        f"  AUC:    {raw.get('auc', 0):.4f} → {cal.get('auc', 0):.4f}",
        f"  ExpR5%: {raw.get('expectancy_r_top5pct', 0):.4f} → "
        f"{cal.get('expectancy_r_top5pct', 0):.4f}",
    ]
    return "\n".join(lines)


def _fmt_error_msg(reason: str) -> str:
    return f"⚠️ <b>Meta Model Calibrator — ошибка</b>\n\n{reason}"


# ---------------------------------------------------------------------------
# Prometheus metrics (optional — degrades gracefully if not installed)
# ---------------------------------------------------------------------------

try:
    from prometheus_client import Gauge as _PGauge, start_http_server as _prom_start
    _PROM = True
except ImportError:  # pragma: no cover
    _PROM = False
    _PGauge = None  # type: ignore[assignment]
    _prom_start = None  # type: ignore[assignment]

_pg_phase_ok: Any = None
_pg_promoted_age: Any = None
_pg_run_age: Any = None
_pg_n_samples: Any = None
_pg_blocked: Any = None
_pg_enforce_ok: Any = None


def _prom_init(port: int) -> None:
    global _pg_phase_ok, _pg_promoted_age, _pg_run_age, _pg_n_samples, _pg_blocked, _pg_enforce_ok
    if not _PROM:
        _log("prometheus_client not installed — metrics disabled")
        return
    assert _PGauge is not None and _prom_start is not None  # noqa: S101
    _pg_phase_ok = _PGauge("meta_model_calibrator_phase_ok",
                            "1=promoted, 0=blocked/error in last run")
    _pg_promoted_age = _PGauge("meta_model_calibrator_last_promoted_age_sec",
                                "Seconds since last successful ENFORCE promotion")
    _pg_run_age = _PGauge("meta_model_calibrator_last_run_age_sec",
                           "Seconds since last calibration run attempt")
    _pg_n_samples = _PGauge("meta_model_calibrator_n_samples_last_run",
                             "Sample count used in last calibration run")
    _pg_blocked = _PGauge("meta_model_calibrator_blocked",
                           "1=gates blocked in last run, 0=passed")
    _pg_enforce_ok = _PGauge("meta_model_calibrator_enforce_applied",
                              "1=ENFORCE flip applied in last completed run")
    _prom_start(port)
    _log(f"Prometheus metrics on :{port}")


def _prom_update(state: dict[str, Any]) -> None:
    if not _PROM or _pg_phase_ok is None:
        return
    now_ms = int(time.time() * 1000)
    phase = state.get("phase", "")
    _pg_phase_ok.set(1.0 if phase == "promoted" else 0.0)
    last_prom = float(state.get("last_promoted_ms") or 0)
    if last_prom > 0 and _pg_promoted_age is not None:
        _pg_promoted_age.set((now_ms - last_prom) / 1000.0)
    last_run = float(state.get("last_run_ms") or 0)
    if last_run > 0 and _pg_run_age is not None:
        _pg_run_age.set((now_ms - last_run) / 1000.0)
    history = state.get("history") or []
    if history:
        last_ev = history[-1]
        if _pg_n_samples is not None:
            _pg_n_samples.set(float(last_ev.get("n", 0)))
        if _pg_blocked is not None:
            _pg_blocked.set(0.0 if last_ev.get("passed") else 1.0)
    if _pg_enforce_ok is not None:
        _pg_enforce_ok.set(1.0 if state.get("enforced") else 0.0)


# ---------------------------------------------------------------------------
# Arg parser (shared between one-shot and daemon modes)
# ---------------------------------------------------------------------------

def _build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(description="Meta model calibrator + auto-enforce")
    ap.add_argument("--redis-url", default=_env("REDIS_URL", "redis://redis-worker-1:6379/0"))
    ap.add_argument("--redis-main-url",
                    default=_env("REDIS_MAIN_URL", _env("REDIS_URL", "redis://redis:6379/0")),
                    help="Redis main (for notify:telegram stream)")
    ap.add_argument("--dataset", default=_env("V14_CAL_DATASET", ""),
                    help="Path to ml_dataset_v14.jsonl (if empty, load from Redis streams)")
    ap.add_argument("--inputs-stream", default=_env("V14_INPUTS_STREAM", "signals:of:inputs"))
    ap.add_argument("--labels-stream", default=_env("V14_LABELS_STREAM", "labels:tb"))
    ap.add_argument("--max-records", type=int, default=_env_int("V14_CAL_MAX_RECORDS", 5000))
    ap.add_argument("--since-hours", type=float, default=_env_float("V14_CAL_SINCE_HOURS", 72.0))
    ap.add_argument("--champion-key", default=_env("V14_CHAMPION_KEY", "cfg:ml_confirm:champion"))
    ap.add_argument("--cal-key", default=_env("V14_CAL_KEY", "cfg:ml_confirm:v14_of:calibrator"))
    ap.add_argument("--shadow-state-key",
                    default=_env("V14_SHADOW_STATE_KEY", "cfg:ml_confirm:champion"))
    ap.add_argument("--out-dir", default=_env("V14_CAL_OUT_DIR", "/var/lib/trade/of_reports/models"))
    ap.add_argument("--state-path", default=_env("V14_CAL_STATE_PATH",
                    "/var/lib/trade/of_reports/meta_model_calibrator_state.json"))
    ap.add_argument("--notify-stream", default=_env("NOTIFY_STREAM", "notify:telegram"))
    ap.add_argument("--cal-method", default=_env("V14_CAL_METHOD", "temp_logit"),
                    choices=["temp_logit", "platt_logit", "identity"])
    ap.add_argument("--apply", type=int, default=_env_int("V14_CAL_APPLY", 0),
                    help="1 = write calibrator + update Redis; 0 = dry-run")
    ap.add_argument("--enforce", type=int, default=_env_int("V14_CAL_ENFORCE", 0),
                    help="1 = flip champion mode to ENFORCE if gates pass")
    ap.add_argument("--cooldown-sec", type=int, default=_env_int("V14_CAL_COOLDOWN_SEC", 21600),
                    help="Min seconds between runs (default 6h)")
    ap.add_argument("--daemon", type=int, default=_env_int("V14_CAL_DAEMON", 0),
                    help="1 = run as daemon loop with Prometheus metrics; 0 = one-shot")
    ap.add_argument("--prom-port", type=int, default=_env_int("V14_CAL_PROM_PORT", 9842),
                    help="Prometheus metrics port (daemon mode only)")
    return ap


# ---------------------------------------------------------------------------
# Core execution — state machine (one run)
# ---------------------------------------------------------------------------

def _execute(args: argparse.Namespace) -> int:
    state_path = args.state_path
    state = _load_json_safe(state_path)
    now_ms = int(time.time() * 1000)

    # Cooldown guard
    last_run_ms = int(state.get("last_run_ms") or 0)
    if last_run_ms > 0 and (now_ms - last_run_ms) < args.cooldown_sec * 1000:
        remaining = (args.cooldown_sec * 1000 - (now_ms - last_run_ms)) / 1000
        _log(f"Cooldown active — {remaining:.0f}s remaining, skipping")
        return 0

    state["last_run_ms"] = now_ms
    state["pid"] = os.getpid()
    state.setdefault("history", [])

    _log(f"Starting calibration run (apply={args.apply}, enforce={args.enforce})")

    # --- Load samples ---
    rows: list[SampleRow]
    if args.dataset and os.path.exists(args.dataset):
        _log(f"Loading from dataset: {args.dataset}")
        rows = load_from_dataset_ndjson(args.dataset)
    else:
        _log(f"Loading from Redis streams (since {args.since_hours}h)")
        rows = load_from_redis_streams(
            redis_url=args.redis_url,
            inputs_stream=args.inputs_stream,
            labels_stream=args.labels_stream,
            max_records=args.max_records,
            since_hours=args.since_hours,
        )

    if not rows:
        msg = "Нет данных для калибровки (streams пусты или join rate = 0)"
        _log(msg)
        state["phase"] = "blocked"
        state["block_reason"] = "no_data"
        _atomic_write_json(state_path, state)
        _notify_telegram(args.redis_main_url, _fmt_error_msg(msg),
                         severity="warn", dedup_key="meta_cal_no_data",
                         notify_stream=args.notify_stream)
        return 1

    # --- Artifact age + shadow hours ---
    _, artifact_age_days = load_champion_path_from_redis(args.redis_url, args.champion_key)
    shadow_start_ms = load_shadow_start_ms_from_redis(args.redis_url, args.shadow_state_key)
    shadow_hours: float | None = None
    if shadow_start_ms and shadow_start_ms > 0:
        shadow_hours = (now_ms - shadow_start_ms) / 3_600_000

    _log(f"artifact_age={artifact_age_days}d shadow_hours={shadow_hours}")

    # --- Run calibration ---
    gate_cfg = GateConfig.from_env()
    cal_result = run_calibration(rows, prefer_method=args.cal_method)

    raw_m: dict[str, float] = cal_result["raw_metrics"]
    cal_m: dict[str, float] = cal_result["cal_metrics"]
    cal_payload: dict[str, Any] = cal_result["cal_payload"]
    n_total: int = cal_result["n_total"]
    ts_str: str = cal_result["ts_str"]
    run_id: str = cal_payload.get("run_id", "")

    # --- Check gates (calibrated metrics for ECE/Brier, raw for AUC/ExpR) ---
    gate_metrics = {
        "ece": cal_m.get("ece", raw_m.get("ece", 1.0)),
        "brier": cal_m.get("brier", raw_m.get("brier", 1.0)),
        "auc": cal_m.get("auc", raw_m.get("auc", 0.0)),
        "expectancy_r_top5pct": cal_m.get("expectancy_r_top5pct",
                                           raw_m.get("expectancy_r_top5pct", -999.0)),
        "n": float(n_total),
    }
    passed, blockers = check_gates(
        gate_metrics, gate_cfg,
        artifact_age_days=artifact_age_days,
        shadow_hours=shadow_hours,
    )

    event: dict[str, Any] = {
        "ts_ms": now_ms,
        "n": n_total,
        "raw_metrics": raw_m,
        "cal_metrics": cal_m,
        "cal_type": cal_result["cal_type"],
        "passed": passed,
        "blockers": blockers,
        "shadow_hours": shadow_hours,
        "artifact_age_days": artifact_age_days,
        "apply": args.apply,
        "enforce": args.enforce,
    }
    state["history"] = (state.get("history") or [])[-49:] + [event]

    if not passed:
        _log(f"Gates FAILED: {blockers}")
        state["phase"] = "blocked"
        state["last_blockers"] = blockers
        _atomic_write_json(state_path, state)
        _notify_telegram(
            args.redis_main_url,
            _fmt_blocked_msg(blockers, raw_m, cal_m, n_total, shadow_hours),
            severity="warn",
            dedup_key=f"meta_cal_blocked_{ts_str[:8]}",
            notify_stream=args.notify_stream,
        )
        return 0

    # --- Gates passed ---
    _log("Gates PASSED — proceeding with calibrator write")
    state["phase"] = "promoting"

    out_path = ""
    if args.apply:
        out_dir = Path(args.out_dir)
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = str(out_dir / f"calibrator_v14_of_{ts_str}.json")
        try:
            with open(out_path, "w", encoding="utf-8") as fh:
                json.dump(cal_payload, fh, ensure_ascii=False, indent=2)
            _log(f"Calibrator written: {out_path}")
        except Exception as exc:
            _log(f"Failed to write calibrator file: {exc}")
            state["phase"] = "blocked"
            state["block_reason"] = f"file_write_failed: {exc}"
            _atomic_write_json(state_path, state)
            _notify_telegram(args.redis_main_url,
                             _fmt_error_msg(f"Ошибка записи файла: {exc}"),
                             severity="error", notify_stream=args.notify_stream)
            return 1

        cal_key_payload = dict(cal_payload)
        cal_key_payload["model_path"] = out_path
        ok_redis = _write_calibrator_to_redis(args.redis_url, args.cal_key, cal_key_payload)
        if not ok_redis:
            _log("Warning: calibrator written to file but Redis update failed")

        if args.enforce:
            enforce_ok = _update_champion_mode(args.redis_url, args.champion_key, "ENFORCE")
            state["enforced"] = enforce_ok
            state["enforce_ts_ms"] = now_ms
            _log(f"Champion mode → ENFORCE: {'ok' if enforce_ok else 'FAILED'}")
        else:
            _log("--enforce=0: calibrator saved, champion mode NOT changed")
    else:
        _log("Dry-run (--apply=0): no files written, no Redis updates")

    state["phase"] = "promoted"
    state["last_promoted_ms"] = now_ms
    state["last_cal_path"] = out_path
    state["last_run_id"] = run_id
    state["last_blockers"] = []
    _atomic_write_json(state_path, state)

    _notify_telegram(
        args.redis_main_url,
        _fmt_promoted_msg(
            cal_type=cal_result["cal_type"],
            t=cal_result.get("t"),
            a=cal_result.get("a"),
            b=cal_result.get("b"),
            raw=raw_m,
            cal=cal_m,
            n=n_total,
            shadow_hours=shadow_hours,
            run_id=run_id,
        ),
        severity="info",
        dedup_key=f"meta_cal_promoted_{ts_str}",
        notify_stream=args.notify_stream,
    )

    _log(f"Done. phase=promoted run_id={run_id} apply={args.apply} enforce={args.enforce}")
    return 0


# ---------------------------------------------------------------------------
# Daemon mode — periodic loop + Prometheus exporter
# ---------------------------------------------------------------------------

def run_daemon(args: argparse.Namespace) -> None:
    """Long-running daemon: expose Prometheus on :prom_port, run calibration every cooldown_sec."""
    _prom_init(args.prom_port)
    state_path = args.state_path

    # Warm up gauges from existing state on startup (survive restarts gracefully)
    existing = _load_json_safe(state_path)
    if existing:
        _prom_update(existing)

    while True:
        try:
            _execute(args)
        except Exception as exc:
            _log(f"daemon: _execute crashed: {exc}")
        state = _load_json_safe(state_path)
        _prom_update(state)
        _log(f"daemon: sleeping {args.cooldown_sec}s")
        time.sleep(args.cooldown_sec)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> int:
    args = _build_parser().parse_args()
    if args.daemon:
        run_daemon(args)
        return 0
    return _execute(args)


if __name__ == "__main__":
    raise SystemExit(main())
