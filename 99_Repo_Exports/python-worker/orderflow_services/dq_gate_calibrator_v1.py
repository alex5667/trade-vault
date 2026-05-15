from __future__ import annotations

"""dq_gate_calibrator_v1.py

Авто-калибратор DQ Gate.

Зачем
  Penalty schedule в core/dq_gate_v1.py статична: множители (0.8, 0.2, ...)
  и пороги (gap_soft_ms, tick_seq_hard, ...) захардкожены. Реальный DQ-профиль
  по символам/режимам меняется со временем — статичный schedule может либо
  перегейтить хорошие сигналы, либо пропустить деградации.

Что делает
  A) Распределения (без join): из dq_components в signals:of:inputs строит
     эмпирические гистограммы tick_gap_p95_ms / tick_missing_seq_ema /
     book_missing_seq_ema → пороги soft = p75, hard = p92.

  B) Outcome-split (нужен join с labels:tb): для каждого DQ-бакета
     (gap/tick_seq/book_seq/nan/stuck/latency/skew) считает AUC модели в
     cohort "clean" и cohort "degraded" (по soft/hard срабатыванию). Чем
     больше падает AUC при срабатывании — тем сильнее penalty multiplier.

Output
  - File: V14_DQ_CAL_OUT_DIR/dq_gate_calibration_<ts>.json
  - Redis main: cfg:dq_gate:v1:calibration
  - notify:telegram (success/blocked/error)
  - State: V14_DQ_CAL_STATE_PATH (cooldown + history)

Run
  python -m orderflow_services.dq_gate_calibrator_v1
  python -m orderflow_services.dq_gate_calibrator_v1 --apply 1
  python -m orderflow_services.dq_gate_calibrator_v1 --dataset /tmp/of_inputs.ndjson --apply 1
"""

import argparse
import json
import math
import os
import time
from dataclasses import dataclass, field
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
    print(f"[{ts}] [dq_gate_calibrator] {msg}", flush=True)


# ---------------------------------------------------------------------------
# Hardcoded baseline weights (mirror of dq_gate_v1.py section 4)
# ---------------------------------------------------------------------------

DEFAULT_WEIGHTS: dict[str, float] = {
    "dq_pen_weight_gap_soft": 0.80,
    "dq_pen_weight_gap_hard": 0.20,
    "dq_pen_weight_tick_seq_soft": 0.85,
    "dq_pen_weight_tick_seq_hard": 0.30,
    "dq_pen_weight_book_seq_soft": 0.85,
    "dq_pen_weight_book_seq_hard": 0.20,
    "dq_pen_weight_nan_soft": 0.70,
    "dq_pen_weight_nan_hard": 0.20,
    "dq_pen_weight_stuck_soft": 0.80,
    "dq_pen_weight_stuck_hard": 0.20,
    "dq_pen_weight_latency_soft": 0.10,
    "dq_pen_weight_skew_now_soft": 0.70,
    "dq_pen_weight_skew_stream_soft": 0.80,
}

# Per-weight allowed bounds [min, max] — never go beyond these on calibration.
# Soft-trigger weights stay relatively close to 1.0 (mild penalty).
# Hard-trigger weights can be aggressive (close to 0.0).
WEIGHT_BOUNDS: dict[str, tuple[float, float]] = {
    "dq_pen_weight_gap_soft": (0.50, 0.98),
    "dq_pen_weight_gap_hard": (0.05, 0.60),
    "dq_pen_weight_tick_seq_soft": (0.50, 0.98),
    "dq_pen_weight_tick_seq_hard": (0.05, 0.70),
    "dq_pen_weight_book_seq_soft": (0.50, 0.98),
    "dq_pen_weight_book_seq_hard": (0.05, 0.60),
    "dq_pen_weight_nan_soft": (0.30, 0.95),
    "dq_pen_weight_nan_hard": (0.05, 0.60),
    "dq_pen_weight_stuck_soft": (0.40, 0.98),
    "dq_pen_weight_stuck_hard": (0.05, 0.60),
    "dq_pen_weight_latency_soft": (0.05, 0.50),
    "dq_pen_weight_skew_now_soft": (0.40, 0.95),
    "dq_pen_weight_skew_stream_soft": (0.50, 0.98),
}


# ---------------------------------------------------------------------------
# Histograms (no numpy)
# ---------------------------------------------------------------------------

@dataclass
class HistSpec:
    name: str
    min_v: float
    max_v: float
    step: float


class Hist:
    __slots__ = ("spec", "bins", "n", "sum", "min_seen", "max_seen")

    def __init__(self, spec: HistSpec):
        self.spec = spec
        n_bins = max(1, int(math.ceil((spec.max_v - spec.min_v) / spec.step)))
        self.bins: list[int] = [0] * n_bins
        self.n: int = 0
        self.sum: float = 0.0
        self.min_seen: float | None = None
        self.max_seen: float | None = None

    def add(self, x: float) -> None:
        if not math.isfinite(x):
            return
        self.n += 1
        self.sum += float(x)
        self.min_seen = x if self.min_seen is None else min(self.min_seen, x)
        self.max_seen = x if self.max_seen is None else max(self.max_seen, x)
        xx = min(max(x, self.spec.min_v), self.spec.max_v - 1e-12)
        idx = int((xx - self.spec.min_v) / self.spec.step)
        idx = max(0, min(idx, len(self.bins) - 1))
        self.bins[idx] += 1

    def quantile(self, q: float) -> float:
        if self.n <= 0:
            return 0.0
        q = max(0.0, min(1.0, q))
        target = q * (self.n - 1)
        cum = 0
        for i, c in enumerate(self.bins):
            if c <= 0:
                continue
            cum += c
            if target < cum:
                return self.spec.min_v + (i + 0.5) * self.spec.step
        return float(self.spec.max_v)

    def as_dict(self) -> dict[str, Any]:
        return {
            "n": int(self.n),
            "min": float(self.min_seen) if self.min_seen is not None else None,
            "max": float(self.max_seen) if self.max_seen is not None else None,
            "mean": float(self.sum / self.n) if self.n else 0.0,
            "p50": float(self.quantile(0.50)),
            "p75": float(self.quantile(0.75)),
            "p90": float(self.quantile(0.90)),
            "p92": float(self.quantile(0.92)),
            "p95": float(self.quantile(0.95)),
            "p99": float(self.quantile(0.99)),
        }


HIST_SPECS = {
    "tick_gap_p95_ms": HistSpec("tick_gap_p95_ms", 0.0, 60_000.0, 50.0),
    "tick_missing_seq_ema": HistSpec("tick_missing_seq_ema", 0.0, 50.0, 0.05),
    "book_missing_seq_ema": HistSpec("book_missing_seq_ema", 0.0, 100.0, 0.1),
    "tick_time_age_ms": HistSpec("tick_time_age_ms", 0.0, 60_000.0, 50.0),
    "skew_now_ema_ms": HistSpec("skew_now_ema_ms", 0.0, 30_000.0, 25.0),
    "skew_stream_ema_ms": HistSpec("skew_stream_ema_ms", 0.0, 30_000.0, 25.0),
}


# ---------------------------------------------------------------------------
# Sample row
# ---------------------------------------------------------------------------

@dataclass
class DQSample:
    sid: str
    symbol: str
    p_hat: float                       # raw model confidence (for outcome-split AUC)
    indicators: dict[str, float]       # tick_gap_p95_ms, tick_missing_seq_ema, ...
    thr: dict[str, float] = field(default_factory=dict)  # thresholds used at decision time
    outcome: int | None = None         # filled on join
    r_value: float = 0.0


def _norm_sid(s: str) -> str:
    for prefix in ("crypto-of:", "of:"):
        if s.startswith(prefix):
            return s[len(prefix):]
    return s


def _clamp01(x: float) -> float:
    if x <= 0.0: return 0.0
    if x >= 1.0: return 1.0
    return x


def _extract_dq_indicators(payload: dict[str, Any]) -> tuple[dict[str, float], dict[str, float]]:
    """Pull dq_components + thresholds from a decision-record payload.

    Tolerant to layout: top-level, or nested under `indicators`, or both.
    """
    comp: dict[str, Any] = {}
    src = payload.get("dq_components")
    if isinstance(src, dict):
        comp = src
    if not comp:
        ind = payload.get("indicators") or {}
        if isinstance(ind, dict):
            sub = ind.get("dq_components")
            if isinstance(sub, dict):
                comp = sub

    def _f(d: dict[str, Any], k: str, default: float = 0.0) -> float:
        v = d.get(k)
        try:
            return float(v) if v is not None else default
        except Exception:
            return default

    indicators = {
        "tick_gap_p95_ms": _f(comp, "tick_gap_p95_ms"),
        "tick_missing_seq_ema": _f(comp, "tick_missing_seq_ema"),
        "book_missing_seq_ema": _f(comp, "book_missing_seq_ema"),
        "tick_time_age_ms": _f(comp, "tick_time_age_ms"),
        "skew_now_ema_ms": _f(comp, "skew_now_ema_ms"),
        "skew_stream_ema_ms": _f(comp, "skew_stream_ema_ms"),
        "data_health": _f(comp, "data_health", 1.0),
        "book_health_ok": _f(comp, "book_health_ok", 1.0),
        "feature_nan_rate_ema": _f(comp, "feature_nan_rate_ema"),
        "feature_stuck_sec": _f(comp, "feature_stuck_sec"),
    }
    thr_raw = comp.get("thr") if isinstance(comp.get("thr"), dict) else {}
    thr = {k: float(v) for k, v in thr_raw.items() if isinstance(v, (int, float))}
    return indicators, thr


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def load_from_dataset_ndjson(path: str) -> list[DQSample]:
    rows: list[DQSample] = []
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
                sid = _norm_sid(str(rec.get("sid") or ""))
                if not sid:
                    continue
                ind = rec.get("indicators") or {}
                if not isinstance(ind, dict):
                    ind = {}
                p_hat = _clamp01(float(ind.get("confidence") or ind.get("of_score_final") or 0.5))
                dq_ind, thr = _extract_dq_indicators(rec)
                # If dataset already has a label, propagate it (single-pass mode)
                y_raw = rec.get("y_edge_cost_aware") or rec.get("y_edge")
                outcome = None
                if y_raw is not None:
                    try:
                        outcome = int(y_raw)
                    except Exception:
                        outcome = None
                r_val = float(ind.get("edge_after_cost_bps") or 0.0)
                rows.append(DQSample(
                    sid=sid,
                    symbol=str(rec.get("symbol") or "").upper(),
                    p_hat=p_hat,
                    indicators=dq_ind,
                    thr=thr,
                    outcome=outcome,
                    r_value=r_val,
                ))
    except Exception as exc:
        _log(f"load_from_dataset_ndjson error: {exc}")
    return rows


def _xread(r: Any, stream: str, start_id: str, max_records: int) -> list[tuple[str, dict[str, str]]]:
    try:
        from typing import cast as _cast
        res = _cast(list, r.xrange(stream, min=start_id, max="+", count=max_records))
        out: list[tuple[str, dict[str, str]]] = []
        for item in (res or []):
            out.append((str(item[0]), dict(item[1])))
        return out
    except Exception as exc:
        _log(f"xrange {stream} error: {exc}")
        return []


def load_from_redis(
    redis_url: str,
    inputs_stream: str,
    labels_stream: str,
    max_records: int = 5000,
    since_hours: float = 72.0,
) -> tuple[list[DQSample], int, int]:
    """Возвращает (rows_with_indicators, n_inputs_records, n_label_records)."""
    try:
        import redis as redis_lib
        r = redis_lib.Redis.from_url(redis_url, decode_responses=True)
    except Exception as exc:
        _log(f"Redis connect failed: {exc}")
        return [], 0, 0

    now_ms = int(time.time() * 1000)
    start_id = f"{now_ms - int(since_hours * 3600 * 1000)}-0"

    samples_by_sid: dict[str, DQSample] = {}
    input_items = _xread(r, inputs_stream, start_id, max_records)
    for _, fields in input_items:
        try:
            payload_raw = fields.get("payload") or fields.get("data") or ""
            payload: dict[str, Any]
            if payload_raw and isinstance(payload_raw, str) and payload_raw.lstrip().startswith("{"):
                payload = json.loads(payload_raw)
            else:
                payload = dict(fields)
            sid = _norm_sid(str(payload.get("sid") or fields.get("sid") or ""))
            if not sid:
                continue
            ind = payload.get("indicators") or {}
            ind_d: dict[str, Any] = ind if isinstance(ind, dict) else {}
            p_hat = _clamp01(float(ind_d.get("confidence") or ind_d.get("of_score_final") or 0.5))
            dq_ind, thr = _extract_dq_indicators(payload)
            # Skip rows without any DQ context (cannot calibrate)
            if all(v == 0.0 for k, v in dq_ind.items() if k.endswith("_ms") or k.endswith("_ema") or k.endswith("_sec")):
                if dq_ind.get("data_health", 1.0) >= 1.0 and dq_ind.get("book_health_ok", 1.0) >= 1.0:
                    pass  # could be genuinely clean — keep
            samples_by_sid[sid] = DQSample(
                sid=sid,
                symbol=str(payload.get("symbol") or "").upper(),
                p_hat=p_hat,
                indicators=dq_ind,
                thr=thr,
            )
        except Exception:
            continue

    n_inputs = len(samples_by_sid)
    if n_inputs == 0:
        _log("No inputs found in stream")
        return [], 0, 0

    label_items = _xread(r, labels_stream, start_id, max_records)
    n_labels = 0
    for _, fields in label_items:
        try:
            payload_raw = fields.get("payload") or fields.get("data") or ""
            if payload_raw and isinstance(payload_raw, str) and payload_raw.lstrip().startswith("{"):
                payload = json.loads(payload_raw)
            else:
                payload = dict(fields)
            n_labels += 1
            prim = payload.get("primary", 1)
            if isinstance(prim, dict):
                prim = prim.get("flag", 1)
            try:
                if not int(prim or 1):
                    continue
            except Exception:
                pass
            sid = _norm_sid(str(payload.get("sid") or fields.get("sid") or ""))
            row = samples_by_sid.get(sid)
            if row is None:
                continue
            y_raw = payload.get("y_edge_cost_aware") or payload.get("y_edge") or 0
            try:
                row.outcome = int(y_raw)
            except Exception:
                row.outcome = 0
            try:
                row.r_value = float(payload.get("edge_after_cost_bps") or 0.0)
            except Exception:
                row.r_value = 0.0
        except Exception:
            continue

    rows = list(samples_by_sid.values())
    n_joined = sum(1 for r2 in rows if r2.outcome is not None)
    _log(f"Loaded: inputs={n_inputs} labels={n_labels} joined={n_joined}")
    return rows, n_inputs, n_labels


# ---------------------------------------------------------------------------
# Metrics: AUC (no sklearn)
# ---------------------------------------------------------------------------

def compute_auc(probs: list[float], labels: list[int]) -> float:
    n = len(probs)
    if n < 5:
        return 0.5
    pos = [p for p, y in zip(probs, labels) if y == 1]
    neg = [p for p, y in zip(probs, labels) if y == 0]
    if not pos or not neg:
        return 0.5
    pairs = sorted([(p, 1) for p in pos] + [(p, 0) for p in neg], key=lambda x: x[0])
    rank_sum = 0.0
    for i, (_, lbl) in enumerate(pairs):
        if lbl == 1:
            rank_sum += i + 1
    n1, n0 = len(pos), len(neg)
    u = rank_sum - n1 * (n1 + 1) / 2.0
    return u / (n1 * n0)


def cohort_metrics(rows: list[DQSample]) -> dict[str, float]:
    if not rows:
        return {"n": 0.0, "auc": 0.5, "pos_rate": 0.0}
    probs = [r.p_hat for r in rows]
    labels = [int(r.outcome or 0) for r in rows]
    pos = sum(labels)
    return {
        "n": float(len(rows)),
        "auc": float(compute_auc(probs, labels)),
        "pos_rate": float(pos / max(1, len(rows))),
    }


# ---------------------------------------------------------------------------
# Bucket classification & cohort split
# ---------------------------------------------------------------------------

# Each entry: (weight_key_soft, weight_key_hard, indicator_field,
#              effective_soft_thr_fn, effective_hard_thr_fn, "gt|ge")
# Hard threshold function may return None when only soft applies (latency/skew).

def _thr(row: DQSample, key: str, fallback: float) -> float:
    return float(row.thr.get(key, fallback))


BUCKETS: list[dict[str, Any]] = [
    {
        "name": "gap",
        "indicator": "tick_gap_p95_ms",
        "w_soft": "dq_pen_weight_gap_soft",
        "w_hard": "dq_pen_weight_gap_hard",
        "thr_soft": lambda r: _thr(r, "gap_soft_ms", 3000.0),
        "thr_hard": lambda r: _thr(r, "gap_hard_ms", 10000.0),
        "cmp": "ge",
    },
    {
        "name": "tick_seq",
        "indicator": "tick_missing_seq_ema",
        "w_soft": "dq_pen_weight_tick_seq_soft",
        "w_hard": "dq_pen_weight_tick_seq_hard",
        "thr_soft": lambda r: _thr(r, "tick_seq_soft", 2.0),
        "thr_hard": lambda r: _thr(r, "tick_seq_hard", 10.0),
        "cmp": "ge",
    },
    {
        "name": "book_seq",
        "indicator": "book_missing_seq_ema",
        "w_soft": "dq_pen_weight_book_seq_soft",
        "w_hard": "dq_pen_weight_book_seq_hard",
        "thr_soft": lambda r: _thr(r, "book_seq_soft", 10.0),
        "thr_hard": lambda r: _thr(r, "book_seq_hard", 30.0),
        "cmp": "ge",
    },
    {
        "name": "nan",
        "indicator": "feature_nan_rate_ema",
        "w_soft": "dq_pen_weight_nan_soft",
        "w_hard": "dq_pen_weight_nan_hard",
        "thr_soft": lambda r: _thr(r, "nan_soft", 0.01),
        "thr_hard": lambda r: _thr(r, "nan_hard", 0.05),
        "cmp": "ge",
    },
    {
        "name": "stuck",
        "indicator": "feature_stuck_sec",
        "w_soft": "dq_pen_weight_stuck_soft",
        "w_hard": "dq_pen_weight_stuck_hard",
        "thr_soft": lambda r: _thr(r, "stuck_soft_s", 15.0),
        "thr_hard": lambda r: _thr(r, "stuck_hard_s", 60.0),
        "cmp": "ge",
    },
    # Soft-only buckets (no hard split in penalty schedule)
    {
        "name": "latency",
        "indicator": "tick_time_age_ms",
        "w_soft": "dq_pen_weight_latency_soft",
        "w_hard": None,
        "thr_soft": lambda r: _thr(r, "age_soft_ms", 5000.0),
        "thr_hard": None,
        "cmp": "gt",
    },
    {
        "name": "skew_now",
        "indicator": "skew_now_ema_ms",
        "w_soft": "dq_pen_weight_skew_now_soft",
        "w_hard": None,
        "thr_soft": lambda r: _thr(r, "skew_soft_ms", 1000.0),
        "thr_hard": None,
        "cmp": "gt",
    },
    {
        "name": "skew_stream",
        "indicator": "skew_stream_ema_ms",
        "w_soft": "dq_pen_weight_skew_stream_soft",
        "w_hard": None,
        "thr_soft": lambda r: _thr(r, "skew_soft_ms", 1000.0),
        "thr_hard": None,
        "cmp": "gt",
    },
]


def _trigger(val: float, thr: float, cmp_op: str) -> bool:
    if cmp_op == "gt":
        return val > thr
    return val >= thr


def split_cohorts(rows: list[DQSample], bucket: dict[str, Any]) -> dict[str, list[DQSample]]:
    """Split joined rows into clean/soft/hard cohorts for one bucket."""
    ind_key = bucket["indicator"]
    cmp_op = bucket["cmp"]
    thr_soft_fn = bucket["thr_soft"]
    thr_hard_fn = bucket.get("thr_hard")

    out: dict[str, list[DQSample]] = {"clean": [], "soft": [], "hard": []}
    for r in rows:
        v = float(r.indicators.get(ind_key, 0.0))
        soft_thr = float(thr_soft_fn(r))
        if thr_hard_fn is not None:
            hard_thr = float(thr_hard_fn(r))
        else:
            hard_thr = float("inf")
        if _trigger(v, hard_thr, cmp_op):
            out["hard"].append(r)
        elif _trigger(v, soft_thr, cmp_op):
            out["soft"].append(r)
        else:
            out["clean"].append(r)
    return out


# ---------------------------------------------------------------------------
# Weight calibration core
# ---------------------------------------------------------------------------

def _calibrate_weight(
    auc_clean: float,
    auc_degraded: float,
    n_clean: int,
    n_degraded: int,
    current_weight: float,
    bounds: tuple[float, float],
    blend_alpha: float = 0.30,
    min_n_per_cohort: int = 30,
) -> tuple[float, str]:
    """Return (new_weight, status). Status ∈ {applied, kept_low_n, kept_low_auc}."""
    lo, hi = bounds
    if n_clean < min_n_per_cohort or n_degraded < min_n_per_cohort:
        return float(max(lo, min(hi, current_weight))), "kept_low_n"
    if auc_clean < 0.51:
        # Model itself has no signal — can't infer DQ damage. Keep current.
        return float(max(lo, min(hi, current_weight))), "kept_low_auc"
    quality_drop = max(0.0, (auc_clean - auc_degraded) / auc_clean)
    desired = 1.0 - quality_drop
    blended = (1.0 - blend_alpha) * current_weight + blend_alpha * desired
    return float(max(lo, min(hi, blended))), "applied"


def calibrate_weights(
    joined_rows: list[DQSample],
    current_weights: dict[str, float],
    blend_alpha: float = 0.30,
    min_n_per_cohort: int = 30,
) -> tuple[dict[str, float], dict[str, Any]]:
    """Returns (new_weights, outcome_analysis_report)."""
    new_weights = dict(current_weights)
    report: dict[str, Any] = {}

    if not joined_rows:
        report["note"] = "no joined rows — weights unchanged"
        return new_weights, report

    for b in BUCKETS:
        cohorts = split_cohorts(joined_rows, b)
        m_clean = cohort_metrics(cohorts["clean"])
        m_soft = cohort_metrics(cohorts["soft"])
        m_hard = cohort_metrics(cohorts["hard"])

        entry: dict[str, Any] = {
            "n_clean": int(m_clean["n"]),
            "n_soft": int(m_soft["n"]),
            "n_hard": int(m_hard["n"]),
            "auc_clean": round(m_clean["auc"], 4),
            "auc_soft": round(m_soft["auc"], 4),
            "auc_hard": round(m_hard["auc"], 4),
            "pos_rate_clean": round(m_clean["pos_rate"], 4),
            "pos_rate_soft": round(m_soft["pos_rate"], 4),
            "pos_rate_hard": round(m_hard["pos_rate"], 4),
        }

        # Soft weight
        w_key_soft = b["w_soft"]
        bounds_soft = WEIGHT_BOUNDS.get(w_key_soft, (0.05, 0.99))
        cur_soft = float(current_weights.get(w_key_soft, DEFAULT_WEIGHTS[w_key_soft]))
        new_soft, status_soft = _calibrate_weight(
            m_clean["auc"], m_soft["auc"],
            int(m_clean["n"]), int(m_soft["n"]),
            cur_soft, bounds_soft,
            blend_alpha=blend_alpha,
            min_n_per_cohort=min_n_per_cohort,
        )
        new_weights[w_key_soft] = round(new_soft, 4)
        entry["w_soft"] = {"current": cur_soft, "new": round(new_soft, 4), "status": status_soft}

        # Hard weight (if applicable)
        w_key_hard = b.get("w_hard")
        if w_key_hard is not None:
            bounds_hard = WEIGHT_BOUNDS.get(w_key_hard, (0.05, 0.99))
            cur_hard = float(current_weights.get(w_key_hard, DEFAULT_WEIGHTS[w_key_hard]))
            new_hard, status_hard = _calibrate_weight(
                m_clean["auc"], m_hard["auc"],
                int(m_clean["n"]), int(m_hard["n"]),
                cur_hard, bounds_hard,
                blend_alpha=blend_alpha,
                min_n_per_cohort=min_n_per_cohort,
            )
            new_weights[w_key_hard] = round(new_hard, 4)
            entry["w_hard"] = {"current": cur_hard, "new": round(new_hard, 4), "status": status_hard}

        report[b["name"]] = entry

    return new_weights, report


# ---------------------------------------------------------------------------
# Threshold recommendations (distribution-only — no join required)
# ---------------------------------------------------------------------------

def build_distributions(rows: list[DQSample]) -> dict[str, Hist]:
    hists: dict[str, Hist] = {k: Hist(spec) for k, spec in HIST_SPECS.items()}
    for r in rows:
        for k, h in hists.items():
            v = r.indicators.get(k, 0.0)
            if isinstance(v, (int, float)) and math.isfinite(float(v)):
                h.add(float(v))
    return hists


def threshold_recommendations(hists: dict[str, Hist]) -> dict[str, float]:
    """Empirical percentiles → soft = p75, hard = p92.

    Floored to safe minima so calibration never relaxes below operationally
    reasonable values (e.g. soft tick gap < 500ms is meaningless).
    """
    def _p(h: Hist, q: float) -> float:
        return float(h.quantile(q)) if h.n > 0 else 0.0

    g = hists["tick_gap_p95_ms"]
    ts = hists["tick_missing_seq_ema"]
    bs = hists["book_missing_seq_ema"]

    rec: dict[str, float] = {
        "rec_dq_tick_gap_p95_soft_ms": max(500.0, _p(g, 0.75)),
        "rec_dq_tick_gap_p95_hard_ms": max(3000.0, _p(g, 0.92)),
        "rec_dq_tick_missing_seq_soft": max(0.5, _p(ts, 0.80)),
        "rec_dq_tick_missing_seq_hard": max(3.0, _p(ts, 0.95)),
        "rec_dq_book_missing_seq_soft": max(2.0, _p(bs, 0.80)),
        "rec_dq_book_missing_seq_hard": max(10.0, _p(bs, 0.95)),
    }
    return {k: round(v, 2) for k, v in rec.items()}


# ---------------------------------------------------------------------------
# Gates
# ---------------------------------------------------------------------------

@dataclass
class DQGateCfg:
    min_n_records: int = 500
    min_n_joined: int = 100
    max_weight_change: float = 0.35
    shadow_min_hours: float = 0.0
    cooldown_sec: int = 21600

    @staticmethod
    def from_env() -> "DQGateCfg":
        return DQGateCfg(
            min_n_records=_env_int("V14_DQ_CAL_MIN_N_RECORDS", 500),
            min_n_joined=_env_int("V14_DQ_CAL_MIN_N_JOINED", 100),
            max_weight_change=_env_float("V14_DQ_CAL_MAX_WEIGHT_CHANGE", 0.35),
            shadow_min_hours=_env_float("V14_DQ_CAL_SHADOW_MIN_HOURS", 0.0),
            cooldown_sec=_env_int("V14_DQ_CAL_COOLDOWN_SEC", 21600),
        )


def check_gates(
    n_records: int,
    n_joined: int,
    current_weights: dict[str, float],
    new_weights: dict[str, float],
    shadow_hours: float | None,
    cfg: DQGateCfg,
) -> tuple[bool, list[str]]:
    blockers: list[str] = []
    if n_records < cfg.min_n_records:
        blockers.append(f"n_records={n_records} < {cfg.min_n_records}")
    if n_joined < cfg.min_n_joined:
        # Soft block: still allow threshold-only mode (no weight changes).
        # But if any weight changed materially we must have join.
        max_delta = max(
            (abs(new_weights.get(k, v) - v) for k, v in current_weights.items()),
            default=0.0,
        )
        if max_delta > 1e-6:
            blockers.append(
                f"n_joined={n_joined} < {cfg.min_n_joined} but weights changed (max_delta={max_delta:.4f})"
            )
    # Per-weight change cap
    for k, cur in current_weights.items():
        new = new_weights.get(k, cur)
        if abs(new - cur) > cfg.max_weight_change:
            blockers.append(
                f"weight {k} delta={new - cur:+.3f} exceeds max_change={cfg.max_weight_change}"
            )
    if shadow_hours is not None and shadow_hours < cfg.shadow_min_hours:
        blockers.append(f"shadow_hours={shadow_hours:.1f} < {cfg.shadow_min_hours}")
    return (len(blockers) == 0), blockers


# ---------------------------------------------------------------------------
# Output: payload + Redis + file
# ---------------------------------------------------------------------------

def build_calibration_payload(
    new_weights: dict[str, float],
    rec_thresholds: dict[str, float],
    distributions: dict[str, Hist],
    outcome_report: dict[str, Any],
    n_records: int,
    n_joined: int,
    n_label_records: int,
    blockers: list[str],
    gates_passed: bool,
    shadow_hours: float | None,
    run_id: str,
) -> dict[str, Any]:
    now_ms = int(time.time() * 1000)
    return {
        "schema_version": 1,
        "calibrated_ms": now_ms,
        "run_id": run_id,
        "method": "outcome_split_v1",
        "n_records": int(n_records),
        "n_joined": int(n_joined),
        "n_label_records": int(n_label_records),
        "shadow_hours": float(shadow_hours) if shadow_hours is not None else None,
        "gates_passed": bool(gates_passed),
        "blockers": list(blockers),
        "weights": {k: float(v) for k, v in new_weights.items()},
        "thresholds": {k: float(v) for k, v in rec_thresholds.items()},
        "outcome_analysis": outcome_report,
        "distributions": {k: h.as_dict() for k, h in distributions.items()},
    }


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


def _write_to_redis(redis_url: str, key: str, payload: dict[str, Any]) -> bool:
    try:
        import redis as redis_lib
        r = redis_lib.Redis.from_url(redis_url, decode_responses=True)
        r.set(key, json.dumps(payload, ensure_ascii=False, separators=(",", ":")))
        return True
    except Exception as exc:
        _log(f"write to Redis failed: {exc}")
        return False


def _load_current_weights_from_redis(redis_url: str, key: str) -> dict[str, float]:
    """Read previous calibration weights from Redis. Falls back to DEFAULT_WEIGHTS."""
    try:
        import redis as redis_lib
        r = redis_lib.Redis.from_url(redis_url, decode_responses=True)
        raw = r.get(key)
        if not raw:
            return dict(DEFAULT_WEIGHTS)
        obj = json.loads(str(raw))
        w = obj.get("weights") if isinstance(obj, dict) else None
        if not isinstance(w, dict):
            return dict(DEFAULT_WEIGHTS)
        out: dict[str, float] = dict(DEFAULT_WEIGHTS)
        for k in DEFAULT_WEIGHTS:
            v = w.get(k)
            try:
                if v is not None:
                    out[k] = float(v)
            except Exception:
                pass
        return out
    except Exception:
        return dict(DEFAULT_WEIGHTS)


# ---------------------------------------------------------------------------
# Telegram notify
# ---------------------------------------------------------------------------

def _notify_telegram(
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
            "source": "dq_gate_calibrator_v1",
            "severity": severity,
            "timestamp": str(int(time.time() * 1000)),
        }
        if dedup_key:
            msg["dedup_key"] = dedup_key
        r.xadd(notify_stream, msg, maxlen=5000)
    except Exception as exc:
        _log(f"Telegram notify error: {exc}")


def _fmt_msg(payload: dict[str, Any], phase: str) -> str:
    weights = payload.get("weights") or {}
    thr = payload.get("thresholds") or {}
    n_rec = payload.get("n_records", 0)
    n_join = payload.get("n_joined", 0)
    head = {
        "promoted": "✅ <b>DQ Gate Calibrator — применено</b>",
        "blocked": "🚫 <b>DQ Gate Calibrator — заблокировано</b>",
        "dry_run": "ℹ️ <b>DQ Gate Calibrator — dry-run</b>",
    }.get(phase, "ℹ️ <b>DQ Gate Calibrator</b>")
    lines = [head, ""]
    lines.append(f"<b>Run:</b> <code>{payload.get('run_id','')}</code>")
    lines.append(f"<b>Записей:</b> {n_rec}  <b>joined:</b> {n_join}")
    if payload.get("blockers"):
        lines.append("")
        lines.append("<b>Блокеры:</b>")
        for b in payload["blockers"]:
            lines.append(f"  ❌ {b}")
    if weights:
        lines.append("")
        lines.append("<b>Веса (новые):</b>")
        for k in sorted(weights.keys()):
            lines.append(f"  {k}: <code>{weights[k]:.3f}</code>")
    if thr:
        lines.append("")
        lines.append("<b>Threshold recommendations:</b>")
        for k in sorted(thr.keys()):
            lines.append(f"  {k}: <code>{thr[k]:g}</code>")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# main()
# ---------------------------------------------------------------------------

def main() -> int:
    ap = argparse.ArgumentParser(description="DQ Gate auto-calibrator")
    ap.add_argument("--redis-url", default=_env("REDIS_URL", "redis://redis-worker-1:6379/0"))
    ap.add_argument("--redis-main-url",
                    default=_env("REDIS_MAIN_URL", _env("REDIS_URL", "redis://redis:6379/0")))
    ap.add_argument("--dataset", default=_env("V14_DQ_CAL_DATASET", ""))
    ap.add_argument("--inputs-stream", default=_env("V14_INPUTS_STREAM", "signals:of:inputs"))
    ap.add_argument("--labels-stream", default=_env("V14_LABELS_STREAM", "labels:tb"))
    ap.add_argument("--max-records", type=int, default=_env_int("V14_DQ_CAL_MAX_RECORDS", 5000))
    ap.add_argument("--since-hours", type=float, default=_env_float("V14_DQ_CAL_SINCE_HOURS", 72.0))
    ap.add_argument("--cal-key", default=_env("V14_DQ_CAL_KEY", "cfg:dq_gate:v1:calibration"))
    ap.add_argument("--out-dir", default=_env("V14_DQ_CAL_OUT_DIR", "/var/lib/trade/of_reports/dq_calibration"))
    ap.add_argument("--state-path", default=_env("V14_DQ_CAL_STATE_PATH",
                    "/var/lib/trade/of_reports/dq_gate_calibrator_state.json"))
    ap.add_argument("--notify-stream", default=_env("NOTIFY_STREAM", "notify:telegram"))
    ap.add_argument("--blend-alpha", type=float, default=_env_float("V14_DQ_CAL_BLEND_ALPHA", 0.30))
    ap.add_argument("--min-n-per-cohort", type=int, default=_env_int("V14_DQ_CAL_MIN_N_COHORT", 30))
    ap.add_argument("--apply", type=int, default=_env_int("V14_DQ_CAL_APPLY", 0),
                    help="1 = write Redis key + file; 0 = dry-run")
    args = ap.parse_args()

    cfg = DQGateCfg.from_env()
    state = _load_json_safe(args.state_path)
    now_ms = int(time.time() * 1000)

    last_run_ms = int(state.get("last_run_ms") or 0)
    if last_run_ms > 0 and (now_ms - last_run_ms) < cfg.cooldown_sec * 1000:
        remaining = (cfg.cooldown_sec * 1000 - (now_ms - last_run_ms)) / 1000
        _log(f"Cooldown active — {remaining:.0f}s remaining, skipping")
        return 0

    state["last_run_ms"] = now_ms
    state["pid"] = os.getpid()
    state.setdefault("history", [])

    _log(f"Starting DQ Gate calibration (apply={args.apply})")

    # --- Load samples ---
    rows: list[DQSample]
    n_label_records = 0
    if args.dataset and os.path.exists(args.dataset):
        _log(f"Loading from dataset: {args.dataset}")
        rows = load_from_dataset_ndjson(args.dataset)
        n_inputs = len(rows)
        n_label_records = sum(1 for r in rows if r.outcome is not None)
    else:
        _log(f"Loading from Redis streams (since {args.since_hours}h)")
        rows, n_inputs, n_label_records = load_from_redis(
            redis_url=args.redis_url,
            inputs_stream=args.inputs_stream,
            labels_stream=args.labels_stream,
            max_records=args.max_records,
            since_hours=args.since_hours,
        )

    if not rows:
        msg = "Нет данных для калибровки DQ Gate (streams пусты)"
        _log(msg)
        state["phase"] = "blocked"
        state["block_reason"] = "no_data"
        _atomic_write_json(args.state_path, state)
        _notify_telegram(args.redis_main_url,
                         f"⚠️ <b>DQ Gate Calibrator — ошибка</b>\n\n{msg}",
                         severity="warn",
                         dedup_key="dq_cal_no_data",
                         notify_stream=args.notify_stream)
        return 1

    n_records = len(rows)
    joined_rows = [r for r in rows if r.outcome is not None]
    n_joined = len(joined_rows)

    # --- Distributions & threshold recommendations ---
    hists = build_distributions(rows)
    rec_thresholds = threshold_recommendations(hists)

    # --- Weight calibration ---
    current_weights = _load_current_weights_from_redis(args.redis_url, args.cal_key)
    new_weights, outcome_report = calibrate_weights(
        joined_rows,
        current_weights=current_weights,
        blend_alpha=args.blend_alpha,
        min_n_per_cohort=args.min_n_per_cohort,
    )

    # --- Gates ---
    passed, blockers = check_gates(
        n_records=n_records,
        n_joined=n_joined,
        current_weights=current_weights,
        new_weights=new_weights,
        shadow_hours=None,
        cfg=cfg,
    )

    ts_str = time.strftime("%Y%m%d_%H%M%S", time.gmtime())
    run_id = f"dq_cal_v1_{ts_str}"

    payload = build_calibration_payload(
        new_weights=new_weights,
        rec_thresholds=rec_thresholds,
        distributions=hists,
        outcome_report=outcome_report,
        n_records=n_records,
        n_joined=n_joined,
        n_label_records=n_label_records,
        blockers=blockers,
        gates_passed=passed,
        shadow_hours=None,
        run_id=run_id,
    )

    # --- Persist history (without bulky distributions) ---
    event = {
        "ts_ms": now_ms,
        "run_id": run_id,
        "n_records": n_records,
        "n_joined": n_joined,
        "gates_passed": passed,
        "blockers": blockers,
        "weights": new_weights,
        "thresholds": rec_thresholds,
        "apply": args.apply,
    }
    state["history"] = (state.get("history") or [])[-49:] + [event]

    if not passed:
        _log(f"Gates FAILED: {blockers}")
        state["phase"] = "blocked"
        state["last_blockers"] = blockers
        _atomic_write_json(args.state_path, state)
        _notify_telegram(args.redis_main_url,
                         _fmt_msg(payload, "blocked"),
                         severity="warn",
                         dedup_key=f"dq_cal_blocked_{ts_str[:8]}",
                         notify_stream=args.notify_stream)
        return 0

    out_path = ""
    if args.apply:
        out_dir = Path(args.out_dir)
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = str(out_dir / f"dq_gate_calibration_{ts_str}.json")
        try:
            with open(out_path, "w", encoding="utf-8") as fh:
                json.dump(payload, fh, ensure_ascii=False, indent=2, default=str)
            _log(f"Calibration written: {out_path}")
        except Exception as exc:
            _log(f"Failed to write calibration file: {exc}")
            state["phase"] = "blocked"
            state["block_reason"] = f"file_write_failed: {exc}"
            _atomic_write_json(args.state_path, state)
            _notify_telegram(args.redis_main_url,
                             f"⚠️ <b>DQ Gate Calibrator — ошибка</b>\n\nОшибка записи файла: {exc}",
                             severity="error",
                             notify_stream=args.notify_stream)
            return 1

        ok = _write_to_redis(args.redis_url, args.cal_key, payload)
        if not ok:
            _log("Warning: file written but Redis update failed")
            state["redis_update_failed"] = True

        state["phase"] = "applied"
        state["last_applied_ms"] = now_ms
        state["last_cal_path"] = out_path
        _notify_telegram(args.redis_main_url,
                         _fmt_msg(payload, "promoted"),
                         severity="info",
                         dedup_key=f"dq_cal_applied_{ts_str}",
                         notify_stream=args.notify_stream)
    else:
        _log("Dry-run (--apply=0): no file written, no Redis update")
        state["phase"] = "dry_run"
        _notify_telegram(args.redis_main_url,
                         _fmt_msg(payload, "dry_run"),
                         severity="info",
                         dedup_key=f"dq_cal_dryrun_{ts_str[:8]}",
                         notify_stream=args.notify_stream)

    state["last_run_id"] = run_id
    state["last_blockers"] = []
    _atomic_write_json(args.state_path, state)
    _log(f"Done. phase={state.get('phase')} run_id={run_id} apply={args.apply}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
