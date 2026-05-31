"""calibration/conf_meta_gate_dataset.py — Plan 1 Phase 1 dataset builder.

Joins two Timescale sources into a single training dataset for the
confidence meta-gate:

  * passed cohort   ← `signal_outcome` (decision-time features + outcome)
  * gated_out cohort ← `signal_gated_out_outcomes` (decision-time confidence
                       + outcome). When a `signal_feature_snapshots` row
                       exists for the same sid we attach the richer
                       feature vector; otherwise the row is kept with a
                       minimal feature set (lighter but still trainable).

Compatibility filter (the plan's hard rule "do not mix labels with
different TP/SL/horizon"): rows are kept only when
  * horizon_ms ∈ allowed_horizons_ms,
  * tp_bps / sl_bps inside (min, max) buckets,
  * tp_bps > 0 and sl_bps > 0.

Pure Python; the only IO is psycopg2 reads. Tests work against a fake
connection (see tests/test_conf_meta_gate_dataset.py).
"""
from __future__ import annotations

import json
import logging
import math
from dataclasses import asdict, dataclass, field
from typing import Any, Iterable

log = logging.getLogger("conf_meta_gate.dataset")


# ── Row contract ────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class MetaGateTrainRow:
    sid: str
    ts_ms: int
    symbol: str
    kind: str
    side: str
    cohort: str  # "passed" | "gated_out"

    legacy_confidence: float
    legacy_min_confidence: float
    legacy_would_allow: int  # 1 / 0

    # Probability / score surface (best-effort — may be 0 when missing).
    p_edge_raw: float
    p_edge_cal: float
    rule_score: float
    have_need_ratio: float

    # Cost surface (bp-denominated).
    spread_bps: float
    expected_slippage_bps: float
    fee_bps: float
    exec_cost_bps: float
    expected_edge_bps: float
    exec_risk_norm: float

    # DQ / signal staleness.
    dq_score: float
    dq_flag_count: float
    signal_age_ms: float

    # Regime / session.
    regime_code: float
    session_asia: float
    session_europe: float
    session_us: float
    weekend_flag: float

    # Priors (rolling per-bucket; computed by trainer, kept zero here).
    prior_winrate: float
    prior_ev_r: float
    prior_sample_count_log: float

    # Outcome contract.
    horizon_ms: int
    tp_bps: float
    sl_bps: float

    y_win: int
    y_util_pos: int
    r_mult: float
    ret_bps: float

    # Raw feature blob (per-cohort, sparse). The trainer picks the subset
    # actually present across cohorts as the model feature_cols.
    features: dict[str, float] = field(default_factory=dict)


# ── helpers ─────────────────────────────────────────────────────────────────


def _safe_float(v: Any, default: float = 0.0) -> float:
    if v is None:
        return default
    try:
        f = float(v)
        return f if math.isfinite(f) else default
    except (TypeError, ValueError):
        return default


def _safe_int(v: Any, default: int = 0) -> int:
    if v is None:
        return default
    try:
        return int(v)
    except (TypeError, ValueError):
        try:
            return int(float(v))
        except (TypeError, ValueError):
            return default


_REGIME_CODE = {
    "trending_bull": 1.0,
    "trending_bear": 2.0,
    "range": 3.0,
    "squeeze": 4.0,
    "unknown": 0.0,
    "na": 0.0,
    "": 0.0,
}


def regime_to_code(regime: Any) -> float:
    return _REGIME_CODE.get(str(regime or "").lower().strip(), 0.0)


def session_to_onehot(session: Any) -> tuple[float, float, float, float]:
    """Returns (asia, europe, us, weekend)."""
    s = str(session or "").lower().strip()
    return (
        1.0 if s == "asia" else 0.0,
        1.0 if s in ("europe", "eu", "european") else 0.0,
        1.0 if s in ("us", "ny") else 0.0,
        1.0 if s in ("weekend", "wknd") else 0.0,
    )


def coerce_features(payload: Any) -> dict[str, float]:
    """Coerce a JSONB blob (dict or str) into a dict[str, float].

    Non-numeric values are silently dropped — the trainer requires numeric
    inputs only.
    """
    if payload is None:
        return {}
    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except (json.JSONDecodeError, TypeError):
            return {}
    if not isinstance(payload, dict):
        return {}
    out: dict[str, float] = {}
    for k, v in payload.items():
        f = _safe_float(v, default=math.nan)
        if math.isnan(f):
            continue
        out[str(k)] = f
    return out


# ── Compatibility filter ────────────────────────────────────────────────────


@dataclass(frozen=True)
class CompatibilityFilter:
    """Hard rules that keep cohorts comparable.

    horizon_ms_allowed: explicit allow-set; if non-empty, only those values
    pass. tp_bps / sl_bps ranges are inclusive on both ends.
    """

    horizon_ms_allowed: frozenset[int] = frozenset()
    tp_bps_min: float = 1.0
    tp_bps_max: float = 200.0
    sl_bps_min: float = 1.0
    sl_bps_max: float = 200.0

    def keep(self, row: MetaGateTrainRow) -> bool:
        if row.horizon_ms <= 0:
            return False
        if self.horizon_ms_allowed and row.horizon_ms not in self.horizon_ms_allowed:
            return False
        if not (self.tp_bps_min <= row.tp_bps <= self.tp_bps_max):
            return False
        if not (self.sl_bps_min <= row.sl_bps <= self.sl_bps_max):
            return False
        return True


def apply_compatibility_filter(
    rows: Iterable[MetaGateTrainRow], flt: CompatibilityFilter,
) -> list[MetaGateTrainRow]:
    return [r for r in rows if flt.keep(r)]


# ── Row builders (DB-backed) ────────────────────────────────────────────────


_PASSED_SQL = """
    SELECT
        sid,
        decision_time_ms,
        ingest_time_ms,
        symbol,
        side,
        COALESCE(kind, '')           AS kind,
        regime,
        atr_bps,
        ttl_ms,
        tp_r,
        sl_r,
        r_unit_px,
        entry_px,
        calib_prob,
        raw_score,
        label,
        realized_r,
        realized_bps,
        features
    FROM signal_outcome
    WHERE decision_time_ms BETWEEN %s AND %s
      AND label IS NOT NULL
      AND realized_r IS NOT NULL
    ORDER BY decision_time_ms ASC
    LIMIT %s
"""


_GATED_OUT_SQL = """
    SELECT
        sid,
        ts_ms,
        ts_close_ms,
        symbol,
        direction,
        COALESCE(kind, '')           AS kind,
        entry_px,
        tp_bps,
        sl_bps,
        horizon_ms,
        confidence,
        min_conf,
        label,
        r_mult,
        ret_bps,
        cost_bps,
        cost_fees_bps,
        cost_spread_bps,
        cost_slippage_bps,
        y_edge_cost_aware
    FROM signal_gated_out_outcomes
    WHERE ts_ms BETWEEN %s AND %s
      AND label IS NOT NULL
    ORDER BY ts_ms ASC
    LIMIT %s
"""


_FEATURE_SNAPSHOT_SQL = """
    SELECT sid, features
    FROM signal_feature_snapshots
    WHERE sid = ANY(%s)
"""


def _utc_session_from_ms(ts_ms: int) -> str:
    """Lightweight session bucketer mirroring `services.orderflow.utils.session_utc`.

    Kept local so the dataset module has no orderflow runtime dependency.
    """
    if ts_ms <= 0:
        return ""
    hour = (ts_ms // 3_600_000) % 24
    # ISO weekday: epoch ms / 86_400_000 → days since 1970-01-01 (Thu).
    days = ts_ms // 86_400_000
    weekday = int((days + 3) % 7)  # 0=Mon … 6=Sun
    if weekday >= 5:
        return "weekend"
    if 0 <= hour < 7:
        return "asia"
    if 7 <= hour < 13:
        return "europe"
    return "us"


def _passed_row_from_db(rec: dict) -> MetaGateTrainRow | None:
    sid = str(rec.get("sid") or "").strip()
    if not sid:
        return None
    ts_ms = _safe_int(rec.get("decision_time_ms"))
    if ts_ms <= 0:
        return None
    label = _safe_int(rec.get("label"))
    realized_r = _safe_float(rec.get("realized_r"))
    side_int = _safe_int(rec.get("side"))
    side = "LONG" if side_int >= 0 else "SHORT"

    sl_r = _safe_float(rec.get("sl_r"))
    tp_r = _safe_float(rec.get("tp_r"))
    r_unit_px = _safe_float(rec.get("r_unit_px"))
    entry_px = _safe_float(rec.get("entry_px"))
    sl_bps = (r_unit_px / entry_px) * 10_000 if entry_px > 0 and r_unit_px > 0 else 0.0
    tp_bps = sl_bps * tp_r if sl_r > 0 else 0.0
    horizon_ms = _safe_int(rec.get("ttl_ms"))

    features = coerce_features(rec.get("features"))
    realized_bps = _safe_float(rec.get("realized_bps"))
    calib_prob = _safe_float(rec.get("calib_prob"))
    raw_score = _safe_float(rec.get("raw_score"))

    y_win = 1 if label == 1 else 0
    # Utility-positive = win after a baseline execution-cost estimate (bp).
    # We don't have spread/slip per row here; use a 4 bp blended cost as a
    # placeholder so the binary target is well-defined.
    util_threshold_bps = 4.0
    y_util_pos = 1 if realized_bps > util_threshold_bps else 0
    session = _utc_session_from_ms(ts_ms)
    a, e, u, w = session_to_onehot(session)

    return MetaGateTrainRow(
        sid=sid,
        ts_ms=ts_ms,
        symbol=str(rec.get("symbol") or ""),
        kind=str(rec.get("kind") or ""),
        side=side,
        cohort="passed",
        legacy_confidence=calib_prob if calib_prob > 0 else _safe_float(features.get("confidence", 0.0)),
        legacy_min_confidence=_safe_float(features.get("min_conf", 0.7)),
        legacy_would_allow=1,
        p_edge_raw=raw_score,
        p_edge_cal=calib_prob,
        rule_score=_safe_float(features.get("rule_score", 0.0)),
        have_need_ratio=_safe_float(features.get("have_need_ratio", 0.0)),
        spread_bps=_safe_float(features.get("spread_bps", 0.0)),
        expected_slippage_bps=_safe_float(features.get("expected_slippage_bps", 0.0)),
        fee_bps=_safe_float(features.get("fee_bps", 0.0)),
        exec_cost_bps=_safe_float(features.get("exec_cost_bps", 0.0)),
        expected_edge_bps=_safe_float(features.get("expected_edge_bps", 0.0)),
        exec_risk_norm=_safe_float(features.get("exec_risk_norm", 0.0)),
        dq_score=_safe_float(features.get("dq_score", 1.0)),
        dq_flag_count=_safe_float(features.get("dq_flag_count", 0.0)),
        signal_age_ms=_safe_float(features.get("signal_age_ms", 0.0)),
        regime_code=regime_to_code(rec.get("regime")),
        session_asia=a,
        session_europe=e,
        session_us=u,
        weekend_flag=w,
        prior_winrate=0.0,
        prior_ev_r=0.0,
        prior_sample_count_log=0.0,
        horizon_ms=horizon_ms,
        tp_bps=tp_bps,
        sl_bps=sl_bps,
        y_win=y_win,
        y_util_pos=y_util_pos,
        r_mult=realized_r,
        ret_bps=realized_bps,
        features=features,
    )


def _gated_out_row_from_db(rec: dict, snapshot_features: dict[str, dict[str, float]]) -> MetaGateTrainRow | None:
    sid = str(rec.get("sid") or "").strip()
    if not sid:
        return None
    ts_ms = _safe_int(rec.get("ts_ms"))
    if ts_ms <= 0:
        return None
    label = _safe_int(rec.get("label"))
    side_int = _safe_int(rec.get("direction"))
    side = "LONG" if side_int >= 0 else "SHORT"

    tp_bps = _safe_float(rec.get("tp_bps"))
    sl_bps = _safe_float(rec.get("sl_bps"))
    horizon_ms = _safe_int(rec.get("horizon_ms"))
    confidence = _safe_float(rec.get("confidence"))
    min_conf = _safe_float(rec.get("min_conf"))
    r_mult = _safe_float(rec.get("r_mult"))
    ret_bps = _safe_float(rec.get("ret_bps"))
    cost_bps = _safe_float(rec.get("cost_bps"))
    cost_fees_bps = _safe_float(rec.get("cost_fees_bps"))
    cost_spread_bps = _safe_float(rec.get("cost_spread_bps"))
    cost_slippage_bps = _safe_float(rec.get("cost_slippage_bps"))
    y_util_pos = _safe_int(rec.get("y_edge_cost_aware"))

    # Best-effort feature attachment — gated_out_outcomes does not store the
    # full feature vector, so we mine signal_feature_snapshots if available.
    features = snapshot_features.get(sid, {})
    y_win = 1 if label == 1 else 0
    session = _utc_session_from_ms(ts_ms)
    a, e, u, w = session_to_onehot(session)

    return MetaGateTrainRow(
        sid=sid,
        ts_ms=ts_ms,
        symbol=str(rec.get("symbol") or ""),
        kind=str(rec.get("kind") or ""),
        side=side,
        cohort="gated_out",
        legacy_confidence=confidence,
        legacy_min_confidence=min_conf,
        legacy_would_allow=0,
        p_edge_raw=_safe_float(features.get("p_edge_raw", 0.0)),
        p_edge_cal=_safe_float(features.get("p_edge_cal", 0.0)),
        rule_score=_safe_float(features.get("rule_score", 0.0)),
        have_need_ratio=_safe_float(features.get("have_need_ratio", 0.0)),
        spread_bps=cost_spread_bps or _safe_float(features.get("spread_bps", 0.0)),
        expected_slippage_bps=cost_slippage_bps or _safe_float(features.get("expected_slippage_bps", 0.0)),
        fee_bps=cost_fees_bps or _safe_float(features.get("fee_bps", 0.0)),
        exec_cost_bps=cost_bps or _safe_float(features.get("exec_cost_bps", 0.0)),
        expected_edge_bps=_safe_float(features.get("expected_edge_bps", 0.0)),
        exec_risk_norm=_safe_float(features.get("exec_risk_norm", 0.0)),
        dq_score=_safe_float(features.get("dq_score", 1.0)),
        dq_flag_count=_safe_float(features.get("dq_flag_count", 0.0)),
        signal_age_ms=_safe_float(features.get("signal_age_ms", 0.0)),
        regime_code=regime_to_code(features.get("regime")),
        session_asia=a,
        session_europe=e,
        session_us=u,
        weekend_flag=w,
        prior_winrate=0.0,
        prior_ev_r=0.0,
        prior_sample_count_log=0.0,
        horizon_ms=horizon_ms,
        tp_bps=tp_bps,
        sl_bps=sl_bps,
        y_win=y_win,
        y_util_pos=y_util_pos,
        r_mult=r_mult,
        ret_bps=ret_bps,
        features=features,
    )


def fetch_passed(conn: Any, since_ms: int, until_ms: int, max_rows: int = 200_000) -> list[dict]:
    with conn.cursor() as cur:
        cur.execute(_PASSED_SQL, (since_ms, until_ms, max_rows))
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]


def fetch_gated_out(conn: Any, since_ms: int, until_ms: int, max_rows: int = 200_000) -> list[dict]:
    with conn.cursor() as cur:
        cur.execute(_GATED_OUT_SQL, (since_ms, until_ms, max_rows))
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]


def fetch_feature_snapshots(conn: Any, sids: list[str]) -> dict[str, dict[str, float]]:
    if not sids:
        return {}
    with conn.cursor() as cur:
        cur.execute(_FEATURE_SNAPSHOT_SQL, (sids,))
        out: dict[str, dict[str, float]] = {}
        for sid, payload in cur.fetchall():
            out[str(sid)] = coerce_features(payload)
    return out


def build_dataset(
    conn: Any,
    *,
    since_ms: int,
    until_ms: int,
    flt: CompatibilityFilter | None = None,
    max_rows_per_cohort: int = 200_000,
) -> list[MetaGateTrainRow]:
    """End-to-end: query both cohorts, attach features, apply filter."""
    flt = flt or CompatibilityFilter()
    passed_recs = fetch_passed(conn, since_ms, until_ms, max_rows_per_cohort)
    gated_recs = fetch_gated_out(conn, since_ms, until_ms, max_rows_per_cohort)
    gated_sids = [str(r["sid"]) for r in gated_recs if r.get("sid")]
    snaps = fetch_feature_snapshots(conn, gated_sids) if gated_sids else {}

    rows: list[MetaGateTrainRow] = []
    for rec in passed_recs:
        r = _passed_row_from_db(rec)
        if r is not None:
            rows.append(r)
    for rec in gated_recs:
        r = _gated_out_row_from_db(rec, snaps)
        if r is not None:
            rows.append(r)

    filtered = apply_compatibility_filter(rows, flt)
    log.info(
        "conf_meta_gate dataset built passed=%d gated_out=%d kept=%d "
        "(filter horizons=%s tp=[%.1f,%.1f] sl=[%.1f,%.1f])",
        sum(1 for r in rows if r.cohort == "passed"),
        sum(1 for r in rows if r.cohort == "gated_out"),
        len(filtered),
        sorted(flt.horizon_ms_allowed) or "any",
        flt.tp_bps_min, flt.tp_bps_max,
        flt.sl_bps_min, flt.sl_bps_max,
    )
    return filtered


def write_ndjson(rows: Iterable[MetaGateTrainRow], path: str) -> int:
    """Append-friendly NDJSON writer. Returns row count."""
    n = 0
    with open(path, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(asdict(r), default=str))
            f.write("\n")
            n += 1
    return n
