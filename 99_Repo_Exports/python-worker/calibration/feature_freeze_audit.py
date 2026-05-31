"""
feature_freeze_audit.py — Phase 2.2 leakage guard.

Audits signal_outcome.features JSONB for point-in-time violations:
any nested value with a `_ts_ms` (or `ts_ms`, `timestamp_ms`) field
greater than decision_time_ms indicates look-ahead leakage.

Two entry points:
  * audit_record(decision_time_ms, features) -> list[Violation]
    Pure function for unit tests / one-off checks.
  * scan_db(conn, since_ms, until_ms, limit) -> dict
    Scans a window of signal_outcome rows; returns counters
    and a sample of violations for ops dashboards.

Design notes:
  * Pure-Python, no external deps for audit_record.
  * Scanner uses psycopg2 only when invoked.
  * Tolerance window (FREEZE_TOLERANCE_MS, default 0) allows benign
    clock skew between producers without false positives.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterator

_TS_KEYS = ("_ts_ms", "ts_ms", "timestamp_ms", "event_time_ms", "ingest_time_ms")


@dataclass(frozen=True)
class Violation:
    path: str
    value_ts_ms: int
    decision_time_ms: int
    delta_ms: int


def _walk(node: Any, path: str = "") -> Iterator[tuple[str, Any]]:
    if isinstance(node, dict):
        for k, v in node.items():
            yield from _walk(v, f"{path}.{k}" if path else str(k))
    elif isinstance(node, list):
        for i, v in enumerate(node):
            yield from _walk(v, f"{path}[{i}]")
    else:
        yield path, node


def audit_record(
    decision_time_ms: int,
    features: dict,
    tolerance_ms: int = 0,
) -> list[Violation]:
    """Returns list of leakage violations (ts > decision_time_ms + tolerance)."""
    if not features:
        return []
    out: list[Violation] = []
    for path, value in _walk(features):
        leaf_key = path.rsplit(".", 1)[-1]
        if leaf_key not in _TS_KEYS:
            continue
        try:
            ts = int(value)
        except (TypeError, ValueError):
            continue
        # Sanity: ignore obviously bad values (epoch < 2000-01-01)
        if ts < 946_684_800_000:
            continue
        delta = ts - decision_time_ms
        if delta > tolerance_ms:
            out.append(
                Violation(
                    path=path,
                    value_ts_ms=ts,
                    decision_time_ms=decision_time_ms,
                    delta_ms=delta,
                )
            )
    return out


def scan_db(
    conn: Any,
    since_ms: int,
    until_ms: int,
    limit: int = 1000,
    tolerance_ms: int = 0,
) -> dict:
    """Scans signal_outcome rows in [since_ms, until_ms]; returns audit report."""
    sql = """
        SELECT sid, decision_time_ms, symbol, source, features
        FROM signal_outcome
        WHERE decision_time_ms >= %s AND decision_time_ms < %s
        ORDER BY decision_time_ms DESC
        LIMIT %s
    """
    samples: list[dict] = []
    n_scanned = 0
    n_violated = 0
    max_delta = 0
    with conn.cursor() as cur:
        cur.execute(sql, (since_ms, until_ms, limit))
        for sid, dt_ms, symbol, source, features in cur.fetchall():
            n_scanned += 1
            feats = features if isinstance(features, dict) else {}
            viols = audit_record(int(dt_ms), feats, tolerance_ms=tolerance_ms)
            if viols:
                n_violated += 1
                worst = max(v.delta_ms for v in viols)
                max_delta = max(max_delta, worst)
                if len(samples) < 20:
                    samples.append(
                        dict(
                            sid=sid,
                            symbol=symbol,
                            source=source,
                            decision_time_ms=int(dt_ms),
                            violations=[
                                dict(path=v.path, delta_ms=v.delta_ms) for v in viols
                            ],
                        )
                    )
    return dict(
        scanned=n_scanned,
        violated=n_violated,
        violation_rate=(n_violated / n_scanned) if n_scanned else 0.0,
        max_delta_ms=max_delta,
        samples=samples,
    )
