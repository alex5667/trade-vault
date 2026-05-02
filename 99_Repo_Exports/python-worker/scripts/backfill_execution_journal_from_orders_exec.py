#!/usr/bin/env python3
from __future__ import annotations

"""Backfill execution journal tables from Redis `orders:exec` stream.

Purpose
-------
When P5 is rolled out after Redis stream facts have already been produced, the
SQL execution journal must be backfilled so incident analysis and Grafana/BI
queries can span pre- and post-cutover periods.

Design notes
------------
* Redis stream remains the primary online source of truth.
* Backfill is intentionally append-friendly and idempotent on the SQL side via
  `ON CONFLICT DO NOTHING` / `UPSERT` patterns.
* Snapshot rows are derived from the latest seen event per `sid` while preserving
  raw JSON payloads for forensic review.
* The script does not assume every stream message has identical fields.
"""

import argparse
import json
import os
from dataclasses import dataclass
from typing import Any, Dict, Iterable, Iterator, List, Optional, Tuple


@dataclass(frozen=True)
class ExecEventRow:
    stream_id: str
    sid: str
    symbol: str
    event_type: str
    event_ts_ms: int
    payload_jsonb: str


@dataclass(frozen=True)
class ExecSnapshotRow:
    sid: str
    symbol: str
    action: str
    status: str
    fsm_state: str
    execution_policy: str
    venue: str
    position_mode: str
    position_side: str
    working_type_policy: str
    state_jsonb: str
    created_at_ms: int
    updated_at_ms: int


@dataclass(frozen=True)
class ProtectionRefsRow:
    sid: str
    symbol: str
    sl_algo_id: Optional[int]
    sl_client_algo_id: str
    tp1_algo_id: Optional[int]
    tp2_algo_id: Optional[int]
    tp3_algo_id: Optional[int]
    trail_algo_id: Optional[int]
    trail_client_algo_id: str
    updated_at_ms: int


def _i(v: Any, default: int = 0) -> int:
    try:
        return int(float(v))
    except Exception:
        return int(default)


def parse_exec_stream_entry(stream_id: str, fields: Dict[str, Any]) -> ExecEventRow:
    payload = {str(k): v for k, v in dict(fields or {}).items()}
    return ExecEventRow(
        stream_id=str(stream_id),
        sid=str(payload.get("sid") or ""),
        symbol=str(payload.get("symbol") or ""),
        event_type=str(payload.get("event_type") or payload.get("action") or "event"),
        event_ts_ms=_i(payload.get("ts_ms") or 0),
        payload_jsonb=json.dumps(payload, ensure_ascii=False, default=str),
    )


def derive_snapshot_rows(events: Iterable[ExecEventRow]) -> Tuple[List[ExecSnapshotRow], List[ProtectionRefsRow]]:
    latest_by_sid: Dict[str, Dict[str, Any]] = {}
    for ev in events:
        try:
            payload = json.loads(ev.payload_jsonb)
        except Exception:
            payload = {}
        sid = str(payload.get("sid") or ev.sid or "")
        if not sid:
            continue
        doc = latest_by_sid.get(sid, {})
        if not doc:
            created_at_ms = ev.event_ts_ms or _i(payload.get("created_at_ms"), 0)
        else:
            created_at_ms = int(doc.get("created_at_ms") or ev.event_ts_ms or 0)
        merged = dict(doc)
        merged.update(payload)
        merged.setdefault("sid", sid)
        merged.setdefault("symbol", ev.symbol)
        merged["created_at_ms"] = created_at_ms
        merged["updated_at_ms"] = max(int(merged.get("updated_at_ms") or 0), int(ev.event_ts_ms or 0))
        latest_by_sid[sid] = merged

    snapshots: List[ExecSnapshotRow] = []
    refs: List[ProtectionRefsRow] = []
    for sid, doc in latest_by_sid.items():
        snapshots.append(
            ExecSnapshotRow(
                sid=sid,
                symbol=str(doc.get("symbol") or ""),
                action=str(doc.get("action") or ""),
                status=str(doc.get("status") or ""),
                fsm_state=str(doc.get("fsm_state") or ""),
                execution_policy=str(doc.get("execution_policy") or ""),
                venue=str(doc.get("venue") or "binance"),
                position_mode=str(doc.get("position_mode") or ""),
                position_side=str(doc.get("position_side") or ""),
                working_type_policy=str(doc.get("working_type_policy") or ""),
                state_jsonb=json.dumps(doc, ensure_ascii=False, default=str),
                created_at_ms=_i(doc.get("created_at_ms") or doc.get("ts_ms") or 0),
                updated_at_ms=_i(doc.get("updated_at_ms") or doc.get("ts_ms") or 0),
            )
        )
        refs.append(
            ProtectionRefsRow(
                sid=sid,
                symbol=str(doc.get("symbol") or ""),
                sl_algo_id=_i(doc.get("sl_algo_id"), 0) or None,
                sl_client_algo_id=str(doc.get("sl_client_algo_id") or ""),
                tp1_algo_id=_i(doc.get("tp1_algo_id"), 0) or None,
                tp2_algo_id=_i(doc.get("tp2_algo_id"), 0) or None,
                tp3_algo_id=_i(doc.get("tp3_algo_id"), 0) or None,
                trail_algo_id=_i(doc.get("trail_algo_id"), 0) or None,
                trail_client_algo_id=str(doc.get("trail_client_algo_id") or ""),
                updated_at_ms=_i(doc.get("updated_at_ms") or doc.get("ts_ms") or 0),
            )
        )
    return snapshots, refs


def _iter_stream(redis_client: Any, stream: str, count: int = 1000) -> Iterator[ExecEventRow]:
    cursor = "-"
    while True:
        rows = redis_client.xrange(stream, min=cursor, max="+", count=count)
        if not rows:
            break
        for stream_id, fields in rows:
            yield parse_exec_stream_entry(stream_id, fields)
            cursor = stream_id
        # advance after the last emitted id to avoid endless repetition
        major, _, minor = str(cursor).partition("-")
        cursor = f"{major}-{int(minor or 0) + 1}"


def _connect_redis():
    import redis  # type: ignore
    return redis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379/0"), decode_responses=True)


def _connect_pg(dsn: str):
    try:
        import psycopg  # type: ignore
        return psycopg.connect(dsn)
    except Exception:  # pragma: no cover
        import psycopg2  # type: ignore
        return psycopg2.connect(dsn)


def _write_pg(conn: Any, events: Iterable[ExecEventRow], snapshots: Iterable[ExecSnapshotRow], refs: Iterable[ProtectionRefsRow]) -> None:
    with conn:
        with conn.cursor() as cur:
            for ev in events:
                cur.execute(
                    "INSERT INTO execution_order_events (sid, symbol, event_type, event_ts_ms, payload_jsonb) VALUES (%s,%s,%s,%s,%s::jsonb) ON CONFLICT DO NOTHING",
                    (ev.sid, ev.symbol, ev.event_type, ev.event_ts_ms, ev.payload_jsonb),
                )
            for row in snapshots:
                cur.execute(
                    """
                    INSERT INTO execution_orders (sid, symbol, action, status, fsm_state, execution_policy, venue, position_mode, position_side, working_type_policy, state_jsonb, created_at_ms, updated_at_ms)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s,%s)
                    ON CONFLICT (sid) DO UPDATE SET
                      symbol = EXCLUDED.symbol,
                      action = EXCLUDED.action,
                      status = EXCLUDED.status,
                      fsm_state = EXCLUDED.fsm_state,
                      execution_policy = EXCLUDED.execution_policy,
                      venue = EXCLUDED.venue,
                      position_mode = EXCLUDED.position_mode,
                      position_side = EXCLUDED.position_side,
                      working_type_policy = EXCLUDED.working_type_policy,
                      state_jsonb = EXCLUDED.state_jsonb,
                      updated_at_ms = GREATEST(execution_orders.updated_at_ms, EXCLUDED.updated_at_ms)
                    """
                    (row.sid, row.symbol, row.action, row.status, row.fsm_state, row.execution_policy, row.venue, row.position_mode, row.position_side, row.working_type_policy, row.state_jsonb, row.created_at_ms, row.updated_at_ms),
                )
            for row in refs:
                cur.execute(
                    """
                    INSERT INTO execution_protection_refs (sid, symbol, sl_algo_id, sl_client_algo_id, tp1_algo_id, tp2_algo_id, tp3_algo_id, trail_algo_id, trail_client_algo_id, updated_at_ms)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (sid) DO UPDATE SET
                      symbol = EXCLUDED.symbol,
                      sl_algo_id = COALESCE(EXCLUDED.sl_algo_id, execution_protection_refs.sl_algo_id),
                      sl_client_algo_id = COALESCE(NULLIF(EXCLUDED.sl_client_algo_id, ''), execution_protection_refs.sl_client_algo_id),
                      tp1_algo_id = COALESCE(EXCLUDED.tp1_algo_id, execution_protection_refs.tp1_algo_id),
                      tp2_algo_id = COALESCE(EXCLUDED.tp2_algo_id, execution_protection_refs.tp2_algo_id),
                      tp3_algo_id = COALESCE(EXCLUDED.tp3_algo_id, execution_protection_refs.tp3_algo_id),
                      trail_algo_id = COALESCE(EXCLUDED.trail_algo_id, execution_protection_refs.trail_algo_id),
                      trail_client_algo_id = COALESCE(NULLIF(EXCLUDED.trail_client_algo_id, ''), execution_protection_refs.trail_client_algo_id),
                      updated_at_ms = GREATEST(execution_protection_refs.updated_at_ms, EXCLUDED.updated_at_ms)
                    """
                    (row.sid, row.symbol, row.sl_algo_id, row.sl_client_algo_id, row.tp1_algo_id, row.tp2_algo_id, row.tp3_algo_id, row.trail_algo_id, row.trail_client_algo_id, row.updated_at_ms),
                )


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Backfill execution journal SQL tables from Redis orders:exec stream.")
    parser.add_argument("--stream", default=os.getenv("EXEC_STREAM", "orders:exec"))
    parser.add_argument("--journal-dsn", default=os.getenv("EXECUTION_JOURNAL_DSN", ""))
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)

    if not args.journal_dsn and not args.dry_run:
        raise SystemExit("EXECUTION_JOURNAL_DSN/--journal-dsn is required unless --dry-run is used")

    r = _connect_redis()
    events = list(_iter_stream(r, args.stream))
    snapshots, refs = derive_snapshot_rows(events)
    print(f"loaded events={len(events)} snapshots={len(snapshots)} refs={len(refs)} from stream={args.stream}")

    if args.dry_run:
        return 0

    conn = _connect_pg(args.journal_dsn)
    _write_pg(conn, events, snapshots, refs)
    print("backfill completed")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
