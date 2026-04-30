#!/usr/bin/env python3
"""
P5.6 execution audit-chain checker.

Purpose
-------
Validate that execution linkage is preserved across the most important entities:

- execution_orders            -> seed execution events / orders
- signals                     -> signal envelope exists
- signal_execution_plan       -> plan row exists
- trades_closed               -> closed trade row exists
- position_events             -> position lifecycle references exist
- entry_policy_audit          -> entry-policy evidence exists
- decision_snapshot           -> analytics snapshot row exists

Outputs
-------
1) JSON report for runbook/UI consumption
2) Prometheus textfile exporter for node_exporter textfile collector

The checker is intentionally conservative and read-only.
It supports partial environments: if some downstream tables are not present yet
this is surfaced in the report rather than silently ignored.

Usage
-----
python scripts/check_execution_audit_chain.py \\
  --dsn "$TRADES_DB_DSN" \\
  --report-json /var/lib/node_exporter/textfile_collector/latest_execution_audit_chain.json \\
  --report-prom /var/lib/node_exporter/textfile_collector/latest_execution_audit_chain.prom
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Set

try:
    import psycopg2  # type: ignore
except Exception:  # pragma: no cover - optional in unit tests
    psycopg2 = None  # type: ignore


DEFAULT_REPORT_JSON = "latest_execution_audit_chain.json"
DEFAULT_REPORT_PROM = "latest_execution_audit_chain.prom"
DEFAULT_LOOKBACK_HOURS = 24
DEFAULT_LIMIT = 1000


@dataclass(frozen=True)
class AuditRow:
    """Single seed row from execution_orders for verification."""
    sid: str
    signal_id: str
    closed_trade_id: str
    symbol: str
    source_ts: Optional[float] = None


def env_str(name: str, default: str) -> str:
    """Read a string env var with a fallback default."""
    value = os.getenv(name)
    return str(value).strip() if value is not None and str(value).strip() else default


def env_int(name: str, default: int) -> int:
    """Read an integer env var with a fallback default; returns default on parse error."""
    raw = os.getenv(name)
    if raw is None or not str(raw).strip():
        return default
    try:
        return int(str(raw).strip())
    except Exception:
        return default


def normalize_id(value: Any) -> str:
    """Normalize DB value to a str key; None -> empty string."""
    if value is None:
        return ""
    return str(value).strip()


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    """Parse CLI arguments; env vars provide defaults."""
    parser = argparse.ArgumentParser(description="Check execution audit-chain health")
    parser.add_argument(
        "--dsn"
        default=env_str("TRADES_DB_DSN", env_str("DATABASE_URL", ""))
    )
    parser.add_argument(
        "--lookback-hours"
        type=int
        default=env_int("EXEC_AUDIT_LOOKBACK_HOURS", DEFAULT_LOOKBACK_HOURS)
    )
    parser.add_argument(
        "--limit"
        type=int
        default=env_int("EXEC_AUDIT_LIMIT", 10000)
    )
    parser.add_argument(
        "--report-json"
        default=env_str("EXEC_AUDIT_REPORT_JSON", DEFAULT_REPORT_JSON)
    )
    parser.add_argument(
        "--report-prom"
        default=env_str("EXEC_AUDIT_REPORT_PROM", DEFAULT_REPORT_PROM)
    )
    return parser.parse_args(argv)


def table_name(env_name: str, default: str) -> str:
    """Read a table name from env with default."""
    return env_str(env_name, default)


def get_table_map() -> Dict[str, str]:
    """Return the mapping of logical role -> actual table name (configurable via ENV)."""
    return {
        "execution_orders": table_name("EXEC_AUDIT_TBL_EXECUTION_ORDERS", "execution_orders")
        "signals": table_name("EXEC_AUDIT_TBL_SIGNALS", "signals")
        "signal_execution_plan": table_name("EXEC_AUDIT_TBL_SIGNAL_EXECUTION_PLAN", "signal_execution_plan")
        "trades_closed": table_name("EXEC_AUDIT_TBL_TRADES_CLOSED", "trades_closed")
        "position_events": table_name("EXEC_AUDIT_TBL_POSITION_EVENTS", "position_events")
        "entry_policy_audit": table_name("EXEC_AUDIT_TBL_ENTRY_POLICY_AUDIT", "entry_policy_audit")
        "decision_snapshot": table_name("EXEC_AUDIT_TBL_DECISION_SNAPSHOT", "decision_snapshot")
    }


def _split_table_name(name: str) -> Sequence[str]:
    """Split 'schema.table' into (schema, table); defaults to ('public', table)."""
    parts = [p for p in str(name).split(".") if p]
    if len(parts) == 1:
        return ("public", parts[0])
    if len(parts) >= 2:
        return (parts[-2], parts[-1])
    return ("public", str(name))


def discover_existing_tables(conn: Any, tables: Mapping[str, str]) -> Set[str]:
    """
    Query information_schema to discover which of the desired tables actually exist.
    Returns a set of logical role names (e.g. 'signals', 'trades_closed').
    Supports partial environments where some tables may not yet be deployed.
    """
    wanted_pairs = {_split_table_name(v) for v in tables.values()}
    with conn.cursor() as cur:
        cur.execute(
            """
            select table_schema, table_name
            from information_schema.tables
            where (table_schema, table_name) in (
                values %s
            )
            """.replace("%s", ", ".join(["(%s,%s)"] * len(wanted_pairs)))
            [item for pair in wanted_pairs for item in pair]
        )
        rows = cur.fetchall()
    existing_pairs = {(str(r[0]), str(r[1])) for r in rows}
    existing: Set[str] = set()
    for logical, table in tables.items():
        if tuple(_split_table_name(table)) in existing_pairs:
            existing.add(logical)
    return existing


def read_seed_execution_orders(
    conn: Any, table: str, lookback_hours: int, limit: int
) -> List[AuditRow]:
    """
    Read seed rows from execution_orders within lookback window.
    Returns list of AuditRow objects for downstream linkage verification.
    """
    schema, raw_table = _split_table_name(table)
    sql = f"""
        select
            coalesce(cast(sid as text), '') as sid
            coalesce(cast(signal_id as text), '') as signal_id
            coalesce(cast(closed_trade_id as text), '') as closed_trade_id
            coalesce(cast(symbol as text), '') as symbol
            created_at_ms / 1000.0 as source_ts
        from {schema}.{raw_table}
        where created_at_ms >= (extract(epoch from now() - (%s || ' hours')::interval) * 1000)
        order by created_at_ms desc
        limit %s
    """
    with conn.cursor() as cur:
        cur.execute(sql, (int(lookback_hours), int(limit)))
        rows = cur.fetchall()
    return [
        AuditRow(
            sid=normalize_id(r[0])
            signal_id=normalize_id(r[1])
            closed_trade_id=normalize_id(r[2])
            symbol=normalize_id(r[3])
            source_ts=float(r[4]) if r[4] is not None else None
        )
        for r in rows
    ]


def build_lookup_set(conn: Any, table: str, columns: Sequence[str]) -> Set[str]:
    """
    Build a set of composite keys from *columns* in *table*.
    Key format: 'col1_val|col2_val'.
    Used for O(1) existence checks during audit.
    """
    schema, raw_table = _split_table_name(table)
    # Use qualified column references to prevent alias ambiguity
    expr = " || '|' || ".join([f"coalesce(cast({raw_table}.{c} as text), '')" for c in columns])
    sql = f"select distinct {expr} as k from {schema}.{raw_table}"
    with conn.cursor() as cur:
        cur.execute(sql)
        return {normalize_id(row[0]) for row in cur.fetchall()}


def analyze_chain_rows(
    seed_rows: Sequence[AuditRow]
    signal_keys: Set[str]
    plan_keys: Set[str]
    trade_keys: Set[str]
    position_event_keys: Set[str]
    entry_policy_keys: Set[str]
    decision_snapshot_keys: Set[str]
    *
    now_ts: Optional[float] = None
    existing_tables: Optional[Iterable[str]] = None
    lookback_hours: int = DEFAULT_LOOKBACK_HOURS
) -> Dict[str, Any]:
    """
    Core linkage checker: for each seed execution_order row, verify that
    all downstream tables (when they exist) have a matching key.
    Broken links are collected by 'kind' for Prometheus cardinality.
    """
    existing = set(existing_tables or [])
    broken: List[Dict[str, Any]] = []
    counts: Dict[str, int] = {}

    def add(kind: str, row: AuditRow, details: Optional[Mapping[str, Any]] = None) -> None:
        """Record a broken link entry with row metadata."""
        counts[kind] = counts.get(kind, 0) + 1
        item: Dict[str, Any] = {
            "kind": kind
            "sid": row.sid
            "signal_id": row.signal_id
            "closed_trade_id": row.closed_trade_id
            "symbol": row.symbol
            "source_ts": row.source_ts
        }
        if details:
            item.update(dict(details))
        broken.append(item)

    for row in seed_rows:
        sid = row.sid
        signal_id = row.signal_id
        closed_trade_id = row.closed_trade_id

        sid_trade = f"{sid}|{closed_trade_id}"

        # Check upstream signal linkage
        if "signals" in existing and signal_id and signal_id not in signal_keys:
            add("broken_signal_link", row)
        # Check signal execution plan linkage
        if "signal_execution_plan" in existing and signal_id and signal_id not in plan_keys:
            add("broken_signal_plan", row)
        # Check closed trade linkage (only if closed_trade_id is set)
        if "trades_closed" in existing and closed_trade_id and sid_trade not in trade_keys:
            add("broken_trade_link", row)
        # Check position event linkage
        if "position_events" in existing and closed_trade_id and sid_trade not in position_event_keys:
            add("broken_position_event_link", row)
        # Check entry policy audit linkage
        if "entry_policy_audit" in existing and sid not in entry_policy_keys:
            add("broken_entry_policy_link", row)
        # Check decision snapshot (analytics) linkage
        if "decision_snapshot" in existing and sid not in decision_snapshot_keys:
            add("broken_analytics_link", row)

    now_ts = float(now_ts if now_ts is not None else time.time())
    report = {
        "schema_version": "p5.6.v1"
        "generated_at_ts": now_ts
        "generated_at_iso": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(now_ts))
        "lookback_hours": int(lookback_hours)
        "seed_rows": len(seed_rows)
        "existing_tables": sorted(existing)
        "total_broken": sum(counts.values())
        "broken_by_kind": dict(sorted(counts.items()))
        "broken": broken
    }
    return report


def render_textfile_metrics(
    report: Mapping[str, Any], *, now_ts: Optional[float] = None
) -> str:
    """
    Render Prometheus textfile exporter format from a report dict.
    Includes: timestamp, freshness, stale flag, total broken, broken by kind.
    """
    generated_at_ts = float(report.get("generated_at_ts") or 0.0)
    now_ts = float(now_ts if now_ts is not None else time.time())
    freshness = max(0.0, now_ts - generated_at_ts) if generated_at_ts > 0 else float("nan")
    stale_threshold = float(env_int("EXEC_AUDIT_REPORT_STALE_SECONDS", 900))
    # NaN check: freshness != freshness is True when freshness is NaN
    stale = 1 if freshness != freshness or freshness > stale_threshold else 0

    lines = [
        "# HELP trade_execution_audit_chain_report_timestamp_seconds Unix timestamp of the last generated audit-chain report"
        "# TYPE trade_execution_audit_chain_report_timestamp_seconds gauge"
        f"trade_execution_audit_chain_report_timestamp_seconds {generated_at_ts:.0f}"
        "# HELP trade_execution_audit_chain_report_freshness_seconds Age of the last generated audit-chain report"
        "# TYPE trade_execution_audit_chain_report_freshness_seconds gauge"
        f"trade_execution_audit_chain_report_freshness_seconds {0.0 if freshness != freshness else freshness:.6f}"
        "# HELP trade_execution_audit_chain_report_stale 1 when the report is stale beyond configured threshold"
        "# TYPE trade_execution_audit_chain_report_stale gauge"
        f"trade_execution_audit_chain_report_stale {stale}"
        "# HELP trade_execution_audit_chain_total_broken Total number of broken execution audit-chain links"
        "# TYPE trade_execution_audit_chain_total_broken gauge"
        f"trade_execution_audit_chain_total_broken {int(report.get('total_broken') or 0)}"
        "# HELP trade_execution_audit_chain_broken_total Broken execution audit-chain rows by kind"
        "# TYPE trade_execution_audit_chain_broken_total gauge"
    ]
    for kind, value in sorted(dict(report.get("broken_by_kind") or {}).items()):
        safe_kind = str(kind).replace('"', '\\"')
        lines.append(f'trade_execution_audit_chain_broken_total{{kind="{safe_kind}"}} {int(value)}')
    return "\n".join(lines) + "\n"


def atomic_write_text(path: str, text: str) -> None:
    """Write *text* to *path* atomically via a tmp file + rename."""
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_suffix(target.suffix + ".tmp")
    tmp.write_text(text, encoding="utf-8")
    tmp.replace(target)


def write_json_report(path: str, report: Mapping[str, Any]) -> None:
    """Write report dict as formatted JSON to *path* atomically."""
    atomic_write_text(path, json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n")


def write_textfile_report(
    path: str, report: Mapping[str, Any], *, now_ts: Optional[float] = None
) -> None:
    """Write Prometheus textfile metrics to *path* atomically."""
    atomic_write_text(path, render_textfile_metrics(report, now_ts=now_ts))


def build_report_from_db(dsn: str, *, lookback_hours: int, limit: int) -> Dict[str, Any]:
    """
    Connect to DB via *dsn*, discover tables, read seed rows, and run linkage analysis.
    Returns the complete report dict.
    """
    if not dsn:
        raise RuntimeError("missing TRADES_DB_DSN/DATABASE_URL")
    if psycopg2 is None:
        raise RuntimeError("psycopg2 is required to read audit-chain data")

    tables = get_table_map()
    conn = psycopg2.connect(dsn)
    try:
        existing = discover_existing_tables(conn, tables)
        if "execution_orders" not in existing:
            raise RuntimeError(f"required table missing: {tables['execution_orders']}")

        seed_rows = read_seed_execution_orders(
            conn, tables["execution_orders"], lookback_hours, limit
        )
        # Build lookup sets only for tables that actually exist
        signal_keys = (
            build_lookup_set(conn, tables["signals"], ["signal_id"])
            if "signals" in existing
            else set()
        )
        plan_keys = (
            build_lookup_set(conn, tables["signal_execution_plan"], ["signal_id"])
            if "signal_execution_plan" in existing
            else set()
        )
        trade_keys = (
            build_lookup_set(conn, tables["trades_closed"], ["sid", "order_id"])
            if "trades_closed" in existing
            else set()
        )
        position_event_keys = (
            build_lookup_set(conn, tables["position_events"], ["sid", "position_id"])
            if "position_events" in existing
            else set()
        )
        entry_policy_keys = (
            build_lookup_set(conn, tables["entry_policy_audit"], ["sid"])
            if "entry_policy_audit" in existing
            else set()
        )
        decision_snapshot_keys = (
            build_lookup_set(conn, tables["decision_snapshot"], ["sid"])
            if "decision_snapshot" in existing
            else set()
        )
        return analyze_chain_rows(
            seed_rows
            signal_keys
            plan_keys
            trade_keys
            position_event_keys
            entry_policy_keys
            decision_snapshot_keys
            existing_tables=existing
            lookback_hours=lookback_hours
        )
    finally:
        conn.close()


def main(argv: Optional[Sequence[str]] = None) -> int:
    """Entry point: parse args, run audit chain check, write reports."""
    args = parse_args(argv)
    try:
        report = build_report_from_db(
            args.dsn, lookback_hours=args.lookback_hours, limit=args.limit
        )
        write_json_report(args.report_json, report)
        write_textfile_report(args.report_prom, report)
        print(
            json.dumps(
                {
                    "ok": True
                    "report_json": args.report_json
                    "report_prom": args.report_prom
                    "total_broken": int(report.get("total_broken") or 0)
                }
                ensure_ascii=False
            )
        )
        return 0
    except Exception as exc:
        failure_ts = time.time()
        failure_report = {
            "schema_version": "p5.6.v1"
            "generated_at_ts": failure_ts
            "generated_at_iso": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(failure_ts))
            "error": str(exc)
            "total_broken": 0
            "broken_by_kind": {}
            "broken": []
            "seed_rows": 0
        }
        try:
            write_json_report(args.report_json, failure_report)
            write_textfile_report(args.report_prom, failure_report, now_ts=failure_ts)
        except Exception:
            pass
        print(json.dumps({"ok": False, "error": str(exc)}, ensure_ascii=False), file=sys.stderr)
        return 2


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
