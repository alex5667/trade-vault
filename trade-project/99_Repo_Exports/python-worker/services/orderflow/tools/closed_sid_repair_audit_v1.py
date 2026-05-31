from __future__ import annotations

"""
Audit and optional repair helper for historical `trades:closed` SIDs.

Why this exists:
  - `trades:closed` stream entries are immutable once written.
  - Older rows may carry non-canonical SIDs such as `weak_progress:SYMBOL:TS:L`.
  - Dataset joins expect canonical `crypto-of:SYMBOL:TS`.

This tool does NOT rewrite Redis stream entries in place.
It provides:
  1. a report quantifying repairable historical rows;
  2. an NDJSON manifest of candidate repairs;
  3. optional best-effort update of mutable `order:{order_id}` hashes only.
"""

import argparse
import json
import os
from collections.abc import Iterable
from typing import Any

import redis

from infra.redis_repo import _normalize_crypto_sid


def _env(name: str, default: str) -> str:
    v = os.getenv(name)
    return v if (v is not None and str(v).strip() != "") else default


def _to_str(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, (bytes, bytearray)):
        return v.decode("utf-8", "ignore")
    return str(v)


def _to_int(v: Any, default: int = 0) -> int:
    try:
        if v is None:
            return default
        return int(float(_to_str(v).strip()))
    except Exception:
        return default


def _decode_fields(fields: dict[Any, Any]) -> dict[str, Any]:
    return {_to_str(k): _to_str(v) for k, v in dict(fields or {}).items()}


def _stream_id_ts_ms(stream_id: str) -> int:
    try:
        return int(str(stream_id).split("-", 1)[0])
    except Exception:
        return 0


def inspect_closed_record(stream_id: str, fields: dict[str, Any]) -> dict[str, Any]:
    oid = _to_str(fields.get("order_id")).strip()
    raw_sid = _to_str(fields.get("sid") or fields.get("signal_id")).strip()
    symbol = _to_str(fields.get("symbol")).strip().upper()
    exit_ts_ms = _to_int(fields.get("exit_ts_ms") or fields.get("closed_time") or fields.get("ts_close"))
    if exit_ts_ms <= 0:
        exit_ts_ms = _stream_id_ts_ms(stream_id)

    normalized_sid = _normalize_crypto_sid(raw_sid, symbol=symbol, ts_ms=exit_ts_ms)
    kind = raw_sid.split(":", 1)[0] if raw_sid and ":" in raw_sid else ""
    canonical = bool(raw_sid and normalized_sid == raw_sid and raw_sid.startswith("crypto-of:"))
    repairable = bool(raw_sid and normalized_sid and normalized_sid != raw_sid and normalized_sid.startswith("crypto-of:"))
    missing_sid = not bool(raw_sid)
    weak_progress_sid = raw_sid.startswith("weak_progress:")

    if missing_sid:
        status = "missing_sid"
    elif canonical:
        status = "canonical"
    elif repairable:
        status = "repairable"
    else:
        status = "unrepairable"

    return {
        "stream_id": stream_id,
        "order_id": oid,
        "symbol": symbol,
        "exit_ts_ms": int(exit_ts_ms),
        "old_sid": raw_sid,
        "new_sid": normalized_sid,
        "kind": kind,
        "status": status,
        "canonical": canonical,
        "repairable": repairable,
        "missing_sid": missing_sid,
        "weak_progress_sid": weak_progress_sid,
    }


def _iter_recent_closed(cli: redis.Redis, stream: str, max_scan: int) -> Iterable[tuple[str, dict[str, Any]]]:
    rows = cli.xrevrange(stream, "+", "-", count=int(max_scan))
    for stream_id, fields in rows:
        yield _to_str(stream_id), _decode_fields(fields)


def run_audit(
    cli: redis.Redis,
    *,
    stream: str,
    max_scan: int,
    write_order_hash: bool,
    manifest_path: str | None,
    sample_limit: int,
) -> dict[str, Any]:
    counts = {
        "scanned": 0,
        "canonical": 0,
        "repairable": 0,
        "repairable_weak_progress": 0,
        "missing_sid": 0,
        "unrepairable": 0,
        "order_hash_repaired": 0,
    }
    samples: list[dict[str, Any]] = []

    manifest_fh = open(manifest_path, "w", encoding="utf-8") if manifest_path else None
    try:
        for stream_id, fields in _iter_recent_closed(cli, stream, max_scan):
            item = inspect_closed_record(stream_id, fields)
            counts["scanned"] += 1
            counts[item["status"]] += 1
            if item["repairable"] and item["weak_progress_sid"]:
                counts["repairable_weak_progress"] += 1

            if item["repairable"]:
                if len(samples) < int(sample_limit):
                    samples.append(item)
                if manifest_fh is not None:
                    manifest_fh.write(json.dumps(item, ensure_ascii=False) + "\n")
                if write_order_hash and item["order_id"]:
                    cli.hset(
                        f"order:{item['order_id']}",
                        mapping={
                            "sid": item["new_sid"],
                            "signal_id": item["new_sid"],
                            "sid_repair_old": item["old_sid"],
                            "sid_repair_ts_ms": str(item["exit_ts_ms"]),
                        },
                    )
                    counts["order_hash_repaired"] += 1
    finally:
        if manifest_fh is not None:
            manifest_fh.close()

    repairable_ratio = (counts["repairable"] / counts["scanned"]) if counts["scanned"] else 0.0
    return {
        "stream": stream,
        "max_scan": int(max_scan),
        "write_order_hash": bool(write_order_hash),
        "stream_entries_rewritten": False,
        "note": "Redis streams are immutable; only order:{id} hashes can be repaired in place. Use the manifest for any backfill/rebuild workflow.",
        "counts": counts,
        "repairable_ratio": repairable_ratio,
        "samples": samples,
    }


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Audit historical trades:closed sid canonicalization drift.")
    ap.add_argument("--redis_url", default=_env("REDIS_URL", "redis://localhost:6379/0"))
    ap.add_argument("--stream", default=_env("TRADES_CLOSED_STREAM_NAME", "trades:closed"))
    ap.add_argument("--max_scan", type=int, default=int(_env("CLOSED_SID_AUDIT_MAX_SCAN", "200000")))
    ap.add_argument("--sample_limit", type=int, default=int(_env("CLOSED_SID_AUDIT_SAMPLE_LIMIT", "20")))
    ap.add_argument("--write_order_hash", type=int, choices=[0, 1], default=0)
    ap.add_argument("--out_json", default="")
    ap.add_argument("--out_manifest_ndjson", default="")
    args = ap.parse_args(argv)

    cli = redis.Redis.from_url(args.redis_url, decode_responses=True)
    report = run_audit(
        cli,
        stream=str(args.stream),
        max_scan=int(args.max_scan),
        write_order_hash=bool(int(args.write_order_hash)),
        manifest_path=str(args.out_manifest_ndjson or "") or None,
        sample_limit=int(args.sample_limit),
    )

    if args.out_json:
        with open(args.out_json, "w", encoding="utf-8") as fh:
            json.dump(report, fh, ensure_ascii=False, indent=2)

    print(json.dumps(report, ensure_ascii=False))
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
