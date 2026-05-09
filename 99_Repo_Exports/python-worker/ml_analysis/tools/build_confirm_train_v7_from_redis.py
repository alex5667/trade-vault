#!/usr/bin/env python3
from __future__ import annotations

"""Build confirm_train_v7 NDJSON dataset by joining decisions:final + trades:closed.

Produces two files consumed by downstream nightly timers:
  1) decisions NDJSON (latest_confirm_train_v7.ndjson) — full decision payload per SID
  2) outcomes  NDJSON (latest_outcomes.ndjson)         — outcome per SID

Sources:
  - decisions:final  — Redis stream with `payload` JSON field (decision record v1)
  - trades:closed    — Redis stream with PnL/risk/exit data

Join key: canonical SID = crypto-of:{SYMBOL}:{ts_ms}

CLI example:
  python -m ml_analysis.tools.build_confirm_train_v7_from_redis \\
    --redis_url redis://redis-worker-1:6379/0 \\
    --out_decisions /var/lib/trade/training/latest_confirm_train_v7.ndjson \\
    --out_outcomes  /var/lib/trade/training/latest_outcomes.ndjson \\
    --out_report    /var/lib/trade/training/confirm_v7_report.json
"""
import argparse
import gzip
import json
import os
import re
import sys
import time
from typing import Any

from utils.time_utils import get_ny_time_millis

# ─── helpers ──────────────────────────────────────────────────────────────────

def _now_ms() -> int:
    return get_ny_time_millis()


def _as_str(x: Any) -> str:
    if x is None:
        return ""
    if isinstance(x, bytes):
        return x.decode("utf-8", "ignore")
    return str(x)


def _as_int(x: Any, default: int = 0) -> int:
    if x is None:
        return default
    if isinstance(x, (int, float)):
        try:
            return int(x)
        except Exception:
            return default
    if isinstance(x, bytes):
        try:
            x = x.decode("utf-8", "ignore")
        except Exception:
            return default
    try:
        s = str(x).strip()
        return int(float(s)) if s else default
    except Exception:
        return default


def _as_float(x: Any, default: float = 0.0) -> float:
    if x is None:
        return default
    if isinstance(x, (int, float)):
        try:
            return float(x)
        except Exception:
            return default
    if isinstance(x, bytes):
        try:
            x = x.decode("utf-8", "ignore")
        except Exception:
            return default
    try:
        s = str(x).strip()
        return float(s) if s else default
    except Exception:
        return default


def _safe_json_loads(x: Any) -> Any:
    if x is None:
        return None
    if isinstance(x, (dict, list)):
        return x
    if isinstance(x, bytes):
        try:
            x = x.decode("utf-8", "ignore")
        except Exception:
            return None
    if not isinstance(x, str):
        return None
    s = x.strip()
    if not s:
        return None
    try:
        return json.loads(s)
    except Exception:
        return None


def _normalize_sid(raw_sid: Any, *, symbol="", ts_ms: int = 0) -> str:
    """Normalize/derive canonical SID: crypto-of:{SYMBOL}:{ts_ms}."""
    s = _as_str(raw_sid).strip()
    if s.startswith("crypto-of:"):
        parts = s.split(":")
        if len(parts) >= 3:
            sym = (parts[1] or symbol or "").upper()
            try:
                t = int(parts[2])
            except Exception:
                t = ts_ms
            return f"crypto-of:{sym}:{t}"
    if "|" in s:
        parts = s.split("|")
        if len(parts) >= 2:
            sym = (parts[0] or symbol or "").upper()
            try:
                t = int(parts[1])
            except Exception:
                t = ts_ms
            return f"crypto-of:{sym}:{t}"
    if symbol and ts_ms > 0:
        return f"crypto-of:{symbol.upper()}:{ts_ms}"
    return s


def _decode_fields(fields: dict[Any, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for k, v in (fields or {}).items():
        kk = _as_str(k)
        out[kk] = v.decode("utf-8", "ignore") if isinstance(v, bytes) else v
    return out


# ─── Redis stream reader ─────────────────────────────────────────────────────

def _xrevrange_recent(r: Any, stream: str, *, count: int) -> list[tuple[str, dict[str, Any]]]:
    """Read last N entries from a Redis stream in reverse order."""
    out: list[tuple[str, dict[str, Any]]] = []
    last_id = "+"
    remaining = int(count)
    chunk_size = 1000

    while remaining > 0:
        batch_size = min(remaining, chunk_size)
        items = r.xrevrange(stream, max=last_id, min="-", count=batch_size)
        if not items:
            break
        if len(items) == 1 and _as_str(items[0][0]) == _as_str(last_id):
            break

        for _id, f in items:
            _id_s = _as_str(_id)
            if _id_s == last_id:
                continue
            out.append((_id_s, _decode_fields(f)))
            remaining -= 1
            if remaining <= 0:
                break

        last_id = _as_str(items[-1][0])
        time.sleep(0.05)  # yield to Redis

    return out


# ─── Archive fallback (NDJSON.gz files) ──────────────────────────────────────

_DAY_RE = re.compile(r"^(\d{4}-\d{2}-\d{2})\.ndjson(?:\.gz)?$")


def _utc_day_from_ts_ms(ts_ms: int) -> str:
    return time.strftime("%Y-%m-%d", time.gmtime(int(ts_ms) / 1000))


def _list_archive_files(archive_dir: str, *, lookback_days: int) -> list[str]:
    d = (archive_dir or "").strip()
    if not d or not os.path.isdir(d):
        return []
    now = _now_ms()
    day_a = _utc_day_from_ts_ms(now - lookback_days * 86400 * 1000)
    day_b = _utc_day_from_ts_ms(now)
    names: list[str] = []
    for nm in os.listdir(d):
        m = _DAY_RE.match(nm)
        if not m:
            continue
        day = m.group(1)
        if day < day_a or day > day_b:
            continue
        names.append(os.path.join(d, nm))
    names.sort()
    return names


def _open_text_maybe_gzip(path: str):
    if str(path).endswith(".gz"):
        return gzip.open(path, "rt", encoding="utf-8", errors="replace")
    return open(path, encoding="utf-8", errors="replace")


def _read_archive_items(archive_dir: str, *, lookback_days: int, max_records: int) -> list[tuple[str, dict[str, Any]]]:
    files = _list_archive_files(archive_dir, lookback_days=lookback_days)
    items: list[tuple[str, dict[str, Any]]] = []
    limit = max_records if max_records > 0 else 10_000_000
    for fp in files:
        if len(items) >= limit:
            break
        try:
            with _open_text_maybe_gzip(fp) as f:
                for ln, line in enumerate(f, start=1):
                    if len(items) >= limit:
                        break
                    s = (line or "").strip()
                    if not s:
                        continue
                    try:
                        obj = json.loads(s)
                    except Exception:
                        continue
                    if isinstance(obj, dict):
                        msg_id = str(obj.get("stream_id") or f"file:{os.path.basename(fp)}:{ln}")
                        items.append((msg_id, obj))
        except Exception:
            continue
    return items


# ─── Decision parsing ────────────────────────────────────────────────────────

def parse_decision(fields: dict[str, Any]) -> dict[str, Any] | None:
    """Parse a decisions:final stream entry → full decision payload dict.

    Returns a dict with at minimum: sid, symbol, ts_ms, direction.
    The full payload (with of_confirm, rule, ml, etc.) is preserved as-is.
    """
    payload = _safe_json_loads(fields.get("payload"))
    if not isinstance(payload, dict):
        payload = fields

    sid = _as_str(payload.get("sid") or fields.get("sid") or "").strip()
    if not sid:
        return None

    symbol = _as_str(payload.get("symbol") or fields.get("symbol") or "").upper()
    ts_ms = _as_int(payload.get("ts_ms") or payload.get("decision_ts_ms") or fields.get("ts_ms"), 0)
    direction = _as_str(payload.get("direction") or fields.get("direction") or "").upper()

    if not symbol or ts_ms <= 0:
        return None

    # Normalize SID
    norm_sid = _normalize_sid(sid, symbol=symbol, ts_ms=ts_ms)

    # Build the decision record: keep full payload, ensure required top-level fields
    rec = dict(payload)
    rec["sid"] = norm_sid
    rec["symbol"] = symbol
    rec["ts_ms"] = ts_ms
    rec["decision_ts_ms"] = ts_ms
    rec["direction"] = direction

    # Ensure of_confirm is present (main consumer expectation)
    # If it lives in indicators.of_confirm, promote it
    if "of_confirm" not in rec:
        indicators = rec.get("indicators") if isinstance(rec.get("indicators"), dict) else {}
        ofc = indicators.get("of_confirm") if isinstance(indicators.get("of_confirm"), dict) else {}
        if ofc:
            rec["of_confirm"] = ofc

    # Also promote indicators-level fields needed by OFC contextual builder
    if "spread_bps" not in rec:
        inputs = rec.get("inputs") if isinstance(rec.get("inputs"), dict) else {}
        rec["spread_bps"] = _as_float(inputs.get("spread_bps") or 0.0)
    if "expected_slippage_bps" not in rec:
        inputs = rec.get("inputs") if isinstance(rec.get("inputs"), dict) else {}
        rec["expected_slippage_bps"] = _as_float(inputs.get("expected_slippage_bps") or 0.0)
    if "scenario_v4" not in rec:
        rule = rec.get("rule") if isinstance(rec.get("rule"), dict) else {}
        rec["scenario_v4"] = _as_str(rule.get("scenario_v4") or rule.get("scenario") or "")

    # of_score_final from rule.score
    if "of_score_final" not in rec:
        rule = rec.get("rule") if isinstance(rec.get("rule"), dict) else {}
        rec["of_score_final"] = _as_float(rule.get("score") or 0.0)

    return rec


# ─── Outcome parsing ─────────────────────────────────────────────────────────

def parse_outcome(fields: dict[str, Any]) -> dict[str, Any] | None:
    """Parse a trades:closed stream entry → outcome dict.

    Returns: {sid, symbol, pnl, risk_usd, pnl_bps_net, realized_slippage_bps, fill_delay_ms, exit_ts_ms, direction}
    """
    # Merge payload if present
    payload_obj = _safe_json_loads(fields.get("payload"))
    if isinstance(payload_obj, dict):
        merged = dict(payload_obj)
        for k, v in fields.items():
            if k not in merged:
                merged[k] = v
        fields = merged

    symbol = _as_str(fields.get("symbol") or fields.get("sym") or "").upper()
    raw_sid = _as_str(fields.get("sid") or fields.get("signal_id") or "").strip()
    meta = _safe_json_loads(fields.get("meta") or fields.get("metadata"))
    if isinstance(meta, dict):
        raw_sid = raw_sid or _as_str(meta.get("sid") or meta.get("signal_id") or "").strip()
        if not symbol:
            symbol = _as_str(meta.get("symbol") or "").upper()

    if not raw_sid or not symbol:
        return None

    exit_ts_ms = _as_int(fields.get("exit_ts_ms") or fields.get("ts_ms") or 0, 0)
    if exit_ts_ms <= 0 and isinstance(meta, dict):
        exit_ts_ms = _as_int(meta.get("exit_ts_ms") or meta.get("ts_ms") or 0, 0)

    norm_sid = _normalize_sid(raw_sid, symbol=symbol, ts_ms=exit_ts_ms or 0)
    pnl = _as_float(fields.get("pnl") or fields.get("pnl_net") or 0.0)
    risk_usd = _as_float(fields.get("risk_usd") or fields.get("risk_amount") or fields.get("one_r_money") or 0.0)
    if risk_usd <= 0.0 and isinstance(meta, dict):
        risk_usd = _as_float(meta.get("risk_usd") or meta.get("risk_amount") or meta.get("one_r_money") or 0.0)
    if pnl == 0.0 and isinstance(meta, dict):
        pnl = _as_float(meta.get("pnl") or meta.get("pnl_net") or 0.0)

    # Slippage / fill delay
    realized_slippage_bps = _as_float(fields.get("realized_slippage_bps") or fields.get("slippage_bps") or fields.get("realized_slip_worse_bps") or 0.0)
    fill_delay_ms = _as_int(fields.get("fill_delay_ms") or 0, 0)

    # PnL in bps (if available)
    pnl_bps_net = _as_float(fields.get("pnl_bps_net") or fields.get("net_pnl_bps") or 0.0)

    direction = _as_str(fields.get("direction") or fields.get("side") or "").upper()
    if isinstance(meta, dict) and not direction:
        direction = _as_str(meta.get("direction") or meta.get("side") or meta.get("pos_side") or "").upper()

    return {
        "sid": norm_sid,
        "symbol": symbol,
        "pnl": pnl,
        "risk_usd": risk_usd,
        "pnl_bps_net": pnl_bps_net,
        "realized_slippage_bps": realized_slippage_bps,
        "fill_delay_ms": fill_delay_ms,
        "exit_ts_ms": exit_ts_ms,
        "direction": direction,
    }


# ─── Atomic file write ───────────────────────────────────────────────────────

def _write_jsonl_atomic(path: str, rows: list[dict[str, Any]]) -> int:
    os.makedirs(os.path.dirname(os.path.abspath(path)) or ".", exist_ok=True)
    tmp = f"{path}.tmp"
    n = 0
    with open(tmp, "w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False, sort_keys=True, default=str) + "\n")
            n += 1
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)
    return n


def _write_json_atomic(path: str, obj: dict[str, Any]) -> None:
    os.makedirs(os.path.dirname(os.path.abspath(path)) or ".", exist_ok=True)
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2, sort_keys=True, default=str)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


# ─── Main build pipeline ─────────────────────────────────────────────────────

def build_confirm_train_v7(
    *,
    redis_url: str = "",
    decisions_stream: str = "decisions:final",
    closes_stream: str = "trades:closed",
    decisions_count: int = 200_000,
    closes_count: int = 200_000,
    decisions_archive_dir: str = "",
    closes_archive_dir: str = "",
    lookback_days: int = 7,
    out_decisions: str = "/var/lib/trade/training/latest_confirm_train_v7.ndjson",
    out_outcomes: str = "/var/lib/trade/training/latest_outcomes.ndjson",
    out_report: str = "/var/lib/trade/training/confirm_v7_report.json",
) -> dict[str, Any]:
    """Build confirm_train_v7 + outcomes NDJSON files."""

    start_ts = _now_ms()
    print(f"[confirm_v7] Starting build at {time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime())}")

    # ── 1. Read decisions ────────────────────────────────────────────────────
    decisions_raw: list[tuple[str, dict[str, Any]]] = []
    redis_decisions_n = 0
    archive_decisions_n = 0

    if redis_url:
        try:
            import redis as redis_lib  # type: ignore
            r = redis_lib.Redis.from_url(redis_url, decode_responses=False)
            decisions_raw = _xrevrange_recent(r, decisions_stream, count=decisions_count)
            redis_decisions_n = len(decisions_raw)
            print(f"[confirm_v7] Read {redis_decisions_n} entries from Redis stream '{decisions_stream}'")
        except Exception as e:
            print(f"[confirm_v7] ⚠️ Redis read failed for {decisions_stream}: {e}")

    # Archive fallback
    if decisions_archive_dir:
        archive_items = _read_archive_items(decisions_archive_dir, lookback_days=lookback_days, max_records=decisions_count)
        archive_decisions_n = len(archive_items)
        if archive_items:
            print(f"[confirm_v7] Archive fallback: {archive_decisions_n} decisions from {decisions_archive_dir}")
            decisions_raw.extend(archive_items)

    # ── 2. Read outcomes (trades:closed) ─────────────────────────────────────
    closes_raw: list[tuple[str, dict[str, Any]]] = []
    redis_closes_n = 0
    archive_closes_n = 0

    if redis_url:
        try:
            r2 = redis_lib.Redis.from_url(redis_url, decode_responses=False)  # type: ignore
            closes_raw = _xrevrange_recent(r2, closes_stream, count=closes_count)
            redis_closes_n = len(closes_raw)
            print(f"[confirm_v7] Read {redis_closes_n} entries from Redis stream '{closes_stream}'")
        except Exception as e:
            print(f"[confirm_v7] ⚠️ Redis read failed for {closes_stream}: {e}")

    if closes_archive_dir:
        archive_items = _read_archive_items(closes_archive_dir, lookback_days=lookback_days, max_records=closes_count)
        archive_closes_n = len(archive_items)
        if archive_items:
            print(f"[confirm_v7] Archive fallback: {archive_closes_n} outcomes from {closes_archive_dir}")
            closes_raw.extend(archive_items)

    # ── 3. Parse decisions ───────────────────────────────────────────────────
    decisions_by_sid: dict[str, dict[str, Any]] = {}
    dec_parse_ok = 0
    dec_parse_fail = 0
    for _id, fields in decisions_raw:
        rec = parse_decision(fields)
        if rec is None:
            dec_parse_fail += 1
            continue
        dec_parse_ok += 1
        sid = rec["sid"]
        # Keep latest by ts_ms (dedup by SID)
        existing = decisions_by_sid.get(sid)
        if existing is None or rec.get("ts_ms", 0) > existing.get("ts_ms", 0):
            decisions_by_sid[sid] = rec

    print(f"[confirm_v7] Parsed decisions: {dec_parse_ok} ok, {dec_parse_fail} failed, {len(decisions_by_sid)} unique SIDs")

    # ── 4. Parse outcomes ────────────────────────────────────────────────────
    outcomes_by_sid: dict[str, dict[str, Any]] = {}
    out_parse_ok = 0
    out_parse_fail = 0
    for _id, fields in closes_raw:
        rec = parse_outcome(fields)
        if rec is None:
            out_parse_fail += 1
            continue
        out_parse_ok += 1
        sid = rec["sid"]
        existing = outcomes_by_sid.get(sid)
        if existing is None or rec.get("exit_ts_ms", 0) > existing.get("exit_ts_ms", 0):
            outcomes_by_sid[sid] = rec

    print(f"[confirm_v7] Parsed outcomes: {out_parse_ok} ok, {out_parse_fail} failed, {len(outcomes_by_sid)} unique SIDs")

    # ── 5. Join by SID ───────────────────────────────────────────────────────
    joined_sids = set(decisions_by_sid.keys()) & set(outcomes_by_sid.keys())
    print(f"[confirm_v7] SID join: {len(joined_sids)} matched (decisions={len(decisions_by_sid)}, outcomes={len(outcomes_by_sid)})")

    # Sort by ts_ms for deterministic output
    joined_decisions: list[dict[str, Any]] = []
    joined_outcomes: list[dict[str, Any]] = []
    for sid in sorted(joined_sids, key=lambda s: decisions_by_sid[s].get("ts_ms", 0)):
        joined_decisions.append(decisions_by_sid[sid])
        joined_outcomes.append(outcomes_by_sid[sid])

    # ── 6. Write output files ────────────────────────────────────────────────
    n_dec = _write_jsonl_atomic(out_decisions, joined_decisions)
    n_out = _write_jsonl_atomic(out_outcomes, joined_outcomes)

    elapsed_ms = _now_ms() - start_ts
    report = {
        "ts_ms": _now_ms(),
        "elapsed_ms": elapsed_ms,
        "redis_decisions_read": redis_decisions_n,
        "archive_decisions_read": archive_decisions_n,
        "redis_closes_read": redis_closes_n,
        "archive_closes_read": archive_closes_n,
        "decisions_parsed_ok": dec_parse_ok,
        "decisions_parsed_fail": dec_parse_fail,
        "decisions_unique_sids": len(decisions_by_sid),
        "outcomes_parsed_ok": out_parse_ok,
        "outcomes_parsed_fail": out_parse_fail,
        "outcomes_unique_sids": len(outcomes_by_sid),
        "joined_sids": len(joined_sids),
        "decisions_written": n_dec,
        "outcomes_written": n_out,
        "out_decisions": out_decisions,
        "out_outcomes": out_outcomes,
    }
    _write_json_atomic(out_report, report)

    print(f"[confirm_v7] Done in {elapsed_ms}ms: {n_dec} decisions + {n_out} outcomes written")
    print(f"[confirm_v7] Files: {out_decisions}, {out_outcomes}")
    return report


# ─── CLI ──────────────────────────────────────────────────────────────────────

def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Build confirm_train_v7 + outcomes NDJSON from Redis streams")
    ap.add_argument("--redis_url", default=os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0"))
    ap.add_argument("--decisions_stream", default=os.getenv("DECISIONS_FINAL_STREAM", "decisions:final"))
    ap.add_argument("--closes_stream", default=os.getenv("TRADES_CLOSED_STREAM", "trades:closed"))
    ap.add_argument("--decisions_count", type=int, default=int(os.getenv("CONFIRM_V7_DECISIONS_COUNT", "200000")))
    ap.add_argument("--closes_count", type=int, default=int(os.getenv("CONFIRM_V7_CLOSES_COUNT", "200000")))
    ap.add_argument("--decisions_archive_dir", default=os.getenv("DECISIONS_ARCHIVE_DIR", "/var/lib/trade/archives/decisions"))
    ap.add_argument("--closes_archive_dir", default=os.getenv("TRADES_CLOSED_ARCHIVE_DIR", os.getenv("CLOSES_ARCHIVE_DIR", "/var/lib/trade/archives/trades")))
    ap.add_argument("--lookback_days", type=int, default=int(os.getenv("CONFIRM_V7_LOOKBACK_DAYS", "7")))
    ap.add_argument("--out_decisions", default=os.getenv("CONFIRM_V7_OUT_DECISIONS", "/var/lib/trade/training/latest_confirm_train_v7.ndjson"))
    ap.add_argument("--out_outcomes", default=os.getenv("CONFIRM_V7_OUT_OUTCOMES", "/var/lib/trade/training/latest_outcomes.ndjson"))
    ap.add_argument("--out_report", default=os.getenv("CONFIRM_V7_OUT_REPORT", "/var/lib/trade/training/confirm_v7_report.json"))
    args = ap.parse_args(argv)

    try:
        report = build_confirm_train_v7(
            redis_url=args.redis_url,
            decisions_stream=args.decisions_stream,
            closes_stream=args.closes_stream,
            decisions_count=args.decisions_count,
            closes_count=args.closes_count,
            decisions_archive_dir=args.decisions_archive_dir,
            closes_archive_dir=args.closes_archive_dir,
            lookback_days=args.lookback_days,
            out_decisions=args.out_decisions,
            out_outcomes=args.out_outcomes,
            out_report=args.out_report,
        )
        if report.get("joined_sids", 0) == 0:
            print("[confirm_v7] ⚠️ No joined records found. Downstream pipelines will skip.")
        return 0
    except Exception as e:
        print(f"[confirm_v7] ❌ Fatal error: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
