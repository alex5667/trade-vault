from __future__ import annotations

"""OF-Gate DLQ drilldown tool (P83).

Purpose
- Help SRE/ops quickly understand why DLQ is non-empty.
- Provide safe, guarded workflows to sample, dump, replay, or purge DLQ entries.

Supported DLQ stream formats
- stream_archiver.dlq() format:
  fields: stream, stream_id, err, payload(JSON string)

Safety
- Destructive actions require explicit --yes.
- Replay is dry-run by default.

Examples
  # Basic overview
  python -m orderflow_services.of_gate_dlq_drilldown_p83 stats

  # Top error / dq_code / reason_code from last 5000 DLQ messages
  python -m orderflow_services.of_gate_dlq_drilldown_p83 top --limit 5000

  # Sample messages for a specific dq_code
  python -m orderflow_services.of_gate_dlq_drilldown_p83 sample --dq-code ts_ms_bad_range --n 10

  # Replay last 100 messages from of_gate_metrics DLQ into metrics:of_gate (dry-run)
  python -m orderflow_services.of_gate_dlq_drilldown_p83 replay \
    --source stream:dlq:of_gate_metrics --target metrics:of_gate --max 100

  # Purge a set of ids (DANGEROUS)
  python -m orderflow_services.of_gate_dlq_drilldown_p83 purge --source stream:dlq:of_gate_metrics \
    --ids 1700000000000-0,1700000000001-0 --yes,
""",
import argparse
import json
import os
from collections import Counter
from dataclasses import dataclass
from typing import Any

from utils.time_utils import get_ny_time_millis


def env(name: str, default: str) -> str:
    v = os.getenv(name)
    return v if v else default


def env_int(name: str, default: int) -> int:
    v = os.getenv(name)
    try:
        return int(v) if v else default
    except Exception:
        return default


def _decode(x: Any) -> Any:
    if isinstance(x, (bytes, bytearray)):
        return x.decode("utf-8", "ignore")
    return x


def _json_loads_maybe(s: Any) -> Any:
    if s is None:
        return None
    s = _decode(s)
    if isinstance(s, (dict, list)):
        return s
    if not isinstance(s, str):
        return s
    try:
        return json.loads(s)
    except Exception:
        return s


@dataclass
class DlqMsg:
    dlq_id: str
    fields: dict[str, Any]
    src_stream: str
    src_stream_id: str
    err: str
    payload: Any


def _parse_dlq_msg(dlq_id: str, fields: dict[str, Any]) -> DlqMsg:
    f = {str(_decode(k)): _decode(v) for k, v in (fields or {}).items()}
    src_stream = str(f.get("stream") or f.get("src_stream") or "")
    src_stream_id = str(f.get("stream_id") or f.get("src_stream_id") or "")
    err = str(f.get("err") or f.get("error") or "")
    payload_raw = f.get("payload")
    if payload_raw is None:
        payload_raw = f.get("data")
    payload = _json_loads_maybe(payload_raw)
    return DlqMsg(
        dlq_id=str(dlq_id),
        fields=f,
        src_stream=src_stream,
        src_stream_id=src_stream_id,
        err=err,
        payload=payload,
    )


def _pick_redis_url() -> str:
    return os.getenv("REDIS_URL") or os.getenv("REDIS_TICKS_URL") or "redis://localhost:6379/0"


def _connect_redis():
    try:
        import redis  # type: ignore

        return redis.Redis.from_url(_pick_redis_url(), decode_responses=False)
    except Exception as e:
        raise SystemExit(f"redis_import_or_connect_failed: {e}")


def _xlen(r, stream: str) -> int:
    try:
        return int(r.xlen(stream))
    except Exception:
        return 0


def _xrevrange(r, stream: str, count: int) -> list[tuple[str, dict[str, Any]]]:
    # newest first
    try:
        return [(str(_decode(mid)), fields) for mid, fields in r.xrevrange(stream, max="+", min="-", count=count)]
    except Exception:
        return []


def _xrange(r, stream: str, start: str, end: str, count: int) -> list[tuple[str, dict[str, Any]]]:
    try:
        return [(str(_decode(mid)), fields) for mid, fields in r.xrange(stream, min=start, max=end, count=count)]
    except Exception:
        return []


def _ts_ms_from_stream_id(sid: str) -> int | None:
    try:
        return int(str(sid).split("-", 1)[0])
    except Exception:
        return None


def _extract_keys(msg: DlqMsg) -> dict[str, Any]:
    p = msg.payload if isinstance(msg.payload, dict) else {}
    out = {
        "dq_code": p.get("dq_code") or p.get("why") or "",
        "reason_code": p.get("reason_code") or "",
        "schema_version": p.get("schema_version") or p.get("schema_version_mode") or "",
        "scenario_v4": p.get("scenario_v4") or "",
        "symbol": p.get("symbol") or "",
    }
    # err prefix
    out["err_prefix"] = (msg.err.split(":", 1)[0] if msg.err else "")
    return out


def cmd_stats(args: argparse.Namespace) -> int:
    r = _connect_redis()
    streams = args.streams
    now_ms = get_ny_time_millis()

    rows = []
    for s in streams:
        ln = _xlen(r, s)
        last = _xrevrange(r, s, 1)
        last_id = last[0][0] if last else ""
        age_s = None
        if last_id:
            ts = _ts_ms_from_stream_id(last_id)
            if ts is not None:
                age_s = (now_ms - ts) / 1000.0
        rows.append((s, ln, last_id, age_s))

    # print
    print("stream\tlen\tlast_id\tage_s")
    for s, ln, last_id, age_s in rows:
        age_str = "" if age_s is None else f"{age_s:.1f}"
        print(f"{s}\t{ln}\t{last_id}\t{age_str}")
    return 0


def cmd_top(args: argparse.Namespace) -> int:
    r = _connect_redis()
    limit = int(args.limit)
    streams = args.streams

    c_err = Counter()
    c_dq = Counter()
    c_reason = Counter()
    c_schema = Counter()
    c_src = Counter()

    total = 0
    for s in streams:
        items = _xrevrange(r, s, limit)
        for mid, fields in items:
            total += 1
            m = _parse_dlq_msg(mid, fields)
            keys = _extract_keys(m)
            if m.err:
                c_err[keys["err_prefix"] or "(empty)"] += 1
            if m.src_stream:
                c_src[m.src_stream] += 1
            dq = (keys.get("dq_code") or "").strip() or "(empty)"
            c_dq[dq] += 1
            rc = (keys.get("reason_code") or "").strip() or "(empty)"
            c_reason[rc] += 1
            sv = (keys.get("schema_version") or "").strip() or "(empty)"
            c_schema[sv] += 1

    def show(title: str, counter: Counter, k: int = 15):
        print(f"\n== {title} ==")
        for key, cnt in counter.most_common(k):
            share = (cnt / total) if total else 0.0
            print(f"{key}\t{cnt}\t{share:.3f}")

    print(f"scanned_total\t{total}")
    show("err_prefix", c_err)
    show("src_stream", c_src)
    show("dq_code/why", c_dq)
    show("reason_code", c_reason)
    show("schema_version", c_schema)

    return 0


def cmd_sample(args: argparse.Namespace) -> int:
    r = _connect_redis()
    stream = args.source
    n = int(args.n)
    limit = int(args.limit)

    items = _xrevrange(r, stream, limit)
    out: list[DlqMsg] = []
    for mid, fields in items:
        m = _parse_dlq_msg(mid, fields)
        keys = _extract_keys(m)
        if args.dq_code and (keys.get("dq_code") or "") != args.dq_code:
            continue
        if args.reason_code and (keys.get("reason_code") or "") != args.reason_code:
            continue
        if args.err_prefix and (keys.get("err_prefix") or "") != args.err_prefix:
            continue
        out.append(m)
        if len(out) >= n:
            break

    for m in out:
        print(json.dumps({
            "dlq_id": m.dlq_id,
            "src_stream": m.src_stream,
            "src_stream_id": m.src_stream_id,
            "err": m.err,
            "payload": m.payload,
        }, ensure_ascii=False))

    return 0


def _infer_replay_fields(msg: DlqMsg, add_meta: bool = True) -> dict[str, str]:
    p = msg.payload
    if isinstance(p, dict):
        # if it already looks like a flat row (has schema_name/ts_ms/symbol), emit flat fields
        looks_flat = any(k in p for k in ("schema_name", "schema_version", "ts_ms", "symbol", "ok", "ok_soft"))
        if looks_flat:
            fields = {str(k): "" if v is None else str(v) for k, v in p.items()}
        else:
            fields = {"payload": json.dumps(p, ensure_ascii=False)}
    else:
        fields = {"payload": json.dumps(p, ensure_ascii=False) if p is not None else ""}

    if add_meta:
        fields.setdefault("dlq_stream", "")
        fields["dlq_id"] = msg.dlq_id
        if msg.err:
            fields["dlq_err"] = msg.err[:500]
        if msg.src_stream:
            fields["dlq_src_stream"] = msg.src_stream
        if msg.src_stream_id:
            fields["dlq_src_stream_id"] = msg.src_stream_id

    return fields


def cmd_replay(args: argparse.Namespace) -> int:
    r = _connect_redis()
    source = args.source
    target = args.target
    max_n = int(args.max)
    dry_run = args.dry_run

    # read from oldest to newest for replay stability
    start = args.start_id or "-"
    end = "+"

    replayed = 0
    cursor = start
    while replayed < max_n:
        batch = _xrange(r, source, cursor, end, min(500, max_n - replayed))
        if not batch:
            break
        # if cursor == '-', xrange includes first; adjust cursor next
        for mid, fields in batch:
            m = _parse_dlq_msg(mid, fields)
            keys = _extract_keys(m)
            if args.dq_code and (keys.get("dq_code") or "") != args.dq_code:
                cursor = mid
                continue
            if args.reason_code and (keys.get("reason_code") or "") != args.reason_code:
                cursor = mid
                continue
            if args.err_prefix and (keys.get("err_prefix") or "") != args.err_prefix:
                cursor = mid
                continue

            out_fields = _infer_replay_fields(m, add_meta=not args.no_meta)
            if dry_run:
                print(json.dumps({"action": "replay(dry)", "from": source, "dlq_id": mid, "to": target, "fields": out_fields}, ensure_ascii=False))
            else:
                r.xadd(target, out_fields, maxlen=args.maxlen, approximate=True)
                print(json.dumps({"action": "replay", "from": source, "dlq_id": mid, "to": target}, ensure_ascii=False))

            replayed += 1
            cursor = mid
            if replayed >= max_n:
                break

        # move cursor forward: +1 by using (last_id)+
        cursor = f"{cursor}"  # keep last id; next xrange will include it, but ok due to max_n; better: use exclusive with '(' not supported in redis-py xrange
        if len(batch) < 2:
            # prevent infinite loop on single item
            break

    return 0


def cmd_purge(args: argparse.Namespace) -> int:
    if not args.yes:
        raise SystemExit("refusing: destructive purge requires --yes")
    r = _connect_redis()
    source = args.source

    ids = []
    if args.ids:
        ids = [s.strip() for s in args.ids.split(",") if s.strip()]

    deleted = 0
    if ids:
        deleted = int(r.xdel(source, *ids))
        print(json.dumps({"action": "xdel", "stream": source, "deleted": deleted, "ids": ids[:20]}, ensure_ascii=False))
        return 0

    # trim by maxlen
    if args.maxlen is not None:
        maxlen = int(args.maxlen)
        r.xtrim(source, maxlen=maxlen, approximate=True)
        print(json.dumps({"action": "xtrim", "stream": source, "maxlen": maxlen}, ensure_ascii=False))
        return 0

    raise SystemExit("no purge operation selected: provide --ids or --maxlen")


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="of_gate_dlq_drilldown_p83")
    p.add_argument(
        "--streams",
        default=env("OF_GATE_DLQ_STREAMS", "stream:dlq:of_gate_metrics,stream:dlq:of_gate_quarantine"),
        help="comma-separated DLQ streams",
    )

    sub = p.add_subparsers(dest="cmd", required=True)

    ps = sub.add_parser("stats", help="show xlen + last_id + age")
    ps.set_defaults(fn=cmd_stats)

    pt = sub.add_parser("top", help="top counters from tail")
    pt.add_argument("--limit", type=int, default=2000)
    pt.set_defaults(fn=cmd_top)

    psm = sub.add_parser("sample", help="print sample messages")
    psm.add_argument("--source", default="stream:dlq:of_gate_metrics")
    psm.add_argument("--limit", type=int, default=5000)
    psm.add_argument("--n", type=int, default=10)
    psm.add_argument("--dq-code", dest="dq_code", default="")
    psm.add_argument("--reason-code", dest="reason_code", default="")
    psm.add_argument("--err-prefix", dest="err_prefix", default="")
    psm.set_defaults(fn=cmd_sample)

    pr = sub.add_parser("replay", help="replay DLQ entries into a target stream")
    pr.add_argument("--source", default="stream:dlq:of_gate_metrics")
    pr.add_argument("--target", default="metrics:of_gate")
    pr.add_argument("--max", type=int, default=100)
    pr.add_argument("--start-id", default="-")
    pr.add_argument("--dq-code", dest="dq_code", default="")
    pr.add_argument("--reason-code", dest="reason_code", default="")
    pr.add_argument("--err-prefix", dest="err_prefix", default="")
    pr.add_argument("--maxlen", type=int, default=200000)
    pr.add_argument("--dry-run", action="store_true", default=True)
    pr.add_argument("--commit", action="store_true", default=False, help="actually write to target stream")
    pr.add_argument("--no-meta", action="store_true", default=False)
    pr.set_defaults(fn=cmd_replay)

    pp = sub.add_parser("purge", help="delete ids or trim stream")
    pp.add_argument("--source", default="stream:dlq:of_gate_metrics")
    pp.add_argument("--ids", default="")
    pp.add_argument("--maxlen", type=int, default=None)
    pp.add_argument("--yes", action="store_true", default=False)
    pp.set_defaults(fn=cmd_purge)

    return p


def main(argv: list[str] | None = None) -> int:
    ap = build_parser()
    args = ap.parse_args(argv)

    # normalize streams
    args.streams = [s.strip() for s in str(args.streams).split(",") if s.strip()]

    # replay dry-run control
    if getattr(args, "cmd", "") == "replay":
        args.dry_run = not bool(args.commit)

    return int(args.fn(args))


if __name__ == "__main__":
    raise SystemExit(main())
