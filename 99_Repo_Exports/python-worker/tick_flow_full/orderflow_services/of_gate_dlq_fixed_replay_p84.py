from __future__ import annotations
"""P84: OF-Gate DLQ fixed-then-replay pipeline (operator tool).

What it does
- Reads DLQ streams produced by stream_archiver.dlq() (fields: stream, stream_id, err, payload).
- Parses payload JSON.
- For parse_error DLQ entries, attempts to recover original payload from stored "fields".
- Applies safe, conservative fixes (schema markers, schema_version coercion, ts_ms normalization,
  missing_legs default/sanitization).
- Validates using the canonical OF-gate contract validator.
- Optionally replays fixed rows back to their original stream.

Safety
- Default is DRY RUN (no writes).
- Replay requires --commit.
- Deleting from DLQ requires BOTH --commit and --delete-after-replay.
- For automation, you should restrict fixes via --allow-fixes (default: allow all).

Usage
  # Triage only (summary, top causes, hints)
  REDIS_URL=... python -m orderflow_services.of_gate_dlq_fixed_replay_p84 triage --limit 5000

  # Replay fixable (dry-run)
  python -m orderflow_services.of_gate_dlq_fixed_replay_p84 replay --source stream:dlq:of_gate_metrics --max 500

  # Replay fixable (commit) and delete from DLQ after success
  python -m orderflow_services.of_gate_dlq_fixed_replay_p84 replay --commit --delete-after-replay --max 500

  # Automation-style: triage + safe replay (restricted fixes)
  python -m orderflow_services.of_gate_dlq_fixed_replay_p84 auto --commit --delete-after-replay \
    --allow-fixes add_schema_name,add_schema_version,coerce_schema_version_int,normalize_ts_ms,ts_from_stream_id,default_missing_legs_empty,coerce_missing_legs_to_json,stringify_missing_legs \
    --require-fix,
""",
from utils.time_utils import get_ny_time_millis

import argparse
import json
import os
import time
from collections import Counter
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

from orderflow_services.of_gate_dlq_fix_hints_registry_p84 import FixHint, hint_for


def env(name: str, default: str) -> str:
    v = os.getenv(name)
    return v if v else default


def env_int(name: str, default: int) -> int:
    v = os.getenv(name)
    try:
        return int(v) if v else default
    except Exception:
        return default


def env_bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None or v == "":
        return default
    return str(v).strip().lower() in ("1", "true", "yes", "y", "on")


def _decode(x: Any) -> Any:
    if isinstance(x, (bytes, bytearray)):
        return x.decode("utf-8", "ignore")
    return x


def _json_loads_maybe(s: Any) -> Any:
    s = _decode(s)
    if s is None:
        return None
    if isinstance(s, (dict, list)):
        return s
    if not isinstance(s, str):
        return s
    try:
        return json.loads(s)
    except Exception:
        return s


def _connect_redis():
    import redis  # type: ignore

    url = os.getenv("REDIS_URL") or os.getenv("REDIS_TICKS_URL") or "redis://localhost:6379/0"
    return redis.Redis.from_url(url, decode_responses=False)


def _ts_ms_from_stream_id(stream_id: str) -> int:
    try:
        return int(str(stream_id).split("-", 1)[0])
    except Exception:
        return 0


def _err_prefix(err: str) -> str:
    s = (err or "").strip()
    if not s:
        return "(empty)"
    p = s.split(":", 1)[0].strip()
    if not p:
        p = s.split(" ", 1)[0].strip()
    return p[:64] if p else "(empty)"


# Contract imports (canonical)
def _load_contract():
    # Prefer canonical location
    try:
        from services.orderflow.of_gate_metrics_contract import (  # type: ignore
            enrich_schema_fields,
            validate_of_gate_row,
            derive_reason_code,
        )

        return enrich_schema_fields, validate_of_gate_row, derive_reason_code
    except Exception:
        pass
    try:
        from tick_flow_full.common.of_gate_metrics_contract import (  # type: ignore
            enrich_schema_fields,
            validate_of_gate_row,
            derive_reason_code,
        )

        return enrich_schema_fields, validate_of_gate_row, derive_reason_code
    except Exception:
        pass
    from ok_rate_logic.of_gate_metrics_contract import (  # type: ignore
        enrich_schema_fields,
        validate_of_gate_row,
        derive_reason_code,
    )

    return enrich_schema_fields, validate_of_gate_row, derive_reason_code


enrich_schema_fields, validate_of_gate_row, derive_reason_code = _load_contract()


@dataclass
class DLQEntry:
    dlq_id: str
    src_stream: str
    src_stream_id: str
    err: str
    payload: Any


def _parse_dlq_entry(dlq_id: Any, fields: Dict[Any, Any]) -> Optional[DLQEntry]:
    dlq_id_s = str(_decode(dlq_id))
    f = {str(_decode(k)): _decode(v) for k, v in (fields or {}).items()}

    src_stream = str(_decode(f.get("stream") or f.get("src_stream") or ""))
    src_stream_id = str(_decode(f.get("stream_id") or f.get("src_stream_id") or ""))
    err = str(_decode(f.get("err") or f.get("error") or ""))
    payload_raw = f.get("payload")
    payload = _json_loads_maybe(payload_raw)
    return DLQEntry(dlq_id_s, src_stream, src_stream_id, err, payload)


def _coerce_int01(v: Any) -> Optional[int]:
    if v is None:
        return None
    if isinstance(v, bool):
        return 1 if v else 0
    if isinstance(v, (int, float)):
        iv = int(v)
        return 1 if iv != 0 else 0
    s = str(v).strip()
    if s in ("0", "1"):
        return int(s)
    if s.lower() in ("true", "yes", "y", "on"):
        return 1
    if s.lower() in ("false", "no", "n", "off"):
        return 0
    return None


def _normalize_ts_ms(v: Any) -> Optional[int]:
    try:
        if v is None:
            return None
        x = int(float(v))
    except Exception:
        return None
    # Heuristics: sec/us/ns -> ms
    if x < 100_000_000_000:  # <1e11 => seconds
        return x * 1000
    if x > 10_000_000_000_000_000:  # >1e16 => ns
        return x // 1_000_000
    if x > 10_000_000_000_000:  # >1e13 => us
        return x // 1000
    return x


def _parse_stream_payload_from_fields(fields: Dict[str, Any]) -> Dict[str, Any]:
    """Best-effort parse of original stream payload from message fields.""",
    raw = fields.get("data")
    if raw is None:
        raw = fields.get("payload")
    if raw is None:
        return dict(fields)
    raw = _decode(raw)
    if isinstance(raw, str):
        try:
            j = json.loads(raw)
            return j if isinstance(j, dict) else {"_raw_payload": j}
        except Exception:
            # fallback to raw fields
            return dict(fields)
    if isinstance(raw, dict):
        return raw
    return dict(fields)


def _payload_for_fix(entry: DLQEntry) -> Dict[str, Any]:
    """Return a dict payload suitable for fix+validate.

    Handles parse_error DLQ entries where payload is {"fields": {...}}.
    """,
    p = entry.payload
    if isinstance(p, dict) and "fields" in p and isinstance(p.get("fields"), dict):
        try:
            return _parse_stream_payload_from_fields(p["fields"])  # type: ignore[index]
        except Exception:
            return {"_raw_payload": p}
    if isinstance(p, dict):
        return p
    return {"_raw_payload": p}


def _coerce_schema_version(v: Any) -> Optional[int]:
    if v is None:
        return None
    try:
        return int(v)
    except Exception:
        try:
            return int(float(str(v).strip()))
        except Exception:
            return None


def _safe_fix_payload(payload: Dict[str, Any], stream_id_hint: str) -> Tuple[Dict[str, Any], List[str]]:
    """Apply conservative fixes. Returns (new_payload, fixes_applied).""",
    p = dict(payload)
    fixes: List[str] = []

    # Ensure schema markers exist (additive)
    if not p.get("schema_name"):
        p["schema_name"] = "of_gate_metrics"
        fixes.append("add_schema_name")

    sv0 = p.get("schema_version")
    sv1 = _coerce_schema_version(sv0)
    if sv1 is None:
        p["schema_version"] = 2
        fixes.append("add_schema_version")
    else:
        if sv1 != sv0:
            p["schema_version"] = sv1
            fixes.append("coerce_schema_version_int")

    # ts_ms
    ts0 = p.get("ts_ms")
    ts1 = _normalize_ts_ms(ts0)
    if ts1 is None or ts1 <= 0:
        sid_ms = _ts_ms_from_stream_id(stream_id_hint)
        if sid_ms > 0:
            p["ts_ms"] = sid_ms
            fixes.append("ts_from_stream_id")
    else:
        if ts1 != ts0:
            p["ts_ms"] = ts1
            fixes.append("normalize_ts_ms")

    # ok / ok_soft (sanitize only if present)
    ok = _coerce_int01(p.get("ok"))
    if ok is not None:
        p["ok"] = ok
    ok_soft = _coerce_int01(p.get("ok_soft"))
    if ok_soft is not None:
        p["ok_soft"] = ok_soft

    # missing_legs: allow dict/list or valid JSON string
    if "missing_legs" not in p:
        p["missing_legs"] = "[]"
        fixes.append("default_missing_legs_empty")
    else:
        ml = p.get("missing_legs")
        if isinstance(ml, (dict, list)):
            # ok
            pass
        elif isinstance(ml, str):
            try:
                json.loads(ml)
            except Exception:
                p["missing_legs"] = json.dumps([ml], ensure_ascii=False)
                fixes.append("coerce_missing_legs_to_json")
        else:
            p["missing_legs"] = json.dumps([str(ml)], ensure_ascii=False)
            fixes.append("stringify_missing_legs")

    # Let contract normalizer do low-card normalization
    try:
        p = enrich_schema_fields(p)
    except Exception:
        # Keep partial
        pass

    return p, fixes


def _validate(payload: Dict[str, Any]) -> Tuple[bool, str]:
    try:
        ok, code = validate_of_gate_row(payload)
        return bool(ok), str(code or "")
    except Exception as e:
        return False, f"validate_exc:{type(e).__name__}"


def _iter_dlq(r, dlq_stream: str, start: str, count: int) -> Iterable[DLQEntry]:
    # newest first
    try:
        rows = r.xrevrange(dlq_stream, max=start, min="-", count=count)
    except TypeError:
        rows = r.xrevrange(dlq_stream, start, "-", count=count)
    for dlq_id, fields in rows:
        e = _parse_dlq_entry(dlq_id, fields)
        if e:
            yield e


def _notify_stream_name() -> str:
    return (
        os.getenv("TELEGRAM_NOTIFY_STREAM")
        or os.getenv("NOTIFY_TELEGRAM_STREAM")
        or os.getenv("CRYPTO_NOTIFY_STREAM")
        or "notify:telegram"
    )


def _xadd_best_effort(r, stream: str, fields: Dict[str, Any], maxlen: int = 200000) -> None:
    payload = {
        k: (v if isinstance(v, (str, bytes, bytearray, int, float)) else json.dumps(v, ensure_ascii=False))
        for k, v in fields.items()
    }
    r.xadd(stream, payload, maxlen=maxlen, approximate=True)


def _parse_allow_fixes(s: str) -> Set[str]:
    items = [x.strip() for x in (s or "").split(",")]
    return {x for x in items if x}


def _fixes_allowed(fixes: List[str], allow: Set[str]) -> bool:
    if not allow:
        return True
    return all(f in allow for f in fixes)


def triage(args: argparse.Namespace) -> int:
    r = _connect_redis()
    streams = [
        s.strip()
        for s in (
            args.streams
            or env("OF_GATE_DLQ_STREAMS", "stream:dlq:of_gate_metrics,stream:dlq:of_gate_quarantine")
        ).split(",")
        if s.strip()
    ]
    limit = int(args.limit)

    total = 0
    fixable = 0
    by_dq: Dict[str, int] = {}
    by_hint: Dict[str, int] = {}
    by_stream: Dict[str, int] = {}
    by_err: Dict[str, int] = {}

    for s in streams:
        by_stream.setdefault(s, 0)
        for e in _iter_dlq(r, s, "+", limit):
            total += 1
            by_stream[s] += 1
            by_err[_err_prefix(e.err)] = by_err.get(_err_prefix(e.err), 0) + 1
            base = _payload_for_fix(e)
            fixed, _fixes = _safe_fix_payload(base, e.src_stream_id or e.dlq_id)
            ok, dq_code = _validate(fixed)
            dq_code = dq_code or "dq_unknown"
            by_dq[dq_code] = by_dq.get(dq_code, 0) + (0 if ok else 1)
            if ok:
                fixable += 1
                by_hint.setdefault("fixable", 0)
                by_hint["fixable"] += 1
            else:
                h = hint_for(dq_code, e.err)
                by_hint[h.hint_code] = by_hint.get(h.hint_code, 0) + 1

    lines = []
    lines.append(
        f"OF-Gate DLQ triage (P84): total={total} fixable={fixable} fixable_share={(fixable/total if total else 0):.3f}"
    )
    for s, n in sorted(by_stream.items(), key=lambda x: -x[1]):
        lines.append(f"  {s}: {n}")
    lines.append("Top err_prefix:")
    for k, v in sorted(by_err.items(), key=lambda x: -x[1])[:10]:
        lines.append(f"  {k}: {v}")
    lines.append("Top dq_code (non-fixable counts):")
    for k, v in sorted(by_dq.items(), key=lambda x: -x[1])[:10]:
        lines.append(f"  {k}: {v}")
    lines.append("Top hint_code:")
    for k, v in sorted(by_hint.items(), key=lambda x: -x[1])[:10]:
        lines.append(f"  {k}: {v}")

    out = "\n".join(lines)
    print(out)

    if args.notify:
        try:
            _xadd_best_effort(
                r,
                _notify_stream_name(),
                {
                    "ts_ms": get_ny_time_millis(),
                    "source": "of_gate_dlq_fixed_replay_p84",
                    "message": out[:3500],
                    "severity": "warn" if total > 0 else "info",
                }
            )
        except Exception:
            pass

    return 0


def replay(args: argparse.Namespace) -> int:
    r = _connect_redis()
    source = args.source or env("OF_GATE_DLQ_SOURCE", "stream:dlq:of_gate_metrics")
    max_n = int(args.max)
    commit = bool(args.commit)
    delete_after = bool(args.delete_after_replay)
    target_override = args.target

    allow_fixes = _parse_allow_fixes(args.allow_fixes or env("OF_GATE_DLQ_REPLAY_ALLOW_FIXES", ""))
    require_fix = bool(args.require_fix) or env_bool("OF_GATE_DLQ_REPLAY_REQUIRE_FIX", False)

    out_stream_unfixable = env("OF_GATE_DLQ_UNFIXABLE_STREAM", "stream:dlq:of_gate_unfixable")
    move_unfixable = env_bool("OF_GATE_DLQ_MOVE_UNFIXABLE", False)

    seen = 0
    ok_replayed = 0
    skipped_disallowed = 0
    skipped_no_fix = 0
    still_bad = 0

    for e in _iter_dlq(r, source, "+", max_n):
        seen += 1
        base = _payload_for_fix(e)
        fixed, fixes = _safe_fix_payload(base, e.src_stream_id or e.dlq_id)
        ok, dq_code = _validate(fixed)

        if ok:
            if require_fix and not fixes:
                skipped_no_fix += 1
                continue
            if not _fixes_allowed(fixes, allow_fixes):
                skipped_disallowed += 1
                continue

            ok_replayed += 1
            tgt = target_override or (e.src_stream or "")
            if not tgt:
                tgt = "metrics:of_gate"

            if commit:
                fields = dict(fixed)
                fields["replay"] = 1
                fields["replay_src_dlq_stream"] = source
                fields["replay_src_dlq_id"] = e.dlq_id
                fields["replay_src_stream_id"] = e.src_stream_id
                fields["replay_src_err_prefix"] = _err_prefix(e.err)
                fields["replay_fix_tags"] = ",".join(fixes)[:500]

                try:
                    r.xadd(
                        tgt,
                        {
                            k: json.dumps(v, ensure_ascii=False)
                            if isinstance(v, (dict, list))
                            else str(v)
                            for k, v in fields.items()
                        }, maxlen=2000000,
                        approximate=True,
                    )
                except Exception as ex:
                    still_bad += 1
                    print(f"replay write failed: dlq_id={e.dlq_id} err={type(ex).__name__}:{ex}")
                    continue

                if delete_after:
                    try:
                        r.xdel(source, e.dlq_id)
                    except Exception:
                        pass
            else:
                print(f"DRYRUN replay OK: dlq_id={e.dlq_id} -> {tgt} fixes={fixes}")
        else:
            still_bad += 1
            h: FixHint = hint_for(dq_code, e.err)
            if move_unfixable and commit:
                try:
                    r.xadd(
                        out_stream_unfixable,
                        {
                            "src_dlq_stream": source,
                            "src_dlq_id": e.dlq_id,
                            "src_stream": e.src_stream,
                            "src_stream_id": e.src_stream_id,
                            "dq_code": dq_code,
                            "hint_code": h.hint_code,
                            "err": (e.err or "")[:500],
                            "payload": json.dumps(e.payload, ensure_ascii=False)[:4000],
                        }, maxlen=200000,
                        approximate=True,
                    )
                except Exception:
                    pass
            if not commit:
                print(f"DRYRUN still bad: dlq_id={e.dlq_id} dq_code={dq_code} hint={h.hint_code}")

    print(
        "Replay summary: "
        f"source={source} seen={seen} replay_ok={ok_replayed} "
        f"skipped_no_fix={skipped_no_fix} skipped_disallowed={skipped_disallowed} "
        f"still_bad={still_bad} commit={int(commit)} delete_after={int(delete_after)}"
    )
    # Exit code 2 if any still bad (useful for automations)
    return 2 if still_bad > 0 else 0


def auto(args: argparse.Namespace) -> int:
    """Automation: triage + safe replay across streams.""",
    r = _connect_redis()
    streams = [s.strip() for s in (args.streams or env("OF_GATE_DLQ_STREAMS", "stream:dlq:of_gate_metrics,stream:dlq:of_gate_quarantine")).split(",") if s.strip()]

    max_per_stream = int(args.max_per_stream)
    commit = bool(args.commit)
    delete_after = bool(args.delete_after_replay)
    notify = bool(args.notify)
    target_override = args.target

    allow_fixes = _parse_allow_fixes(args.allow_fixes or env(
        "OF_GATE_DLQ_AUTO_ALLOW_FIXES",
        "add_schema_name,add_schema_version,coerce_schema_version_int,normalize_ts_ms,ts_from_stream_id,default_missing_legs_empty,coerce_missing_legs_to_json,stringify_missing_legs",
    ))
    require_fix = bool(args.require_fix) or env_bool("OF_GATE_DLQ_AUTO_REQUIRE_FIX", True)

    totals = Counter()
    by_err = Counter()
    by_stream = Counter()
    by_fix = Counter()

    for s in streams:
        by_stream[s] = 0
        for e in _iter_dlq(r, s, "+", max_per_stream):
            totals["seen"] += 1
            by_stream[s] += 1
            by_err[_err_prefix(e.err)] += 1

            base = _payload_for_fix(e)
            fixed, fixes = _safe_fix_payload(base, e.src_stream_id or e.dlq_id)
            ok, dq_code = _validate(fixed)

            if not ok:
                totals["still_bad"] += 1
                continue

            if require_fix and not fixes:
                totals["skipped_no_fix"] += 1
                continue

            if not _fixes_allowed(fixes, allow_fixes):
                totals["skipped_disallowed"] += 1
                continue

            for fx in fixes:
                by_fix[fx] += 1

            totals["eligible"] += 1

            if not commit:
                totals["dryrun_ok"] += 1
                continue

            # Replay to original stream by default
            tgt = target_override or (e.src_stream or "")
            if not tgt:
                tgt = "metrics:of_gate"

            fields = dict(fixed)
            fields["replay"] = 1
            fields["replay_src_dlq_stream"] = s
            fields["replay_src_dlq_id"] = e.dlq_id
            fields["replay_src_stream_id"] = e.src_stream_id
            fields["replay_src_err_prefix"] = _err_prefix(e.err)
            fields["replay_fix_tags"] = ",".join(fixes)[:500]

            try:
                r.xadd(
                    tgt,
                    {k: json.dumps(v, ensure_ascii=False) if isinstance(v, (dict, list)) else str(v) for k, v in fields.items()},
                    maxlen=2000000,
                    approximate=True,
                )
                totals["replayed"] += 1
                if delete_after:
                    try:
                        r.xdel(s, e.dlq_id)
                        totals["deleted"] += 1
                    except Exception:
                        totals["delete_failed"] += 1
            except Exception:
                totals["replay_write_failed"] += 1

    # Build summary
    def top(counter: Counter, k: int = 8) -> str:
        return ", ".join([f"{a}:{b}" for a, b in counter.most_common(k)])

    out_lines = [
        "OF-Gate DLQ auto-replay (P3):",
        f"  streams={len(streams)} seen={totals['seen']} eligible={totals['eligible']} replayed={totals['replayed']} deleted={totals['deleted']}",
        f"  skipped_no_fix={totals['skipped_no_fix']} skipped_disallowed={totals['skipped_disallowed']} still_bad={totals['still_bad']} replay_write_failed={totals['replay_write_failed']}",
        f"  allow_fixes={','.join(sorted(allow_fixes))}",
        "  by_stream: " + ", ".join([f"{k}:{v}" for k, v in by_stream.most_common()]),
        "  top_err_prefix: " + top(by_err, 10),
        "  top_fix_tags: " + top(by_fix, 10),
    ]
    out = "\n".join(out_lines)
    print(out)

    if notify:
        try:
            _xadd_best_effort(
                r,
                _notify_stream_name(),
                {
                    "ts_ms": get_ny_time_millis(),
                    "source": "of_gate_dlq_auto_replay_p3",
                    "message": out[:3500],
                    "severity": "warn" if totals["seen"] else "info",
                }
            )
        except Exception:
            pass

    # Exit 2 if anything still bad (useful for cron/timers)
    return 2 if totals["still_bad"] or totals["replay_write_failed"] else 0


def main() -> None:
    ap = argparse.ArgumentParser(prog="of_gate_dlq_fixed_replay_p84")
    sub = ap.add_subparsers(dest="cmd", required=True)

    ap_t = sub.add_parser("triage", help="Summarize DLQ + hints (no writes)")
    ap_t.add_argument("--streams", default="", help="Comma-separated DLQ streams")
    ap_t.add_argument("--limit", type=int, default=5000)
    ap_t.add_argument("--notify", action="store_true")
    ap_t.set_defaults(fn=triage)

    ap_r = sub.add_parser("replay", help="Fix+validate and replay back to src stream")
    ap_r.add_argument("--source", default="", help="DLQ stream to read")
    ap_r.add_argument("--target", default="", help="Override target stream")
    ap_r.add_argument("--max", type=int, default=500)
    ap_r.add_argument("--commit", action="store_true")
    ap_r.add_argument("--delete-after-replay", action="store_true")
    ap_r.add_argument("--allow-fixes", default="", help="Comma-separated allowlist of fix tags (empty=allow all)")
    ap_r.add_argument("--require-fix", action="store_true", help="Only replay entries that required at least one fix")
    ap_r.set_defaults(fn=replay)

    ap_a = sub.add_parser("auto", help="Automation: triage + safe replay across streams")
    ap_a.add_argument("--streams", default="", help="Comma-separated DLQ streams")
    ap_a.add_argument("--target", default="", help="Override target stream")
    ap_a.add_argument("--max-per-stream", type=int, default=env_int("OF_GATE_DLQ_AUTO_MAX_PER_STREAM", 2000))
    ap_a.add_argument("--commit", action="store_true")
    ap_a.add_argument("--delete-after-replay", action="store_true")
    ap_a.add_argument("--notify", action="store_true")
    ap_a.add_argument("--allow-fixes", default="", help="Comma-separated allowlist of fix tags")
    ap_a.add_argument("--require-fix", action="store_true")
    ap_a.set_defaults(fn=auto)

    args = ap.parse_args()
    rc = int(args.fn(args))
    raise SystemExit(rc)


if __name__ == "__main__":
    main()
