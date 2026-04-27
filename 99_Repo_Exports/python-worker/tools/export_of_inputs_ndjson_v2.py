from __future__ import annotations
from utils.time_utils import get_ny_time_millis

'''
Export OFInputsV1 from Redis Stream to NDJSON (one JSON object per line).

Why:
  - Golden replay requires deterministic inputs captured at decision time.
  - Stream format: XADD <stream> * payload '<raw-json OFInputsV1>'

Defaults (per repo config):
  - stream:  OF_INPUTS_STREAM (default: signals:of:inputs)
  - field:   OF_INPUTS_STREAM_FIELD (default: payload)

Features:
  - pagination via XRANGE, deterministic by stream id order
  - resume via state file (stores last processed stream id)
  - robust parsing: bytes/str payload, validates JSON, fail-open on bad rows
  - writes direct OFInputsV1 dict per line (compatible with tools/of_replay_from_inputs.py)
'''

import argparse
import json
import os
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, Optional, Tuple


def _b(x: Any) -> bytes:
    if isinstance(x, bytes):
        return x
    if isinstance(x, str):
        return x.encode("utf-8", errors="ignore")
    return str(x).encode("utf-8", errors="ignore")


def _s(x: Any) -> str:
    if isinstance(x, str):
        return x
    if isinstance(x, bytes):
        return x.decode("utf-8", errors="ignore")
    return str(x)


def _atomic_write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(text, encoding="utf-8")
    tmp.replace(path)


def _read_last_id(state_file: Path) -> Optional[str]:
    try:
        if not state_file.exists():
            return None
        text = state_file.read_text(encoding="utf-8").strip()
        if not text:
            return None
        if text.startswith("{"):
            import json
            try:
                obj = json.loads(text)
                if isinstance(obj, dict):
                    return obj.get("last_id")
            except Exception:
                pass
        return text
    except Exception:
        return None


@dataclass
class ExportStats:
    scanned: int = 0
    written: int = 0
    bad_json: int = 0
    missing_field: int = 0


def iter_stream_payloads(
    *,
    r: Any,
    stream: str,
    field: str,
    start_id: str,
    end_id: str,
    batch: int,
) -> Iterable[Tuple[str, Optional[str]]]:
    '''
    Yield (stream_id, payload_str or None) from XRANGE in deterministic order.
    Uses exclusive start_id semantics by passing '(' prefix after the first page.
    '''
    try:
        import redis
    except ImportError:
        redis = None
        
    cur = start_id
    first = True
    while True:
        min_id = cur if first else f"({cur}"
        first = False
        try:
            rows = r.xrange(stream, min=min_id, max=end_id, count=batch)
        except Exception as e:
            if redis and isinstance(e, redis.exceptions.ConnectionError):
                print(f"[ERROR] Redis connection failed during xrange: {e}", file=sys.stderr)
                raise
            # slightly inconsistent with original which didn't verify redis import here, 
            # but we want to fail fast on connection errors.
            # For other errors, we might want to retry or just raise. 
            # The original code didn't catch errors here, so raising is safer.
            raise
            
        if not rows:
            return
        for sid, fields in rows:
            sid_s = _s(sid)
            payload = None
            try:
                # redis-py returns dict[bytes, bytes]
                v = None
                try:
                    v = fields.get(_b(field))
                except Exception:
                    v = None
                if v is None:
                    try:
                        v = fields.get(field)
                    except Exception:
                        v = None
                if v is not None:
                    payload = _s(v)
            except Exception:
                payload = None
            yield sid_s, payload
            cur = sid_s



def export_of_inputs(
    *,
    redis_url: str,
    stream: str,
    field: str,
    out_path: Path,
    state_file: Optional[Path],
    resume: bool,
    start_id: str,
    end_id: str,
    batch: int,
    max_records: int,
    validate: bool,
    quiet: bool,
) -> ExportStats:
    try:
        import redis  # type: ignore
    except Exception as exc:
        raise RuntimeError("Missing dependency 'redis'. Install redis-py in python-worker venv.") from exc

    r = redis.Redis.from_url(redis_url, decode_responses=False)

    last_id = None
    if resume and state_file is not None:
        last_id = _read_last_id(state_file)

    effective_start = last_id or start_id

    out_path.parent.mkdir(parents=True, exist_ok=True)
    mode = "a" if (resume and last_id and out_path.exists()) else "w"

    st = ExportStats()
    last_written_id: Optional[str] = None

    with out_path.open(mode, encoding="utf-8") as f:
        for sid, payload in iter_stream_payloads(
            r=r,
            stream=stream,
            field=field,
            start_id=effective_start,
            end_id=end_id,
            batch=batch,
        ):
            st.scanned += 1

            if payload is None:
                st.missing_field += 1
                if not quiet and st.missing_field <= 5:
                    print(f"[WARN] missing field '{field}' at id={sid}", file=sys.stderr)
                continue

            try:
                obj = json.loads(payload) if validate else None
            except Exception:
                st.bad_json += 1
                if not quiet and st.bad_json <= 5:
                    print(f"[WARN] bad JSON payload at id={sid}: {payload[:200]}", file=sys.stderr)
                continue

            if validate:
                if not isinstance(obj, dict) or "v" not in obj or "symbol" not in obj or "ts_ms" not in obj:
                    st.bad_json += 1
                    if not quiet and st.bad_json <= 5:
                        print(f"[WARN] payload not OFInputsV1-like at id={sid}", file=sys.stderr)
                if "sid" not in obj:
                    sym = str(obj.get("symbol") or "")
                    ts_val = obj.get("ts_ms") or obj.get("ts")
                    if sym and ts_val:
                        obj["sid"] = f"crypto-of:{sym}:{ts_val}"
                f.write(json.dumps(obj, ensure_ascii=False, separators=(",", ":")) + "\n")
            else:
                # Raw passthrough (each payload already a JSON object string)
                # Ensure sid if missing (best-effort, requires parsing)
                if field == "payload":
                     try:
                         # We need to parse to check/add sid
                         o = json.loads(payload)
                         if "sid" not in o:
                             sym = str(o.get("symbol") or "")
                             ts_val = o.get("ts_ms") or o.get("ts")
                             if sym and ts_val:
                                 o["sid"] = f"crypto-of:{sym}:{ts_val}"
                                 payload = json.dumps(o, ensure_ascii=False)
                     except Exception:
                         pass
                f.write(payload.strip() + "\n")

            st.written += 1
            last_written_id = sid

            if max_records > 0 and st.written >= max_records:
                break

    if state_file is not None and last_written_id is not None:
        _atomic_write_text(state_file, last_written_id + "\n")

    return st





def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--redis-url", default=os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0"))
    ap.add_argument("--stream", default=os.getenv("OF_INPUTS_STREAM", "stream:of:inputs"))
    ap.add_argument("--field", default=os.getenv("OF_INPUTS_STREAM_FIELD", "payload"))
    ap.add_argument("--since-hours", type=float, default=float(os.getenv("OF_INPUTS_SINCE_HOURS", "24")))
    ap.add_argument("--since-ts-ms", type=int, default=0)
    ap.add_argument("--max-records", type=int, default=int(os.getenv("OF_INPUTS_MAX_RECORDS", "250000")))
    ap.add_argument("--out", required=True)
    ap.add_argument("--resume", action="store_true", default=False)
    ap.add_argument("--state-file", default=os.getenv("OF_INPUTS_STATE_FILE", ""))
    ap.add_argument("--start-id", default="-")
    ap.add_argument("--end-id", default="+")
    ap.add_argument("--batch", type=int, default=1000)
    ap.add_argument("--no-validate", action="store_true")
    ap.add_argument("--quiet", action="store_true")
    args = ap.parse_args()

    since_ms = int(args.since_ts_ms) if int(args.since_ts_ms) > 0 else (get_ny_time_millis() - int(args.since_hours * 3600_000))
    start_id = args.start_id
    
    # If using time-based start and no explicit start-id, calculate it
    if args.start_id == "-" and since_ms > 0 and not (args.resume and args.state_file and Path(args.state_file).exists()):  # noqa: E501
         start_id = f"{since_ms}-0"

    out_path = Path(args.out)
    state_file = Path(args.state_file) if args.state_file else None

    # Unified call - redundant export_stream_since removed
    st = export_of_inputs(
        redis_url=str(args.redis_url),
        stream=str(args.stream),
        field=str(args.field),
        out_path=out_path,
        state_file=state_file,
        resume=args.resume,
        start_id=str(start_id),
        end_id=str(args.end_id),
        batch=int(args.batch),
        max_records=int(args.max_records),
        validate=not bool(args.no_validate),
        quiet=bool(args.quiet),
    )

    if not args.quiet:
        print(
            json.dumps(
                {
                    "ok": True,
                    "stream": str(args.stream),
                    "field": str(args.field),
                    "out": str(out_path),
                    "scanned": st.scanned,
                    "written": st.written,
                    "missing_field": st.missing_field,
                    "bad_json": st.bad_json,
                },
                ensure_ascii=False,
                indent=2,
                sort_keys=True,
            )
        )


if __name__ == "__main__":
    main()
