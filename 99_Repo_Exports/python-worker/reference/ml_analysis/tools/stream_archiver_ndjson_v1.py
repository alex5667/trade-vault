"""Generic Redis Stream → NDJSON archiver (P58).

Goal:
- Provide durable, day-partitioned archives for short-retention streams (e.g., signals:of:inputs, trades:closed).
- Enable offline KPI audits and ML dataset building with file fallback.

Format:
- Each line is a JSON object with:
    stream_id: Redis XADD entry id
    stream: stream name
    archived_ts_ms: archive timestamp (ms)
    ...original fields (decoded)
  If PAYLOAD_FIELD exists and contains JSON, we parse it and store as a dict under the same key.

Files:
- ARCHIVE_DIR/YYYY-MM-DD.ndjson (or .ndjson.gz if GZIP=1), partitioned by UTC day derived from:
    - payload.exit_ts_ms / payload.close_ts_ms / payload.ts_ms
    - or top-level equivalents
    - fallback: archived_ts_ms

At-least-once:
- Uses consumer groups; write happens before ACK.
- Optional seen-id dedup key reduces duplicates across restarts.

Typical usage (periodic drain):
  python -m ml_analysis.tools.stream_archiver_ndjson_v1 --once --max-messages 200000

Continuous:
  python -m ml_analysis.tools.stream_archiver_ndjson_v1 --loop-s 1 --batch 2000
"""

from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import argparse
import gzip
import json
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional, TYPE_CHECKING, Tuple

if TYPE_CHECKING:  # pragma: no cover
    import redis  # type: ignore


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
        return int(default)
    if isinstance(x, bool):
        return int(default)
    if isinstance(x, (int, float)):
        try:
            return int(x)
        except Exception:
            return int(default)
    if isinstance(x, bytes):
        try:
            x = x.decode("utf-8", "ignore")
        except Exception:
            return int(default)
    try:
        s = str(x).strip()
        if not s:
            return int(default)
        return int(float(s))
    except Exception:
        return int(default)


def _safe_json_loads(x: Any) -> Optional[Dict[str, Any]]:
    if x is None:
        return None
    if isinstance(x, dict):
        return x
    if isinstance(x, bytes):
        try:
            x = x.decode("utf-8", "replace")
        except Exception:
            return None
    if not isinstance(x, str):
        return None
    s = x.strip()
    if not s:
        return None
    try:
        obj = json.loads(s)
        return obj if isinstance(obj, dict) else None
    except Exception:
        return None


def _utc_day_from_ts_ms(ts_ms: int) -> str:
    return time.strftime("%Y-%m-%d", time.gmtime(int(ts_ms) / 1000))


def _pick_event_ts_ms(rec: Dict[str, Any], payload_field: str) -> int:
    # payload dominates
    payload = rec.get(payload_field)
    if isinstance(payload, dict):
        for k in ("exit_ts_ms", "close_ts_ms", "ts_ms", "ts", "t", "t_ms", "timestamp_ms"):
            if k in payload:
                v = _as_int(payload.get(k), 0)
                if v > 0:
                    if 0 < v < 10_000_000_000:
                        v *= 1000
                    return int(v)
        meta = payload.get("meta") or payload.get("metadata")
        if isinstance(meta, dict):
            for k in ("exit_ts_ms", "close_ts_ms", "ts_ms", "ts", "t", "t_ms", "timestamp_ms"):
                if k in meta:
                    v = _as_int(meta.get(k), 0)
                    if v > 0:
                        if 0 < v < 10_000_000_000:
                            v *= 1000
                        return int(v)

    for k in ("exit_ts_ms", "close_ts_ms", "ts_ms", "ts", "t", "t_ms", "timestamp_ms"):
        if k in rec:
            v = _as_int(rec.get(k), 0)
            if v > 0:
                if 0 < v < 10_000_000_000:
                    v *= 1000
                return int(v)

    return int(rec.get("archived_ts_ms") or _now_ms())


@dataclass
class Cfg:
    redis_url: str
    stream: str
    group: str
    consumer: str

    archive_dir: Path
    gzip_enabled: bool
    payload_field: str

    batch: int
    max_messages: int
    loop_s: float
    once: bool

    flush_every: int
    fsync_every: int

    seen_prefix: str
    seen_ttl_sec: int

    delete_after_ack: bool
    metrics_hash: str


def _env_str(name: str, default: str) -> str:
    v = os.getenv(name)
    return default if v is None else v


def _env_int(name: str, default: int) -> int:
    try:
        return int(_env_str(name, str(default)).strip())
    except Exception:
        return default


def _env_bool(name: str, default: str = "0") -> bool:
    return _env_str(name, default).strip().lower() in ("1", "true", "yes", "on")


def load_cfg(args: argparse.Namespace) -> Cfg:
    consumer = args.consumer or _env_str("ARCHIVER_CONSUMER", f"archiver-{os.getpid()}")
    return Cfg(
        redis_url=args.redis_url or _env_str("REDIS_URL", "redis://localhost:6379/0")
        stream=args.stream or _env_str("ARCHIVE_STREAM", "")
        group=args.group or _env_str("ARCHIVER_GROUP", "archiver_v1")
        consumer=consumer
        archive_dir=Path(args.archive_dir or _env_str("ARCHIVE_DIR", "./archives")).expanduser().resolve()
        gzip_enabled=bool(args.gzip_enabled) if args.gzip_enabled is not None else _env_bool("GZIP", "0")
        payload_field=str(args.payload_field or _env_str("PAYLOAD_FIELD", "payload"))
        batch=int(args.batch or _env_int("BATCH", 2000))
        max_messages=int(args.max_messages or _env_int("MAX_MESSAGES", 0))
        loop_s=float(args.loop_s if args.loop_s is not None else float(_env_str("LOOP_S", "1")))
        once=bool(args.once) if args.once is not None else _env_bool("ONCE", "0")
        flush_every=int(args.flush_every or _env_int("FLUSH_EVERY", 1000))
        fsync_every=int(args.fsync_every or _env_int("FSYNC_EVERY", 20000))
        seen_prefix=str(args.seen_prefix or _env_str("SEEN_PREFIX", ""))
        seen_ttl_sec=int(args.seen_ttl_sec or _env_int("SEEN_TTL_SEC", 7 * 24 * 3600))
        delete_after_ack=bool(args.delete_after_ack) if args.delete_after_ack is not None else _env_bool("DELETE_AFTER_ACK", "0")
        metrics_hash=str(args.metrics_hash or _env_str("ARCHIVER_METRICS_HASH", f"metrics:archiver:{args.stream or ''}"))
    )


def _ensure_group(r: "redis.Redis", stream: str, group: str) -> None:
    try:
        r.xgroup_create(name=stream, groupname=group, id="0-0", mkstream=True)
    except Exception as e:
        # BUSYGROUP is fine
        if "BUSYGROUP" in str(e):
            return
        # If stream doesn't exist and mkstream unsupported? retry with mkstream False then create empty
        try:
            r.xgroup_create(name=stream, groupname=group, id="0-0", mkstream=False)
        except Exception:
            return


def _open_out(path: Path, gzip_enabled: bool):
    if gzip_enabled:
        return gzip.open(str(path) + ".gz", "at", encoding="utf-8")
    return open(path, "a", encoding="utf-8")


def _metrics_incr(r: "redis.Redis", h: str, field: str, n: int = 1) -> None:
    try:
        r.hincrby(h, field, int(n))
        r.hset(h, mapping={"last_ts_ms": str(_now_ms())})
    except Exception:
        pass


def main(argv: Optional[list[str]] = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--redis_url", default=os.environ.get("REDIS_URL", "redis://localhost:6379/0"))
    ap.add_argument("--stream", default=os.environ.get("ARCHIVE_STREAM", ""), required=False)
    ap.add_argument("--group", default=os.environ.get("ARCHIVER_GROUP", "archiver_v1"))
    ap.add_argument("--consumer", default=os.environ.get("ARCHIVER_CONSUMER", ""))
    ap.add_argument("--archive_dir", default=os.environ.get("ARCHIVE_DIR", "./archives"))
    ap.add_argument("--gzip_enabled", type=int, default=int(os.environ.get("GZIP", "0")))
    ap.add_argument("--payload_field", default=os.environ.get("PAYLOAD_FIELD", "payload"))
    ap.add_argument("--batch", type=int, default=int(os.environ.get("BATCH", "2000")))
    ap.add_argument("--max_messages", type=int, default=int(os.environ.get("MAX_MESSAGES", "0")))
    ap.add_argument("--loop_s", type=float, default=float(os.environ.get("LOOP_S", "1")))
    ap.add_argument("--once", type=int, default=int(os.environ.get("ONCE", "0")))
    ap.add_argument("--flush_every", type=int, default=int(os.environ.get("FLUSH_EVERY", "1000")))
    ap.add_argument("--fsync_every", type=int, default=int(os.environ.get("FSYNC_EVERY", "20000")))
    ap.add_argument("--seen_prefix", default=os.environ.get("SEEN_PREFIX", ""))
    ap.add_argument("--seen_ttl_sec", type=int, default=int(os.environ.get("SEEN_TTL_SEC", str(7 * 24 * 3600))))
    ap.add_argument("--delete_after_ack", type=int, default=int(os.environ.get("DELETE_AFTER_ACK", "0")))
    ap.add_argument("--metrics_hash", default=os.environ.get("ARCHIVER_METRICS_HASH", ""))
    args = ap.parse_args(list(argv) if argv is not None else None)

    if not str(args.stream).strip():
        raise SystemExit("ARCHIVE_STREAM/--stream is required")

    try:
        import redis  # type: ignore
    except Exception as e:
        raise SystemExit(f"redis-py is required: {e}")

    cfg = load_cfg(args)
    r = redis.Redis.from_url(cfg.redis_url, decode_responses=False)

    _ensure_group(r, cfg.stream, cfg.group)
    cfg.archive_dir.mkdir(parents=True, exist_ok=True)

    # file handles by day
    handles: Dict[str, Any] = {}
    written = 0
    loop = True
    last_fsync = 0

    def _get_handle(day: str):
        if day in handles:
            return handles[day]
        fp = cfg.archive_dir / f"{day}.ndjson"
        h = _open_out(fp, cfg.gzip_enabled)
        handles[day] = h
        return h

    while loop:
        try:
            resp = r.xreadgroup(cfg.group, cfg.consumer, {cfg.stream: ">"}, count=int(cfg.batch), block=1000)
        except Exception:
            resp = []

        if not resp:
            if cfg.once:
                break
            time.sleep(max(0.1, float(cfg.loop_s)))
            continue

        # resp: [(stream, [(id, {field:val})...])]
        for _stream_name, entries in resp:
            for msg_id_b, fields in entries:
                msg_id = _as_str(msg_id_b)
                if not msg_id:
                    continue

                # Optional seen-id dedup
                if cfg.seen_prefix:
                    try:
                        ok = r.set(cfg.seen_prefix + msg_id, b"1", nx=True, ex=int(cfg.seen_ttl_sec))
                        if not ok:
                            # already archived -> ack and skip
                            r.xack(cfg.stream, cfg.group, msg_id)
                            _metrics_incr(r, cfg.metrics_hash, "dedup_skip_total", 1)
                            continue
                    except Exception:
                        pass

                rec: Dict[str, Any] = {"stream_id": msg_id, "stream": cfg.stream, "archived_ts_ms": _now_ms()}
                # decode fields
                if isinstance(fields, dict):
                    for k_b, v_b in fields.items():
                        k = _as_str(k_b)
                        if not k:
                            continue
                        if k == cfg.payload_field:
                            pobj = _safe_json_loads(v_b)
                            if pobj is not None:
                                rec[k] = pobj
                            else:
                                rec[k] = _as_str(v_b)
                        else:
                            rec[k] = _as_str(v_b)

                ts_ms = _pick_event_ts_ms(rec, cfg.payload_field)
                day = _utc_day_from_ts_ms(ts_ms)

                try:
                    h = _get_handle(day)
                    h.write(json.dumps(rec, ensure_ascii=False, separators=(",", ":")) + "\n")
                    written += 1
                except Exception:
                    _metrics_incr(r, cfg.metrics_hash, "write_error_total", 1)
                    # no ACK -> retry later
                    continue

                try:
                    r.xack(cfg.stream, cfg.group, msg_id)
                except Exception:
                    pass

                if cfg.delete_after_ack:
                    try:
                        r.xdel(cfg.stream, msg_id)
                    except Exception:
                        pass

                _metrics_incr(r, cfg.metrics_hash, "archived_total", 1)

                if cfg.flush_every > 0 and written % int(cfg.flush_every) == 0:
                    for hh in handles.values():
                        try:
                            hh.flush()
                        except Exception:
                            pass

                if cfg.fsync_every > 0 and written - last_fsync >= int(cfg.fsync_every):
                    last_fsync = written
                    for hh in handles.values():
                        try:
                            hh.flush()
                            os.fsync(hh.fileno())
                        except Exception:
                            pass

                if cfg.max_messages > 0 and written >= int(cfg.max_messages):
                    loop = False
                    break
            if not loop:
                break

    for hh in handles.values():
        try:
            hh.flush()
            hh.close()
        except Exception:
            pass

    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
