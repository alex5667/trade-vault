"""Archive ml_replay_inputs_v1 stream to NDJSON on disk.

Why (P56):
- Redis streams have finite retention and are not a durable dataset store.
- We need deterministic replay inputs for offline analysis, KPI audits, and ML training.

Inputs:
- Redis stream: ml_replay_inputs_v1 (payload-only field "payload" with JSON)

Output:
- NDJSON files under ARCHIVE_DIR partitioned by day (UTC), e.g.:
    ./archives/ml_replay_inputs_v1/2026-02-15.ndjson
  Each line is a JSON object with injected fields:
    - stream_id: Redis stream entry id
    - archived_ts_ms: time archived

Operational notes:
- Uses consumer group for at-least-once. File write happens before ACK.
- Safe idempotency relies on consumer group and optional seen-id dedup (ARCHIVER_SEEN_PREFIX).

CLI examples:
  python -m ml_analysis.tools.replay_inputs_archiver --once --batch 5000
  python -m ml_analysis.tools.replay_inputs_archiver --loop-s 1 --batch 2000
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
from typing import Any, Dict, Optional, TYPE_CHECKING

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


def _pick_ts_ms(payload: Dict[str, Any]) -> int:
    close = payload.get("close") if isinstance(payload.get("close"), dict) else None
    if close:
        v = close.get("close_ts_ms")
        if v is not None:
            return _as_int(v, _now_ms())
    v2 = payload.get("ts_ms") or payload.get("timestamp_ms")
    if v2 is not None:
        return _as_int(v2, _now_ms())
    return _now_ms()


@dataclass
class ArchiverCfg:
    redis_url: str
    stream: str
    group: str
    consumer: str

    archive_dir: Path
    gzip_enabled: bool
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


def load_cfg() -> ArchiverCfg:
    consumer = _env_str("REPLAY_ARCHIVER_CONSUMER", f"replay-archiver-{os.getpid()}")
    return ArchiverCfg(
        redis_url=_env_str("REDIS_URL", "redis://localhost:6379/0"),
        stream=_env_str("REPLAY_INPUTS_STREAM", "ml_replay_inputs_v1"),
        group=_env_str("REPLAY_ARCHIVER_GROUP", "ml_replay_archiver_v1"),
        consumer=consumer,
        archive_dir=Path(_env_str("ARCHIVE_DIR", "./archives/ml_replay_inputs_v1")).expanduser(),
        gzip_enabled=_env_bool("ARCHIVE_GZIP", "0"),
        flush_every=_env_int("ARCHIVE_FLUSH_EVERY", 100),
        fsync_every=_env_int("ARCHIVE_FSYNC_EVERY", 1000),
        seen_prefix=_env_str("ARCHIVER_SEEN_PREFIX", "archiver:seen:"),
        seen_ttl_sec=_env_int("ARCHIVER_SEEN_TTL_SEC", 14 * 24 * 3600),
        delete_after_ack=_env_bool("REPLAY_ARCHIVER_DELETE_AFTER_ACK", "0"),
        metrics_hash=_env_str("REPLAY_ARCHIVER_METRICS_HASH", "metrics:replay_inputs_archiver"),
    )


def _redis(cfg: ArchiverCfg):
    import redis  # type: ignore

    return redis.Redis.from_url(cfg.redis_url, decode_responses=False)


def _ensure_group(r, stream: str, group: str) -> None:
    import redis  # type: ignore

    try:
        r.xgroup_create(stream, group, id="0-0", mkstream=True)
    except redis.ResponseError as e:  # type: ignore
        if "BUSYGROUP" in str(e):
            return
        raise


def _metrics_incr(r, key: str, field: str, inc: int = 1) -> None:
    try:
        r.hincrby(key, field, inc)
    except Exception:
        pass


def _metrics_set(r, key: str, field: str, value: Any) -> None:
    try:
        r.hset(key, field, _as_str(value).encode("utf-8"))
    except Exception:
        pass


class _Writer:
    def __init__(self, cfg: ArchiverCfg):
        self.cfg = cfg
        self.cfg.archive_dir.mkdir(parents=True, exist_ok=True)
        self._open_day: Optional[str] = None
        self._fh = None  # type: ignore
        self._n_since_flush = 0
        self._n_since_fsync = 0

    def _open_for_day(self, day: str):
        if self._fh is not None and self._open_day == day:
            return
        self.close()
        ext = ".ndjson.gz" if self.cfg.gzip_enabled else ".ndjson"
        path = self.cfg.archive_dir / f"{day}{ext}"
        if self.cfg.gzip_enabled:
            self._fh = gzip.open(path, "ab")
        else:
            self._fh = open(path, "ab", buffering=0)
        self._open_day = day
        self._n_since_flush = 0
        self._n_since_fsync = 0

    def write_line(self, day: str, obj: Dict[str, Any]) -> None:
        self._open_for_day(day)
        assert self._fh is not None
        line = (json.dumps(obj, ensure_ascii=False, separators=(",", ":")) + "\n").encode("utf-8")
        self._fh.write(line)
        self._n_since_flush += 1
        self._n_since_fsync += 1
        if (not self.cfg.gzip_enabled) and self.cfg.flush_every > 0 and self._n_since_flush >= self.cfg.flush_every:
            try:
                self._fh.flush()
            except Exception:
                pass
            self._n_since_flush = 0
        if (not self.cfg.gzip_enabled) and self.cfg.fsync_every > 0 and self._n_since_fsync >= self.cfg.fsync_every:
            try:
                os.fsync(self._fh.fileno())
            except Exception:
                pass
            self._n_since_fsync = 0

    def close(self):
        if self._fh is not None:
            try:
                self._fh.close()
            except Exception:
                pass
        self._fh = None
        self._open_day = None


def _seen_key(cfg: ArchiverCfg, stream_id: str) -> bytes:
    return f"{cfg.seen_prefix}{stream_id}".encode("utf-8")


def _ack_and_optional_delete(r, cfg: ArchiverCfg, stream_id: bytes) -> None:
    r.xack(cfg.stream, cfg.group, stream_id)
    if cfg.delete_after_ack:
        try:
            r.xdel(cfg.stream, stream_id)
        except Exception:
            pass


def _process_one(r, cfg: ArchiverCfg, writer: _Writer, stream_id: bytes, fields: Dict[bytes, bytes]) -> None:
    sid = None
    try:
        stream_id_str = stream_id.decode("utf-8", "replace")
        if r.set(_seen_key(cfg, stream_id_str), b"1", nx=True, ex=cfg.seen_ttl_sec) is None:
            _metrics_incr(r, cfg.metrics_hash, "seen_dedup_skipped_total", 1)
            _ack_and_optional_delete(r, cfg, stream_id)
            return

        payload = _safe_json_loads(fields.get(b"payload"))
        if not payload:
            _metrics_incr(r, cfg.metrics_hash, "bad_payload_total", 1)
            _ack_and_optional_delete(r, cfg, stream_id)
            return

        sid = _as_str(payload.get("sid"))
        if not sid:
            _metrics_incr(r, cfg.metrics_hash, "no_sid_total", 1)
            _ack_and_optional_delete(r, cfg, stream_id)
            return

        ts_ms = _pick_ts_ms(payload)
        day = _utc_day_from_ts_ms(ts_ms)

        out = dict(payload)
        out["stream_id"] = stream_id_str
        out["archived_ts_ms"] = _now_ms()

        writer.write_line(day, out)

        _metrics_incr(r, cfg.metrics_hash, "archived_total", 1)
        _metrics_set(r, cfg.metrics_hash, "last_archived_ts_ms", _now_ms())
        _metrics_set(r, cfg.metrics_hash, "last_sid", sid)
        _metrics_set(r, cfg.metrics_hash, "last_stream_id", stream_id_str)

        _ack_and_optional_delete(r, cfg, stream_id)
    except Exception:
        _metrics_incr(r, cfg.metrics_hash, "error_total", 1)
        _metrics_set(r, cfg.metrics_hash, "last_error_sid", sid or "")


def drain(r, cfg: ArchiverCfg, writer: _Writer, batch: int, block_ms: int, read_pending: bool, read_new: bool) -> int:
    n = 0
    _metrics_set(r, cfg.metrics_hash, "last_run_ts_ms", _now_ms())
    try:
        if read_pending:
            msgs = r.xreadgroup(cfg.group, cfg.consumer, {cfg.stream: b"0-0"}, count=batch)
            for _stream, entries in msgs or []:
                for stream_id, fields in entries:
                    _process_one(r, cfg, writer, stream_id, fields)
                    n += 1
                    if n >= batch:
                        return n
        if read_new:
            msgs = r.xreadgroup(cfg.group, cfg.consumer, {cfg.stream: b">"}, count=max(1, batch - n), block=block_ms)
            for _stream, entries in msgs or []:
                for stream_id, fields in entries:
                    _process_one(r, cfg, writer, stream_id, fields)
                    n += 1
                    if n >= batch:
                        return n
    except Exception as e:
        if "NOGROUP" in str(e).upper():
            print(f"⚠️ NOGROUP error detected, recreating consumer group {cfg.group} for stream {cfg.stream}...")
            _ensure_group(r, cfg.stream, cfg.group)
        else:
            raise
    return n


def main() -> None:
    cfg = load_cfg()
    r = _redis(cfg)

    import redis
    import time
    while True:
        try:
            r.ping()
            _ensure_group(r, cfg.stream, cfg.group)
            break
        except (redis.exceptions.BusyLoadingError, redis.exceptions.ConnectionError) as e:
            print(f"Redis not ready ({e}). Retrying in 5 seconds...")
            time.sleep(5)

    ap = argparse.ArgumentParser()
    ap.add_argument("--once", action="store_true")
    ap.add_argument("--batch", type=int, default=int(_env_str("REPLAY_ARCHIVER_BATCH", "2000")))
    ap.add_argument("--block-ms", type=int, default=int(_env_str("REPLAY_ARCHIVER_BLOCK_MS", "1000")))
    ap.add_argument("--loop-s", type=float, default=float(_env_str("REPLAY_ARCHIVER_LOOP_S", "1")))
    args = ap.parse_args()

    writer = _Writer(cfg)
    _metrics_set(r, cfg.metrics_hash, "start_ts_ms", _now_ms())

    if args.once:
        drained = drain(r, cfg, writer, batch=args.batch, block_ms=0, read_pending=True, read_new=True)
        _metrics_set(r, cfg.metrics_hash, "last_once_drained", drained)
        writer.close()
        return

    while True:
        drained = drain(r, cfg, writer, batch=args.batch, block_ms=args.block_ms, read_pending=True, read_new=True)
        if drained == 0:
            time.sleep(max(0.05, float(args.loop_s)))


if __name__ == "__main__":
    main()
