# python-worker/services/tb_labeler_worker_v10_1.py
from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import json
import os
import time
from typing import Any, Dict, List, Tuple, Optional
import traceback

import redis

from core.tb_labeling import infer_tp_sl_bps, barrier_stats, exec_cost_r

# prometheus is optional; TB labeler can run without it
try:
    from prometheus_client import Counter, Histogram, Gauge, start_http_server  # type: ignore
except Exception:  # pragma: no cover
    Counter = Histogram = Gauge = start_http_server = None  # type: ignore


class _NoopMetric:
    def labels(self, *args: Any, **kwargs: Any) -> "_NoopMetric":
        return self

    def inc(self, n: float = 1.0) -> None:
        return

    def observe(self, v: float) -> None:
        return

    def set(self, v: float) -> None:
        return


_METRICS_ENABLE = os.getenv("TB_METRICS_ENABLE", "1") == "1" and Counter is not None

TB_JOBS_TOTAL = Counter("tb_label_jobs_total", "TB labeler jobs processed", ["status"]) if _METRICS_ENABLE else _NoopMetric()
TB_INPUT_LOOKUP_TOTAL = Counter(
    "tb_label_input_lookup_total", "How OF input was looked up", ["mode"]
) if _METRICS_ENABLE else _NoopMetric()
TB_INPUT_LOOKUP_MS = Histogram(
    "tb_label_input_lookup_ms", "OF input lookup latency (ms)", buckets=(1, 2, 5, 10, 20, 50, 100, 200, 500, 1000, 2000)
) if _METRICS_ENABLE else _NoopMetric()
TB_INPUT_LAG_MS = Gauge("tb_label_input_lag_ms", "Lag between now and input ts_ms") if _METRICS_ENABLE else _NoopMetric()
TB_LABEL_WRITE_TOTAL = Counter("tb_label_write_total", "TB labels written") if _METRICS_ENABLE else _NoopMetric()
TB_TICK_FETCH_MS = Histogram(
    "tb_label_tick_fetch_ms", "Tick fetch latency (ms)", buckets=(1, 2, 5, 10, 20, 50, 100, 200, 500, 1000, 2000, 5000)
) if _METRICS_ENABLE else _NoopMetric()
TB_TICKS_USED = Histogram(
    "tb_label_ticks_used", "Number of ticks used per label job", buckets=(50, 100, 200, 500, 1000, 2000, 5000, 10000, 50000)
) if _METRICS_ENABLE else _NoopMetric()


# Optional PG (enabled only if TB_PG_DSN is set)
try:
    import psycopg2  # type: ignore
except Exception:  # pragma: no cover
    psycopg2 = None  # type: ignore


# ----------------------------
# Config
# ----------------------------
REDIS_URL = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
TICKS_REDIS_URL = os.getenv("TICKS_REDIS_URL", "redis://redis-ticks:6379/0")

OF_INPUTS_STREAM = os.getenv("OF_INPUTS_STREAM", "signals:of:inputs")  # already used in your stack
OF_INPUTS_FIELD = os.getenv("OF_INPUTS_FIELD", "payload")
# O(1) SID -> stream_id index to avoid O(N) tail scans when resolving inputs for scheduled jobs.
OF_INPUTS_SID_INDEX_PREFIX = os.getenv("OF_INPUTS_SID_INDEX_PREFIX", "idx:of_inputs:sid:")
OF_INPUTS_SID_INDEX_TTL_SEC = int(os.getenv("OF_INPUTS_SID_INDEX_TTL_SEC", "172800"))  # 2d
OF_INPUTS_SCAN_COUNT = int(os.getenv("OF_INPUTS_SCAN_COUNT", "2000"))

TB_JOBS_ZSET = os.getenv("TB_JOBS_ZSET", "tb:jobs:due")
TB_JOB_KEY_PREFIX = os.getenv("TB_JOB_KEY_PREFIX", "tb:job:")
TB_LABELS_STREAM = os.getenv("TB_LABELS_STREAM", "labels:tb")

TB_TICK_STREAM_PREFIX = os.getenv("TB_TICK_STREAM_PREFIX", "stream:tick_")

# Horizons: keep multi-horizons to improve training accuracy
TB_HORIZONS_MS = os.getenv("TB_HORIZONS_MS", "60000,180000,300000")
HORIZONS = sorted([int(x) for x in TB_HORIZONS_MS.split(",") if x.strip().isdigit()])
PRIMARY_H_MS = int(os.getenv("TB_PRIMARY_H_MS", "180000"))

TB_SLACK_MS = int(os.getenv("TB_SLACK_MS", "15000"))          # wait a bit after horizon end
TB_JOB_TTL_SEC = int(os.getenv("TB_JOB_TTL_SEC", "7200"))     # 2h

# Tick window sampling to avoid huge JSON in PG
TB_STORE_TICKS = int(os.getenv("TB_STORE_TICKS", "1"))
TB_TICKS_SAMPLE_EVERY = int(os.getenv("TB_TICKS_SAMPLE_EVERY", "10"))
TB_TICKS_MAX = int(os.getenv("TB_TICKS_MAX", "2000"))

# Barrier parameters
TP_K_ATR = float(os.getenv("TB_TP_K_ATR", "1.0"))
SL_K_ATR = float(os.getenv("TB_SL_K_ATR", "1.0"))
FALLBACK_TP_BPS = float(os.getenv("TB_FALLBACK_TP_BPS", "30"))
FALLBACK_SL_BPS = float(os.getenv("TB_FALLBACK_SL_BPS", "30"))

# Adverse proxy threshold for y_edge
TB_ADV_MAX = float(os.getenv("TB_ADV_MAX", "1.2"))

# Max rows to scan per symbol
TB_MAX_ROWS_PER_SYMBOL = int(os.getenv("TB_MAX_ROWS_PER_SYMBOL", "200000"))

# Done key prefix (for deduplication)
TB_DONE_KEY_PREFIX = os.getenv("TB_DONE_KEY_PREFIX", "tb:done:")

# Postgres (optional)
TB_PG_DSN = os.getenv("TB_PG_DSN", "")  # e.g. postgresql://user:pass@host:5432/db
TB_PG_ENABLE = int(os.getenv("TB_PG_ENABLE", "0"))


def now_ms() -> int:
    return get_ny_time_millis()


def _i(x: Any, d: int = 0) -> int:
    try:
        return int(float(x))
    except Exception:
        return d


def _f(x: Any, d: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return d


def _safe_loads(s: Any) -> Dict[str, Any]:
    try:
        if isinstance(s, dict):
            return s
        if s is None:
            return {}
        if isinstance(s, bytes):
            s = s.decode("utf-8", "ignore")
        s = str(s)
        if not s.strip().startswith("{"):
            return {}
        obj = json.loads(s)
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


def _merge_tick_fields(fields: Dict[str, Any]) -> Dict[str, Any]:
    merged = dict(fields)
    nested = _safe_loads(fields.get("data"))
    if nested:
        merged.update(nested)
    return merged


def _pick_tick_ts_ms(t: Dict[str, Any]) -> int:
    """Extract timestamp from tick fields."""
    ts = _i(t.get("ts", 0), 0)
    if ts <= 0:
        ts = _i(t.get("ts_ms", 0), 0)
    if ts <= 0:
        ts = _i(t.get("timestamp", 0), 0)
    return ts


def _pick_price(t: Dict[str, Any]) -> float:
    """Extract price from tick fields (mid > price > last > (bid+ask)/2)."""
    def _f(x: Any, d: float = 0.0) -> float:
        try:
            return float(x)
        except Exception:
            return d

    px = _f(t.get("mid"), 0.0)
    if px <= 0.0:
        px = _f(t.get("price"), 0.0)
    if px <= 0.0:
        px = _f(t.get("last"), 0.0)
    if px <= 0.0:
        bid = _f(t.get("bid"), 0.0)
        ask = _f(t.get("ask"), 0.0)
        if bid > 0.0 and ask > 0.0:
            px = (bid + ask) / 2.0
    return float(px)


# Barriers and barrier_stats are now imported from core.tb_labeling


def _stream_id(ms: int, seq: int) -> str:
    return f"{int(ms)}-{int(seq)}"


def fetch_ticks_window(
    r_ticks: redis.Redis
    stream: str
    start_ms: int
    end_ms: int
    max_rows: int = 250_000
) -> List[Tuple[int, float]]:
    """
    XRANGE by ID because your IDs are ms-based (<ms>-<seq>).
    """
    out: List[Tuple[int, float]] = []
    cur = _stream_id(start_ms, 0)
    end_id = _stream_id(end_ms, 999999)
    scanned = 0

    while scanned < max_rows:
        try:
            batch = r_ticks.xrange(stream, min=cur, max=end_id, count=2000)
            if not batch:
                break
            last_id = None
            for msg_id, fields in batch:
                scanned += 1
                last_id = msg_id
                if not isinstance(fields, dict):
                    continue
                t = _merge_tick_fields(fields)
                ts = _pick_tick_ts_ms(t)
                if ts <= 0 or ts < start_ms or ts > end_ms:
                    continue
                px = _pick_price(t)
                if px <= 0.0:
                    continue
                out.append((ts, px))

            if last_id is None:
                break
            if isinstance(last_id, bytes):
                last_id = last_id.decode("utf-8", "ignore")
            # advance inclusive cursor
            ms_s, seq_s = str(last_id).split("-", 1)
            cur = _stream_id(int(ms_s), int(seq_s) + 1)
        except redis.exceptions.BusyLoadingError:
            print(f"Redis-ticks is loading while fetching {stream}. Waiting 2s...")
            time.sleep(2.0)
            continue
        except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError) as e:
            print(f"Redis-ticks connection error for {stream}: {e}. Waiting 2s...")
            time.sleep(2.0)
            continue
        except Exception as e:
            print(f"Unexpected error fetching ticks for {stream}: {e}")
            break

    return out


def sample_ticks(path: List[Tuple[int, float]], every: int, max_n: int) -> Optional[List[List[float]]]:
    """Sample ticks for storage (to avoid huge JSON in PG)."""
    if not path:
        return None
    step = max(1, int(every))
    sampled = path[::step]
    if len(sampled) > max_n:
        sampled = sampled[:max_n]
    return [[float(ts), float(px)] for ts, px in sampled]


class TBLabelerWorker:
    def __init__(self) -> None:
        self.r = redis.Redis.from_url(REDIS_URL, decode_responses=True)
        self.r_ticks = redis.Redis.from_url(TICKS_REDIS_URL, decode_responses=True)

        self.pg = None
        if TB_PG_ENABLE == 1 and TB_PG_DSN and psycopg2 is not None:
            self.pg = psycopg2.connect(TB_PG_DSN)
            self.pg.autocommit = True
            # Ensure table exists (auto-migration for existing databases)
            self._ensure_table_exists()

    def _load_of_input(self, sid: str) -> Optional[Dict[str, Any]]:
        """Fetch the originating OF input by SID from signals:of:inputs.

        Fast path: GET idx:of_inputs:sid:{sid} -> stream_id, then XRANGE stream_id..stream_id.
        Fallback: bounded tail scan (XREVRANGE count=OF_INPUTS_SCAN_COUNT).
        """
        t0 = time.time()
        key = f"{OF_INPUTS_SID_INDEX_PREFIX}{sid}"
        stream_id: Optional[str] = None
        try:
            v = self.r.get(key)
            if isinstance(v, bytes):
                stream_id = v.decode()
            elif isinstance(v, str):
                stream_id = v
        except Exception:
            stream_id = None

        # index path
        if stream_id:
            try:
                # Use decode_responses=True from self.r
                msgs = self.r.xrange(OF_INPUTS_STREAM, min=stream_id, max=stream_id, count=1)
            except Exception:
                msgs = []
            if msgs:
                _id, fields = msgs[0]
                raw = fields.get(OF_INPUTS_FIELD) if isinstance(fields, dict) else None
                o = _safe_loads(raw)
                if o:
                    TB_INPUT_LOOKUP_TOTAL.labels("index").inc()
                    TB_INPUT_LOOKUP_MS.observe((time.time() - t0) * 1000.0)
                    return o
            # stale index: fallthrough to scan, but record miss
            TB_INPUT_LOOKUP_TOTAL.labels("miss").inc()

        # fallback scan
        try:
            msgs = self.r.xrevrange(OF_INPUTS_STREAM, max="+", min="-", count=OF_INPUTS_SCAN_COUNT)
        except Exception:
            TB_INPUT_LOOKUP_TOTAL.labels("err").inc()
            TB_INPUT_LOOKUP_MS.observe((time.time() - t0) * 1000.0)
            return None

        for _id, fields in msgs:
            if not isinstance(fields, dict):
                continue
            raw = fields.get(OF_INPUTS_FIELD)
            o = _safe_loads(raw)
            if not o:
                continue
            if str(o.get("sid") or "") == sid:
                TB_INPUT_LOOKUP_TOTAL.labels("scan").inc()
                TB_INPUT_LOOKUP_MS.observe((time.time() - t0) * 1000.0)
                # refresh index for next time
                try:
                    sid_key = f"{OF_INPUTS_SID_INDEX_PREFIX}{sid}"
                    sid_val = _id.decode() if isinstance(_id, bytes) else str(_id)
                    self.r.setex(sid_key, OF_INPUTS_SID_INDEX_TTL_SEC, sid_val)
                except Exception:
                    pass
                return o

        TB_INPUT_LOOKUP_TOTAL.labels("miss").inc()
        TB_INPUT_LOOKUP_MS.observe((time.time() - t0) * 1000.0)
        return None


    def _ensure_table_exists(self) -> None:
        """Ensure tb_labels table exists, create if missing (for existing databases)"""
        if not self.pg:
            return
        try:
            with self.pg.cursor() as cur:
                # Check if table exists
                cur.execute("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_schema = 'public' 
                        AND table_name = 'tb_labels'
                    );
                """)
                exists = cur.fetchone()[0]
                if not exists:
                    # Apply migration
                    migration_sql = """
                    CREATE TABLE tb_labels (
                      sid            TEXT PRIMARY KEY
                      symbol         TEXT NOT NULL
                      ts_ms          BIGINT NOT NULL
                      direction      TEXT NOT NULL
                      primary_h_ms   INTEGER NOT NULL
                      primary_label  TEXT NOT NULL
                      primary_hit_ms BIGINT NOT NULL
                      primary_ret_bps DOUBLE PRECISION NOT NULL
                      primary_r_mult  DOUBLE PRECISION NOT NULL
                      primary_y_edge  INTEGER NOT NULL
                      horizons_json  JSONB NOT NULL
                      ticks_sample   JSONB
                      meta           JSONB
                      created_ms     BIGINT NOT NULL
                    );
                    CREATE INDEX IF NOT EXISTS tb_labels_symbol_ts_idx ON tb_labels(symbol, ts_ms);
                    CREATE INDEX IF NOT EXISTS tb_labels_ts_ms_idx ON tb_labels(ts_ms DESC);
                    CREATE INDEX IF NOT EXISTS tb_labels_direction_idx ON tb_labels(direction, ts_ms DESC);
                    CREATE INDEX IF NOT EXISTS tb_labels_primary_label_idx ON tb_labels(primary_label, ts_ms DESC);
                    CREATE INDEX IF NOT EXISTS tb_labels_horizons_gin_idx ON tb_labels USING gin (horizons_json);
                    CREATE INDEX IF NOT EXISTS tb_labels_meta_gin_idx ON tb_labels USING gin (meta);
                    """
                    cur.execute(migration_sql)
                    print("✅ tb_labels table created automatically")
        except Exception as e:
            print(f"⚠️  Warning: Could not ensure tb_labels table exists: {e}")

    def _pg_upsert(self, row: Dict[str, Any]) -> None:
        if not self.pg:
            return
        q = """
        insert into tb_labels
        (sid, symbol, ts_ms, direction
         primary_h_ms, primary_label, primary_hit_ms, primary_ret_bps, primary_r_mult, primary_y_edge
         horizons_json, ticks_sample, meta, created_ms)
        values (%(sid)s, %(symbol)s, %(ts_ms)s, %(direction)s
                %(primary_h_ms)s, %(primary_label)s, %(primary_hit_ms)s, %(primary_ret_bps)s, %(primary_r_mult)s, %(primary_y_edge)s
                %(horizons_json)s::jsonb, %(ticks_sample)s::jsonb, %(meta)s::jsonb, %(created_ms)s)
        on conflict (sid) do nothing
        """
        with self.pg.cursor() as cur:
            cur.execute(q, row)

    def enqueue_job(self, inp: Dict[str, Any], msg_id: Optional[str] = None) -> None:
        sid = str(inp.get("sid", "") or msg_id or "")
        symbol = str(inp.get("symbol", "") or "").upper()
        ts_ms = _i(inp.get("ts_ms", inp.get("ts", 0)), 0)
        direction = str(inp.get("direction", "") or "").upper()
        indicators = inp.get("indicators") if isinstance(inp.get("indicators"), dict) else inp

        print(f"DEBUG: ENQUEUE: sid={sid} sym={symbol} ts={ts_ms} direction={direction}")

        if not sid or not symbol or ts_ms <= 0 or direction not in ("LONG", "SHORT"):
            print(f"DEBUG: SKIP_VALIDATION: sid={sid} sym={symbol} ts={ts_ms} direction={direction}")
            return

        done_key = TB_DONE_KEY_PREFIX + sid
        if self.r.exists(done_key):
            print(f"DEBUG: SKIP_DONE: {sid}")
            return

        # store compact job payload with TTL (so zset member is only sid)
        job_key = TB_JOB_KEY_PREFIX + sid
        job = {
            "sid": sid
            "symbol": symbol
            "ts_ms": ts_ms
            "direction": direction
            "indicators": {
                # keep minimal set
                "stop_bps": indicators.get("stop_bps", 0.0)
                "atr_bps": indicators.get("atr_bps", 0.0)
                "spread_bps": indicators.get("spread_bps", 0.0)
                "expected_slippage_bps": indicators.get("expected_slippage_bps", 0.0)
            }
        }
        self.r.set(job_key, json.dumps(job, ensure_ascii=False, separators=(",", ":")), ex=TB_JOB_TTL_SEC)

        due_ms = ts_ms + max(HORIZONS) + TB_SLACK_MS
        self.r.zadd(TB_JOBS_ZSET, {sid: float(due_ms)})

    def process_due(self, limit: int = 200) -> int:
        now = now_ms()
        due = self.r.zrangebyscore(TB_JOBS_ZSET, 0, now, start=0, num=limit)
        if not due:
            return 0

        done = 0
        for sid in due:
            job_key = TB_JOB_KEY_PREFIX + sid
            raw = self.r.get(job_key)
            if not raw:
                self.r.zrem(TB_JOBS_ZSET, sid)
                continue
            job = _safe_loads(raw)
            symbol = str(job.get("symbol", "") or "").upper()
            ts0 = _i(job.get("ts_ms", 0), 0)
            direction = str(job.get("direction", "") or "").upper()
            indicators = job.get("indicators") if isinstance(job.get("indicators"), dict) else {}

            if not symbol or ts0 <= 0 or direction not in ("LONG", "SHORT"):
                self.r.zrem(TB_JOBS_ZSET, sid)
                self.r.delete(job_key)
                continue

            if not symbol or ts0 <= 0 or direction not in ("LONG", "SHORT"):
                self.r.zrem(TB_JOBS_ZSET, sid)
                self.r.delete(job_key)
                continue

            try:
                stream = TB_TICK_STREAM_PREFIX + symbol
                end_ms = ts0 + max(HORIZONS)
                t_fetch0 = time.time()
                ticks = fetch_ticks_window(self.r_ticks, stream, ts0, end_ms, max_rows=TB_MAX_ROWS_PER_SYMBOL)
                TB_TICK_FETCH_MS.observe((time.time() - t_fetch0) * 1000.0)
                TB_TICKS_USED.observe(float(len(ticks)))


                # entry = first tick >= ts0
                entry_px = ticks[0][1] if ticks else 0.0

                b = infer_tp_sl_bps(
                    indicators
                    tp_k_atr=TP_K_ATR
                    sl_k_atr=SL_K_ATR
                    fallback_tp_bps=FALLBACK_TP_BPS
                    fallback_sl_bps=FALLBACK_SL_BPS
                )

                horizons_out: Dict[str, Any] = {}
                for h in HORIZONS:
                    horizons_out[str(h)] = barrier_stats(
                        ts0=ts0
                        direction=direction
                        entry_px=entry_px
                        path=ticks
                        b=b
                        h_ms=h
                        adv_max=TB_ADV_MAX
                    )

                primary = horizons_out.get(str(PRIMARY_H_MS), horizons_out[str(HORIZONS[len(HORIZONS)//2])])

                meta = {
                    "tp_bps": float(b.tp_bps)
                    "sl_bps": float(b.sl_bps)
                    "scale_bps": float(b.scale_bps)
                    "exec_cost_r": exec_cost_r(indicators, b.scale_bps)
                }
                meta["util_r"] = float(primary.get("r_mult", 0.0) or 0.0) - float(meta["exec_cost_r"])  # risk penalty can be added later

                payload = {
                    "sid": sid
                    "symbol": symbol
                    "ts_ms": ts0
                    "direction": direction
                    "primary_h_ms": PRIMARY_H_MS
                    "primary": primary
                    "horizons": horizons_out
                    "entry_px": float(entry_px)
                    "created_ms": now_ms()
                    "ticks_sample": sample_ticks(ticks, every=TB_TICKS_SAMPLE_EVERY, max_n=TB_TICKS_MAX) if TB_STORE_TICKS == 1 else None
                    "meta": meta
                }

                # write to Redis stream
                try:
                    self.r.xadd(TB_LABELS_STREAM, {"payload": json.dumps(payload, ensure_ascii=False, separators=(",", ":"))}, maxlen=200000, approximate=True)
                    TB_LABEL_WRITE_TOTAL.inc()
                    try:
                        self.r.set(b"tb:last_label_ts_ms", str(int(payload.get("ts_ms", 0))).encode())
                    except Exception:
                        pass
                except Exception:
                    traceback.print_exc()
                
                TB_JOBS_TOTAL.labels("ok").inc()


                # write to Postgres (optional)
                row = {
                    "sid": sid
                    "symbol": symbol
                    "ts_ms": ts0
                    "direction": direction
                    "primary_h_ms": int(PRIMARY_H_MS)
                    "primary_label": str(primary["label"])
                    "primary_hit_ms": int(primary["hit_ms"])
                    "primary_ret_bps": float(primary["ret_bps"])
                    "primary_r_mult": float(primary["r_mult"])
                    "primary_y_edge": int(primary["y_edge"])
                    "horizons_json": json.dumps(horizons_out, ensure_ascii=False, separators=(",", ":"))
                    "ticks_sample": json.dumps(payload.get("ticks_sample"), ensure_ascii=False, separators=(",", ":")) if payload.get("ticks_sample") is not None else None
                    "meta": json.dumps(meta, ensure_ascii=False, separators=(",", ":"))
                    "created_ms": int(payload["created_ms"])
                }
                try:
                    self._pg_upsert(row)
                except Exception:
                    traceback.print_exc()

                # mark done (long ttl so we don't relabel)
                try:
                    self.r.set(TB_DONE_KEY_PREFIX + sid, "1", ex=7 * 24 * 3600)
                except Exception:
                    pass

                # cleanup job
                self.r.zrem(TB_JOBS_ZSET, sid)
                self.r.delete(job_key)
                done += 1

            except Exception as e:
                print(f"ERROR: Failed to process job {sid}: {e}")
                traceback.print_exc()
                # remove bad job so we don't loop forever
                self.r.zrem(TB_JOBS_ZSET, sid)
                self.r.delete(job_key)


        return done

    def _ensure_consumer_group(self, stream: str, group: str) -> None:
        """Ensure consumer group exists, create if missing."""
        try:
            self.r.xgroup_create(stream, group, id="$", mkstream=True)
            print(f"INFO: Created consumer group {group} on {stream}")
        except redis.exceptions.ResponseError as e:
            if "BUSYGROUP" in str(e):
                # Group already exists, which is fine
                pass
            else:
                print(f"WARNING: Could not create consumer group {group}: {e}")
                # We re-raise to let the caller handle retry or fail
                raise e

    def run_forever(self) -> None:
        # Consume OF inputs and enqueue jobs
        group = os.getenv("TB_INPUTS_GROUP", "tb-labeler")
        consumer = os.getenv("TB_INPUTS_CONSUMER", "c1")
        block_ms = int(os.getenv("TB_XREAD_BLOCK_MS", "1000"))
        count = int(os.getenv("TB_XREAD_COUNT", "200"))

        print(f"DEBUG: Starting run_forever on {OF_INPUTS_STREAM} group={group}")

        # Initial creation attempt with retries
        while True:
            try:
                self._ensure_consumer_group(OF_INPUTS_STREAM, group)
                break
            except redis.exceptions.BusyLoadingError:
                print(f"Redis is loading while ensuring group on {OF_INPUTS_STREAM}. Waiting 5s...")
                time.sleep(5)
            except Exception as e:
                print(f"ERROR: Failed to ensure consumer group: {e}. Retrying in 5s...")
                time.sleep(5)

        while True:
            # 1) enqueue new signals
            try:
                resp = self.r.xreadgroup(group, consumer, {OF_INPUTS_STREAM: ">"}, count=count, block=block_ms)
                if resp:
                    for _stream, msgs in resp:
                        for msg_id, fields in msgs:
                            raw = fields.get(OF_INPUTS_FIELD)
                            inp = _safe_loads(raw)
                            sid = str(inp.get("sid") or "")
                            ts_ms = _i(inp.get("ts_ms") or inp.get("ts") or 0, 0)
                            if ts_ms <= 0:
                                try:
                                    ts_ms = int((msg_id.decode("utf-8") if isinstance(msg_id, bytes) else str(msg_id)).split("-", 1)[0])
                                except Exception:
                                    pass

                            print(f"DEBUG: Processing msg_id={msg_id} sid={sid}")
                            # update lag metric and persist last seen ts_ms
                            try:
                                now_m = get_ny_time_millis()
                                if ts_ms > 0:
                                    TB_INPUT_LAG_MS.set(max(0.0, float(now_m - int(ts_ms))))
                                    self.r.set(b"tb:last_ts_ms", str(int(ts_ms)).encode())
                            except Exception:
                                pass

                            # write SID index for O(1) lookup later
                            if sid:
                                try:
                                    sid_key = f"{OF_INPUTS_SID_INDEX_PREFIX}{sid}"
                                    sid_val = msg_id.decode() if isinstance(msg_id, bytes) else str(msg_id)
                                    self.r.setex(sid_key, OF_INPUTS_SID_INDEX_TTL_SEC, sid_val)
                                except Exception:
                                    pass

                            # expect sid/symbol/ts_ms/direction/indicators in payload
                            # fallback to msg_id if sid is missing
                            self.enqueue_job(inp, msg_id=msg_id)
                            self.r.xack(OF_INPUTS_STREAM, group, msg_id)
            except redis.exceptions.ResponseError as e:
                if "NOGROUP" in str(e):
                    print(f"WARNING: Group {group} missing (NOGROUP error), recreating...")
                    try:
                        self._ensure_consumer_group(OF_INPUTS_STREAM, group)
                    except Exception:
                        pass
                    continue
                else:
                    print("ERROR: In run_forever consumer loop (ResponseError):")
                    traceback.print_exc()
            except (redis.exceptions.BusyLoadingError, redis.exceptions.ConnectionError, redis.exceptions.TimeoutError) as e:
                print(f"Redis transient error in run_forever: {e}. Waiting 5s...")
                time.sleep(5.0)
                continue
            except Exception as e:
                # Check for "LOADING" in string representation if not caught by explicit exception
                if "LOADING" in str(e).upper():
                    print(f"Redis is loading (detected via message): {e}. Waiting 5s...")
                    time.sleep(5.0)
                    continue
                print("ERROR: In run_forever consumer loop:")
                traceback.print_exc()


            # 2) process due
            try:
                self.process_due(limit=200)
            except Exception:
                print("ERROR: In run_forever process_due:")
                traceback.print_exc()


            time.sleep(0.05)


def main() -> None:
    # Optional Prometheus metrics server (separate process)
    if _METRICS_ENABLE and start_http_server is not None:
        try:
            port = int(os.getenv("TB_METRICS_PORT", "9112"))
            addr = os.getenv("TB_METRICS_ADDR", "0.0.0.0")
            start_http_server(port, addr=addr)
        except Exception:
            pass
    TBLabelerWorker().run_forever()


if __name__ == "__main__":
    main()

