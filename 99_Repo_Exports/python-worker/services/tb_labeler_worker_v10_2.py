# python-worker/services/tb_labeler_worker_v10_2.py

from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import json
import os
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import redis  # runtime dependency

from core.tb_labeling import infer_tp_sl_bps

try:
    from prometheus_client import Counter, Gauge, Histogram, start_http_server  # type: ignore
except Exception:  # pragma: no cover
    Counter = Gauge = Histogram = start_http_server = None  # type: ignore


# ----------------------------
# Config
# ----------------------------
REDIS_URL = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
TICKS_REDIS_URL = os.getenv("TICKS_REDIS_URL", "redis://redis-ticks:6379/0")

OF_INPUTS_STREAM = os.getenv("OF_INPUTS_STREAM", "signals:of:inputs")
OF_INPUTS_FIELD = os.getenv("OF_INPUTS_FIELD", "payload")

OF_INPUTS_GROUP = os.getenv("OF_INPUTS_GROUP", "tb_labeler_v2")
OF_INPUTS_CONSUMER = os.getenv("OF_INPUTS_CONSUMER", "tb_labeler_01")
OF_INPUTS_BLOCK_MS = int(os.getenv("OF_INPUTS_BLOCK_MS", "2000"))
OF_INPUTS_COUNT = int(os.getenv("OF_INPUTS_COUNT", "200"))

# Pending recovery
OF_INPUTS_CLAIM_ENABLE = os.getenv("OF_INPUTS_CLAIM_ENABLE", "1") == "1"
OF_INPUTS_CLAIM_IDLE_MS = int(os.getenv("OF_INPUTS_CLAIM_IDLE_MS", "60000"))
OF_INPUTS_CLAIM_COUNT = int(os.getenv("OF_INPUTS_CLAIM_COUNT", "200"))
OF_INPUTS_CLAIM_INTERVAL_SEC = int(os.getenv("OF_INPUTS_CLAIM_INTERVAL_SEC", "10"))

# SID index (compact mode)
SID_INDEX_MODE = os.getenv("OF_INPUTS_SID_INDEX_MODE", "keys")  # keys | hash_day
SID_INDEX_PREFIX = os.getenv("OF_INPUTS_SID_INDEX_PREFIX", "idx:of_inputs:sid:")
SID_INDEX_DAY_PREFIX = os.getenv("OF_INPUTS_SID_INDEX_DAY_PREFIX", "idx:of_inputs:sid_day:")
SID_INDEX_TTL_SEC = int(os.getenv("OF_INPUTS_SID_INDEX_TTL_SEC", "172800"))  # 48h

TB_JOBS_ZSET = os.getenv("TB_JOBS_ZSET", "tb:jobs:due")
TB_JOB_KEY_PREFIX = os.getenv("TB_JOB_KEY_PREFIX", "tb:job:")
TB_LABELS_STREAM = os.getenv("TB_LABELS_STREAM", "labels:tb")

TB_TICK_STREAM_PREFIX = os.getenv("TB_TICK_STREAM_PREFIX", "stream:tick_")

TB_HORIZONS_MS = os.getenv("TB_HORIZONS_MS", "60000,180000,300000")
HORIZONS = sorted([int(x) for x in TB_HORIZONS_MS.split(",") if x.strip().isdigit()])
PRIMARY_H_MS = int(os.getenv("TB_PRIMARY_H_MS", "180000"))

TB_SLACK_MS = int(os.getenv("TB_SLACK_MS", "15000"))
TB_JOB_TTL_SEC = int(os.getenv("TB_JOB_TTL_SEC", "7200"))

TB_STORE_TICKS = int(os.getenv("TB_STORE_TICKS", "0"))
TB_TICKS_SAMPLE_EVERY = int(os.getenv("TB_TICKS_SAMPLE_EVERY", "10"))
TB_TICKS_MAX = int(os.getenv("TB_TICKS_MAX", "2000"))

TP_K_ATR = float(os.getenv("TB_TP_K_ATR", "1.0"))
SL_K_ATR = float(os.getenv("TB_SL_K_ATR", "1.0"))
FALLBACK_TP_BPS = float(os.getenv("TB_FALLBACK_TP_BPS", "30"))
FALLBACK_SL_BPS = float(os.getenv("TB_FALLBACK_SL_BPS", "30"))

TB_ADV_MAX = float(os.getenv("TB_ADV_MAX", "1.2"))

TB_DONE_KEY_PREFIX = os.getenv("TB_DONE_KEY_PREFIX", "tb:done:")

# Health keys
TB_LAST_TS_MS_KEY = os.getenv("TB_LAST_TS_MS_KEY", "tb:last_ts_ms")
TB_LAST_LABEL_TS_MS_KEY = os.getenv("TB_LAST_LABEL_TS_MS_KEY", "tb:last_label_ts_ms")
TB_LAST_ERR_TS_MS_KEY = os.getenv("TB_LAST_ERR_TS_MS_KEY", "tb:last_err_ts_ms")

# Metrics
TB_METRICS_ENABLE = os.getenv("TB_METRICS_ENABLE", "1") == "1" and Counter is not None
TB_METRICS_PORT = int(os.getenv("TB_METRICS_PORT", "9112"))
TB_METRICS_ADDR = os.getenv("TB_METRICS_ADDR", "0.0.0.0")


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


def _normalize_direction(x: Any) -> str:
    s = str(x or "").upper()
    if s in ("BUY", "LONG", "B"):
        return "LONG"
    if s in ("SELL", "SHORT", "S"):
        return "SHORT"
    return s or "LONG"


def _stream_id(ms: int, seq: int = 0) -> str:
    return f"{ms}-{seq}"


def _parse_stream_id(id_: Any) -> Tuple[int, int]:
    if isinstance(id_, bytes):
        id_ = id_.decode("utf-8", "ignore")
    s = str(id_ or "0-0")
    try:
        a, b = s.split("-", 1)
        return int(a), int(b)
    except Exception:
        return 0, 0


def _sid_to_ts_ms(sid: str) -> int:
    # canonical: crypto-of:{SYMBOL}:{ts_ms}
    try:
        parts = sid.split(":")
        if len(parts) >= 3:
            return int(parts[-1])
    except Exception:
        return 0
    return 0


def _day_bucket_yyyymmdd(ts_ms: int) -> str:
    # use UTC day bucket
    if ts_ms <= 0:
        ts_ms = now_ms()
    t = time.gmtime(ts_ms / 1000.0)
    return f"{t.tm_year:04d}{t.tm_mon:02d}{t.tm_mday:02d}"


# ----------------------------
# Metrics
# ----------------------------
class _NoopMetric:
    def labels(self, *args: Any, **kwargs: Any) -> "_NoopMetric":
        return self

    def inc(self, n: float = 1.0) -> None:
        return

    def observe(self, v: float) -> None:
        return

    def set(self, v: float) -> None:
        return


TB_JOBS_TOTAL = Counter("tb_label_jobs_total", "TB labeler jobs processed", ["status"]) if TB_METRICS_ENABLE else _NoopMetric()
TB_INPUT_LOOKUP_TOTAL = Counter("tb_label_input_lookup_total", "OF input lookup mode", ["mode"]) if TB_METRICS_ENABLE else _NoopMetric()
TB_INPUT_LOOKUP_MS = Histogram(
    "tb_label_input_lookup_ms", "OF input lookup latency (ms)",
    buckets=(1, 2, 5, 10, 20, 50, 100, 200, 500, 1000, 2000)
) if TB_METRICS_ENABLE else _NoopMetric()
TB_TICK_FETCH_MS = Histogram(
    "tb_label_tick_fetch_ms", "Tick fetch latency (ms)",
    buckets=(1, 2, 5, 10, 20, 50, 100, 200, 500, 1000, 2000, 5000)
) if TB_METRICS_ENABLE else _NoopMetric()
TB_TICKS_USED = Histogram(
    "tb_label_ticks_used", "Ticks used per label job",
    buckets=(50, 100, 200, 500, 1000, 2000, 5000, 10000, 50000)
) if TB_METRICS_ENABLE else _NoopMetric()
TB_LABEL_WRITE_TOTAL = Counter("tb_label_write_total", "TB labels written") if TB_METRICS_ENABLE else _NoopMetric()

TB_GROUP_PENDING = Gauge("tb_of_inputs_group_pending", "OF inputs consumer group pending") if TB_METRICS_ENABLE else _NoopMetric()
TB_GROUP_LAG_MS = Gauge("tb_of_inputs_group_lag_ms", "Approx lag between stream head and group last-delivered (ms)") if TB_METRICS_ENABLE else _NoopMetric()
TB_GROUP_CLAIM_TOTAL = Counter("tb_of_inputs_claim_total", "Claimed pending OF inputs", ["mode"]) if TB_METRICS_ENABLE else _NoopMetric()


# ----------------------------
# TB Labeling
# ----------------------------
def sample_ticks(path: List[Tuple[int, float]], every: int, max_n: int) -> Optional[List[List[float]]]:
    if not path:
        return None
    out: List[List[float]] = []
    for i, (ts, px) in enumerate(path):
        if i % max(1, every) != 0:
            continue
        out.append([int(ts), float(px)])
        if len(out) >= max_n:
            break
    return out


def _signed_ret_bps(entry_px: float, px: float, side: str) -> float:
    if entry_px <= 0:
        return 0.0
    ret = (px / entry_px - 1.0) * 10000.0
    return ret if side == "LONG" else -ret


@dataclass
class TBResult:
    y_edge: int
    hit_ms: int
    ret_bps: float
    r_mult: float
    util_r: float
    exec_cost_r: float
    adv_r: float
    ticks_used: int
    side: str


def eval_barrier(path: List[Tuple[int, float]], entry_ts_ms: int, horizon_ms: int, side: str,
                tp_bps: float, sl_bps: float, adv_max: float,
                spread_bps: float, slip_bps: float, exec_cost_mult: float = 1.0) -> TBResult:
    side = _normalize_direction(side)
    if not path:
        return TBResult(0, 0, 0.0, 0.0, 0.0, 0.0, 0.0, 0, side)

    entry_px = path[0][1]
    end_ts = entry_ts_ms + horizon_ms
    hit_ms = 0
    ret_bps = 0.0
    adv_r = 0.0
    ticks_used = 0

    # walk path, find first barrier hit
    for ts, px in path:
        ticks_used += 1
        if ts < entry_ts_ms:
            continue
        signed = _signed_ret_bps(entry_px, px, side)
        ret_bps = signed
        # adverse proxy: how much price moved against direction in R units relative to sl
        if sl_bps > 0:
            # adverse in bps = -signed_ret if negative
            adv_bps = max(0.0, -signed)
            adv_r = max(adv_r, adv_bps / sl_bps)

        if signed >= tp_bps:
            hit_ms = ts
            break
        if signed <= -sl_bps:
            hit_ms = ts
            break
        if ts >= end_ts:
            hit_ms = ts
            break

    hit_ms = int(hit_ms) if hit_ms else int(end_ts)
    # y_edge: 1 if tp hit first and adverse proxy not too large
    y_edge = 1 if ret_bps >= tp_bps and adv_r <= adv_max else 0

    # R multiple using sl
    r_mult = (ret_bps / sl_bps) if sl_bps > 0 else 0.0

    # Execution cost in R
    # ecr = exec_cost_r(spread_bps=spread_bps, slippage_bps=slip_bps, sl_bps=sl_bps, mult=exec_cost_mult) if sl_bps > 0 else 0.0
    # Inline to avoid signature mismatch
    ecr = (spread_bps + slip_bps) * exec_cost_mult / sl_bps if sl_bps > 0 else 0.0

    util_r = r_mult - ecr
    return TBResult(y_edge, hit_ms, float(ret_bps), float(r_mult), float(util_r), float(ecr), float(adv_r), ticks_used, side)


# ----------------------------
# Worker
# ----------------------------
class TBLabelerWorkerV10_2:
    def __init__(self) -> None:
        self.r = redis.Redis.from_url(REDIS_URL, decode_responses=True)
        self.rticks = redis.Redis.from_url(TICKS_REDIS_URL, decode_responses=True)

        self._last_claim_ts = 0

        if TB_METRICS_ENABLE and start_http_server is not None:
            start_http_server(TB_METRICS_PORT, addr=TB_METRICS_ADDR)

    def _idx_set(self, sid: str, msg_id: str, ts_ms: int) -> None:
        if not sid:
            return
        try:
            if SID_INDEX_MODE == "hash_day":
                day = _day_bucket_yyyymmdd(ts_ms or _sid_to_ts_ms(sid))
                key = f"{SID_INDEX_DAY_PREFIX}{day}"
                # value store as bytes; decode_responses False
                self.r.hset(key, sid, msg_id)
                # coarse TTL per-day bucket
                self.r.expire(key, SID_INDEX_TTL_SEC)
            else:
                key = f"{SID_INDEX_PREFIX}{sid}"
                self.r.setex(key, SID_INDEX_TTL_SEC, msg_id)
        except Exception:
            return

    def _idx_get(self, sid: str, ts_ms: int) -> Optional[str]:
        if not sid:
            return None
        try:
            if SID_INDEX_MODE == "hash_day":
                day = _day_bucket_yyyymmdd(ts_ms or _sid_to_ts_ms(sid))
                key = f"{SID_INDEX_DAY_PREFIX}{day}"
                v = self.r.hget(key, sid)
            else:
                v = self.r.get(f"{SID_INDEX_PREFIX}{sid}")
            if v is None:
                return None
            if isinstance(v, bytes):
                return v.decode("utf-8", "ignore")
            return str(v)
        except Exception:
            return None

    def _ensure_group(self, max_retries: int = 30, retry_sleep: float = 5.0) -> None:
        """Create the consumer group, retrying on Redis LOADING / connection errors."""
        for attempt in range(1, max_retries + 1):
            try:
                self.r.xgroup_create(OF_INPUTS_STREAM, OF_INPUTS_GROUP, id="0-0", mkstream=True)
                print(f"INFO: Created consumer group {OF_INPUTS_GROUP} on {OF_INPUTS_STREAM}")
                return
            except redis.exceptions.ResponseError as e:
                if "BUSYGROUP" in str(e):
                    # Group already exists — that is fine
                    print(f"INFO: Consumer group {OF_INPUTS_GROUP} already exists on {OF_INPUTS_STREAM}")
                    return
                print(f"CRITICAL: Could not create consumer group {OF_INPUTS_GROUP}: {e}")
                raise e
            except redis.exceptions.BusyLoadingError as e:
                print(f"WARN: Redis is loading (attempt {attempt}/{max_retries}): {e}. Waiting {retry_sleep}s...")
                time.sleep(retry_sleep)
            except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError) as e:
                print(f"WARN: Redis connection error in _ensure_group (attempt {attempt}/{max_retries}): {e}. Waiting {retry_sleep}s...")
                time.sleep(retry_sleep)
            except Exception as e:
                if "LOADING" in str(e).upper():
                    print(f"WARN: Redis loading detected (attempt {attempt}/{max_retries}): {e}. Waiting {retry_sleep}s...")
                    time.sleep(retry_sleep)
                else:
                    print(f"CRITICAL: Unexpected error creating group {OF_INPUTS_GROUP}: {e}")
                    raise e
        raise RuntimeError(f"Failed to ensure consumer group {OF_INPUTS_GROUP} after {max_retries} attempts")

    def _update_group_metrics(self) -> None:
        if not TB_METRICS_ENABLE:
            return
        try:
            s_info = self.r.xinfo_stream(OF_INPUTS_STREAM)
            last_id = s_info.get("last-generated-id") or s_info.get("last_generated_id") or b"0-0"
            last_ms, _ = _parse_stream_id(last_id)
            groups = self.r.xinfo_groups(OF_INPUTS_STREAM)
            for g in groups:
                name = g.get("name") if isinstance(g, dict) else None
                if isinstance(name, bytes):
                    name = name.decode("utf-8", "ignore")
                if str(name) != OF_INPUTS_GROUP:
                    continue
                pending = int(g.get("pending", 0))
                last_delivered = g.get("last-delivered-id") or g.get("last_delivered_id") or b"0-0"
                del_ms, _ = _parse_stream_id(last_delivered)
                TB_GROUP_PENDING.set(pending)
                TB_GROUP_LAG_MS.set(max(0, last_ms - del_ms))
                break
        except Exception:
            return

    def _maybe_claim_pending(self) -> None:
        if not OF_INPUTS_CLAIM_ENABLE:
            return
        now = time.time()
        if now - self._last_claim_ts < OF_INPUTS_CLAIM_INTERVAL_SEC:
            return
        self._last_claim_ts = now
        # Prefer XAUTOCLAIM (Redis >= 6.2)
        try:
            # returns (next_start_id, [ (id, {fields}), ... ], deleted_ids)
            resp = self.r.xautoclaim(
                OF_INPUTS_STREAM,
                OF_INPUTS_GROUP,
                OF_INPUTS_CONSUMER,
                min_idle_time=OF_INPUTS_CLAIM_IDLE_MS,
                start_id="0-0",
                count=OF_INPUTS_CLAIM_COUNT,
            )
            if resp and len(resp) >= 2:
                msgs = resp[1]
                if msgs:
                    TB_GROUP_CLAIM_TOTAL.labels("xautoclaim").inc(len(msgs))
                    for msg_id, fields in msgs:
                        inp = _safe_loads(fields.get(OF_INPUTS_FIELD))
                        self._on_input(inp, msg_id=msg_id, from_claim=True)
                        # ack claimed message
                        self.r.xack(OF_INPUTS_STREAM, OF_INPUTS_GROUP, msg_id)
        except Exception:
            # fallback to xpending_range + xclaim
            try:
                pend = self.r.xpending_range(
                    OF_INPUTS_STREAM,
                    OF_INPUTS_GROUP,
                    min="-",
                    max="+",
                    count=OF_INPUTS_CLAIM_COUNT,
                )
                ids: List[str] = []
                for p in pend:
                    pid = p.get("message_id") if isinstance(p, dict) else None
                    if isinstance(pid, bytes):
                        pid = pid.decode("utf-8", "ignore")
                    idle = int(p.get("time_since_delivered", 0)) if isinstance(p, dict) else 0
                    if pid and idle >= OF_INPUTS_CLAIM_IDLE_MS:
                        ids.append(str(pid))
                if ids:
                    claimed = self.r.xclaim(
                        OF_INPUTS_STREAM, OF_INPUTS_GROUP, OF_INPUTS_CONSUMER,
                        min_idle_time=OF_INPUTS_CLAIM_IDLE_MS,
                        message_ids=ids
                    )
                    TB_GROUP_CLAIM_TOTAL.labels("xclaim").inc(len(claimed))
                    for msg_id, fields in claimed:
                        inp = _safe_loads(fields.get(OF_INPUTS_FIELD))
                        self._on_input(inp, msg_id=msg_id, from_claim=True)
                        self.r.xack(OF_INPUTS_STREAM, OF_INPUTS_GROUP, msg_id)
            except Exception:
                return

    def _canonical_sid(self, inp: Dict[str, Any], msg_id: Any) -> str:
        sid = str(inp.get("sid") or "")
        symbol = str(inp.get("symbol") or inp.get("sym") or "").upper()
        ts_ms = _i(inp.get("ts_ms") or inp.get("ts") or 0, 0)
        if sid and ":" in sid:
            return sid
        if ts_ms <= 0:
            # msg_id ms
            ms, _ = _parse_stream_id(msg_id)
            ts_ms = ms
        if not symbol:
            symbol = "UNKNOWN"
        return f"crypto-of:{symbol}:{ts_ms}"

    def _on_input(self, inp: Dict[str, Any], msg_id: Any, from_claim: bool = False) -> None:
        if not inp:
            return
        # support flat payloads (OFInputsV2)
        indicators = inp.get("indicators")
        if not isinstance(indicators, dict):
            indicators = inp

        ts_ms = _i(inp.get("ts_ms") or indicators.get("ts_ms") or 0, 0)
        if ts_ms <= 0:
            try:
                ts_ms = int((msg_id.decode("utf-8") if isinstance(msg_id, bytes) else str(msg_id)).split("-", 1)[0])
            except Exception:
                pass
        sid = self._canonical_sid(inp, msg_id)
        symbol = str(inp.get("symbol") or indicators.get("symbol") or "").upper()
        direction = _normalize_direction(inp.get("direction") or indicators.get("direction") or "LONG")

        # index sid->msg_id for fast lookup (optional)
        try:
            msg_id_s = msg_id.decode("utf-8", "ignore") if isinstance(msg_id, bytes) else str(msg_id)
            self._idx_set(sid, msg_id_s, ts_ms)
        except Exception:
            pass

        # lag key for health
        try:
            if ts_ms > 0:
                self.r.set(TB_LAST_TS_MS_KEY, str(ts_ms))
        except Exception:
            pass

        # schedule jobs for horizons
        for h in HORIZONS:
            due_ms = ts_ms + h + TB_SLACK_MS
            job_id = f"{sid}:{h}"
            job_key = f"{TB_JOB_KEY_PREFIX}{job_id}"
            job = {
                "sid": sid,
                "symbol": symbol,
                "ts_ms": ts_ms,
                "direction": direction,
                "h_ms": h,
                "msg_id": msg_id_s,
            }
            try:
                self.r.setex(job_key, TB_JOB_TTL_SEC, json.dumps(job))
                self.r.zadd(TB_JOBS_ZSET, {job_id: float(due_ms)})
            except Exception:
                continue

    def _load_of_input(self, sid: str, msg_id_hint: str, ts_ms: int) -> Optional[Dict[str, Any]]:
        t0 = time.time()
        # index lookup
        try:
            idx_id = self._idx_get(sid, ts_ms)
            if idx_id:
                xr = self.r.xrange(OF_INPUTS_STREAM, min=idx_id, max=idx_id, count=1)
                if xr:
                    _, fields = xr[0]
                    inp = _safe_loads(fields.get(OF_INPUTS_FIELD))
                    TB_INPUT_LOOKUP_TOTAL.labels("index").inc()
                    TB_INPUT_LOOKUP_MS.observe((time.time() - t0) * 1000.0)
                    return inp
        except Exception:
            pass

        # direct msg_id hint
        try:
            if msg_id_hint:
                xr = self.r.xrange(OF_INPUTS_STREAM, min=msg_id_hint, max=msg_id_hint, count=1)
                if xr:
                    _, fields = xr[0]
                    inp = _safe_loads(fields.get(OF_INPUTS_FIELD))
                    TB_INPUT_LOOKUP_TOTAL.labels("msg_id").inc()
                    TB_INPUT_LOOKUP_MS.observe((time.time() - t0) * 1000.0)
                    return inp
        except Exception:
            pass

        TB_INPUT_LOOKUP_TOTAL.labels("miss").inc()
        TB_INPUT_LOOKUP_MS.observe((time.time() - t0) * 1000.0)
        return None

    def _fetch_ticks(self, symbol: str, start_ms: int, end_ms: int) -> List[Tuple[int, float]]:
        # Scan ticks stream in [start,end], pick price from field names used in your tick ingest
        stream = f"{TB_TICK_STREAM_PREFIX}{symbol}"
        cur = _stream_id(start_ms, 0)
        end_id = _stream_id(end_ms, 0)
        out: List[Tuple[int, float]] = []
        scanned = 0

        def _merge_tick_fields(fields: Dict[str, Any]) -> Dict[str, Any]:
            merged = dict(fields)
            if "data" in fields:
                nested = _safe_loads(fields.get("data"))
                if nested:
                    merged.update(nested)
            return merged

        def _pick_tick_ts_ms(t: Dict[str, Any]) -> int:
            ts = _i(t.get("ts", 0), 0)
            if ts <= 0:
                ts = _i(t.get("ts_ms", 0), 0)
            if ts <= 0:
                ts = _i(t.get("timestamp", 0), 0)
            return ts

        def _pick_price(t: Dict[str, Any]) -> float:
            px = _f(t.get("mid"), 0.0)
            if px <= 0.0:
                px = _f(t.get("price"), 0.0)
            if px <= 0.0:
                px = _f(t.get("last"), 0.0)
            if px <= 0.0:
                bid = _f(t.get("bid"), 0.0)
                ask = _f(t.get("ask"), 0.0)
                if bid > 0 and ask > 0:
                    px = (bid + ask) / 2.0
            return px

        while scanned < 250000 and len(out) < 200000:
            try:
                batch = self.rticks.xrange(stream, min=cur, max=end_id, count=2000)
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
                    if ts <= 0 or ts < start_ms:
                        continue
                    if ts > end_ms:
                        continue
                    px = _pick_price(t)
                    if px <= 0:
                        continue
                    out.append((int(ts), float(px)))
                if last_id is None:
                    break
                ms_s, seq_s = (last_id.decode("utf-8","ignore") if isinstance(last_id,bytes) else str(last_id)).split("-", 1)
                cur = _stream_id(int(ms_s), int(seq_s) + 1)
            except redis.exceptions.BusyLoadingError:
                print(f"Redis-ticks is loading while fetching {symbol}. Waiting 2s...")
                time.sleep(2.0)
                continue
            except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError) as e:
                print(f"Redis-ticks connection error for {symbol}: {e}. Waiting 2s...")
                time.sleep(2.0)
                continue
            except Exception as e:
                print(f"Unexpected error fetching ticks for {symbol}: {e}")
                break
        return out

    def process_due(self, limit: int = 200) -> None:
        now = now_ms()
        jobs = self.r.zrangebyscore(TB_JOBS_ZSET, min=0, max=now, start=0, num=limit)
        if not jobs:
            return
        for job_id_b in jobs:
            job_id_b_str = job_id_b.decode("utf-8", "ignore") if isinstance(job_id_b, bytes) else str(job_id_b)
            # print(f"Processing job {job_id_b_str}")
            if not job_id_b:
                continue
            
            # dedup
            try:
                done_key = f"{TB_DONE_KEY_PREFIX}{job_id_b_str}"
                if self.r.get(done_key):
                    self.r.zrem(TB_JOBS_ZSET, job_id_b)
                    continue

                job_key = f"{TB_JOB_KEY_PREFIX}{job_id_b_str}"
                job_data = self.r.get(job_key)
                if not job_data:
                    self.r.zrem(TB_JOBS_ZSET, job_id_b)
                    continue
                
                job = _safe_loads(job_data)
                if not job:
                    self.r.zrem(TB_JOBS_ZSET, job_id_b)
                    continue

                sid = str(job.get("sid") or "")
                symbol = str(job.get("symbol") or "").upper()
                ts_ms = _i(job.get("ts_ms"), 0)
                h_ms = _i(job.get("h_ms"), 0)
                direction = _normalize_direction(job.get("direction") or "LONG")
                msg_id_hint = str(job.get("msg_id") or "")

                inp = self._load_of_input(sid, msg_id_hint, ts_ms)
                if not inp:
                    TB_JOBS_TOTAL.labels("skip").inc()
                    self.r.set(TB_LAST_ERR_TS_MS_KEY, str(now_ms()))
                    self.r.zrem(TB_JOBS_ZSET, job_id_b)
                    continue

                indicators = inp.get("indicators")
                if not isinstance(indicators, dict):
                    indicators = inp

                # Need prices from ticks for horizon window
                start_ms = ts_ms
                end_ms = ts_ms + h_ms
                t0 = time.time()
                try:
                    path = self._fetch_ticks(symbol, start_ms, end_ms)
                except redis.exceptions.BusyLoadingError:
                    # RE-RAISE to exit loop and NOT remove from ZSET
                    raise
                except Exception as e:
                    print(f"Error fetching ticks for {symbol}: {e}")
                    path = []
                    
                TB_TICK_FETCH_MS.observe((time.time() - t0) * 1000.0)
                TB_TICKS_USED.observe(len(path))

                if not path:
                    TB_JOBS_TOTAL.labels("skip").inc()
                    self.r.set(TB_LAST_ERR_TS_MS_KEY, str(now_ms()))
                    self.r.zrem(TB_JOBS_ZSET, job_id_b)
                    continue

                # derive barriers
                # atr_bps/stop_bps extraction redundant as they are extracted in infer_tp_sl_bps
                
                barriers = infer_tp_sl_bps(
                    indicators,
                    tp_k_atr=TP_K_ATR,
                    sl_k_atr=SL_K_ATR,
                    fallback_tp_bps=FALLBACK_TP_BPS,
                    fallback_sl_bps=FALLBACK_SL_BPS,
                )
                tp_bps = barriers.tp_bps
                sl_bps = barriers.sl_bps
                
                spread_bps = _f(indicators.get("spread_bps") or 0.0, 0.0)
                slip_bps = _f(indicators.get("expected_slippage_bps") or indicators.get("slippage_bps") or 0.0, 0.0)

                res = eval_barrier(
                    path=path,
                    entry_ts_ms=ts_ms,
                    horizon_ms=h_ms,
                    side=direction,
                    tp_bps=tp_bps,
                    sl_bps=sl_bps,
                    adv_max=TB_ADV_MAX,
                    spread_bps=spread_bps,
                    slip_bps=slip_bps,
                    exec_cost_mult=1.0,
                )

                # Build payload for labels stream
                primary = (h_ms == PRIMARY_H_MS)
                payload = {
                    "sid": sid,
                    "symbol": symbol,
                    "ts_ms": ts_ms,
                    "direction": direction,
                    "h_ms": h_ms,
                    "primary": 1 if primary else 0,
                    "y_edge": res.y_edge,
                    "hit_ms": res.hit_ms,
                    "ret_bps": res.ret_bps,
                    "r_mult": res.r_mult,
                    "util_r": res.util_r,
                    "exec_cost_r": res.exec_cost_r,
                    "adv_r": res.adv_r,
                    "tp_bps": tp_bps,
                    "sl_bps": sl_bps,
                    "ticks_used": res.ticks_used,
                }
                if TB_STORE_TICKS:
                    payload["ticks"] = sample_ticks(path, TB_TICKS_SAMPLE_EVERY, TB_TICKS_MAX)

                try:
                    self.r.xadd(TB_LABELS_STREAM, {"payload": json.dumps(payload)}, maxlen=200000, approximate=True)
                    TB_LABEL_WRITE_TOTAL.inc()
                    self.r.set(TB_LAST_LABEL_TS_MS_KEY, str(now_ms()))
                    self.r.setex(done_key, 86400, "1")
                    TB_JOBS_TOTAL.labels("ok").inc()
                except redis.exceptions.BusyLoadingError:
                    raise
                except Exception as e:
                    print(f"Error writing label for {sid}: {e}")
                    TB_JOBS_TOTAL.labels("err").inc()
                    self.r.set(TB_LAST_ERR_TS_MS_KEY, str(now_ms()))
                
                # Success -> remove from ZSET
                self.r.zrem(TB_JOBS_ZSET, job_id_b)

            except redis.exceptions.BusyLoadingError:
                # RE-RAISE to let outer loop handle it (stop batch, wait)
                raise
            except Exception as e:
                 print(f"CRITICAL: Error processing job {job_id_b_str}: {e}")
                 import traceback
                 traceback.print_exc()
                 # Remove if it's a non-retriable error to avoid infinite loop
                 try:
                    self.r.zrem(TB_JOBS_ZSET, job_id_b)
                 except Exception:
                    pass

    def run_forever(self) -> None:
        self._ensure_group()
        print(f"TBLabelerWorker v10.2 consuming {OF_INPUTS_STREAM} group={OF_INPUTS_GROUP} consumer={OF_INPUTS_CONSUMER}")

        while True:
            try:
                # recover pending
                try:
                    self._maybe_claim_pending()
                except redis.exceptions.BusyLoadingError:
                    raise
                except Exception:
                    pass

                # ingest new messages
                try:
                    resp = self.r.xreadgroup(
                        OF_INPUTS_GROUP,
                        OF_INPUTS_CONSUMER,
                        {OF_INPUTS_STREAM: ">"},
                        count=OF_INPUTS_COUNT,
                        block=OF_INPUTS_BLOCK_MS,
                    )
                    if resp:
                        for _stream, msgs in resp:
                            for msg_id, fields in msgs:
                                inp = _safe_loads(fields.get(OF_INPUTS_FIELD))
                                # print(f"Ingesting msg_id={msg_id} payload={json.dumps(inp)}")
                                self._on_input(inp, msg_id=msg_id)
                                try:
                                    self.r.xack(OF_INPUTS_STREAM, OF_INPUTS_GROUP, msg_id)
                                except redis.exceptions.BusyLoadingError:
                                    raise
                                except Exception:
                                    pass
                except redis.exceptions.BusyLoadingError:
                    raise
                except Exception:
                    pass

                # due processing
                try:
                    self.process_due(limit=200)
                except redis.exceptions.BusyLoadingError:
                    raise
                except Exception:
                    pass

                # group health metrics
                try:
                    self._update_group_metrics()
                except redis.exceptions.BusyLoadingError:
                    raise
                except Exception:
                    pass

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
                print(f"Unexpected error in run_forever: {e}")
                time.sleep(1.0)

            time.sleep(0.05)


def main() -> None:
    TBLabelerWorkerV10_2().run_forever()


if __name__ == "__main__":
    main()
