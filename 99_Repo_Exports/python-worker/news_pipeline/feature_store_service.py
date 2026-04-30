from __future__ import annotations

import logging
import json
import time
from typing import Dict, Tuple, List, Optional, Any

import redis

from .models import NewsAnalysisCompact
from .redis_streams import ensure_group, xreadgroup_block, xack
from .utils import now_ms, safe_float, safe_int
from . import config
from .grade import (
    compute_grade_id
    compute_horizon_sec
    compute_horizon_sec_with_grade
)

from common.redis_errors import (
    is_transient_error as is_transient_redis_error
    get_redis_error_category
)

try:
    from common.metrics2 import NoopMetrics
except Exception:  # pragma: no cover
    NoopMetrics = None  # type: ignore

try:
    from common.dlq_sanitize import safe_json_dumps, truncate_message
except Exception:  # pragma: no cover
    safe_json_dumps = None  # type: ignore
    truncate_message = None  # type: ignore


log = logging.getLogger("news-feature-store")


def _agg_key_global() -> str:
    return "news:agg:global"


def _agg_key_symbol(symbol: str) -> str:
    return f"news:agg:{symbol}"


def _safe_json_dumps(d: Dict[str, Any], limit: int = 4096) -> str:
    try:
        s = json.dumps(d, ensure_ascii=False, separators=(",", ":"))
        return s[:limit]
    except Exception:
        return "{}"


def _dlq_stream() -> str:
    return str(getattr(config, "NEWS_FEATURE_DLQ_STREAM", getattr(config, "NEWS_ANALYSIS_DLQ", "news:analysis:dlq")))


def _update_ewma(prev: float, x: float, alpha: float) -> float:
    return (1.0 - alpha) * prev + alpha * x


def _read_prev_state(prev: Dict[str, Any]) -> Tuple[int, float, float, int]:
    """
    Read previous EWMA state from Redis HASH.
    We keep compatibility with both *_ema and *_ewma field names.
    """
    prev_ts = safe_int(prev.get("ts_ms") or prev.get("asof_ts_ms"), 0)
    prev_risk = safe_float(prev.get("risk_ewma") or prev.get("risk_ema"), 0.0)
    prev_sur = safe_float(prev.get("surprise_ewma") or prev.get("surprise_ema"), 0.0)
    prev_grade = safe_int(prev.get("news_grade_id"), 0)
    return prev_ts, prev_risk, prev_sur, prev_grade


def _read_prev_grade_change_ts(prev: Dict[str, Any], *, prev_ts_ms: int) -> int:
    """
    grade_change_ts_ms is used for cooldown anti-flap.
    If missing, fall back to prev_ts_ms.
    """
    gts = safe_int(prev.get("grade_change_ts_ms"), 0)
    if gts > 0:
        return gts
    return int(prev_ts_ms or 0)


def _apply_grade_cooldown(
    *
    prev_grade_id: int
    prev_change_ts_ms: int
    new_grade_id: int
    now_ts_ms: int
    cooldown_up_sec: int
    cooldown_down_sec: int
) -> Tuple[int, int, bool]:
    """
    Store-level hysteresis:
      - upgrades need cooldown_up_sec since last change
      - downgrades need cooldown_down_sec since last change
    Returns (final_grade_id, final_change_ts_ms, frozen_flag)
    """
    try:
        prev_g = int(prev_grade_id)
        new_g = int(new_grade_id)
        if new_g == prev_g:
            return prev_g, int(prev_change_ts_ms or now_ts_ms), False

        dt_ms = int(now_ts_ms) - int(prev_change_ts_ms or 0)
        if dt_ms < 0:
            dt_ms = 0

        if new_g > prev_g:
            need_ms = max(0, int(cooldown_up_sec)) * 1000
            if need_ms > 0 and dt_ms < need_ms:
                return prev_g, int(prev_change_ts_ms or now_ts_ms), True
            return new_g, int(now_ts_ms), False

        # new_g < prev_g
        need_ms = max(0, int(cooldown_down_sec)) * 1000
        if need_ms > 0 and dt_ms < need_ms:
            return prev_g, int(prev_change_ts_ms or now_ts_ms), True
        return new_g, int(now_ts_ms), False
    except Exception:
        # fail-open: accept new grade
        return int(new_grade_id), int(now_ts_ms), False


def _read_int(v: Any, default: int = 0) -> int:
    try:
        if v is None:
            return int(default)
        if isinstance(v, (int, float)):
            return int(v)
        s = str(v).strip()
        if not s:
            return int(default)
        return int(float(s))
    except Exception:
        return int(default)


def _analysis_ts_ms(a: Any) -> int:
    """
    Best-effort timestamp extraction for NewsAnalysisCompact.
    Supported candidates (depending on producer version):
      - a.ts_ms
      - a.asof_ts_ms
      - a.analysis_ts_ms
      - a.created_ts_ms
      - a.published_ts_ms
    Returns 0 if unavailable.
    """
    for name in ("ts_ms", "asof_ts_ms", "analysis_ts_ms", "created_ts_ms", "published_ts_ms"):
        try:
            v = getattr(a, name, None)
            ts = _read_int(v, 0)
            if ts > 0:
                return ts
        except Exception:
            continue
    return 0


def _ewma_alpha(dt_sec: float, halflife_sec: float) -> float:
    # alpha = 1 - 0.5^(dt/halflife)
    if halflife_sec <= 1e-6:
        return 1.0
    if dt_sec <= 0:
        return 1.0
    return 1.0 - (0.5 ** (dt_sec / halflife_sec))


def _update_ewma(prev: float, x: float, alpha: float) -> float:
    return (1.0 - alpha) * prev + alpha * x


def _read_prev_state(prev: Dict[str, Any]) -> Tuple[int, float, float, int]:
    """
    Read previous EWMA state from Redis HASH.
    Compatibility: support both *_ema and *_ewma.
    """
    prev_ts = safe_int(prev.get("ts_ms") or prev.get("asof_ts_ms"), 0)
    prev_risk = safe_float(prev.get("risk_ewma") or prev.get("risk_ema"), 0.0)
    prev_sur = safe_float(prev.get("surprise_ewma") or prev.get("surprise_ema"), 0.0)
    prev_grade = safe_int(prev.get("news_grade_id"), 0)
    return prev_ts, prev_risk, prev_sur, prev_grade


def _read_prev_grade_change_ts(prev: Dict[str, Any], *, prev_ts_ms: int) -> int:
    gts = safe_int(prev.get("grade_change_ts_ms"), 0)
    if gts > 0:
        return gts
    return int(prev_ts_ms or 0)


def _apply_grade_cooldown(
    *
    prev_grade_id: int
    prev_change_ts_ms: int
    new_grade_id: int
    now_ts_ms: int
    cooldown_up_sec: int
    cooldown_down_sec: int
) -> Tuple[int, int, bool]:
    """
    Store-level anti-flap:
      - grade up requires cooldown_up_sec since last change
      - grade down requires cooldown_down_sec since last change
    Returns (final_grade_id, change_ts_ms, frozen_flag).
    """
    try:
        pg = int(prev_grade_id)
        ng = int(new_grade_id)
        if ng == pg:
            return pg, int(prev_change_ts_ms or now_ts_ms), False
        dt_ms = int(now_ts_ms) - int(prev_change_ts_ms or 0)
        if dt_ms < 0:
            dt_ms = 0
        if ng > pg:
            need = max(0, int(cooldown_up_sec)) * 1000
            if need > 0 and dt_ms < need:
                return pg, int(prev_change_ts_ms or now_ts_ms), True
            return ng, int(now_ts_ms), False
        # ng < pg
        need = max(0, int(cooldown_down_sec)) * 1000
        if need > 0 and dt_ms < need:
            return pg, int(prev_change_ts_ms or now_ts_ms), True
        return ng, int(now_ts_ms), False
    except Exception:
        return int(new_grade_id), int(now_ts_ms), False


def _read_prev_grade_change_ts(prev: Dict[str, Any], prev_ts_ms: int) -> int:
    # New canonical field
    t = safe_int(prev.get("grade_change_ts_ms"), 0)
    if t <= 0:
        # fallback aliases (if any older experiments existed)
        t = safe_int(prev.get("last_grade_change_ts_ms"), 0)
    return int(t if t > 0 else prev_ts_ms)


def _apply_grade_cooldown(
    *
    prev_grade_id: int
    prev_change_ts_ms: int
    new_grade_id: int
    now_ts_ms: int
    cooldown_up_sec: int
    cooldown_down_sec: int
) -> Tuple[int, int, bool]:
    """
    Anti-flap for discrete grade.

    Semantics:
      - If grade increases: allow only if (now - prev_change_ts) >= cooldown_up_sec
      - If grade decreases: allow only if (now - prev_change_ts) >= cooldown_down_sec
      - If prev_change_ts_ms is missing/invalid -> allow immediately (bootstrap).

    Returns:
      (effective_grade_id, effective_change_ts_ms, changed_flag)
    """
    pg = int(prev_grade_id or 0)
    ng = int(new_grade_id or 0)
    if ng < 0:
        ng = 0
    if ng > 4:
        ng = 4
    if pg < 0:
        pg = 0
    if pg > 4:
        pg = 4

    if ng == pg:
        # no change; keep the prior change timestamp intact
        return pg, int(prev_change_ts_ms or 0), False

    # If we have no timestamp, we allow update immediately (first write / corrupted state).
    if prev_change_ts_ms <= 0:
        return ng, int(now_ts_ms), False

    dt_sec = max(0.0, (now_ts_ms - prev_change_ts_ms) / 1000.0)
    if ng > pg:
        if dt_sec >= float(max(0, cooldown_up_sec)):
            return ng, int(now_ts_ms), False
        # frozen: change blocked by cooldown
        return pg, int(prev_change_ts_ms), True

    # ng < pg
    if dt_sec >= float(max(0, cooldown_down_sec)):
        return ng, int(now_ts_ms), False
    # frozen: change blocked by cooldown
    return pg, int(prev_change_ts_ms), True


def _safe_preview_fields(fields: Dict[str, Any], *, limit: int = 2048) -> str:
    """
    Best-effort compact snapshot for DLQ debugging.
    Avoid large payloads; keep deterministic & decode_responses-safe.
    """
    try:
        raw = json.dumps(fields, ensure_ascii=False, default=str)
        if len(raw) > limit:
            return raw[:limit] + "…"
        return raw
    except Exception:
        return "<unserializable>"


def _safe_json(fields: Dict[str, Any], *, limit: int = 4096) -> str:
    try:
        s = json.dumps(fields, ensure_ascii=False, separators=(",", ":"))
        return s[:limit]
    except Exception:
        return "{}"


def _dir_tag(prev_grade: int, new_grade: int) -> str:
    if int(new_grade) > int(prev_grade):
        return "up"
    if int(new_grade) < int(prev_grade):
        return "down"
    return "flat"


def _compute_grade_and_horizon(
    *
    risk_ewma: float
    surprise_ewma: float
    confidence: float
    primary_tag_id: int
    tags_mask: int
) -> Tuple[int, int]:
    """
    grade_id: 0..4
    horizon_sec: 0..(72h cap inside compute_horizon_sec_with_grade)
    """
    grade_id = compute_grade_id(
        risk=float(risk_ewma)
        surprise=float(surprise_ewma)
        confidence=float(confidence)
    )

    # grade=0 => ignore semantics => horizon=0
    if grade_id <= 0:
        return 0, 0

    base_h = compute_horizon_sec(primary_tag_id=int(primary_tag_id), tags_mask=int(tags_mask))
    horizon = compute_horizon_sec_with_grade(base_horizon_sec=int(base_h), grade_id=int(grade_id))
    if horizon < 0:
        horizon = 0
    return int(grade_id), int(horizon)


def _clamp(x: float, lo: float, hi: float) -> float:
    if x < lo:
        return lo
    if x > hi:
        return hi
    return x


def _as_text(v: Any) -> str:
    """
    Redis HASH values can be bytes if decode_responses=False.
    Keep this helper fail-open.
    """
    if v is None:
        return ""
    if isinstance(v, (bytes, bytearray, memoryview)):
        try:
            return bytes(v).decode("utf-8", errors="ignore")
        except Exception:
            return ""
    try:
        return str(v)
    except Exception:
        return ""


def _prev_ts_ms(prev: Dict[str, Any]) -> int:
    """
    Backward/forward compatible timestamp getter.

    Historical mismatch bug:
      - ранее читали prev["ts_ms"], но писали "asof_ts_ms"
      - из-за этого dt=0 -> alpha=1 постоянно, EWMA деградировала в "последнее значение"
    """
    # prefer canonical key
    ts = safe_int(prev.get("ts_ms"), 0)
    if ts > 0:
        return ts
    # compatibility: older versions might use asof_ts_ms
    ts = safe_int(prev.get("asof_ts_ms"), 0)
    if ts > 0:
        return ts
    return 0


def _prev_ema(prev: Dict[str, Any], key_new: str, key_old: str) -> float:
    """
    Backward compatible EMA getter (risk_ema/surprise_ema vs legacy *_ewma).
    """
    v = safe_float(prev.get(key_new), 0.0)
    if v != 0.0:
        return v
    return safe_float(prev.get(key_old), 0.0)


class NewsFeatureStoreService:
    """
    ConsumerGroup:
      - читает news:analysis
      - обновляет:
          news:agg:global
          news:agg:<symbol>
    Хранилище — Redis HASH, только компактные поля.
    """

    def __init__(
        self
        r: redis.Redis
        consumer: str = "fs-1"
        block_ms: int = 5000
        batch: int = 50
        *
        retry_attempts: Optional[int] = None
        retry_sleep_ms: Optional[int] = None
        metrics: Optional[Any] = None
    ) -> None:
        self.r = r
        self.consumer = consumer
        self.block_ms = block_ms
        self.batch = batch
        self._stop = False
        self._retry_attempts = int(retry_attempts if retry_attempts is not None else getattr(config, "NEWS_REDIS_RETRY_ATTEMPTS", 2))
        self._retry_sleep_ms = int(retry_sleep_ms if retry_sleep_ms is not None else getattr(config, "NEWS_REDIS_RETRY_SLEEP_MS", 25))

        if metrics is not None:
            self._metrics = metrics
        else:
            self._metrics = NoopMetrics() if NoopMetrics is not None else None

    def _m_inc(self, name: str, value: int = 1, tags: Optional[dict[str, Any]] = None) -> None:
        m = self._metrics
        if not m:
            return
        try:
            m.inc(name, value, tags=tags)
        except Exception:
            return

    def _m_observe(self, name: str, value: float, tags: Optional[dict[str, Any]] = None) -> None:
        m = self._metrics
        if not m:
            return
        try:
            m.observe(name, float(value), tags=tags)
        except Exception:
            return

    def _target_keys(self, a: NewsAnalysisCompact) -> List[Tuple[str, str]]:
        # Scope tag is low-cardinality: global|symbol
        targets: List[Tuple[str, str]] = [("global", _agg_key_global())]
        for s in (a.symbols or []):
            if s:
                targets.append(("symbol", _agg_key_symbol(str(s))))
        return targets

    def _dlq_allow(self, *, now_ts_ms: Optional[int] = None) -> bool:
        """
        Rate-limit DLQ: NEWS_FEATURE_DLQ_MAX_PER_MIN per consumer.
        Implementation: INCR key with EXPIRE 70s.
        """
        try:
            limit = int(getattr(config, "NEWS_FEATURE_DLQ_MAX_PER_MIN", 120))
            if limit <= 0:
                return True
            ts = now_ts_ms if now_ts_ms is not None else now_ms()
            bucket = int(ts // 60000)
            k = f"news:dlq:rate:{self.consumer}:{bucket}"
            v = int(self.r.incr(k))
            if v == 1:
                self.r.expire(k, 70)
            return v <= limit
        except Exception:
            return True  # fail-open

    def _dlq_add(self, *, src_msg_id: str, a: Optional[NewsAnalysisCompact], fields: Dict[str, Any], err: BaseException) -> None:
        try:
            if not self._dlq_allow():
                self._m_inc("news_feature_store_dlq_dropped_total", 1, tags=None)
                return
            stream = str(getattr(config, "NEWS_FEATURE_DLQ_STREAM", getattr(config, "NEWS_ANALYSIS_DLQ", "news:analysis:dlq")))
            maxlen = int(getattr(config, "NEWS_DLQ_MAXLEN", 200000))
            now = now_ms()
            cat = get_redis_error_category(err)
            try:
                payload_json = safe_json_dumps(fields) if safe_json_dumps else json.dumps(fields, ensure_ascii=False)
                if truncate_message:
                    payload_json = truncate_message(payload_json, 16_000)
            except Exception:
                payload_json = "{}"
            uid = ""
            try:
                if a is not None:
                    uid = str(getattr(a, "uid", "") or "")
                if not uid:
                    uid = str(fields.get("uid") or "")
            except Exception:
                uid = ""
            self.r.xadd(
                stream
                {
                    "ts_ms": str(int(now))
                    "src_stream": str(getattr(config, "NEWS_ANALYSIS_STREAM", "news:analysis"))
                    "src_msg_id": str(src_msg_id)
                    "consumer": str(self.consumer)
                    "uid": str(uid)
                    "err_category": str(cat)
                    "err_type": str(type(err).__name__)
                    "err": str(err)[:512]
                    "fields_json": payload_json
                }
                maxlen=maxlen
                approximate=True
            )
            self._m_inc("news_feature_store_dlq_total", 1, tags={"cat": str(cat)})
        except Exception:
            return  # fail-open

    def _target_keys(self, a: NewsAnalysisCompact) -> List[Tuple[str, str]]:
        targets: List[Tuple[str, str]] = [("global", _agg_key_global())]
        for s in (a.symbols or []):
            if s:
                targets.append(("symbol", _agg_key_symbol(str(s))))
        return targets

    def _dlq_add(self, *, src_msg_id: str, uid: str, fields: Dict[str, Any], err: BaseException) -> None:
        if not self._dlq_allow(now_ts_ms=now_ms()):
            self._m_inc("news_feature_store_dlq_dropped_total", 1, tags=None)
            return
        stream = str(getattr(config, "NEWS_FEATURE_DLQ_STREAM", "news:analysis:dlq"))
        maxlen = int(getattr(config, "NEWS_FEATURE_DLQ_MAXLEN", 200000))
        now = now_ms()
        cat = get_redis_error_category(err)
        try:
            payload_json = safe_json_dumps(fields) if safe_json_dumps else json.dumps(fields, ensure_ascii=False)
            if truncate_message:
                payload_json = truncate_message(payload_json, 16_000)
        except Exception:
            payload_json = "{}"
        try:
            self.r.xadd(
                stream
                {
                    "ts_ms": str(int(now))
                    "src_stream": str(getattr(config, "NEWS_ANALYSIS_STREAM", "news:analysis"))
                    "src_msg_id": str(src_msg_id)
                    "consumer": str(self.consumer)
                    "uid": str(uid or "")
                    "err_category": str(cat)
                    "err_type": str(type(err).__name__)
                    "err": str(err)[:512]
                    "fields_json": payload_json
                }
                maxlen=maxlen
                approximate=True
            )
            self._m_inc("news_feature_store_dlq_total", 1, tags={"cat": str(cat)})
        except Exception:
            return

    def _inc(self, name: str, value: int = 1, tags: Optional[dict[str, Any]] = None) -> None:
        """
        Fail-open metrics increment.
        Low-cardinality tags only (grade 0..4, result, category, scope, dir).
        """
        m = getattr(self, "_metrics", None)
        if not m:
            return
        try:
            m.inc(str(name), int(value), tags if isinstance(tags, dict) else None)
        except Exception:
            return

    def _observe(self, name: str, value: float, tags: Optional[dict[str, Any]] = None) -> None:
        """
        Fail-open metrics observe.
        Low-cardinality tags only.
        """
        m = getattr(self, "_metrics", None)
        if not m:
            return
        try:
            m.observe(str(name), float(value), tags if isinstance(tags, dict) else None)
        except Exception:
            return

    def stop(self) -> None:
        self._stop = True

    def _push_dlq(
        self
        *
        src_msg_id: str
        fields: Dict[str, Any]
        uid: str
        err: BaseException
        category: Optional[str] = None
        now_ts_ms: Optional[int] = None
    ) -> None:
        """
        Best-effort DLQ (fail-open).
        Uses XADD to NEWS_FEATURE_DLQ_STREAM with approximate trimming.
        """
        try:
            now = int(now_ts_ms if now_ts_ms is not None else now_ms())
            if not self._dlq_allow(now):
                return

            dlq_stream = str(getattr(config, "NEWS_FEATURE_DLQ_STREAM", "news:analysis:dlq"))
            maxlen = int(getattr(config, "NEWS_DLQ_MAXLEN", 200000))
            cat = str(category) if category else get_redis_error_category(err)
            payload = sanitize_for_dlq(fields if isinstance(fields, dict) else {"_raw": str(fields)})
            payload_json = truncate_message(safe_json_dumps(payload), 16_384)
            err_s = truncate_message(str(err) or "", 1024)

            self.r.xadd(
                dlq_stream
                {
                    "ts_ms": str(now)
                    "src_stream": str(config.NEWS_ANALYSIS_STREAM)
                    "src_msg_id": str(src_msg_id)
                    "consumer": str(self.consumer)
                    "uid": str(uid or "")
                    "err_category": str(cat)
                    "err_type": type(err).__name__
                    "err": err_s
                    "fields_json": payload_json
                }
                maxlen=maxlen
                approximate=True
            )
            self._inc("news_feature_store_dlq_total", 1, tags={"category": str(cat)})
        except Exception:
            # Fail-open: DLQ push must not break main loop
            log.exception("dlq push failed src_msg_id=%s", src_msg_id)

    def _target_keys(self, a: NewsAnalysisCompact) -> List[Tuple[str, str]]:
        """
        Returns list of (name, redis_key) targets:
          - always global
          - per symbol for a.symbols (if any)
        """
        targets: List[Tuple[str, str]] = [("global", _agg_key_global())]
        for s in (a.symbols or []):
            if s:
                targets.append((str(s), _agg_key_symbol(str(s))))
        return targets

    def process_compact(self, a: NewsAnalysisCompact, *, grade_id: Optional[int] = None, horizon_sec: Optional[int] = None, now: Optional[int] = None) -> None:
        """
        Process single NewsAnalysisCompact and update Redis aggregates.
        This method is intentionally separated from stream reading to enable unit testing.
        """
        t0 = time.time()
        if not a.uid:
            return

        targets = self._target_keys(a)

        pipe = self.r.pipeline()
        for _name, key in targets:
            pipe.hgetall(key)
        prev_list = pipe.execute()

        cur_now = int(now if now is not None else now_ms())

        pipe = self.r.pipeline()
        for idx, (_name, key) in enumerate(targets):
            prev = prev_list[idx] if isinstance(prev_list[idx], dict) else {}
            prev_ts, prev_risk, prev_sur, prev_grade = _read_prev_state(prev)
            prev_grade_change_ts = _read_prev_grade_change_ts(prev, prev_ts_ms=prev_ts)

            dt = max(0.0, (cur_now - prev_ts) / 1000.0) if prev_ts > 0 else 0.0
            alpha = _ewma_alpha(dt, config.NEWS_EWMA_HALFLIFE_SEC)

            # EWMA update
            risk_ewma = _update_ewma(prev_risk, float(a.risk), alpha)
            sur_ewma = _update_ewma(prev_sur, float(a.surprise), alpha)

            # ---- IMPORTANT FIX #1:
            # compute grade from *smoothed* EWMA values (reduces flapping).
            local_grade_in = grade_id
            local_horizon_in = horizon_sec

            if local_grade_in is None:
                cand_grade = compute_grade_id(
                    risk=float(risk_ewma)
                    surprise=float(sur_ewma)
                    confidence=float(getattr(a, "confidence", 0.0) or 0.0)
                )
            else:
                cand_grade = int(local_grade_in)

            # ---- IMPORTANT FIX #2:
            # apply cooldown first => effective grade_id
            # then compute horizon using *effective* grade (keeps semantics consistent).
            eff_grade_id, grade_change_ts_ms, frozen = _apply_grade_cooldown(
                prev_grade_id=int(prev_grade)
                prev_change_ts_ms=int(prev_grade_change_ts)
                new_grade_id=int(cand_grade)
                now_ts_ms=int(cur_now)
                cooldown_up_sec=int(getattr(config, "NEWS_GRADE_COOLDOWN_UP_SEC", 900))
                cooldown_down_sec=int(getattr(config, "NEWS_GRADE_COOLDOWN_DOWN_SEC", 300))
            )

            if local_horizon_in is None:
                if int(eff_grade_id) <= 0:
                    eff_horizon = 0
                else:
                    base_h = compute_horizon_sec(
                        primary_tag_id=int(a.primary_tag_id or 0)
                        tags_mask=int(a.tags_mask or 0)
                    )
                    eff_horizon = compute_horizon_sec_with_grade(
                        base_horizon_sec=int(base_h)
                        grade_id=int(eff_grade_id)
                    )
                    if eff_horizon < 0:
                        eff_horizon = 0
            else:
                eff_horizon = int(local_horizon_in)

            # Store minimal compact state (strings for Redis HASH)
            mapping = {
                "ref": str(a.news_ref or "")
                # EWMA: write both naming conventions (compat)
                "risk_ewma": f"{float(risk_ewma):.6f}"
                "surprise_ewma": f"{float(sur_ewma):.6f}"
                "risk_ema": f"{float(risk_ewma):.6f}"
                "surprise_ema": f"{float(sur_ewma):.6f}"

                "news_grade_id": str(int(eff_grade_id))
                "horizon_sec": str(int(eff_horizon))
                "grade_change_ts_ms": str(int(grade_change_ts_ms or cur_now))
                "grade_frozen": "1" if frozen else "0"

                "tags_mask": str(int(a.tags_mask))
                "primary_tag_id": str(int(a.primary_tag_id))
                "confidence": f"{float(getattr(a, 'confidence', 0.0) or 0.0):.6f}"

                # canonical timestamp for EWMA dt
                "ts_ms": str(int(cur_now))
                # legacy alias
                "asof_ts_ms": str(int(cur_now))
            }

            pipe.hset(key, mapping=mapping)
            pipe.expire(key, int(config.NEWS_AGG_TTL_SEC))

        # --- execute with small transient retry ---
        last_err: Optional[Exception] = None
        for attempt in range(max(1, self._retry_attempts)):
            try:
                pipe.execute()
                last_err = None
                break
            except Exception as e:
                last_err = e
                if is_transient_redis_error(e) and attempt + 1 < self._retry_attempts:
                    time.sleep(max(0.0, float(self._retry_sleep_ms) / 1000.0))
                    continue
                raise

    def _process_compact(self, a: Any, *, msg_id: str, raw_fields: Dict[str, Any]) -> None:
        """
        Core logic extracted for unit testing:
          - updates global + per-symbol agg hashes
          - computes grade + horizon (+ cooldown)
          - writes TTL
          - uses small transient retry around pipe.execute()

        NOTE: ACK/DLQ is handled by caller.
        """
        t0 = time.time()

        # update global + per symbol (если symbols пусты — только global)
        targets: List[Tuple[str, str]] = [("global", _agg_key_global())]
        for s in (getattr(a, "symbols", None) or []):
            try:
                ss = str(s or "").strip()
                if ss:
                    targets.append(("symbol", _agg_key_symbol(ss)))
            except Exception:
                continue

        # Fetch previous snapshots in one roundtrip
        pipe = self.r.pipeline()
        for _scope, key in targets:
            pipe.hgetall(key)
        prev_list = pipe.execute()

        now = now_ms()
        pipe = self.r.pipeline()

        # optional age metric (if analysis payload has ts_ms)
        try:
            a_ts = int(getattr(a, "ts_ms", 0) or 0)
            if a_ts > 0:
                self._gauge("news_feature_store_data_age_ms", float(max(0, now - a_ts)))
        except Exception:
            pass

        # counters: symbols count (low cardinality tags)
        self._inc("news_feature_store_symbols_total", int(max(0, len(targets) - 1)))

        for idx, (scope, key) in enumerate(targets):
            prev = prev_list[idx] if isinstance(prev_list[idx], dict) else {}

            # prev timestamp compatibility: asof_ts_ms is the current canonical field
            prev_ts = safe_int(prev.get("asof_ts_ms") or prev.get("ts_ms"), 0)
            dt = max(0.0, (now - prev_ts) / 1000.0) if prev_ts > 0 else 0.0
            alpha = _ewma_alpha(dt, config.NEWS_EWMA_HALFLIFE_SEC)

            # compat: read risk_ema/surprise_ema, fallback to risk_ewma/surprise_ewma
            prev_risk = safe_float(prev.get("risk_ema") or prev.get("risk_ewma"), 0.0)
            prev_sur = safe_float(prev.get("surprise_ema") or prev.get("surprise_ewma"), 0.0)

            risk_ewma = _update_ewma(prev_risk, float(getattr(a, "risk", 0.0) or 0.0), alpha)
            sur_ewma = _update_ewma(prev_sur, float(getattr(a, "surprise", 0.0) or 0.0), alpha)

            # ---- TODOs implemented: grade + horizon ----
            grade_id = int(
                compute_grade_id(
                    risk=float(risk_ewma)
                    surprise=float(sur_ewma)
                    confidence=float(getattr(a, "confidence", 0.0) or 0.0)
                )
            )

            base_hz = int(
                compute_horizon_sec(
                    primary_tag_id=int(getattr(a, "primary_tag_id", 0) or 0)
                    tags_mask=int(getattr(a, "tags_mask", 0) or 0)
                )
            )
            horizon_sec = int(compute_horizon_sec_with_grade(base_horizon_sec=base_hz, grade_id=grade_id))

            # --- anti-flap cooldown ---
            prev_grade = safe_int(prev.get("news_grade_id"), 0)
            prev_chg_ts = safe_int(prev.get("grade_change_ts_ms"), 0)
            eff_grade, eff_chg_ts, changed = _apply_grade_cooldown(
                prev_grade_id=int(prev_grade)
                prev_change_ts_ms=int(prev_chg_ts)
                new_grade_id=int(grade_id)
                now_ts_ms=int(now)
                cooldown_up_sec=int(getattr(config, "NEWS_GRADE_COOLDOWN_UP_SEC", 900))
                cooldown_down_sec=int(getattr(config, "NEWS_GRADE_COOLDOWN_DOWN_SEC", 300))
            )

            # metrics: grade distribution + changes + cooldown blocks
            self._inc("news_feature_store_grade_total", 1, tags={"grade": str(int(eff_grade)), "scope": str(scope)})
            if changed:  # change was blocked by cooldown (changed=True means frozen in our convention)
                self._inc("news_feature_store_grade_cooldown_block_total", 1, tags={"scope": str(scope)})
            if not changed and int(eff_grade) != int(prev_grade):
                self._inc(
                    "news_feature_store_grade_change_total"
                    1
                    tags={"from": str(int(prev_grade)), "to": str(int(eff_grade)), "scope": str(scope)}
                )

            pipe.hset(
                key
                mapping={
                    "ref": str(getattr(a, "news_ref", "") or "")
                    "risk_ewma": f"{float(risk_ewma):.6f}"
                    "surprise_ewma": f"{float(sur_ewma):.6f}"
                    "news_grade_id": str(int(eff_grade))
                    "grade_change_ts_ms": str(int(eff_chg_ts))
                    "tags_mask": str(int(getattr(a, "tags_mask", 0) or 0))
                    "primary_tag_id": str(int(getattr(a, "primary_tag_id", 0) or 0))
                    "horizon_sec": str(int(horizon_sec))
                    "confidence": f"{float(getattr(a, 'confidence', 0.0) or 0.0):.6f}"
                    "asof_ts_ms": str(int(now))
                }
            )
            pipe.expire(key, int(config.NEWS_AGG_TTL_SEC))

        # --- execute with small transient retry + metrics ---
        last_err: Optional[Exception] = None
        for attempt in range(max(1, self._retry_attempts)):
            try:
                pipe.execute()
                last_err = None
                break
            except Exception as e:
                last_err = e
                cat = "unknown"
                try:
                    cat = get_redis_error_category(e)
                except Exception:
                    cat = "unknown"
                self._inc("news_feature_store_redis_exec_err_total", 1, tags={"category": str(cat)})
                if is_transient_redis_error(e) and attempt + 1 < self._retry_attempts:
                    self._inc("news_feature_store_redis_retry_total", 1, tags={"category": str(cat)})
                    time.sleep(max(0.0, float(self._retry_sleep_ms) / 1000.0))
                    continue
                raise

        self._inc("news_feature_store_processed_total", 1, tags={"result": "ok"})

    def _dlq_allow(self, now_ts_ms: int) -> bool:
        """
        Per-process, per-minute limiter for DLQ writes.
        Prevents Redis overload during массовой деградации.
        """
        try:
            if self._dlq_max_per_min <= 0:
                return False
            minute = int(now_ts_ms // 60000)
            if minute != self._dlq_minute:
                self._dlq_minute = minute
                self._dlq_count = 0
            if self._dlq_count >= self._dlq_max_per_min:
                self._inc("news_feature_store_dlq_dropped_total", 1)
                return False
            self._dlq_count += 1
            return True
        except Exception:
            return False

    def process_stream_fields(self, msg_id: str, fields: Dict[str, Any]) -> None:
        """
        One-message handler with retry + DLQ (no xack here).
        """
        a: Optional[NewsAnalysisCompact] = None
        uid = ""
        try:
            a = NewsAnalysisCompact.from_stream_fields(fields)
            uid = str(getattr(a, "uid", "") or "")
            if not uid:
                return

            retries = int(getattr(config, "NEWS_FEATURE_PROCESS_RETRIES", 3))
            base_ms = int(getattr(config, "NEWS_FEATURE_RETRY_BASE_SLEEP_MS", 200))
            if retries < 1:
                retries = 1
            if base_ms < 0:
                base_ms = 0

            last_err: Optional[BaseException] = None
            for attempt in range(retries):
                try:
                    self.process_compact(a)
                    return
                except Exception as e:
                    last_err = e
                    # retry only transient redis errors
                    if attempt + 1 < retries and is_transient_redis_error(e):
                        sleep_s = (base_ms / 1000.0) * (2 ** attempt)
                        if sleep_s > 0:
                            time.sleep(sleep_s)
                        continue
                    raise

        except Exception as e:
            # DLQ + fail-open (do not block pending)
            self._push_dlq(src_msg_id=msg_id, fields=fields, uid=uid, err=e)
            log.exception("feature update failed msg_id=%s uid=%s err_cat=%s", msg_id, uid, get_redis_error_category(e))

    def run_forever(self) -> None:
        ensure_group(self.r, config.NEWS_ANALYSIS_STREAM, config.NEWS_FEATURE_GROUP, mkstream=True)
        log.info("news-feature-store started consumer=%s", self.consumer)

        while not self._stop:
            items = xreadgroup_block(
                self.r
                config.NEWS_ANALYSIS_STREAM
                config.NEWS_FEATURE_GROUP
                consumer=self.consumer
                count=self.batch
                block_ms=self.block_ms
            )
            if not items:
                continue

            # items read successfully
            try:
                n_msgs = 0
                for _stream, msgs in items:
                    n_msgs += len(msgs or {})
                self._inc("news_feature_store_ingest_total", n_msgs, tags={"result": "read"})
            except Exception:
                pass

            def _dlq_add(*, src_msg_id: str, uid: str, fields: Dict[str, Any], err: Exception) -> None:
                try:
                    # category is low-cardinality
                    try:
                        from common.redis_errors import get_redis_error_category
                        cat = get_redis_error_category(err)
                    except Exception:
                        cat = "unknown"

                    now = int(now_ms())
                    payload_json = _safe_json_dumps(fields, limit=int(getattr(config, "NEWS_FEATURE_DLQ_FIELDS_LIMIT", 4096)))
                    err_s = (str(err) or "")[:512]
                    self.r.xadd(
                        _dlq_stream()
                        {
                            "ts_ms": str(now)
                            "src_stream": str(config.NEWS_ANALYSIS_STREAM)
                            "src_msg_id": str(src_msg_id)
                            "consumer": str(self.consumer)
                            "uid": str(uid or "")
                            "err_category": str(cat)
                            "err_type": type(err).__name__
                            "err": err_s
                            "fields_json": payload_json
                        }
                        maxlen=int(getattr(config, "NEWS_FEATURE_DLQ_MAXLEN", 200_000))
                        approximate=True
                    )
                    self._inc("news_feature_store_dlq_total", 1, tags={"category": str(cat)})
                except Exception:
                    return

            def _ack(msg_id: str) -> None:
                try:
                    xack(self.r, config.NEWS_ANALYSIS_STREAM, config.NEWS_FEATURE_GROUP, msg_id)
                    self._inc("news_feature_store_ack_total", 1, tags={"result": "ok"})
                except Exception as e:
                    self._inc("news_feature_store_ack_total", 1, tags={"result": "err"})

            for _stream, msgs in items:
                for msg_id, fields in msgs.items():
                    try:
                        a = NewsAnalysisCompact.from_stream_fields(fields)
                        if not a.uid:
                            xack(self.r, config.NEWS_ANALYSIS_STREAM, config.NEWS_FEATURE_GROUP, msg_id)
                            continue

                        # update global + per symbol (если symbols пусты — только global)
                        targets = [("global", _agg_key_global())]
                        for s in (a.symbols or []):
                            targets.append((s, _agg_key_symbol(s)))

                        pipe = self.r.pipeline()
                        for _name, key in targets:
                            pipe.hgetall(key)
                        prev_list = pipe.execute()

                        now = now_ms()
                        pipe = self.r.pipeline()

                        for idx, (name, key) in enumerate(targets):
                            prev = prev_list[idx] if isinstance(prev_list[idx], dict) else {}
                            prev_ts, prev_risk, prev_sur, _prev_grade = _read_prev_state(prev)
                            dt = max(0.0, (now - prev_ts) / 1000.0) if prev_ts > 0 else 0.0
                            alpha = _ewma_alpha(dt, config.NEWS_EWMA_HALFLIFE_SEC)

                            risk_ewma = _update_ewma(prev_risk, a.risk, alpha)
                            sur_ewma = _update_ewma(prev_sur, a.surprise, alpha)

                            # Grade: use EWMA risk/surprise for stability, and current confidence to suppress noise.
                            grade_id = compute_grade_id(
                                risk=float(risk_ewma)
                                surprise=float(sur_ewma)
                                confidence=float(getattr(a, "confidence", 0.0) or 0.0)
                            )

                            # Horizon: tag-based base (2h..48h) then grade scaling + caps.
                            base_h = compute_horizon_sec(
                                primary_tag_id=int(a.primary_tag_id or 0)
                                tags_mask=int(a.tags_mask or 0)
                            )
                            horizon_sec = compute_horizon_sec_with_grade(
                                base_horizon_sec=int(base_h)
                                grade_id=int(grade_id)
                            )

                            # tags_mask/primary_tag — храним последнее (самое свежее)
                            # Если хочешь "max risk" по окну — добавь отдельное поле.
                            pipe.hset(
                                key
                                mapping={
                                    "ref": a.news_ref
                                    # EWMA (write both aliases so old consumers keep working)
                                    "risk_ewma": f"{float(risk_ewma):.6f}"
                                    "surprise_ewma": f"{float(sur_ewma):.6f}"
                                    "risk_ema": f"{float(risk_ewma):.6f}"
                                    "surprise_ema": f"{float(sur_ewma):.6f}"
                                    # Grade 0..4
                                    "news_grade_id": str(int(grade_id))
                                    "tags_mask": str(int(a.tags_mask))
                                    "primary_tag_id": str(int(a.primary_tag_id))
                                    # Horizon in seconds (0 => ignore)
                                    "horizon_sec": str(int(horizon_sec))
                                    "confidence": f"{float(a.confidence):.6f}"
                                    # ts_ms is the canonical "asof" for EWMA dt;
                                    # keep asof_ts_ms alias for backward compatibility.
                                    "ts_ms": str(int(now))
                                    "asof_ts_ms": str(int(now))
                                }
                            )
                            pipe.expire(key, int(config.NEWS_AGG_TTL_SEC))

                        pipe.execute()

                        xack(self.r, config.NEWS_ANALYSIS_STREAM, config.NEWS_FEATURE_GROUP, msg_id)

                    except Exception as e:
                        log.exception("feature update failed msg_id=%s err=%s", msg_id, e)
                        # fail-open: ack, чтобы не забить pending
                        try:
                            xack(self.r, config.NEWS_ANALYSIS_STREAM, config.NEWS_FEATURE_GROUP, msg_id)
                        except Exception:
                            pass
