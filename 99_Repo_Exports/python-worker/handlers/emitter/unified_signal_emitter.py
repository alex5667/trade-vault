from __future__ import annotations

import hashlib
import math
import os
from dataclasses import dataclass
from typing import Any

from common.json_safe import make_json_safe_inplace
from common.strict_mode import strict_contracts_enabled
from services.telegram.analytics_reporter import AnalyticsReporter, NoopAnalyticsReporter
from utils.time_utils import get_ny_time_millis

from .outbox_writer import OutboxWriter
import contextlib


def _tags(kind: str, payload: dict[str, Any]) -> dict[str, str]:
    tags: dict[str, str] = {"kind": (kind or "unknown")}
    sym = payload.get("symbol", "") or ""
    if sym:
        tags["symbol"] = str(sym)
    tf = payload.get("timeframe", "") or payload.get("tf", "") or ""
    if tf:
        tags["timeframe"] = tf
    ven = payload.get("venue", "") or ""
    if ven:
        tags["venue"] = str(ven)
    fam = payload.get("family", "") or ""
    if fam:
        tags["family"] = str(fam)
    return tags


@dataclass(frozen=True)
class _SemDedupCfg:
    enabled: bool
    bucket_ms: int
    ttl_ms: int
    level_decimals: int
    level_key_kinds: set[str]


class _NoopMetrics:
    """Fail-open metrics shim. Real implementation can be StatsD/Prometheus wrapper, etc."""
    def inc(self, _name: str, _value: int = 1, _tags: dict[str, str] | None = None) -> None:
        return
    def gauge(self, _name: str, _value: float, _tags: dict[str, str] | None = None) -> None:
        return
    def observe(self, _name: str, _value: float, _tags: dict[str, str] | None = None) -> None:
        return


#
# UnifiedSignalEmitter
# -------------------
# This emitter is intentionally "dumb": it writes payloads to an outbox (Redis stream via outbox.publish()).
# Any fanout (WS/TG/etc) must be done by downstream consumers reading from the outbox.
#
# Two dedup layers exist:
#  1) "structural" TTL dedup (existing): stable hash of (symbol|kind|ts|signal_id|level_price) -> blocks duplicates.
#  2) "semantic" TTL dedup (new, opt-in): blocks duplicates that are "same signal meaning" within a bucket window.
#     Final requested policy:
#       - always include venue + timeframe (when present) to avoid cross-contour collisions
#       - include level_key ONLY for selected kinds via feature flag (to avoid killing dedup everywhere)
#

class UnifiedSignalEmitter:
    """
    ВАЖНО (то, о чём вы спрашивали):
      emit(..., labels=None)
      payload.setdefault("labels", {}).update(labels)
    FINАЛЬНО:
      - retries
      - dedup TTL (идемпотентность по signal_id)
      - fail-open на битых labels
      - semantic dedup (opt-in): venue+timeframe always, level_key only for selected kinds
    """
    def __init__(
        self,
        *,
        outbox: Any,
        logger: Any,
        outbox_labels: Any | None = None,
        metrics: Any | None = None,
        analytics: Any | None = None,
    ) -> None:
        # Publisher'ы (обычно пишут в Redis Stream / outbox)
        self._outbox_pub = outbox
        self._outbox_labels_pub = outbox_labels or outbox
        self._logger = logger
        # Метрики должны быть fail-open: если не передали — Noop.
        self._metrics = metrics if (metrics is not None and hasattr(metrics, "inc")) else  _NoopMetrics()  # type: ignore
        self._analytics = analytics if (analytics is not None and hasattr(analytics, "record_sem_dedup")) else NoopAnalyticsReporter()  # type: ignore
        self._retries = int(os.getenv("EMIT_RETRIES", "2"))
        self._retry_sleep_ms = int(os.getenv("EMIT_RETRY_SLEEP_MS", "15"))

        # Дедуп TTL по signal_id должен переживать рестарты процесса.
        # Поэтому: 1) Redis-idempotency (если доступен redis у publisher'а),
        #          2) локальный hot-dedup как ускоритель (не обязан быть идеальным).
        self._dedup_ttl_ms = int(os.getenv("EMIT_DEDUP_TTL_MS", "60000"))
        self._dedup_pending_ttl_ms = int(os.getenv("EMIT_DEDUP_PENDING_TTL_MS", "60000"))
        self._hot_dedup = _DedupTTL(
            ttl_ms=int(os.getenv("EMIT_HOT_DEDUP_TTL_MS", str(self._dedup_ttl_ms))),
            max_items=int(os.getenv("EMIT_HOT_DEDUP_MAX", "20000")),
        )

        # ---- Semantic dedup configuration (requested "last safe step") ----
        # Enable: OUTBOX_SEM_DEDUP=1
        # Bucket id: ts_ms // OUTBOX_SEM_DEDUP_BUCKET_MS (default 1000ms)
        # Key fields ALWAYS include: symbol, kind, bucket_id, side, venue, timeframe, level_price_rounded
        # Optional: include level_key only for kinds in OUTBOX_SEM_DEDUP_LEVEL_KEY_KINDS.
        #
        # Why not always include level_key?
        #   level_key is often absent/unstable -> it can silently disable dedup.
        #   Поэтому: включаем только для "breakout/absorption" (или вашего списка) фичефлагом.
        self._sem_cfg = self._load_sem_cfg()
        self._sem_dedup = _DedupTTL(
            ttl_ms=self._sem_cfg.ttl_ms,
            max_items=int(os.getenv("OUTBOX_SEM_DEDUP_MAX", "50000")),
        )
        # Local counters (per-process). Dashboard should aggregate across instances.
        self._sem_counts: dict[tuple[str, str], tuple[int, int]] = {}

        # "последняя гайка": если outbox publisher даёт redis + stream_name, OutboxWriter сделает атомарный XADD+dedup.
        outbox_stream = os.getenv("OUTBOX_STREAM", None)  # можно не задавать, если publisher.stream_name есть
        labels_stream = os.getenv("OUTBOX_LABELS_STREAM", None)

        self._writer = OutboxWriter(
            publisher=self._outbox_pub,
            logger=self._logger,
            retries=self._retries,
            retry_sleep_ms=self._retry_sleep_ms,
            dedup_ttl_ms=self._dedup_ttl_ms,
            dedup_pending_ttl_ms=self._dedup_pending_ttl_ms,
            stream_key=outbox_stream,
        )
        self._writer_labels = OutboxWriter(
            publisher=self._outbox_labels_pub,
            logger=self._logger,
            retries=self._retries,
            retry_sleep_ms=self._retry_sleep_ms,
            dedup_ttl_ms=self._dedup_ttl_ms,
            dedup_pending_ttl_ms=self._dedup_pending_ttl_ms,
            stream_key=labels_stream,
        )

    def _load_sem_cfg(self) -> _SemDedupCfg:
        enabled = os.getenv("OUTBOX_SEM_DEDUP", "0").strip() in {"1", "true", "yes", "on"}
        bucket_ms = int(os.getenv("OUTBOX_SEM_DEDUP_BUCKET_MS", "1000"))
        ttl_ms = int(os.getenv("OUTBOX_SEM_DEDUP_TTL_MS", "15000"))
        level_decimals = int(os.getenv("OUTBOX_SEM_DEDUP_LEVEL_DECIMALS", "2"))
        raw_kinds = os.getenv("OUTBOX_SEM_DEDUP_LEVEL_KEY_KINDS", "").strip()
        # Example: "breakout,absorption"
        level_key_kinds = {k.strip().lower() for k in raw_kinds.split(",") if k.strip()} if raw_kinds else set()
        return _SemDedupCfg(
            enabled=bool(enabled),
            bucket_ms=max(1, bucket_ms),
            ttl_ms=max(1, ttl_ms),
            level_decimals=max(0, min(8, level_decimals)),
            level_key_kinds=level_key_kinds,
        )

    def _now_ms(self) -> int:
        return get_ny_time_millis()

    def _tags(self, payload: dict[str, Any]) -> dict[str, str]:
        kind = (payload.get("kind", "") or "unknown")
        tags: dict[str, str] = {"kind": kind}
        sym = payload.get("symbol", "") or ""
        if sym:
            tags["symbol"] = str(sym)
        tf = payload.get("timeframe", "") or payload.get("tf", "") or ""
        if tf:
            tags["timeframe"] = tf
        ven = payload.get("venue", "") or ""
        if ven:
            tags["venue"] = str(ven)
        fam = payload.get("family", "") or ""
        if fam:
            tags["family"] = str(fam)
        return tags

    def _idempotency_key(self, payload: dict[str, Any]) -> str:
        """
        Idempotency key: first by signal_id (strongest), fallback to stable hash.
        This is the "exact duplicate" layer (not semantic).
        """
        sid = (payload.get("signal_id", "") or "").strip()
        if sid:
            # signal_id may be globally unique already; still namespace it to avoid collisions with future formats.
            return f"sid:{sid}"
        sym = (payload.get("symbol", "") or "")
        kind = (payload.get("kind", "") or "")
        ts = (payload.get("ts", "") or "")
        lvl = (payload.get("level_price", "") or "")
        base = f"fallback|{sym}|{kind}|{ts}|{lvl}"
        return hashlib.sha1(base.encode("utf-8")).hexdigest()

    def _semantic_key(self, payload: dict[str, Any]) -> str | None:
        """
        Semantic key = "same event inside a short window".
        Requested safe policy:
          - ALWAYS split by venue + timeframe when present (reduces cross-contour collisions).
          - level_key only for selected kinds (feature flag), to avoid disabling dedup globally.
        Fail-open: if we cannot build a sane key (missing ts/kind/symbol), return None.
        """
        kind = (payload.get("kind", "") or "").strip().lower()
        sym = (payload.get("symbol", "") or "").strip()
        if not kind or not sym:
            return None

        b = self._sem_bucket_id(payload.get("ts"))
        if b is None:
            return None

        # side/direction: keep best-effort stable
        side = str(payload.get("side", "") or payload.get("direction", "") or "").strip().lower()
        venue = (payload.get("venue", "") or "").strip().lower() or "unknown_venue"
        tf = str(payload.get("timeframe", "") or payload.get("tf", "") or "").strip().lower() or "unknown_tf"

        lvl = self._round_level(payload.get("level_price"))
        if lvl is None:
            # If no level_price, still allow semantic dedup for "event-like" kinds by using "na".
            lvl = "na"

        parts = [sym, kind, str(b), side, venue, tf, lvl]

        # level_key: only for kinds explicitly allowed
        if kind in self._sem_cfg.level_key_kinds:
            level_key = (payload.get("level_key", "") or "").strip().lower() or "na"
            parts.append(level_key)

        base = "|".join(parts)
        return hashlib.sha1(base.encode("utf-8")).hexdigest()

    def _pick_writer(self, payload: dict[str, Any]) -> OutboxWriter:
        if (payload.get("kind", "")) == "label_update":
            return self._writer_labels
        return self._writer

    def _safe_float(self, x: Any) -> float | None:
        try:
            v = float(x)
            if math.isnan(v) or math.isinf(v):
                return None
            return v
        except Exception:
            return None

    def _round_level(self, level_price: Any) -> str | None:
        """
        Normalize level price so semantic dedup does not depend on tiny float noise.
        Result is a STRING to keep semantic key stable across float formatting differences.
        """
        v = self._safe_float(level_price)
        if v is None:
            return None
        d = self._sem_cfg.level_decimals
        # quantize using decimals -> string
        q = round(v, d)
        fmt = f"{{:.{d}f}}"
        return fmt.format(q)

    def _sem_bucket_id(self, ts_ms: Any) -> int | None:
        v = self._safe_float(ts_ms)
        if v is None:
            return None
        # allow ints/floats; treat as ms
        t = int(v)
        if t <= 0:
            return None
        return t // self._sem_cfg.bucket_ms

    def _sem_tags(self, payload: dict[str, Any]) -> dict[str, str]:
        # Keep tags low-cardinality: kind/symbol only (requested).
        sym = (payload.get("symbol", "") or "unknown")
        kind = (payload.get("kind", "") or "unknown")
        return {"symbol": sym, "kind": kind}

    def _sem_count_hit(self, payload: dict[str, Any]) -> None:
        tags = self._sem_tags(payload)
        if self._metrics is not None:
            self._metrics.inc("sem_dedup_hits_total", 1, tags)
        k = (tags["kind"], tags["symbol"])
        hits, writes = self._sem_counts.get(k, (0, 0))
        hits += 1
        self._sem_counts[k] = (hits, writes)
        denom = hits + writes
        if denom > 0 and self._metrics is not None:
            self._metrics.gauge("sem_dedup_ratio", float(hits) / float(denom), tags)

        # Analytics for Telegram alerts
        try:
            self._analytics.record_sem_dedup(symbol=tags["symbol"], kind=tags["kind"], hit=True)
            self._analytics.record_soft_reasons(symbol=tags["symbol"], kind=tags["kind"], payload=payload)
            self._analytics.maybe_flush(now_ms=self._now_ms())
        except Exception:
            pass

    def _sem_count_write(self, payload: dict[str, Any]) -> None:
        tags = self._sem_tags(payload)
        if self._metrics is not None:
            self._metrics.inc("sem_dedup_writes_total", 1, tags)
        k = (tags["kind"], tags["symbol"])
        hits, writes = self._sem_counts.get(k, (0, 0))
        writes += 1
        self._sem_counts[k] = (hits, writes)
        denom = hits + writes
        if denom > 0 and self._metrics is not None:
            self._metrics.gauge("sem_dedup_ratio", float(hits) / float(denom), tags)

        # Analytics for Telegram alerts
        with contextlib.suppress(Exception):
            self._analytics.record_sem_dedup(symbol=tags["symbol"], kind=tags["kind"], hit=False)

    def sem_dedup_enabled(self) -> bool:
        return bool(self._sem_cfg.enabled)

    def get_sem_stats_snapshot(self) -> dict[str, Any]:
        """
        Snapshot cumulative counters (since process start).
        Used by SemDedupReporter to compute interval deltas and send Telegram diagnostics.
        Structure:
          {
            "enabled": bool,
            "bucket_ms": int,
            "level_decimals": int,
            "ttl_ms": int,
            "hits": { "SYMBOL|KIND": int, ... },
            "writes": { "SYMBOL|KIND": int, ... },
          }
        """
        with self._sem_stats_lock:  # type: ignore
            hits = {f"{k[0]}|{k[1]}": int(v) for k, v in self._sem_hits.items()}  # type: ignore
            writes = {f"{k[0]}|{k[1]}": int(v) for k, v in self._sem_writes.items()}  # type: ignore
        return {
            "enabled": bool(self._sem_cfg.enabled),
            "bucket_ms": int(self._sem_cfg.bucket_ms),
            "level_decimals": int(self._sem_cfg.level_decimals),
            "ttl_ms": int(self._sem_cfg.ttl_ms),
            "hits": hits,
            "writes": writes,
        }

    def _ensure_signal_id(self, payload: dict[str, Any]) -> str:
        """
        Ensure signal_id is present, generate if missing.
        """
        sid = payload.get("signal_id")
        if isinstance(sid, str) and sid.strip():
            return sid
        # Generate fallback signal_id
        key = self._idempotency_key(payload)
        payload["signal_id"] = key
        # Mark as generated
        labels = payload.get("labels")
        if not isinstance(labels, dict):
            labels = {}
        labels["missing_signal_id_fail_open"] = 1
        payload["labels"] = labels
        return key

    def emit(
        self,
        payload: dict[str, Any],
        *,
        labels: dict[str, Any] | None = None,
        dedup: bool = True,
        meta: dict[str, Any] | None = None,
    ) -> bool:
        """
        Записывает сигнал в outbox (Redis Stream) через writer.

        ВАЖНО: meta НЕ часть payload.
        --------------------------------
        payload проходит через пайплайн/валидаторы/дедуп/форматтер — его важно держать компактным.
        meta — "sidecar" данные, которые:
          - не участвуют в правилах/скоринге/дедупе,
          - но должны быть сохранены в Redis рядом с записью,
          - и могут понадобиться downstream (бот/аналитика).

        Пример meta:
          {"config_params": {...}}

        Writer должен сохранить meta отдельным ключом по signal_id (атомарно с XADD, если возможно).
        """
        # ВАЖНО: candidates_total НЕ должен считаться здесь, чтобы не было double-count.
        # candidates_total считается в handler ДО validate/emit (см. common.signal_metrics.SignalMetrics).

        now_ms = self._now_ms()
        # Гарантируем signal_id до дедупа и записи в outbox
        signal_id = self._ensure_signal_id(payload)

        # ---- Semantic dedup (opt-in) ----
        # This runs BEFORE structural dedup to cut duplicate writes earlier.
        # Fail-open: any error in key build must not block the signal.
        if self._sem_cfg.enabled:
            try:
                sk = self._semantic_key(payload)
                if sk is not None and self._sem_dedup.seen(sk, now_ms):
                    self._sem_count_hit(payload)
                    return False
            except Exception:
                # fail-open: do not block if semantic dedup logic breaks
                pass

        if dedup:
            # 1) Exact idempotency dedup (by signal_id if present)
            k = self._idempotency_key(payload)
            if self._hot_dedup.seen(k, now_ms):
                with contextlib.suppress(Exception):
                    self._metrics.inc("signals_veto", 1, {**self._tags(payload), "reason": "dedup"})
                return False

        # labels: должны быть dict, и должны попасть в payload["labels"].
        # Fail-open: если labels битые/не-сериализуемые — не блокируем сигнал.
        if labels:
            try:
                payload.setdefault("labels", {})
                if isinstance(payload["labels"], dict):
                    payload["labels"].update(labels)
                else:
                    payload["labels"] = dict(labels)
            except Exception:
                # fail-open: не блокируем сигнал
                payload["labels"] = {"labels_corrupt_fail_open": 1}

        # strict: labels must be dict
        if "labels" in payload and not isinstance(payload["labels"], dict):
            payload["labels"] = {"labels_schema_fail_open": 1}

        # labels-driven защитные метрики (работает ТОЛЬКО для не-veto ветки, дошедшей до emit)
        try:
            lbs = payload.get("labels")
            if isinstance(lbs, dict):
                if lbs.get("touch_suppressed", 0):
                    self._metrics.inc("touch_suppressed_total", 1, self._tags(payload))
                if lbs.get("spread_filter_drop", 0):
                    self._metrics.inc("spread_filter_drops", 1, self._tags(payload))
                if lbs.get("cooldown_drop", 0):
                    self._metrics.inc("cooldown_drops", 1, self._tags(payload))
        except Exception:
            pass

        # --------------------------------------------------------------------
        # Quality histograms (минимальный набор):
        #   - conf_factor_hist{kind}
        #   - final_score_hist{kind}
        #
        # Мы НЕ предполагаем конкретную схему payload.
        # Best-effort:
        #   conf_factor:
        #     - payload["conf_factor"] (0..1)
        #     - или payload["confidence"] (0..100 или 0..1)
        #   final_score:
        #     - payload["final_score"] (raw_score * conf_factor)
        #
        # Это позволяет "подсветить" распределения без жёсткой зависимости от форматтера.
        # --------------------------------------------------------------------
        kind = (payload.get("kind", "") or "unknown")
        try:
            cf = payload.get("conf_factor")
            if cf is None:
                c = payload.get("confidence")
                if c is not None:
                    fc = float(c)
                    # если прислали pct [0..100] — нормализуем
                    cf = (fc / 100.0) if fc > 1.0 else fc
            if cf is not None:
                v = float(cf)
                # clamp
                if not (math.isnan(v) or math.isinf(v)):
                    v = max(0.0, min(1.0, v))
                    self._metrics.observe("conf_factor_hist", v, _tags(kind, payload))
        except Exception:
            pass
        try:
            fs = payload.get("final_score")
            if fs is not None:
                v = float(fs)
                if not (math.isnan(v) or math.isinf(v)):
                    self._metrics.observe("final_score_hist", v, _tags(kind, payload))
        except Exception:
            pass

        # ---- JSON safety: NaN/Inf/bytes/objects -> JSON-safe структура ----
        # Это критично для "жёсткого" свойства 6.3: никакие входы не должны ломать outbox.
        try:
            make_json_safe_inplace(payload)
            # Доп. жёсткость: проверим, что реально сериализуется стандартным json
            # (в проде это чуть дороже — поэтому только strict-mode).
            if strict_contracts_enabled():
                json.dumps(payload, ensure_ascii=False, separators=(",", ":"))  # type: ignore
        except Exception:
            # fail-open: минимальный "спасательный" payload, чтобы не потерять факт события
            payload.clear()
            payload.update({"kind": "emit_fail_open", "labels": {"payload_json_safe_fail_open": 1}})

        writer = self._pick_writer(payload)
        # ВАЖНО: не публикуем "напрямую" во внешние системы.
        # Здесь единственная обязанность — надежно записать в outbox.
        success = writer.write(payload=payload, signal_id=signal_id, dedup=dedup, meta=meta)
        if success:
            with contextlib.suppress(Exception):
                self._metrics.inc("signals_sent", 1, self._tags(payload))
            # "writes" means: semantic dedup did NOT block and we actually wrote to outbox
            if self._sem_cfg.enabled:
                self._sem_count_write(payload)
            # Update analytics on successful publish (also records soft reasons and flushes at interval)
            try:
                sym = (payload.get("symbol", "") or "")
                kind = (payload.get("kind", "") or "")
                self._analytics.record_soft_reasons(symbol=sym, kind=kind, payload=payload)
                self._analytics.maybe_flush(now_ms=self._now_ms())
            except Exception:
                pass
        else:
            with contextlib.suppress(Exception):
                self._metrics.inc("signals_veto", 1, {**self._tags(payload), "reason": "publish_failed"})
        return success
        if success:
            # "writes" means: semantic dedup did NOT block and we actually wrote to outbox
            if self._sem_cfg.enabled:
                self._sem_count_write(payload)
        return success


@dataclass
class _DedupEntry:
    ts_ms: int


class _DedupTTL:
    def __init__(self, ttl_ms: int, max_items: int) -> None:
        self.ttl_ms = int(ttl_ms)
        self.max_items = int(max_items)
        self._m: dict[str, _DedupEntry] = {}

    def _gc(self, now_ms: int) -> None:
        if len(self._m) <= self.max_items:
            # всё равно чистим просрочку
            dead = [k for k, v in self._m.items() if (now_ms - v.ts_ms) > self.ttl_ms]
            for k in dead:
                self._m.pop(k, None)
            return
        # жёсткая чистка: TTL + обрезка по времени
        items = sorted(self._m.items(), key=lambda kv: kv[1].ts_ms)
        for k, v in items:
            if (now_ms - v.ts_ms) > self.ttl_ms or len(self._m) > self.max_items:
                self._m.pop(k, None)

    def seen(self, key: str, now_ms: int) -> bool:
        self._gc(now_ms)
        e = self._m.get(key)
        if e is None:
            self._m[key] = _DedupEntry(ts_ms=now_ms)
            return False
        if (now_ms - e.ts_ms) > self.ttl_ms:
            self._m[key] = _DedupEntry(ts_ms=now_ms)
            return False
        return True
