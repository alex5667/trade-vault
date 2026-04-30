from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import hashlib
import json
import math
import os
import time
from typing import Any, Optional

from common.json_fast import dumps1

import uuid


class OutboxWriter:
    """
    Надёжная запись в outbox (обычно Redis Stream) с идемпотентностью по signal_id.

    Требования "последней гайки":
      - outbox пишем всегда (локально, дешево)
      - идемпотентность по signal_id (через Redis SET NX + TTL)
      - retry на publish
      - fail-open при отсутствии Redis-дедупа (оставляем хотя бы retries)

    "Совсем последняя гайка":
      - если доступен redis и мы знаем stream key, пишем В ОДНОЙ АТОМАРНОЙ ОПЕРАЦИИ:
          (dedup NX) + (XADD) + (dedup commit) через Lua-script.
        Это закрывает гонки/дубликаты даже при параллельных воркерах.
      - если redis/stream неизвестны, падаем назад на publish(payload) + двухфазный дедуп (SETNX PENDING / commit)
        который хуже при гонках, но сохраняет поведение совместимости.
    """

    def __init__(
        self
        *
        publisher: Any
        logger: Any
        retries: int
        retry_sleep_ms: int
        dedup_ttl_ms: int
        dedup_pending_ttl_ms: int
        stream_key: Optional[str] = None
        sem_enabled: Optional[bool] = None
        sem_ttl_ms: Optional[int] = None
        sem_pending_ttl_ms: Optional[int] = None
        sem_bucket_ms: Optional[int] = None
        sem_level_decimals: Optional[int] = None
    ) -> None:
        self._pub = publisher
        self._logger = logger
        self._retries = int(max(0, retries))
        self._retry_sleep_ms = int(max(0, retry_sleep_ms))
        self._dedup_ttl_ms = int(max(1000, dedup_ttl_ms))
        self._dedup_pending_ttl_ms = int(max(1000, dedup_pending_ttl_ms))
        # Если stream_key задан — можем XADD напрямую (atomic Lua path).
        # Если не задан, попробуем взять из publisher.stream_name, иначе fallback.
        self._stream_key = stream_key or getattr(publisher, "stream_name", None) or getattr(publisher, "stream", None)

        # namespace для дедуп-ключей
        self._dedup_prefix = os.getenv("OUTBOX_DEDUP_PREFIX", "outbox:dedup:")
        self._sem_dedup_prefix = os.getenv("OUTBOX_SEM_DEDUP_PREFIX", "outbox:sdup:")
        self._maxlen = int(os.getenv("OUTBOX_STREAM_MAXLEN", "100000"))  # XADD MAXLEN ~
        self._payload_max_bytes = int(os.getenv("OUTBOX_PAYLOAD_MAX_BYTES", "65000"))  # защитный лимит

        # "0.5 гайки": semantic dedup конфиг
        self._sem_enabled = sem_enabled if sem_enabled is not None else str(os.getenv("OUTBOX_SEM_DEDUP", "0")).strip().lower() in {"1", "true", "yes", "on"}
        self._sem_ttl_ms = sem_ttl_ms if sem_ttl_ms is not None else int(os.getenv("OUTBOX_SEM_DEDUP_TTL_MS", "15000"))
        self._sem_pending_ttl_ms = sem_pending_ttl_ms if sem_pending_ttl_ms is not None else int(os.getenv("OUTBOX_SEM_DEDUP_PENDING_TTL_MS", "15000"))
        self._sem_bucket_ms = sem_bucket_ms if sem_bucket_ms is not None else int(os.getenv("OUTBOX_SEM_DEDUP_BUCKET_MS", "1000"))
        self._sem_level_decimals = sem_level_decimals if sem_level_decimals is not None else int(os.getenv("OUTBOX_SEM_DEDUP_LEVEL_DECIMALS", "2"))

    def _normalize_sidecar_meta(self, meta: Optional[dict[str, Any]]) -> Optional[dict[str, Any]]:
        """
        ЖЁСТКИЙ КОНТРАКТ:
          - payload_meta (parts_full и т.п.) ДОЛЖНО лежать в meta["payload_meta"]
          - tradeable envelope/targets не содержит payload_meta (это делает PayloadBuilder)

        Допускаем 2 формы ввода (для совместимости):
          A) meta уже полноценный sidecar (schema/trace_id/decision_trace/...) + payload_meta
          B) meta это "payload_meta" (только диагностические части из пайплайна)
        """
        if not meta:
            return None
        if not isinstance(meta, dict):
            return None
        m = dict(meta)
        looks_like_sidecar = any(k in m for k in ("schema", "decision_trace", "trace_id", "trace_summary", "updated_ms"))
        if not looks_like_sidecar:
            # treat as payload_meta only
            return {"payload_meta": m}
        pm = m.get("payload_meta")
        if not isinstance(pm, dict):
            pm = {}
        # If callers accidentally put parts_full at top-level, move it under payload_meta.
        if "parts_full" in m and "parts_full" not in pm:
            pm["parts_full"] = m.pop("parts_full")
        m["payload_meta"] = pm
        return m

    def _now_ms(self) -> int:
        return get_ny_time_millis()

    def _redis(self) -> Optional[Any]:
        # несколько "популярных" имён
        r = getattr(self._pub, "redis", None)
        if r is not None:
            return r
        r = getattr(self._pub, "client", None)
        if r is not None:
            return r
        r = getattr(self._pub, "_redis", None)
        if r is not None:
            return r
        return None

    def _serialize_payload(self, payload: dict[str, Any]) -> str:
        """
        Пишем в outbox компактный JSON.
        Важно: любые "несериализуемые" типы не должны ломать сигнал → default=str (fail-open).
        Лимит размера: если payload раздувается, пишем усечённую версию и помечаем label'ом.
        """
        try:
            raw = json.dumps(payload, ensure_ascii=False, separators=(",", ":"), default=str)
            if len(raw.encode("utf-8")) <= self._payload_max_bytes:
                return raw
        except Exception:
            # fallback ниже
            raw = None

        # "последняя гайка": hard-ограничение размера / сериализации.
        # Не блокируем outbox запись: кладём только минимальные поля + маркер.
        minimal = {
            "kind": payload.get("kind")
            "symbol": payload.get("symbol")
            "ts": payload.get("ts")
            "signal_id": payload.get("signal_id")
            "labels": {"payload_truncated_fail_open": 1}
        }
        try:
            return json.dumps(minimal, ensure_ascii=False, separators=(",", ":"), default=str)
        except Exception:
            # совсем край: уже гарантированно сериализуемая строка
            return '{"labels":{"payload_unserializable_fail_open":1}}'

    def _dedup_key(self, signal_id: str) -> str:
        return f"{self._dedup_prefix}{signal_id}"

    def _sem_key(self, payload: dict[str, Any]) -> Optional[str]:
        """
        Semantic key: hash(symbol|kind|bucket_ts|side|level_price_rounded)

        Требования:
          - не блокировать сигнал, если не хватает полей (return None)
          - не падать на NaN/Inf/нечислах (return None)
        """
        if not self._sem_enabled:
            return None
        try:
            symbol = str(payload.get("symbol", "") or "")
            kind = str(payload.get("kind", "") or "")
            if not symbol or not kind:
                return None

            ts = payload.get("ts", None)
            ts_i = int(ts) if ts is not None else None
            if ts_i is None or ts_i <= 0:
                return None
            bucket_ms = max(1, int(self._sem_bucket_ms))
            bucket_ts = (ts_i // bucket_ms) * bucket_ms

            side = payload.get("side", None)
            direction = payload.get("direction", None)
            sd = str(side if side is not None else (direction if direction is not None else ""))

            lvl = payload.get("level_price", None)
            if lvl is None:
                lvl = payload.get("level", None)
            if lvl is None:
                return None
            lvf = float(lvl)
            if not math.isfinite(lvf) or lvf <= 0:
                return None
            lvf_r = round(lvf, int(self._sem_level_decimals))

            base = f"{symbol}|{kind}|{bucket_ts}|{sd}|{lvf_r}"
            h = hashlib.sha1(base.encode("utf-8")).hexdigest()
            return f"{self._sem_dedup_prefix}{h}"
        except Exception:
            return None

    def _set_pending(self, redis: Any, key: str) -> bool:
        """
        Двухфазный дедуп:
          1) SET key=PENDING NX EX pending_ttl  -> бронируем право писать сигнал
          2) publish
          3) SET key=<entry_id/1> XX EX dedup_ttl -> закрепляем как "уже опубликован"
        Если publish упал — удаляем key, чтобы следующий retry мог пройти.
        """
        try:
            # redis-py: set(name, value, nx=True, ex=seconds)
            ok = redis.set(key, "PENDING", nx=True, ex=int(self._dedup_pending_ttl_ms / 1000))
            return bool(ok)
        except Exception as e:
            # fail-open: дедуп не должен валить сигнал
            self._logger.warning(f"OutboxWriter dedup setnx failed (fail-open): {e}")
            return True

    def _commit(self, redis: Any, key: str, value: str) -> None:
        try:
            redis.set(key, value, xx=True, ex=int(self._dedup_ttl_ms / 1000))
        except Exception as e:
            # fail-open: если commit упал, сигнал уже в outbox; дедуп-штамп вторичен
            self._logger.warning(f"OutboxWriter dedup commit failed (fail-open): {e}")

    # --- "совсем последняя гайка": атомарная запись + дедуп через Lua ---
    _LUA_ATOMIC_XADD = r"""
    -- KEYS[1] = dedup_key (signal_id)
    -- KEYS[2] = semantic_key (or "__none__" to disable)
    -- KEYS[3] = stream_key
    -- KEYS[4] = meta_key (or "__none__" to disable)
    -- ARGV[1] = dedup_ttl_sec
    -- ARGV[2] = pending_ttl_sec
    -- ARGV[3] = signal_id
    -- ARGV[4] = kind
    -- ARGV[5] = symbol
    -- ARGV[6] = ts
    -- ARGV[7] = payload_json
    -- ARGV[8] = maxlen (0 disables)
    -- ARGV[9] = sem_ttl_sec
    -- ARGV[10] = sem_pending_ttl_sec
    -- ARGV[11] = meta_json
    -- ARGV[12] = meta_ttl_sec
    -- ARGV[13] = trace_id (correlation id)
    --
    -- returns: {1, entry_id} on success
    --          {0} on dedup hit
    --          {2, err} on xadd failure (dedup key is rolled back)
    local sem_enabled = (KEYS[2] ~= '__none__')
    local maxlen = tonumber(ARGV[8]) or 0
    local trace_id = tostring(ARGV[13] or '')

    if redis.call('EXISTS', KEYS[1]) == 1 then
        return {0}
    end
    if sem_enabled and redis.call('EXISTS', KEYS[2]) == 1 then
        return {0}
    end
    local ok = redis.call('SET', KEYS[1], 'PENDING', 'NX', 'EX', tonumber(ARGV[2]))
    if not ok then
        return {0}
    end
    if sem_enabled then
        local ok2 = redis.call('SET', KEYS[2], 'PENDING', 'NX', 'EX', tonumber(ARGV[10]))
        if not ok2 then
            redis.call('DEL', KEYS[1])
            return {0}
        end
    end
    local xadd_ok, entry_id
    if maxlen > 0 then
        if trace_id ~= '' then
            xadd_ok, entry_id = pcall(redis.call, 'XADD', KEYS[3], 'MAXLEN', '~', maxlen, '*'
                'signal_id', ARGV[3]
                'trace_id',  trace_id
                'kind',      ARGV[4]
                'symbol',    ARGV[5]
                'ts',        ARGV[6]
                -- Backward compatibility:
                -- - SignalDispatcher historically expects "data"
                -- - some legacy readers expect "payload"
                'data',      ARGV[7]
                'payload',   ARGV[7]
            )
        else
            xadd_ok, entry_id = pcall(redis.call, 'XADD', KEYS[3], 'MAXLEN', '~', maxlen, '*'
                'signal_id', ARGV[3]
                'kind',      ARGV[4]
                'symbol',    ARGV[5]
                'ts',        ARGV[6]
                'data',      ARGV[7]
                'payload',   ARGV[7]
            )
        end
    else
        if trace_id ~= '' then
            xadd_ok, entry_id = pcall(redis.call, 'XADD', KEYS[3], '*'
                'signal_id', ARGV[3]
                'trace_id',  trace_id
                'kind',      ARGV[4]
                'symbol',    ARGV[5]
                'ts',        ARGV[6]
                'data',      ARGV[7]
                'payload',   ARGV[7]
            )
        else
            xadd_ok, entry_id = pcall(redis.call, 'XADD', KEYS[3], '*'
                'signal_id', ARGV[3]
                'kind',      ARGV[4]
                'symbol',    ARGV[5]
                'ts',        ARGV[6]
                'data',      ARGV[7]
                'payload',   ARGV[7]
            )
        end
    end
    if not xadd_ok then
        redis.call('DEL', KEYS[1])
        if sem_enabled then redis.call('DEL', KEYS[2]) end
        return {2, tostring(entry_id)}
    end
    redis.call('SET', KEYS[1], entry_id, 'XX', 'EX', tonumber(ARGV[1]))
    if sem_enabled then redis.call('SET', KEYS[2], entry_id, 'XX', 'EX', tonumber(ARGV[9])) end

    -- NEW: store meta sidecar only after successful XADD
    local meta_json = ARGV[11]
    local meta_ttl  = tonumber(ARGV[12]) or 0
    if KEYS[4] ~= '__none__' and meta_ttl > 0 and meta_json ~= nil and meta_json ~= '' then
      redis.call('SET', KEYS[4], meta_json, 'NX', 'EX', meta_ttl)
    end

    return {1, entry_id}
    """

    def _meta_key(self, signal_id: str) -> str:
        """
        Ключ для sidecar meta в Redis.

        По умолчанию: signal:meta:<signal_id>
        Можно переопределить через ENV OUTBOX_META_PREFIX.
        """
        prefix = os.getenv("OUTBOX_META_PREFIX", "signal:meta:")
        return f"{prefix}{signal_id}"

    def _serialize_meta(self, meta: Optional[dict[str, Any]]) -> str:
        """
        Сериализация meta.
        Требование: meta должна быть компактной и JSON-совместимой.
        Fail-open: при ошибке возвращаем пустую строку => meta не будет сохранена.
        """
        if not meta:
            return ""
        try:
            # Normalize to sidecar schema:
            #  - payload_meta MUST be under meta["payload_meta"]
            #  - decision_trace MAY be present as meta["decision_trace"] (dict)
            m = dict(meta)
            if "payload_meta" not in m and "decision_trace" not in m and "trace" not in m:
                # call-sites that pass "just payload_meta" (parts_full, etc.)
                m = {"payload_meta": m}
            if "schema" not in m:
                m["schema"] = "outbox_sidecar:v2"
            if "updated_ms" not in m:
                try:
                    m["updated_ms"] = get_ny_time_millis()
                except Exception:
                    pass

            # используем тот же сериализатор, что и для payload (если есть)
            return self._serialize_payload(m)  # type: ignore[attr-defined]
        except Exception:
            try:
                return json.dumps(m, ensure_ascii=False, separators=(",", ":"))
            except Exception:
                return ""

    def _ensure_envelope_trace_id(self, env: dict) -> str:
        """
        Гарантирует correlation-id на самом envelope (не только в payload сигнала).
        Это критично, потому что SignalDispatcher коррелирует доставку targets по trace_id.
        """
        try:
            tid = str(env.get("trace_id") or env.get("correlation_id") or "").strip()
        except Exception:
            tid = ""
        if not tid:
            tid = uuid.uuid4().hex
            try:
                env["trace_id"] = tid
                env["corr_id"] = tid  # alias для grep/legacy
            except Exception:
                pass
        return tid

    def _atomic_xadd(self, redis: Any, *, stream_key: str, payload: dict[str, Any], signal_id: str, meta_json: str = "", meta_ttl_sec: int = 0) -> Optional[str]:
        """
        Возвращает entry_id если запись успешно сделана
        None если дедуп сработал (уже видели signal_id).
        Бросать исключения наружу нежелательно — write() сам ретраит.
        """
        dedup_key = self._dedup_key(signal_id)
        sem_key = self._sem_key(payload) or "__none__"
        meta_key = "__none__"
        if meta_json and meta_ttl_sec > 0:
            meta_key = self._meta_key(signal_id)

        # ---------------------------------------------------------------------
        # DecisionTrace / correlation-id
        # - We always attach trace_id into payload (for joining streams).
        # - Full trace (events) should go into meta sidecar (keeps stream small).
        # ---------------------------------------------------------------------
        trace_id = str(payload.get("trace_id") or payload.get("correlation_id") or "") or uuid.uuid4().hex
        payload["trace_id"] = trace_id

        # If caller didn't pass meta_json, try to source it from ctx-like payload.
        # Expected upstream pattern: payload is an envelope which may already contain trace dict.
        if not meta_json:
            try:
                # if payload carries a DecisionTrace instance (rare), serialize safely
                tr = payload.get("decision_trace")
                if tr is not None:
                    if hasattr(tr, "to_dict"):
                        meta_json = json.dumps({"trace": tr.to_dict(max_events=200)}, ensure_ascii=False, separators=(",", ":"))
                        meta_ttl_sec = int(meta_ttl_sec or int(os.getenv("OUTBOX_TRACE_META_TTL_SEC", "86400")))
            except Exception:
                pass

        kind = str(payload.get("kind", "") or "")
        symbol = str(payload.get("symbol", "") or "")
        ts = str(payload.get("ts", "") or "")
        payload_json = self._serialize_payload(payload)

        # sidecar key
        if meta_json and (meta_ttl_sec > 0):
            meta_prefix = os.getenv("OUTBOX_META_PREFIX", "signal:meta:")
            meta_key = f"{meta_prefix}{signal_id}"
        else:
            meta_key = "__none__"

        res = redis.eval(
            self._LUA_ATOMIC_XADD
            4,                # было 3
            dedup_key,         # KEYS[1]
            sem_key,           # KEYS[2]
            stream_key,        # KEYS[3]
            meta_key,          # KEYS[4]  NEW
            int(self._dedup_ttl_ms / 1000),                 # ARGV[1]
            int(self._dedup_pending_ttl_ms / 1000),         # ARGV[2]
            signal_id,                                     # ARGV[3]
            kind,                                          # ARGV[4]
            symbol,                                        # ARGV[5]
            ts,                                            # ARGV[6]
            payload_json,                                  # ARGV[7]
            int(self._maxlen),                              # ARGV[8]
            int(max(1, self._sem_ttl_ms) / 1000),           # ARGV[9]
            int(max(1, self._sem_pending_ttl_ms) / 1000),   # ARGV[10]
            meta_json or "",                                # ARGV[11] NEW
            int(meta_ttl_sec or 0),                         # ARGV[12] NEW
            trace_id,                                       # ARGV[13] NEW
        )
        # Ожидаем массив/список: [1, entry_id] / [0] / [2, err]
        if isinstance(res, (list, tuple)) and len(res) >= 1:
            code = int(res[0])
            if code == 0:
                return None
            if code == 1 and len(res) >= 2:
                return str(res[1])
            if code == 2:
                self._logger.warning(f"OutboxWriter atomic XADD failed (rolled back): {res[1] if len(res) > 1 else 'unknown'}")
                raise RuntimeError("atomic_xadd_failed")
        # непредвиденный ответ — fail-open: считаем, что лучше попробовать fallback publish
        raise RuntimeError("atomic_xadd_bad_response")

    def _rollback(self, redis: Any, key: str) -> None:
        try:
            redis.delete(key)
        except Exception:
            pass

    def write(
        self
        *
        payload: dict[str, Any]
        signal_id: str
        dedup: bool
        meta: Optional[dict[str, Any]] = None
    ) -> bool:
        """
        Запись в outbox.

        meta:
          - не влияет на дедуп/валидаторы
          - сохраняется отдельно по ключу OUTBOX_META_PREFIX + signal_id
          - по умолчанию TTL берём OUTBOX_META_TTL_SEC (fallback на dedup TTL).
        """
        redis = self._redis()
        dedup_key = self._dedup_key(signal_id)
        sem_key = self._sem_key(payload)
        meta_norm = self._normalize_sidecar_meta(meta)

        # 1) "Самый последний слой жёсткости": если можем — делаем атомарный XADD+dedup в Redis.
        #    Это закрывает:
        #      - гонки между воркерами
        #      - дубликаты на ретраях/рестартах
        #      - ситуацию, когда publish упал после SETNX PENDING
        if dedup and redis is not None and self._stream_key:
            meta_json = self._serialize_meta(meta_norm)
            # TTL meta: либо явно задан, либо совпадает с TTL дедупа (логично хранить meta не меньше дедуп окна)
            meta_ttl_sec = int(os.getenv("OUTBOX_META_TTL_SEC", "0") or 0)
            if meta_ttl_sec <= 0:
                meta_ttl_sec = int(self._dedup_ttl_ms / 1000)

            last_exc: Optional[Exception] = None
            for i in range(max(1, self._retries + 1)):
                try:
                    entry_id = self._atomic_xadd(
                        redis
                        stream_key=str(self._stream_key)
                        payload=payload
                        signal_id=signal_id
                        meta_json=meta_json
                        meta_ttl_sec=meta_ttl_sec if meta_json else 0
                    )
                    return bool(entry_id)  # None => dedup hit => False
                except Exception as e:
                    last_exc = e
                    if i >= self._retries:
                        break
                    try:
                        time.sleep(self._retry_sleep_ms / 1000.0)
                    except Exception:
                        pass
            self._logger.exception(f"OutboxWriter.atomic write failed for signal_id={signal_id}: {last_exc}")
            # если atomic не работает, возвращаем False без fallback'а
            # (чтобы не плодить дубликаты через publisher.publish() без дедупа)
            return False

        # 2) Fallback: двухфазный дедуп + publisher.publish(payload)
        #    Используется когда stream_key неизвестен (совместимость со старым кодом)
        if dedup and redis is not None:
            # сначала — semantic ключ (если включён), затем signal_id ключ.
            # важно: если semantic сработал, возвращаем False (дубликат "по смыслу")
            # и НЕ трогаем signal_id ключ.
            if sem_key is not None:
                try:
                    ok_sem = bool(redis.set(sem_key, "PENDING", nx=True, ex=int(max(1000, self._sem_pending_ttl_ms) / 1000)))
                except Exception:
                    ok_sem = True  # fail-open
                if not ok_sem:
                    return False
            if not self._set_pending(redis, dedup_key):
                if sem_key is not None:
                    self._rollback(redis, sem_key)
                return False

            # ------------------------------------------------------------
            # ВАЖНО (жёсткий слой):
            # sidecar meta (trace/payload_meta) пишем NX=True ДО publish
            # чтобы даже при падении publish/worker мы не теряли diagnostics.
            # ------------------------------------------------------------
            try:
                if meta_norm:
                    meta_json = self._serialize_meta(meta_norm)
                    if meta_json:
                        ttl = int(os.getenv("OUTBOX_META_TTL_SEC", "0") or 0) or int(self._dedup_ttl_ms / 1000)
                        redis.set(self._meta_key(signal_id), meta_json, nx=True, ex=ttl)
            except Exception:
                pass

        last_exc: Optional[Exception] = None
        for i in range(max(1, self._retries + 1)):
            try:
                # publish должен писать в outbox stream (XADD внутри publisher'а).
                # Возврат entry_id желателен, но не обязателен.
                entry_id = self._pub.publish(payload)
                if dedup and redis is not None:
                    self._commit(redis, dedup_key, str(entry_id or "1"))
                    if sem_key is not None:
                        try:
                            redis.set(sem_key, str(entry_id or "1"), xx=True, ex=int(max(1000, self._sem_ttl_ms) / 1000))
                        except Exception:
                            pass  # fail-open
                # Best-effort: если pre-write meta не успел/упал — пробуем ещё раз после успеха.
                try:
                    if redis is not None and meta_norm:
                        meta_json = self._serialize_meta(meta_norm)
                        if meta_json:
                            ttl = int(os.getenv("OUTBOX_META_TTL_SEC", "0") or 0) or int(self._dedup_ttl_ms / 1000)
                            redis.set(self._meta_key(signal_id), meta_json, nx=True, ex=ttl)
                except Exception:
                    pass
                return True
            except Exception as e:
                last_exc = e
                if i >= self._retries:
                    break
                try:
                    time.sleep(self._retry_sleep_ms / 1000.0)
                except Exception:
                    pass

        # publish окончательно упал: если мы ставили PENDING, нужно откатить, иначе "заблокируем" сигнал.
        if dedup and redis is not None:
            self._rollback(redis, dedup_key)
            if sem_key is not None:
                self._rollback(redis, sem_key)

        # NB: meta уже пытались писать до publish и после успеха.
        self._logger.exception(f"OutboxWriter.write failed for signal_id={signal_id}: {last_exc}")
        return False

        self._logger.exception(f"OutboxWriter.write failed for signal_id={signal_id}: {last_exc}")
        return False
