from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Optional

from core.outbox_envelope import OutboxEnvelope
from core.redis_keys import RedisStreams as RS
from core.retention import MAXLEN_OUTBOX
from prometheus_client import Counter, Histogram

# ── Prometheus metrics ────────────────────────────────────────────────────────
OUTBOX_WRITE_LATENCY_SECONDS = Histogram(
    "outbox_write_latency_seconds",
    "Latency of successful XADD to SIGNAL_OUTBOX stream (seconds)",
    buckets=(0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0),
)

OUTBOX_DEDUP_HIT_TOTAL = Counter(
    "outbox_dedup_hit_rate",
    "Total signals suppressed by outbox idempotency / deduplication (dedup efficiency)",
)

OUTBOX_XADD_FAILED_FROZEN_TOTAL = Counter(
    "outbox_xadd_failed_frozen_total",
    "XADD failures where dedup key was frozen (fail-closed). Signal lost until dedup TTL expires.",
)


@dataclass(frozen=True)
class EmitResult:
    """
    Результат emit().

    - written: реально записали в outbox stream (новый сигнал)
    - duplicate: уже был (idempotency по signal_id) — повторно не пишем
    - ok: written or duplicate
    """
    ok: bool
    written: bool
    duplicate: bool
    entry_id: Optional[str] = None


@dataclass(frozen=True)
class OutboxWriteConfig:
    stream_name: str = RS.SIGNAL_OUTBOX
    # Fail-closed dedup TTL: максимальное окно потери сигнала при XADD timeout.
    # Default 1800s (30 мин) — компромисс между защитой от дубликатов и потерей.
    # Override: OUTBOX_DEDUP_TTL_S (не более 3600s рекомендуется).
    dedup_ttl_s: int = 1800
    # Короткий TTL для "placeholder" дедуп-ключа,
    # чтобы при падении процесса не блокировать навсегда.
    placeholder_ttl_s: int = 60
    max_retries: int = 3
    retry_backoff_ms: int = 30


class OutboxWriter:
    """
    Надёжная запись в Redis Stream с idempotency по signal_id.

    Проблема "классического" подхода:
      - если сначала сделать XADD, а потом setnx — возможны дубликаты (гонки).
      - если сначала setnx, а потом XADD — при падении между шагами можно "потерять" сообщение.

    Решение:
      1) SET key placeholder NX EX placeholder_ttl
         - если не получилось -> duplicate (сигнал уже был)
      2) XADD envelope
      3) SET key entry_id XX EX dedup_ttl
         - "закрепляем" дедуп на нормальный TTL
      4) если XADD упал -> DEL key (чтобы следующий ретрай смог дописать)
    """

    def __init__(
        self,
        *,
        redis,
        logger,
        metrics=None,
        stream_name: str = RS.SIGNAL_OUTBOX,
        dedup_ttl_s: Optional[int] = None,
        placeholder_ttl_s: int = 60,
        max_retries: int = 3,
        retry_backoff_ms: int = 30,
    ):
        self.redis = redis
        self.logger = logger
        self.metrics = metrics
        import os as _os
        _resolved_dedup_ttl = dedup_ttl_s if dedup_ttl_s is not None else int(
            _os.getenv("OUTBOX_DEDUP_TTL_S", "1800")
        )
        self.cfg = OutboxWriteConfig(
            stream_name=stream_name,
            dedup_ttl_s=_resolved_dedup_ttl,
            placeholder_ttl_s=placeholder_ttl_s,
            max_retries=max_retries,
            retry_backoff_ms=retry_backoff_ms,
        )

    def write(self, env: OutboxEnvelope) -> EmitResult:
        if not self.redis:
            self._m_inc("outbox.redis_missing")
            return EmitResult(ok=False, written=False, duplicate=False, entry_id=None)

        dedup_key = f"outbox:dedup:{env.signal_id}"
        placeholder = "PENDING"

        # 1) placeholder NX
        try:
            ok = self._redis_set(dedup_key, placeholder, nx=True, ex=self.cfg.placeholder_ttl_s)
        except Exception as e:
            self._m_inc("outbox.dedup.setnx_error")
            self.logger.warning(f"Outbox dedup SETNX failed: {e}")
            # Fail-closed для outbox: если дедуп не работает, лучше не плодить дубликаты.
            return EmitResult(ok=False, written=False, duplicate=False, entry_id=None)

        if not ok:
            # уже было
            self._m_inc("outbox.duplicate")
            try:
                OUTBOX_DEDUP_HIT_TOTAL.inc()
            except Exception:
                pass
            return EmitResult(ok=True, written=False, duplicate=True, entry_id=None)

        # 2) XADD с ретраями
        entry_id: Optional[str] = None
        last_err: Optional[Exception] = None
        for i in range(max(1, self.cfg.max_retries)):
            _t_xadd = time.monotonic()
            try:
                fields = env.to_stream_fields()
                entry_id = self.redis.xadd(self.cfg.stream_name, fields, maxlen=MAXLEN_OUTBOX, approximate=True)
                try:
                    OUTBOX_WRITE_LATENCY_SECONDS.observe(time.monotonic() - _t_xadd)
                except Exception:
                    pass
                break
            except Exception as e:
                last_err = e
                self._m_inc("outbox.xadd_error")
                # небольшой backoff
                time.sleep((self.cfg.retry_backoff_ms * (i + 1)) / 1000.0)

        if entry_id is None:
            # 4) XADD не удался — Fail-Closed защита от дубликатов.
            # При таймауте XADD мог физически выполниться на сервере.
            # Удаление dedup-ключа привело бы к дубликату при повторном emit.
            # Поэтому замораживаем ключ на dedup_ttl_s (fail-closed).
            # Максимальное окно потери: dedup_ttl_s (≤1800s по умолчанию).
            try:
                self._redis_set(dedup_key, "XADD_FAILED_FALLBACK", xx=True, ex=self.cfg.dedup_ttl_s)
            except Exception:
                pass
            try:
                OUTBOX_XADD_FAILED_FROZEN_TOTAL.inc()
            except Exception:
                pass
            self.logger.warning(f"Outbox XADD failed after retries: {last_err}")
            return EmitResult(ok=False, written=False, duplicate=False, entry_id=None)

        # 3) закрепляем дедуп ключ на "длинный TTL"
        try:
            # XX = только если ключ существует; EX = длинный TTL
            self._redis_set(dedup_key, str(entry_id), xx=True, ex=self.cfg.dedup_ttl_s)
        except Exception as e:
            # Даже если "закрепление" не удалось — сигнал уже в stream.
            # Риск: повторная запись при повторном emit, пока placeholder TTL не истёк.
            # Это лучше, чем "не записать вообще".
            self._m_inc("outbox.dedup.promote_error")
            self.logger.warning(f"Outbox dedup promote failed: {e}")

        self._m_inc("outbox.written")
        return EmitResult(ok=True, written=True, duplicate=False, entry_id=str(entry_id))

    def _redis_set(self, key: str, value: str, *, nx: bool = False, xx: bool = False, ex: Optional[int] = None) -> bool:
        """
        Совместимость с redis-py:
          redis.set(name, value, nx=True, xx=True, ex=seconds) -> True/False
        """
        # redis-py: set(name, value, ex=None, px=None, nx=False, xx=False, keepttl=False, get=False)
        return bool(self.redis.set(key, value, nx=nx, xx=xx, ex=ex))

    def _m_inc(self, name: str, v: int = 1) -> None:
        if not self.metrics:
            return
        try:
            if hasattr(self.metrics, "inc"):
                self.metrics.inc(name, v)
            elif hasattr(self.metrics, "counter"):
                self.metrics.counter(name, v)
        except Exception:
            pass
