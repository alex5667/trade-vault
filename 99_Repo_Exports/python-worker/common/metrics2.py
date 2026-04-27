from __future__ import annotations
from utils.time_utils import get_ny_time_millis

"""
metrics2.py
-----------
Минимальный, fail-open слой метрик с единым контрактом:
  - inc(name, value=1, tags=None)     -> counter
  - gauge(name, value, tags=None)     -> gauge
  - observe(name, value, tags=None)   -> histogram/summary (как поддержит backend)

Задача файла:
  1) Дать единый интерфейс "куда-то отправить" метрику (StatsD/Prometheus/Noop).
  2) Дать лёгкие трекеры (lag p50/p95, rate, missing_rate), чтобы не городить это в handler'ах.

ВАЖНО:
  - По умолчанию backend = Noop (ничего не делает), чтобы метрики никогда не ломали сигналинг.
  - Интеграция в StatsD/Prometheus может быть сделана отдельно, не меняя места вызовов.
"""

import math
import time
from collections import deque
from dataclasses import dataclass
from typing import Any, Deque, Optional


def _safe_tags(tags: Optional[dict[str, Any]]) -> Optional[dict[str, str]]:
    """Стандартизируем tags (низкая кардинальность — ответственность вызывающей стороны)."""
    if not tags:
        return None
    out: dict[str, str] = {}
    for k, v in tags.items():
        if k is None:
            continue
        ks = str(k)
        if v is None:
            continue
        out[ks] = str(v)
    return out or None


class Metrics:
    """Единый контракт. Любая реализация должна быть fail-open."""
    def inc(self, name: str, value: int = 1, tags: Optional[dict[str, Any]] = None) -> None:  # pragma: no cover
        raise NotImplementedError

    def gauge(self, name: str, value: float, tags: Optional[dict[str, Any]] = None) -> None:  # pragma: no cover
        raise NotImplementedError

    def observe(self, name: str, value: float, tags: Optional[dict[str, Any]] = None) -> None:  # pragma: no cover
        raise NotImplementedError


class NoopMetrics(Metrics):
    """Полностью пустая реализация (использовать по умолчанию)."""
    def inc(self, name: str, value: int = 1, tags: Optional[dict[str, Any]] = None) -> None:
        return

    def gauge(self, name: str, value: float, tags: Optional[dict[str, Any]] = None) -> None:
        return

    def observe(self, name: str, value: float, tags: Optional[dict[str, Any]] = None) -> None:
        return


class InMemoryMetrics(Metrics):
    """
    Тестовая/локальная реализация.
    Хранит события в списках — удобно assert'ить.
    """
    def __init__(self) -> None:
        self.counters: list[tuple[str, int, Optional[dict[str, str]]]] = []
        self.gauges: list[tuple[str, float, Optional[dict[str, str]]]] = []
        self.observations: list[tuple[str, float, Optional[dict[str, str]]]] = []

    def inc(self, name: str, value: int = 1, tags: Optional[dict[str, Any]] = None) -> None:
        self.counters.append((str(name), int(value), _safe_tags(tags)))

    def gauge(self, name: str, value: float, tags: Optional[dict[str, Any]] = None) -> None:
        self.gauges.append((str(name), float(value), _safe_tags(tags)))

    def observe(self, name: str, value: float, tags: Optional[dict[str, Any]] = None) -> None:
        self.observations.append((str(name), float(value), _safe_tags(tags)))


def safe_float(x: Any) -> Optional[float]:
    try:
        v = float(x)
        if math.isnan(v) or math.isinf(v):
            return None
        return v
    except Exception:
        return None


class NoopMetrics:
    """
    Fail-open sink: принимает любые метрики и ничего не делает.
    Используйте его как дефолт, чтобы инструментирование НИКОГДА не ломало trading path.
    """
    def inc(self, name: str, value: int = 1, tags: Optional[dict[str, Any]] = None) -> None:
        return

    def gauge(self, name: str, value: float, tags: Optional[dict[str, Any]] = None) -> None:
        return

    def observe(self, name: str, value: float, tags: Optional[dict[str, Any]] = None) -> None:
        return


class InMemoryMetrics:
    """
    Тестовый backend: хранит вызовы в памяти.
    Не используйте в проде.
    """
    def __init__(self) -> None:
        self.counters: list[tuple[str, int, Optional[dict[str, Any]]]] = []
        self.gauges: list[tuple[str, float, Optional[dict[str, Any]]]] = []
        self.observations: list[tuple[str, float, Optional[dict[str, Any]]]] = []

    def inc(self, name: str, value: int = 1, tags: Optional[dict[str, Any]] = None) -> None:
        self.counters.append((name, int(value), dict(tags) if tags else None))

    def gauge(self, name: str, value: float, tags: Optional[dict[str, Any]] = None) -> None:
        self.gauges.append((name, float(value), dict(tags) if tags else None))

    def observe(self, name: str, value: float, tags: Optional[dict[str, Any]] = None) -> None:
        self.observations.append((name, float(value), dict(tags) if tags else None))


def _quantile(sorted_xs: list[float], q: float) -> float:
    """
    Простая квантиль по ближайшему рангу.
    Достаточно для p50/p95 в окне.
    """
    if not sorted_xs:
        return 0.0
    q = max(0.0, min(1.0, float(q)))
    n = len(sorted_xs)
    idx = int(round((n - 1) * q))
    idx = max(0, min(n - 1, idx))
    return float(sorted_xs[idx])


class LagTracker:
    """
    Трекер лагов (ms) с фиксированным окном и экспортом p50/p95.
    Экспорт делаем gauge'ами:
      - <metric>_p50
      - <metric>_p95

    Пример:
      LagTracker(metric="tick_lag_ms", export_every_n=200, window=2048)
      -> gauge("tick_lag_ms_p50", ...)
      -> gauge("tick_lag_ms_p95", ...)
    """
    def __init__(
        self,
        *,
        metric: str,
        export_every_n: int = 200,
        window: int = 2048,
        tags: Optional[dict[str, Any]] = None,
    ) -> None:
        self.metric = metric
        self.export_every_n = max(1, int(export_every_n))
        self._xs: Deque[float] = deque(maxlen=max(16, int(window)))
        self._n = 0
        self._tags = dict(tags) if tags else None

    def feed(self, lag_ms: float) -> None:
        v = float(lag_ms)
        if math.isnan(v) or math.isinf(v):
            return
        # лаг < 0 (future tick) — это отдельная проблема; тут просто не портим процентили
        if v < 0:
            return
        self._xs.append(v)
        self._n += 1

    def maybe_export(self, m: Any) -> None:
        if m is None or not hasattr(m, "gauge"):
            return
        if self._n % self.export_every_n != 0:
            return
        xs = list(self._xs)
        xs.sort()
        p50 = _quantile(xs, 0.50)
        p95 = _quantile(xs, 0.95)
        m.gauge(f"{self.metric}_p50", p50, self._tags)
        m.gauge(f"{self.metric}_p95", p95, self._tags)


class MissingRateTracker:
    """
    Считает ratio missing/stale в скользящем окне N событий.
    Экспортирует gauge(metric, missing_ratio).
    """
    def __init__(self, *, metric: str, export_every_n: int = 200, window: int = 500, tags: Optional[dict[str, Any]] = None) -> None:
        self.metric = metric
        self.export_every_n = max(1, int(export_every_n))
        self._win: Deque[int] = deque(maxlen=max(10, int(window)))  # 1=miss, 0=ok
        self._n = 0
        self._tags = dict(tags) if tags else None

    def mark(self, miss: bool) -> None:
        self._win.append(1 if miss else 0)
        self._n += 1

    def maybe_export(self, m: Any) -> None:
        if m is None or not hasattr(m, "gauge"):
            return
        if self._n % self.export_every_n != 0:
            return
        if not self._win:
            return
        ratio = float(sum(self._win)) / float(len(self._win))
        m.gauge(self.metric, ratio, self._tags)


class EventRateTracker:
    """
    EMA events/sec. Полезно для L3 event rate.
    """
    def __init__(self, *, metric: str, alpha: float = 0.3, export_every_ms: int = 1000, tags: Optional[dict[str, Any]] = None) -> None:
        self.metric = metric
        self.alpha = max(0.01, min(0.99, float(alpha)))
        self.export_every_ms = max(200, int(export_every_ms))
        self._last_export_ms = 0
        self._last_tick_ms = 0
        self._ema = 0.0
        self._tags = dict(tags) if tags else None

    def mark_event(self, now_ms: int) -> None:
        now_ms = int(now_ms)
        if self._last_tick_ms <= 0:
            self._last_tick_ms = now_ms
            return
        dt = max(1, now_ms - self._last_tick_ms)
        inst = 1000.0 / float(dt)  # events/sec
        self._ema = self.alpha * inst + (1.0 - self.alpha) * self._ema
        self._last_tick_ms = now_ms

    def maybe_export(self, m: Any, now_ms: int) -> None:
        if m is None or not hasattr(m, "gauge"):
            return
        now_ms = int(now_ms)
        if (now_ms - self._last_export_ms) < self.export_every_ms:
            return
        self._last_export_ms = now_ms
        m.gauge(self.metric, float(self._ema), self._tags)


def extract_ts_ms(obj: Any) -> Optional[int]:
    """
    Best-effort извлечение timestamp (ms) из L2/L3/снапшотов/контекстов.
    Поддерживаем частые поля/форматы:
      - ts (ms или sec)
      - ts_ms (ms)
      - ts_utc (sec, float)
      - updated_ts / updated_ms / updated_at / updatedAt (sec/ms/ISO)
    Возвращает int ms или None.
    """
    if obj is None:
        return None

    # 1) прямые числовые поля
    for k in ("ts_ms", "updated_ms", "updated_ts", "ts", "ts_utc"):
        try:
            v = getattr(obj, k, None)
            if v is None and isinstance(obj, dict):
                v = obj.get(k)
            fv = safe_float(v)
            if fv is None:
                continue
            iv = int(fv)
            # ts_utc почти всегда seconds
            if k == "ts_utc":
                return int(fv * 1000.0)
            # поля с "ms" в названии — уже миллисекунды
            if "ms" in k.lower():
                return iv
            # эвристика sec/ms: числа > 1e10 считаем ms, иначе sec
            if iv > 1_000_000_000_000:  # > 1e12 => definitely ms (2023+)
                return iv
            # числа < 1e12 могут быть как sec так и ms, но в контексте трейдинга
            # числа ~1e9-1e10 — скорее всего sec (unix timestamp)
            # числа ~1e7-1e9 — могут быть как sec так и ms
            # числа < 1e7 — скорее всего ms (тестовые данные)
            if iv < 10_000_000:  # < 10M => probably ms (test data, small timestamps)
                return iv
            # 10M - 1e12 => assume sec
            return int(fv * 1000.0)
        except Exception:
            continue

    # 2) ISO-like поля updated_at/updatedAt
    for k in ("updated_at", "updatedAt"):
        try:
            v = getattr(obj, k, None)
            if v is None and isinstance(obj, dict):
                v = obj.get(k)
            if not v:
                continue
            s = str(v)
            # минимальный ISO парсер: YYYY-mm-ddTHH:MM:SS(.ms)?Z?
            # (не используем dateutil, чтобы не добавлять зависимость)
            s2 = s.replace("Z", "+00:00") if s.endswith("Z") else s
            dt = _dt.datetime.fromisoformat(s2)
            return int(dt.timestamp() * 1000)
        except Exception:
            continue

    return None


def is_stale(*, obj: Any, now_ms: int, max_age_ms: int) -> bool:
    """
    Единая проверка staleness:
      stale = (now_ms - ts_ms) > max_age_ms
    Если ts не извлекается -> считаем stale (fail-closed для метрики качества).
    Future ticks/snapshots: lag < 0 => НЕ stale (но future можно отдельно дропать на ingress).
    """
    ts_ms = extract_ts_ms(obj)
    if ts_ms is None:
        return True
    lag = int(now_ms) - int(ts_ms)
    if lag < 0:
        # future снапшот для staleness не считаем "stale" (это другая проблема).
        return False
    return lag > int(max_age_ms)


def normalize_ts_ms(ts: Any) -> Optional[int]:
    """
    Нормализация входного ts:
      - если < 1e12 => seconds => *1000
      - иначе считаем что ms
    Возвращает int ms или None.
    """
    fv = safe_float(ts)
    if fv is None:
        return None
    iv = int(fv)
    if iv <= 0:
        return None
    if iv < 1_000_000_000_000:
        return int(fv * 1000.0)
    return iv


def should_drop_by_watermark(*, now_ms: int, ts_ms: int, max_future_ms: int, max_past_ms: int) -> tuple[bool, str]:
    """
    Watermark-guard:
      - слишком в будущем: drop (future)
      - слишком в прошлом: drop (past)
    Возвращает (drop, reason_code).
    """
    now_ms = int(now_ms)
    ts_ms = int(ts_ms)
    lag = now_ms - ts_ms
    if lag < -abs(int(max_future_ms)):
        return True, "future_tick"
    if lag > abs(int(max_past_ms)):
        return True, "past_tick"
    return False, ""


@dataclass
class LagSnapshot:
    p50: float
    p95: float
    p99: float


class LagTracker:
    """
    Sliding-window оценка p50/p95/p99 по tick lag.
    - update(lag_ms) на каждом тике
    - maybe_export() периодически экспортирует tick_lag_ms_p50/p95/p99
    """
    def __init__(
        self,
        *,
        window: int = 2048,
        export_every_n: int = 200,
        metric_p50: str = "tick_lag_ms_p50",
        metric_p95: str = "tick_lag_ms_p95",
        metric_p99: str = "tick_lag_ms_p99",
        tags: Optional[dict[str, Any]] = None,
    ) -> None:
        self._xs: Deque[float] = deque(maxlen=max(16, int(window)))
        self._n = 0
        self._export_every_n = max(1, int(export_every_n))
        self._metric_p50 = metric_p50
        self._metric_p95 = metric_p95
        self._metric_p99 = metric_p99
        self._tags = tags

    def update(self, lag_ms: Any) -> None:
        v = safe_float(lag_ms)
        if v is None:
            return
        if v < 0:
            # future ticks не ломают распределение — clamp, а drop решается отдельно.
            v = 0.0
        self._xs.append(float(v))
        self._n += 1

    def snapshot(self) -> Optional[LagSnapshot]:
        if len(self._xs) < 8:
            return None
        ys = sorted(self._xs)
        def q(p: float) -> float:
            # nearest-rank (простая и стабильная)
            i = int(round((len(ys) - 1) * p))
            i = max(0, min(len(ys) - 1, i))
            return float(ys[i])
        return LagSnapshot(p50=q(0.50), p95=q(0.95), p99=q(0.99))

    def maybe_export(self, metrics: Metrics) -> None:
        if self._n % self._export_every_n != 0:
            return
        snap = self.snapshot()
        if not snap:
            return
        try:
            metrics.gauge(self._metric_p50, snap.p50, self._tags)
            metrics.gauge(self._metric_p95, snap.p95, self._tags)
            metrics.gauge(self._metric_p99, snap.p99, self._tags)
        except Exception:
            # fail-open
            return


class MissingRateTracker:
    """
    Для l3_missing_rate / l2_stale_rate:
      - mark(total += 1, miss += 1 если missing/stale)
      - экспортирует gauge rate = miss/total (EMA можно добавить позже)
    """
    def __init__(self, *, metric: str, export_every_n: int = 200, tags: Optional[dict[str, Any]] = None) -> None:
        self._metric = metric
        self._tags = tags
        self._total = 0
        self._miss = 0
        self._n = 0
        self._export_every_n = max(1, int(export_every_n))

    def mark(self, *, miss: bool) -> None:
        self._total += 1
        if miss:
            self._miss += 1
        self._n += 1

    def rate(self) -> float:
        if self._total <= 0:
            return 0.0
        return float(self._miss) / float(self._total)

    def maybe_export(self, metrics: Metrics) -> None:
        if self._n % self._export_every_n != 0:
            return
        try:
            metrics.gauge(self._metric, self.rate(), self._tags)
        except Exception:
            return


class EventRateTracker:
    """
    Оценка "events per second" (грубая, но дешевая) для l3_event_rate.
    - mark_event(now_ms)
    - maybe_export() -> gauge rate
    """
    def __init__(
        self,
        *,
        metric: str,
        export_every_ms: int = 1000,
        alpha: float = 0.3,
        tags: Optional[dict[str, Any]] = None,
    ) -> None:
        self._metric = metric
        self._tags = tags
        self._alpha = max(0.01, min(0.99, float(alpha)))
        self._export_every_ms = max(250, int(export_every_ms))
        self._last_export_ms: Optional[int] = None
        self._since = 0
        self._rate = 0.0

    def mark_event(self) -> None:
        self._since += 1

    def maybe_export(self, metrics: Metrics, now_ms: Optional[int] = None) -> None:
        nms = int(now_ms if now_ms is not None else get_ny_time_millis())
        if self._last_export_ms is None:
            self._last_export_ms = nms
            self._since = 0
            return
        dt = nms - self._last_export_ms
        if dt < self._export_every_ms:
            return
        inst = float(self._since) / max(1e-6, float(dt) / 1000.0)
        self._rate = self._alpha * inst + (1.0 - self._alpha) * self._rate
        self._since = 0
        self._last_export_ms = nms
        try:
            metrics.gauge(self._metric, self._rate, self._tags)
        except Exception:
            return
