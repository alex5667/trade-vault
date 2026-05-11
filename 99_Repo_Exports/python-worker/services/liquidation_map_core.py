from __future__ import annotations

"""services/liquidation_map_core.py

Фаза B (Python): построение "карты ликвидаций" (Liquidation Heatmap / Map).

Design goals (под ваш стиль trade проекта):
- Детерминированность по времени: работаем в epoch ms; не используем float для сумм.
- Управляемый риск: входной DQ (validate → drop/dlq), ограничение памяти, предсказуемые бюджеты.
- Инкрементальная агрегация: sliding window (1h/4h/24h) с вычитанием по expiry.

Этот файл содержит чистое ядро (без Redis) для удобных unit-тестов.
"""


import math
from collections import deque
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, getcontext

# Достаточная точность для USD notional суммирования
getcontext().prec = 28


class LiqMapError(Exception):
    pass


@dataclass(frozen=True)
class LiqEventV1:
    """Нормализованное событие ликвидации (после Go-ingestion, Redis Stream DTO)."""

    ts_event_ms: int
    ts_ingest_ms: int
    venue: str
    symbol: str
    order_side: str
    liq_side: str  # long|short
    price_s: str
    qty_s: str
    notional_usd_s: str


def _safe_int(s: object, default: int = 0) -> int:
    try:
        if s is None:
            return default
        return int(str(s))
    except Exception:
        return default


def _safe_str(s: object) -> str:
    if s is None:
        return ""
    return str(s)


def _first_present(fields: dict[str, object], *keys: str) -> object:
    for key in keys:
        value = fields.get(key)
        if value is not None and str(value).strip() != "":
            return value
    return None


def _safe_decimal_str(s: object) -> Decimal | None:
    """Parse Decimal from string-like input.

    NOTE: строгое поведение: NaN/inf/пусто -> None.
    """

    if s is None:
        return None
    ss = str(s).strip()
    if not ss:
        return None
    # Reject NaN/inf explicitly
    low = ss.lower()
    if "nan" in low or "inf" in low:
        return None
    try:
        return Decimal(ss)
    except (InvalidOperation, ValueError):
        return None


def normalize_liq_event(fields: dict[str, object]) -> tuple[LiqEventV1 | None, str | None]:
    """Normalize raw Redis stream fields into LiqEventV1.

    Returns:
        (event, reason) where reason != None indicates validation failure.
    """

    ts_event_ms = _safe_int(
        _first_present(fields, "ts_event_ms", "event_time_ms", "ts_ms", "ts"),
        -1,
    )
    ts_ingest_ms = _safe_int(
        _first_present(fields, "ts_ingest_ms", "ingest_time_ms", "recv_ts_ms", "written_at"),
        -1,
    )

    venue = _safe_str(_first_present(fields, "venue", "src", "source")).strip()
    if venue == "binance_usdm":
        venue = "binance_usdtm"
    symbol = _safe_str(fields.get("symbol")).strip().upper()
    order_side = _safe_str(_first_present(fields, "order_side", "raw_side")).strip().upper()
    liq_side = _safe_str(fields.get("liq_side")).strip().lower()

    price_s = _safe_str(fields.get("price")).strip()
    qty_s = _safe_str(fields.get("qty")).strip()
    notional_s = _safe_str(fields.get("notional_usd")).strip()

    if not symbol:
        return None, "empty_symbol"
    if venue not in ("binance_usdtm", "bybit_linear"):
        # allow forward-compat but mark for DLQ to keep contract strict
        return None, "bad_venue"
    if order_side not in ("BUY", "SELL"):
        return None, "bad_order_side"
    if liq_side not in ("long", "short"):
        return None, "bad_liq_side"

    if ts_event_ms <= 0:
        return None, "bad_ts_event"
    if ts_ingest_ms <= 0:
        return None, "bad_ts_ingest"

    # Price/qty/notional: must parse and be > 0
    px = _safe_decimal_str(price_s)
    q = _safe_decimal_str(qty_s)
    n = _safe_decimal_str(notional_s)

    if px is None or px <= 0:
        return None, "bad_price"
    if q is None or q <= 0:
        return None, "bad_qty"
    if n is None or n <= 0:
        return None, "bad_notional"

    ev = LiqEventV1(
        ts_event_ms=ts_event_ms,
        ts_ingest_ms=ts_ingest_ms,
        venue=venue,
        symbol=symbol,
        order_side=order_side,
        liq_side=liq_side,
        price_s=price_s,
        qty_s=qty_s,
        notional_usd_s=notional_s,
    )
    return ev, None


class Bucketizer:
    """Bucketizer для price->bucket.

    Поддерживаем 3 режима:
      - abs: фиксированный шаг в price units (например, 10 USDT)
      - log_bps: геометрическая сетка с шагом в bps (ratio = 1 + bps/10000)
      - log_pct: геометрическая сетка с шагом в % (ratio = 1 + pct/100)

    Почему log_*:
      - стабильно по масштабу цены
      - естественно для "heatmap" вокруг текущей цены

    Возвращаем bucket_key (строка):
      - abs: price_bucket_str
      - log_*: integer bucket_index as string

    NOTE: Для лог-сетки price отображается как float (для UI)
    но суммирование notional остаётся Decimal.
    """

    def __init__(
        self,
        mode: str,
        abs_step: Decimal | None = None,
        bps: int | None = None,
        pct: float | None = None,
    ) -> None:
        self.mode = mode
        self.abs_step = abs_step
        self.bps = bps
        self.pct = pct

        if self.mode == "abs":
            if self.abs_step is None or self.abs_step <= 0:
                raise LiqMapError("abs_step must be > 0 for abs mode")
        elif self.mode == "log_bps":
            if self.bps is None or self.bps <= 0:
                raise LiqMapError("bps must be > 0 for log_bps")
        elif self.mode == "log_pct":
            if self.pct is None or self.pct <= 0:
                raise LiqMapError("pct must be > 0 for log_pct")
        else:
            raise LiqMapError(f"unknown bucket mode: {self.mode}")

    def _ratio(self) -> float:
        if self.mode == "log_bps":
            return 1.0 + float(self.bps) / 10000.0  # type: ignore
        if self.mode == "log_pct":
            return 1.0 + float(self.pct) / 100.0  # type: ignore
        raise LiqMapError("ratio is only for log_* modes")

    def bucket_key(self, price: Decimal) -> str:
        if price <= 0:
            raise LiqMapError("price must be > 0")

        if self.mode == "abs":
            # round to nearest step
            step = self.abs_step  # type: ignore
            # idx = round(price / step)
            idx = int((price / step).to_integral_value(rounding="ROUND_HALF_UP"))  # type: ignore
            bucket_price = step * Decimal(idx)  # type: ignore
            # normalizing string (no scientific)
            s = format(bucket_price, "f")
            if "." in s:
                s = s.rstrip("0").rstrip(".")
            return s

        ratio = self._ratio()
        p = float(price)
        if p <= 0.0 or not math.isfinite(p):
            raise LiqMapError("price not finite")
        # nearest index
        idx = int(round(math.log(p) / math.log(ratio)))
        return str(idx)

    def bucket_price(self, bucket_key: str) -> float:
        """Approximate bucket price for UI ordering/range filtering."""

        if self.mode == "abs":
            try:
                return float(Decimal(bucket_key))
            except Exception:
                return float("nan")
        ratio = self._ratio()
        try:
            idx = int(bucket_key)
        except Exception:
            return float("nan")
        return math.exp(idx * math.log(ratio))


@dataclass
class _Contribution:
    ts_ms: int
    bucket_key: str
    side: str  # long|short
    notional: Decimal


class LiqMapWindowAgg:
    """Sliding window aggregator: bucket -> {long_usd, short_usd}.

    Complexity:
      - add: O(1)
      - evict: amortized O(1)
      - snapshot: O(B log B) где B = число активных bucket'ов (после фильтра/trim)

    Memory:
      - O(N) contributions within window
    """

    def __init__(
        self,
        window_ms: int,
        bucketizer: Bucketizer,
    ) -> None:
        if window_ms <= 0:
            raise LiqMapError("window_ms must be > 0")
        self.window_ms = window_ms
        self.bucketizer = bucketizer

        self._q: deque[_Contribution] = deque()
        # bucket_key -> [long, short]
        self._buckets: dict[str, list[Decimal]] = {}
        self.anchor_price: float | None = None  # latest event price (for range filter)

    def add(self, ts_event_ms: int, price: Decimal, liq_side: str, notional: Decimal) -> None:
        bk = self.bucketizer.bucket_key(price)
        self.anchor_price = float(price)

        if bk not in self._buckets:
            self._buckets[bk] = [Decimal("0"), Decimal("0")]

        if liq_side == "long":
            self._buckets[bk][0] += notional
            side = "long"
        elif liq_side == "short":
            self._buckets[bk][1] += notional
            side = "short"
        else:
            raise LiqMapError("liq_side must be long|short")

        self._q.append(_Contribution(ts_ms=ts_event_ms, bucket_key=bk, side=side, notional=notional))

    def evict(self, now_ms: int) -> int:
        """Evict expired contributions.

        Returns number of evicted events.
        """

        cutoff = now_ms - self.window_ms
        n = 0
        while self._q and self._q[0].ts_ms < cutoff:
            c = self._q.popleft()
            b = self._buckets.get(c.bucket_key)
            if b is None:
                n += 1
                continue
            if c.side == "long":
                b[0] -= c.notional
            else:
                b[1] -= c.notional

            # cleanup near-zero buckets
            if b[0] <= 0 and b[1] <= 0:
                self._buckets.pop(c.bucket_key, None)
            n += 1
        return n

    def levels(
        self,
        *,
        max_levels: int,
        range_pct: float,
    ) -> list[tuple[float, str, Decimal, Decimal]]:
        """Return snapshot levels.

        Returns list of tuples:
            (price_float, bucket_key, long_usd, short_usd)
        """

        items: list[tuple[float, str, Decimal, Decimal, Decimal]] = []
        anchor = self.anchor_price
        for bk, (l, s) in self._buckets.items():
            if l <= 0 and s <= 0:
                continue
            p = self.bucketizer.bucket_price(bk)
            if not math.isfinite(p) or p <= 0:
                continue

            if range_pct and range_pct > 0 and anchor and anchor > 0:
                if abs(p - anchor) / anchor * 100.0 > range_pct:
                    continue

            total = l + s
            items.append((p, bk, l, s, total))

        # trim by total notional
        if max_levels and max_levels > 0 and len(items) > max_levels:
            items.sort(key=lambda x: x[4], reverse=True)
            items = items[:max_levels]

        # sort by price
        items.sort(key=lambda x: x[0])

        return [(p, bk, l, s) for (p, bk, l, s, _t) in items]


def format_price(p: float) -> str:
    """Format price for JSON without scientific notation."""
    if not math.isfinite(p) or p <= 0:
        return "0"
    # Use fixed precision, then strip.
    s = f"{p:.10f}"
    s = s.rstrip("0").rstrip(".")
    return s


def format_decimal(d: Decimal) -> str:
    """Format Decimal for JSON as non-scientific string."""
    s = format(d, "f")
    if "." in s:
        s = s.rstrip("0").rstrip(".")
    return s
