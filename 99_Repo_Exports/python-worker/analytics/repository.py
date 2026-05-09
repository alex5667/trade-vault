from __future__ import annotations
from core.redis_keys import RedisStreams as RS

"""
Repository - Доступ к данным торговых сигналов и ордеров из Redis.

Предоставляет унифицированный интерфейс для чтения:
- Сигналов из Redis Streams и hashes
- Закрытых сделок/ордеров
- Событий по сделкам
- Вычисление P/L

Интеграция с Signal Performance Tracker:
- Читает данные из order:{id}, signal:{id}
- Использует trades:closed stream
- Совместим с существующей схемой Redis
"""

from collections.abc import Iterator
from dataclasses import dataclass, field
from typing import Any

import redis

from common.log import setup_logger
from services.trade_closed_hydrator import hydrate_trade_closed_batch


@dataclass
class RepoConfig:
    """Конфигурация репозитория"""
    redis_url: str = "redis://redis-worker-1:6379/0"
    default_symbol=""
    default_strategy: str = "orderflow"


@dataclass
class Signal:
    """Структура торгового сигнала"""
    signal_id: str
    symbol: str
    strategy: str
    direction: str  # LONG/SHORT
    price: float
    ts: float  # timestamp в секундах
    confidence: float | None = None
    score: float | None = None
    source: str | None = None  # OrderFlow, AggregatedHub-V2, etc
    atr: float | None = None
    timeframe: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class Order:
    """Структура торгового ордера/сделки"""
    order_id: str
    signal_id: str | None
    symbol: str
    strategy: str
    direction: str
    lot: float
    entry_price: float
    entry_time: float
    exit_price: float | None = None
    exit_time: float | None = None
    pnl_usd: float | None = None
    pnl_pct: float | None = None
    result: str | None = None  # win/loss/breakeven

    # TP/SL уровни и времена
    tp1_price: float | None = None
    tp1_time: float | None = None
    tp2_price: float | None = None
    tp2_time: float | None = None
    tp3_price: float | None = None
    tp3_time: float | None = None
    sl_price: float | None = None
    sl_time: float | None = None

    # Флаги достижения
    tp1_hit: bool = False
    tp2_hit: bool = False
    tp3_hit: bool = False

    # Метрики упущенной прибыли
    tp_before_sl: int = 0  # Какой TP был достигнут до SL

    timeframe: str | None = None
    source: str | None = None
    close_reason: str | None = None


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------

def _redis_row_to_order(order_id: str, data: dict[str, str]) -> Order:
    """Build an Order from a Redis hash (string values)."""
    return Order(
        order_id=order_id,
        signal_id=order_id,  # В нашей схеме order_id == signal_id
        symbol=data.get("symbol", "UNKNOWN"),
        strategy=data.get("strategy", "unknown"),
        direction=data.get("direction", "LONG"),
        lot=float(data.get("lot", 0)),
        entry_price=float(data.get("entry_price", 0)),
        entry_time=float(data.get("entry_time", 0)) / 1000.0,  # ms → seconds
        exit_price=float(data["exit_price"]) if data.get("exit_price") else None,
        exit_time=float(data["closed_time"]) / 1000.0 if data.get("closed_time") else None,
        pnl_usd=float(data["pnl"]) if data.get("pnl") else None,
        pnl_pct=float(data["pnl_pct"]) if data.get("pnl_pct") else None,
        result=data.get("result"),
        tp1_price=float(data["tp1"]) if data.get("tp1") else None,
        tp2_price=float(data["tp2"]) if data.get("tp2") else None,
        tp3_price=float(data["tp3"]) if data.get("tp3") else None,
        tp1_hit=bool(int(data.get("tp1_hit", 0))),
        tp2_hit=bool(int(data.get("tp2_hit", 0))),
        tp3_hit=bool(int(data.get("tp3_hit", 0))),
        sl_price=float(data["sl"]) if data.get("sl") else None,
        tp_before_sl=int(data.get("tp_before_sl", 0)),
        timeframe=data.get("tf"),
        source=data.get("source"),
        close_reason=data.get("close_reason"),
    )


def _pg_row_to_order(order_id: str, row: dict[str, Any]) -> Order:
    """Build an Order from a Postgres dict row."""
    return Order(
        order_id=order_id,
        signal_id=row.get("sid") or order_id,
        symbol=row.get("symbol", "UNKNOWN"),
        strategy=row.get("strategy", "unknown"),
        direction=row.get("direction", "LONG"),
        lot=float(row.get("lot", 0)),
        entry_price=float(row.get("entry_price", 0)),
        entry_time=float(row.get("entry_ts_ms", 0)) / 1000.0,
        exit_price=float(row["exit_price"]) if row.get("exit_price") else None,
        exit_time=float(row["exit_ts_ms"]) / 1000.0 if row.get("exit_ts_ms") else None,
        pnl_usd=float(row["pnl_net"]) if row.get("pnl_net") else None,
        pnl_pct=float(row["pnl_pct"]) if row.get("pnl_pct") else None,
        result=None,  # Derived if needed
        tp1_hit=bool(row.get("tp1_hit")),
        tp2_hit=bool(row.get("tp2_hit")),
        tp3_hit=bool(row.get("tp3_hit")),
        sl_price=float(row["sl_price"]) if row.get("sl_price") else None,
        tp_before_sl=int(row.get("tp_before_sl", 0)),
        timeframe=row.get("tf"),
        source=row.get("source"),
        close_reason=row.get("close_reason"),
    )


class Repository:
    """
    Репозиторий для доступа к данным торговых сигналов и ордеров.

    Интегрирован с Signal Performance Tracker:
    - Читает из order:{id}, signal:{id}
    - Использует trades:closed stream
    - Совместим с stats:{strategy}:{symbol}:{tf}
    """

    def __init__(self, config: RepoConfig | None = None):
        """
        Инициализация репозитория.

        Args:
            config: Конфигурация (опционально)
        """
        self.config = config or RepoConfig()
        self.logger = setup_logger("Repository")

        self.r = redis.from_url(self.config.redis_url, decode_responses=True)

        try:
            self.r.ping()
            self.logger.info("✅ Redis подключение установлено")
        except Exception as e:
            self.logger.error(f"❌ Ошибка подключения к Redis: {e}")
            raise

    def _norm_map(self, m: dict[str, Any]) -> dict[str, str]:
        out: dict[str, str] = {}
        for k, v in (m or {}).items():
            if v is None:
                continue
            out[str(k)] = str(v)
        return out

    def read_signal(self, signal_id: str) -> Signal | None:
        """
        Чтение сигнала по ID.

        Args:
            signal_id: ID сигнала

        Returns:
            Объект Signal или None
        """
        try:
            data = self.r.hgetall(f"signal:{signal_id}")

            if not data:
                return None

            return Signal(
                signal_id=signal_id,
                symbol=data.get("symbol", "UNKNOWN"),
                strategy=data.get("strategy", "unknown"),
                direction=data.get("direction", "LONG"),
                price=float(data.get("price", 0.0)),
                ts=float(data.get("timestamp", 0)) / 1000.0,  # ms → seconds
                confidence=float(data["confidence"]) if data.get("confidence") else None,
                score=float(data["score"]) if data.get("score") else None,
                source=data.get("source"),
                atr=float(data["atr"]) if data.get("atr") else None,
                timeframe=data.get("tf"),
                metadata=data,
            )

        except Exception as e:
            self.logger.error(f"❌ Ошибка чтения сигнала {signal_id} из Redis: {e}")

        # Fallback to Postgres
        try:
            import services.analytics_db as analytics_db
            db_row = analytics_db.fetch_signal_by_id(signal_id)
            if db_row:
                side_val = db_row.get("side", 1)
                direction = "LONG" if side_val > 0 else "SHORT"

                return Signal(
                    signal_id=signal_id,
                    symbol=db_row.get("symbol", "UNKNOWN"),
                    strategy=db_row.get("setup_type", "unknown"),
                    direction=direction,
                    price=float(db_row.get("price_at_signal", 0.0)),
                    ts=db_row.get("ts_signal").timestamp() if db_row.get("ts_signal") else 0.0,
                    confidence=float(db_row.get("final_score", 0)) / 100.0,
                    score=float(db_row.get("final_score", 0)),
                    source=None,
                    atr=float(db_row["atr_1m"]) if db_row.get("atr_1m") else None,
                    timeframe=None,
                    metadata=dict(db_row),
                )
        except Exception as e:
            self.logger.error(f"❌ Ошибка чтения сигнала {signal_id} из Postgres: {e}")

        return None

    def read_order(self, order_id: str) -> Order | None:
        """
        Чтение ордера по ID.

        Args:
            order_id: ID ордера

        Returns:
            Объект Order или None
        """
        try:
            data = self.r.hgetall(f"order:{order_id}")

            if not data:
                return None

            return _redis_row_to_order(order_id, data)

        except Exception as e:
            self.logger.error(f"❌ Ошибка чтения ордера {order_id} из Redis: {e}")

        # Fallback to Postgres
        try:
            import services.analytics_db as analytics_db
            db_row = analytics_db.fetch_trade_by_order_id(order_id)
            if db_row:
                return _pg_row_to_order(order_id, db_row)
        except Exception as e:
            self.logger.error(f"❌ Ошибка чтения ордера {order_id} из Postgres: {e}")

        return None

    def read_closed_trades(
        self,
        limit: int = 1000,
        strategy: str | None = None,
        symbol: str | None = None,
        tf: str | None = None,
    ) -> list[Order]:
        """
        Читаем trades:closed stream и строим сущности для аналитики.

        Важно:
          - compact-stream режим поддерживается автоматически, потому что order_id всегда есть,
            а детали читаем из order:{id}.
          - оптимизация: hash остаётся source-of-truth; hydrate_batch даёт возможность
            корректно прожевать частичные/legacy payload без падений.
        """
        try:
            messages = self.r.xrevrange("trades:closed", count=int(limit)) or []

            raw_items = [self._norm_map(fields or {}) for _id, fields in messages]
            hydrated = hydrate_trade_closed_batch(
                self.r,
                raw_items,
                require_closed=False,
                merge_precedence="hash",
            )

            orders = []
            for f in hydrated:
                oid = str(f.get("order_id") or f.get("id") or "").strip()
                if not oid:
                    continue
                order = self.read_order(oid)
                if order:
                    orders.append(order)

            self.logger.info(f"✅ Прочитано {len(orders)} закрытых сделок")
            return orders

        except Exception as e:
            self.logger.error(f"❌ Ошибка чтения закрытых сделок из Redis: {e}")

        # Fallback to Postgres
        try:
            self.logger.info("📡 Falling back to Postgres for closed trades...")
            import services.analytics_db as analytics_db
            db_rows = analytics_db.fetch_trades_closed(limit=limit, symbol=symbol, source=None)

            orders = [_pg_row_to_order(row["order_id"], row) for row in db_rows]

            self.logger.info(f"✅ Прочитано {len(orders)} закрытых сделок из Postgres")
            return orders
        except Exception as ex:
            self.logger.error(f"❌ Ошибка чтения закрытых сделок из Postgres: {ex}")
            return []

    def iter_signals(
        self,
        symbol: str | None = None,
        strategy: str | None = None,
        since_ts: float | None = None,
        until_ts: float | None = None,
        limit: int = 10000,
    ) -> Iterator[Signal]:
        """
        Итерация по сигналам из Redis.

        Args:
            symbol: Фильтр по символу
            strategy: Фильтр по стратегии
            since_ts: Начало периода (Unix time в секундах)
            until_ts: Конец периода (Unix time в секундах)
            limit: Максимальное количество

        Yields:
            Объекты Signal
        """
        try:
            orders = self.read_closed_trades(
                limit=limit,
                strategy=strategy,
                symbol=symbol,
            )

            for order in orders:
                if since_ts and order.entry_time < since_ts:
                    continue
                if until_ts and order.entry_time > until_ts:
                    continue

                signal = self.read_signal(order.order_id)
                if signal:
                    yield signal

        except Exception as e:
            self.logger.error(f"❌ Ошибка итерации по сигналам: {e}")

    def compute_pnl_usd(self, order: Order) -> float:
        """
        Вычисление P/L в USD для ордера.

        Args:
            order: Объект Order

        Returns:
            P/L в USD
        """
        if order.pnl_usd is not None:
            return order.pnl_usd

        if order.exit_price is None or order.entry_price == 0:
            return 0.0

        if order.direction == "LONG":
            return (order.exit_price - order.entry_price) * order.lot
        else:
            return (order.entry_price - order.exit_price) * order.lot

    def get_trade_events(self, order_id: str) -> list[dict[str, Any]]:
        """
        Получение событий по сделке из events:trades stream.

        Args:
            order_id: ID ордера

        Returns:
            Список событий
        """
        try:
            messages = self.r.xrevrange(RS.EVENTS_TRADES, count=1000)

            events = [
                {"id": msg_id, **data}
                for msg_id, data in messages
                if data.get("order_id") == order_id
            ]

            # Сортируем по времени (старые первыми)
            events.reverse()

            return events

        except Exception as e:
            self.logger.error(f"❌ Ошибка получения событий: {e}")
            return []
