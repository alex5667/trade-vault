from utils.time_utils import get_ny_time_millis

"""
Reporting Service - Генерация отчётов и уведомлений по торговым сигналам.

Основные функции:
- Формирование агрегированных отчётов по стратегиям
- Постраничная выборка закрытых сделок
- Отправка уведомлений в Telegram
- API для запросов статистики
- Периодические сводки (ежедневные, еженедельные)

Интеграция:
- Использует Stats Aggregator для получения метрик
- Читает данные из Redis
- Отправляет сообщения через Telegram Bot API
"""

import html
import json
import os
from datetime import datetime
from typing import Any, cast

from redis import Redis  # type: ignore

from common.log import setup_logger
from core.redis_client import get_redis
from core.redis_keys import STREAM_RETENTION
from core.redis_keys import RedisStreams as RS


class ReportingService:
    """
    Сервис генерации отчётов и уведомлений.
    
    Предоставляет API для получения статистики и отправки
    уведомлений в различные каналы (Telegram, и т.д.).
    """

    def __init__(
        self,
        redis_url: str | None = None,
        telegram_config: dict | None = None
    ):
        """
        Инициализация Reporting Service.
        
        Args:
            redis_url: URL для подключения к Redis (опционально)
            telegram_config: Устарел, сохранен для обратной совместимости
        """
        # Настройка логирования
        self.logger = setup_logger("ReportingService")

        # Redis клиент
        self.redis: Redis
        if redis_url:
            import redis as redis_lib  # type: ignore
            self.redis = redis_lib.from_url(redis_url, decode_responses=True)
        else:
            self.redis = get_redis()

        # Проверка подключения
        try:
            self.redis.ping()
            self.logger.info("✅ Redis подключение установлено")
        except Exception as e:
            self.logger.error(f"❌ Ошибка подключения к Redis: {e}")
            raise

        # Теперь отправка через Redis stream, telegram_config не требуется
        self.telegram_enabled = True  # Всегда включено, отправка через Redis
        self.logger.info("📊 Reporting Service инициализирован (отправка через notify:telegram stream)")

    # ============================================================
    # Helpers (format / safe parsing / aggregation)
    # ============================================================

    def _to_int(self, v, d=0) -> int:
        try:
            return int(float(v)) if v is not None and str(v).strip() != "" else d
        except Exception:
            return d

    def _to_float(self, v, d=0.0) -> float:
        try:
            return float(str(v).replace(",", ".")) if v is not None and str(v).strip() != "" else d
        except Exception:
            return d

    def _safe_div(self, a: float, b: float, default: float = 0.0) -> float:
        return (a / b) if abs(b) > 1e-12 else default

    def _ms_to_hhmm(self, ms: float) -> str:
        ms = self._to_float(ms, 0.0)
        if ms <= 0:
            return "0m"
        sec = int(ms // 1000)
        m = sec // 60
        h = m // 60
        m = m % 60
        if h > 0:
            return f"{h}h {m}m"
        return f"{m}m"

    def _fmt_money(self, v: float, digits: int = 2) -> str:
        v = self._to_float(v)
        return f"{v:+.{digits}f}"

    def _fmt_pct(self, v: float, digits: int = 2) -> str:
        v = self._to_float(v)
        return f"{v:+.{digits}f}%"

    def _fmt_rate(self, hits: int, total: int, digits: int = 1) -> str:
        hits = self._to_int(hits)
        total = self._to_int(total)
        r = self._safe_div(hits * 100.0, total, 0.0)
        return f"{hits} ({r:.{digits}f}%)"

    def _infer_ts_ms(self, ts) -> int:
        """
        Нормализует timestamp в ms.
        - если похоже на секунды (<= 1e11) -> *1000
        - если уже ms -> как есть
        """
        t = self._to_float(ts, 0.0)
        if t <= 0:
            return 0
        if t < 1e11:  # seconds
            return int(t * 1000)
        return int(t)

    def _compute_r(self, direction: str, entry: float, sl: float, exit_price: float) -> float:
        entry = self._to_float(entry)
        sl = self._to_float(sl)
        exit_price = self._to_float(exit_price)
        risk = abs(entry - sl)
        if risk <= 1e-12:
            return 0.0
        if direction.upper() == "LONG":
            reward = exit_price - entry
        else:
            reward = entry - exit_price
        return reward / risk

    # ============================================================
    # API для получения отчётов
    # ============================================================

    def get_strategy_report(
        self,
        strategy: str,
        symbol: str | None = None,
        tf: str | None = None,
        include_sources: bool = True
    ) -> dict[str, Any]:
        """
        Получение отчёта по стратегии.
        
        Args:
            strategy: Название стратегии
            symbol: Символ (опционально, для фильтрации)
            tf: Таймфрейм (опционально)
            include_sources: Включить разбивку по источникам (OrderFlow, AggregatedHub-V2, etc)
            
        Returns:
            Словарь с агрегированной статистикой
        """
        from services.stats_aggregator import StatsAggregator

        try:
            if symbol and tf:
                # Детальная статистика по конкретной комбинации
                stats = StatsAggregator.get_stats(self.redis, strategy, symbol, tf)

                # Добавляем разбивку по источникам
                if include_sources and stats:
                    sources = StatsAggregator.get_strategy_sources(self.redis, strategy, symbol, tf)
                    stats["sources"] = {}

                    for source in sources:
                        source_stats = StatsAggregator.get_stats_by_source(
                            self.redis, strategy, symbol, tf, source
                        )
                        if source_stats:
                            stats["sources"][source] = source_stats

                return stats or {}
            elif symbol:
                # Агрегация по всем TF для данного символа (С ПОЛНЫМИ метриками)
                tfs = StatsAggregator.get_strategy_timeframes(self.redis, strategy, symbol)

                combined: dict[str, Any] = {
                    "strategy": strategy,
                    "symbol": symbol,
                    "total_trades": 0,
                    "wins": 0,
                    "losses": 0,
                    "breakevens": 0,

                    "total_pnl": 0.0,          # net
                    "total_pnl_gross": 0.0,    # gross
                    "total_fees": 0.0,

                    "gross_profit": 0.0,
                    "gross_loss": 0.0,

                    "sum_r": 0.0,
                    "sum_duration_ms": 0.0,

                    "missed_profit_total": 0.0,
                    "missed_profit_trades": 0,
                    "giveback_total": 0.0,

                    "trailing_started": 0,
                    "trailing_stop_hits": 0,
                    "trailing_moves_total": 0.0,

                    "tp1_hits": 0,
                    "tp2_hits": 0,
                    "tp3_hits": 0,
                    "tp1_then_sl": 0,
                    "tp2_then_sl": 0,
                    "tp3_then_sl": 0,

                    "timeframes": {}
                }

                for tf_item in tfs:
                    s = StatsAggregator.get_stats(self.redis, strategy, symbol, tf_item)
                    if not s:
                        continue

                    combined["timeframes"][tf_item] = s

                    combined["total_trades"] += self._to_int(s.get("total_trades"))
                    combined["wins"] += self._to_int(s.get("wins"))
                    combined["losses"] += self._to_int(s.get("losses"))
                    combined["breakevens"] += self._to_int(s.get("breakevens"))

                    combined["total_pnl"] += self._to_float(s.get("total_pnl"))
                    combined["total_pnl_gross"] += self._to_float(s.get("total_pnl_gross"))
                    combined["total_fees"] += self._to_float(s.get("total_fees"))

                    combined["gross_profit"] += self._to_float(s.get("gross_profit"))
                    combined["gross_loss"] += self._to_float(s.get("gross_loss"))

                    combined["sum_r"] += self._to_float(s.get("sum_r"))
                    combined["sum_duration_ms"] += self._to_float(s.get("sum_duration_ms"))

                    combined["missed_profit_total"] += self._to_float(s.get("missed_profit_total"))
                    combined["missed_profit_trades"] += self._to_int(s.get("missed_profit_trades"))
                    combined["giveback_total"] += self._to_float(s.get("giveback_total"))

                    combined["trailing_started"] += self._to_int(s.get("trailing_started"))
                    combined["trailing_stop_hits"] += self._to_int(s.get("trailing_stop_hits"))
                    combined["trailing_moves_total"] += self._to_float(s.get("trailing_moves_total"))

                    combined["tp1_hits"] += self._to_int(s.get("tp1_hits"))
                    combined["tp2_hits"] += self._to_int(s.get("tp2_hits"))
                    combined["tp3_hits"] += self._to_int(s.get("tp3_hits"))
                    combined["tp1_then_sl"] += self._to_int(s.get("tp1_then_sl"))
                    combined["tp2_then_sl"] += self._to_int(s.get("tp2_then_sl"))
                    combined["tp3_then_sl"] += self._to_int(s.get("tp3_then_sl"))

                total = self._to_float(combined["total_trades"])
                if total > 0:
                    combined["winrate"] = round(self._to_float(combined["wins"]) / total * 100.0, 2)
                    combined["avg_pnl"] = round(self._to_float(combined["total_pnl"]) / total, 2)
                    combined["avg_r"] = round(self._to_float(combined["sum_r"]) / total, 4)
                    combined["avg_duration_ms"] = round(self._to_float(combined["sum_duration_ms"]) / total, 0)
                    combined["profit_factor"] = round(
                        self._safe_div(self._to_float(combined["gross_profit"]), self._to_float(combined["gross_loss"]), 0.0), 3
                    )
                    missed_trades = self._to_float(combined["missed_profit_trades"])
                    if missed_trades > 0:
                        combined["missed_profit_avg"] = round(
                            self._to_float(combined["missed_profit_total"]) / missed_trades, 2
                        )
                    else:
                        combined["missed_profit_avg"] = 0.0

                combined["trailing_effectiveness"] = round(
                    self._safe_div(self._to_float(combined["trailing_stop_hits"]), self._to_float(combined["trailing_started"]), 0.0) * 100.0, 2
                )

                return combined
            else:
                # Общая сводка по стратегии
                return self._get_strategy_summary(strategy)

        except Exception as e:
            self.logger.error(f"❌ Ошибка получения отчёта: {e}", exc_info=True)
            return {}

    def _get_strategy_summary(self, strategy: str) -> dict[str, Any]:
        """Внутренний метод для агрегации сводки по стратегии."""
        from services.stats_aggregator import StatsAggregator
        
        symbols = StatsAggregator.get_strategy_symbols(self.redis, strategy)
        combined: dict[str, Any] = {
            "strategy": strategy,
            "total_trades": 0, "wins": 0, "losses": 0, "breakevens": 0,
            "total_pnl": 0.0, "total_pnl_gross": 0.0, "total_fees": 0.0,
            "gross_profit": 0.0, "gross_loss": 0.0,
            "sum_r": 0.0, "sum_duration_ms": 0.0,
            "missed_profit_total": 0.0, "missed_profit_trades": 0, "giveback_total": 0.0,
            "trailing_started": 0, "trailing_stop_hits": 0, "trailing_moves_total": 0.0,
        }
        for sym in symbols:
            s = self.get_strategy_report(strategy, sym, include_sources=False)
            if not s:
                continue
            combined["total_trades"] += self._to_int(s.get("total_trades"))
            combined["wins"] += self._to_int(s.get("wins"))
            combined["losses"] += self._to_int(s.get("losses"))
            combined["breakevens"] += self._to_int(s.get("breakevens"))
            combined["total_pnl"] += self._to_float(s.get("total_pnl"))
            combined["total_pnl_gross"] += self._to_float(s.get("total_pnl_gross"))
            combined["total_fees"] += self._to_float(s.get("total_fees"))
            combined["gross_profit"] += self._to_float(s.get("gross_profit"))
            combined["gross_loss"] += self._to_float(s.get("gross_loss"))
            combined["sum_r"] += self._to_float(s.get("sum_r"))
            combined["sum_duration_ms"] += self._to_float(s.get("sum_duration_ms"))
            combined["missed_profit_total"] += self._to_float(s.get("missed_profit_total"))
            combined["missed_profit_trades"] += self._to_int(s.get("missed_profit_trades"))
            combined["giveback_total"] += self._to_float(s.get("giveback_total"))
            combined["trailing_started"] += self._to_int(s.get("trailing_started"))
            combined["trailing_stop_hits"] += self._to_int(s.get("trailing_stop_hits"))
            combined["trailing_moves_total"] += self._to_float(s.get("trailing_moves_total"))

        total = self._to_float(combined["total_trades"])
        if total > 0:
            combined["winrate"] = round(self._to_float(combined["wins"]) / total * 100.0, 2)
            combined["avg_pnl"] = round(self._to_float(combined["total_pnl"]) / total, 2)
            combined["avg_r"] = round(self._to_float(combined["sum_r"]) / total, 4)
            combined["avg_duration_ms"] = round(self._to_float(combined["sum_duration_ms"]) / total, 0)
            combined["profit_factor"] = round(
                self._safe_div(self._to_float(combined["gross_profit"]), self._to_float(combined["gross_loss"]), 0.0), 3
            )
            missed_trades = self._to_float(combined["missed_profit_trades"])
            if missed_trades > 0:
                combined["missed_profit_avg"] = round(
                    self._to_float(combined["missed_profit_total"]) / missed_trades, 2
                )
            else:
                combined["missed_profit_avg"] = 0.0

        combined["trailing_effectiveness"] = round(
            self._safe_div(self._to_float(combined["trailing_stop_hits"]), self._to_float(combined["trailing_started"]), 0.0) * 100.0, 2
        )
        return combined

    def get_all_strategies_report(self) -> dict[str, Any]:
        """
        Получение отчёта по всем стратегиям.
        
        Returns:
            Словарь с данными по каждой стратегии
        """
        from services.stats_aggregator import StatsAggregator

        try:
            raw_strategies = cast(Any, self.redis.smembers("stats:strategies"))
            strategies = [v.decode() if isinstance(v, bytes) else str(v) for v in raw_strategies] if raw_strategies else []

            result: dict[str, Any] = {
                "timestamp": get_ny_time_millis(),
                "strategies": {},
                "total_trades": 0,
                "total_wins": 0,
                "total_losses": 0,
                "total_pnl": 0.0
            }

            for strategy in strategies:
                summary = self._get_strategy_summary(strategy)
                if summary:
                    result["strategies"][strategy] = summary
                    result["total_trades"] += int(summary.get("total_trades", 0))
                    result["total_wins"] += int(summary.get("wins", 0))
                    result["total_losses"] += int(summary.get("losses", 0))
                    result["total_pnl"] += float(summary.get("total_pnl", 0.0))

            # Общий winrate
            if result["total_trades"] > 0:
                result["overall_winrate"] = round(
                    result["total_wins"] / result["total_trades"] * 100.0, 2
                )
                result["avg_pnl"] = round(
                    result["total_pnl"] / result["total_trades"], 2
                )

            return result

        except Exception as e:
            self.logger.error(f"❌ Ошибка получения общего отчёта: {e}", exc_info=True)
            return {}

    def get_recent_trades(
        self,
        strategy: str,
        symbol: str,
        tf: str,
        limit: int = 50,
        offset: int = 0
    ) -> list[dict[str, Any]]:
        """
        Получение списка недавних сделок с пагинацией.
        
        Args:
            strategy: Название стратегии
            symbol: Символ
            tf: Таймфрейм
            limit: Количество записей на страницу
            offset: Смещение (для пагинации)
            
        Returns:
            Список сделок
        """
        try:
            list_key = f"closed:{strategy}:{symbol}:{tf}"

            # Получаем список ID сделок (от конца, т.е. новые первыми)
            # Redis LRANGE: 0 = начало, -1 = конец
            # Для получения последних N элементов используем отрицательные индексы
            start = -(offset + limit)
            end = -(offset + 1) if offset > 0 else -1

            trade_ids = self.redis.lrange(list_key, start, end)

            # Приводим к списку на случай если это Awaitable (в тестах) и переворачиваем
            if not isinstance(trade_ids, list):
                trade_ids = list(trade_ids)  # type: ignore[arg-type]
            trade_ids.reverse()

            # Получаем детали каждой сделки
            trades = []
            for trade_id in trade_ids:
                order_data = self.redis.hgetall(f"order:{trade_id}")
                if order_data and isinstance(order_data, dict):
                    trades.append(order_data)

            return trades

        except Exception as e:
            self.logger.error(f"❌ Ошибка получения списка сделок: {e}", exc_info=True)
            return []

    def get_trade_details(self, order_id: str) -> dict[str, Any] | None:
        """
        Получение детальной информации о сделке.
        
        Args:
            order_id: ID ордера
            
        Returns:
            Словарь с данными сделки или None
        """
        try:
            order_data = self.redis.hgetall(f"order:{order_id}")
            if not order_data or not isinstance(order_data, dict):
                return None

            signal_id = (
                order_data.get("signal_id")
                or order_data.get("sid")
                or order_data.get("signal")
                or order_data.get("signalId")
            )

            signal_data = {}
            if signal_id:
                sig_raw = self.redis.hgetall(f"signal:{signal_id}")
                if isinstance(sig_raw, dict):
                    signal_data = sig_raw

            closed_summary = {}
            if signal_id:
                closed_raw = self.redis.hgetall(f"trades:closed:{signal_id}")
                if isinstance(closed_raw, dict):
                    closed_summary = closed_raw

            events = self._get_trade_events(order_id)

            return {
                "order": order_data,
                "signal": signal_data,
                "closed": closed_summary,
                "events": events
            }
        except Exception as e:
            self.logger.error(f"❌ Ошибка получения деталей сделки: {e}", exc_info=True)
            return None

    def _get_trade_events(self, order_id: str, limit: int = 1000) -> list[dict[str, Any]]:
        """Получение событий по сделке из потока events:trades"""
        try:
            events = cast(Any, self.redis.xrevrange(RS.EVENTS_TRADES, count=limit))

            trade_events = []
            for event_id, event_data in events:
                if event_data.get("order_id") == order_id:
                    trade_events.append({"id": event_id, **event_data})

            trade_events.reverse()
            return trade_events

        except Exception as e:
            self.logger.error(f"❌ Ошибка получения событий: {e}", exc_info=True)
            return []

    # ============================================================
    # Уведомления в Telegram
    # ============================================================

    def send_telegram_message(
        self,
        text: str,
        parse_mode: str = "HTML",
        tags: list[str] | None = None,
        severity: str = "info",              # info|warn|error
        dedup_key: str | None = None,     # ключ дедупликации
        meta: dict[str, Any] | None = None
    ) -> bool:
        """
        Отправка сообщения в Telegram через Redis stream notify:telegram (расширенная).

        Новые поля:
        - tags: список тегов
        - severity: уровень
        - dedup_key: дедупликация на стороне gateway/бота
        - meta: JSON с контекстом (для логов/аналитики)
        """
        notify_stream = os.getenv("NOTIFY_STREAM", RS.NOTIFY_TELEGRAM)
        try:
            # Recommendation C: Circuit Breaker for Telegram notifications
            try:
                q_len = self.redis.xlen(notify_stream)
                if isinstance(q_len, int) and q_len > 10000:
                    self.logger.error(f"🔥 Telegram stream overloaded (>{q_len}). Dropping message to prevent cascading failure.")
                    return False
            except Exception as e:
                self.logger.warning(f"⚠️ Circuit breaker check failed: {e}")
                # continue if redis check fails but ping was okay previously

            message_data: dict[str, Any] = {
                "type": "report",
                "text": text,
                "parse_mode": parse_mode,
                "source": "ReportingService",
                "severity": severity,
                "timestamp": str(get_ny_time_millis()),
            }

            if tags:
                message_data["tags"] = ",".join([t.strip() for t in tags if t.strip()])

            if dedup_key:
                # Recommendation F: server-side dedup before xadd
                # TTL 6 hours to prevent spam during restarts/retries
                d_key = f"dedup:reporting:{dedup_key}"
                if not self.redis.set(d_key, "1", nx=True, ex=6*3600):
                    self.logger.info(f"⏭️ Dedup hit for reporting: {dedup_key}, skip xadd")
                    return True # already processed

                message_data["dedup_key"] = dedup_key

            if meta:
                try:
                    message_data["meta"] = json.dumps(meta, ensure_ascii=False)
                except Exception:
                    message_data["meta"] = "{}"

            msg_id = self.redis.xadd(notify_stream, cast(dict[Any, Any], message_data), maxlen=STREAM_RETENTION.get(notify_stream, STREAM_RETENTION[RS.NOTIFY_TELEGRAM]))
            self.logger.info(f"✅ Отчет опубликован в {notify_stream}: msg_id={msg_id}, type={message_data.get('type')}, text_len={len(text)}")

            # Дополнительная проверка: убеждаемся, что сообщение попало в stream
            try:
                stream_len = self.redis.xlen(notify_stream)
                self.logger.debug(f"📊 Длина stream {notify_stream}: {stream_len} сообщений (после публикации)")
            except Exception as check_e:
                self.logger.warning(f"⚠️ Не удалось проверить длину stream после публикации: {check_e}")

            return True

        except Exception as e:
            self.logger.error(f"❌ Ошибка отправки отчета в Redis stream {notify_stream}: {e}", exc_info=True)
            return False

    def notify_trade_closed(self, trade_summary: dict[str, Any]):
        try:
            strategy = trade_summary.get("strategy", "Unknown")
            symbol = trade_summary.get("symbol", "UNKNOWN")
            tf = trade_summary.get("tf", "tick")
            direction = (trade_summary.get("direction", "LONG")).upper()
            source = trade_summary.get("source") or "unknown"
            close_reason = trade_summary.get("close_reason", "Unknown")
            tp_count = self._to_int(trade_summary.get("tp_count") or trade_summary.get("tp_hits") or 0)

            order_id = (
                trade_summary.get("order_id")
                or trade_summary.get("position_id")
                or trade_summary.get("id")
                or ""
            )
            order_id = str(order_id) if order_id is not None else ""

            summary = None
            if order_id:
                order = self.redis.hgetall(f"order:{order_id}")
                if order and isinstance(order, dict):
                    summary = self._build_trade_summary_from_order(order)
            if summary is None:
                summary = self._build_trade_summary_from_order(dict(trade_summary))

            pnl_net = self._to_float(summary.get("pnl_net"))
            pnl_gross = self._to_float(summary.get("pnl_gross"))
            fees = self._to_float(summary.get("fees"))
            r = self._to_float(summary.get("r"))
            duration_ms = self._to_float(summary.get("duration_ms"))
            mae = self._to_float(summary.get("mae"))
            mfe = self._to_float(summary.get("mfe"))
            giveback = self._to_float(summary.get("giveback"))
            missed_profit = self._to_float(summary.get("missed_profit"))
            trailing_started = self._to_int(summary.get("trailing_started"))
            trailing_moves = self._to_float(summary.get("trailing_moves"))
            trailing_stop_hit = self._to_int(summary.get("trailing_stop_hit"))
            result = summary.get("result") or ("win" if pnl_net > 1e-9 else ("loss" if pnl_net < -1e-9 else "breakeven"))

            pnl_pct = self._to_float(trade_summary.get("pnl_pct") or 0.0)

            result_emoji = "✅" if result == "win" else ("❌" if result == "loss" else "➖")
            direction_emoji = "📈" if direction == "LONG" else "📉"
            severity = "error" if result == "loss" else ("warn" if (missed_profit > 0 or giveback > 0) else "info")

            dur_str = self._ms_to_hhmm(duration_ms)

            lines = [
                f"{result_emoji} <b>Сделка закрыта</b>",
                "",
                f"<b>Стратегия:</b> {html.escape(strategy)}",
                f"<b>Источник:</b> {html.escape(str(source))}",
                f"<b>Инструмент:</b> {html.escape(symbol)} ({html.escape(tf)})",
                f"<b>Направление:</b> {direction_emoji} {direction}",
                f"<b>Причина:</b> {html.escape(str(close_reason))}",
                f"<b>TP достигнуто:</b> {tp_count}/3",
            ]
            if order_id:
                lines.append(f"<b>Order ID:</b> <code>{html.escape(order_id)}</code>")

            lines += [
                "",
                f"<b>P/L:</b> Net <b>{pnl_net:+.2f}</b> ({pnl_pct:+.2f}%)",
                f"<b>Gross / Fees:</b> {pnl_gross:+.2f} / {fees:+.2f}",
                f"<b>R:</b> {r:+.3f}",
                f"<b>Duration:</b> {dur_str}",
            ]

            extra = []
            if abs(mfe) > 1e-12 or abs(mae) > 1e-12:
                extra.append(f"MFE {mfe:+.2f}, MAE {mae:+.2f}")
            if giveback > 1e-12:
                extra.append(f"Giveback {giveback:+.2f}")
            if missed_profit > 1e-12:
                extra.append(f"Missed {missed_profit:+.2f}")
            if extra:
                lines.append("<b>Экстремумы:</b> " + " | ".join(extra))

            if trailing_started > 0 or trailing_stop_hit > 0:
                lines.append(f"<b>Trailing:</b> started {trailing_started}, moves {trailing_moves:.2f}, hit {trailing_stop_hit}")

            msg = "\n".join(lines)

            tags = ["trade", "closed", strategy, symbol, tf, source]
            meta = {
                "strategy": strategy, "symbol": symbol, "tf": tf, "source": source,
                "direction": direction, "result": result, "close_reason": close_reason,
                "pnl_net": pnl_net, "pnl_gross": pnl_gross, "fees": fees, "r": r,
                "duration_ms": duration_ms, "mae": mae, "mfe": mfe,
                "giveback": giveback, "missed_profit": missed_profit,
                "trailing_started": trailing_started, "trailing_moves": trailing_moves,
                "trailing_stop_hit": trailing_stop_hit, "order_id": order_id,
            }

            self.send_telegram_message(
                msg,
                tags=tags,
                severity=severity,
                dedup_key=(f"trade_closed:{order_id}" if order_id else None),
                meta=meta,
            )

        except Exception as e:
            self.logger.error(f"❌ Ошибка отправки уведомления о сделке: {e}", exc_info=True)

    def send_daily_summary(self, include_sources: bool = True):
        """
        Ежедневная сводка: использует StatsAggregator.get_all_stats() и агрегирует по стратегиям.
        """
        try:
            from services.stats_aggregator import StatsAggregator

            all_stats = StatsAggregator.get_all_stats(self.redis)
            if not all_stats:
                self.logger.info("📊 Нет данных для ежедневной сводки")
                return

            # aggregate by strategy from keys "strategy:symbol:tf"
            by_strategy: dict[str, dict[str, Any]] = {}
            overall: dict[str, Any] = {}

            for key, st in all_stats.items():
                # key format: "{strategy}:{symbol}:{tf}"
                parts = key.split(":")
                if len(parts) < 3:
                    continue
                strat = parts[0]
                if strat not in by_strategy:
                    by_strategy[strat] = {}
                by_strategy[strat] = self._accumulate_stats(by_strategy[strat], st)
                overall = self._accumulate_stats(overall, st)

            overall = self._finalize_accumulated(overall)
            if self._to_int(overall.get("total_trades"), 0) == 0:
                self.logger.info("📭 Пропускаем ежедневную сводку: сделок нет")
                return

            today = datetime.now().strftime("%Y-%m-%d")

            lines = [
                "📅 <b>Ежедневная сводка (расширенная)</b>",
                f"🗓️ {today}",
                f"{'='*40}\n",
                "<b>📈 ОБЩИЕ</b>",
                f"Сделок: <b>{overall.get('total_trades', 0)}</b>",
                f"W/L/BE: <b>{overall.get('wins', 0)}/{overall.get('losses', 0)}/{overall.get('breakeven', 0)}</b>",
                f"WinRate: <b>{self._to_float(overall.get('winrate'), 0.0):.1f}%</b>",
            ]

            total_pnl = self._to_float(overall.get("total_pnl"), 0.0)
            avg_pnl = self._to_float(overall.get("avg_pnl"), 0.0)
            total_gross = self._to_float(overall.get("total_pnl_gross"), 0.0)
            total_fees = self._to_float(overall.get("total_fees"), 0.0)
            lines += [
                f"Net P/L: <b>{total_pnl:+.2f}</b> | Avg: <b>{avg_pnl:+.2f}</b>",
                f"Gross / Fees: <b>{total_gross:+.2f}</b> / <b>{total_fees:+.2f}</b>",
                f"PF: <b>{self._to_float(overall.get('profit_factor'), 0.0):.2f}</b> | Avg R: <b>{self._to_float(overall.get('avg_r'), 0.0):+.3f}</b>",
                f"Avg Duration: <b>{self._ms_to_hhmm(self._to_float(overall.get('avg_duration_ms'), 0.0))}</b>",
                "",
            ]

            # per strategy
            lines.append("<b>📊 ПО СТРАТЕГИЯМ</b>")
            for strat in sorted(by_strategy.keys()):
                acc = self._finalize_accumulated(by_strategy[strat])
                t = self._to_int(acc.get("total_trades"), 0)
                if t <= 0:
                    continue

                net = self._to_float(acc.get("total_pnl"), 0.0)
                avg_pnl_strat = self._to_float(acc.get("avg_pnl"), 0.0)

                lines.append(
                    f"• <b>{html.escape(strat)}</b>: {t} | WR {self._to_float(acc.get('winrate'), 0.0):.1f}% | "
                    f"Net {net:+.2f} (Avg {avg_pnl_strat:+.2f}) | PF {self._to_float(acc.get('profit_factor'), 0.0):.2f} | AvgR {self._to_float(acc.get('avg_r'), 0.0):+.3f}"
                )

            # sources (optional)
            if include_sources:
                lines.append("")
                lines.append("<b>📡 ПО ИСТОЧНИКАМ</b>")
                srcs = self.get_sources_summary()
                if not srcs:
                    lines.append("• Нет данных по источникам")
                else:
                    for src, acc in sorted(srcs.items(), key=lambda x: self._to_int(x[1].get("total_trades"), 0), reverse=True):
                        t = self._to_int(acc.get("total_trades"), 0)
                        net = self._to_float(acc.get("total_pnl"), 0.0)

                        lines.append(
                            f"• <b>{html.escape(src)}</b>: {t} | WR {self._to_float(acc.get('winrate'), 0.0):.1f}% | "
                            f"Net {net:+.2f} | PF {self._to_float(acc.get('profit_factor'), 0.0):.2f} | AvgR {self._to_float(acc.get('avg_r'), 0.0):+.3f}"
                        )

            text = "\n".join(lines)
            self.send_telegram_message(
                text,
                tags=["report", "daily"],
                severity="info",
                dedup_key=f"daily_summary:{today}",
                meta={"date": today, "total_trades": self._to_int(overall.get("total_trades"), 0)},
            )

            self.logger.info("📊 Ежедневная сводка отправлена")

        except Exception as e:
            self.logger.error(f"❌ Ошибка отправки ежедневной сводки: {e}", exc_info=True)

    def send_strategy_report(self, strategy: str, symbol: str, tf: str = "tick"):
        from services.stats_aggregator import StatsAggregator

        stats = StatsAggregator.get_stats(self.redis, strategy, symbol, tf)
        if not stats:
            self.logger.warning(f"⚠️ Нет данных для {strategy}:{symbol}:{tf}")
            return

        total = self._to_int(stats.get("total_trades"))
        wins = self._to_int(stats.get("wins"))
        losses = self._to_int(stats.get("losses"))
        be = self._to_int(stats.get("breakeven"))

        winrate = self._to_float(stats.get("winrate"))
        total_pnl = self._to_float(stats.get("total_pnl"))
        avg_pnl = self._to_float(stats.get("avg_pnl"))

        total_gross = self._to_float(stats.get("total_pnl_gross"))
        total_fees = self._to_float(stats.get("total_fees"))

        avg_pnl_pct = self._to_float(stats.get("avg_pnl_pct"))
        avg_r = self._to_float(stats.get("avg_r"))
        avg_duration_ms = self._to_float(stats.get("avg_duration_ms"))
        profit_factor = self._to_float(stats.get("profit_factor"))

        tp1 = self._to_int(stats.get("tp1_hits"))
        tp2 = self._to_int(stats.get("tp2_hits"))
        tp3 = self._to_int(stats.get("tp3_hits"))

        tp1_then_sl = self._to_int(stats.get("tp1_then_sl"))
        tp2_then_sl = self._to_int(stats.get("tp2_then_sl"))
        tp3_then_sl = self._to_int(stats.get("tp3_then_sl"))

        trailing_started = self._to_int(stats.get("trailing_started"))
        trailing_hits = self._to_int(stats.get("trailing_stop_hits"))
        trailing_eff = (trailing_hits / trailing_started * 100.0) if trailing_started > 0 else 0.0

        missed_total = self._to_float(stats.get("missed_profit_total"))
        missed_n = self._to_int(stats.get("missed_profit_trades"))
        missed_avg = self._to_float(stats.get("missed_profit_avg"))
        giveback_total = self._to_float(stats.get("giveback_total"))
        giveback_avg = (giveback_total / total) if total > 0 else 0.0

        trailing_moves_total = self._to_float(stats.get("trailing_moves_total"))
        trailing_moves_avg = (trailing_moves_total / total) if total > 0 else 0.0

        def rate(x: int) -> float:
            return (x / total * 100.0) if total > 0 else 0.0

        msg = [
            f"📊 <b>Отчёт: {html.escape(strategy)}:{html.escape(symbol)}:{html.escape(tf)}</b>",
            f"{'='*40}\n",
            "<b>📈 ОСНОВНЫЕ</b>",
            f"Сделок: <b>{total}</b>",
            f"W/L/BE: <b>{wins}/{losses}/{be}</b>",
            f"WinRate: <b>{winrate:.2f}%</b>",
            f"Net P/L: <b>{total_pnl:+.2f}</b> | Avg: <b>{avg_pnl:+.2f}</b> | Avg%: <b>{avg_pnl_pct:+.4f}</b>",
            f"Gross / Fees: <b>{total_gross:+.2f}</b> / <b>{total_fees:+.2f}</b>",
            f"PF: <b>{profit_factor:.2f}</b> | Avg R: <b>{avg_r:+.4f}</b> | Avg Dur: <b>{self._ms_to_hhmm(avg_duration_ms)}</b>",
            "",
            "<b>🎯 TP</b>",
            f"TP1: <b>{tp1}</b> ({rate(tp1):.1f}%) | TP2: <b>{tp2}</b> ({rate(tp2):.1f}%) | TP3: <b>{tp3}</b> ({rate(tp3):.1f}%)",
            f"TP→SL: TP1 <b>{tp1_then_sl}</b> ({rate(tp1_then_sl):.1f}%) | TP2 <b>{tp2_then_sl}</b> ({rate(tp2_then_sl):.1f}%) | TP3 <b>{tp3_then_sl}</b> ({rate(tp3_then_sl):.1f}%)",
            "",
            "<b>🧷 TRAILING</b>",
            f"Started: <b>{trailing_started}</b> | Hits: <b>{trailing_hits}</b> | Eff: <b>{trailing_eff:.1f}%</b> | Moves avg: <b>{trailing_moves_avg:.2f}</b>",
            "",
            "<b>⭐ MISSED / GIVEBACK</b>",
            f"Missed total: <b>{missed_total:+.2f}</b> | trades: <b>{missed_n}</b> | avg: <b>{missed_avg:+.2f}</b>",
            f"Giveback total: <b>{giveback_total:+.2f}</b> | avg/trade: <b>{giveback_avg:+.2f}</b>",
        ]

        # По источникам (коротко)
        sources = StatsAggregator.get_strategy_sources(self.redis, strategy, symbol, tf)
        if sources:
            msg.append("")
            msg.append("<b>📡 ПО ИСТОЧНИКАМ</b>")
            for src in sources:
                s = StatsAggregator.get_stats_by_source(self.redis, strategy, symbol, tf, src)
                if not s:
                    continue
                t = self._to_int(s.get("total_trades"))
                wr = self._to_float(s.get("winrate"))
                pnl_net = self._to_float(s.get("total_pnl"))
                pf = self._to_float(s.get("profit_factor"))
                ar = self._to_float(s.get("avg_r"))
                msg.append(f"• <b>{html.escape(src)}</b>: {t} | WR {wr:.1f}% | Net {pnl_net:+.2f} | PF {pf:.2f} | AvgR {ar:+.4f}")

        self.send_telegram_message(
            "\n".join(msg),
            tags=["report", "strategy", strategy, symbol, tf],
            severity="info",
            dedup_key=f"strategy_report:{strategy}:{symbol}:{tf}",
            meta={"strategy": strategy, "symbol": symbol, "tf": tf},
        )

    def notify_periodic_summary(self, stats: dict[str, Any], period: str = "day"):
        """
        Отправляет сводку результатов за период (гибкий формат).
        
        Может обрабатывать как статистику одной стратегии,
        так и словарь с несколькими стратегиями.
        
        Args:
            stats: Статистика (одна стратегия или словарь стратегий)
            period: Период (day, week, month и т.д.)
        """

        try:
            if not stats:
                self.logger.warning("⚠️ Пустая статистика для периодической сводки")
                return

            # Проверяем формат статистики
            if "strategy" in stats:
                # Статистика одной стратегии
                strat = stats.get("strategy", "Unknown")
                total = stats.get("total_trades", 0)
                winrate = stats.get("winrate", 0)
                total_pnl = stats.get("total_pnl", 0.0)
                avg_pnl = stats.get("avg_pnl", 0.0)

                message = (
                    f"🗓 <b>Итоги за {period}</b>\n\n"
                    f"<b>Стратегия:</b> {html.escape(str(strat))}\n"
                    f"<b>Сделок:</b> {total}\n"
                    f"<b>WinRate:</b> {winrate:.1f}%\n"
                    f"<b>Общий P/L:</b> {total_pnl:+.2f}\n"
                    f"<b>Средний P/L:</b> {avg_pnl:+.2f}"
                )
            else:
                # Сводка по нескольким стратегиям
                message_lines = [f"🗓 <b>Итоги за {period}</b>\n"]

                total_overall = 0
                wins_overall = 0
                pnl_overall = 0.0

                for strat, data in stats.items():
                    if isinstance(data, dict):
                        total = data.get("total_trades", 0)
                        wins = data.get("wins", 0)
                        winrate = data.get("winrate", 0)
                        total_pnl = data.get("total_pnl", 0.0)

                        message_lines.append(
                            f"• <b>{html.escape(strat)}:</b> {total} сделок, "
                            f"WR {winrate:.1f}%, P/L {total_pnl:+.2f}"
                        )

                        total_overall += total
                        wins_overall += wins
                        pnl_overall += total_pnl

                # Добавляем общую сводку
                if total_overall > 0:
                    wr_overall = (wins_overall / total_overall * 100.0)
                    message_lines.append("")
                    message_lines.append(
                        f"<b>Итого:</b> {total_overall} сделок, "
                        f"WR {wr_overall:.1f}%, P/L {pnl_overall:+.2f}"
                    )

                # Добавляем разбивку по источникам
                message_lines.append("\n<b>📊 По источникам:</b>")
                sources_summary = self.get_sources_summary()

                for source, source_stats in sources_summary.items():
                    message_lines.append(
                        f"  • <b>{html.escape(source)}:</b> "
                        f"{source_stats.get('total_trades', 0)} сделок, "
                        f"WR {source_stats.get('winrate', 0):.1f}%, "
                        f"P/L {source_stats.get('total_pnl', 0):+.2f}"
                    )

                message = "\n".join(message_lines)

            self.send_telegram_message(
                message,
                tags=["summary", period],
                severity="info"
            )
            self.logger.info(f"📊 Периодическая сводка за {period} отправлена")

        except Exception as e:
            self.logger.error(f"❌ Ошибка отправки периодической сводки: {e}")

    # ============================================================
    # Экспорт данных
    # ============================================================

    def export_trades_to_json(
        self,
        strategy: str,
        symbol: str,
        tf: str,
        filepath: str,
        include_signal: bool = True,
        include_events: bool = True,
        include_summary: bool = True,
        events_limit: int = 2000,
    ) -> bool:
        """
        Экспорт сделок в JSON файл (расширенный).

        include_summary:
          - добавляет вычисляемые поля (net/gross/fees, R, duration, giveback, missed, trailing)
          - даже если каких-то полей нет в order:* — они будут 0/None (без падений)
        """
        try:
            list_key = f"closed:{strategy}:{symbol}:{tf}"
            trade_ids = cast(Any, self.redis.lrange(list_key, 0, -1))

            trades_out: list[dict[str, Any]] = []

            for order_id in trade_ids:
                order = self.redis.hgetall(f"order:{order_id}")
                if not order or not isinstance(order, dict):
                    continue

                out: dict[str, Any] = {"order_id": order_id, "order": order}

                # signal (пытаемся по двум ключам)
                if include_signal:
                    sig = self.redis.hgetall(f"signal:{order_id}")
                    if not isinstance(sig, dict):
                        sig = None
                    if not sig:
                        signal_id = order.get("signal_id") or order.get("sid") or order.get("signalId")
                        if signal_id:
                            sig = self.redis.hgetall(f"signal:{signal_id}")
                            if not isinstance(sig, dict):
                                sig = None
                    if sig:
                        out["signal"] = sig

                # events
                if include_events:
                    out["events"] = self._get_trade_events(order_id, limit=events_limit)

                # summary (приводим всё в нормальный вид)
                if include_summary:
                    out["summary"] = self._build_trade_summary_from_order(order)

                trades_out.append(out)

            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(trades_out, f, indent=2, ensure_ascii=False)

            self.logger.info(f"💾 Экспортировано {len(trades_out)} сделок в {filepath}")
            return True

        except Exception as e:
            self.logger.error(f"❌ Ошибка экспорта в JSON: {e}", exc_info=True)
            return False

    def _accumulate_stats(self, acc: dict[str, Any], s: dict[str, Any]) -> dict[str, Any]:
        """
        Складывает статистику (счётчики + суммы). Работает даже если часть полей отсутствует.
        """
        if not s:
            return acc

        # Counters
        for k in [
            "total_trades", "wins", "losses", "breakeven",
            "tp1_hits", "tp2_hits", "tp3_hits",
            "tp1_then_sl", "tp2_then_sl", "tp3_then_sl",
            "trailing_started", "trailing_stop_hits",
            "missed_profit_trades",
        ]:
            acc[k] = self._to_int(acc.get(k), 0) + self._to_int(s.get(k), 0)

        # Sums
        for k in [
            "total_pnl",
            "total_pnl_gross",
            "total_fees",
            "total_pnl_pct",
            "sum_r",
            "gross_profit",
            "gross_loss",
            "sum_duration_ms",
            "trailing_moves_total",
            "missed_profit_total",
            "giveback_total",
        ]:
            acc[k] = self._to_float(acc.get(k), 0.0) + self._to_float(s.get(k), 0.0)

        # Extremes
        for k in ["max_win", "max_loss", "max_r", "min_r", "max_drawdown"]:
            if k in s:
                if k not in acc:
                    acc[k] = self._to_float(s.get(k))
                else:
                    v = self._to_float(s.get(k))
                    if k.startswith("max_"):
                        acc[k] = max(self._to_float(acc.get(k)), v)
                    else:
                        acc[k] = min(self._to_float(acc.get(k)), v)

        return acc

    def _finalize_accumulated(self, acc: dict[str, Any]) -> dict[str, Any]:
        total = self._to_int(acc.get("total_trades"), 0)
        wins = self._to_int(acc.get("wins"), 0)
        losses = self._to_int(acc.get("losses"), 0)
        if total <= 0:
            acc["winrate"] = 0.0
            return acc

        acc["winrate"] = round(self._safe_div(wins * 100.0, total, 0.0), 2)

        # avg pnl
        total_pnl = self._to_float(acc.get("total_pnl"), 0.0)
        acc["avg_pnl"] = round(self._safe_div(total_pnl, total, 0.0), 4)

        # avg pnl_pct
        total_pnl_pct = self._to_float(acc.get("total_pnl_pct"), 0.0)
        acc["avg_pnl_pct"] = round(self._safe_div(total_pnl_pct, total, 0.0), 4)

        acc["avg_r"] = round(self._safe_div(self._to_float(acc.get("sum_r"), 0.0), total, 0.0), 4)
        acc["avg_duration_ms"] = round(self._safe_div(self._to_float(acc.get("sum_duration_ms"), 0.0), total, 0.0), 2)

        # Profit factor (gross)
        gp = self._to_float(acc.get("gross_profit"), 0.0)
        gl = self._to_float(acc.get("gross_loss"), 0.0)
        acc["profit_factor"] = round(self._safe_div(gp, gl, 0.0), 4)

        # missed_profit_avg
        missed_n = self._to_int(acc.get("missed_profit_trades"), 0)
        missed_total = self._to_float(acc.get("missed_profit_total"), 0.0)
        acc["missed_profit_avg"] = round(self._safe_div(missed_total, missed_n, 0.0), 2)

        # Breakeven
        if "breakeven" not in acc or self._to_int(acc.get("breakeven"), 0) == 0:
            # если не ведёте breakeven отдельно — оценим как остаток
            be = max(0, total - wins - losses)
            acc["breakeven"] = be

        return acc

    def _build_trade_summary_from_order(self, order: dict[str, Any]) -> dict[str, Any]:
        """
        Собирает нормализованную сводку сделки из order:* (или trade_summary-like dict).
        Ожидаемые новые поля (если есть): pnl_gross, pnl_net, fees, r, duration_ms, mae, mfe,
        giveback, missed_profit, trailing_started, trailing_moves, trailing_stop_hit.
        """
        raw_dir = order.get("direction") or order.get("side") or "LONG"
        direction = str(raw_dir).upper()

        entry = order.get("entry") or order.get("entry_price")
        sl = order.get("sl")

        exit_price = (
            order.get("exit")
            or order.get("exit_price")
            or order.get("close_price")
            or order.get("close_price_last")
        )

        entry_t = self._infer_ts_ms(order.get("entry_time") or order.get("entry_ts") or order.get("open_time"))
        close_t = self._infer_ts_ms(order.get("close_time") or order.get("close_ts") or order.get("exit_time"))

        duration_ms = self._to_float(order.get("duration_ms"), 0.0)
        if duration_ms <= 0 and entry_t > 0 and close_t > 0 and close_t >= entry_t:
            duration_ms = float(close_t - entry_t)

        commission = self._to_float(order.get("commission"), 0.0)
        swap = self._to_float(order.get("swap"), 0.0)
        fees = self._to_float(order.get("fees"), 0.0)
        if abs(fees) <= 1e-12:
            fees = commission + swap

        pnl_gross = self._to_float(order.get("pnl_gross"), 0.0)
        pnl_net = self._to_float(order.get("pnl_net"), 0.0)

        if abs(pnl_gross) <= 1e-12:
            # fallback: realized+remaining (если есть) или pnl
            realized = self._to_float(order.get("realized_pnl"), 0.0)
            remaining = self._to_float(order.get("remaining_pnl"), 0.0)
            pnl = self._to_float(order.get("pnl"), 0.0)
            pnl_gross = pnl if abs(pnl) > 1e-12 else (realized + remaining)

        if abs(pnl_net) <= 1e-12:
            pnl_net = pnl_gross - fees

        r = self._to_float(order.get("r"), 0.0)
        if abs(r) <= 1e-12 and entry is not None and sl is not None and exit_price is not None:
            r = self._compute_r(direction, self._to_float(entry), self._to_float(sl), self._to_float(exit_price))

        mae = self._to_float(order.get("mae"), 0.0)
        mfe = self._to_float(order.get("mfe"), 0.0)
        giveback = self._to_float(order.get("giveback"), 0.0)
        missed_profit = self._to_float(order.get("missed_profit"), 0.0)

        trailing_started = self._to_int(order.get("trailing_started"), 0)
        trailing_moves = self._to_float(order.get("trailing_moves"), 0.0)
        trailing_stop_hit = self._to_int(order.get("trailing_stop_hit"), 0)

        result = order.get("result")
        if not result:
            result = "win" if pnl_net > 1e-9 else ("loss" if pnl_net < -1e-9 else "breakeven")

        return {
            "pnl_gross": pnl_gross,
            "pnl_net": pnl_net,
            "fees": fees,
            "r": r,
            "duration_ms": duration_ms,
            "mae": mae,
            "mfe": mfe,
            "giveback": giveback,
            "missed_profit": missed_profit,
            "trailing_started": trailing_started,
            "trailing_moves": trailing_moves,
            "trailing_stop_hit": trailing_stop_hit,
            "result": result,
        }

    def get_sources_summary(self) -> dict[str, dict[str, Any]]:
        from services.stats_aggregator import StatsAggregator

        try:
            sources_summary: dict[str, dict[str, Any]] = {}
            raw_strategies = cast(Any, self.redis.smembers("stats:strategies"))
            strategies = [v.decode() if isinstance(v, bytes) else str(v) for v in raw_strategies] if raw_strategies else []
            
            for strategy in strategies:
                symbols = StatsAggregator.get_strategy_symbols(self.redis, strategy)
                for symbol in symbols:
                    tfs = StatsAggregator.get_strategy_timeframes(self.redis, strategy, symbol)
                    for tf in tfs:
                        sources = StatsAggregator.get_strategy_sources(self.redis, strategy, symbol, tf)
                        for src in sources:
                            st = StatsAggregator.get_stats_by_source(self.redis, strategy, symbol, tf, src)
                            if not st:
                                continue

                            if src not in sources_summary:
                                sources_summary[src] = {}

                            sources_summary[src] = self._accumulate_stats(sources_summary[src], st)

            # finalize derived
            for src, acc in sources_summary.items():
                # Вычисляем производные метрики
                total = self._to_int(acc.get("total_trades"))
                wins = self._to_int(acc.get("wins"))
                acc["winrate"] = (wins / total * 100.0) if total > 0 else 0.0
                acc["avg_pnl"] = (self._to_float(acc.get("total_pnl")) / total) if total > 0 else 0.0
                acc["avg_pnl_pct"] = (self._to_float(acc.get("total_pnl_pct")) / total) if total > 0 else 0.0
                acc["avg_r"] = (self._to_float(acc.get("sum_r")) / total) if total > 0 else 0.0
                acc["avg_duration_ms"] = (self._to_float(acc.get("sum_duration_ms")) / total) if total > 0 else 0.0

                gp = self._to_float(acc.get("gross_profit"))
                gl = self._to_float(acc.get("gross_loss"))
                acc["profit_factor"] = (gp / gl) if gl > 0 else 0.0

                mp_n = self._to_int(acc.get("missed_profit_trades"))
                mp_total = self._to_float(acc.get("missed_profit_total"))
                acc["missed_profit_avg"] = (mp_total / mp_n) if mp_n > 0 else 0.0

                tr_started = self._to_int(acc.get("trailing_started"))
                tr_hits = self._to_int(acc.get("trailing_stop_hits"))
                acc["trailing_effectiveness"] = (tr_hits / tr_started * 100.0) if tr_started > 0 else 0.0

            # если сделок нет — вернуть {}
            total_trades = sum(self._to_int(v.get("total_trades"), 0) for v in sources_summary.values())
            if total_trades == 0:
                return {}

            return sources_summary

        except Exception as e:
            self.logger.error(f"❌ Ошибка получения сводки по источникам: {e}", exc_info=True)
            return {}

    def get_performance_summary(self) -> dict[str, Any]:
        """
        Получение краткой сводки производительности системы.
        
        Returns:
            Словарь с ключевыми метриками
        """
        from services.stats_aggregator import StatsAggregator

        try:
            all_stats = StatsAggregator.get_all_stats(self.redis)  # { "strategy:symbol:tf": stats }

            total_trades = sum(self._to_int(s.get("total_trades")) for s in all_stats.values())
            wins = sum(self._to_int(s.get("wins")) for s in all_stats.values())
            losses = sum(self._to_int(s.get("losses")) for s in all_stats.values())
            breakevens = sum(self._to_int(s.get("breakevens")) for s in all_stats.values())

            total_pnl = sum(self._to_float(s.get("total_pnl")) for s in all_stats.values())
            total_pnl_gross = sum(self._to_float(s.get("total_pnl_gross")) for s in all_stats.values())
            total_fees = sum(self._to_float(s.get("total_fees")) for s in all_stats.values())

            gross_profit = sum(self._to_float(s.get("gross_profit")) for s in all_stats.values())
            gross_loss = sum(self._to_float(s.get("gross_loss")) for s in all_stats.values())
            profit_factor = self._safe_div(gross_profit, gross_loss, 0.0)

            sum_r = sum(self._to_float(s.get("sum_r")) for s in all_stats.values())
            sum_duration_ms = sum(self._to_float(s.get("sum_duration_ms")) for s in all_stats.values())

            missed_profit_total = sum(self._to_float(s.get("missed_profit_total")) for s in all_stats.values())
            missed_profit_trades = sum(self._to_int(s.get("missed_profit_trades")) for s in all_stats.values())
            giveback_total = sum(self._to_float(s.get("giveback_total")) for s in all_stats.values())

            trailing_started = sum(self._to_int(s.get("trailing_started")) for s in all_stats.values())
            trailing_stop_hits = sum(self._to_int(s.get("trailing_stop_hits")) for s in all_stats.values())
            trailing_moves_total = sum(self._to_float(s.get("trailing_moves_total")) for s in all_stats.values())

            winrate = (wins / total_trades * 100.0) if total_trades > 0 else 0.0
            avg_pnl = (total_pnl / total_trades) if total_trades > 0 else 0.0
            avg_r = (sum_r / total_trades) if total_trades > 0 else 0.0
            avg_duration_ms = (sum_duration_ms / total_trades) if total_trades > 0 else 0.0

            missed_profit_avg = (missed_profit_total / missed_profit_trades) if missed_profit_trades > 0 else 0.0
            trailing_eff = self._safe_div(trailing_stop_hits, trailing_started, 0.0) * 100.0

            return {
                "timestamp": get_ny_time_millis(),

                "total_groups": len(all_stats),
                "total_trades": total_trades,
                "wins": wins,
                "losses": losses,
                "breakevens": breakevens,
                "winrate": round(winrate, 2),

                "total_pnl": round(total_pnl, 2),
                "avg_pnl": round(avg_pnl, 2),

                "total_pnl_gross": round(total_pnl_gross, 2),
                "total_fees": round(total_fees, 2),

                "gross_profit": round(gross_profit, 2),
                "gross_loss": round(gross_loss, 2),
                "profit_factor": round(profit_factor, 3),

                "avg_r": round(avg_r, 4),
                "avg_duration_ms": round(avg_duration_ms, 0),

                "missed_profit_total": round(missed_profit_total, 2),
                "missed_profit_trades": missed_profit_trades,
                "missed_profit_avg": round(missed_profit_avg, 2),

                "giveback_total": round(giveback_total, 2),

                "trailing_started": trailing_started,
                "trailing_stop_hits": trailing_stop_hits,
                "trailing_effectiveness": round(trailing_eff, 2),
                "trailing_moves_total": round(trailing_moves_total, 2),
            }

        except Exception as e:
            self.logger.error(f"❌ Ошибка получения сводки: {e}", exc_info=True)
            return {}

