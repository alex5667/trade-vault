from __future__ import annotations
from core.redis_keys import RedisStreams as RS

"""
Threshold Tuner - Автоматический подбор порога для confidence/score.

Функции:
- Построение ROC кривой
- Выбор оптимального порога (Youden Index, F1-score)
- Публикация порога в Redis
- Уведомление aggregated hub о перезагрузке
- Telegram отчёт

Использование:
- Анализ эффективности сигналов
- Оптимизация фильтрации по confidence
- Автоматическая калибровка
"""

import json
import os
import time
from typing import Any

from analytics.metrics import calculate_confusion_matrix, calculate_precision_recall, roc_from_signals
from analytics.repository import Order, Repository, Signal
from analytics.roc_store import ROCStore
from common.log import setup_logger


class ThresholdTuner:
    """
    Автоподбор порога по ROC кривой.
    
    Публикует:
    - hub:threshold:{strategy}:{symbol} - порог для aggregated hub
    - analytics:roc:{strategy}:{symbol} - ROC точки
    - aggregated_hub:control stream - команда перезагрузки
    - notify:telegram stream - отчёт
    """

    def __init__(self, repo: Repository):
        """
        Инициализация Threshold Tuner.
        
        Args:
            repo: Repository для доступа к данным
        """
        self.logger = setup_logger("ThresholdTuner")

        self.repo = repo
        self.r = repo.r

        self.ctrl_stream = os.getenv("AGG_HUB_CONTROL_STREAM", RS.AGG_HUB_CONTROL)
        self.notify_stream = os.getenv("NOTIFY_STREAM", RS.NOTIFY_TELEGRAM)

        self.roc_store = ROCStore(os.getenv("REDIS_URL"))

    # _confusion_matrix and _f1_score removed — use shared helpers:
    #   calculate_confusion_matrix(scores, labels, thr)  → (tp, fp, tn, fn)
    #   calculate_precision_recall(tp, fp, fn)            → (precision, recall, f1)

    def _extract_scores_labels(
        self,
        signals: list[Signal],
        order_by_signal: dict[str, Order]
    ) -> list[tuple[float, int]]:
        """Извлечение пар (score, label) из сигналов и ордеров"""
        pairs = []

        for s in signals:
            # Приоритет: score > confidence
            sc = s.score if s.score is not None else s.confidence

            if sc is None:
                continue

            # Получаем результат сделки
            o = order_by_signal.get(s.signal_id)

            if not o:
                continue

            # Метка: 1 если прибыль, 0 если убыток
            label = 1 if (o.pnl_usd is None or o.pnl_usd > 0) else 0

            pairs.append((float(sc), label))

        return pairs

    def tune_and_publish(
        self,
        *,
        strategy: str,
        symbol: str,
        signals: list[Signal],
        orders: list[Order],
        emit_telegram: bool = True
    ) -> dict[str, Any] | None:
        """
        Подбор оптимального порога и публикация.
        
        Args:
            strategy: Название стратегии
            symbol: Символ
            signals: Список сигналов
            orders: Список ордеров
            emit_telegram: Отправлять уведомление в Telegram
            
        Returns:
            Словарь с результатами или None
        """
        try:
            self.logger.info(f"🔧 Тюнинг порога для {strategy}/{symbol}...")

            order_by_signal = {o.signal_id: o for o in orders if o.signal_id}
            pairs = self._extract_scores_labels(signals, order_by_signal)

            if not pairs:
                self.logger.warning(f"⚠️ Нет данных для тюнинга {strategy}/{symbol}")
                return None

            # Строим ROC
            roc = roc_from_signals(signals, order_by_signal)

            if not roc:
                self.logger.warning(f"⚠️ Не удалось построить ROC для {strategy}/{symbol}")
                return None

            # Вычисляем метрики для каждого порога
            thresholds = sorted({s for s, _ in pairs})
            points_payload = []

            best_by_j = None  # Youden Index
            best_by_f1 = None  # F1-score

            scores_list = [s for s, _ in pairs]
            labels_list = [label for _, label in pairs]

            for thr in thresholds:
                tp, fp, tn, fn = calculate_confusion_matrix(scores_list, labels_list, thr)

                tpr = tp / (tp + fn) if (tp + fn) > 0 else 0.0
                fpr = fp / (fp + tn) if (fp + tn) > 0 else 0.0
                _prec, rec, f1 = calculate_precision_recall(tp, fp, fn)

                youden_j = tpr - fpr

                points_payload.append({
                    "thr": round(thr, 4),
                    "tpr": round(tpr, 4),
                    "fpr": round(fpr, 4),
                    "prec": round(_prec, 4),
                    "rec": round(rec, 4),
                    "f1": round(f1, 4),
                    "support": tp + fp + tn + fn
                })

                # Отслеживаем лучшие
                if (best_by_j is None) or (youden_j > best_by_j[0]):
                    best_by_j = (youden_j, thr, tpr, fpr, f1)

                if (best_by_f1 is None) or (f1 > best_by_f1[0]):
                    best_by_f1 = (f1, thr, tpr, fpr)

            # Сохраняем ROC точки
            self.roc_store.save(strategy, symbol, points_payload, roc.auc)

            # Выбираем финальный порог по Youden J
            chosen_thr = float(best_by_j[1])

            payload = {
                "thr": chosen_thr,
                "auc": round(roc.auc, 4),
                "youdenJ": round(best_by_j[0], 4),
                "f1_at_thr": round(best_by_j[4], 4),
                "tpr": round(best_by_j[2], 4),
                "fpr": round(best_by_j[3], 4),
                "tuned_at": time.time(),
                "support": len(pairs),
                "rule": "youdenJ"
            }

            # Публикуем порог для aggregated hub
            key = f"hub:threshold:{strategy}:{symbol}"
            self.r.set(key, json.dumps(payload))

            self.logger.info(
                f"✅ Порог установлен: {strategy}/{symbol} | "
                f"thr={chosen_thr:.2f} | AUC={payload['auc']:.3f} | J={payload['youdenJ']:.3f}"
            )

            # Уведомляем aggregated hub о перезагрузке
            self.r.xadd(
                self.ctrl_stream,
                {
                    "action": "reload",
                    "scope": f"{strategy}:{symbol}",
                    "ts": time.time()
                },
                maxlen=50000,
                approximate=True
            )

            # Telegram уведомление
            if emit_telegram:
                msg = (
                    f"<b>⚙️ Threshold Tuned</b>\n\n"
                    f"<b>Strategy:</b> <code>{strategy}</code>\n"
                    f"<b>Symbol:</b> <b>{symbol}</b>\n\n"
                    f"<b>Threshold:</b> {chosen_thr:.2f}\n"
                    f"<b>AUC:</b> {payload['auc']:.3f}\n"
                    f"<b>Youden J:</b> {payload['youdenJ']:.3f}\n"
                    f"<b>F1-score:</b> {payload['f1_at_thr']:.3f}\n"
                    f"<b>TPR:</b> {payload['tpr']:.1%}\n"
                    f"<b>FPR:</b> {payload['fpr']:.1%}\n\n"
                    f"<b>Support:</b> {payload['support']} сделок"
                )

                self.r.xadd(
                    self.notify_stream,
                    {
                        "text": msg,
                        "parse_mode": "HTML"
                    },
                    maxlen=50000,
                    approximate=True
                )

                self.logger.info("📱 Telegram уведомление отправлено")

            return payload

        except Exception as e:
            self.logger.error(f"❌ Ошибка тюнинга порога: {e}", exc_info=True)
            return None

    def get_threshold(self, strategy: str, symbol: str) -> dict[str, Any] | None:
        """
        Получение текущего порога.
        
        Args:
            strategy: Название стратегии
            symbol: Символ
            
        Returns:
            Словарь с порогом и метриками или None
        """
        try:
            key = f"hub:threshold:{strategy}:{symbol}"
            data = self.r.get(key)

            if not data:
                return None

            return json.loads(data)

        except Exception as e:
            self.logger.error(f"❌ Ошибка получения порога: {e}")
            return None

