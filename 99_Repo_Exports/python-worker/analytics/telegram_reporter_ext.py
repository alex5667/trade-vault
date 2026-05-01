from __future__ import annotations
"""
Telegram Reporter Extended - Расширенные отчёты с графиками в Telegram.

Функции:
- Отправка "карусели" сообщений (group_id)
- Генерация PNG графиков (ROC, Confusion Matrix)
- Публикация через notify:telegram stream
- Поддержка фото и текста

Требования:
- matplotlib для графиков (опционально)
- notify-worker должен поддерживать photo_path
"""

import os
import time
from typing import Dict, List, Any, Optional

import redis

from common.log import setup_logger

# Попытка импорта matplotlib
try:
    import matplotlib
    matplotlib.use('Agg')  # Без GUI
    import matplotlib.pyplot as plt
    import numpy as np
    _HAS_MPL = True
except ImportError:
    _HAS_MPL = False
    plt = None
    np = None


class TelegramReporterExt:
    """
    Расширенный репортер для Telegram с поддержкой графиков.
    
    Отправляет "карусели" сообщений с одинаковым group_id
    и опциональными PNG графиками.
    """

    def __init__(self, redis_url: Optional[str] = None):
        """
        Инициализация репортера.
        
        Args:
            redis_url: URL Redis (опционально)
        """
        self.logger = setup_logger("TelegramReporterExt")

        redis_url = redis_url or os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
        self.r = redis.from_url(redis_url, decode_responses=True)

        try:
            self.r.ping()
            self.logger.info("✅ Redis подключение установлено")
        except Exception as e:
            self.logger.error(f"❌ Ошибка подключения к Redis: {e}")
            raise

        self.stream = os.getenv("NOTIFY_STREAM", "notify:telegram")

        if _HAS_MPL:
            self.logger.info("✅ Matplotlib доступен, графики будут генерироваться")
        else:
            self.logger.warning("⚠️ Matplotlib недоступен, графики отключены")

    def _push_text(self, group_id: str, title: str, lines: List[str]):
        """Отправка текстового сообщения"""
        try:
            text = f"<b>{title}</b>\n" + "\n".join(lines)

            self.r.xadd(
                self.stream,
                {
                    "group_id": group_id,
                    "text": text,
                    "parse_mode": "HTML"
                },
                maxlen=50000,
                approximate=True
            )

        except Exception as e:
            self.logger.error(f"❌ Ошибка отправки текста: {e}")

    def _save_png(self, fig, path: str):
        """Сохранение графика в PNG"""
        try:
            fig.savefig(path, bbox_inches="tight", dpi=100)
            plt.close(fig)
        except Exception as e:
            self.logger.error(f"❌ Ошибка сохранения PNG: {e}")

    def _push_photo(self, group_id: str, caption: str, photo_path: str):
        """
        Отправка фотографии.
        
        Требует поддержки photo_path в notify-worker.
        Если не поддерживается, поле просто игнорируется.
        """
        try:
            self.r.xadd(
                self.stream,
                {
                    "group_id": group_id,
                    "photo_path": photo_path,
                    "text": caption, # notify-worker expects 'text' or 'message'
                    "caption": caption, # also keep caption for standard Telegram API compatibility
                    "parse_mode": "HTML"
                },
                maxlen=2000,
                approximate=True
            )

        except Exception as e:
            self.logger.error(f"❌ Ошибка отправки фото: {e}")

    def send_roc_report(
        self,
        *,
        strategy: str,
        symbol: str,
        roc_points: List[Dict[str, Any]],
        auc: float,
        summary: Dict[str, Any]
    ):
        """
        Отправка ROC отчёта с графиком.
        
        Args:
            strategy: Название стратегии
            symbol: Символ
            roc_points: Точки ROC кривой
            auc: AUC значение
            summary: Сводка с best threshold
        """
        try:
            gid = f"roc:{strategy}:{symbol}:{int(time.time())}"

            # 1) Текстовая сводка
            lines = [
                f"<b>Strategy:</b> <code>{strategy}</code>",
                f"<b>Symbol:</b> {symbol}",
                f"<b>AUC:</b> {auc:.3f}",
                f"<b>Best threshold:</b> {summary.get('thr', 0):.2f}",
                f"<b>Youden J:</b> {summary.get('youdenJ', 0):.3f}",
                f"<b>F1-score:</b> {summary.get('f1_at_thr', 0):.3f}",
                f"<b>Support:</b> {summary.get('support', 0)} сделок"
            ]

            self._push_text(gid, "📈 ROC Analysis", lines)

            # 2) График ROC (если есть matplotlib)
            if _HAS_MPL and roc_points and plt and np:
                try:
                    fprs = [p["fpr"] for p in roc_points]
                    tprs = [p["tpr"] for p in roc_points]

                    fig, ax = plt.subplots(figsize=(8, 6))
                    ax.plot(fprs, tprs, label=f"AUC={auc:.3f}", linewidth=2)
                    ax.plot([0, 1], [0, 1], 'k--', label="Random", alpha=0.3)
                    ax.set_xlabel("False Positive Rate", fontsize=12)
                    ax.set_ylabel("True Positive Rate", fontsize=12)
                    ax.set_title(f"ROC Curve: {strategy}/{symbol}", fontsize=14)
                    ax.legend()
                    ax.grid(True, alpha=0.3)

                    # Сохраняем
                    out_dir = os.getenv("REPORT_IMG_DIR", "/data/reports")
                    os.makedirs(out_dir, exist_ok=True)
                    out_path = os.path.join(out_dir, f"roc_{strategy}_{symbol}_{int(time.time())}.png")

                    self._save_png(fig, out_path)

                    # Отправляем
                    self._push_photo(
                        gid,
                        f"ROC Curve: {strategy}/{symbol}",
                        out_path
                    )

                    self.logger.info(f"📊 ROC график сохранён: {out_path}")

                except Exception as e:
                    self.logger.error(f"❌ Ошибка генерации ROC графика: {e}")

            self.logger.info(f"📱 ROC отчёт отправлен: {strategy}/{symbol}")

        except Exception as e:
            self.logger.error(f"❌ Ошибка отправки ROC отчёта: {e}", exc_info=True)

    def send_confusion_matrix_report(
        self,
        *,
        strategy: str,
        symbol: str,
        tp: int,
        fp: int,
        tn: int,
        fn: int,
        threshold: float
    ):
        """
        Отправка отчёта с confusion matrix.
        
        Args:
            strategy: Название стратегии
            symbol: Символ
            tp, fp, tn, fn: Confusion matrix значения
            threshold: Используемый порог
        """
        try:
            gid = f"cm:{strategy}:{symbol}:{int(time.time())}"

            # Вычисляем метрики
            precision = tp / (tp + fp) if (tp + fp) > 0 else 0.0
            recall = tp / (tp + fn) if (tp + fn) > 0 else 0.0
            f1 = 2 * precision * recall / (precision + recall) if (precision + recall) > 0 else 0.0
            accuracy = (tp + tn) / (tp + fp + tn + fn) if (tp + fp + tn + fn) > 0 else 0.0

            # Текстовый отчёт
            lines = [
                f"<b>Strategy:</b> <code>{strategy}</code>",
                f"<b>Symbol:</b> {symbol}",
                f"<b>Threshold:</b> {threshold:.2f}",
                "",
                "<b>Confusion Matrix:</b>",
                f"  TP: {tp}  |  FP: {fp}",
                f"  FN: {fn}  |  TN: {tn}",
                "",
                "<b>Metrics:</b>",
                f"  Precision: {precision:.1%}",
                f"  Recall: {recall:.1%}",
                f"  F1-score: {f1:.3f}",
                f"  Accuracy: {accuracy:.1%}"
            ]

            self._push_text(gid, "🎯 Confusion Matrix", lines)

            # График confusion matrix (если есть matplotlib)
            if _HAS_MPL and plt and np:
                try:
                    cm = np.array([[tp, fp], [fn, tn]])

                    fig, ax = plt.subplots(figsize=(6, 5))
                    im = ax.imshow(cm, cmap='Blues')

                    # Аннотации
                    for i in range(2):
                        for j in range(2):
                             _ = ax.text(j, i, cm[i, j], ha="center", va="center", fontsize=20)

                    ax.set_xticks([0, 1])
                    ax.set_yticks([0, 1])
                    ax.set_xticklabels(['Predicted Win', 'Predicted Loss'])
                    ax.set_yticklabels(['Actual Win', 'Actual Loss'])
                    ax.set_title(f"Confusion Matrix: {strategy}/{symbol}\n(threshold={threshold:.2f})")

                    plt.colorbar(im, ax=ax)

                    # Сохраняем
                    out_dir = os.getenv("REPORT_IMG_DIR", "/data/reports")
                    os.makedirs(out_dir, exist_ok=True)
                    out_path = os.path.join(out_dir, f"cm_{strategy}_{symbol}_{int(time.time())}.png")

                    self._save_png(fig, out_path)

                    # Отправляем
                    self._push_photo(
                        gid,
                        f"Confusion Matrix: {strategy}/{symbol}",
                        out_path
                    )

                except Exception as e:
                    self.logger.error(f"❌ Ошибка генерации CM графика: {e}")

            self.logger.info(f"📱 Confusion Matrix отчёт отправлен: {strategy}/{symbol}")

        except Exception as e:
            self.logger.error(f"❌ Ошибка отправки CM отчёта: {e}", exc_info=True)

