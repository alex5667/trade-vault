import logging

logger = logging.getLogger(__name__)

class RegimeDriftDetector:
    """
    Page-Hinkley test для детекции изменения распределения метрик сигналов.
    Если win_rate падает более чем на 2σ за последние N сигналов → алерт.
    """
    def __init__(self, delta: float = 0.005, lambda_: float = 50.0):
        self.delta = delta    # минимальный ожидаемый сдвиг
        self.lambda_ = lambda_  # порог срабатывания
        self._m = 0.0
        self._M = 0.0
        self._n = 0

    def update(self, outcome: float) -> bool:
        """
        outcome: 1.0 = win, 0.0 = loss. Returns True если drift detected.
        """
        self._n += 1
        self._m += (outcome - self._m) / self._n  # online mean
        self._M = max(self._M, self._m)
        ph = self._M - self._m + self._n * self.delta
        if ph > self.lambda_:
            logger.warning(f"DRIFT DETECTED: Page-Hinkley statistic {ph:.2f} > threshold {self.lambda_}")
            # 1. Алерт в Telegram/Alertmanager
            # 2. Уменьшить position size (переключить в conservative risk profile)
            # 3. Запустить re-calibration
            self._reset()
            return True  # DRIFT DETECTED
        return False

    def _reset(self):
        self._m = self._M = 0.0
        self._n = 0
