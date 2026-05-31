import logging

logger = logging.getLogger(__name__)

class RegimeDriftDetector:
    """
    Page-Hinkley test for sequential drift detection.
    Optimized for detecting negative drift in performance metrics (e.g., Win Rate, PnL).
    """
    def __init__(self, min_instances=30, delta=0.005, threshold=50, alpha=0.9999):
        self.min_instances = min_instances
        self.delta = delta
        self.threshold = threshold
        self.alpha = alpha  # forgetting factor

        self.n = 0
        self.mean = 0.0
        self.sum = 0.0
        self.ph_statistic = 0.0
        self.ph_max = 0.0

    def update(self, x: float) -> bool:
        """
        Update the Page-Hinkley statistics with a new value.
        Returns True if a drift is detected.
        """
        self.n += 1

        if self.n <= self.min_instances:
            self.sum += x
            self.mean = self.sum / self.n
            return False

        # Incremental mean with forgetting factor
        self.mean = self.alpha * self.mean + (1 - self.alpha) * x

        # We track cumulative difference from mean minus tolerance
        diff = self.mean - x - self.delta
        self.ph_statistic += diff

        # Track maximum seen so far
        if self.ph_statistic > self.ph_max:
            self.ph_max = self.ph_statistic

        # If difference from max exceeds threshold, drift is detected
        is_drift = (self.ph_max - self.ph_statistic) > self.threshold

        if is_drift:
            logger.info("Regime Drift detected! Resetting Page-Hinkley variables.")
            self.reset()
            return True

        return False

    def reset(self):
        self.n = 0
        self.mean = 0.0
        self.sum = 0.0
        self.ph_statistic = 0.0
        self.ph_max = 0.0
