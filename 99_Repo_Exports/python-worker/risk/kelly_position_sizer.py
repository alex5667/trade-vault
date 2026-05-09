import logging

logger = logging.getLogger(__name__)

class KellyPositionSizer:
    """
    Computes position sizes dynamically using the Kelly Criterion:
    f* = W - ((1 - W) / R)
    where:
    W = Historical win rate
    R = Historical average Reward to Risk ratio (e.g. Mean PnL of Wins / Mean Loss of Losses)
    
    This acts as a fraction of total account capital or a scaling multiplier.
    Typically, "Half Kelly" or "Quarter Kelly" is used to manage variance.
    """
    def __init__(self, kelly_fraction=0.5, max_risk_per_trade: float = 0.05, min_risk_per_trade: float = 0.005):
        self.kelly_fraction = kelly_fraction
        self.max_risk_per_trade = max_risk_per_trade
        self.min_risk_per_trade = min_risk_per_trade

    def get_position_size(self, win_rate: float, reward_risk_ratio: float, current_capital: float = 1.0) -> float:
        """
        Calculates the amount of capital/risk to allocate to the next trade.
        Returns the raw percentage risk to take (or scaled capital multiplier).
        """
        if reward_risk_ratio <= 0:
            logger.warning("Reward to risk ratio is <= 0. Cannot compute Kelly. Assuming minimum risk.")
            return self.min_risk_per_trade * current_capital

        if win_rate <= 0 or win_rate >= 1.0:
            if win_rate <= 0:
                logger.warning("Win rate is 0. Returning 0 size.")
                return 0.0
            else:
                logger.info("Win rate is 1.0. Betting max allowed risk.")
                return self.max_risk_per_trade * current_capital

        # Full Kelly fraction
        kelly = win_rate - ((1.0 - win_rate) / reward_risk_ratio)

        # Apply Kelly fraction (e.g., half-kelly)
        adjusted_kelly = kelly * self.kelly_fraction

        if adjusted_kelly <= 0:
            logger.info(f"Kelly fraction is non-positive ({adjusted_kelly:.4f}). Returning 0 size.")
            return 0.0

        # Bound the risk
        bounded_kelly = min(max(adjusted_kelly, self.min_risk_per_trade), self.max_risk_per_trade)
        return bounded_kelly * current_capital
