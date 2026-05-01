import logging

logger = logging.getLogger(__name__)

class DefaultStats:
    def __init__(self):
        self.win_rate = 0.5
        self.avg_rr = 1.0
        self.n_trades = 0

class KellyPositionSizer:
    """
    Half-Kelly для безопасности: f* = (p*b - q) / b / 2
    p = win_rate, b = avg_win/avg_loss (R:R), q = 1-p
    """
    def __init__(self, db_pool, min_size: float = 0.01, max_size: float = 0.1):
        self.db = db_pool
        self.min_size = min_size
        self.max_size = max_size

    async def compute(self, symbol: str, regime: str, confidence: float) -> float:
        stats = await self.get_rolling_stats(symbol, regime, days=30)
        if stats.n_trades < 20:
            return self.min_size  # недостаточно данных
        
        p = stats.win_rate
        b = stats.avg_rr  # avg R:R ratio
        q = 1.0 - p
        
        # Full Kelly
        kelly_f = (p * b - q) / b if b > 0.0 else 0.0
        
        # Half-Kelly для снижения дисперсии
        half_kelly = kelly_f * 0.5
        
        # Масштабируем на уверенность сигнала
        adjusted = half_kelly * confidence  # confidence из ensemble
        
        # Hard limits
        return max(self.min_size, min(adjusted, self.max_size))
    
    async def get_rolling_stats(self, symbol: str, regime: str, days: int) -> DefaultStats:
        # Из signal_outcomes с фильтром по режиму
        # NOTE: Using a query structured for timescaledb/postgres
        query = """
            SELECT 
                COALESCE(AVG(CAST(is_win AS integer)), 0.5) as win_rate, 
                COALESCE(AVG(r_multiple), 1.0) as avg_rr,
                COUNT(*) as n_trades
            FROM signal_outcomes
            WHERE symbol=$1 AND regime=$2 
              AND ts > now() - $3 * interval '1 day'
        """
        try:
            row = await self.db.fetchrow(query, symbol, regime, days)
            if row and row['n_trades'] > 0:
                stats = DefaultStats()
                stats.win_rate = float(row['win_rate'])
                stats.avg_rr = float(row['avg_rr'])
                stats.n_trades = int(row['n_trades'])
                return stats
        except Exception as e:
            logger.error(f"Error fetching rolling stats: {e}")
            
        return DefaultStats()
