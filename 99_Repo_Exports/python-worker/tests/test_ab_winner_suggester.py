
import unittest
from unittest.mock import MagicMock, patch
from services.ab_winner_suggester_service import ABWinnerSuggesterService, ArmStats

class TestABWinnerSuggester(unittest.TestCase):
    def test_arm_stats(self):
        s = ArmStats()
        s.wins = 5
        s.total = 10
        s.pnl_sum = 100.0
        
        self.assertEqual(s.win_rate, 0.5)
        self.assertEqual(s.mean_pnl, 10.0)
        
    def test_choose_winner_basic(self):
        svc = ABWinnerSuggesterService()
        svc.min_samples = 5
        
        arms = {
            "A": ArmStats(wins=6, total=10, pnl_sum=100.0), # Mean: 10
            "B": ArmStats(wins=8, total=10, pnl_sum=200.0), # Mean: 20
        }
        
        winner = svc.choose_winner(arms)
        self.assertEqual(winner, "B")

    def test_choose_winner_min_samples(self):
        svc = ABWinnerSuggesterService()
        svc.min_samples = 5
        
        arms = {
            "A": ArmStats(wins=2, total=4, pnl_sum=100.0), # Mean: 25 (but low samples)
            "B": ArmStats(wins=8, total=10, pnl_sum=50.0), # Mean: 5
        }
        
        # A has high mean but < min_samples. B has > min_samples.
        # Should pick B presumably? Or fallback to A if B is bad? 
        # Logic says: filter < min_samples.
        # So only B remains.
        winner = svc.choose_winner(arms)
        self.assertEqual(winner, "B")
        
    def test_choose_winner_all_low_samples(self):
        svc = ABWinnerSuggesterService()
        svc.min_samples = 10
        
        arms = {
            "A": ArmStats(wins=2, total=4, pnl_sum=100.0),
            "B": ArmStats(wins=2, total=4, pnl_sum=200.0),
        }
        
        # Both filtered out. Fallback to "A".
        winner = svc.choose_winner(arms)
        self.assertEqual(winner, "A")

    @patch("redis.asyncio.Redis")
    async def test_process_flow(self, mock_redis):
        # Ensure we can import and instantiate
        svc = ABWinnerSuggesterService()
        pass

if __name__ == "__main__":
    unittest.main()
