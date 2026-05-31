import unittest
from dataclasses import dataclass

from domain.calculators import update_excursions


@dataclass
class MockPos:
    direction: str = "LONG"
    max_price_seen: float = 0.0
    min_price_seen: float = 0.0
    max_favorable_price: float = 0.0
    max_favorable_ts: int = 0
    max_adverse_price: float = 0.0
    max_adverse_ts: int = 0

    def is_long(self):
        return self.direction == "LONG"

class TestUpdateExcursionsFix(unittest.TestCase):
    def test_initialization_on_first_tick(self):
        # Scenario: New position, max_price_seen is 0.0. Tick price 100.
        # It MUST update max_price_seen to 100.
        pos = MockPos()
        update_excursions(pos, 100.0, 1000)

        self.assertEqual(pos.max_price_seen, 100.0, "max_price_seen not initialized")
        self.assertEqual(pos.min_price_seen, 100.0, "min_price_seen not initialized")
        self.assertEqual(pos.max_favorable_price, 100.0)

    def test_update_higher(self):
        pos = MockPos(max_price_seen=100.0, min_price_seen=100.0)
        update_excursions(pos, 105.0, 2000)

        self.assertEqual(pos.max_price_seen, 105.0)
        self.assertEqual(pos.min_price_seen, 100.0)

    def test_update_lower(self):
        pos = MockPos(max_price_seen=100.0, min_price_seen=100.0)
        update_excursions(pos, 95.0, 2000)

        self.assertEqual(pos.max_price_seen, 100.0)
        self.assertEqual(pos.min_price_seen, 95.0)

if __name__ == "__main__":
    unittest.main()
