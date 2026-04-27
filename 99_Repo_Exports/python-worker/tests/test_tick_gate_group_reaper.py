import unittest

from tools.tick_gate_group_reaper import _env_int, _env_float


class TestTickGateGroupReaper(unittest.TestCase):
    def test_env_int(self):
        self.assertEqual(_env_int("NO_SUCH", 7), 7)

    def test_env_float(self):
        self.assertEqual(_env_float("NO_SUCH", 0.25), 0.25)


if __name__ == "__main__":
    unittest.main()
