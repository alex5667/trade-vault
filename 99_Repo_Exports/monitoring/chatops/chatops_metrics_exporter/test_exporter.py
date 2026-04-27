import unittest
from unittest.mock import patch, MagicMock
from exporter import _scan_cmd_keys, render_metrics, _get_int

class TestExporter(unittest.TestCase):
    def test_get_int(self):
        m = MagicMock()
        m.get.return_value = b'42'
        self.assertEqual(_get_int(m, "foo"), 42)
        m.get.return_value = None
        self.assertEqual(_get_int(m, "bar"), 0)
        m.get.side_effect = Exception("error")
        self.assertEqual(_get_int(m, "baz"), 0)

    def test_scan_cmd_keys(self):
        m = MagicMock()
        m.scan.side_effect = [(0, ["metrics:chatops:cmd_total:set", "metrics:chatops:cmd_total:clear"])]
        m.get.side_effect = lambda k: b'10' if 'set' in k else b'5'
        
        res = _scan_cmd_keys(m)
        self.assertEqual(res, [("clear", 5), ("set", 10)])

    @patch('exporter._r')
    def test_render_metrics(self, mock_r):
        m = MagicMock()
        mock_r.return_value = m
        def mock_get_int(r, k):
            if "unauth" in k: return 12
            if "rate_lim" in k and "last" not in k: return 3
            return 0
            
        with patch('exporter._get_int', side_effect=mock_get_int):
            with patch('exporter._scan_cmd_keys', return_value=[("status", 15)]):
                metrics = render_metrics()
                self.assertIn("chatops_unauthorized_total 12", metrics)
                self.assertIn("chatops_rate_limited_total 3", metrics)
                self.assertIn('chatops_cmd_total{cmd="status"} 15', metrics)

if __name__ == '__main__':
    unittest.main()
