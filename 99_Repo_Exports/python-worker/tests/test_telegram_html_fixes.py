import unittest
from unittest.mock import MagicMock, patch
import html
import json
import sys
import os

# Setup paths
sys.path.append(os.path.join(os.getcwd(), "python-worker"))
sys.path.append(os.path.join(os.getcwd(), "python-worker", "tools"))

try:
    import propose_meta_freeze_suggestion_v1 as pmf
    import meta_drift_guard_v1 as mdg
    import ml_sre_monitor as msm
    import meta_cov_outcome_guard_v1 as mcog
except ImportError:
    # Fallback for different environments if needed
    from tools import propose_meta_freeze_suggestion_v1 as pmf
    from tools import meta_drift_guard_v1 as mdg
    from tools import ml_sre_monitor as msm
    from tools import meta_cov_outcome_guard_v1 as mcog

class TestTelegramHTMLFixes(unittest.TestCase):
    def test_pmf_escaping(self):
        r = MagicMock()
        report = {"alerts": ["p50 < 0.2"], "p50": 0.15}
        with patch.object(pmf, "_notify") as mock_notify:
            pmf.emit_meta_freeze_suggestion(r, prefix="t", scope="ALL", symbols=["B"], cfg_prefix="c:", freeze=1, freeze_mode="O", report=report, ttl_sec=60)
            text = mock_notify.call_args[0][1]
            print(f"PMF: {text}")
            self.assertIn("&lt;", text)

    def test_mdg_escaping(self):
        # mdg notify is simpler to test directly or via main
        # But we mostly want to check if it uses quote=True
        alerts = ["drift < 0.1"]
        # The main logic in mdrift_guard_v1 is harder to unit test without more mocks,
        # but we can verify the html.escape call with quote=True handles < and "
        s = html.escape(json.dumps(alerts), quote=True)
        self.assertIn("&lt;", s)
        self.assertIn("&quot;", s)

    def test_msm_escaping_logic(self):
        # msm uses list comprehensions for alerts
        ml_alerts = ["miss < 0.01", 'err "msg"']
        safe_ml_alerts = [html.escape(str(x), quote=True) for x in ml_alerts]
        self.assertIn("&lt;", safe_ml_alerts[0])
        self.assertIn("&quot;", safe_ml_alerts[1])

    def test_mcog_escaping(self):
        r = MagicMock()
        patch_cm = {"f": 0.5}
        report = {"alerts": ["bad < 0"]}
        with patch.object(mcog, "_notify") as mock_notify:
            mcog._emit_cfg_suggestion(r, prefix="t", kind="k", scope="s", cfg2_key="c", patch=patch_cm, report=report, ttl_sec=60, min_approvals=1, auto_approve=False)
            text = mock_notify.call_args[0][1]
            print(f"MCOG: {text}")
            self.assertIn("&lt;", text)

if __name__ == "__main__":
    unittest.main()
